from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
from typing import Any, Callable

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from asic_pipeline.audit.report import utc_timestamp
from asic_pipeline.config import load_yaml_mapping, required_string, resolve_path
from asic_pipeline.errors import ConfigurationError, HarmonizationError
from asic_pipeline.harmonization.categorical_decisions import (
    ReviewedCategoricalDecisions,
    ReviewedCategoricalRule,
    load_reviewed_categorical_decisions,
)
from asic_pipeline.harmonization.consolidated_decisions import (
    ApprovedHospitalConversion,
    ReviewedConsolidatedDecisions,
    load_reviewed_consolidated_decisions,
)
from asic_pipeline.harmonization.schema_dictionary_freeze import (
    SchemaDictionaryFreezeConfig,
    SchemaDictionaryFreezePolicy,
    load_schema_dictionary_freeze_config,
    load_schema_dictionary_freeze_policy,
)
from asic_pipeline.harmonization.schema_dictionary_review import (
    resolve_reviewed_categorical_token,
)
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.inventory.report import RUN_ID_PATTERN, default_run_id
from asic_pipeline.privacy import assert_review_payload_is_safe


@dataclass(frozen=True)
class HarmonizedBuildPolicy:
    version: str
    candidate_run_id: str
    candidate_artifact_version: str
    candidate_status: str
    frozen_contract_version: str
    frozen_contract_directory: Path
    consolidated_decisions_path: Path
    consolidated_decisions_sha256: str
    categorical_decisions_path: Path
    categorical_decisions_sha256: str
    freeze_policy_path: Path
    freeze_policy_sha256: str
    hospitals: tuple[str, ...]
    expected_rows: tuple[tuple[str, int], ...]
    time_source: str
    time_target: str
    time_anchor: str
    rows_per_batch: int
    compression: str
    output_directory_name: str
    artifact_version: str
    review_directory_name: str
    output_status: str
    source_path: Path


@dataclass(frozen=True)
class HarmonizedBuildConfig:
    freeze: SchemaDictionaryFreezeConfig
    policy_path: Path

    @property
    def dataset_context(self) -> str:
        return self.freeze.dataset_context

    @property
    def data_root(self) -> Path:
        return self.freeze.data_root

    @property
    def reports_root(self) -> Path:
        return self.freeze.reports_root


@dataclass(frozen=True)
class HarmonizedBuildResult:
    run_id: str
    output_directory: Path
    manifest_path: Path
    review_json_path: Path
    review_markdown_path: Path


def _mapping(value: Any, location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigurationError(f"{location} must be a YAML mapping")
    return dict(value)


def _positive_int(value: Any, location: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ConfigurationError(f"{location} must be a positive integer")
    return value


def load_harmonized_build_config(path: str | Path) -> HarmonizedBuildConfig:
    source = Path(path).expanduser().resolve()
    freeze = load_schema_dictionary_freeze_config(source)
    raw = load_yaml_mapping(source, "Harmonized build configuration")
    policy_path = resolve_path(
        required_string(raw, "harmonized_build_policy", "config"), source
    )
    return HarmonizedBuildConfig(freeze, policy_path)


def load_harmonized_build_policy(path: str | Path) -> HarmonizedBuildPolicy:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Harmonized build policy")
    if raw.get("harmonized_build_policy_version") != "0.1" or raw.get(
        "status"
    ) != "approved_for_frozen_contract_bound_nonpublishable_build":
        raise ConfigurationError("Harmonized build policy is invalid")
    inputs = _mapping(raw.get("inputs"), "inputs")
    decisions = _mapping(raw.get("decision_sources"), "decision_sources")
    hospitals = raw.get("approved_hospitals")
    if (
        not isinstance(hospitals, list)
        or len(hospitals) != 8
        or len(set(hospitals)) != len(hospitals)
        or any(not isinstance(item, str) or not item for item in hospitals)
    ):
        raise ConfigurationError("Approved build hospitals are invalid")
    expected_rows_raw = _mapping(raw.get("expected_rows"), "expected_rows")
    expected_rows = tuple(
        (table, _positive_int(expected_rows_raw.get(table), f"expected_rows.{table}"))
        for table in ("static", "dynamic")
    )
    transformations = _mapping(
        raw.get("approved_transformations"), "approved_transformations"
    )
    expected_transformations = {
        "apply_complete_categorical_contract": True,
        "apply_four_hospital_unit_conversions": True,
        "generate_artificial_anchored_time": True,
        "artificial_time_source": "minutes_since_icu_admission",
        "artificial_time_target": "anchored_time_since_icu_admission",
        "artificial_time_anchor": "2020-01-01 00:00:00",
        "preserve_five_operational_provenance_fields": True,
        "preserve_row_and_hospital_order": True,
    }
    if transformations != expected_transformations:
        raise ConfigurationError("Approved harmonized transformations changed")
    streaming = _mapping(raw.get("streaming"), "streaming")
    output = _mapping(raw.get("outputs"), "outputs")
    boundary = _mapping(raw.get("boundary"), "boundary")
    if boundary != {
        "read_verified_candidate_artifacts": True,
        "modify_candidate_artifacts": False,
        "overwrite_outputs": False,
        "filter_rows": False,
        "filter_stays": False,
        "apply_cleaning": False,
        "apply_derivation": False,
        "publish": False,
        "update_release_pointer": False,
    }:
        raise ConfigurationError("Harmonized build boundary changed")
    frozen_directory = Path(
        required_string(inputs, "frozen_contract_directory", "inputs")
    )
    if frozen_directory.is_absolute() or ".." in frozen_directory.parts:
        raise ConfigurationError("Frozen contract directory is not contained")
    policy = HarmonizedBuildPolicy(
        version="0.1",
        candidate_run_id=required_string(inputs, "candidate_run_id", "inputs"),
        candidate_artifact_version=required_string(
            inputs, "candidate_artifact_version", "inputs"
        ),
        candidate_status=required_string(inputs, "candidate_status", "inputs"),
        frozen_contract_version=required_string(
            inputs, "frozen_contract_version", "inputs"
        ),
        frozen_contract_directory=frozen_directory,
        consolidated_decisions_path=resolve_path(
            required_string(decisions, "consolidated_decisions", "decision_sources"),
            source,
        ),
        consolidated_decisions_sha256=required_string(
            decisions, "consolidated_decisions_sha256", "decision_sources"
        ),
        categorical_decisions_path=resolve_path(
            required_string(decisions, "categorical_decisions", "decision_sources"),
            source,
        ),
        categorical_decisions_sha256=required_string(
            decisions, "categorical_decisions_sha256", "decision_sources"
        ),
        freeze_policy_path=resolve_path(
            required_string(decisions, "schema_dictionary_freeze", "decision_sources"),
            source,
        ),
        freeze_policy_sha256=required_string(
            decisions, "schema_dictionary_freeze_sha256", "decision_sources"
        ),
        hospitals=tuple(hospitals),
        expected_rows=expected_rows,
        time_source=str(transformations["artificial_time_source"]),
        time_target=str(transformations["artificial_time_target"]),
        time_anchor=str(transformations["artificial_time_anchor"]),
        rows_per_batch=_positive_int(
            streaming.get("rows_per_batch"), "streaming.rows_per_batch"
        ),
        compression=required_string(
            streaming, "parquet_compression", "streaming"
        ),
        output_directory_name=required_string(
            output, "directory_name", "outputs"
        ),
        artifact_version=required_string(output, "artifact_version", "outputs"),
        review_directory_name=required_string(
            output, "review_directory_name", "outputs"
        ),
        output_status=required_string(output, "status", "outputs"),
        source_path=source,
    )
    for decision_path, expected_hash in (
        (policy.consolidated_decisions_path, policy.consolidated_decisions_sha256),
        (policy.categorical_decisions_path, policy.categorical_decisions_sha256),
        (policy.freeze_policy_path, policy.freeze_policy_sha256),
    ):
        if sha256_file(decision_path) != expected_hash:
            raise ConfigurationError("An approved build decision source changed")
    return policy


def _read_json(path: Path, label: str) -> Any:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HarmonizationError(f"{label} cannot be read") from exc
    return value


def _write_json(path: Path, value: Any, mode: int) -> None:
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)
            stream.write("\n")
        temporary.replace(path)
        path.chmod(mode)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _arrow_type(value: str) -> pa.DataType:
    types = {
        "large_string": pa.large_string(),
        "double": pa.float64(),
        "bool": pa.bool_(),
        "int32": pa.int32(),
        "list<item: double>": pa.list_(pa.float64()),
        "list<item: large_string>": pa.list_(pa.large_string()),
        "fixed_size_list<item: bool>[2]": pa.list_(pa.bool_(), 2),
        "timestamp[ns]": pa.timestamp("ns"),
        "string": pa.string(),
        "int64": pa.int64(),
    }
    try:
        return types[value]
    except KeyError as exc:
        raise HarmonizationError(f"Unsupported frozen Arrow type: {value}") from exc


def _load_frozen_schemas(
    contract_dir: Path,
    freeze_policy: SchemaDictionaryFreezePolicy,
) -> tuple[dict[str, pa.Schema], dict[str, Any]]:
    manifest_path = contract_dir / "freeze_manifest.json"
    manifest = _read_json(manifest_path, "Frozen schema/dictionary manifest")
    if not isinstance(manifest, dict):
        raise HarmonizationError("Frozen schema/dictionary manifest is not an object")
    files = manifest.get("files")
    if (
        manifest.get("artifact") != "asic_v3_frozen_harmonized_schema_dictionary"
        or manifest.get("contract_version") != freeze_policy.contract_version
        or manifest.get("schema_frozen") is not True
        or manifest.get("dictionary_frozen") is not True
        or manifest.get("publication_ready") is not False
        or not isinstance(files, dict)
    ):
        raise HarmonizationError("Frozen schema/dictionary contract is invalid")
    for name, expected_hash in files.items():
        path = contract_dir / str(name)
        if not path.is_file() or sha256_file(path) != expected_hash:
            raise HarmonizationError("A frozen contract file changed")
    schemas: dict[str, pa.Schema] = {}
    for table in ("static", "dynamic"):
        rows = _read_json(contract_dir / f"{table}_schema.json", f"Frozen {table} schema")
        if not isinstance(rows, list):
            raise HarmonizationError("Frozen schema JSON must contain a list")
        fields = [
            pa.field(
                str(row["variable"]),
                _arrow_type(str(row["physical_type"])),
                nullable=True,
                metadata={
                    b"asic_v3_unit": str(row["unit"]).encode(),
                    b"asic_v3_analysis_eligibility": str(
                        row["analysis_eligibility"]
                    ).encode(),
                },
            )
            for row in rows
        ]
        fields.extend(
            pa.field(name, _arrow_type(physical_type), nullable=False)
            for name, physical_type in freeze_policy.provenance_fields
        )
        schemas[table] = pa.schema(
            fields,
            metadata={
                b"asic_v3_stage": b"harmonized_candidate",
                b"asic_v3_contract_version": freeze_policy.contract_version.encode(),
                b"publication_ready": b"false",
                b"cleaning_applied": b"false",
                b"derivation_applied": b"false",
            },
        )
    return schemas, manifest


def _candidate_token(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _categorical_array(
    source: pa.Array,
    rule: ReviewedCategoricalRule,
    hospital: str,
    target_type: pa.DataType,
    counters: Counter[str],
) -> pa.Array:
    encoded = pc.dictionary_encode(source)
    dictionary = encoded.dictionary.to_pylist()
    counts = {
        row["values"]: int(row["counts"])
        for row in pc.value_counts(encoded.indices).to_pylist()
    }
    output: list[Any] = []
    null_input_count = int(counts.get(None, 0))
    counters["input_null"] += null_input_count
    counters["output_null"] += null_input_count
    for index, value in enumerate(dictionary):
        count = int(counts.get(index, 0))
        resolved, transformed = resolve_reviewed_categorical_token(
            rule, hospital, _candidate_token(value)
        )
        if not resolved:
            counters["unresolved"] += count
            output.append(None)
            continue
        output.append(transformed)
        counters["output_null" if transformed is None else "output_non_null"] += count
    dictionary_array = pa.array(output, type=target_type)
    return pc.take(dictionary_array, encoded.indices)


def _anchored_time_array(source: pa.Array, anchor: str) -> pa.Array:
    try:
        anchor_dt = datetime.strptime(anchor, "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=timezone.utc
        )
    except ValueError as exc:
        raise HarmonizationError("Approved artificial time anchor is invalid") from exc
    anchor_ns = int(anchor_dt.timestamp()) * 1_000_000_000
    values: list[int | None] = []
    for value in source.to_pylist():
        if value is None:
            values.append(None)
            continue
        numeric = float(value)
        if not math.isfinite(numeric):
            raise HarmonizationError("Relative minutes contain a non-finite value")
        offset = numeric * 60_000_000_000
        rounded = round(offset)
        if abs(offset - rounded) > 0.25:
            raise HarmonizationError("Relative minutes cannot be represented exactly in ns")
        values.append(anchor_ns + rounded)
    return pa.array(values, type=pa.timestamp("ns"))


def _convert_numeric(
    source: pa.Array,
    conversion: ApprovedHospitalConversion,
) -> pa.Array:
    values = pc.cast(source, pa.float64(), safe=True)
    factor = pa.scalar(conversion.factor, pa.float64())
    if conversion.operation == "multiply":
        return pc.multiply(values, factor)
    if conversion.operation == "divide":
        return pc.divide(values, factor)
    raise HarmonizationError("Approved hospital conversion operation changed")


def _schema_digest(schema: pa.Schema) -> str:
    return hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()


def _markdown(payload: dict[str, Any]) -> str:
    return "\n".join(
        (
            "# ASIC v3 harmonized build review",
            "",
            f"- Generated (UTC): `{payload['generated_at_utc']}`",
            f"- Dataset context: `{payload['dataset_context']}`",
            f"- Build run: `{payload['run_id']}`",
            f"- Frozen contract: `{payload['frozen_contract_version']}`",
            "- Overall status: **PASS**",
            "- Blocking findings: `0`",
            "- Technical blocking findings: `0`",
            f"- Static rows: `{payload['row_counts']['static']}`",
            f"- Dynamic rows: `{payload['row_counts']['dynamic']}`",
            "- Cleaning applied: `false`",
            "- Derivation applied: `false`",
            "- Production clinical candidate artifacts generated: `true`",
            "- Publication ready: `false`",
            "",
            "## Transformation accounting",
            "",
            f"- Reviewed categorical rules: `{payload['metrics']['categorical_rule_count']}`",
            f"- Unresolved categorical cells: `{payload['metrics']['categorical_unresolved_count']}`",
            f"- Hospital conversion rules: `{payload['metrics']['conversion_rule_count']}`",
            f"- Generated artificial-time rows: `{payload['metrics']['anchored_time_row_count']}`",
            "",
            "## Human review gate",
            "",
            "The build is complete but non-publishable. Run the independent frozen-contract, conservation, and transformation audit before approving any release.",
            "",
        )
    )


def run_harmonized_build(
    config: HarmonizedBuildConfig,
    run_id: str | None = None,
    progress: Callable[[str], None] | None = None,
) -> HarmonizedBuildResult:
    policy = load_harmonized_build_policy(config.policy_path)
    if config.dataset_context != "production":
        raise HarmonizationError("The approved harmonized build is production-only")
    selected_run = run_id or default_run_id()
    if not RUN_ID_PATTERN.fullmatch(selected_run):
        raise HarmonizationError("Invalid harmonized build run ID")

    consolidated = load_reviewed_consolidated_decisions(
        policy.consolidated_decisions_path
    )
    categorical = load_reviewed_categorical_decisions(
        policy.categorical_decisions_path
    )
    freeze_policy = load_schema_dictionary_freeze_policy(policy.freeze_policy_path)
    if (
        consolidated.candidate_run_id != policy.candidate_run_id
        or categorical.candidate_run_id != policy.candidate_run_id
        or freeze_policy.source_review_run_id != "20260806T103811Z"
        or freeze_policy.contract_version != policy.frozen_contract_version
    ):
        raise HarmonizationError("Approved build decision lineage disagrees")

    frozen_dir = config.data_root / policy.frozen_contract_directory
    schemas, freeze_manifest = _load_frozen_schemas(frozen_dir, freeze_policy)
    if (
        freeze_manifest.get("lineage", {}).get("candidate_run_id")
        != policy.candidate_run_id
        or freeze_manifest.get("harmonized_build_implementation_authorized") is not True
    ):
        raise HarmonizationError("Frozen contract does not authorize this build")

    candidate_root = (
        config.data_root / "harmonization_candidates" / policy.candidate_run_id
    )
    candidate_manifest_path = candidate_root / "candidate_run_manifest.json"
    candidate_manifest = _read_json(candidate_manifest_path, "Candidate run manifest")
    if not isinstance(candidate_manifest, dict):
        raise HarmonizationError("Candidate run manifest is not an object")
    if (
        candidate_manifest.get("artifact") != "asic_v3_harmonization_candidate_run"
        or candidate_manifest.get("artifact_version") != policy.candidate_artifact_version
        or candidate_manifest.get("run_id") != policy.candidate_run_id
        or candidate_manifest.get("status") != policy.candidate_status
        or candidate_manifest.get("publication_ready") is not False
    ):
        raise HarmonizationError("Verified candidate input contract changed")
    hospital_rows = candidate_manifest.get("hospitals")
    if not isinstance(hospital_rows, list):
        raise HarmonizationError("Candidate hospital manifests are absent")
    hospitals = {
        str(row.get("hospital")): row for row in hospital_rows if isinstance(row, dict)
    }
    if tuple(hospitals) != policy.hospitals:
        raise HarmonizationError("Candidate hospital order or coverage changed")

    output_parent = config.data_root / policy.output_directory_name
    final_output = output_parent / selected_run
    staging_output = output_parent / f".{selected_run}.incomplete"
    review_dir = config.reports_root / "review" / policy.review_directory_name
    review_json = review_dir / f"{selected_run}.json"
    review_md = review_dir / f"{selected_run}.md"
    if any(
        path.exists()
        for path in (final_output, staging_output, review_json, review_md)
    ):
        raise HarmonizationError("Harmonized build run already exists")
    staging_output.mkdir(parents=True, mode=0o700)
    staging_output.chmod(0o700)

    conversion_by_key = {
        (item.hospital, item.table, item.variable): item
        for item in consolidated.conversions
    }
    category_counts: dict[tuple[str, str], Counter[str]] = {
        (rule.table, rule.variable): Counter() for rule in categorical.rules
    }
    conversion_counts: Counter[str] = Counter()
    row_counts: Counter[str] = Counter()
    hospital_counts: dict[str, dict[str, int]] = {
        hospital: {"static": 0, "dynamic": 0} for hospital in policy.hospitals
    }
    anchor_non_null = 0
    output_files: dict[str, dict[str, Any]] = {}

    for table in ("static", "dynamic"):
        output_path = staging_output / f"{table}.parquet"
        schema = schemas[table]
        with pq.ParquetWriter(
            output_path, schema, compression=policy.compression
        ) as writer:
            for hospital in policy.hospitals:
                summary = hospitals[hospital]
                table_summary = summary.get("outputs", {}).get(table, {})
                input_path = candidate_root / hospital / f"{table}.parquet"
                if (
                    not input_path.is_file()
                    or table_summary.get("sha256") != sha256_file(input_path)
                ):
                    raise HarmonizationError("A verified candidate table changed")
                parquet = pq.ParquetFile(input_path)
                expected_hospital_rows = int(table_summary.get("row_count", -1))
                if parquet.metadata.num_rows != expected_hospital_rows:
                    raise HarmonizationError("Candidate hospital row count changed")
                for batch in parquet.iter_batches(
                    batch_size=policy.rows_per_batch, use_threads=True
                ):
                    arrays: list[pa.Array] = []
                    for field in schema:
                        name = field.name
                        if name == policy.time_target:
                            source_index = batch.schema.get_field_index(policy.time_source)
                            if source_index < 0:
                                raise HarmonizationError(
                                    "Artificial-time source is absent from candidate"
                                )
                            transformed = _anchored_time_array(
                                batch.column(source_index), policy.time_anchor
                            )
                            anchor_non_null += len(transformed) - transformed.null_count
                        else:
                            source_index = batch.schema.get_field_index(name)
                            if source_index < 0:
                                raise HarmonizationError(
                                    f"Frozen field is absent from candidate: {table}.{name}"
                                )
                            source_array = batch.column(source_index)
                            rule = categorical.rule_for(table, name)
                            if (
                                rule is not None
                                and rule.parser
                                != "previously_reviewed_fixed_position_composite"
                            ):
                                transformed = _categorical_array(
                                    source_array,
                                    rule,
                                    hospital,
                                    field.type,
                                    category_counts[(table, name)],
                                )
                            else:
                                conversion = conversion_by_key.get(
                                    (hospital, table, name)
                                )
                                transformed = (
                                    _convert_numeric(source_array, conversion)
                                    if conversion is not None
                                    else pc.cast(source_array, field.type, safe=True)
                                )
                                if conversion is not None:
                                    conversion_counts[conversion.decision_id] += (
                                        len(transformed) - transformed.null_count
                                    )
                        if transformed.type != field.type:
                            transformed = pc.cast(transformed, field.type, safe=True)
                        arrays.append(transformed)
                    output_batch = pa.RecordBatch.from_arrays(arrays, schema=schema)
                    writer.write_batch(output_batch)
                    row_counts[table] += batch.num_rows
                    hospital_counts[hospital][table] += batch.num_rows
                    if (
                        progress is not None
                        and hospital_counts[hospital][table]
                        % (policy.rows_per_batch * 20)
                        == 0
                    ):
                        progress(
                            f"harmonized_build_progress hospital={hospital} "
                            f"table={table} rows={hospital_counts[hospital][table]}"
                        )
                if hospital_counts[hospital][table] != expected_hospital_rows:
                    raise HarmonizationError("Harmonized hospital rows were not conserved")
                if progress is not None:
                    progress(
                        f"harmonized_build_table_complete hospital={hospital} table={table}"
                    )
        output_path.chmod(0o600)
        if row_counts[table] != dict(policy.expected_rows)[table]:
            raise HarmonizationError("Harmonized pooled row count changed")
        output_files[table] = {
            "path": str(final_output / output_path.name),
            "sha256": sha256_file(output_path),
            "row_count": row_counts[table],
            "schema_sha256": _schema_digest(schema),
        }

    categorical_unresolved = sum(
        counts["unresolved"] for counts in category_counts.values()
    )
    if categorical_unresolved:
        unresolved_variables = ", ".join(
            f"{table}.{variable}={counts['unresolved']}"
            for (table, variable), counts in sorted(category_counts.items())
            if counts["unresolved"]
        )
        raise HarmonizationError(
            "Reviewed categorical contract left unresolved cells: "
            f"{unresolved_variables}"
        )
    for rule in categorical.rules:
        if rule.parser == "previously_reviewed_fixed_position_composite":
            continue
        counts = category_counts[(rule.table, rule.variable)]
        if (
            rule.expected_non_null_count is not None
            and counts["output_non_null"] != rule.expected_non_null_count
        ) or (
            rule.expected_null_count is not None
            and counts["output_null"] != rule.expected_null_count
        ):
            raise HarmonizationError("Reviewed categorical output counts changed")
    if set(conversion_counts) != {
        item.decision_id for item in consolidated.conversions
    }:
        raise HarmonizationError("An approved conversion had no applicable values")
    if anchor_non_null <= 0:
        raise HarmonizationError("Artificial anchored time was not generated")

    generated = utc_timestamp()
    manifest_path = staging_output / "harmonized_build_manifest.json"
    manifest = {
        "artifact": "asic_v3_harmonized_build_candidate",
        "artifact_version": policy.artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "run_id": selected_run,
        "status": policy.output_status,
        "lineage": {
            "candidate_run_id": policy.candidate_run_id,
            "candidate_manifest_sha256": sha256_file(candidate_manifest_path),
            "frozen_contract_version": policy.frozen_contract_version,
            "freeze_manifest_sha256": sha256_file(
                frozen_dir / "freeze_manifest.json"
            ),
            "decision_sha256": {
                "consolidated": policy.consolidated_decisions_sha256,
                "categorical": policy.categorical_decisions_sha256,
                "schema_dictionary_freeze": policy.freeze_policy_sha256,
            },
        },
        "outputs": output_files,
        "hospital_row_counts": hospital_counts,
        "transformation_accounting": {
            "categorical": {
                f"{table}.{variable}": dict(sorted(counts.items()))
                for (table, variable), counts in sorted(category_counts.items())
            },
            "hospital_conversions": dict(sorted(conversion_counts.items())),
            "anchored_time_non_null_count": anchor_non_null,
        },
        "row_filtering_applied": False,
        "stay_filtering_applied": False,
        "cleaning_applied": False,
        "derivation_applied": False,
        "publication_ready": False,
    }
    _write_json(manifest_path, manifest, 0o600)
    staging_output.replace(final_output)
    final_output.chmod(0o700)
    manifest_path = final_output / manifest_path.name

    review_payload = {
        "artifact": "asic_v3_harmonized_build_review",
        "artifact_version": policy.artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "run_id": selected_run,
        "frozen_contract_version": policy.frozen_contract_version,
        "overall_status": "pass",
        "blocking_findings": [],
        "technical_blocking_findings": [],
        "row_counts": dict(row_counts),
        "metrics": {
            "categorical_rule_count": len(categorical.rules),
            "categorical_unresolved_count": categorical_unresolved,
            "conversion_rule_count": len(conversion_counts),
            "anchored_time_row_count": anchor_non_null,
        },
        "production_data_artifacts_generated": True,
        "cleaning_applied": False,
        "derivation_applied": False,
        "publication_ready": False,
    }
    assert_review_payload_is_safe(review_payload)
    review_dir.mkdir(parents=True, mode=0o750, exist_ok=True)
    review_dir.chmod(0o750)
    _write_json(review_json, review_payload, 0o640)
    temporary_md = review_md.with_suffix(".md.tmp")
    descriptor = os.open(temporary_md, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o640)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(_markdown(review_payload))
    temporary_md.replace(review_md)
    review_md.chmod(0o640)
    return HarmonizedBuildResult(
        run_id=selected_run,
        output_directory=final_output,
        manifest_path=manifest_path,
        review_json_path=review_json,
        review_markdown_path=review_md,
    )
