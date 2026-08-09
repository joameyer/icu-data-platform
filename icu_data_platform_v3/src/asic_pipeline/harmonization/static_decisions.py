from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any

from asic_pipeline.config import load_yaml_mapping, required_string
from asic_pipeline.errors import ConfigurationError, HarmonizationError
from asic_pipeline.harmonization.registry import HarmonizationRule
from asic_pipeline.inventory.hashing import sha256_file


RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,79}$")
RULE_ID = re.compile(r"^[A-Z][A-Z0-9-]*$")
HOSPITAL_ID = re.compile(r"^asic_UK\d{2}$")


@dataclass(frozen=True)
class ReviewedStaticBooleanDecision:
    rule_id: str
    hospital: str
    table: str
    raw_name: str
    occurrence: int
    target: str
    evidence_nonempty_count: int
    normalization: str
    value_mapping: tuple[tuple[str, bool], ...]
    output_value_type: str
    output_unit: str

    def normalized_value(self, raw_value: str) -> str:
        if self.normalization != "strip_lower":
            raise HarmonizationError(
                f"Reviewed static rule {self.rule_id} has unsupported normalization"
            )
        return raw_value.strip().casefold()

    def mapped_value(self, raw_value: str) -> bool | None:
        return dict(self.value_mapping).get(self.normalized_value(raw_value))


@dataclass(frozen=True)
class ReviewedStaticNumericDecision:
    rule_id: str
    hospital: str
    table: str
    raw_name: str
    occurrence: int
    target: str
    evidence_nonempty_count: int
    evidence_direct_numeric_count: int
    evidence_custom_parsed_count: int
    evidence_approved_missing_count: int
    approved_parser: str
    approved_missing_tokens: tuple[str, ...]
    output_value_type: str
    output_unit: str
    evidence_run_id: str

    def as_harmonization_rule(self) -> HarmonizationRule:
        return HarmonizationRule(
            rule_id=self.rule_id,
            hospital=self.hospital,
            table=self.table,
            raw_name=self.raw_name,
            occurrence=self.occurrence,
            target=self.target,
            kind="numeric",
            approved_parser=(
                self.approved_parser
                if self.approved_parser != "direct_numeric"
                else None
            ),
            approved_list_missing_tokens=(),
            allowed_tokens=(),
            approved_missing_sentinel_tokens=self.approved_missing_tokens,
            canonical_value_type=self.output_value_type,
            expected_unit=self.output_unit,
            preserve_list_order=False,
            preserve_duplicates=False,
            evidence_nonempty_count=self.evidence_nonempty_count,
            evidence_run_id=self.evidence_run_id,
        )


@dataclass(frozen=True)
class ReviewedStayIdentifierDecision:
    rule_id: str
    table: str
    source_raw_name_variants: tuple[str, ...]
    source_occurrence: int
    local_target: str
    global_target: str
    output_value_type: str
    separator: str
    hospital_suffixes: tuple[tuple[str, str], ...]
    static_row_count: int
    nonempty_local_id_count: int
    within_hospital_duplicate_count: int
    cross_hospital_local_overlap_count: int
    hospital_static_row_counts: tuple[tuple[str, int], ...]
    evidence_run_id: str

    def suffix_for(self, hospital: str) -> str:
        suffix = dict(self.hospital_suffixes).get(hospital)
        if suffix is None:
            raise HarmonizationError(
                f"Identifier rule {self.rule_id} has no approved hospital suffix"
            )
        return suffix


@dataclass(frozen=True)
class ReviewedStaticDecisionRegistry:
    version: str
    approved_by_role: str
    approved_at: str
    static_contract_audit_run_id: str
    registry_review_run_id: str
    composite_audit_run_id: str
    decisions: tuple[
        ReviewedStaticBooleanDecision | ReviewedStaticNumericDecision, ...
    ]
    identifier_decision: ReviewedStayIdentifierDecision | None
    allow_synthetic_batch_transform: bool
    allow_production_reads: bool
    allow_artifact_writes: bool
    allow_hospital_concatenation: bool
    allow_union_schema_freeze: bool
    source_path: Path

    def rule_for(
        self,
        hospital: str,
        table: str,
        raw_name: str,
        occurrence: int = 1,
    ) -> ReviewedStaticBooleanDecision | ReviewedStaticNumericDecision | None:
        return next(
            (
                rule
                for rule in self.decisions
                if rule.hospital == hospital
                and rule.table == table
                and rule.raw_name == raw_name
                and rule.occurrence == occurrence
            ),
            None,
        )

    def require_production_ready(self) -> None:
        if not all(
            (
                self.allow_production_reads,
                self.allow_artifact_writes,
                self.allow_hospital_concatenation,
                self.allow_union_schema_freeze,
            )
        ):
            raise HarmonizationError(
                "Production harmonization remains blocked by the partial static decision registry"
            )


def _mapping(value: Any, location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigurationError(f"{location} must be a YAML mapping")
    return dict(value)


def _load_reviewed_static_decision_registry_v01(
    path: str | Path,
) -> ReviewedStaticDecisionRegistry:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Reviewed static decision registry")
    if raw.get("decision_registry_version") != "0.1":
        raise ConfigurationError("Static decision registry version must be 0.1")
    if raw.get("status") != "approved_partial_static_raw_v3_decisions":
        raise ConfigurationError("Static decision registry is not approved")
    review = _mapping(raw.get("review"), "static decisions.review")
    approved_at = required_string(review, "approved_at", "static decisions.review")
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", approved_at):
        raise ConfigurationError("Static decision approval date must be YYYY-MM-DD")
    run_ids = {
        key: required_string(review, key, "static decisions.review")
        for key in (
            "static_contract_audit_run_id",
            "registry_review_run_id",
            "composite_audit_run_id",
        )
    }
    if any(not RUN_ID.fullmatch(value) for value in run_ids.values()):
        raise ConfigurationError("Static decision evidence run ID is invalid")
    if run_ids != {
        "static_contract_audit_run_id": "20260805T112746Z",
        "registry_review_run_id": "20260805T104417Z",
        "composite_audit_run_id": "20260805T111026Z",
    }:
        raise ConfigurationError("Static decision evidence basis changed unexpectedly")
    raw_decisions = raw.get("decisions")
    if not isinstance(raw_decisions, list) or len(raw_decisions) != 1:
        raise ConfigurationError("Exactly one reviewed static decision is expected")
    item = _mapping(raw_decisions[0], "static decisions.decisions[0]")
    rule_id = required_string(item, "rule_id", "static decision")
    hospital = required_string(item, "hospital", "static decision")
    occurrence = item.get("occurrence")
    evidence_count = item.get("evidence_nonempty_count")
    unknown_count = item.get("evidence_unknown_nonempty_count")
    if not RULE_ID.fullmatch(rule_id) or not HOSPITAL_ID.fullmatch(hospital):
        raise ConfigurationError("Static Boolean decision identity is invalid")
    if not isinstance(occurrence, int) or isinstance(occurrence, bool) or occurrence <= 0:
        raise ConfigurationError("Static Boolean source occurrence is invalid")
    if (
        not isinstance(evidence_count, int)
        or isinstance(evidence_count, bool)
        or evidence_count < 0
        or not isinstance(unknown_count, int)
        or isinstance(unknown_count, bool)
        or unknown_count != 0
        or item.get("evidence_domain_complete") is not True
    ):
        raise ConfigurationError("Static Boolean evidence accounting is invalid")
    value_mapping = _mapping(item.get("value_mapping"), "static decision.value_mapping")
    expected_mapping = {"false": False, "true": True}
    if value_mapping != expected_mapping:
        raise ConfigurationError("Static mortality mapping must remain false/true Boolean")
    expected_fixed = {
        "hospital": "asic_UK00",
        "table": "static",
        "raw_name": "KH-Sterblichkeit",
        "occurrence": 1,
        "target": "hospital_mortality_reported",
        "kind": "categorical",
        "evidence_nonempty_count": 3676,
        "evidence_domain_complete": True,
        "evidence_unknown_nonempty_count": 0,
        "normalization": "strip_lower",
        "output_value_type": "bool",
        "output_unit": "not_applicable",
        "literal_empty_handling": "null",
        "source_absence_handling": "null",
        "unknown_nonempty_handling": "block",
    }
    observed_fixed = {key: item.get(key) for key in expected_fixed}
    if observed_fixed != expected_fixed:
        raise ConfigurationError("Reviewed static mortality decision changed unexpectedly")
    scope = _mapping(raw.get("scope"), "static decisions.scope")
    expected_scope = {
        "allow_synthetic_batch_transform": True,
        "allow_production_reads": False,
        "allow_artifact_writes": False,
        "allow_hospital_concatenation": False,
        "allow_union_schema_freeze": False,
    }
    if scope != expected_scope:
        raise ConfigurationError("Static decision registry must remain fail-closed")
    decision = ReviewedStaticBooleanDecision(
        rule_id=rule_id,
        hospital=hospital,
        table="static",
        raw_name="KH-Sterblichkeit",
        occurrence=occurrence,
        target="hospital_mortality_reported",
        evidence_nonempty_count=evidence_count,
        normalization="strip_lower",
        value_mapping=tuple(expected_mapping.items()),
        output_value_type="bool",
        output_unit="not_applicable",
    )
    return ReviewedStaticDecisionRegistry(
        version="0.1",
        approved_by_role=required_string(
            review, "approved_by_role", "static decisions.review"
        ),
        approved_at=approved_at,
        static_contract_audit_run_id=run_ids["static_contract_audit_run_id"],
        registry_review_run_id=run_ids["registry_review_run_id"],
        composite_audit_run_id=run_ids["composite_audit_run_id"],
        decisions=(decision,),
        identifier_decision=None,
        allow_synthetic_batch_transform=True,
        allow_production_reads=False,
        allow_artifact_writes=False,
        allow_hospital_concatenation=False,
        allow_union_schema_freeze=False,
        source_path=source,
    )


def _string_list(value: Any, location: str, *, allow_empty: bool) -> tuple[str, ...]:
    if not isinstance(value, list) or any(
        not isinstance(item, str) or not item for item in value
    ):
        raise ConfigurationError(f"{location} must be a list of non-empty strings")
    result = tuple(value)
    if not allow_empty and not result:
        raise ConfigurationError(f"{location} must not be empty")
    if len(result) != len(set(result)):
        raise ConfigurationError(f"{location} contains duplicates")
    return result


def _load_reviewed_static_decision_registry_v02(
    path: str | Path,
) -> ReviewedStaticDecisionRegistry:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Reviewed static decision registry")
    if raw.get("decision_registry_version") != "0.2":
        raise ConfigurationError("Static decision registry version must be 0.2")
    if raw.get("status") != "approved_partial_static_raw_v3_decisions":
        raise ConfigurationError("Static decision registry is not approved")
    if raw.get("supersedes") != "reviewed_static_decisions_0_1.yaml":
        raise ConfigurationError("Static decision registry predecessor is invalid")

    review = _mapping(raw.get("review"), "static decisions.review")
    approved_at = required_string(review, "approved_at", "static decisions.review")
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", approved_at):
        raise ConfigurationError("Static decision approval date must be YYYY-MM-DD")
    run_ids = {
        key: required_string(review, key, "static decisions.review")
        for key in (
            "static_contract_audit_run_id",
            "registry_review_run_id",
            "composite_audit_run_id",
        )
    }
    expected_run_ids = {
        "static_contract_audit_run_id": "20260805T112746Z",
        "registry_review_run_id": "20260805T104417Z",
        "composite_audit_run_id": "20260805T111026Z",
    }
    if run_ids != expected_run_ids or any(
        not RUN_ID.fullmatch(value) for value in run_ids.values()
    ):
        raise ConfigurationError("Static decision evidence basis changed unexpectedly")

    raw_decisions = raw.get("decisions")
    if not isinstance(raw_decisions, list) or len(raw_decisions) != 3:
        raise ConfigurationError("Static decision registry 0.2 requires three value rules")

    mortality = _mapping(raw_decisions[0], "static decisions.decisions[0]")
    mortality_expected = {
        "rule_id": "HARM-STATIC-HOSP-MORT-UK00-001",
        "hospital": "asic_UK00",
        "table": "static",
        "raw_name": "KH-Sterblichkeit",
        "occurrence": 1,
        "target": "hospital_mortality_reported",
        "kind": "boolean",
        "evidence_nonempty_count": 3676,
        "evidence_direct_numeric_count": 0,
        "evidence_custom_parsed_count": 0,
        "evidence_approved_missing_count": 0,
        "evidence_unknown_nonempty_count": 0,
        "evidence_domain_complete": True,
        "normalization": "strip_lower",
        "output_value_type": "bool",
        "output_unit": "not_applicable",
        "literal_empty_handling": "null",
        "source_absence_handling": "null",
        "unknown_nonempty_handling": "block",
    }
    if {key: mortality.get(key) for key in mortality_expected} != mortality_expected:
        raise ConfigurationError("Reviewed static mortality decision changed unexpectedly")
    mortality_mapping = _mapping(
        mortality.get("value_mapping"), "static mortality.value_mapping"
    )
    if mortality_mapping != {"false": False, "true": True}:
        raise ConfigurationError("Static mortality mapping must remain Boolean")
    mortality_rule = ReviewedStaticBooleanDecision(
        rule_id=mortality_expected["rule_id"],
        hospital=mortality_expected["hospital"],
        table="static",
        raw_name=mortality_expected["raw_name"],
        occurrence=1,
        target=mortality_expected["target"],
        evidence_nonempty_count=3676,
        normalization="strip_lower",
        value_mapping=(("false", False), ("true", True)),
        output_value_type="bool",
        output_unit="not_applicable",
    )

    numeric_specs = (
        {
            "rule_id": "HARM-STATIC-WEIGHT-UK00-001",
            "raw_name": "weightKg",
            "target": "weight_kg",
            "direct": 3614,
            "custom": 62,
            "missing": 0,
            "parser": "decimal_comma",
            "missing_tokens": (),
            "unit": "kg",
        },
        {
            "rule_id": "HARM-STATIC-HOSP-LOS-UK00-001",
            "raw_name": "Liegedauer_KH",
            "target": "hosp_los",
            "direct": 0,
            "custom": 0,
            "missing": 3676,
            "parser": "approved_missing_only",
            "missing_tokens": ("nan",),
            "unit": "day",
        },
    )
    numeric_rules: list[ReviewedStaticNumericDecision] = []
    for offset, spec in enumerate(numeric_specs, start=1):
        location = f"static decisions.decisions[{offset}]"
        item = _mapping(raw_decisions[offset], location)
        expected = {
            "rule_id": spec["rule_id"],
            "hospital": "asic_UK00",
            "table": "static",
            "raw_name": spec["raw_name"],
            "occurrence": 1,
            "target": spec["target"],
            "kind": "numeric",
            "evidence_nonempty_count": 3676,
            "evidence_direct_numeric_count": spec["direct"],
            "evidence_custom_parsed_count": spec["custom"],
            "evidence_approved_missing_count": spec["missing"],
            "evidence_unknown_nonempty_count": 0,
            "approved_parser": spec["parser"],
            "output_value_type": "float64",
            "output_unit": spec["unit"],
            "literal_empty_handling": "null",
            "source_absence_handling": "null",
            "unknown_nonempty_handling": "block",
        }
        if {key: item.get(key) for key in expected} != expected:
            raise ConfigurationError(f"{location} changed unexpectedly")
        missing_tokens = _string_list(
            item.get("approved_missing_tokens"),
            f"{location}.approved_missing_tokens",
            allow_empty=True,
        )
        if missing_tokens != spec["missing_tokens"]:
            raise ConfigurationError(f"{location} missing-token policy changed")
        if 3676 != spec["direct"] + spec["custom"] + spec["missing"]:
            raise ConfigurationError(f"{location} evidence accounting is invalid")
        numeric_rules.append(
            ReviewedStaticNumericDecision(
                rule_id=str(spec["rule_id"]),
                hospital="asic_UK00",
                table="static",
                raw_name=str(spec["raw_name"]),
                occurrence=1,
                target=str(spec["target"]),
                evidence_nonempty_count=3676,
                evidence_direct_numeric_count=int(spec["direct"]),
                evidence_custom_parsed_count=int(spec["custom"]),
                evidence_approved_missing_count=int(spec["missing"]),
                approved_parser=str(spec["parser"]),
                approved_missing_tokens=missing_tokens,
                output_value_type="float64",
                output_unit=str(spec["unit"]),
                evidence_run_id=run_ids["static_contract_audit_run_id"],
            )
        )

    identifier_raw = _mapping(
        raw.get("identifier_contract"), "static decisions.identifier_contract"
    )
    identifier_expected = {
        "rule_id": "HARM-STATIC-STAY-ID-001",
        "table": "static",
        "source_occurrence": 1,
        "local_target": "stay_id_local",
        "global_target": "stay_id_global",
        "output_value_type": "large_string",
        "separator": ":",
        "local_value_policy": "preserve_exact_nonempty_without_separator",
        "empty_or_missing_handling": "block",
        "local_separator_collision_handling": "block",
        "within_hospital_duplicate_handling": "block",
    }
    if {key: identifier_raw.get(key) for key in identifier_expected} != identifier_expected:
        raise ConfigurationError("Reviewed static identifier contract changed unexpectedly")
    source_names = _string_list(
        identifier_raw.get("source_raw_name_variants"),
        "static identifier.source_raw_name_variants",
        allow_empty=False,
    )
    if source_names != ("Pseudo-ID", "PseudoID"):
        raise ConfigurationError("Static identifier raw-name variants changed")
    suffixes_raw = _mapping(
        identifier_raw.get("hospital_suffixes"), "static identifier.hospital_suffixes"
    )
    expected_suffixes = {
        "asic_UK00": "0",
        "asic_UK01": "1",
        "asic_UK02": "2",
        "asic_UK03": "3",
        "asic_UK04": "4",
        "asic_UK06": "6",
        "asic_UK07": "7",
        "asic_UK08": "8",
    }
    if suffixes_raw != expected_suffixes or len(set(suffixes_raw.values())) != 8:
        raise ConfigurationError("Static identifier hospital suffix mapping changed")
    evidence = _mapping(identifier_raw.get("evidence"), "static identifier.evidence")
    hospital_counts_raw = _mapping(
        evidence.get("hospital_static_row_counts"),
        "static identifier.evidence.hospital_static_row_counts",
    )
    expected_hospital_counts = {
        "asic_UK00": 3676,
        "asic_UK01": 1571,
        "asic_UK02": 902,
        "asic_UK03": 1360,
        "asic_UK04": 486,
        "asic_UK06": 678,
        "asic_UK07": 2217,
        "asic_UK08": 5164,
    }
    if hospital_counts_raw != expected_hospital_counts:
        raise ConfigurationError("Static identifier hospital row evidence changed")
    evidence_expected = {
        "static_row_count": 16054,
        "nonempty_local_id_count": 16054,
        "within_hospital_duplicate_count": 0,
        "cross_hospital_local_overlap_count": 3676,
        "ingestion_identifier_agreement": True,
    }
    if {key: evidence.get(key) for key in evidence_expected} != evidence_expected:
        raise ConfigurationError("Static identifier evidence changed unexpectedly")
    if sum(expected_hospital_counts.values()) != 16054:
        raise ConfigurationError("Static identifier row accounting is invalid")
    identifier_rule = ReviewedStayIdentifierDecision(
        rule_id="HARM-STATIC-STAY-ID-001",
        table="static",
        source_raw_name_variants=source_names,
        source_occurrence=1,
        local_target="stay_id_local",
        global_target="stay_id_global",
        output_value_type="large_string",
        separator=":",
        hospital_suffixes=tuple(expected_suffixes.items()),
        static_row_count=16054,
        nonempty_local_id_count=16054,
        within_hospital_duplicate_count=0,
        cross_hospital_local_overlap_count=3676,
        hospital_static_row_counts=tuple(expected_hospital_counts.items()),
        evidence_run_id=run_ids["static_contract_audit_run_id"],
    )

    scope = _mapping(raw.get("scope"), "static decisions.scope")
    expected_scope = {
        "allow_synthetic_batch_transform": True,
        "allow_production_reads": False,
        "allow_artifact_writes": False,
        "allow_hospital_concatenation": False,
        "allow_union_schema_freeze": False,
    }
    if scope != expected_scope:
        raise ConfigurationError("Static decision registry must remain fail-closed")
    return ReviewedStaticDecisionRegistry(
        version="0.2",
        approved_by_role=required_string(
            review, "approved_by_role", "static decisions.review"
        ),
        approved_at=approved_at,
        static_contract_audit_run_id=run_ids["static_contract_audit_run_id"],
        registry_review_run_id=run_ids["registry_review_run_id"],
        composite_audit_run_id=run_ids["composite_audit_run_id"],
        decisions=(mortality_rule, *numeric_rules),
        identifier_decision=identifier_rule,
        allow_synthetic_batch_transform=True,
        allow_production_reads=False,
        allow_artifact_writes=False,
        allow_hospital_concatenation=False,
        allow_union_schema_freeze=False,
        source_path=source,
    )


def _v03_numeric_specs() -> tuple[dict[str, Any], ...]:
    specs: list[dict[str, Any]] = []
    icu_counts = {
        "asic_UK00": 3676,
        "asic_UK01": 0,
        "asic_UK02": 902,
        "asic_UK03": 1360,
        "asic_UK04": 486,
        "asic_UK06": 678,
        "asic_UK07": 2217,
        "asic_UK08": 5164,
    }
    for hospital, count in icu_counts.items():
        specs.append(
            {
                "rule_id": f"HARM-STATIC-ICU-LOS-{hospital[-4:]}-001",
                "hospital": hospital,
                "raw_name": "Liegedauer_ICU",
                "target": "icu_los",
                "nonempty": count,
                "direct": count,
                "missing": 0,
                "parser": "direct_numeric" if count else "approved_missing_only",
                "missing_tokens": (),
            }
        )
    for prefix, target, raw_name in (
        (
            "DFD",
            "dialysis_free_days",
            "Dialyse_(dialysefreie_Tage)",
        ),
        ("VFD", "vent_free_days", "Beatmungsfreie_Tage"),
    ):
        for hospital, count in (
            ("asic_UK01", 0),
            ("asic_UK02", 902),
            ("asic_UK06", 678),
            ("asic_UK07", 2217),
            ("asic_UK08", 5164),
        ):
            sentinel_only = hospital == "asic_UK08"
            specs.append(
                {
                    "rule_id": f"HARM-STATIC-{prefix}-{hospital[-4:]}-001",
                    "hospital": hospital,
                    "raw_name": (
                        f"{raw_name}\u00a0"
                        if prefix == "DFD" and hospital == "asic_UK01"
                        else raw_name
                    ),
                    "target": target,
                    "nonempty": count,
                    "direct": 0 if sentinel_only else count,
                    "missing": count if sentinel_only else 0,
                    "parser": (
                        "approved_missing_only"
                        if sentinel_only or count == 0
                        else "direct_numeric"
                    ),
                    "missing_tokens": ("-1", "-1.0") if sentinel_only else (),
                }
            )
    return tuple(specs)


def _load_reviewed_static_decision_registry_v03(
    path: str | Path,
) -> ReviewedStaticDecisionRegistry:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Reviewed static decision registry")
    if raw.get("decision_registry_version") != "0.3":
        raise ConfigurationError("Static decision registry version must be 0.3")
    if raw.get("status") != "approved_partial_static_raw_v3_decisions":
        raise ConfigurationError("Static decision registry is not approved")
    if raw.get("extends") != "reviewed_static_decisions_0_2.yaml":
        raise ConfigurationError("Static decision registry predecessor is invalid")
    base_path = source.parent / "reviewed_static_decisions_0_2.yaml"
    expected_base_hash = required_string(
        raw, "extends_sha256", "static decisions"
    )
    if (
        expected_base_hash
        != "fbe08f1a997c5264dbe44d225b81bd66bd8436190fc4259af9ec2d50e0d3977f"
        or not base_path.is_file()
        or sha256_file(base_path) != expected_base_hash
    ):
        raise ConfigurationError("Static decision registry predecessor hash changed")
    base = _load_reviewed_static_decision_registry_v02(base_path)
    review = _mapping(raw.get("review"), "static decisions.review")
    review_expected = {
        "approved_by_role": "project_data_owner",
        "approved_at": "2026-08-05",
        "static_contract_audit_run_id": "20260805T112746Z",
        "static_completion_review_generated_at_utc": (
            "2026-08-05T12:52:14+00:00"
        ),
    }
    if {key: review.get(key) for key in review_expected} != review_expected:
        raise ConfigurationError("Static decision 0.3 review evidence changed")
    common = _mapping(raw.get("common_contract"), "static decisions.common_contract")
    expected_common = {
        "table": "static",
        "occurrence": 1,
        "kind": "numeric",
        "evidence_custom_parsed_count": 0,
        "evidence_unknown_nonempty_count": 0,
        "output_value_type": "float64",
        "output_unit": "day",
        "literal_empty_handling": "null",
        "source_absence_handling": "null",
        "unknown_nonempty_handling": "block",
    }
    if common != expected_common:
        raise ConfigurationError("Static decision 0.3 common contract changed")
    raw_decisions = raw.get("new_numeric_decisions")
    specs = _v03_numeric_specs()
    if not isinstance(raw_decisions, list) or len(raw_decisions) != len(specs):
        raise ConfigurationError("Static decision registry 0.3 requires 18 new rules")
    rules: list[ReviewedStaticNumericDecision] = []
    for index, (value, spec) in enumerate(zip(raw_decisions, specs, strict=True)):
        location = f"static decisions.new_numeric_decisions[{index}]"
        item = _mapping(value, location)
        expected = {
            "rule_id": spec["rule_id"],
            "hospital": spec["hospital"],
            "raw_name": spec["raw_name"],
            "target": spec["target"],
            "evidence_nonempty_count": spec["nonempty"],
            "evidence_direct_numeric_count": spec["direct"],
            "evidence_approved_missing_count": spec["missing"],
            "approved_parser": spec["parser"],
        }
        if {key: item.get(key) for key in expected} != expected:
            raise ConfigurationError(f"{location} changed unexpectedly")
        missing_tokens = _string_list(
            item.get("approved_missing_tokens"),
            f"{location}.approved_missing_tokens",
            allow_empty=True,
        )
        if missing_tokens != spec["missing_tokens"]:
            raise ConfigurationError(f"{location} missing-token policy changed")
        if spec["nonempty"] != spec["direct"] + spec["missing"]:
            raise ConfigurationError(f"{location} evidence accounting is invalid")
        rules.append(
            ReviewedStaticNumericDecision(
                rule_id=str(spec["rule_id"]),
                hospital=str(spec["hospital"]),
                table="static",
                raw_name=str(spec["raw_name"]),
                occurrence=1,
                target=str(spec["target"]),
                evidence_nonempty_count=int(spec["nonempty"]),
                evidence_direct_numeric_count=int(spec["direct"]),
                evidence_custom_parsed_count=0,
                evidence_approved_missing_count=int(spec["missing"]),
                approved_parser=str(spec["parser"]),
                approved_missing_tokens=missing_tokens,
                output_value_type="float64",
                output_unit="day",
                evidence_run_id="20260805T112746Z",
            )
        )
    scopes = [
        (rule.hospital, rule.table, rule.raw_name, rule.occurrence)
        for rule in (*base.decisions, *rules)
    ]
    if len(scopes) != len(set(scopes)):
        raise ConfigurationError("Static decision 0.3 source scopes collide")
    scope = _mapping(raw.get("scope"), "static decisions.scope")
    expected_scope = {
        "allow_synthetic_batch_transform": True,
        "allow_production_reads": False,
        "allow_artifact_writes": False,
        "allow_hospital_concatenation": False,
        "allow_union_schema_freeze": False,
    }
    if scope != expected_scope:
        raise ConfigurationError("Static decision registry must remain fail-closed")
    return ReviewedStaticDecisionRegistry(
        version="0.3",
        approved_by_role="project_data_owner",
        approved_at="2026-08-05",
        static_contract_audit_run_id="20260805T112746Z",
        registry_review_run_id=base.registry_review_run_id,
        composite_audit_run_id=base.composite_audit_run_id,
        decisions=(*base.decisions, *rules),
        identifier_decision=base.identifier_decision,
        allow_synthetic_batch_transform=True,
        allow_production_reads=False,
        allow_artifact_writes=False,
        allow_hospital_concatenation=False,
        allow_union_schema_freeze=False,
        source_path=source,
    )


def load_reviewed_static_decision_registry(
    path: str | Path,
) -> ReviewedStaticDecisionRegistry:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Reviewed static decision registry")
    version = raw.get("decision_registry_version")
    if version == "0.1":
        return _load_reviewed_static_decision_registry_v01(source)
    if version == "0.2":
        return _load_reviewed_static_decision_registry_v02(source)
    if version == "0.3":
        return _load_reviewed_static_decision_registry_v03(source)
    raise ConfigurationError("Static decision registry version must be 0.1, 0.2, or 0.3")
