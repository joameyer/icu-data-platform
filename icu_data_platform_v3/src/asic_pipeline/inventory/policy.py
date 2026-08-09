from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any

from asic_pipeline.config import load_yaml_mapping, required_string
from asic_pipeline.errors import ConfigurationError


@dataclass(frozen=True)
class CSVInventoryPolicy:
    encodings: tuple[str, ...]
    delimiters: tuple[str, ...]
    sample_records: int
    minimum_header_columns: int
    require_consistent_record_width: bool


@dataclass(frozen=True)
class DuplicateHeaderLayout:
    column_count: int
    zero_based_positions: tuple[int, ...]


@dataclass(frozen=True)
class ReviewedDuplicateHeader:
    rule_id: str
    hospital_folder: str
    raw_column_name: str
    expected_occurrences: int
    allowed_layouts: tuple[DuplicateHeaderLayout, ...]
    ingestion_action: str
    translation_action: str
    evidence: dict[str, Any]


@dataclass(frozen=True)
class ProvisionalArchiveExclusion:
    rule_id: str
    hospital_folder: str
    expected_size_bytes: int
    expected_sha256: str
    ingestion_action: str
    retained_role: str
    approval_status: str
    evidence: dict[str, Any]


@dataclass(frozen=True)
class HospitalMapping:
    source_folder: str
    canonical_hospital_id: str
    approval_status: str
    cohort_action: str
    evidence: dict[str, Any]


@dataclass(frozen=True)
class InventoryPolicy:
    policy_version: str
    status: str
    expected_hospital_folders: tuple[str, ...]
    blocking_unmapped_hospital_folders: tuple[str, ...]
    identity_investigation_folders: tuple[str, ...]
    hospital_mappings: tuple[HospitalMapping, ...]
    checkpoint_directory_name: str
    untrusted_pooled_directory_name: str
    known_root_metadata_files: tuple[str, ...]
    default_static_filename: str
    additional_static_candidate_filenames: tuple[str, ...]
    dynamic_filename_patterns: tuple[tuple[str, str], ...]
    candidate_identifier_columns: tuple[str, ...]
    candidate_hospital_identity_columns: tuple[str, ...]
    candidate_time_columns: tuple[str, ...]
    reviewed_duplicate_headers: tuple[ReviewedDuplicateHeader, ...]
    provisional_archive_exclusions: tuple[ProvisionalArchiveExclusion, ...]
    csv: CSVInventoryPolicy
    inventory_manifest_version: str
    source_path: Path

    def dynamic_pattern_for(self, folder: str) -> re.Pattern[str]:
        patterns = dict(self.dynamic_filename_patterns)
        return re.compile(patterns.get(folder, patterns["default"]))

    def hospital_mapping_for(self, folder: str) -> HospitalMapping | None:
        return next(
            (
                mapping
                for mapping in self.hospital_mappings
                if mapping.source_folder == folder
            ),
            None,
        )

    def reviewed_duplicate_header_for(
        self,
        hospital_folder: str | None,
        header: tuple[str, ...],
        duplicate_names: tuple[str, ...],
    ) -> ReviewedDuplicateHeader | None:
        if hospital_folder is None or len(duplicate_names) != 1:
            return None
        duplicate_name = duplicate_names[0]
        for rule in self.reviewed_duplicate_headers:
            if (
                rule.hospital_folder != hospital_folder
                or rule.raw_column_name != duplicate_name
            ):
                continue
            positions = tuple(
                index for index, name in enumerate(header) if name == duplicate_name
            )
            if len(positions) != rule.expected_occurrences:
                return None
            if any(
                layout.column_count == len(header)
                and layout.zero_based_positions == positions
                for layout in rule.allowed_layouts
            ):
                return rule
        return None

    def provisional_archive_exclusion_for(
        self,
        hospital_folder: str | None,
        size_bytes: int,
        sha256_value: str,
    ) -> ProvisionalArchiveExclusion | None:
        for rule in self.provisional_archive_exclusions:
            if (
                rule.hospital_folder == hospital_folder
                and rule.expected_size_bytes == size_bytes
                and rule.expected_sha256 == sha256_value
            ):
                return rule
        return None


def _string_list(raw: Any, location: str, *, allow_empty: bool = False) -> tuple[str, ...]:
    if not isinstance(raw, list) or (not raw and not allow_empty):
        qualifier = "a list" if allow_empty else "a non-empty list"
        raise ConfigurationError(f"{location} must be {qualifier}")
    if any(not isinstance(value, str) or not value for value in raw):
        raise ConfigurationError(f"{location} must contain only non-empty strings")
    values = tuple(raw)
    if len(values) != len(set(values)):
        raise ConfigurationError(f"{location} contains duplicates")
    return values


def _positive_int(raw: Any, location: str) -> int:
    if not isinstance(raw, int) or isinstance(raw, bool) or raw <= 0:
        raise ConfigurationError(f"{location} must be a positive integer")
    return raw


def _boolean(raw: Any, location: str) -> bool:
    if not isinstance(raw, bool):
        raise ConfigurationError(f"{location} must be a boolean")
    return raw


def _mapping(raw: Any, location: str) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ConfigurationError(f"{location} must be a mapping")
    return dict(raw)


def _parse_reviewed_anomalies(
    raw: Any,
    expected_hospitals: tuple[str, ...],
) -> tuple[
    tuple[ReviewedDuplicateHeader, ...],
    tuple[ProvisionalArchiveExclusion, ...],
]:
    anomalies = _mapping(raw, "policy.reviewed_anomalies")
    duplicate_raw = anomalies.get("duplicate_headers")
    archive_raw = anomalies.get("provisional_archive_exclusions")
    if not isinstance(duplicate_raw, list) or not isinstance(archive_raw, list):
        raise ConfigurationError(
            "Reviewed duplicate headers and provisional archive exclusions must be lists"
        )

    duplicate_rules: list[ReviewedDuplicateHeader] = []
    for index, value in enumerate(duplicate_raw):
        location = f"policy.reviewed_anomalies.duplicate_headers[{index}]"
        item = _mapping(value, location)
        if item.get("approval_status") != "approved":
            raise ConfigurationError(f"{location} is not approved")
        hospital = required_string(item, "hospital_folder", location)
        if hospital not in expected_hospitals:
            raise ConfigurationError(f"{location} has an unexpected hospital folder")
        occurrences = _positive_int(
            item.get("expected_occurrences"), f"{location}.expected_occurrences"
        )
        layouts_raw = item.get("allowed_layouts")
        if not isinstance(layouts_raw, list) or not layouts_raw:
            raise ConfigurationError(f"{location}.allowed_layouts must be non-empty")
        layouts: list[DuplicateHeaderLayout] = []
        for layout_index, layout_value in enumerate(layouts_raw):
            layout_location = f"{location}.allowed_layouts[{layout_index}]"
            layout = _mapping(layout_value, layout_location)
            column_count = _positive_int(
                layout.get("column_count"), f"{layout_location}.column_count"
            )
            positions_raw = layout.get("one_based_positions")
            if (
                not isinstance(positions_raw, list)
                or len(positions_raw) != occurrences
                or any(
                    not isinstance(position, int)
                    or isinstance(position, bool)
                    or position <= 0
                    or position > column_count
                    for position in positions_raw
                )
                or len(set(positions_raw)) != len(positions_raw)
                or positions_raw != sorted(positions_raw)
            ):
                raise ConfigurationError(
                    f"{layout_location}.one_based_positions is invalid"
                )
            layouts.append(
                DuplicateHeaderLayout(
                    column_count=column_count,
                    zero_based_positions=tuple(position - 1 for position in positions_raw),
                )
            )
        ingestion_action = required_string(item, "ingestion_action", location)
        translation_action = required_string(item, "translation_action", location)
        if ingestion_action != "preserve_as_separate_positional_source_columns":
            raise ConfigurationError(f"{location} must preserve both raw occurrences")
        if translation_action != "drop_second_only_if_all_empty_revalidated":
            raise ConfigurationError(
                f"{location} must revalidate the all-empty drop precondition"
            )
        duplicate_rules.append(
            ReviewedDuplicateHeader(
                rule_id=required_string(item, "rule_id", location),
                hospital_folder=hospital,
                raw_column_name=required_string(item, "raw_column_name", location),
                expected_occurrences=occurrences,
                allowed_layouts=tuple(layouts),
                ingestion_action=ingestion_action,
                translation_action=translation_action,
                evidence=_mapping(item.get("evidence"), f"{location}.evidence"),
            )
        )

    archive_rules: list[ProvisionalArchiveExclusion] = []
    for index, value in enumerate(archive_raw):
        location = (
            f"policy.reviewed_anomalies.provisional_archive_exclusions[{index}]"
        )
        item = _mapping(value, location)
        status = required_string(item, "approval_status", location)
        if status != "provisional_pending_owner_confirmation":
            raise ConfigurationError(
                f"{location} must remain pending owner confirmation"
            )
        hospital = required_string(item, "hospital_folder", location)
        if hospital not in expected_hospitals:
            raise ConfigurationError(f"{location} has an unexpected hospital folder")
        digest = required_string(item, "expected_sha256", location).casefold()
        if not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ConfigurationError(f"{location}.expected_sha256 is invalid")
        ingestion_action = required_string(item, "ingestion_action", location)
        retained_role = required_string(item, "retained_role", location)
        if ingestion_action != "exclude":
            raise ConfigurationError(f"{location} must be excluded from ingestion")
        if retained_role != "untrusted_read_only_comparison":
            raise ConfigurationError(f"{location} must remain an untrusted comparison")
        archive_rules.append(
            ProvisionalArchiveExclusion(
                rule_id=required_string(item, "rule_id", location),
                hospital_folder=hospital,
                expected_size_bytes=_positive_int(
                    item.get("expected_size_bytes"),
                    f"{location}.expected_size_bytes",
                ),
                expected_sha256=digest,
                ingestion_action=ingestion_action,
                retained_role=retained_role,
                approval_status=status,
                evidence=_mapping(item.get("evidence"), f"{location}.evidence"),
            )
        )

    duplicate_rule_ids = [rule.rule_id for rule in duplicate_rules]
    archive_rule_ids = [rule.rule_id for rule in archive_rules]
    if len(duplicate_rule_ids) != len(set(duplicate_rule_ids)) or len(
        archive_rule_ids
    ) != len(set(archive_rule_ids)):
        raise ConfigurationError("Reviewed anomaly rule IDs must be unique")
    return tuple(duplicate_rules), tuple(archive_rules)


def _parse_hospital_mappings(
    raw: Any,
    expected_hospitals: tuple[str, ...],
) -> tuple[HospitalMapping, ...]:
    mappings = _mapping(raw, "policy.hospital_mappings")
    if set(mappings) != set(expected_hospitals):
        raise ConfigurationError(
            "policy.hospital_mappings keys must exactly equal expected_hospital_folders"
        )
    parsed: list[HospitalMapping] = []
    allowed_statuses = {"reviewed_existing_contract", "approved_by_data_owner"}
    for folder in expected_hospitals:
        location = f"policy.hospital_mappings.{folder}"
        item = _mapping(mappings[folder], location)
        canonical = required_string(item, "canonical_hospital_id", location)
        expected_canonical = f"asic_UK{folder}"
        if canonical != expected_canonical:
            raise ConfigurationError(
                f"{location}.canonical_hospital_id must be {expected_canonical!r}"
            )
        status = required_string(item, "approval_status", location)
        if status not in allowed_statuses:
            raise ConfigurationError(
                f"{location}.approval_status is not an approved status"
            )
        cohort_action = required_string(item, "cohort_action", location)
        if cohort_action != "include":
            raise ConfigurationError(
                f"{location}.cohort_action must be 'include' for the frozen contract"
            )
        evidence_raw = item.get("evidence", {})
        evidence = _mapping(evidence_raw, f"{location}.evidence")
        if status == "approved_by_data_owner" and (
            evidence.get("identity_confirmed") is not True
            or evidence.get("cohort_inclusion_confirmed") is not True
        ):
            raise ConfigurationError(
                f"{location} requires identity and cohort-inclusion confirmation"
            )
        parsed.append(
            HospitalMapping(
                source_folder=folder,
                canonical_hospital_id=canonical,
                approval_status=status,
                cohort_action=cohort_action,
                evidence=evidence,
            )
        )
    return tuple(parsed)


def _parse_patterns(raw: Any) -> tuple[tuple[str, str], ...]:
    if not isinstance(raw, dict) or "default" not in raw:
        raise ConfigurationError(
            "policy.dynamic_filename_patterns must be a mapping with a default"
        )
    parsed: list[tuple[str, str]] = []
    for scope, pattern in raw.items():
        if not isinstance(scope, str) or not isinstance(pattern, str) or not pattern:
            raise ConfigurationError(
                "policy.dynamic_filename_patterns must map strings to regex strings"
            )
        try:
            compiled = re.compile(pattern)
        except re.error as exc:
            raise ConfigurationError(
                f"Invalid dynamic filename regex for {scope!r}: {exc}"
            ) from exc
        if "stay_id" not in compiled.groupindex:
            raise ConfigurationError(
                f"Dynamic filename regex for {scope!r} must define group 'stay_id'"
            )
        parsed.append((scope, pattern))
    return tuple(parsed)


def load_inventory_policy(path: str | Path) -> InventoryPolicy:
    policy_path = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(policy_path, "Inventory policy")
    expected = _string_list(
        raw.get("expected_hospital_folders"),
        "policy.expected_hospital_folders",
    )
    if any(not re.fullmatch(r"\d{2}", folder) for folder in expected):
        raise ConfigurationError(
            "policy.expected_hospital_folders must contain two-digit folder names"
        )
    blocking = _string_list(
        raw.get("blocking_unmapped_hospital_folders"),
        "policy.blocking_unmapped_hospital_folders",
        allow_empty=True,
    )
    if not set(blocking).issubset(expected):
        raise ConfigurationError(
            "Blocking unmapped folders must be part of expected_hospital_folders"
        )
    investigation_folders = _string_list(
        raw.get("identity_investigation_folders"),
        "policy.identity_investigation_folders",
        allow_empty=True,
    )
    if not set(investigation_folders).issubset(expected):
        raise ConfigurationError(
            "Identity investigation folders must be expected hospital folders"
        )
    hospital_mappings = _parse_hospital_mappings(
        raw.get("hospital_mappings"), expected
    )
    if blocking:
        raise ConfigurationError(
            "All expected hospital mappings are now approved; blocking_unmapped_hospital_folders must be empty"
        )

    excluded = raw.get("excluded_directories")
    if not isinstance(excluded, dict):
        raise ConfigurationError("policy.excluded_directories must be a mapping")
    checkpoint = required_string(excluded, "checkpoint", "policy.excluded_directories")
    if checkpoint != ".ipynb_checkpoints":
        raise ConfigurationError(
            "The checkpoint exclusion must be exactly '.ipynb_checkpoints'"
        )
    pooled = required_string(
        excluded,
        "untrusted_pooled",
        "policy.excluded_directories",
    )

    csv_raw = raw.get("csv")
    if not isinstance(csv_raw, dict):
        raise ConfigurationError("policy.csv must be a mapping")
    delimiters = tuple(
        "\t" if delimiter == "\\t" else delimiter
        for delimiter in _string_list(csv_raw.get("delimiters"), "policy.csv.delimiters")
    )
    if any(len(delimiter) != 1 for delimiter in delimiters):
        raise ConfigurationError("Every configured delimiter must be one character")

    reporting = raw.get("reporting")
    if not isinstance(reporting, dict):
        raise ConfigurationError("policy.reporting must be a mapping")
    if reporting.get("include_protected_values_in_review_report") is not False:
        raise ConfigurationError(
            "Protected values must be disabled in the review report"
        )
    if reporting.get("private_directory_mode") != "0700":
        raise ConfigurationError("Private report directory mode must be 0700")
    if reporting.get("private_file_mode") != "0600":
        raise ConfigurationError("Private report file mode must be 0600")

    duplicate_rules, archive_rules = _parse_reviewed_anomalies(
        raw.get("reviewed_anomalies"), expected
    )

    policy = InventoryPolicy(
        policy_version=required_string(raw, "policy_version", "policy"),
        status=required_string(raw, "status", "policy"),
        expected_hospital_folders=expected,
        blocking_unmapped_hospital_folders=blocking,
        identity_investigation_folders=investigation_folders,
        hospital_mappings=hospital_mappings,
        checkpoint_directory_name=checkpoint,
        untrusted_pooled_directory_name=pooled,
        known_root_metadata_files=_string_list(
            raw.get("known_root_metadata_files"),
            "policy.known_root_metadata_files",
        ),
        default_static_filename=required_string(
            raw,
            "default_static_filename",
            "policy",
        ),
        additional_static_candidate_filenames=_string_list(
            raw.get("additional_static_candidate_filenames"),
            "policy.additional_static_candidate_filenames",
            allow_empty=True,
        ),
        dynamic_filename_patterns=_parse_patterns(
            raw.get("dynamic_filename_patterns")
        ),
        candidate_identifier_columns=_string_list(
            raw.get("candidate_identifier_columns"),
            "policy.candidate_identifier_columns",
        ),
        candidate_hospital_identity_columns=_string_list(
            raw.get("candidate_hospital_identity_columns"),
            "policy.candidate_hospital_identity_columns",
        ),
        candidate_time_columns=_string_list(
            raw.get("candidate_time_columns"),
            "policy.candidate_time_columns",
        ),
        reviewed_duplicate_headers=duplicate_rules,
        provisional_archive_exclusions=archive_rules,
        csv=CSVInventoryPolicy(
            encodings=_string_list(csv_raw.get("encodings"), "policy.csv.encodings"),
            delimiters=delimiters,
            sample_records=_positive_int(
                csv_raw.get("sample_records"),
                "policy.csv.sample_records",
            ),
            minimum_header_columns=_positive_int(
                csv_raw.get("minimum_header_columns"),
                "policy.csv.minimum_header_columns",
            ),
            require_consistent_record_width=(
                _boolean(
                    csv_raw.get("require_consistent_record_width"),
                    "policy.csv.require_consistent_record_width",
                )
            ),
        ),
        inventory_manifest_version=required_string(
            reporting,
            "inventory_manifest_version",
            "policy.reporting",
        ),
        source_path=policy_path,
    )
    if policy.status != "approved_for_read_only_inventory":
        raise ConfigurationError(
            f"Inventory policy status is not executable: {policy.status!r}"
        )
    return policy
