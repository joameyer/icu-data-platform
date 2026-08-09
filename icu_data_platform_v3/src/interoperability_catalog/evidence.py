from __future__ import annotations

from collections import Counter, defaultdict
import csv
import gzip
import json
from pathlib import Path
from typing import Any, Iterator, TextIO

import pyarrow as pa
import pyarrow.csv as pacsv


EVENT_FILES = {
    "chartevents": ("icu/chartevents.csv.gz", "valueuom"),
    "labevents": ("hosp/labevents.csv.gz", "valueuom"),
    "inputevents": ("icu/inputevents.csv.gz", "rateuom"),
    "outputevents": ("icu/outputevents.csv.gz", "valueuom"),
    "procedureevents": ("icu/procedureevents.csv.gz", "valueuom"),
}
DICTIONARY_FILES = {
    "d_items": "icu/d_items.csv.gz",
    "d_labitems": "hosp/d_labitems.csv.gz",
}


def _open_text(path: Path) -> TextIO:
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8", newline="")
    return path.open("r", encoding="utf-8", newline="")


def _read_mappings(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _selected_items(mappings: list[dict[str, str]]) -> dict[str, set[int]]:
    selected: dict[str, set[int]] = defaultdict(set)
    for row in mappings:
        if row["dictionary_table"] != "not_applicable":
            selected[row["dictionary_table"]].add(int(row["item_id"]))
    return selected


def _audit_dictionaries(
    mimic_root: Path, mappings: list[dict[str, str]]
) -> tuple[dict[str, dict[int, dict[str, str]]], list[str]]:
    selected = _selected_items(mappings)
    found: dict[str, dict[int, dict[str, str]]] = {}
    findings: list[str] = []
    for dictionary, relative in DICTIONARY_FILES.items():
        required = selected.get(dictionary, set())
        observed: dict[int, dict[str, str]] = {}
        with _open_text(mimic_root / relative) as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                try:
                    item_id = int(row.get("itemid", ""))
                except ValueError:
                    continue
                if item_id in required:
                    observed[item_id] = {
                        "label": row.get("label", ""),
                        "category": row.get("category", ""),
                        "linksto": row.get("linksto", "labevents" if dictionary == "d_labitems" else ""),
                        "unitname": row.get("unitname", ""),
                        "fluid": row.get("fluid", ""),
                    }
        missing = sorted(required.difference(observed))
        if missing:
            findings.append(f"{dictionary}_missing_required_item_count={len(missing)}")
        found[dictionary] = observed
    return found, findings


def _scan_event_units(
    mimic_root: Path, mappings: list[dict[str, str]]
) -> tuple[dict[str, Counter[tuple[int, str]]], Counter[str], list[str]]:
    accepted: dict[str, dict[int, set[str]]] = defaultdict(lambda: defaultdict(set))
    for row in mappings:
        table = row["mimic_source_table"]
        if table in EVENT_FILES:
            accepted[table][int(row["item_id"])].add(row["accepted_source_unit"])
    counts: dict[str, Counter[tuple[int, str]]] = {}
    summary: Counter[str] = Counter()
    findings: list[str] = []
    for table, items in accepted.items():
        relative, unit_column = EVENT_FILES[table]
        item_unit_counts: Counter[tuple[int, str]] = Counter()
        try:
            reader = pacsv.open_csv(
                mimic_root / relative,
                read_options=pacsv.ReadOptions(block_size=32 * 1024 * 1024, use_threads=True),
                convert_options=pacsv.ConvertOptions(
                    include_columns=["itemid", unit_column],
                    column_types={"itemid": pa.int64(), unit_column: pa.string()},
                    strings_can_be_null=False,
                ),
            )
        except (pa.ArrowInvalid, KeyError):
            findings.append(f"{table}_missing_required_unit_column_count=1")
            counts[table] = item_unit_counts
            continue
        for batch in reader:
            item_values = batch.column("itemid").to_pylist()
            unit_values = batch.column(unit_column).to_pylist()
            for item_id, raw_unit in zip(item_values, unit_values, strict=True):
                if item_id is None or int(item_id) not in items:
                    continue
                item_id = int(item_id)
                unit = raw_unit or ""
                normalized_unit = unit if unit else "<missing>"
                item_unit_counts[(item_id, normalized_unit)] += 1
                summary[f"{table}.selected_observation_count"] += 1
                if unit in items[item_id]:
                    summary[f"{table}.accepted_unit_observation_count"] += 1
                else:
                    summary[f"{table}.unknown_or_unaccepted_unit_observation_count"] += 1
        counts[table] = item_unit_counts
    return counts, summary, findings


def audit_mapping_evidence(
    *, mimic_root: Path, mapping_csv: Path, scan_event_units: bool = True
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Read dictionaries and unit columns only; emit no patient or row examples."""

    mappings = _read_mappings(mapping_csv)
    dictionary_rows, findings = _audit_dictionaries(mimic_root, mappings)
    unit_counts: dict[str, Counter[tuple[int, str]]] = {}
    unit_summary: Counter[str] = Counter()
    if scan_event_units:
        unit_counts, unit_summary, unit_findings = _scan_event_units(mimic_root, mappings)
        findings.extend(unit_findings)
    selected = _selected_items(mappings)
    dictionary_summary = {
        dictionary: {
            "required_item_count": len(selected.get(dictionary, set())),
            "present_item_count": len(dictionary_rows.get(dictionary, {})),
            "missing_item_count": len(selected.get(dictionary, set()).difference(dictionary_rows.get(dictionary, {}))),
        }
        for dictionary in sorted(DICTIONARY_FILES)
    }
    private = {
        "artifact": "asic_mimic_alignment_cluster_private_aggregate_evidence",
        "artifact_version": "0.1",
        "privacy": "cluster_private_aggregate_item_and_unit_counts_no_patient_rows",
        "dictionary_summary": dictionary_summary,
        "dictionary_metadata": {
            dictionary: {str(item): metadata for item, metadata in sorted(rows.items())}
            for dictionary, rows in dictionary_rows.items()
        },
        "observed_item_unit_counts": {
            table: [
                {"item_id": item, "source_unit": unit, "observation_count": count}
                for (item, unit), count in sorted(counts.items())
            ]
            for table, counts in unit_counts.items()
        },
        "unit_summary": dict(sorted(unit_summary.items())),
        "mapping_row_count": len(mappings),
        "findings": sorted(findings),
        "production_source_modified": False,
        "profile_expanded": False,
        "release_created": False,
        "pointer_modified": False,
        "row_level_data_exported": False,
    }
    public = {
        "artifact": "asic_mimic_alignment_public_aggregate_summary",
        "artifact_version": "0.1",
        "privacy": "public_aggregate_counts_only",
        "dictionary_summary": dictionary_summary,
        "unit_summary": dict(sorted(unit_summary.items())),
        "mapping_row_count": len(mappings),
        "finding_count": len(findings),
        "production_source_modified": False,
        "profile_expanded": False,
        "release_created": False,
        "pointer_modified": False,
        "row_level_data_exported": False,
    }
    return private, public


def write_mapping_evidence(
    *,
    mimic_root: Path,
    mapping_csv: Path,
    private_output: Path,
    public_output: Path,
    scan_event_units: bool = True,
) -> None:
    private, public = audit_mapping_evidence(
        mimic_root=mimic_root,
        mapping_csv=mapping_csv,
        scan_event_units=scan_event_units,
    )
    for path, payload in ((private_output, private), (public_output, public)):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
