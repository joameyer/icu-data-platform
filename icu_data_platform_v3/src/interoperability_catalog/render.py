from __future__ import annotations

from collections import defaultdict
import csv
from pathlib import Path
from typing import Any

import yaml


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")


def render_alignment_table(project_root: Path) -> str:
    concepts = _read_csv(project_root / "interoperability/config/clinical_concepts_0_1.csv")
    mappings = _read_csv(project_root / "interoperability/config/mimic_source_mappings_0_1.csv")
    conversions: dict[str, dict[str, Any]] = yaml.safe_load(
        (project_root / "interoperability/config/unit_conversions_0_1.yaml").read_text(encoding="utf-8")
    )["conversions"]
    by_concept: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in mappings:
        by_concept[row["canonical_concept_id"]].append(row)
    lines = [
        "# Generated ASIC--MIMIC alignment table 0.1",
        "",
        "Generated from the CSV/YAML contract. This view contains no clinical rows or identifier values. Unmapped ASIC fields are deliberately retained.",
        "",
        "| ASIC name | MIMIC name and item IDs | Clinical description | ASIC unit | MIMIC source units | MIMIC-to-canonical conversion | Inverse where meaningful | Overlap status | Caveat |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for concept in concepts:
        rows = by_concept.get(concept["canonical_concept_id"], [])
        names = "<br>".join(
            f"{row['mimic_source_table']}.{row['dictionary_label']} [{row['item_id']}]"
            for row in rows
        ) or "—"
        units = "<br>".join(sorted({row["accepted_source_unit"] for row in rows})) or "—"
        conversion_ids = sorted({row["conversion_id"] for row in rows})
        formulas = "<br>".join(
            f"{conversion_id}: {conversions[conversion_id]['exact_source_to_canonical_formula']}"
            for conversion_id in conversion_ids
        ) or "—"
        inverses = "<br>".join(
            f"{conversion_id}: {conversions[conversion_id]['inverse_formula']}"
            for conversion_id in conversion_ids
            if conversions[conversion_id].get("round_trip_support")
        ) or "—"
        mapping_caveats = [row["semantic_caveat"] for row in rows if row["semantic_caveat"]]
        caveat = concept["remaining_semantic_gate"]
        if mapping_caveats:
            caveat += " " + " ".join(dict.fromkeys(mapping_caveats))
        cells = [
            concept["asic_field_id"], names, concept["clinical_definition"], concept["asic_unit"],
            units, formulas, inverses, concept["mimic_overlap_disposition"], caveat,
        ]
        lines.append("| " + " | ".join(_cell(cell) for cell in cells) + " |")
    return "\n".join(lines) + "\n"


def write_alignment_table(project_root: Path) -> Path:
    output = project_root / "interoperability/docs/generated_alignment_table_0_1.md"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(render_alignment_table(project_root), encoding="utf-8")
    return output
