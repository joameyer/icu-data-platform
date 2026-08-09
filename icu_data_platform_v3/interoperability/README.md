# Platform interoperability contracts

This directory owns reusable, analysis-independent clinical-variable alignment
contracts. It belongs to `icu_data_platform_v3`; neither the ASIC nor MIMIC
pipeline imports an analysis repository at runtime.

## Artifact ownership

| Artifact | Owner | Change rule |
|---|---|---|
| `config/clinical_concepts_0_1.csv` | platform clinical-data contract | Every ASIC core-derived field occurrence is explicit; disposition/unit changes require clinical and data-owner review. |
| `config/mimic_source_mappings_0_1.csv` | MIMIC source steward plus clinical reviewer | One row per source table/item/unit combination. New rows do not activate extraction. |
| `config/unit_conversions_0_1.yaml` | clinical unit reviewer | Exact source-unit spelling, formula, context, test examples, and review state are mandatory. |
| `config/independent_draft_manifest_0_1.json` | provenance record | Immutable evidence that the authoritative draft was hashed before legacy inspection. |
| `config/legacy_variable_configuration_comparison_0_1.csv` | regression evidence | Historical comparison only; never an executable input. |
| `docs/generated_alignment_table_0_1.md` | generated view | Regenerate from CSV/YAML; do not edit by hand. |
| `src/interoperability_catalog/` | platform engineering | Validation, rendering, and bounded aggregate evidence only. |

The downloaded ASIC Parquet/schema metadata under
`asic/data/local_metadata/harmonized_schema_dictionary/0.2/` is local review
input and remains ignored by source control. Committed CSV/YAML contracts carry
its frozen hashes and release lineage; production use does not require those
local files.

## Boundaries

- The catalog can describe more overlap than a current analysis uses.
- Catalog membership never adds an item to the approved MIMIC extraction
  profile. Profile changes require a separate reviewed MIMIC contract.
- Original MIMIC tables remain immutable.
- Unit conversion belongs in the MIMIC canonical-event layer before
  canonical-unit plausibility and before blocking.
- Unknown, absent, ambiguous, or incompatible units remain ineligible.
- Review outputs contain no patient rows or identifier values.

See [the complete contract](docs/asic_mimic_alignment_0_1.md) and the
[generated field-by-field view](docs/generated_alignment_table_0_1.md).
