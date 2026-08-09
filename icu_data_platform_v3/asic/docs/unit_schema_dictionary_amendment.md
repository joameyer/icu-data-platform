# Harmonized schema and dictionary 0.2 amendment review

## Purpose

This report-only checkpoint converts the approved unit and semantic-split
decisions into one complete ordered schema and variable-dictionary proposal.
It reads frozen contract `0.1`, its small dictionary artifacts, and the
immutable unit-decision audit manifest. It reads no clinical row.

## Proposed contract shape

- Static clinical schema: 23 variables, unchanged and in the same order.
- Existing dynamic schema: the first 130 variables remain in their existing
  order and positions.
- Formerly unresolved units: all 72 receive the reviewed team-derived unit and
  become eligible independently of current availability.
- Semantic splits: nine `float64` fields are appended at dynamic positions
  131–139.
- Six new representations have reviewed units and are eligible.
- Three hospital-scoped source representations retain unit
  `unresolved_source_scale` and remain analysis-ineligible pending definition.
- Total proposed clinical variables: 162.
- All 34 medication or therapy variables carry explicit dictionary metadata:
  observed zero is preserved, null is not assumed to be zero or inactive,
  null-to-zero and zero-to-null conversion are prohibited, and carry-forward
  requires a separate reviewed contract.
- Those 34 variables also carry a nonnegative domain and the reviewed general
  cleaned-layer rule `mask_negative_medication_or_therapy_value`. Negative
  values remain visible in harmonized data and are masked only in cleaning.

Every dictionary row carries contract and unit-provenance fields. The mandatory
provenance statement remains that ASIC hospitals supplied no complete unit
dictionary and these units are team-derived assumptions.

## Outputs

The sanitized report is written below
`asic/reports/production/review/unit_schema_dictionary_amendment/`. The
owner-only bundle contains:

- `proposed_static_schema.json`;
- `proposed_dynamic_schema.json`;
- `proposed_variable_dictionary.parquet`; and
- `schema_amendment_manifest.json`.

No patient row, identifier, raw token, or clinical Parquet value is read or
written.

## Human gate

Technical completion intentionally remains `pending_human_review`. Freezing
contract `0.2` requires explicit approval of the complete ordered schemas,
dictionary, and medication zero/missing evidence. This review does not
authorize harmonized, cleaned, or derived
builds and does not activate the separately pending 12-variable cleaning-range
policy.

The data owner approved the exact revised review generated at
`2026-08-07T10:14:15+00:00`; its immutable run ID is `20260807T101321Z`.
The subsequent immutable metadata freeze is documented in
[Frozen harmonized schema and variable dictionary 0.2](unit_schema_dictionary_freeze_0_2.md).
