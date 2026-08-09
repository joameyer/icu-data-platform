# Ordered-schema and variable-dictionary freeze review

## Purpose

This is the final report-only contract review before a new harmonized build can
be implemented. It combines:

- reviewed consolidated unit, conversion, semantic, and cleaning-boundary
  decisions `0.1`;
- reviewed complete categorical decisions `0.1`;
- immutable candidate dictionary and list-shape evidence from consolidated
  audit `20260806T092818Z`; and
- immutable complete categorical evidence from review `20260806T100728Z`.

The command verifies every decision-source and report hash. It does not read
candidate clinical rows, rescan the 24 million dynamic records, or modify the
existing candidate.

## Outputs

The sanitized Markdown contains the complete ordered static and dynamic
clinical schemas with physical types, units, and analysis-eligibility status.
Five operational provenance fields are appended to each table and listed
separately.

The proposed dynamic schema also restores the reviewed artificial anchored-time
representation as `anchored_time_since_icu_admission`. Raw v3 contains elapsed
`minutes_since_icu_admission`, but no raw `timeidx` occurrence. The output is
therefore explicitly generated as `2020-01-01 00:00:00` plus the elapsed
minutes; it is never represented as a source-backed or real calendar time.

Approved hospital conversions determine the common output unit even where the
unit was not separately repeated in the named-unit map. In particular, UK03
`hematocrit` and UK00 `lymph_pct` have common harmonized unit `percent` after
their reviewed multiplication by 100.

The owner-only bundle contains:

- `proposed_variable_dictionary.parquet`, including definitions, source
  availability, caveats, all-missing flags, and provenance strategy; and
- `categorical_contract_validation.parquet`, proving that every observed domain
  token and count resolves under the approved categorical contract.

An unresolved unit or definition is retained explicitly and makes the affected
variable analysis-ineligible. It does not trigger a guessed conversion and does
not silently remove the field.

`vt_per_kg` is absent from the raw-v3 occurrence registry and is therefore not
fabricated as an all-null clinical column. The distinct source-backed
`vt_per_kg_ideal_body_weight` remains in the schema. This does not authorize a
merge between those concepts.

## Cluster invocation

This is a small report-only frontend command; no Slurm job is needed:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
unset SLURM_JOB_ID SLURM_STEP_ID

.venv/bin/asic-pipeline review-schema-dictionary \
  --config asic/config/datasets/production.yaml
```

Exit status `2` is expected because the ordered schemas and dictionary still
require explicit human freeze approval. Exit status `1` is a technical failure
and must not be bypassed.

## Boundary

This review does not freeze a contract automatically. It creates no harmonized,
cleaned, derived, or published data. After explicit approval, a separately
implemented build must create a new harmonized artifact from ingested data; the
existing candidate remains immutable.
