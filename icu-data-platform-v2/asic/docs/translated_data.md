# Translated data

**Contract status:** frozen and executable  
**Executable contract:** [`asic/config/translated/contract.yaml`](../config/translated/contract.yaml)  
**Contract version:** `1.3`

This document is the readable reference for the translated artifact accepted
as input to downstream ASIC processing. The executable YAML contract is the
source of truth for exact names, order, Arrow types, producer provenance, and
manifest requirements.

## Artifact boundary

```text
asic/data/<context>/translated/
├── static.parquet
├── dynamic.parquet
└── translation_manifest.json
```

The boundary is read-only. Auditing it may write an aggregate JSON report under
`asic/reports/<context>/`, but it never changes pooled, translated, cleaned, or
derived data.

## Accepted producer

Contract version `1.3` accepts translated artifacts with:

| Provenance item | Required value |
|---|---|
| Artifact | `asic_translated` |
| Pooled input contract | `1.2` |
| Translation policy | `1.3` |
| Compatible translation generator | `0.6.3` |
| Translation status | `pass` |
| Policy documentation | `asic/docs/pooled_to_translated.md` |

A future producer version is not accepted automatically. Its compatibility
must be reviewed and added explicitly or accompanied by a new translated
contract version.

## Frozen schemas

The static schema contains exactly 22 columns and the dynamic schema exactly
132 columns. Names, order, and Arrow types must match the executable contract;
missing, additional, reordered, or retyped fields are blocking failures.

Demo uses the production schema because it is an unchanged production subset.
Mock retains the documented physical differences created by its generator:

| Field | Production and demo | Mock |
|---|---|---|
| `stay_id_global` | `large_string` | `string` |
| `study_implementation_phase` | `int8` | `double` |
| `age_group` | `large_string` | `string` |
| `height_group` | `large_string` | `string` |
| `icd10_codes` | `large_string` | `string` |
| `ards_diagnosis_app` | `int8` | `double` |

Categorically translated fields such as `sex`, `weight_group`, `bmi_group`,
`discharge_status`, and `death_status` are `large_string` in every context.
`hospital_mortality_reported` is Boolean.

The dynamic schema separately represents the pooled source
`individuelles_Tidalvolumen_pro_kg_idealem_Koerpergewicht`:

- `vt_per_ideal_bw_total` contains the unchanged UK00 values and is missing at
  all other hospitals; and
- `vt_per_kg_ideal_body_weight` contains the unchanged non-UK00 values and is
  missing at UK00.

This is a lossless semantic split, not a calculation or unit correction. Its
exact row accounting and hospital-scope checks are required in the translation
manifest.

The complete ordered schema is intentionally maintained only once, in the
machine-readable contract. The readable names and semantic groups are
documented in [`pooled_to_translated.md`](pooled_to_translated.md).

## Identifier contract

Every static and dynamic row must contain mutually consistent identifiers:

- `stay_id_global` is the authoritative stay identifier and ends in `:<hid>`;
- `hospital_code_source` is integral and equals that suffix;
- `hospital_id` equals `asic_UKNN` for the same code; and
- the only accepted codes are `0`, `2`, `3`, `4`, `6`, `7`, and `8`.

One `stay_id_global` occurs exactly once in static. Static and dynamic stay
sets must match.

## Time contract

Dynamic rows retain both representations of time since ICU admission:

- `minutes_since_icu_admission`; and
- `anchored_time_since_icu_admission`.

The anchored value must equal `2020-01-01 00:00:00` plus the relative minutes
exactly. It remains an artificial timestamp, not a real admission or event
date. Each `(stay_id_global, minutes_since_icu_admission)` key must be unique.

## Manifest contract

The audit requires the manifest to match the configured context and physical
files. It blocks on:

- a producer, policy, source-contract, documentation, or context mismatch;
- any required translation check with the wrong value;
- source/output row-count disagreement;
- manifest row or column counts that differ from Parquet metadata;
- a translated output path that differs from the configured context path; or
- a non-passing translation status.

The exact required check names and values are declared under
`manifest.required_checks` in the executable contract.

## Reviewed artifacts used to freeze the schema

| Context | Static rows | Dynamic rows | Static columns | Dynamic columns | Translation status |
|---|---:|---:|---:|---:|---|
| Demo | 700 | 1,091,385 | 22 | 131 | Superseded by contract `1.3` |
| Production | 14,483 | 21,876,966 | 22 | 131 | Superseded by contract `1.3` |

These row counts describe the most recently reviewed artifacts and are not
permanent hard-coded cohort sizes. They predate the new dynamic field and must
be regenerated under policy `1.3` before they satisfy contract `1.3`. The audit
requires actual Parquet metadata, manifest output counts, and manifest
source/output preservation to agree.

## Downstream preservation rule

Every translated column must remain available in cleaned data. Contract
version `1.3` requires the future cleaning stage to:

- preserve all 22 static and 132 dynamic translated fields;
- preserve static and dynamic row count and order;
- preserve all identifier and time keys exactly; and
- change values only through named cleaning rules that have been explicitly
  approved.

The immutable translated artifact keeps the accepted source representation
available when a cleaned value is converted, masked, or flagged. Derived
clinical variables and time-blocked representations belong under `derived/`,
not in the cleaned layer.

The translated input contract does not itself authorize mortality
consolidation, unit correction, invalid-value masking, cohort filtering, or a
mechanical-ventilation-duration definition.

Contract `1.3` supersedes `1.2` after approving the lossless UK00 tidal-volume
semantic split described above. Contract `1.2` had already approved
`hosp_los = -1` as an explicit source-system missing sentinel. Producer
versions advance so artifacts created before either representation change
cannot be mistaken for current translated inputs.

## Audit procedure

After synchronizing and testing the repository, audit demo first:

```bash
./.venv/bin/python -m asic_pipeline audit-translated \
  --config asic/config/datasets/demo.yaml
```

Review the generated `translated_input_audit_*.json` under
`asic/reports/demo/`. Every blocking check must pass.

The production audit scans all dynamic identifiers and time keys and must run
through Slurm rather than on a login node:

```bash
mkdir -p asic/runs/production
sbatch asic/run_translated_input_audit_production.sh
```

Review the Slurm log and the new report under `asic/reports/production/`.
Passing this contract establishes structural and provenance eligibility only;
the subsequent clinical and cross-hospital diagnostics remain separate. Their
read-only procedure and complete preserved legacy policy are documented in
[`cross_hospital_data_quality.md`](cross_hospital_data_quality.md).
