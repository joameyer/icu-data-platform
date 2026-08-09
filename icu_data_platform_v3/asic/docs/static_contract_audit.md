# Static-variable contract evidence audit

## Purpose

This is the first bounded variable-review batch before production
harmonization. It collects decision evidence for:

- `stay_id_global`;
- `weight_kg`;
- `hosp_los`;
- `icu_los`;
- `dialysis_free_days`;
- `vent_free_days`;
- `hospital_mortality_reported`; and
- `icd10_codes`.

It does not approve these variables and does not create harmonized data.

## Evidence boundary

The command is bound to registry-review run `20260805T104417Z` and passing
composite-audit run `20260805T111026Z`. It reuses the immutable registry
workbooks and post-ingestion identifier check, re-hashes each selected ingested
static Parquet, validates raw-field metadata, and streams only selected source
columns.

For numeric variables it counts direct numeric tokens, approved scoped missing
sentinels, and unresolved non-empty tokens without converting them. For the
mortality candidate it counts false, true, and non-binary values. For ICD-10 it
records bounded owner-only examples. Identifier values are used transiently to
count within-source duplicates and cross-hospital raw overlaps but are never
written to either report bundle.

Every occurrence must reproduce the prior schema/token counts:

```text
row count = source-null count + literal-empty count + non-empty count
```

Any input-hash, field-binding, row-accounting, or prior-identifier-audit failure
is technical and stops review.

## Outputs

Owner-only evidence:

```text
asic/reports/<context>/private/static_contract_audit/<run_id>/
├── static_contract_audit_manifest.json
├── occurrences.parquet
├── variables.parquet
├── token_examples.parquet
└── README.md
```

The private token workbook deliberately contains no stay identifiers.

Sanitized review output:

```text
asic/reports/<context>/review/static_contract_audit/<run_id>.json
asic/reports/<context>/review/static_contract_audit/<run_id>.md
```

The sanitized report contains canonical names and aggregated counts only. It
contains no raw headers, source filenames, raw tokens, or identifiers.

## Human-review gate

Exit status `2` is expected when the technical checks pass because mappings,
types, units, numeric and binary policies, identifier construction, and
free-text preservation still require approval. The Slurm wrapper converts that
expected review state into a successful job. Exit status `1` indicates a
technical blocker and must not be bypassed.

Review the three private Parquet workbooks only on the authorized cluster and
return the sanitized Markdown report plus any specifically requested aggregate
evidence. Do not copy private workbooks or raw values off the cluster.

## First approved decision from this evidence batch

The project data owner approved the following decisions from audit run
`20260805T112746Z`:

- UK00 `hospital_mortality_reported` as nullable Boolean;
- UK00 `weight_kg` as nullable `float64` kg with decimal-comma parsing;
- UK00 `hosp_los` as nullable `float64` days, with exact token `nan` mapped to
  missing; and
- raw `Pseudo-ID`/`PseudoID` as `stay_id_local`, with `stay_id_global` derived
  by appending the approved numeric hospital suffix after `:`.

Unknown non-empty clinical tokens remain blocking. Invalid, missing,
separator-ambiguous, or duplicate-within-hospital static identifiers also
remain blocking. These approvals do not approve the other variables in the
audit batch and do not authorize production harmonization.
