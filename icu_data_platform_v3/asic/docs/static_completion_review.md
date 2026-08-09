# Remaining-static contract review

## Purpose

This is a read-only decision-support step for the four variables still pending
from static-contract audit run `20260805T112746Z`:

- `icu_los`;
- `dialysis_free_days`;
- `vent_free_days`; and
- `icd10_codes`.

It does not rescan raw CSV or ingested Parquet, transform values, approve a
registry, or generate harmonized data.

## Evidence boundary

`review-remaining-static-contract` reads only the immutable owner-only
`occurrences.parquet`, `variables.parquet`, and manifest from the named
static-contract audit plus its sanitized JSON. It verifies their artifact
versions, dataset context, row counts, accounting, and aggregate agreement. It
also validates that partial static registry `0.2` remains bound to the same
audit and keeps all production gates closed.

Exact raw headers and token examples are never copied. The new sanitized report
contains only canonical targets, hospital IDs, counts, and decision proposals.

## Proposals requiring human approval

- `icu_los`: nullable `float64`, unit day, direct numeric parsing, with literal
  empty cells and source absence represented as null.
- `dialysis_free_days`: nullable `float64`, unit day, direct numeric parsing
  plus already reviewed hospital-scoped missing sentinels.
- `vent_free_days`: nullable `float64`, unit day, direct numeric parsing plus
  already reviewed hospital-scoped missing sentinels.
- `icd10_codes`: nullable `large_string`, no unit, preserving each non-empty
  source cell exactly as one string. No trimming, splitting, deduplication,
  case normalization, code validation, or categorical translation is proposed.

Unresolved numeric tokens remain blocking. The report itself approves none of
these proposals.

The data owner subsequently approved the three numeric proposals from the
production review generated at `2026-08-05T12:52:14+00:00`. They are encoded in
partial registry `0.3`. The ICD-10 string proposal was not approved as the
canonical representation; it proceeds to a dedicated notation audit instead.

## Outputs and exit status

The command writes only:

```text
asic/reports/<context>/review/static_completion_review/<run_id>.json
asic/reports/<context>/review/static_completion_review/<run_id>.md
```

Exit status `2` is the expected human-review outcome when technical checks
pass. The Slurm wrapper converts it to a successful job. Exit status `1`
indicates a technical blocker and must not be bypassed.
