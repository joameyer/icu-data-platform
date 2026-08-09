# Candidate unit-decision audit

## Purpose

This is the single production-scale evidence checkpoint for
`candidate_unit_decisions_0_2.yaml`. It tests all nine proposed
hospital-specific unit conversions and all nine proposed unrecoverable
hospital-variable masks against immutable cleaned release
`20260806T114234Z`.

Corrected audit run `20260807T083117Z` passed technically. After reviewing its
complete aggregate workbooks, the data owner approved the nine conversions
but superseded every proposed mask with a value-preserving semantic split. The
reviewed outcome is recorded in `reviewed_unit_decisions_0_2.yaml`.

The audit is deliberately report-only. It does not update the harmonized,
cleaned, or derived releases; does not activate contract `0.2`; and does not
persist converted or masked clinical values.

## Evidence computed

The job scans the 24,069,379 dynamic rows in bounded Arrow batches. For the 16
variables touched by a conversion or mask, it builds all 128 hospital-variable
profiles. Each profile contains:

- exact row, finite, null, NaN, infinity, zero, negative, and `-1` counts;
- exact minimum, maximum, mean, and standard deviation;
- deterministic finite-aware bounded estimates of Q1, Q5, Q25, median, Q75,
  Q95, and Q99; and
- the quantile sample count and an explicit `quantiles_are_exact` flag.

Sampling operates on finite values rather than arbitrary row positions. A
sparse profile therefore includes every finite value when it fits within the
100,000-value capacity. Positive-value profiles are emitted separately for
every scope so medication zeros cannot conceal a unit-scale discrepancy.

The audit also builds one unaffected-hospital peer profile for each target
variable. Hospitals proposed for conversion or masking are excluded from that
variable's peer pool. For every conversion it compares the target distribution
before and after the proposed operation with this peer evidence. These
comparisons are review evidence, not automatic proof of a source unit.

For UK00 vasopressin, the audit performs exact in-memory linkage from dynamic
`stay_id_global` to released static `weight_kg`. It records static row and
weight coverage, duplicate/missing identifier counts, finite vasopressin input
counts, conversions with valid weight, and values lacking valid weight. No
identifier is written to a report.

## Outputs

The sanitized review Markdown and JSON are written under:

`asic/reports/production/review/unit_decision_audit/<run-id>.*`

Owner-only aggregate workbooks are written with mode `0700`/`0600` under:

`asic/reports/production/private/unit_decision_audit/<run-id>/`

The primary workbooks are:

- `conversion_evidence.parquet`;
- `mask_evidence.parquet`;
- `hospital_predecision_profiles.parquet`;
- `candidate_postconversion_profiles.parquet`; and
- `unaffected_peer_profiles.parquet`.

They contain aggregate profiles only—no patient rows, stay identifiers, raw
tokens, raw headers, or source filenames.

## Human gate

Technical completion intentionally returns overall status
`pending_human_review`. That gate was resolved by approving every conversion
and replacing every mask with a parallel-variable contract. A separate full
schema and dictionary freeze is still required before clinical data may be
rebuilt.
