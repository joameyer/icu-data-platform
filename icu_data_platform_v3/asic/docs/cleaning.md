# ASIC v3 cleaning candidate and complete audit

## Input and scope

Cleaning reads only immutable harmonized release `20260806T111156Z` under
contract `0.1`. It verifies the current-release pointer, release manifest,
hashes, rows, schemas, and promotion lineage before processing any row. The
harmonized release is never modified.

Output is a run-scoped, non-publishable candidate under
`asic/data/production/cleaned_candidates/<run-id>/`. Static and dynamic data
are streamed in bounded Arrow batches; the complete dynamic table is never
materialized in memory.

## Numeric cleaning

The native v3 registry carries 47 selectively migrated legacy range targets.
`delta_p_computed` is intentionally unavailable because it is derived later.
Of the 46 source-backed targets, 34 have approved units and receive range
cleaning. Twelve retain unresolved units and are audited but not range-cleaned.
Non-finite values are converted to null in every float64 field.

Fifteen explicit v3 additions cover static nonnegative durations and weight,
plus resolved-unit dynamic set values and corresponding measurements. Bounds
are inclusive. Each explicit `invalid_zero` decision is also enforced.

An out-of-range value is tested with factors `0.001`, `0.01`, `0.1`, `10`,
`100`, and `1000` only when both bounds exist. It is repaired only when exactly
one factor produces an in-range value. No match or multiple matches results in
null. Every factor, ambiguity, mask, and non-finite value is counted. This can,
for example, repair an isolated pH entry with a unique decimal interpretation
without guessing between ambiguous blood-pressure interpretations.

## Lists and site-specific evidence

`height_measurements_cm` remains an ordered list. Null, non-finite, and values
outside 120–220 cm are removed element-by-element. Order and duplicates remain;
no patient-level aggregate is selected. A non-empty source list whose elements
are all removed becomes an empty list.

All UK06 `vt_per_kg_ideal_body_weight` values are set to null because the mixed
scale remains irrecoverable. Thresholded division was not approved. The absent
`vt_per_kg` field is not manufactured.

The four globally all-missing fields—`feo2`,
`severity_read_confirmation`, `sofa_score_without_gcs`, and
`stroke_volume_bolus`—remain in their frozen positions.

No row, stay, or column is dropped. Identifiers and all five provenance fields
are unchanged. Cleaning does not derive driving pressure, mortality,
ventilation flags, cohorts, or time blocks.

## Complete audit and human gate

A second streaming pass re-verifies input and output hashes and independently
recomputes every output cell. It checks the cleaned schema, conservation,
unchanged values, corrections, masks, lists, all-missing columns, provenance,
and manifest accounting. No governed out-of-range or non-finite value may
remain.

The expected result is technical `PASS`, overall `PENDING HUMAN REVIEW`, and
one blocker: `cleaned_candidate_release_approved`. Owner-only evidence contains
aggregate rule and factor counts, never stay identifiers or patient rows.

## Approved production release

Candidate `20260806T114234Z` passed the complete audit over 16,054 static rows,
24,069,379 dynamic rows, and 3,249,815,677 output cells. The audit found zero
technical blockers and zero post-clean range or non-finite violations. The
data owner subsequently approved promotion under cleaning policy `0.1`.

Promotion copies the audited Parquet bytes unchanged to
`asic/data/production/cleaned/releases/20260806T114234Z/`, creates the
non-overwritable `cleaned/current_release.json` pointer, and preserves the
candidate and all audit evidence. It does not rerun cleaning or apply
derivation, cohort logic, time blocking, or external export.

After deploying and testing the promotion implementation, run:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_promote_cleaned_release_production.sh
```

The expected result is `PASS`, zero blockers,
`audited_parquet_bytes_preserved=true`, `cleaned_layer_ready=true`, and
`derived_input_approved=true`. Do not rerun after success because the release
and current pointer are deliberately non-overwritable.

## Production command

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_cleaning_build_and_audit_production.sh
```

The job requests two CPUs, 8 GiB RAM, 30 minutes, and `c23ms`. Do not rerun an
existing run ID or remove an incomplete directory before examining its error.
