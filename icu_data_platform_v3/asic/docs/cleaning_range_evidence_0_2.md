# Cleaning 0.2 range-direction evidence

This checkpoint follows cleaning-policy review `20260807T131906Z`. That review
correctly exposed the complete proposed policy, but its historical counts did
not distinguish nonpositive values from upper-tail values and therefore were
not sufficient for an informed masking decision.

The audit streams only the 12 reviewed variables and `hospital_id` from
harmonized release `20260807T112402Z`. For every hospital-variable pair it
counts:

- null/non-finite, negative, exact-zero, positive-within-range, and above-range
  values separately;
- fractional SOFA values;
- values above both the inherited 24-point SOFA-without-GCS ceiling and the
  structural 20-point candidate;
- power-of-ten candidates with zero, exactly one, or multiple valid factors;
  and
- complete decision-relevant value frequencies, subject to a fail-closed
  reviewed capacity far above the historical finding count.

Exact value-frequency and hospital-level evidence is owner-only. The sanitized
review contains complete aggregate variable counts and exact aggregate outlier
quantiles, without stay identifiers, patient rows, filenames, or raw tokens.

Run the low-resource self-submitting wrapper from a login shell:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_cleaning_range_evidence_0_2_production.sh
```

The wrapper requests one CPU, 2 GiB on `c23ms`, and 15 minutes. Exit `2` from
the audit is the expected technical-pass/human-review state and is handled by
the wrapper. No clinical data artifact or cleaning rule is written.

Do not approve the earlier 12-rule block until this report has been reviewed.
Afterward, revise or approve all range dispositions together and then build one
cleaned `0.2` candidate with an independent cell-level audit.
