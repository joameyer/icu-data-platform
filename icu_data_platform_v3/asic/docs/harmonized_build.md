# Frozen-contract harmonized build

## Purpose and input

The production build consumes verified, immutable harmonization candidate run
`20260806T074852Z`. That candidate is a fully conserved intermediate derived
from the lossless per-hospital ingestion and is bound by the frozen contract;
it is not a replacement authoritative input boundary. Lossless ingested data
and its provenance remain the replay boundary.

The build verifies the candidate file hashes and hospital row counts before
reading them. It then streams hospitals in the approved deterministic order
into one pooled static and one pooled dynamic Parquet. It never materializes
the complete dynamic table in memory.

## Applied transformations

Only frozen harmonization decisions are active:

- the complete reviewed categorical contract, with fail-closed domains;
- UK04 etCO2 division by `7.50062` to mmHg;
- UK03 FiO2 and hematocrit multiplication by `100` to percentage points;
- UK00 lymphocyte percentage multiplication by `100`;
- artificial `anchored_time_since_icu_admission`, equal to
  `2020-01-01 00:00:00` plus relative minutes;
- exact frozen field order, Arrow physical types, units and eligibility; and
- unchanged preservation of all five operational provenance fields.

No row or stay filtering, physiologic masking, power-of-ten repair, cleaning,
derivation, cohort definition or publication occurs.

Outputs are run-scoped under
`asic/data/production/harmonized_candidates/<run-id>/` and remain
non-publishable.

## Complete audit

The second streaming pass independently recomputes the expected output for
every cell. It compares:

- every unchanged clinical value;
- every categorical transformation and its complete counts;
- all four hospital conversion rules;
- every artificial timestamp;
- every identifier value without reporting it;
- all five provenance values;
- the exact frozen schema, hashes, row counts and hospital order; and
- build-manifest rule accounting.

The audit writes reports only. Its expected outcome is technical `PASS` with
overall status `PENDING HUMAN REVIEW` and one release-approval finding.

## Production invocation

Submit both passes as one modest job from a login shell:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_harmonized_build_and_audit_production.sh
```

The job requests two CPUs, 8 GiB RAM, 30 minutes, and the `c23ms` partition.
Do not rerun the same build or audit ID and do not delete an incomplete output
without reviewing its error first.

## Approved promotion

Data-owner approval recorded on 2026-08-06 resolves the sole human gate for
candidate `20260806T111156Z` under frozen contract `0.1`. Promotion performs no
new transformation. It verifies the build manifest, private and sanitized
audit evidence, frozen contract, exact schemas, row counts, and all file hashes
before copying the audited Parquet bytes unchanged to:

`asic/data/production/harmonized/releases/20260806T111156Z/`

The release is immutable. The source candidate remains in place. The release
manifest authorizes the harmonized layer and its use as cleaning input;
cleaning, derivation, cohort selection, and external data export remain outside
the promotion scope.

Submit the modest promotion job from a login shell:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_promote_harmonized_release_production.sh
```

Promotion creates `harmonized/current_release.json` exactly once and will not
overwrite either that pointer or an existing release.
