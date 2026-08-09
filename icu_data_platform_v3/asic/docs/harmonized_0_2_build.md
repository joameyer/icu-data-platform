# Harmonized 0.2 candidate build and audit

## Purpose

Frozen schema and dictionary contract `0.2` authorizes a new run-scoped
harmonized candidate. The build activates exactly the nine reviewed
hospital-scoped unit conversions and nine value-preserving semantic splits. It
does not clean, derive, filter, publish, or modify any existing release.

The clinical source is immutable harmonized release `20260806T111156Z` under
contract `0.1`. This keeps all already audited naming, categorical, parsing,
missing-sentinel, notation, and initial unit decisions. V3 has no runtime
dependency on v2 or the legacy project.

One narrow reference is explicit: the UK00 weight-normalized vasopressin
conversion uses `weight_kg` from immutable cleaned release
`20260806T114234Z`. That is the exact static weight representation used by the
approved unit-decision audit, including its one reviewed recoverable weight
correction. No cleaned dynamic field is read or reused.

## Transformations

The nine unit conversions are scoped by both canonical variable and hospital.
This matters because `d_dimer` has separate UK03 and UK08 rules and
`vasopressin_iv_cont` has separate UK00 and UK08 rules. The implementation
retains every applicable rule rather than selecting one rule per variable.

The nine semantic splits route the affected hospital value unchanged to a
parallel variable. Within that hospital, the ordinary canonical variable is
set to null because it represents a different unit or definition. Outside the
hospital, the parallel variable is null. No split value is coalesced, converted,
or discarded.

Medication semantics are conserved during harmonization:

- explicit numeric zero remains zero;
- null remains unavailable or unobserved;
- finite negative values remain present in harmonized data; and
- no carry-forward or exposure state is inferred.

The reviewed general negative-medication rule belongs to cleaning policy
`0.2`; it is not applied here.

## Outputs and immutability

The candidate is written atomically under:

`asic/data/production/harmonized_0_2_candidates/<run-id>/`

It contains pooled `static.parquet`, pooled `dynamic.parquet`, and a private
build manifest with source, contract, policy, output, and rule-accounting
hashes. It is non-publishable and cannot overwrite a prior or incomplete run.
Harmonized `0.1`, cleaned `0.1`, core-derived `0.1`, and both frozen schema
contracts remain unchanged.

## Complete audit

The audit uses a separately implemented transformation path. It re-hashes all
inputs, independently recomputes all nine conversions and all nine splits,
revalidates UK00 stay-to-weight linkage, compares every output cell and all five
provenance fields, verifies the exact frozen Arrow schemas, and requires exact
agreement with build rule accounting.

The sanitized report contains aggregate per-rule counts. Exact clinical rows,
stay identifiers, and protected provenance remain out of committed and
sanitized reports. A technically passing audit deliberately exits with one
human blocker: exact candidate release approval.

## Production run

Deploy the reviewed source snapshot, install it into the existing v3 virtual
environment, run the tests, and then launch the self-submitting wrapper from a
login shell:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_harmonized_0_2_build_and_audit_production.sh
```

The job requests the `c23ms` partition, one CPU, 4 GiB RAM, and 30 minutes. It
expects the complete audit to return the review-gated exit code `2`; any other
nonzero result fails the job. Do not rerun an existing run ID or remove an
incomplete directory without reviewing the error and preserved artifacts.

After completion, return the sanitized audit Markdown. Promotion, cleaning
policy `0.2`, cleaned rebuild, and downstream derived rebuild remain separate
human-reviewed steps.

## Approved promotion

Production audit `20260807T112402Z` passed over 16,054 static rows,
24,069,379 dynamic rows, and 3,466,440,088 output cells. All nine conversions
and nine semantic splits were independently recomputed, all 201 negative
medication values remained preserved, and UK00 weight linkage had zero missing
or invalid links.

The data owner explicitly approved candidate `20260807T112402Z` under frozen
contract `0.2`. Promotion copies the audited static and dynamic Parquet bytes
unchanged into the versioned harmonized release. Before advancing
`harmonized/current_release.json`, it stores the exact previous pointer bytes
inside the new release. Harmonized release `20260806T111156Z` and every other
prior artifact remain immutable.

Run the low-resource self-submitting promotion wrapper from a login shell:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_promote_harmonized_0_2_release_production.sh
```

This promotion applies no harmonization, cleaning, derivation, filtering, or
external export. It makes the exact 0.2 release available as the later cleaning
policy `0.2` input.
