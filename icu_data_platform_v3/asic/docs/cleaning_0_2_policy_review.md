# Cleaning policy 0.2 consolidated review

This metadata-only checkpoint is the single remaining decision before a
cleaned `0.2` candidate may be built from harmonized release
`20260807T112402Z` under frozen contract `0.2`.

It validates immutable lineage and presents one complete policy:

1. replay every cleaning `0.1` rule without changing its semantics;
2. apply the already approved dictionary-driven rule that masks finite
   negative medication/therapy values while preserving zero and null
   independently; and
3. activate all 12 unit-aware range dispositions that were intentionally left
   pending during the unit and schema work.

The review reads only YAML/JSON policy, contract, manifest, and release-pointer
metadata. It does not read a Parquet row, activate a cleaning rule, generate a
candidate, modify a release, or authorize export.

Run from a login frontend after deploying the code:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
bash asic/slurm/review_cleaning_0_2_policy_production.sh
```

Exit status `2` is expected internally and is handled by the wrapper: it means
the technical review passed and the single human approval gate remains. The
wrapper itself exits successfully after producing the sanitized report.

The historical total of 10,021 values outside old legacy ranges is not treated
as a projected mask count. Harmonization `0.2` changed some hospital scales,
and several reviewed rules deliberately preserve nonzero values that the old
range would have flagged. After policy approval, the cleaned candidate audit
will independently recompute the exact effect of every rule against the
released harmonized `0.2` data.
