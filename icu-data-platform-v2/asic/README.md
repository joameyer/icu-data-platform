# ASIC data pipeline

The ASIC pipeline turns already-pooled ICU data into a lossless translated
layer, an analysis-agnostic cleaned layer, and independently generated derived
datasets. The old
`hpc-icu-data-platform/` implementation is not imported or modified.

Current status:

- pooled input contract and strict audit: implemented;
- production-to-demo and production-to-mock pooled generators: implemented;
- pooled-to-translated names, approved merges, column order, and reviewed
  categorical values: policy `1.3` implemented; demo and production artifact
  regeneration pending human review;
- translated input contract and read-only structural/provenance audit:
  contract `1.3` implemented and becomes the accepted boundary after those
  regenerated artifacts pass audit;
- read-only numeric and cross-hospital data-quality audit, including preserved
  legacy rules: expanded neighbor-context and UK00 PBW-formula demo rerun and
  human review pending; production is gated on that review;
- translated-to-cleaned boundary: documented but not implemented; and
- cleaned-to-derived clinical features and time blocking: documented but not
  implemented.

## Start here

Read the documentation in data-flow order:

1. [Pooled data](docs/pooled_data.md)
2. [Pooled to translated](docs/pooled_to_translated.md)
3. [Translated data](docs/translated_data.md)
4. [Cross-hospital data-quality audit](docs/cross_hospital_data_quality.md)
5. [Translated to cleaned](docs/translated_to_cleaned.md)
6. [Cleaned to derived](docs/cleaned_to_derived.md)

Operational references:

- [Generate the protected demo](docs/generating_demo_data.md)
- [Generate the development mock data](docs/generating_mock_data.md)

## ASIC directory map

```text
asic/
├── README.md
├── run_pooled_to_translated_production.sh
├── run_translated_input_audit_production.sh
├── run_cross_hospital_audit_production.sh
├── config/
│   ├── datasets/              # one profile per isolated data context
│   ├── pooled/                # source contract and pooled generators
│   ├── pooled_to_translated/  # executable translation policy
│   └── translated/            # frozen contract plus read-only quality policy
├── docs/                      # exact human-readable data and stage policies
├── notebooks/                 # exploratory audits, never pipeline logic
├── data/                      # protected/generated artifacts (git-ignored)
│   ├── production/
│   ├── demo/
│   └── mock/
├── reports/                   # generated aggregate reports (git-ignored)
└── runs/                      # cluster run records (git-ignored)
```

The same file in `config/datasets/` configures pooled audit, categorical
inventory, pooled-to-translated processing, translated audit, and the future
cleaned output path for a context. This prevents paths from drifting across
command-specific YAML files. Derived recipes will add their own explicit
output configuration when implemented.

`config/pooled/contract.yaml` is the strict source interface.
`config/pooled_to_translated/policy.yaml` is the executable source of truth for
names, merges, categorical translations, and output order. Human-readable
counterparts live under `docs/`.

`config/translated/contract.yaml` freezes the exact translated schemas,
producer manifest requirements, and downstream column-preservation boundary.
`config/translated/data_quality_audit.yaml` preserves the numeric,
cross-hospital, and legacy review rules without authorizing transformations.

Do not place pipeline logic, policies, or runbooks inside `data/`; that
directory contains artifacts plus a single tracked
[`data/README.md`](data/README.md) explaining the artifact layout.

## Data artifacts

Each context uses the same logical layers:

```text
asic/data/<context>/
├── pooled/
├── translated/
├── cleaned/
└── derived/
    └── time_blocking/
        └── <resolution>/
```

`production` is complete protected data. `demo` is an unchanged protected
subset for bounded audits. `mock` is shuffled/remapped development data. None
of these artifacts are committed.

## Complete pooled-to-translated rerun procedure

Run every command below from the repository root. Production transformation
must be submitted through Slurm; do not run it on a login node. Unexpected
schema, alias, or categorical values block publication rather than being
guessed.

### 1. Install and test the synchronized code

The virtual environment and protected artifacts are not replaced by repository
synchronization.

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform

./.venv/bin/python -m pip install wheel
./.venv/bin/python -m pip install \
  --no-deps \
  --no-build-isolation \
  -e .

./.venv/bin/python -m pytest -q
```

All tests must pass before accessing pooled data. Tests use temporary synthetic
Parquet files and never require protected ASIC data.

### 2. Validate the policy and audit the pooled demo

```bash
./.venv/bin/python -m asic_pipeline validate-translation-policy \
  --config asic/config/datasets/demo.yaml

./.venv/bin/python -m asic_pipeline audit-pooled \
  --config asic/config/datasets/demo.yaml
```

Review the new `pooled_input_audit_*.json` under `asic/reports/demo/`. It must
report `overall_status: pass`, exact schemas, zero identifier mismatches, exact
relative/anchored time agreement, unique stay/time keys, and matching
static/dynamic stay sets.

### 3. Inventory demo categorical values

Run this whenever pooled input, categorical selections, alias rules, or value
mappings change:

```bash
./.venv/bin/python -m asic_pipeline inventory-pooled-categories \
  --config asic/config/datasets/demo.yaml
```

Review the new `categorical_value_inventory_*.json` under
`asic/reports/demo/`. Inventories must be complete within the configured
distinct-value limit, non-binary therapy-confirmation values are forbidden,
and unresolved alias conflicts must be reviewed before continuing.

### 4. Build and review the translated demo

For the first build:

```bash
./.venv/bin/python -m asic_pipeline pooled-to-translated \
  --config asic/config/datasets/demo.yaml
```

If a previous translated demo exists, use `--overwrite` only after confirming
that the target is exactly `asic/data/demo/translated/`. Pooled demo files are
never overwritten:

```bash
./.venv/bin/python -m asic_pipeline pooled-to-translated \
  --config asic/config/datasets/demo.yaml \
  --overwrite
```

Review `asic/data/demo/translated/translation_manifest.json`. Approval requires:

- the expected contract, policy, and pipeline versions;
- identical source/output row counts and preserved source order;
- 22 static and 132 dynamic translated columns under policy version `1.3`;
- all schema, identifier, time, merge, drop, and categorical checks passing;
- zero unresolved merge conflicts and disallowed-hospital alias values;
- zero unmapped non-missing categorical values;
- approved `-1` missing sentinels normalized in `hosp_los`,
  `dialysis_free_days`, and `vent_free_days`, with the exact counts recorded;
- UK00 source values moved losslessly to `vt_per_ideal_bw_total`, with
  `vt_per_kg_ideal_body_weight` missing at UK00, the new field missing outside
  UK00, and manifest source-value conservation passing;
- the ARDS duplicate field proving entirely missing before its drop; and
- no unit corrections, general invalid-value masking, mortality consolidation,
  or cohort filtering.

Do not proceed because the command merely exited successfully; the manifest is
the human approval artifact.

### 5. Audit pooled production as a separate human checkpoint

```bash
./.venv/bin/python -m asic_pipeline audit-pooled \
  --config asic/config/datasets/production.yaml
```

Review the new production audit under `asic/reports/production/`. Confirm the
complete expected hospital set, exact schemas, zero identifier/time/key
failures, and matching static/dynamic stay sets. This audit is read-only.

### 6. Submit production translation through Slurm

The production job is fixed to the production configuration and deliberately
has no overwrite option. It repeats policy validation, the pooled audit, and
the categorical inventory inside the compute job before translating.

Create the log directory before submission because Slurm opens log files before
the script starts:

```bash
mkdir -p asic/runs/production

sbatch asic/run_pooled_to_translated_production.sh
```

The script requests four CPUs, 32 GB memory, and four hours. It uses
`./.venv/bin/python`, reads `asic/config/datasets/production.yaml`, and writes
logs to:

```text
asic/runs/production/asic_pooled_to_translated_<job-id>.log
asic/runs/production/asic_pooled_to_translated_<job-id>.err
```

Monitor the job using the ID printed by `sbatch`:

```bash
squeue -j <job-id>
tail -f asic/runs/production/asic_pooled_to_translated_<job-id>.log
sacct -j <job-id> --format=JobID,State,Elapsed,MaxRSS,ExitCode
```

Replace `<job-id>` with the numeric Slurm job ID. A failed preflight or audit
stops the job before translation. The transformation writes through a staging
directory and publishes completed artifacts only after all checks pass.

### 7. Review the production translation manifest

After a successful Slurm job, review:

```text
asic/data/production/translated/translation_manifest.json
```

Apply the same checklist as for demo, and additionally compare all merge,
categorical, and numeric missing-sentinel counts with the accepted complete
production inventory. In
particular, the known therapy-confirmation `0/1` disagreement must be recorded
as resolved to `1`, with no non-binary values or excluded rows.

Production pooled-to-translated processing is complete only after this
manifest is explicitly accepted. Cleaning and derivation are separate stages.

## Validate the translated input boundary

The accepted translation manifest must also be checked against the frozen
translated contract before any downstream diagnostic or transformation is
used. Audit demo first:

```bash
./.venv/bin/python -m asic_pipeline audit-translated \
  --config asic/config/datasets/demo.yaml
```

Review the new `translated_input_audit_*.json` under `asic/reports/demo/`.
It must confirm contract version `1.3`, exact 22/132 schemas, accepted producer
versions, all required manifest checks, preserved row counts, consistent
identifiers, exact relative/anchored time agreement, unique dynamic keys, and
matching static/dynamic stay sets.

Run the complete production scan through Slurm:

```bash
mkdir -p asic/runs/production
sbatch asic/run_translated_input_audit_production.sh
```

Review both its log and the generated report under
`asic/reports/production/`. This job is read-only and never creates or changes
cleaned or derived data.

## Review cross-hospital data quality

After the translated boundary passes, run the read-only quality audit on demo:

```bash
./.venv/bin/python -m asic_pipeline audit-cross-hospital \
  --config asic/config/datasets/demo.yaml
```

Review the newest `cross_hospital_data_quality_audit_*.json` under
`asic/reports/demo/`, especially `targeted_scale_hypotheses`,
`scale_entry_candidate_discovery`, `scale_entry_candidate_context`,
`scale_entry_candidate_review_coverage`,
`predicted_body_weight_tidal_volume_audit`,
`row_level_scale_entry_audits`,
`targeted_bucket_audits`,
`targeted_cross_hospital_comparisons`, and `relationship_audits`. Candidate
scale profiles and isolated recovery candidates are computed only for review;
they are not written to data and remain explicitly unapproved. Confirm that
`scale_entry_candidate_context.collection_complete` is `true`, then review its
1-, 4-, and 8-hour temporal classifications and same-row related-field
evidence. This context section contains no stay identifiers or candidate
times. It covers the review-selected `core_temp`, `evlwi`, `ph_art`, `fio2`,
`map`, `spo2`, `sao2`, and `scvo2` scopes. Also confirm that
`scale_entry_candidate_review_coverage.uncovered_scope_count` and
`uncovered_candidate_count` are both zero; either nonzero result is blocking.
For UK00, review all three PBW tidal-volume comparisons and confirm the
cross-table linkage check passes. Do not interpret this formula section from a
mock run: mock dynamic columns are independently shuffled, so only demo and
production retain the required row-level relationships.

Only after accepting the expanded neighbor-context and UK00 PBW-formula demo
report, submit the complete production scan to a compute node:

```bash
mkdir -p asic/runs/production
sbatch asic/run_cross_hospital_audit_production.sh
```

The exact evidence fields and review order are documented in
[cross_hospital_data_quality.md](docs/cross_hospital_data_quality.md). Stop for
human review of the production report before defining any cleaning
correction.

## Replacing existing generated artifacts

The production batch script stops if any translated production output exists.
It never accepts `--overwrite`. A rerun requires a separate human decision to
archive the complete existing `asic/data/production/translated/` directory
under a dated, reviewed name before resubmission. Never delete or partially
replace files within a published translated artifact.

Demo and mock generators also refuse replacement by default. Their exact
generation and review procedures are documented in
[generating_demo_data.md](docs/generating_demo_data.md) and
[generating_mock_data.md](docs/generating_mock_data.md).

## Output and evidence locations

| Purpose | Location |
|---|---|
| Dataset profiles | `asic/config/datasets/` |
| Pooled contract | `asic/config/pooled/contract.yaml` |
| Translation policy | `asic/config/pooled_to_translated/policy.yaml` |
| Translated input contract | `asic/config/translated/contract.yaml` |
| Cleaning-stage design | `asic/docs/translated_to_cleaned.md` |
| Derived-stage design | `asic/docs/cleaned_to_derived.md` |
| Demo translated manifest | `asic/data/demo/translated/translation_manifest.json` |
| Production translated manifest | `asic/data/production/translated/translation_manifest.json` |
| Audit and inventory reports | `asic/reports/<context>/` |
| Production Slurm logs | `asic/runs/production/` |

Reports and manifests contain aggregate evidence and no stay-ID listings.
