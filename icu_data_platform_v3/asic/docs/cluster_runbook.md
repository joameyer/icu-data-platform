# Phase 1 cluster runbook

Phase 1 inventory and anomaly-audit runs have been executed on the authorized
cluster. This runbook remains the reproducible procedure for new immutable
runs.

## Cleaning policy 0.2 review

Harmonized release `20260807T112402Z` is the current approved cleaning input.
Before building cleaned data, run the metadata-only consolidated policy review
from the login frontend:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/review_cleaning_0_2_policy_production.sh
```

The wrapper reads no Parquet row and submits no Slurm job. It validates the
current harmonized pointer, release manifest, frozen contract, cleaning `0.1`
policy, unit decisions, and medication semantics. It then presents replay of
all `0.1` rules, the approved negative-medication rule, and all 12 unit-aware
range dispositions as one human gate. Do not build cleaned `0.2` data until
that exact report is approved.

The first review showed that historical aggregate violation totals do not
separate nonpositive values from potentially genuine upper-tail measurements.
Run the follow-up streaming evidence audit before approving the policy:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_cleaning_range_evidence_0_2_production.sh
```

This wrapper requests `c23ms`, one CPU, 2 GiB, and 15 minutes. It scans the 12
variables once, writes aggregate evidence only, and changes no clinical data
or release. Return the sanitized report before freezing cleaning policy `0.2`.

## Harmonized 0.2 candidate and complete audit

After contract `0.2` has frozen successfully, deploy the reviewed code and run
the self-submitting low-resource wrapper from a login shell:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_harmonized_0_2_build_and_audit_production.sh
```

The wrapper requests `c23ms`, one CPU, 4 GiB, and 30 minutes. It validates the
frozen contract, harmonized `0.1` release, cleaned `0.1` static weight
reference, reviewed decisions, and all required paths before submission. It
does not require cleaned dynamic data or any v2/legacy runtime file.

The build writes only a run-scoped non-publishable candidate. Before an exact
approval is recorded, the independent audit returns exit `2` after a technical
pass. Return the sanitized Markdown report and do not promote, clean, derive,
or update a release pointer before that review.

Candidate `20260807T112402Z` passed and subsequently received explicit
data-owner promotion approval. After deploying the promotion implementation,
run:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_promote_harmonized_0_2_release_production.sh
```

The wrapper requests `c23ms`, one CPU, 2 GiB, and 20 minutes. It requires the
exact candidate, audit, frozen contract, previous current pointer, and previous
release manifest. It refuses an existing 0.2 release, staging directory, or
promotion report. Promotion preserves the previous pointer bytes within the
new release before atomically advancing `harmonized/current_release.json`.
It does not clean, derive, filter, or authorize external export.

## Cleaned 0.2 candidate and complete audit

The data owner approved the final 12-variable range matrix after range audit
`20260807T133609Z`, including the conservative upper-tail dispositions, the
unique EVLWI recovery rule, and the general negative-medication rule across 34
source and semantic-split variables. Run the self-submitting wrapper from a
login shell:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_cleaning_0_2_build_and_audit_production.sh
```

The wrapper requests `c23ms`, one CPU, 4 GiB, and 40 minutes. It writes only a
run-scoped cleaned candidate plus Parquet and Markdown cleaned-variable
dictionaries. The independent audit recomputes every output cell and returns
exit `2` after the expected technical pass pending human approval. Return the
sanitized audit report; do not promote the candidate yet.

## Preconditions for human verification

1. Confirm the deployment root is exactly
   `/hpcwork/jrc_combine/joana/icu_data_platform_v3`.
2. Confirm the v3 root has its own environment, code, config, data, reports, and
   runs directories.
3. Confirm `asic/runs/production/` exists before submitting, because Slurm opens
   log files before the job script begins.
4. Confirm the production raw root is mounted read-only for the job account
   wherever operationally possible.
5. Review `asic/config/datasets/production.yaml` and the inventory policy in
   the deployed v3 source snapshot. No older project directory is required.

## Submission

Submit `asic/slurm/run_raw_inventory_production.sh` from the v3 deployment using
the site-approved Slurm command. Do not submit a copied script from v2.

The expected production exit status remains `2` while the reviewed ZIP awaits
owner confirmation. An output report with a failed Slurm step is expected
evidence, not a reason to bypass the gate.

## Review

Review the sanitized Markdown/JSON report first. Then, on the cluster, inspect
the owner-only bundle for exact filenames, hashes, identifier disagreements,
static comparisons, and folder `01` overlap evidence. Record the cluster job ID,
deployed source snapshot identifier, config hashes, private report run ID,
reviewers, findings, and final decision in the decision log.

The expected hospital contract now includes folder `01` as `asic_UK01`, with
cohort action `include`. Do not change that contract without a new documented
human decision.

## Approved raw-anomaly extension

After approving inventory artifact `0.2`, submit the separate read-only anomaly
audit with that immutable inventory run ID:

```bash
sbatch --export=ALL,INVENTORY_RUN_ID=<approved_inventory_run_id> \
  asic/slurm/run_raw_anomaly_audit_production.sh
```

The job re-hashes audited sources, streams UK00 duplicate-column values, and
streams ZIP members without extraction. It writes only report evidence under
the v3 report tree. The expected remaining blocker is archive owner
confirmation; therefore exit status `2` remains an expected human-review
outcome.

## Lossless ingestion implementation checkpoint

The ingestion command and an explicitly approved all-hospital production job
are implemented. Individual hospital execution remains available:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m asic_pipeline \
  ingest-raw-hospital \
  --config asic/config/datasets/production.yaml \
  --inventory-run-id <reviewed_inventory_0.4_run_id> \
  --hospital asic_UKNN
```

The command refuses an existing hospital output and accepts no inventory
blocker except the provisional archive owner confirmation. A successful command
still writes `publication.ready: false`; it does not authorize parsing,
harmonization, concatenation, or publication.

### All-hospital production job

`run_lossless_ingestion_all_production.sh` is self-submitting. Run it with
`bash` from a login shell; do not invoke it directly with `sbatch`. The submit
phase validates the inventory run and calls `sbatch`. The compute phase performs
one complete preflight and then ingests the eight approved hospitals
sequentially in canonical order.

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
export INVENTORY_RUN_ID=20260804T193415Z
bash asic/slurm/run_lossless_ingestion_all_production.sh
```

The preflight refuses to submit or run if any `asic_UKNN` output directory
already exists. It never skips or overwrites a hospital. Therefore, move the
reviewed ingestion `0.1` UK00 directory intact to the quarantine tree before
submission. The job is sequential and fail-fast: a hospital failure prevents
later hospitals from starting.

If the job is interrupted after completing one or more hospitals, preserve all
completed and staging artifacts for review. Do not delete them or resubmit the
all-hospital job—the existing-output preflight will deliberately block—until a
human records which outputs are valid and approves a recovery plan.

## Post-ingestion audit checkpoint

After all eight hospital commands complete, run the self-submitting read-only
audit from a login shell. Use the exact inventory run that produced the
ingested artifacts:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
export INVENTORY_RUN_ID=20260804T193415Z
bash asic/slurm/run_ingestion_audit_production.sh
```

The worker hashes all 16 Parquet outputs and streams only their provenance
columns. It writes owner-only evidence under
`asic/reports/production/private/ingestion_audit/` and a sanitized Markdown and
JSON report under `asic/reports/production/review/ingestion_audit/`. It does not
modify ingested data and does not generate harmonized, pooled, cleaned, or
derived artifacts.

Historical audit runs generated before owner confirmation have status `FAIL`
with the single blocker `provisional_archive_owner_confirmation`. The Slurm
wrapper records that historical human-review state as a completed job; any
command exit other than `0` or the expected review-gated `2` fails the job.
Review both the sanitized report and owner-only `failures.json`.

Reviewed run `20260805T060046Z` met this condition: all 13 technical checks
passed for 8 hospitals, 16 tables, 16,054 static rows, and 24,069,379 dynamic
rows. Its only blocker is archive-owner confirmation. The audit is immutable
and does not need to be rerun for the translated-to-harmonized terminology
decision.

As of 2026-08-05, the data owner confirmed that the archive is an old snapshot
that must remain excluded. Registry-review policy `0.3` resolves the historical
carried finding explicitly; it does not rewrite the older inventory, ingestion,
or schema-token reports.

## Raw-schema and parsing-token inventory

After deploying and verifying the approved read-only implementation, submit the
self-submitting job from a login shell with both immutable input run IDs:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
export INVENTORY_RUN_ID=20260804T193415Z
export INGESTION_AUDIT_RUN_ID=20260805T060046Z
bash asic/slurm/run_schema_token_inventory_production.sh
```

The job re-hashes all 16 ingested Parquet inputs and scans raw string columns in
bounded Arrow batches. It requests 48 hours, 64 GiB, and four CPUs because the
dynamic inputs contain 24,069,379 wide rows. It produces only immutable private
and sanitized reports under `schema_token_inventory/`.

Policy/artifact `0.3` overlays 18 active human-approved raw-v3 rules on the
still-unreviewed seed registry. It supersedes the scalar UK00 height rule with
`height_measurements_cm`, an evidence-only numeric-list parser that treats
`nan` list elements as approved missing elements while preserving list order
and duplicates. On the unchanged inputs, expect zero unmapped occurrences,
zero unresolved numeric tokens, zero unresolved list elements, zero blocking
example truncations, zero reviewed-domain violations, and 15,492 scoped UK08
sentinel annotations. The overall report must still fail the broader
registry-approval and provisional archive-confirmation gates. Any additional
blocker requires review and must not be assumed away.

Exit status `2` is the expected human-review state and is converted by the
wrapper into a completed Slurm job. Any technical blocker returns `1` and fails
the job. The sanitized console summary must report
`technical_blocking_finding_count=0` before private clinical review begins.

Do not print or copy `tokens.parquet`. Review exact raw headers and tokens only
inside the authorized cluster. Return the sanitized Markdown report for the
next decision; if technical blockers exist, return only their generic check
names and keep private details on the cluster.

## Candidate harmonization-registry review

After policy/artifact `0.3` run `20260805T090942Z` has been reviewed, submit the
small read-only registry-review job from a login shell:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
export SCHEMA_TOKEN_RUN_ID=20260805T090942Z
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_harmonization_registry_review_production.sh
```

The worker reads only the immutable private/sanitized schema-token bundle. It
does not reopen ingested Parquet and does not write under `asic/data/`. It writes
private `occurrences.parquet` and `variables.parquet` workbooks under
`asic/reports/production/private/harmonization_registry_review/`, plus sanitized
JSON and Markdown under the matching review directory.

Exit status `2` is expected because candidate mappings, types, units,
categorical policies, parsers, aliases, semantic splits, the ordered union
schema, the variable dictionary, and archive confirmation still require human
review. The wrapper converts that expected review state to a successful Slurm
job. Any technical blocker returns `1` and must not be bypassed.

Use review policy/artifact `0.3` or later for mapping decisions. Initial run
`20260805T093848Z` passed technical accounting but artifact `0.1` did not expose
the preserved UK00 PBW tidal-volume split as a dedicated semantic candidate;
run `20260805T094801Z` corrected that omission under artifact `0.2` but treated
cross-hospital header spelling variants as alias merges. Retain both runs as
immutable evidence and supersede them with a `0.3` review run, which requires
coalescence review only for same-hospital target collisions.

Keep the private workbooks and the referenced token evidence on the authorized
cluster. Return only the sanitized Markdown report and generic check names.

## First static-variable contract audit

After composite-source audit run `20260805T111026Z` passes, submit the small
read-only static evidence job from a login shell:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
export REGISTRY_REVIEW_RUN_ID=20260805T104417Z
export COMPOSITE_AUDIT_RUN_ID=20260805T111026Z
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_static_contract_audit_production.sh
```

The job re-hashes and streams selected columns from all eight ingested static
Parquets. It writes reports only. Exit status `2` is the expected human-review
state and is converted to a successful Slurm job; a technical blocker returns
`1`. Return the sanitized Markdown report. Inspect exact headers and bounded
token examples only in the owner-only cluster workbooks.

## Remaining-static contract review

After deploying and verifying the read-only implementation, re-aggregate the
four still-pending variables from immutable static-contract run
`20260805T112746Z`:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
export STATIC_CONTRACT_AUDIT_RUN_ID=20260805T112746Z
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_static_completion_review_production.sh
```

The job reads only the existing static-contract private/sanitized evidence and
partial registry `0.2`. It does not open raw or ingested data and writes only a
sanitized report under `review/static_completion_review/`. Exit status `2` is
the expected human-review state and is converted to a successful Slurm job.
Return the generated Markdown report for explicit approval or revision of the
four proposals.

## ICD-10 notation audit

After deploying registry `0.3` and the read-only audit implementation, submit
the self-submitting job from a login shell:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
export STATIC_CONTRACT_AUDIT_RUN_ID=20260805T112746Z
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_icd10_notation_audit_production.sh
```

The worker re-hashes all eight ingested static Parquets but reads only the
single ICD-10 source-text field selected by immutable static-contract evidence.
Exact source strings are written only to the owner-only `examples.parquet`.
Return the sanitized Markdown report; inspect exact examples only on the
authorized cluster. Exit status `2` is the expected review state and is
converted to a successful Slurm job. Any technical blocker returns `1`.

Reviewed notation run `20260805T131735Z` had zero technical blockers. The data
owner confirmed its 24 UK03 candidate tokens as the literal missing placeholder
`nan` and approved exact non-missing source-text preservation plus a final
component audit.

## ICD-10 component audit

After deploying reviewed ICD contract `0.1` and the read-only component audit,
submit the self-submitting job from a login shell:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
export ICD10_NOTATION_AUDIT_RUN_ID=20260805T131735Z
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_icd10_component_audit_production.sh
```

The worker revalidates the notation evidence and all eight ingestion hashes,
then streams only the selected static ICD-10 fields. It accounts for every
comma-delimited component, component-edge whitespace change, empty component,
duplicate, and list length. It writes reports only and does not create
`icd10_codes_source_text` or `icd10_codes`.

Exit status `2` is the expected human-review state and is converted to a
successful Slurm job. Return the sanitized Markdown report. If unexpected
components exist, inspect their bounded examples only in the owner-only
private bundle. A technical blocker returns `1` and must not be bypassed.

Reviewed component run `20260805T134245Z` had zero technical blockers. Its
single unexpected component was privately classified as an incomplete
input-error candidate. The empty-component and duplicate policies remain open.

## ICD-10 component-detail audit

After deploying the approved read-only extension, submit it from a login shell:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
export ICD10_COMPONENT_AUDIT_RUN_ID=20260805T134245Z
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_icd10_component_detail_audit_production.sh
```

The worker revalidates the prior component report, ingestion hashes, source
bindings, and all prior hospital counts. It rescans only the selected static
ICD-10 fields and writes bounded exact empty-bearing examples solely to the
owner-only bundle. Sanitized output contains position, multiplicity,
adjacency, and counterfactual list-size counts only.

Exit status `2` is expected and is converted to a successful Slurm job. Return
the sanitized Markdown report for the final empty-component, duplicate, and
incomplete-component decisions. A technical blocker returns `1`.

## Reviewed composite-source audit

After registry-review run `20260805T104417Z` is deployed and reviewed, submit
the bounded UK00 two-source audit from a login shell:

```bash
export REGISTRY_REVIEW_RUN_ID=20260805T104417Z
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_composite_source_audit_production.sh
```

The job re-hashes the immutable UK00 dynamic Parquet and streams only the two
reviewed source columns. It validates both all-missing retirements from the
registry workbook, checks the binary domain, and counts all fixed two-position
Boolean pair patterns. It writes reports only; no harmonized data is produced.

## Candidate harmonization dry run

After deployment tests pass, submit the approved streaming candidate run from
a login shell. This creates protected, non-publishable per-hospital candidate
Parquet under a new immutable run ID; it does not create the harmonized release.

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
export REGISTRY_REVIEW_RUN_ID=20260805T104417Z
export INGESTION_AUDIT_RUN_ID=20260805T060046Z
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_harmonization_dry_run_production.sh
```

Record the printed `harmonization_run_id`. Exit status `2` is the expected
human-review state and is converted by the wrapper to a successful Slurm job.
The console must report `technical_blocking_finding_count=0`. Preserve the
candidate run intact even when human findings remain.

## Consolidated harmonization audit

Use the exact completed candidate run ID to submit the single large read-only
audit. This replaces further one-variable audit increments.

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
export HARMONIZATION_RUN_ID=<completed-candidate-run-id>
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_consolidated_harmonization_audit_production.sh
```

The wrapper requests 30 minutes, 8 GiB, and two CPUs on `c23ms`. It re-hashes candidate,
ingested, and v3-owned policy inputs; streams all eight hospitals; audits every
numeric and numeric-list unit policy (including every unresolved unit); checks
all selectively migrated legacy ranges, scale hypotheses, comparisons, buckets,
relationships, availability, sentinel, semantic, and contextual-cleaning
rules; evaluates the UK00 PBW hypothesis without collapsing height lists; and
records older-version comparison as disabled unless a separate optional,
untrusted reference is explicitly configured. Missing or deleted older project
generations do not block this audit.

The expected result is `FAIL` with `technical_blocking_finding_count=0`. The
wrapper treats exit status `2` as a successful technical job with human-review
findings. Any technical blocker returns `1` and must be investigated rather
than bypassed. Return the sanitized Markdown report and keep the owner-only
workbooks, decision register, identifiers, tokens, and exact evidence on the
authorized cluster.

This audit writes reports only. It does not mutate the candidate, release a
harmonized dataset, clean values, derive features, or publish data. A separate
explicit approval is still required to turn a reviewed candidate into the
frozen harmonized release.

## Categorical contract checkpoint

After consolidated audit `20260806T092818Z`, run the small evidence-only
checkpoint directly on the frontend. It verifies and reuses the immutable audit
bundle and does not scan candidate data:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
unset SLURM_JOB_ID SLURM_STEP_ID
.venv/bin/asic-pipeline review-categorical-contract \
  --config asic/config/datasets/production.yaml \
  --consolidated-audit-run-id 20260806T092818Z
```

Exit status `2` is the expected human-review state. Return the sanitized
Markdown report; inspect exact values only in the printed owner-only private
directory on the cluster. No Slurm submission, data rescan, or candidate rewrite
is needed.

## Ordered-schema and variable-dictionary checkpoint

After deploying reviewed categorical contract `0.1`, run the final report-only
contract checkpoint on the frontend:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
unset SLURM_JOB_ID SLURM_STEP_ID
.venv/bin/asic-pipeline review-schema-dictionary \
  --config asic/config/datasets/production.yaml
```

The expected result is `FAIL` with exactly the schema-freeze and
dictionary-freeze human findings and `technical_blocking_finding_count=0`.
Return the sanitized Markdown report. No candidate scan or Slurm allocation is
required.

After explicit approval of review `20260806T103811Z`, freeze the exact contract
on the frontend:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
unset SLURM_JOB_ID SLURM_STEP_ID
.venv/bin/asic-pipeline freeze-schema-dictionary \
  --config asic/config/datasets/production.yaml
```

This must report `PASS`, zero blockers, and both frozen flags as true. It reads
no candidate rows and creates no clinical data. Publication remains false.

## Frozen-contract harmonized build and complete audit

After contract freeze `0.1` passes, submit the build and full cell-by-cell audit
as one modest job:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_harmonized_build_and_audit_production.sh
```

The build should pass with no technical findings. The audit should pass
technically and report `pending_human_review` with exactly one explicit release
gate. Return the sanitized audit Markdown before any promotion or publication.

## Promote the explicitly approved harmonized candidate

Candidate `20260806T111156Z` passed its complete audit over 16,054 static rows,
24,069,379 dynamic rows, and 3,249,815,677 output cells. The data owner then
explicitly approved promotion under frozen contract `0.1`.

After deploying and testing the promotion implementation, submit:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_promote_harmonized_release_production.sh
```

The expected result is `PASS`, zero blockers,
`audited_parquet_bytes_preserved=true`, `harmonized_layer_ready=true`, and
`cleaning_input_approved=true`. External export remains unauthorized. Do not
rerun the command after success: the immutable release and current pointer are
deliberately non-overwritable.

## Build and audit the cleaned candidate

Submit the candidate build and full cell-level audit together:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_cleaning_build_and_audit_production.sh
```

The build must pass. The audit should pass technically and return exit status
`2` because exactly one human promotion gate remains; the wrapper treats that
as successful completion. Return the sanitized audit Markdown and aggregate
owner-only rule/factor counts before approving a cleaned release. No rows,
stays, columns, derived fields, or time blocks are removed or created.

## Promote the explicitly approved cleaned candidate

Candidate `20260806T114234Z` passed its complete audit over all 16,054 static
rows, 24,069,379 dynamic rows, and 3,249,815,677 output cells. The data owner
approved promotion under cleaning policy `0.1`.

After deploying and testing the promotion implementation, submit:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_promote_cleaned_release_production.sh
```

The expected result is `PASS`, zero blockers, byte-identical Parquet payloads,
`cleaned_layer_ready=true`, and `derived_input_approved=true`. Derivation and
external export remain disabled. Do not rerun after success; promotion will
not overwrite the immutable release or current pointer.

## Consolidated core-derived contract review

After cleaned release `20260806T114234Z` is current, submit one read-only scan
covering exact elapsed hours, computed driving pressure, conflict-aware
mortality, and observed-support ventilation episodes:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_derivation_contract_review_production.sh
```

The expected result is technical `PASS`, overall `pending_human_review`, and
exactly one human blocker: `core_derived_contract_approved`. The command scans
the cleaned release once and writes reports only. It does not build a derived
candidate, filter rows or stays, calculate/coalesce SOFA or iSOFA, create time
blocks, or authorize external export. Return the sanitized Markdown report for
one consolidated formula and semantics decision.

## Hospital-level driving-pressure semantics review

The consolidated evidence does not support activating one driving-pressure
formula without a hospital-specific check. Submit the focused read-only scan:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_driving_pressure_semantics_review_production.sh
```

The job requests one CPU, 2 GiB RAM, 15 minutes, and `c23ms`. The expected
result is technical `PASS`, overall `pending_human_review`, and exactly one
human blocker: `hospital_scoped_driving_pressure_semantics_approved`. The
sanitized report contains hospital-level aggregate comparisons of reported
driving pressure with both `insp_pressure - peep` and unchanged
`insp_pressure`. No identifier, patient row, formula activation, value mask,
clinical artifact, or release mutation is produced.

## Build and completely audit the core-derived candidate

After approval of core-derived contract `0.1`, submit the bounded-memory build
and independent full audit together:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_core_derived_build_and_audit_production.sh
```

The job requests two CPUs, 4 GiB RAM, 30 minutes, and `c23ms`. It first writes
an immutable run-scoped candidate under `derived_candidates/<run-id>`, then
rescans the cleaned release, recomputes every derived value, and compares every
input and output cell. Expected success is technical `PASS`, overall
`pending_human_review`, and exactly one blocker:
`core_derived_candidate_release_approved`.

Return the sanitized audit Markdown before promotion. The candidate is not a
release, filters nothing, creates no cohort or time block, and remains
unauthorized for external export.

## Promote the approved core-derived release

Candidate and audit run `20260806T170134Z` were explicitly approved under
core-derived contract `0.1`. After deploying the promotion implementation, run
the self-submitting wrapper from a login shell:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_promote_core_derived_release_production.sh
```

The job requests one CPU, 4 GiB RAM, 20 minutes, and `c23ms`. It verifies the
candidate, complete audit, cleaned-release lineage, contract, schemas, row
counts, and hashes before copying the audited Parquet payloads byte-for-byte to
`derived/releases/20260806T170134Z`. It then writes the immutable release
manifest and `derived/current_release.json`.

Expected success is `PASS`, zero blockers,
`audited_parquet_bytes_preserved_exactly=true`,
`core_derived_layer_ready=true`, and `analysis_input_approved=true`. The job
does not rerun cleaning or derivation, create a cohort, apply time blocking, or
authorize external export. Do not rerun it after success: the promotion refuses
to overwrite the release or current pointer.

## Consolidated unresolved-unit and legacy-range audit

After the `0.1` releases are preserved, submit the read-only consolidated unit
review from a login shell:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_unit_resolution_audit_production.sh
```

The job requests one CPU, 4 GiB RAM, 30 minutes, and `c23ms`. It scans the
cleaned dynamic release once, profiles all 72 unresolved-unit variables, and
accounts for the 12 variables that also have a migrated legacy range. Expected
completion is technical `PASS`, overall `pending_human_review`, exit status `2`
inside the wrapper, and exactly two human blockers.

Return the sanitized Markdown report plus the owner-only
`unit_decision_workbook.csv`. The operator supplies no hashes or historical run
IDs: all immutable lineage is frozen in the policy and verified automatically.
No unit is inferred automatically, no range is activated, no value is converted
or masked, and no released clinical artifact is modified.

## Complete candidate unit-decision audit

After deploying `candidate_unit_decisions_0_2.yaml` and the audit
implementation, submit the one consolidated conversion/mask evidence job from
a login shell:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_unit_decision_audit_production.sh
```

The wrapper requests one CPU, 2 GiB RAM, 20 minutes, and `c23ms`. It rescans
the released cleaned static and dynamic Parquet files, computes all 128
hospital-variable profiles for the 16 affected variables, tests all nine
proposed conversions, tests all nine proposed site masks, and audits UK00
vasopressin linkage to static weight. Expected completion is technical `PASS`,
overall `pending_human_review`, exit status `2` inside the wrapper, and exactly
one human blocker.

Return the sanitized Markdown report. The complete owner-only evidence remains
under `asic/reports/production/private/unit_decision_audit/<run-id>/`. The job
does not change a clinical data artifact, activate contract `0.2`, or require a
manually supplied SHA-256 value.

## Review the unit schema/dictionary 0.2 amendment

After deploying the reviewed unit decisions, submit the tiny report-only
schema planner from a login shell:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_unit_schema_amendment_review_production.sh
```

The job requests one CPU, 1 GiB RAM, five minutes, and `c23ms`. It reads no
clinical rows. It verifies frozen contract `0.1`, the corrected unit audit, and
the reviewed decision hashes; then writes the proposed ordered schemas and
variable dictionary privately plus one sanitized report. Expected completion
is technical `PASS`, overall `pending_human_review`, and exactly one human
blocker. Contract `0.2` remains unfrozen and no released artifact is changed.

## Audit medication zero/missing semantics and revise the 0.2 schema review

Before freezing contract `0.2`, submit the combined low-resource checkpoint
from a login shell:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_medication_semantics_and_schema_review_production.sh
```

The job requests one CPU, 2 GiB RAM, 15 minutes, and `c23ms`. It scans the
released cleaned dynamic table once, accounts exactly for null, zero, positive,
negative, and non-finite values across all 25 source medication or therapy
variables and eight hospitals, checks all four medication conversions and all
nine semantic splits, and then regenerates the report-only schema/dictionary
`0.2` proposal with explicit medication value semantics.

Expected completion is technical `PASS` for both commands and one human review
finding in each report. No clinical data are written, no release is modified,
and no conversion, split, carry-forward, schema freeze, or external export is
activated.

## Freeze approved schema and dictionary contract 0.2

After the data owner approves exact amendment review `20260807T101321Z`
(generated at `2026-08-07T10:14:15+00:00`), deploy the approval-bound freeze
implementation, run the full test suite, and execute
the metadata-only wrapper on the login frontend:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/freeze_unit_schema_dictionary_0_2_production.sh
```

Do not submit this command with `sbatch`; it reads no clinical rows and needs no
compute allocation. Expected output is `PASS`, zero blockers, contract version
`0.2`, and both frozen flags set to true. The unit conversions, semantic splits,
and negative-medication rule must all still report `activated=false`.

The immutable output is
`asic/data/production/contracts/harmonized_schema_dictionary/0.2/`. Do not rerun
after success: the wrapper and command deliberately refuse to overwrite the
contract or its review record. Return the sanitized freeze Markdown report
before implementing the harmonized `0.2` clinical build.

## Promote the approved cleaned 0.2 release

Candidate and audit run `20260807T154100Z` were explicitly approved under
cleaning policy `0.2`. After deploying the promotion implementation and
passing the test suite, submit the low-resource immutable promotion job:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_promote_cleaned_0_2_release_production.sh
```

The wrapper requests one CPU, 2 GiB, 20 minutes, and `c23ms`. Expected success
is `PASS`, zero blockers, `current_cleaned_release=20260807T154100Z`, and exact
Parquet and dictionary byte preservation. It snapshots the former current
pointer, preserves cleaned release `20260806T114234Z` and every harmonized and
derived release, and advances only `cleaned/current_release.json`. It applies
no cleaning or derivation and authorizes no external export. Do not rerun after
success because every release and report target is overwrite-protected.

## Build and independently audit core-derived 0.2

After cleaned release `20260807T154100Z` is current, submit the derivation
rebuild from a login shell:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_core_derived_0_2_build_and_audit_production.sh
```

The wrapper requests one CPU, 4 GiB, 30 minutes, and `c23ms`. It preserves the
approved core-derived `0.1` formulas exactly, validates the cleaned `0.2`
release and inherited evidence, appends the same derived variables to the
complete cleaned `0.2` schema, then independently recomputes every output
cell. Expected completion is technical `PASS`, one human promotion blocker,
and `publication_ready=false`. Return the sanitized `derived_0_2_audit`
Markdown report. Do not promote until the data owner approves that exact run.

Candidate `20260808T074305Z` passed and received exact data-owner approval.
Promote it with the low-resource immutable wrapper:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_promote_core_derived_0_2_release_production.sh
```

Expected completion is `PASS`, zero blockers, current derived release
`20260808T074305Z`, and exact candidate-byte and prior-pointer preservation.
The prior derived release remains unchanged. The command reruns no clinical
logic and refuses every existing release, staging, or report target.

## Build and independently audit the exact 8-hour candidate

Contract `0.1` is frozen from evidence run `20260808T114902Z`. The data owner
subsequently authorized implementation of the run-scoped production candidate
and independent audit workflow. The data owner then clarified that execution
must be performed by the owner from deployment instructions, not by the
assistant.

The reviewed execution file authorizes candidate and audit run ID
`20260808T130534Z`. After deploying the complete reviewed change set and
passing the cluster test suite, the data owner may submit:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
export TIME_BLOCKING_8H_CANDIDATE_RUN_ID=20260808T130534Z
export TIME_BLOCKING_8H_AUDIT_RUN_ID=20260808T130534Z
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_time_blocking_8h_candidate_and_audit_production.sh
```

The wrapper requests one CPU, 16 GiB, 24 hours, and partition `c23ms`. It
builds the immutable candidate and then runs the separate direct every-cell
audit. A technically passing audit exits `2`; the wrapper treats that as
expected pending human promotion review.

Return the sanitized audit Markdown report. Do not promote anything. The
workflow creates no release or pointer and verifies that both the core-derived
pointer and any pre-existing blocked pointer remain byte-identical.
