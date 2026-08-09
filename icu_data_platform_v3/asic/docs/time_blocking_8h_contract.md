# 8-hour time-blocking contract 0.1

Contract `0.1` is frozen against evidence run `20260808T114902Z`. The data
owner approved the contract with medication dose totals deferred, separately
authorized implementation of the production workflow, and then clarified that
the data owner may deploy and execute exact candidate/audit run
`20260808T130534Z`. The assistant must not access the cluster. Release
promotion and pointer modification remain unauthorized.

## Time index

The recipe is aligned to exact ICU admission and uses signed block indices.

- Negative observations use `floor(minutes / 480)`: index `-1` is `[-8,0)`,
  index `-2` is `[-16,-8)`, and so on. Every negative row is retained.
- Index `0`, labelled `0h`, is the singleton `{0}`. A `0h` row is emitted for
  every source stay even when the stay has no exact-admission observation.
- Positive observations use `ceil(minutes / 480)`: index `1`, labelled `8h`,
  is `(0,8]`; index `2`, labelled `16h`, is `(8,16]`; and so on. A value at
  exactly hour 8 therefore belongs to `8h`.
- Empty intervening blocks and the terminal partial block are retained.

`block_start_h` and `block_end_h` are nominal grid endpoints.
`available_through_h` and `information_cutoff_h` equal the observed recording
extent in a terminal partial block and otherwise equal the nominal endpoint.
`is_full_by_recording_extent_proxy` is deliberately null for pre-admission and
admission rows. For positive blocks it means only that the maximum observed
dynamic time reaches the nominal endpoint; it does not establish ICU
discharge, continuous observation, or clinical completeness.

The anchored block times use the artificial origin `2020-01-01 00:00:00` and
have no calendar meaning. No prediction-time field is emitted.

## Aggregation registry

The exact 147-field registry from the evidence policy is required. A physical
data type alone never selects an aggregation. Key, time, and operational
provenance fields remain in row and lineage accounting but do not become
features. Analysis-ineligible and conditionally ineligible fields are recorded
in the dictionary but do not become blocked features.

- Numeric measurements: count, mean, median, minimum, maximum, and last
  observation value/time/age.
- Medications and therapies: observed count, zero count, positive count, mean,
  median, minimum, maximum, and last observation value/time/age. Dose totals,
  administration counts, and rate integration are deferred and prohibited.
- Scores: count, median, minimum, maximum, and last observation
  value/time/age. SOFA and iSOFA variants remain separate.
- `fluid_balance_24h`: count and last observation value/time/age only. It is an
  already rolling 24-hour value, so summing it would double-count overlapping
  windows; mean also remains unapproved.
- Booleans: count, true count, false count, any true, all true, and last
  observation value/time/age.
- `position_therapy`: numeric proportion summaries without boolean coercion.
- `therapy_read_confirmation_utc`: component-specific nullable-boolean
  summaries plus counts for every exact two-position nullable pair state.

`last_observation_time_h` is the ICU-relative time of the final non-missing
observation in that block. Ties are resolved by ascending hospital-scoped
`__v3_source_order`, selecting the greatest source order. The corresponding
`last_observation_age_h` is `information_cutoff_h -
last_observation_time_h`. It is null when the block has no non-missing value.
No prior-block value participates.

Null means unavailable or unobserved and is never converted to zero. Observed
numeric zero and observed false remain observations. No carry-forward,
interpolation, imputation, cohort filter, or row filter is applied.

## Candidate artifacts

The policy-driven engine is resolution-general; the only frozen policy shipped
at this gate is `8h`. A candidate is written immutably under:

```text
asic/data/<context>/derived/time_blocking/8h/candidates/<run_id>/
├── blocks.parquet
├── stay_block_summary.parquet
├── blocked_variable_dictionary.parquet
├── blocked_variable_dictionary.md
└── candidate_manifest.json
```

Build evidence belongs under
`asic/reports/<context>/{private,review}/time_blocking/8h/build/`; independent
audit evidence belongs under the equivalent `audit/` paths. No `releases/`
directory or `current_release.json` is created by this workflow.

The manifest references the exact core-derived static table and its release
lineage; static bytes are not copied and static values are not repeated on
blocks. It also binds the frozen harmonized `0.2` source dictionary. The five
non-clinical operational provenance definitions are reconstructed from their
reviewed inventory and ingestion semantics because they are intentionally not
clinical-dictionary rows. Each blocked dictionary row contains its exact
source definition and unit,
operation, output name/type/unit, interval and missingness semantics,
eligibility, caveat, and release/contract lineage.

The build scans Parquet in Arrow batches, buffers at most one contiguous stay,
and writes bounded output batches. The independent audit uses different batch
sizes and directly recomputes every output cell without calling the build
aggregator.

## Execution gates

Local smoke tests use the demo implementation policy:

```bash
.venv/bin/asic-pipeline build-time-blocking-candidate \
  --config asic/config/datasets/demo.yaml \
  --resolution 8h \
  --run-id <run_id>

.venv/bin/asic-pipeline audit-time-blocking-candidate \
  --config asic/config/datasets/demo.yaml \
  --resolution 8h \
  --build-run-id <run_id> \
  --run-id <audit_run_id>
```

A technically passing audit exits `2` because exact candidate promotion still
requires human approval.

The production workflow policy references the reviewed exact-run file
`reviewed_8h_production_execution.yaml`. Before reading production evidence or
the core-derived release, both production CLI paths require this file to bind:

- the exact candidate and audit run IDs;
- the frozen contract hash and production workflow-policy hash;
- core-derived release `20260808T074305Z`; and
- permission only to build that candidate and run that audit.

Promotion, pointer modification, core-derived modification, cohort filtering,
carry-forward, imputation, and external export must remain false. The Slurm
workflow is implemented at
`asic/slurm/run_time_blocking_8h_candidate_and_audit_production.sh`. Only the
data owner may deploy and submit it, using candidate and audit run ID
`20260808T130534Z`.
