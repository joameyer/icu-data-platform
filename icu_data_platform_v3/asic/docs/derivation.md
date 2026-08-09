# ASIC v3 core derived contract

## Stage boundary

The derived stage reads only the immutable current cleaned release. It never
modifies harmonized or cleaned artifacts. The first checkpoint is one
consolidated read-only scan covering every proposed core recipe; it writes
reports only and creates no derived clinical data.

The core proposal contains four recipes:

1. `hours_since_icu_admission` is exactly
   `minutes_since_icu_admission / 60`. The minute field remains available.
2. `delta_p_computed` is `insp_pressure - peep`. The source inputs and
   `delta_p_reported` remain separate. The review reports all computed values
   outside the candidate 0–60 cmH2O range before a null policy is frozen.
3. `hospital_mortality` and `icu_mortality` are nullable outcomes derived from
   `death_status`, `hospital_mortality_reported`, and `discharge_status`.
   Hospital and ICU implications are resolved independently. Conflicting
   evidence produces a missing outcome plus its corresponding source-conflict
   flag; no source silently wins.
4. An observed-support ventilation summary uses non-missing `fio2`, `peep`,
   `vt`, or `vt_per_kg_ideal_body_weight` at the direct cleaned timestamps.
   Timestamps are sorted within each contiguous stay before adjacent supported
   timestamps separated by at most eight hours form an episode. An episode
   duration of at least 24 hours satisfies the proposed flag. The proposed ICU
   flag excludes negative pre-admission time while preserving those source
   rows and reports the counterfactual result with negative time included.
   This is explicitly a proxy based on observed markers, not a claim of
   ground-truth ventilation and not a cohort exclusion.

The proposed static additions also retain supported-timestamp count, episode
count, maximum observed episode duration, and maximum dynamic recording extent.

## Driving-pressure semantic checkpoint

The consolidated review showed that `insp_pressure - peep` is not uniformly
supported by the released cleaned data: 3,625 computed values are below zero,
11 exceed 60 cmH2O, and only 839,792 of 3,231,499 rows with a separately
reported driving pressure agree within 0.01 cmH2O. These findings do not prove
a unit error. They may indicate hospital-specific use of absolute inspiratory
pressure versus pressure above PEEP.

Before a computed driving-pressure formula is activated, the dedicated
read-only semantics review compares, for every hospital:

- `delta_p_reported` against `insp_pressure - peep`;
- `delta_p_reported` against unchanged `insp_pressure`;
- negative and above-range subtraction results;
- exact mutually exclusive agreement classes; and
- mean and maximum absolute errors at 0.01, 0.1, and 1 cmH2O tolerances.

The review writes aggregate evidence only. It does not choose a formula,
change the cleaned release, mask a value, or build derived clinical data. A
systematic hospital-specific definition belongs in harmonization; isolated
implausible combinations belong in cleaning.

## Explicit separations

- Computed and reported driving pressure remain separate.
- Existing SOFA and iSOFA variants remain separate and unchanged; the core
  recipe neither coalesces them nor manufactures a new score.
- Mortality source variables remain present alongside derived outcomes.
- No row or stay is filtered.
- No analysis cohort is generated.
- Time blocking is deferred to a named recipe with its own resolution,
  interval, and aggregation contract.

## Consolidated production review

The review validates the current cleaned pointer, manifest, hashes, schemas,
row counts, and release authorization. It then streams all static and dynamic
rows and reports:

- identifier and stay-set conservation;
- complete mortality domains, evidence combinations, candidate outcomes, and
  conflicts;
- exact time availability, negative times, duplicates, within-stay source
  order, and sorted episode construction;
- driving-pressure input availability, candidate-range results, and comparison
  with the reported field; and
- ventilation marker availability, all-time and nonnegative-ICU-time episode
  counts, changed 24-hour flags, missing times, hospital summaries, and
  source-order evidence.

No filenames, stay identifiers, or patient rows are written to the sanitized
report. The expected technical result is `PASS` with one human blocker:
`core_derived_contract_approved`. One approval or revision covers all four
recipes together.

Run from the login shell:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_derivation_contract_review_production.sh
```

The wrapper requests one CPU, 4 GiB RAM, 30 minutes, and `c23ms`. Exit status
`2` is the expected successful human-review state. A technical blocker returns
`1` and must not be bypassed.

After the single contract approval, the next implementation will build and
fully audit one run-scoped derived candidate. Promotion will remain a separate
final human gate.

The driving-pressure checkpoint must be resolved first:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_driving_pressure_semantics_review_production.sh
```

The wrapper requests one CPU, 2 GiB RAM, 15 minutes, and `c23ms`. Exit status
`2` is the expected technically passing state pending one semantic decision.

## Approved core-derived contract 0.1

The data owner approved contract `0.1` after driving-pressure review
`20260806T161232Z`. The ASIC protocol defines driving pressure as end-
inspiratory pressure minus PEEP, so the formula applies to every hospital.
Computed values outside 0–60 cmH2O are set missing and receive a true
`delta_p_computed_out_of_range` flag; the flag is null when either input is
missing. Reported driving pressure and both inputs remain unchanged.

Mortality uses all reviewed implications without source priority. Hospital and
ICU outcomes are resolved separately, and each has its own conflict flag. A
conflicting outcome becomes missing; a source that does not determine ICU
mortality contributes no ICU implication. The complete observed production
evidence contains no conflicts, but the behavior remains executable.

The ventilation summary counts support-marker timestamps at all elapsed times,
while episode construction uses sorted nonnegative ICU-relative timestamps.
Gaps of eight hours remain connected and 24 hours qualifies inclusively.
Negative rows remain in the dynamic output. The flag remains an observed-
support proxy, never a cohort filter or a ground-truth label.

Build and independently audit one immutable candidate together:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_core_derived_build_and_audit_production.sh
```

The combined job requests two CPUs, 4 GiB RAM, 30 minutes, and `c23ms`.
Successful completion ends with a technically passing audit and exactly one
human promotion gate. No release, cohort, time block, or external export is
created.

## Approved core-derived release 20260806T170134Z

## Core-derived 0.2 rebuild from cleaned 0.2

Cleaned release `20260807T154100Z` is the approved derivation input for the
next version. Core-derived contract `0.2` inherits every recipe, definition,
threshold, separation, and deferral from reviewed contract `0.1` with zero
clinical-formula changes and zero output-semantic changes. The version change
exists to bind derivation to cleaned contract `0.2` and to preserve its full
28-column static and 144-column dynamic input schemas, including the nine
reviewed medication semantic-split variables.

The bounded build appends the same nine static and three dynamic derived
fields. The independent audit rereads cleaned `0.2`, recomputes every derived
value, and compares every input and output cell. Expected clinical accounting
remains unchanged: 24,069,379 elapsed-hour values, 3,636 masked-and-flagged
computed driving pressures, 4,726 hospital deaths, 2,146 ICU deaths, 53,535
observed-support episodes, and 12,775 stays with an episode lasting at least
24 hours. Any difference blocks the run rather than silently changing the
contract.

Run the self-submitting low-resource wrapper from a login shell:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_core_derived_0_2_build_and_audit_production.sh
```

The job requests `c23ms`, one CPU, 4 GiB, and 30 minutes. Expected completion
is technical `PASS`, overall `PENDING HUMAN REVIEW`, and exactly one human
blocker: `core_derived_candidate_release_approved`. It writes the candidate
under `derived_0_2_candidates/<run-id>` and sanitized reports under
`derived_0_2_build` and `derived_0_2_audit`. It does not change the cleaned
release, current derived pointer, derived `0.1` release, cohort, time blocks,
or external-export authorization. Return the complete audit report before any
promotion implementation.

Candidate `20260808T074305Z` subsequently passed the complete audit over
16,054 static rows, 24,069,379 dynamic rows, and 3,538,792,711 output cells.
The data owner explicitly approved its promotion under contract `0.2`.

## Promote the approved core-derived 0.2 release

After deploying the approval-bound promotion implementation and passing the
test suite, run:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_promote_core_derived_0_2_release_production.sh
```

The job requests `c23ms`, one CPU, 2 GiB, and 20 minutes. It validates the
exact candidate, full audit, contract, cleaned-release lineage, schemas, row
counts, hashes, prior derived release, and current pointer. It copies the two
audited Parquet files byte-for-byte to
`derived/releases/20260808T074305Z`, stores the exact prior pointer bytes in
the new release, and atomically advances `derived/current_release.json`.

Expected success is `PASS`, zero blockers, exact Parquet-byte preservation,
`core_derived_layer_ready=true`, and `analysis_input_approved=true`. Candidate
artifacts and release `20260806T170134Z` remain unchanged. Promotion reruns no
derivation or cleaning, generates no cohort or time block, and authorizes no
external export. Do not rerun after success; every output is overwrite
protected.

The complete audit of candidate `20260806T170134Z` passed over 16,054 static
rows, 24,069,379 dynamic rows, and 3,322,168,300 output cells. Every input
column and every independently recomputed derived value matched. The data owner
explicitly approved promotion under contract `0.1`.

Promotion copies the two audited Parquet payloads byte-for-byte into
`derived/releases/20260806T170134Z`, writes a release manifest, and creates
`derived/current_release.json`. It preserves the immutable candidate and does
not rerun cleaning or derivation, filter rows or stays, create a cohort, or
apply time blocking. The release is approved as internal analysis input;
external data export remains unauthorized.

## Promote the approved 8-hour time-blocking release

Candidate and audit run `20260808T130534Z` passed the independent audit over
16,054 stays, 24,069,379 assigned source rows, 777,756 blocked rows, and
825,976,872 feature cells with zero technical blockers. The data owner
explicitly approved immutable promotion under time-blocking contract `0.1`.

After deploying the approval-bound implementation and passing the test suite,
run from a login shell:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_promote_time_blocking_8h_release_production.sh
```

The job requests `c23ms`, one CPU, 4 GiB, and 30 minutes. It validates the
exact candidate-manifest hash, candidate files, build evidence, independent
audit, frozen contract, prior execution authorization, core-derived lineage,
and absence of a prior 8-hour release pointer. It copies the four audited
payloads and candidate manifest byte-for-byte into
`derived/time_blocking/8h/releases/20260808T130534Z`, writes a release
manifest, and atomically creates only
`derived/time_blocking/8h/current_release.json`.

The static table remains referenced through core-derived release lineage and
is neither copied nor repeated. Promotion preserves the candidate and leaves
`derived/current_release.json` plus both core-derived Parquet files unchanged.
It reruns no blocking or derivation, filters no rows or stays, creates no
cohort, applies no carry-forward or imputation, emits no medication dose
totals, and authorizes no external export. The wrapper refuses an existing
release, staging directory, promotion report, or blocked current pointer; do
not rerun it after success.

Candidate Parquet validation during promotion is deliberately byte-bound and
metadata-only: the exact approved candidate-manifest SHA-256 binds every
audited row count and schema hash, and each candidate payload is streamed
through SHA-256 before copying. Promotion does not reopen the extremely wide
blocked Parquet footer, whose row-group-by-column metadata is unnecessary once
the independently audited manifest and payload hashes are frozen.
