# ASIC v3 cleaned 0.2 candidate and complete audit

## Approved input and boundary

Cleaning policy `0.2` reads only immutable harmonized release
`20260807T112402Z` under frozen harmonized contract `0.2`. It does not alter
that release, the earlier harmonized `0.1` release, cleaned release `0.1`, or
core-derived release `0.1`.

The build writes a new run-scoped candidate under
`asic/data/production/cleaned_0_2_candidates/<run-id>/`. It preserves every
row, stay, column, ordered schema field, and provenance value. It creates no
derived variables, cohort, aggregation, or time block and does not advance a
release pointer.

## Cleaning rules

The builder first replays all applicable cleaning `0.1` rules unchanged,
including non-finite masking, reviewed power-of-ten recovery, height-list
element cleaning, and the UK06 mixed-scale mask. The 12 formerly deferred
unit-dependent rules are replaced by the data-owner-approved `0.2` matrix:

- albumin, hemoglobin, and platelets use reviewed open/closed domains;
- creatinine masks nonpositive values but retains positive extremes;
- EVLWI repairs an upper value only when exactly one approved power-of-ten
  factor produces a value in `(0, 80]`; ambiguous or unrecoverable values are
  null;
- GEDVI, INR, arterial lactate, PTT, and SVRI mask nonpositive values while
  preserving and reporting uncertain positive upper tails;
- unspecified SOFA requires an integer in `[0, 24]`;
- SOFA without GCS requires an integer in `[0, 20]` if future values appear.

Across all 25 source medication/therapy variables and all nine parallel
semantic-split targets, every finite negative value becomes null. Explicit
numeric zero remains zero. Null remains unavailable or unobserved and is never
imputed to zero or interpreted as inactive treatment.

The implementation is evidence-bound to range audit `20260807T133609Z`. Its
raw inherited-rule metric is 10,565, of which 544 are valid zero-valued SOFA
scores incorrectly treated as violations by the legacy rule. The approved
policy retains those scores. Directionally actionable accounting therefore
includes 9,597 priority zero masks, 50 priority negative masks, 32
unique EVLWI corrections by factor `0.001`, 10 ambiguous EVLWI masks, 332
preserved positive upper-tail values, and 201 negative medication masks. The
number of albumin values above the new broad 1000 dg/L ceiling is recomputed
and reported rather than assumed in advance.

## Cleaned variable dictionary

The candidate contains both:

- `cleaned_variable_dictionary.parquet`, for programmatic use; and
- `cleaned_variable_dictionary.md`, for human review.

They preserve the frozen harmonized `0.2` definitions, units, eligibility,
hospital availability, caveats, and source provenance. Cleaning-specific
columns append the policy version, ordered rule IDs, cleaned valid domain,
rule note, candidate status, and policy source. The dictionary explicitly
distinguishes harmonization assumptions from cleaning actions.

## Independent audit and review gate

The audit performs a second full streaming pass. It independently recomputes
every output cell and rule count, verifies input and candidate hashes before
and after use, checks schemas and row counts, and reconstructs both dictionary
artifacts. It fails if any governed non-finite, invalid-range, invalid-score,
or negative-medication value remains.

The expected result is technical `PASS`, overall `PENDING HUMAN REVIEW`, and
one blocker: `cleaned_0_2_candidate_release_approved`. This is intentional.
Candidate `20260807T154100Z` passed that audit, and the data owner explicitly
approved its promotion under cleaning policy `0.2` on 2026-08-08.

## Production command

After deployment and a passing test suite, run the self-submitting wrapper
from a login shell:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_cleaning_0_2_build_and_audit_production.sh
```

The wrapper requests `c23ms`, one CPU, 4 GiB, and 40 minutes. It validates all
immutable inputs before submission, refuses existing candidate/staging paths,
builds the candidate, and treats audit exit `2` as the expected human-review
state. Return the sanitized audit Markdown before any promotion step.

## Promote the approved cleaned 0.2 candidate

After deploying the approval-bound promotion implementation and passing the
complete test suite, run the self-submitting low-resource wrapper from a login
shell:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_promote_cleaned_0_2_release_production.sh
```

The job requests `c23ms`, one CPU, 2 GiB, and 20 minutes. Before writing, it
verifies the exact candidate manifest, complete audit, immutable policies,
harmonized-release lineage, schemas, row counts, file hashes, both cleaned
variable dictionaries, prior cleaned release, and current pointer. It copies
the audited static, dynamic, Parquet-dictionary, and Markdown-dictionary bytes
unchanged into `cleaned/releases/20260807T154100Z`, stores the exact prior
pointer bytes in that release, and atomically advances
`cleaned/current_release.json`.

Expected success is `PASS`, zero blockers, release ID `20260807T154100Z`,
`audited_parquet_and_dictionary_bytes_preserved=true`,
`cleaned_layer_ready=true`, and `derived_input_approved=true`. Candidate
artifacts, cleaned release `20260806T114234Z`, all harmonized releases, and
core-derived release `20260806T170134Z` remain unchanged. Promotion does not
rerun cleaning, run derivation, filter data, or authorize external export. It
refuses to overwrite any release, staging directory, or promotion report, so
do not rerun it after success.
