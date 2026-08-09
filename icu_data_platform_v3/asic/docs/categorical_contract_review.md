# Categorical contract review

## Purpose

This checkpoint turns the categorical portion of consolidated audit run
`20260806T092818Z` into one bounded human-review bundle. It does not scan the
candidate Parquet files again. It verifies the immutable consolidated manifest
and hashes, then reads only three report artifacts:

- `categorical_domains.parquet`;
- `list_profiles.parquet`; and
- `candidate_variable_dictionary.parquet`.

Exact observed values remain in an owner-only workbook. The sanitized report
contains canonical names, domain sizes, hospital counts, and review status but
no category tokens.

## Prior decisions retained

The checkpoint carries forward the already reviewed raw-v3 contracts for:

- nullable Boolean `hospital_mortality_reported`;
- Boolean `icu_readmit`;
- `study_implementation_phase`; and
- the fixed-position two-element Boolean list
  `therapy_read_confirmation_utc`.

Every other observed scalar categorical domain remains pending. The first
priority review group is `ards_diagnosis_app`, `ecmo`, and `position_therapy`.
An observed complete domain is not by itself approval of its clinical meaning
or canonical mapping.

## Cluster invocation

This is a small report-only operation and should run on the login frontend; no
Slurm allocation is needed:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
unset SLURM_JOB_ID SLURM_STEP_ID

.venv/bin/asic-pipeline review-categorical-contract \
  --config asic/config/datasets/production.yaml \
  --consolidated-audit-run-id 20260806T092818Z
```

Exit status `2` is expected while categorical decisions remain pending. Exit
status `1` means immutable evidence is missing, changed, truncated, or otherwise
technically invalid and must not be bypassed.

The command prints both report locations. Review the sanitized Markdown first,
then use the owner-only `categorical_domain_review.parquet` to inspect exact
values and counts. Do not copy that workbook off the authorized cluster or
commit it.

## Boundary

The command reads no clinical candidate rows and writes no data artifact. It
does not apply translations, freeze the ordered static/dynamic schemas, approve
the variable dictionary, or permit publication. Those remain explicit later
gates.

## Reviewed outcome

Review run `20260806T100728Z` completed with zero technical blockers and no
truncated domain. Complete categorical contract `0.1` records the approved
outcome:

- preserve integral ARDS codes as nullable `int32`; retain the nonstandard UK00
  code domain exactly and mark the variable analysis-ineligible;
- translate ECMO numeric notations to nullable Boolean and the reviewed UK03
  textual missing token to null;
- represent position therapy as nullable `float64` dimensionless proportion,
  preserving reviewed fractional values exactly;
- preserve severity read confirmation as an explicitly all-missing,
  analysis-ineligible field;
- convert only UK01 height-group `-1` to null; and
- retain the reviewed static categorical vocabularies and hospital-scoped
  cluster domains.

Unknown codes, new values, wrong-hospital sentinels, and new hospital-domain
combinations remain blocking. Existing candidate data are not rewritten by
this approval.
