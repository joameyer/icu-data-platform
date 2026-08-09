# Consolidated harmonization audit

## Purpose

This is the single production-scale review gate after the candidate
harmonization dry run. It replaces a sequence of small unit, distribution,
schema, and legacy-QC audits with one immutable, streaming job. It reads the
candidate, its lossless ingested parents, the private parser accounting, and
the v3-owned cross-hospital quality policy. It writes reports only and has no
runtime dependency on an older project generation.

The expected result is `FAIL` with zero technical blocking findings. Human
findings remain until units, hospital-specific scale/definition differences,
categorical domains, semantics, ordered schemas, and the variable dictionary
are explicitly approved.

## Complete audit domains

The job performs the following checks in one run:

- candidate and ingested SHA-256, row, schema, and provenance conservation;
- local/global stay-ID construction, static uniqueness, and dynamic-to-static
  stay coverage;
- complete dry-run parser accounting and unresolved-token counts;
- full numeric scalar and numeric-list profiles for all hospitals;
- categorical domains and list-shape profiles;
- one unit-policy row for every numeric/list variable and every nonnumeric
  field whose unit remains pending or unresolved;
- hospital median-scale and distribution screens, with power-of-ten factors
  treated only as hypotheses;
- every selectively migrated invalid-range, row-scale, targeted-scale,
  targeted-comparison, bucket, relationship, availability, missing-sentinel,
  contextual-cleaning, and semantic rule, revalidated against v3 candidate
  data;
- the UK00 PBW tidal-volume hypothesis against every plausible preserved
  height measurement, without choosing a height aggregation;
- optional row, stay, availability, and numeric-distribution comparisons with
  explicitly configured older-version artifacts. These comparisons are
  disabled by default and may be absent without blocking the audit.

All exact protected evidence remains in the owner-only private report bundle.
The review report contains canonical variable names and aggregate counts only.

## Read-only boundary

The audit does not infer or apply a physical unit from distributions. It does
not convert, mask, clean, filter, concatenate, derive, or publish data. If an
older-version comparison is explicitly enabled, it remains untrusted context
and never becomes a v3 input or prerequisite.

## Cluster invocation

Run the wrapper with `bash` from a login shell, not with `sbatch`:

```bash
export HARMONIZATION_RUN_ID=<completed-candidate-run-id>
bash asic/slurm/run_consolidated_harmonization_audit_production.sh
```

The wrapper submits one 30-minute, 8-GiB, two-CPU job on `c23ms`, creates an
immutable audit run ID, treats exit code 2 as a successful technical run with
human-review findings, and refuses to overwrite an existing report. Worker
thread pools are capped at the allocated CPU count.

## Review outputs

The owner-only directory contains a decision register plus workbooks for
units, scale signals, distributions, invalid/range candidates, targeted
legacy checks, relationships, availability, sentinels, list structures,
identifier integrity, optional older-version comparisons, and the complete
candidate dictionary.
The sanitized Markdown report summarizes coverage and review priorities.

The next implementation step is permitted only after the technical findings
are zero and the human decisions needed for the harmonized contract are
recorded. Cleaning candidates remain separate decisions for the cleaned layer.

## Approved partial disposition

Decision file
`asic/config/harmonization/reviewed_consolidated_decisions_0_1.yaml` records the
data-owner review of candidate run `20260806T074852Z` and technically complete
audit run `20260806T092818Z`. It approves named units, four hospital-specific
conversions, separate-variable semantic policies, preservation of four global
all-missing columns, and deferral of physiologic ranges and row-level scale
corrections to cleaning. Unknown units remain preserved without conversion and
are marked analysis-ineligible.

The decision file does not retroactively alter the immutable candidate. It is
an input to the next candidate/release-building implementation only after the
remaining categorical and schema/dictionary gates close. See
`categorical_contract_review.md` for the next bounded review.

Targeted scale and bucket rules now carry an explicit table scope. Future audit
decision counts are rule-based rather than multiplied by hospital evidence;
the full hospital rows remain in the private evidence tables.
