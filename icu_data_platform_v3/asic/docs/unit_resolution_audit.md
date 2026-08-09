# Unresolved-unit and legacy-range audit

## Purpose

This checkpoint consolidates the remaining unit work after the harmonized,
cleaned, and core-derived `0.1` releases. It profiles all 72 dynamic variables
whose frozen dictionary unit is `unresolved`, and it separately accounts for
the 12 unresolved-unit variables that also have a migrated legacy range rule.

The command is an evidence generator, not a transformation. It does not infer
a physical unit from distributions, activate a legacy range, convert or mask a
value, update a contract, rebuild a release, or authorize export.

## Immutable inputs

The audit is bound to:

- cleaned release `20260806T114234Z` and its current pointer;
- frozen harmonized schema and dictionary contract `0.1`;
- the exact raw-name-to-canonical occurrence plan from harmonization dry-run
  `20260806T074852Z`;
- the prior unit and numeric profiles from consolidated harmonization audit
  `20260806T092818Z`; and
- the v3-owned cross-hospital quality registry.

Hashes and row counts are checked by the command. They are not supplied by the
operator.

## Evidence produced

One streaming pass over the cleaned dynamic table produces complete counts and
bounded deterministic profiles for each hospital-variable pair. Owner-only
outputs include:

- `unit_decision_workbook.csv` and `.parquet`, one row per unresolved variable;
- hospital-level numeric profiles;
- exact source-mapping evidence, including raw headers;
- cross-hospital power-of-ten and distribution screening findings;
- hospital-level legacy-range profiles for the 12 priority variables; and
- an aggregate priority legacy-range summary.

The sanitized review contains only aggregate counts. It excludes patient rows,
stay identifiers, raw tokens, exact raw headers, and filenames.

## Decision order

For every variable, first establish the definition and unit from source-system
documentation or a knowledgeable data owner. Distribution evidence may flag a
hypothesis but cannot prove a unit. Record one unit disposition:

- confirm one shared unit;
- approve an explicit hospital-specific conversion;
- confirm a dimensionless or score representation;
- retain unresolved and analysis-ineligible; or
- classify the source as mixed or unrecoverable.

Only after that decision may a legacy-range target receive a separate range
disposition: activate the old range, replace it with a reviewed unit-specific
range, preserve it as audit-only, apply a narrowly scoped unrecoverable-value
mask, or retire the range as inapplicable.

Any approved conversion requires harmonized contract `0.2` and downstream
rebuilds. A range-only change requires cleaning policy `0.2` and downstream
rebuilds. Existing `0.1` releases remain immutable.

## Expected first-run result

Technical status should be `PASS`; overall status should be
`PENDING HUMAN REVIEW` with exactly two human blockers. The intended aggregate
legacy-range count is 10,021 preserved values across the 12 priority variables.
No clinical artifact is generated or modified.
