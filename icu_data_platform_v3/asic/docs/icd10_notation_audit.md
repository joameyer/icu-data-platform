# ICD-10 notation audit

## Purpose

This audit inventories how the eight hospitals encode the static ICD-10 source
cell before deciding whether a common parsed representation is possible. It is
read-only and does not validate diagnoses, parse codes, or transform values.

The immediate proposal is to preserve each exact non-empty source cell as
nullable `icd10_codes_source_text`. A canonical `icd10_codes` field remains
deferred until the notation evidence supports a deterministic, lossless parser.

## Input and scan boundary

The audit is bound to static-contract run `20260805T112746Z` and reviewed
partial static registry `0.3`. It uses the private occurrence workbook to
select exactly one physical ICD-10 source field per hospital, revalidates the
field’s raw-name and occurrence metadata, checks each static Parquet hash
against its ingestion manifest, and streams only that selected string column.

It never reads identifiers, other clinical columns, or raw CSV files.

Every row is classified as source null, literal empty, or non-empty. Non-empty
cells receive deterministic syntax-feature signatures describing characters
such as delimiters, brackets, quotes, whitespace, and whether the entire cell
resembles one ICD-10 code. Candidate textual-missing spellings are counted but
not converted to null.

## Protected evidence

Exact source strings appear only in bounded owner-only examples:

```text
asic/reports/<context>/private/icd10_notation_audit/<run_id>/
├── icd10_notation_audit_manifest.json
├── patterns.parquet
├── examples.parquet
└── README.md
```

The sanitized JSON and Markdown contain hospital IDs, syntax signatures, and
counts only. Bounded-example truncation is informational at this stage because
all cells still contribute to a deterministic signature; it does not approve a
future parser.

## Human-review gate

Exit status `2` is expected when all technical checks pass. Before approving a
representation, review:

- candidate textual-missing counts and private examples;
- hospital-specific delimiter and container patterns;
- whether exact source text should be retained as
  `icd10_codes_source_text`; and
- whether canonical `icd10_codes` parsing should remain deferred or proceed to
  a separately audited grammar.

Exit status `1` indicates a technical blocker and must not be bypassed. No
harmonized, cleaned, derived, concatenated, or pooled artifact is generated.
