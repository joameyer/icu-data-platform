# ICD-10 component audit

## Purpose

Notation-audit run `20260805T131735Z` established that all non-null source
cells are strings: 16,022 contain comma-delimited values, seven are single-code
candidates, and 24 UK03 cells contain the reviewed textual-missing token
`nan`. Contract `reviewed_icd10_contract_0_1.yaml` approves that UK03 token as
missing and approves exact preservation of every other non-empty source cell
as nullable `icd10_codes_source_text`.

The component audit is the final read-only evidence step before deciding
whether to activate `icd10_codes` as an ordered `list<large_string>`. It does
not create either field.

## Candidate grammar

For each non-missing cell, the audit:

- splits on the literal comma delimiter;
- strips whitespace only at each component boundary;
- represents a source cell without a comma as a one-element candidate list;
- preserves source order, duplicates, case, and component punctuation;
- blocks empty components caused by leading, trailing, or repeated commas;
- counts code-candidate and unexpected component shapes without assigning
  diagnosis semantics; and
- proves row-, cell-, and component-level accounting invariants.

Exact unexpected components, if any, are bounded and written only to the
owner-only private bundle. Sanitized reports expose hospital-level counts,
list-length distributions, whitespace counts, and duplicate counts without raw
tokens, headers, filenames, or identifiers.

## Inputs and safeguards

The audit is bound to:

- notation-audit run `20260805T131735Z`;
- static-contract run `20260805T112746Z`;
- reviewed ICD-10 contract `0.1` and its exact SHA-256; and
- the immutable ingestion manifest and static Parquet hash for each hospital.

Only the selected static ICD-10 field is streamed. The audit does not read raw
CSV files or other clinical columns, transform source values, activate a
parser, concatenate hospitals, or write under `asic/data/`.

## Outputs and review gate

Owner-only evidence is written under:

```text
asic/reports/<context>/private/icd10_component_audit/<run_id>/
├── icd10_component_audit_manifest.json
├── component_patterns.parquet
├── examples.parquet
└── README.md
```

The sanitized JSON and Markdown are written under the matching
`review/icd10_component_audit/` directory. Exit status `2` is expected when all
technical checks pass because the complete component evidence and parser
activation still require explicit human approval. Exit status `1` is a
technical failure and must not be bypassed.
