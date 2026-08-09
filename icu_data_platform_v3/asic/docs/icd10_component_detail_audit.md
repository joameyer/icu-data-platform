# ICD-10 component-detail audit

## Purpose

Component-audit run `20260805T134245Z` found 26 empty components, one
non-empty component outside the bounded code-candidate grammar, and 96,505
duplicate occurrences across 9,012 source cells. The data owner reviewed the
single unexpected component privately and classified it as an incomplete
input-error candidate. Its exact value remains owner-only.

The component-detail audit provides the remaining evidence required to choose
explicit canonical-list policies. It does not drop empty components, remove
duplicates, remove the incomplete component, or activate the parser.

## Evidence produced

For every hospital, the audit reports sanitized counts for:

- leading, trailing, and interior empty components;
- cells that would become empty after empty-component removal;
- adjacent and separated duplicate occurrences;
- maximum within-cell component multiplicity;
- component counts before and after empty removal;
- counterfactual counts after first-occurrence ordered deduplication; and
- list-length distributions before and after both candidate operations.

Bounded exact source strings containing empty components and the exact
incomplete component are written only to the owner-only private bundle. No
identifier or filename is read or reported.

## Immutable inputs

The audit is bound to:

- component-audit run `20260805T134245Z` and its reviewed aggregate counts;
- static-contract run `20260805T112746Z`;
- reviewed ICD-10 contract `0.1` and its exact SHA-256; and
- all eight immutable static ingestion manifests and Parquet hashes.

Only the selected static ICD-10 source fields are streamed. The audit does not
read raw CSVs or other clinical columns and writes no data artifact.

## Human-review gate

The final review must choose:

- whether empty components are retained as null elements or removed as
  delimiter artifacts;
- whether duplicate components are preserved or deduplicated while retaining
  first-occurrence order; and
- whether the incomplete component remains unresolved or is removed under a
  reviewed rule.

Exit status `2` is expected when technical checks pass because those choices
and parser activation remain blocked. Exit status `1` indicates a technical
failure and must not be bypassed.

## Reviewed outcome

Run `20260805T135859Z` passed all technical checks. The data owner subsequently
approved removal of reviewed trailing empty components, first-occurrence
ordered deduplication, and owner-only-evidence-bound removal of the single
incomplete component. Those decisions are encoded in reviewed ICD-10 contract
`0.2`; this audit remains immutable evidence and is not rewritten.
