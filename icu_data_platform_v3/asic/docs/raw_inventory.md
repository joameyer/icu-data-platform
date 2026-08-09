# Phase 1 raw-file inventory

## Purpose

The inventory is read-only with respect to the source. It establishes what is
present and collects evidence needed to approve hospital identity and the
lossless ingestion contract. It deliberately does not apply `na_values`, infer
clinical types, call numeric coercion, parse decimal commas or I:E ratios,
rename columns, merge variables, translate categories, clean values, or filter
rows or stays.

## Discovery and classification

Traversal is deterministic. At the raw root it recognizes only configured
two-digit hospital folders and `README.md`. It prunes the root `pooled/` tree.
At every depth it prunes `.ipynb_checkpoints/` before enumerating contents.
Unexpected directories are also not traversed and are blocking. Symbolic-link
directories are not followed; symbolic-link files are not opened or hashed and
are blocking.

Within each hospital, files are classified as:

- default static candidate (`static.csv`);
- configured additional static candidate;
- dynamic candidate matching the hospital-scoped filename regex;
- unclassified CSV data file; or
- unknown file.

The dynamic conventions are `<stay_id>.csv` for folder `00` and
`dynamische_variablen_kds_patient_<stay_id>.csv` elsewhere. Filenames and
filename-derived stay identifiers are confined to the private evidence bundle.

## Evidence collected

Every non-pruned file receives a deterministic opaque file key, size, and
streamed SHA-256 hash. Classified CSVs receive strict, full-file encoding and
dialect evidence, exact ordered headers, row and record-width counts, schema
variant IDs, duplicate-header findings, and decimal-comma candidate counts.

The inventory reads identifier fields strictly as strings. It separately keeps
the filename-derived stay ID and distinct in-file identifier values. Multiple
identifier columns, multiple values within a dynamic file, or disagreement with
the filename is blocking. No identifier is silently preferred.

Additional static candidates are compared by hash, ordered schema, row count,
stay uniqueness and sets, overlapping values, complementary values, extra rows
or columns, and conflicting non-missing values. Only a proven exact-byte
duplicate is automatically consistent with the already reviewed UK00 decision;
any other classification blocks for review.

Repeated dynamic stay identities are assigned one of `exact_byte_duplicate`,
`equivalent_after_parsing`, `complementary`, `conflicting`, or `unresolved`.
Every repeated identity blocks until a human records the resolution—even an
exact duplicate is not silently selected.

Reviewed duplicate headers are accepted only when hospital, raw name,
occurrence count, total column count, and exact positions match an approved
policy rule. The UK00 ARDS rule preserves both positional occurrences for
lossless ingestion. Its second occurrence may be omitted only later in
translation after revalidating the all-empty invariant.

ZIP files are classified as archive candidates before hashing. The reviewed
UK00 archive is matched without committing its filename, using hospital folder,
exact size, and SHA-256. It is provisionally excluded from ingestion and kept as
an untrusted read-only comparison, but publication remains blocked until the
data owner confirms that it is an old snapshot.

## Folder 01

The approved mapping contract identifies raw folder `01` as `asic_UK01` and
includes it in ingestion. This explicit human decision is encoded in the
policy; it was not inferred from the filename or old pooled contract.

The report continues to record folder `01` static identifier counts, identifier
evidence in its dynamic files, schema/decimal-comma evidence, and, when
accessible, raw stay-set overlap with the old pooled static artifact. For pooled
matching it checks both exact IDs and the legacy raw prefix before the final
`:<hid>` suffix, then reports overlap counts by pooled `hid`. These observations
remain diagnostic evidence. In particular, mapping approval does not approve
legacy UK01 decimal-comma parsing or any other hospital-specific parsing rule.

## Immutable output bundle

Each run ID is write-once. Existing output is never overwritten.

Owner-only private directory (`0700`; files `0600`):

```text
asic/reports/<context>/private/raw_inventory/<run_id>/
├── inventory_manifest.json
├── files.parquet
├── schema_variants.json
├── static_candidates.parquet
├── dynamic_stays.parquet
├── hospital_mapping_evidence.json
└── blocking_findings.json
```

Sanitized human-review files (`0640`):

```text
asic/reports/<context>/review/raw_inventory/<run_id>.json
asic/reports/<context>/review/raw_inventory/<run_id>.md
```

The review payload is validated against protected field names before writing.
It includes aggregates, schema fingerprints and widths, checks, limitations,
and blocking findings, but no exact headers, source filenames, stay IDs,
row-level values, or stay-set hashes. Exact headers remain in the owner-only
bundle because a malformed source can place an identifier in its first record.

## Blocking checks

Publication is blocked when the hospital folder set differs from policy; an
observed folder lacks an approved canonical mapping and inclusion decision; a
default static source is missing or repeated; unknown files/directories or
symbolic links exist; any CSV encoding, delimiter, or structure is unresolved;
static identifiers are missing/non-unique; identifiers conflict; a
dynamic-looking file lacks time-column evidence; a dynamic stay has multiple
source files; or an additional static candidate is not an exact duplicate.

A provisionally excluded archive also remains a publication blocker while its
owner-confirmation status is pending. The lossless-ingestion policy may carry
only this named blocker: it re-hashes the archive as bytes, excludes it from
source selection, and marks every resulting artifact non-publishable.

The inventory command writes evidence first and exits with status `2` when such
findings exist. Status `2` means the publication gate worked. Ingestion requires
its own executable policy and rejects every blocker except the explicitly
reviewed provisional archive confirmation.
