# Phase 1 raw-anomaly audit

## Approved scope

This read-only extension investigates two blockers found by raw inventory
artifact `0.2`:

1. every UK00 dynamic file contains two positional occurrences of
   `ARDS_Diagnose_App`; and
2. UK00 contains an unclassified ZIP whose member names substantially overlap
   the flat export.

The audit does not ingest files, extract ZIP members, infer clinical types,
interpret missing tokens, merge duplicated columns, or choose an authoritative
archive representation. The later approved mapping contract identifies folder
`01` as `asic_UK01` and includes it in ingestion.

## Duplicate-column accounting

Each source file is re-hashed against the approved private inventory evidence.
Both raw occurrences are selected by header position, never by Pandas duplicate
name mangling. Every data record is assigned exactly one literal-string class:

- both cells are exactly empty;
- first occurrence only is non-empty;
- second occurrence only is non-empty;
- both are equal and non-empty; or
- both are non-empty and conflicting.

No whitespace stripping or textual-missing policy is applied. The accounting
invariant is the source data-record count equal to the sum of these five
categories. Private tables retain per-file provenance, raw-token counts, and
raw-token-pair counts. The sanitized report contains only aggregate counts and
positional layouts.

Frozen v2 renamed the second pooled occurrence to
`ARDS_Diagnose_App_duplicated_0` and approved dropping it under an `all_missing`
precondition. V3 treats that conclusion only as a hypothesis to revalidate from
raw strings.

## ZIP comparison

The ZIP file itself is re-hashed against inventory evidence. Members are opened
as streams and never extracted. Unsafe paths, encryption, checkpoints, CRC/read
errors, duplicate basenames, and nested archive entries are recorded.

Unique member basenames are privately paired with flat UK00 filenames. Each
overlap is classified as:

- exact byte duplicate;
- equivalent decoded CSV cell stream;
- different content with equal schema and row count;
- different schema;
- different row count;
- structurally unresolved; or
- different binary content.

Cell-stream equivalence uses length-delimited literal strings, including the
ordered header and every row. It neutralizes representational differences such
as line endings and CSV quoting but performs no clinical parsing. Processing is
bounded to one flat or archive member stream at a time.

Archive-only, flat-only, and nested-ZIP entries remain evidence of a distinct
snapshot even if every overlapping CSV is equivalent. The archive is
provisionally excluded from ingestion and retained read-only for comparison.
That exclusion must not be treated as final until its owner confirms the
legacy-snapshot role.

## Outputs

Owner-only evidence:

```text
asic/reports/<context>/private/raw_anomaly_audit/<run_id>/
├── anomaly_audit_manifest.json
├── duplicate_column_files.parquet
├── duplicate_token_counts.parquet
├── duplicate_pair_counts.parquet
├── archive_entries.parquet
├── flat_only_files.parquet
└── blocking_findings.json
```

Sanitized review:

```text
asic/reports/<context>/review/raw_anomaly_audit/<run_id>.json
asic/reports/<context>/review/raw_anomaly_audit/<run_id>.md
```

Runs are immutable. The remaining expected human blocker is archive owner
confirmation. The hospital mapping check now passes for all expected folders,
including `01`. A non-zero exit therefore indicates that the gate remains
enforced, not that clinical processing should continue.
