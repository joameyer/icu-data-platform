# Post-ingestion audit contract

## Purpose and boundary

The post-ingestion audit is the human-review checkpoint between lossless
per-hospital ingestion and any raw-token parsing work. It reads the immutable
inventory `0.4` evidence, each hospital's ingestion manifest `0.2`, and the two
ingested Parquet files. It does not read clinical value columns, parse tokens,
translate variables, concatenate hospitals, or write a production data
artifact.

The command audits the complete approved hospital set in one run. Partial
hospital sets, unexpected output entries, symlinks, or missing files are
blocking. All artifacts must reference the same inventory run supplied on the
command line.

## Checks

For each of the eight hospitals, the audit verifies:

- the exact v3 directory boundary, expected three-file layout, and owner-only
  modes;
- ingestion artifact, manifest, inventory, and policy versions;
- the approved folder-to-hospital mapping and cohort inclusion;
- exact selected and excluded inventory file accounting, including the
  duplicate UK00 static source and provisionally excluded archive;
- manifest-to-inventory source file, header, schema-variant, and row
  conservation;
- output SHA-256 digests and Parquet metadata row counts;
- standard Arrow dataset-reader compatibility and unique physical field names;
- an inventory-derived schema consisting of the ten provenance fields followed
  by raw string fields with exact positional metadata; and
- the approved two-field physical storage rule for the reviewed UK00 duplicate
  occurrence, whenever that raw layout is present.

Provenance validation is bounded-memory. The audit streams only the ten
provenance fields in configured batches. It requires contiguous hospital-table
source order, contiguous file order, CSV record numbers starting at two,
constant source file/schema evidence within each file, and exact per-file row
counts. Static in-file stay IDs must be non-empty and unique. Dynamic
filename-derived IDs must be present, and every available in-file ID must be
empty, missing, or equal to its filename-derived counterpart. Identifier values
are never copied into audit reports or logs.

File-order runs are identified with Arrow-native run-end encoding. The audit
has no NumPy runtime dependency, matching the minimal production environment.

## Evidence and privacy

Each immutable run writes:

```text
asic/reports/<context>/private/ingestion_audit/<run_id>/
├── ingestion_audit_manifest.json
└── failures.json

asic/reports/<context>/review/ingestion_audit/
├── <run_id>.json
└── <run_id>.md
```

The private directory and files use modes `0700` and `0600`. It may contain
exact paths or protected failure locations and must remain on the authorized
cluster. The sanitized report contains hospital-level aggregate counts and
boolean outcomes only. It excludes filenames, stay identifiers, raw tokens,
and exact raw headers and passes the same fail-closed privacy guard used by the
raw inventory.

## Interpretation

The production audit is expected to remain `FAIL` only for
`provisional_archive_owner_confirmation` while the archive owner's response is
pending. Any additional blocking finding indicates a technical or conservation
failure and stops the next phase. The archive remains excluded from ingestion
regardless of that pending confirmation.

A technically complete audit does not make ingested data publishable. Numeric
token parsing, the harmonization registry, and harmonized union-schema approval
remain separate downstream human gates. Ingestion manifest `0.2` retains its
older translated-gate strings as immutable historical provenance. The next
permissible implementation after review is a read-only raw schema and
parsing-token inventory—not parsing or harmonization itself.

## Reviewed production result

Immutable production audit `20260805T060046Z`, linked to inventory
`20260804T193415Z`, audited all 8 approved hospitals and 16 tables. Parquet and
streamed provenance counts conserved 16,054 static rows and 24,069,379 dynamic
rows, and every output hash matched its ingestion manifest. Thirteen technical
checks passed. Overall status remains `FAIL` solely because
`provisional_archive_owner_confirmation` is pending; no ingestion regeneration
is indicated by the sanitized evidence.
