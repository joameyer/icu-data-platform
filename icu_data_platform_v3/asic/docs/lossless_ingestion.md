# Phase 2 lossless per-hospital ingestion

## Approved implementation boundary

The shared ingestion engine processes exactly one approved canonical hospital
per command. It consumes an immutable private raw-inventory artifact `0.4` and
requires the current folder-to-hospital contract. It does not independently
rediscover files or select sources by filename alone.

The only inventory blocker permitted to cross this boundary is
`provisional_archive_owner_confirmation`. The archive must still match the
exact hospital-folder, byte-size, and SHA-256 policy rule. It is re-hashed as a
source file but never selected or parsed. Every other inventory blocker stops
ingestion before an output directory is committed.

This approval covers implementation and synthetic verification. Production
execution remains a separate, one-hospital-at-a-time human checkpoint.

## Source selection

For the requested hospital, ingestion selects only:

- the one `selected_static_candidate`; and
- every `dynamic_candidate` with complete inventory evidence.

The exact UK00 duplicate static candidate and the provisionally excluded ZIP
are recorded in the manifest but never concatenated or opened as CSV inputs.
Checkpoint and old pooled trees were pruned before inventory and cannot enter
the selected file set. Dynamic filename-derived stay IDs must be non-empty and
unique. Static in-file stay IDs must be non-empty and unique. Every available
dynamic in-file ID is compared to the filename-derived value again.

Every selected and excluded source is re-hashed. Selected headers, record
widths, and row counts are also revalidated against the immutable inventory.
A changed source requires a new inventory run; no stale decision is inherited.

## Raw value representation

Python's strict CSV reader is used with the inventory-approved encoding and
delimiter. No Pandas reader, `na_values`, numeric coercion, type inference,
decimal-comma parsing, I:E parsing, category translation, column merge, unit
correction, value masking, or row/stay filtering occurs.

Every present raw cell is written as an Arrow string. A literal empty CSV cell
is the string `""`. A Parquet null means only that the raw column occurrence was
absent from that source file's schema. Thus empty cells, textual tokens such as
`"nan"`, and schema absence remain distinguishable.

Duplicate raw names are identified by `(raw name, one-based occurrence)`.
Parquet requires unique physical names for reliable standard-reader access.
The reviewed UK00 occurrences therefore use
`ARDS_Diagnose_App_col1` and `ARDS_Diagnose_App_col2`, in original left-to-right
order. Arrow field metadata and the manifest retain the exact raw name
`ARDS_Diagnose_App`, its occurrence number, and the approved naming-rule ID.
These are positional source occurrences, not distinct clinical concepts.
Neither occurrence is merged or dropped during ingestion. Any duplicate without
an exact approved naming rule, or any physical-name collision, blocks ingestion.

## Provenance and deterministic order

Every output row contains:

- source hospital folder and canonical hospital ID;
- source filename and opaque inventory file ID;
- deterministic file order and hospital-table source order;
- one-based CSV record number, where the header is record 1;
- filename-derived and in-file stay IDs kept separately; and
- source schema variant ID.

The per-hospital raw union follows deterministic inventory file order and each
file's exact header order. Dynamic rows stream in configured batches into a
Parquet writer; the complete dynamic dataset is never constructed as one
in-memory DataFrame.

## Atomic, immutable artifacts

Each hospital is staged under the context-specific v3 data root and moved into
place only after static and dynamic conservation checks pass:

```text
asic/data/<context>/ingested/asic_UKNN/
├── static.parquet
├── dynamic.parquet
└── ingestion_manifest.json
```

An existing hospital directory is never overwritten. The manifest records all
selected and excluded files, hashes, dialect evidence, source and output row
counts, raw positional schemas, literal-empty and absent-column counts, output
hashes, and the carried human-review gates. It contains protected provenance
and remains on the authorized cluster with owner-only permissions.

Technical ingestion success does not make the artifact publishable. Parsing,
harmonization-registry, and harmonized union-schema reviews remain pending, and
archive owner confirmation remains carried when applicable. Manifest `0.2`
retains the older translated-gate strings as immutable historical labels; they
do not require artifact regeneration after the approved architecture change.

## Artifact compatibility correction

Ingestion policy and manifest version `0.1` preserved duplicate raw names as
duplicate physical Parquet fields. Although valid at the low-level Parquet-file
API, PyArrow's dataset reader—and therefore `pandas.read_parquet()`—could not
resolve those fields by name. Version `0.2` applies the approved positional
physical names above. A standard `pyarrow.parquet.read_table()` regression
guards this compatibility requirement. Version `0.1` UK00 output must be
quarantined intact and regenerated; it must not be overwritten or silently
treated as the current artifact.
