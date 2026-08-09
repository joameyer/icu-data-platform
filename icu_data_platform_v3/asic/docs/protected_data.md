# Protected-data handling

Raw filenames can encode stay identifiers. Exact filenames, paths, filename
stay IDs, in-file identifier values, and stay-set digests are therefore treated
as protected evidence.

They remain only in owner-only inventory or ingested artifacts on the authorized
cluster. Inventory bundles and hospital ingestion directories use mode `0700`;
their files use `0600`. Logs and CLI summaries contain aggregate status and
counts, never raw filenames or identifier values.

Sanitized review reports contain hospital folder codes, counts, schema
fingerprints and widths, detection statuses, and blocking rules. Exact headers
and raw column names remain private because a malformed header record could
contain a stay identifier. Before either review file is written, a fail-closed
validator rejects known protected fields at any nested level.

Git ignores all `asic/reports/`, `asic/runs/`, `asic/data/` content (except the
data policy README), Parquet files, virtual environments, and caches. Reports
must still be reviewed before any manual copy or publication; being sanitized
by code is evidence, not permission to distribute.

Ingested Parquet contains protected filename and stay provenance by design.
Its manifest contains filenames, paths, hashes, encodings, delimiters, and exact
raw headers. Production data and private evidence must never be copied into this
repository or off the authorized HPC cluster. Tests use synthetic identifiers
only and write generated bundles under temporary test directories.

The post-ingestion audit streams the protected provenance fields but retains no
identifier values in its sanitized output. Exact output paths and protected
failure locations are written only to the owner-only private audit bundle.
Sanitized ingestion-audit reports contain hospital IDs, aggregate table counts,
and check outcomes and are passed through the fail-closed review-payload guard.

Schema/token inventory `columns.parquet` contains exact raw headers and private
candidate numeric summaries. `tokens.parquet` contains bounded exact raw-token
evidence. Both remain owner-only on the cluster. Identifier and free-text
distinct values are not collected, and sanitized reports expose only generic
classification counts. Private token evidence must never be pasted into chat or
copied off the authorized cluster.
