# V3 architecture and review gates

## Authority and isolation

Raw hospital CSV files under `/hpcwork/jrc_combine/richard/asic_2026` are the
future authoritative source. The root `pooled/` directory is pruned from raw
discovery. Old pooled and v2 outputs may be opened only through a configuration
explicitly marked `untrusted_read_only_comparison`.

Every v3 output path contains the dataset context (`demo` or `production`) and
resides below the v3 project. The code does not share report, run, or data paths
with v2. Configuration loading also locks the production raw root to
`/hpcwork/jrc_combine/richard/asic_2026` and rejects report/run paths outside
the v3 `asic/` tree.

## Chronological gates

### Gate 1 — raw inventory and hospital identity

Inventory every expected hospital folder without parsing clinical values.
Classify all files, detect structural CSV properties, preserve private
identifier evidence, investigate folder `01`, and obtain explicit approval for
the folder-to-hospital mapping.

Status: implemented. The mapping contract includes folder `01` as
`asic_UK01`, with cohort action `include`.

### Gate 2 — lossless ingestion contract

Approve provenance columns, raw string/null representation, file/row
conservation rules, identifier conflict policy, and duplicate-file resolution.

Status: implemented for one hospital per immutable command. The complete
all-hospital production run was reported successful on 2026-08-05. A separate
bounded-memory post-ingestion audit is implemented as the review checkpoint
before Gate 3. Outputs remain non-publishable while the archive confirmation or
any downstream gate is pending.

Production audit `20260805T060046Z` covered all 8 hospitals and 16 Parquet
tables. All 13 technical checks passed, including hashes and provenance-row
conservation for 16,054 static and 24,069,379 dynamic rows. The only remaining
finding is the expected archive-owner confirmation gate.

### Gate 3 — raw schema and parsing-token evidence

Inventory every raw column occurrence and every non-empty token in expected
numeric fields without transforming the ingested artifacts. Produce aggregate
evidence for direct numeric, candidate custom syntax, textual missing, and
unresolved tokens. This gate inventories evidence; it does not yet approve or
apply parsing.

Status: implemented for synthetic verification and approved production
execution. The command requires the validated ingestion audit, re-hashes every
input Parquet file, and writes reports only. No parser or harmonized artifact is
created. Production policy/artifact `0.3` run `20260805T090942Z` passed every
technical and token-accounting check for all 934 raw-column occurrences.

### Gate 4 — harmonization policy and variable dictionary

Approve all raw-to-canonical mappings, scoped aliases, categorical vocabularies,
numeric/token parsers, missing sentinels, expected units, notation semantics,
and deterministic hospital/schema-specific conversions. Unknown raw columns and
unresolved non-empty numeric tokens block. Translation is an operation inside
harmonization, not a separate durable dataset.

Status: a fail-closed synthetic bootstrap now exercises the active reviewed
rules on bounded Arrow column batches. Production reads, artifact writes,
hospital concatenation, the complete registry, union schema, and variable
dictionary remain disabled and unapproved. A separate read-only candidate
registry review consumes the immutable schema/token bundle, accounts for every
occurrence, and writes only private review workbooks plus a sanitized report.
It cannot approve or execute its proposals. A synthetic-only complete-registry
contract and Arrow-schema planner additionally enforce exact occurrence
coverage, explicit drops, target type/unit consistency, and alias policies,
while every production execution gate remains closed.

### Gate 5 — harmonized union contract and old-pool comparison

Use cross-hospital evidence to approve semantic and unit decisions, then freeze
one ordered harmonized schema before filling unavailable site fields and
concatenating hospitals. Publish harmonized static/dynamic contracts with their
machine-readable variable dictionary. Compare counts, stay sets, availability,
string-derived values, and distributions against the old lossy pool. The
comparison is diagnostic, never authoritative.

### Gate 6 — cleaning

Starting from the frozen harmonized contract, approve physiologic invalid-value
masking and narrowly evidenced row-level scale/input-error corrections. Preserve
the harmonized value and record cleaning status and rule ID. Cleaning does not
silently filter rows, stays, or cohorts.

### Gate 7 — derived outputs

Implement analysis-specific mortality, ventilation, cohort flags, and optional
time blocking separately. Do not impose a 24-hour ventilation exclusion in the
cleaned layer.

Each gate produces immutable evidence, an explicit decision, and a new scope
approval. A passing technical check does not substitute for clinical or data
governance approval.
