# Raw-schema and parsing-token inventory

## Approved boundary

This read-only evidence stage consumes only the validated lossless per-hospital
Parquet artifacts. It requires immutable raw inventory `0.4`, ingestion
manifests `0.2`, and post-ingestion audit `0.1` with no technical failures. The
pending archive-owner confirmation may be carried because the exact archive is
already excluded and is not read by this command.

The stage does not rename a column, parse a token, normalize missingness,
translate a category, convert a unit, merge an alias, concatenate hospitals, or
write harmonized/cleaned data. Policy `0.2` loads prior v2 and legacy decisions
as an unreviewed seed registry and overlays only the rules explicitly approved
from raw-v3 inventory run `20260805T064135Z`. The overlay records hospital,
table, exact raw occurrence, rule ID, evidence count, reviewer role, and date.

## Schema evidence

Every non-provenance Arrow field is identified from immutable metadata by:

- table and canonical hospital;
- physical storage name;
- exact raw name and one-based raw occurrence;
- candidate canonical target and candidate kind, if known;
- source file and schema-variant availability;
- source-schema-present row count;
- Parquet null, literal empty, and raw non-empty counts; and
- all-missing/empty status.

The inventory checks that a Parquet null occurs only when that raw occurrence
was absent from the source file schema. Literal `""` remains a separate value.
Every cell therefore satisfies:

```text
table rows = schema-absence nulls + literal empty strings + raw non-empty tokens
```

The approved positional UK00 duplicate is inventoried twice. Its second
occurrence remains a drop candidate only if its raw non-empty count is still
zero. No occurrence is removed here.

## Token evidence

Candidate numeric non-empty tokens are partitioned without changing ingested
values. Approved parsers may compute evidence-only candidate values:

- `direct_numeric`;
- `decimal_comma_syntax_candidate`;
- `ratio_syntax_candidate`;
- `threshold_syntax_candidate`;
- `percentage_syntax_candidate`;
- `custom_parsed_numeric_list` for numeric-only UK00 height lists;
- `custom_parsed_numeric_list_with_missing_elements` for UK00 height lists
  containing the approved `nan` element marker;
- `textual_missing_candidate`;
- `whitespace_only_candidate`;
- `unresolved_ratio_zero_denominator`; or
- `unresolved_token`.

The partition is exhaustive and mutually exclusive. Candidate parsed numeric
minimum, maximum, and magnitude bins are private review evidence only. They do
not replace the raw string. The reviewed UK00 height rule interprets a strict
numeric list as repeated centimetre measurements for evidence accounting only.
It preserves order and duplicates, and accounts separately for numeric,
approved-missing, and unresolved list elements. Numeric
`-1`/`-1.0` occurrences in UK08 `hosp_los`, `dialysis_free_days`, and
`vent_free_days` are counted as approved hospital-scoped sentinel annotations
while remaining in the `direct_numeric` partition. No missing value is written
by this command.

Expected categorical tokens are retained exactly in the owner-only evidence up
to the policy cap. The reviewed `cluster_id` rule validates exact membership in
its hospital-specific allowed set and blocks any out-of-domain value. Identifier
and free-text values are counted without building distinct-value inventories.
Truncation remains blocking for categorical, unknown, unresolved, and
unreviewed custom syntax. It is informational only for exact
hospital/variable/class scopes with an approved deterministic grammar. The
initial exemption covers five reviewed UK01 decimal-comma fields and the strict
UK00 numeric-list parser; every cell and list element is still classified and
conserved.

## Artifacts and privacy

Each immutable artifact-`0.3` run writes:

```text
asic/reports/<context>/private/schema_token_inventory/<run_id>/
├── schema_token_inventory_manifest.json
├── columns.parquet
└── tokens.parquet

asic/reports/<context>/review/schema_token_inventory/
├── <run_id>.json
└── <run_id>.md
```

`columns.parquet` contains exact raw names and candidate numeric summaries.
`tokens.parquet` contains bounded exact raw-token evidence, except identifiers
and free text, which are redacted by policy. Both are protected, owner-only
cluster files. They must not be copied into the repository, pasted into chat,
or moved off the authorized cluster.

The sanitized review report contains hospital/table counts and generic token
classification totals only. It excludes filenames, stay identifiers, raw
tokens, and exact raw headers and passes the fail-closed privacy guard before
writing.

## Expected review state

Overall status remains expected to be `FAIL` because the broader seed
harmonization registry is deliberately unapproved and archive-owner
confirmation is still pending. On the unchanged reviewed production inputs,
Policy `0.2` resolved the prior unmapped NBSP header, cluster-domain findings,
and all five example-cap findings, but correctly left 249 multi-measurement
height cells unresolved. Policy `0.3` replaces the superseded singleton rule
with a list-valued rule and is expected to resolve those cells when every
element is numeric or the approved `nan` marker. It must also
revalidate the approved cluster domains, rule evidence counts, UK08 sentinel
counts, list-element conservation, and all-empty ARDS precondition. Any
discrepancy or unknown list element is a new blocker.

Any technical blocker—input hash, schema, null-conservation, cell accounting,
or incomplete hospital set—fails the Slurm job and must be resolved before
clinical review. A review-gated exit does not fail the Slurm wrapper, but it
still authorizes no parser or harmonized artifact.

## Human review order

Review the sanitized report first. Then inspect the private evidence on the
cluster in this order:

1. confirm technical blocker count is zero;
2. classify every unmapped raw occurrence;
3. verify the partial reviewed-rule overlay has zero domain, evidence-count,
   unresolved-token, and blocking-truncation failures;
4. approve or reject ratio, decimal-comma, threshold, percentage, textual
   missing, and numeric-sentinel candidates;
5. compare candidate numeric scales by canonical target and hospital;
6. review categorical domains and prior v2/legacy canonical names;
7. confirm the second positional duplicate remains all-empty; and
8. review the remaining seed mappings in grouped canonical-variable families
   before drafting the complete harmonization registry and variable dictionary.

Every approval must retain a rule ID, hospital/schema scope, evidence count,
reviewer, and date. Unknown raw columns and unresolved non-empty numeric tokens
block harmonized publication.
