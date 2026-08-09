# Data directory policy

Do not commit raw, ingested, harmonized, cleaned, derived, comparison, demo, or
Parquet data here. Production data must remain on the authorized HPC cluster.

Future production artifacts belong only below the separate v3 deployment:

`/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/data/production/`

The v2 deployment and its artifacts remain read-only at:

`/hpcwork/jrc_combine/joana/icu_data_platform/`

This README is the sole source-controlled file permitted below `asic/data/`.
Inventory output belongs under `asic/reports/`. Lossless per-hospital outputs
belong under:

`asic/data/<context>/ingested/asic_UKNN/`

Each hospital directory contains `static.parquet`, `dynamic.parquet`, and
`ingestion_manifest.json`; these are protected artifacts and must never be
committed or copied off the authorized cluster.

Approved dry-run candidates belong under the immutable run scope:

`asic/data/<context>/harmonization_candidates/<run_id>/asic_UKNN/`

They are protected, non-publishable review artifacts. They must not be treated
as the released harmonized layer or consumed by cleaning until the consolidated
review, variable dictionary, union-schema freeze, and conservation audit are
explicitly approved.

Explicitly approved harmonized releases belong under:

`asic/data/<context>/harmonized/releases/<release_id>/`

Each immutable release contains byte-identical copies of the completely
audited static and dynamic Parquet files plus `release_manifest.json`.
`asic/data/<context>/harmonized/current_release.json` is the authoritative
pointer for downstream cleaning. The release manifest, rather than the
candidate-stage Parquet metadata preserved for byte identity, records the
promoted status. Promotion never authorizes copying protected data off the
cluster.

Run-scoped cleaned candidates belong under:

`asic/data/<context>/cleaned_candidates/<run_id>/`

They are protected, non-publishable artifacts generated only from the current
released harmonized layer. They retain every row, stay, column, and provenance
field and require a complete audit plus a separate human promotion decision
before becoming a cleaned release.

Explicitly approved cleaned releases belong under:

`asic/data/<context>/cleaned/releases/<release_id>/`

Each immutable release contains byte-identical copies of the completely
audited cleaned-candidate Parquet files plus `release_manifest.json`.
`asic/data/<context>/cleaned/current_release.json` is the authoritative pointer
for later derived stages. Promotion preserves the candidate and does not rerun
cleaning, derive variables, or authorize external export.

Run-scoped derived candidates will belong under:

`asic/data/<context>/derived_candidates/<run_id>/`

They may be built only from `cleaned/current_release.json` after the
consolidated core-derived contract is approved. Core derivation must not modify
the cleaned release or silently embed a cohort or time-blocking recipe.

Time-blocking contract evidence remains report-only. Frozen contract `0.1`
allows local demo candidates under:

`asic/data/demo/derived/time_blocking/8h/candidates/<run_id>/`

The candidate references the exact core-derived static table through manifest
lineage and neither copies it nor repeats static values on block rows.
Production candidate and audit `20260808T130534Z` subsequently passed complete
independent review and received exact promotion approval. The approved
byte-preserving release belongs under:

`asic/data/production/derived/time_blocking/8h/releases/20260808T130534Z/`

It contains byte-identical copies of `blocks.parquet`,
`stay_block_summary.parquet`, `blocked_variable_dictionary.parquet`,
`blocked_variable_dictionary.md`, and `candidate_manifest.json`, plus the
release manifest. The only authoritative pointer for this optional recipe is:

`asic/data/production/derived/time_blocking/8h/current_release.json`

This pointer is separate from `derived/current_release.json`; promotion of the
blocked recipe must not change the core-derived pointer or copy the static
table. Every other release ID, overwrite, pointer target, and external export
remains rejected.
