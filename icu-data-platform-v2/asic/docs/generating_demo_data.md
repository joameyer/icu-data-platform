# ASIC pooled demo data

This guide documents the small, unchanged subset of pooled production data and
its generation manifest. Unlike the mock dataset, the demo retains real
`Pseudo-ID` values and real source values. The artifacts live under
`asic/data/demo/` and must remain in authorized cluster storage.

The implementation is in
[`src/asic_pipeline/demo_data/generator.py`](../../src/asic_pipeline/demo_data/generator.py),
and paths and sampling parameters are declared in
[`asic/config/pooled/generation/demo.yaml`](../config/pooled/generation/demo.yaml).

## Purpose

The demo makes exploratory data-quality work possible without loading all
21,876,966 production dynamic rows into memory. Typical uses include:

- comparing potentially synonymous source columns such as `DeltaP` and
  `deltaP`;
- inspecting categorical encodings and missing-value representations;
- developing unit, range, semantic, and cross-hospital audit code;
- testing data-loading, joins, and pipeline behavior on unmodified records.

Because the demo is a sample, the absence of an issue does not prove that the
full production dataset is free of that issue. It must not be used to estimate
full-dataset prevalence or to report scientific results.

## What the generator creates

The source is:

```text
asic/data/production/pooled/
├── static.parquet
└── dynamic.parquet
```

The configured outputs are:

```text
asic/data/demo/
├── pooled/
│   ├── static.parquet
│   └── dynamic.parquet
└── demo_generation_manifest.json
```

It does not write anything to `translated/`, `cleaned/`, or `derived/`.

With the default configuration, the generator:

1. Requires both production Parquet schemas to match the frozen pooled-input
   contract exactly.
2. Verifies static stay-ID uniqueness, `Pseudo-ID`/`hid` agreement, and the
   expected hospital set 0, 2, 3, 4, 6, 7, and 8.
3. Uses seed `42` to sample 100 ICU stays without replacement from each
   hospital. It fails if any hospital has fewer than 100 stays.
4. Preserves the source order of the selected static rows.
5. Scans only the dynamic `Pseudo-ID` column one row group at a time, then
   extracts every dynamic row belonging to the selected stays in source order.
6. Writes the selected Arrow tables without renaming, casting, remapping,
   shuffling, imputing, or otherwise changing values.
7. Audits the staged demo using the production Arrow schema and all blocking
   pooled-input checks. Files are published only after the audit passes.
8. Writes a manifest with configuration, seed, versions, aggregate row/stay
   counts, and audit status. Selected `Pseudo-ID` values are not listed in the
   manifest.

One `Pseudo-ID` represents one ICU stay. The default output therefore contains
700 unchanged static stays plus all associated dynamic rows. Edit
`sampling.stays_per_hospital` in the generation configuration if a different
size is required, and record that configuration with the run.

## Preservation guarantee

Selection is the only data operation. For every selected stay:

- the real `Pseudo-ID` is retained;
- all source columns are retained in their original order;
- Arrow types remain identical to production;
- values and nulls are unchanged;
- all and only the corresponding dynamic rows are retained;
- relative and anchored time values are unchanged.

Parquet file encoding and row-group layout may differ because new subset files
are written. That does not change the logical table content.

## Run on the cluster

The default configuration uses project-relative paths, so from the cluster
project root (named `icu_data_platform`, without `-v2`) run:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform

./.venv/bin/python -m asic_pipeline generate-demo \
  --config asic/config/pooled/generation/demo.yaml
```

Run this inside an allocated compute session or batch job, not on a login node.
Memory use is bounded because the complete dynamic source table is never loaded
at once, but the command still scans the `Pseudo-ID` column of every row group.

Before generation, audit the production input and review its result:

```bash
./.venv/bin/python -m asic_pipeline audit-pooled \
  --config asic/config/datasets/production.yaml
```

The generator refuses to replace an existing static file, dynamic file, or
manifest. Only after replacement has been explicitly reviewed and approved,
add `--overwrite`:

```bash
./.venv/bin/python -m asic_pipeline generate-demo \
  --config asic/config/pooled/generation/demo.yaml \
  --overwrite
```

After generation, the demo can be audited independently at any time:

```bash
./.venv/bin/python -m asic_pipeline audit-pooled \
  --config asic/config/datasets/demo.yaml
```

The audit report is written beneath `asic/reports/demo/`.

## Create the translated demo layer

After the unchanged pooled demo and its audit have been reviewed, run the
separate column-harmonization stage:

```bash
./.venv/bin/python -m asic_pipeline pooled-to-translated \
  --config asic/config/datasets/demo.yaml
```

This writes English column names to `asic/data/demo/translated/`, applies only
the reviewed naming-alias merges, and records all merge/drop checks in
`translation_manifest.json`. It does not modify the pooled demo.

## Reproducibility, privacy, and tests

For unchanged source files, configuration, and software versions, seed `42`
selects the same stays. Any production-data change may change the selected
subset even when the seed remains fixed.

This is not anonymized or privacy-protecting mock data. It contains real,
unchanged project data and real stay identifiers. Do not copy it to a local
computer, commit it to version control, or redistribute it.

The automated tests use temporary synthetic Parquet fixtures and never read or
modify production, demo, or mock artifacts:

```bash
./.venv/bin/python -m pytest tests/test_demo_generation.py -q
```
