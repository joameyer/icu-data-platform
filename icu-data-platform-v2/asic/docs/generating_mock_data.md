# ASIC pooled mock data

This guide documents the generated pooled mock-data artifacts and their
generation manifest. The artifacts live under `asic/data/mock/`; generator
code and configuration remain outside the data directory.

The implementation is in
[`src/asic_pipeline/mock_data/generator.py`](../../src/asic_pipeline/mock_data/generator.py),
and all paths and generation choices are declared in
[`asic/config/pooled/generation/mock.yaml`](../config/pooled/generation/mock.yaml).

## What the generator creates

The source is the already-pooled production dataset:

```text
asic/data/production/pooled/
├── static.parquet
└── dynamic.parquet
```

The configured outputs are:

```text
asic/data/mock/
├── pooled/
│   ├── static.parquet
│   └── dynamic.parquet
└── mock_generation_manifest.json
```

It does not write anything to `translated/`, `cleaned/`, or `derived/`.

One `Pseudo-ID` represents one ICU stay; there is no separate patient-level
identifier. With the default configuration, the generator:

1. Requires the production static and dynamic Parquet schemas to match the
   frozen pooled-input contract exactly.
2. Verifies that static `Pseudo-ID` suffixes agree with `hid`, that every static
   stay ID is unique, and that the expected hospitals 0, 2, 3, 4, 6, 7, and 8
   are present.
3. Uses seed `42` to sample 100 ICU stays from each hospital. It fails if any
   hospital has fewer than 100 stays.
4. Shuffles non-protected static values within each hospital. Height, weight,
   and BMI fields move as one block; outcome fields move as another block.
   Missing positions in independently shuffled columns are retained.
5. Extracts all dynamic rows belonging to the sampled stays by scanning the
   source Parquet row groups.
6. Shuffles every non-protected dynamic column independently within its
   hospital while retaining missing positions.
7. Replaces source `Pseudo-ID` values with mock IDs `1:<hid>` through
   `100:<hid>`. The temporary source-to-mock mapping remains in memory and is
   not written to disk.
8. Writes both tables to a temporary staging directory using the exact mock
   Arrow schema, runs the full pooled-input contract audit, and publishes the
   files only if that audit passes.
9. Writes a JSON manifest containing the configuration, seed, contract and
   generator versions, source/output row counts, hospital counts, and audit
   status. The manifest never contains the stay-ID mapping.

Columns protected from shuffling are:

- Static: `Pseudo-ID`, `Liegedauer_ICU`, and `hid`. `Pseudo-ID` is subsequently
  remapped.
- Dynamic: `Pseudo-ID`, `Zeit_ab_Aufnahme`, `hid`, and `timeidx`. `Pseudo-ID` is
  subsequently remapped.

All input columns are retained. The generated artifacts still contain exact
values originating from protected production data. Shuffling and ID remapping
are not formal anonymization: use these files only for authorized software
development, never for redistribution or clinical/statistical inference.

## Run on the cluster

The default configuration resolves these project-relative paths on the
cluster:

- Input: `asic/data/production/pooled/`
- Output: `asic/data/mock/pooled/`
- Manifest: `asic/data/mock/mock_generation_manifest.json`

From the cluster project root (the folder is named `icu_data_platform`, without
`-v2`), activate an allocated compute session or submit a batch job. The
production dynamic table has more than 21 million rows, so do not run the scan
on a login node.

For a first generation, when no mock outputs exist:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform

./.venv/bin/python -m asic_pipeline generate-mock \
  --config asic/config/pooled/generation/mock.yaml
```

Before regenerating, audit the production input and review its result:

```bash
./.venv/bin/python -m asic_pipeline audit-pooled \
  --config asic/config/datasets/production.yaml
```

The generator refuses to replace an existing static file, dynamic file, or
manifest by default. After the existing mock artifacts have been reviewed and
replacement has been explicitly approved, add `--overwrite`:

```bash
./.venv/bin/python -m asic_pipeline generate-mock \
  --config asic/config/pooled/generation/mock.yaml \
  --overwrite
```

The command prints progress while scanning row groups and ends with the output
paths, row counts, stay count, and audit status. Inspect
`asic/data/mock/mock_generation_manifest.json` and retain the cluster job log
with the run records.

After the pooled mock has been reviewed, its translated layer can be built
without modifying the pooled artifacts:

```bash
./.venv/bin/python -m asic_pipeline pooled-to-translated \
  --config asic/config/datasets/mock.yaml
```

## Reproducibility and tests

For the same source files, configuration, and software versions, seed `42`
makes sampling, shuffling, and remapping reproducible. Any change to the pooled
production data can change the result even when the seed is unchanged.

The generator tests use temporary synthetic Parquet fixtures and never read or
modify the protected production or existing mock data:

```bash
./.venv/bin/python -m pytest tests/test_mock_generation.py -q
```
