# ASIC data artifacts

This directory contains ASIC data artifacts. Artifacts are organized along two
independent dimensions:

1. **Dataset context:** `production`, `demo`, or `mock`.
2. **Processing layer:** `pooled`, `translated`, `cleaned`, or `derived`.

Executable rules and runbooks live under [`asic/config/`](../config/) and
[`asic/docs/`](../docs/). Data artifacts are not committed to version control.

## Directory structure

Every context follows the same logical structure. A directory or manifest is
absent until its generation step has been implemented and run.

```text
asic/data/<context>/
├── pooled/
│   ├── static.parquet
│   └── dynamic.parquet
├── translated/
│   ├── static.parquet
│   ├── dynamic.parquet
│   └── translation_manifest.json
├── cleaned/
│   ├── static.parquet
│   ├── dynamic.parquet
│   └── cleaning_manifest.json
└── derived/
    └── time_blocking/
        ├── 4h/
        ├── 8h/
        ├── 12h/
        └── 24h/
```

`demo/demo_generation_manifest.json` and
`mock/mock_generation_manifest.json` sit directly under their context
directories. The listed cleaned and derived outputs are planned boundaries;
their generators have not yet been implemented.

## Dataset contexts

| Context | Origin and contents | Identifiers and values | Intended use | Storage restriction |
|---|---|---|---|---|
| `production` | Complete pooled ASIC dataset supplied on the cluster | Real stay identifiers and real source values | Authoritative audits and final pipeline outputs | Cluster only |
| `demo` | Unchanged stay-level subset of `production/pooled`; by default 100 stays from each expected hospital | Real identifiers, values, nulls, types, column order, and dynamic row order | Bounded semantic, range, unit, and cross-hospital investigations | Authorized cluster storage only; not anonymized |
| `mock` | Stay-level subset of `production/pooled`; by default 100 stays per hospital with remapped stay IDs and within-hospital value shuffling | IDs are remapped and most values shuffled, but exact source values can remain | Development and testing without intact stays | Authorized development only; not formally anonymized |

The sample size and seed are configuration, not permanent dataset properties.
Consult the generation manifest for the exact provenance of a demo or mock
artifact.

## Processing layers

### `pooled/`

The pooled layer is this repository's immutable input boundary.

- `static.parquet` contains one row per ICU stay.
- `dynamic.parquet` contains longitudinal observations.
- One `Pseudo-ID` represents one ICU stay; there is no patient-level ID.
- Source column names are predominantly German.
- The executable input interface is
  [`asic/config/pooled/contract.yaml`](../config/pooled/contract.yaml).

The production pooled layer is supplied externally. The other contexts are
generated from it:

```text
production/pooled ── unchanged stay subset ───> demo/pooled
                  └─ shuffled/remapped subset ─> mock/pooled
```

### `translated/`

Each translated artifact is built independently from its context's pooled
artifact:

```text
<context>/pooled/ -> <context>/translated/
```

It preserves row count and order while applying the reviewed representation
policy: English snake-case names, approved equivalent-column merges, approved
categorical translations, canonical hospital IDs, approved missing-sentinel
normalization, the approved removal of one all-missing duplicate field, and a
lossless hospital-specific split that places UK00's absolute-volume-like tidal
volume values in `vt_per_ideal_bw_total` rather than the non-UK00
`vt_per_kg_ideal_body_weight` field.

It does not filter stays or rows, correct units, mask general invalid values,
impute data, consolidate mortality, construct ventilation episodes, or create
time blocks. Its exact specifications are:

- [`asic/config/pooled_to_translated/policy.yaml`](../config/pooled_to_translated/policy.yaml)
- [`asic/docs/pooled_to_translated.md`](../docs/pooled_to_translated.md)
- [`asic/config/translated/contract.yaml`](../config/translated/contract.yaml)
- [`asic/docs/translated_data.md`](../docs/translated_data.md)

`translation_manifest.json` records the source and output counts, policy and
generator versions, merge/drop checks, categorical mappings, sentinel counts,
semantic-split conservation and hospital-scope checks, and publication status.

### `cleaned/` — planned

The cleaned layer will be the canonical, analysis-agnostic form of translated
data. It will retain every translated column and preserve static and dynamic
row count/order, stay identifiers, hospital identifiers, and both time keys.
It may change values only through named, reviewed cleaning rules covering:

- unit and representation standardization;
- sentinel normalization;
- approved site-specific semantic or unit corrections;
- approved row-level scale-entry recovery for tightly bounded variables;
- invalid or physiologically implausible value handling; and
- physiologic quality control and associated flags.

It will not filter rows or stays, select a cohort, impute missing data, create
time blocks, or add analysis-specific clinical features. The immutable
translated artifact remains the source representation for every changed
value. A future `cleaning_manifest.json` must identify each applied rule,
scope, before/after meaning or unit, and exact affected count.

The stage design is
[`asic/docs/translated_to_cleaned.md`](../docs/translated_to_cleaned.md).

### `derived/` — planned

`derived/` is the umbrella for new analytical representations and features.
Time-blocked data will live under:

```text
<context>/derived/time_blocking/<resolution>/
```

Each resolution is generated independently and directly from the accepted
cleaned artifact—never from another blocked artifact. Thus 4-, 8-, 12-, and
24-hour data can coexist without changing or reconstructing the cleaned data.
Each recipe must document its alignment, interval boundaries, aggregation and
missingness rules, static-field handling, derived fields, and manifest.

Mortality consolidation and the mechanical-ventilation-duration flag also
belong to derived processing because they add analysis-facing clinical
interpretations. See
[`asic/docs/cleaned_to_derived.md`](../docs/cleaned_to_derived.md).

## Context and layer matrix

| Artifact | How it is created | Primary evidence |
|---|---|---|
| `production/pooled` | Supplied upstream on the cluster | Production pooled input audit |
| `demo/pooled` | `asic-pipeline generate-demo` | `demo_generation_manifest.json` and demo pooled audit |
| `mock/pooled` | `asic-pipeline generate-mock` | `mock_generation_manifest.json` and mock pooled audit |
| `<context>/translated` | `asic-pipeline pooled-to-translated` with the matching profile | `translation_manifest.json` |
| `<context>/cleaned` | Not implemented | Future `cleaning_manifest.json` |
| `<context>/derived/...` | Not implemented; each recipe reads accepted cleaned data | Future recipe-specific manifest |

The matching profiles are
[`production.yaml`](../config/datasets/production.yaml),
[`demo.yaml`](../config/datasets/demo.yaml), and
[`mock.yaml`](../config/datasets/mock.yaml). Never use one context's profile
with another context's artifacts.

## Handling rules

- Treat pooled and published translated artifacts as immutable.
- Never partially replace files inside a published artifact directory.
- Review the corresponding generation or stage manifest before approval.
- Do not commit Parquet files, generated manifests, or stay-level data.
- Do not copy production or demo artifacts off the authorized cluster.
- Do not treat mock data as formally anonymized or use it for inference.
- Keep aggregate audit reports under `asic/reports/` and Slurm logs under
  `asic/runs/`.

For the complete run order, start with [`asic/README.md`](../README.md).
