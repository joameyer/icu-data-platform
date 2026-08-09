# Architecture and contracts

## Repository boundary

MIMIC belongs in `icu_data_platform_v3/mimic`, not in the analysis repository
and not in a new repository. This keeps protected-source extraction and reusable
blocking beside the ASIC data platform while the isolated profile prevents a
narrow analysis from forcing extraction of unrelated fields. A future analysis
adds another versioned profile and the union of explicitly approved sources; it
does not silently broaden this profile.

```text
mimic/
  config/{contracts,profiles,registries}/
  docs/
  data/                  # protected/generated; ignored except boundary README
src/mimic_iv_pipeline/   # isolated implementation package
tests/mimic/             # synthetic tests only
```

## Layer graph

`MIMIC-IV 3.1 source -> cohort/static + canonical selected events -> {15m, 8h}`

Both block products read the canonical event manifest directly. The
analysis-facing product is the independent eight-hour layer. A build must fail
if its input artifact type or manifest identifies a blocked layer.

## Canonical static schema

One row per candidate `stay_id`, including excluded candidates:

| Group | Fields |
|---|---|
| Private keys | `subject_id`, `hadm_id`, `stay_id` |
| Dataset/site | `dataset_id`, `site_id` |
| Official time | `intime`, `official_outtime` |
| Cohort evidence | age, attributed service and service time, normalized discharge location, first-stay rank, every exclusion reason, `supervised_eligible` |
| Outcome | nullable source mortality and validated binary hospital mortality |
| Predictors/inputs | age, sex, median valid observed height and weight, continuous BMI, ICD-10 phenotypes plus ICD-version coverage |
| Provenance | source release, contract/profile versions, source-row references, build ID |

BMI is `median(valid observed weight kg) / median(valid observed height m)^2`
using measurements in the official ICU interval, inclusive. No sex- or
cohort-based imputation is allowed. Missing height or weight gives null BMI.

## Canonical selected-event schema

One row represents one selected source measurement (or one separately approved
rolling fluid-balance value). It retains private keys, actual event time,
integer microsecond offset from official `icustays.intime`, logical variable,
role/family, source table/item ID/value/unit/row order, normalized value/unit,
conversion ID, plausibility status, eligibility, and all rejection reasons.
Rejected selected rows remain auditable but never enter aggregates.
Blood-gas specimen item `52033` is retained as a specimen-linked audit marker;
it is not a predictor and does not by itself resolve arterial eligibility.

MIMIC clinical timestamps are kept in their native timezone-naive, shifted
calendar representation. The pipeline does not falsely localize them to UTC.
All comparisons require the same datetime form and offsets use exact integer
microseconds.

## Time intervals

For resolution `delta`:

- admission is the singleton `{0}`;
- post-admission block `k >= 1` is `((k-1)delta, k delta]`;
- pre-admission block `j >= 1` is `[-j delta, -(j-1)delta)` and is separately
  labelled;
- the fixed prehistory is 72 hours;
- official `icustays.outtime` determines the post-admission grid;
- an event exactly on a positive boundary belongs to that right-labelled block;
- an event exactly on a negative boundary belongs to the block beginning there;
- empty admission, pre, and post blocks are retained;
- a final nonempty-duration partial block stores both its nominal boundary and
  actual official-outtime end.

The same convention applies at 15 minutes and 8 hours. This is easier to audit
and prevents boundary-dependent semantic drift between resolutions.

Vitals produce median/minimum/maximum; laboratories produce median; an approved
rolling 24-hour fluid balance will produce last only. Every numeric predictor
also exposes count, last observed value, and actual last observation time.
Ties are ordered by event offset, source table, item ID, and source row number.
There is no LOCF, imputation, interpolation, encoding, scaling, or time
weighting in canonicalization or blocking.

## Bounded-memory production design

The authorized production implementation should scan only approved columns and
item IDs with PyArrow batches, partition canonical Parquet by source domain and
stable stay bucket, and externally sort each partition by the canonical order.
At most one stay (or one bounded stay bucket during external sort) is aggregated
in memory. Fifteen-minute and eight-hour workers separately stream the same
immutable canonical partitions. Atomic, run-scoped output directories and
content-hashed manifests prevent overwrite and mixed-lineage reads.

## Conservation and audit invariants

1. Every selected source row is exactly one of accepted, rejected with reason,
   outside-time-scope, or unresolved-human-gate; counts conserve by source.
2. No unregistered table, item ID, or logical variable can enter canonical data.
3. Unit-conversion accounting conserves source rows and preserves source values.
4. Each eligible stay has exactly one static row, one admission block, the
   contracted prehistory grid, and every post block through official outtime.
5. Every accepted in-scope event maps to exactly one block per resolution.
6. Block counts equal assigned canonical-event counts per stay and variable.
7. Empty and terminal-partial blocks contain no fabricated values.
8. Last value/time is one deterministically identified canonical row.
9. The 8-hour manifest names canonical events as its direct parent and never a
   15-minute artifact.
10. Ventilation markers never appear in predictor columns.
11. Row-level private evidence and identifiers never enter public reports.
12. Repeated runs from identical manifests/contracts have identical schemas,
   ordering, counts, and hashes.

## Test boundary

Local tests use invented identifiers and timestamps only. Synthetic cases cover
cohort exclusions and precedence; service attribution; ties and boundaries;
negative, zero, exact-right-boundary, empty, and terminal-partial blocks;
unit conversions and post-conversion ranges; observed-only BMI; ICD-10 prefix
matching and ICD-9 uncertainty; deterministic last selection; independence of
the two resolutions; and privacy-field rejection.

After separate authorization, cluster integration tests should add aggregate
source/item coverage, cohort funnel counts, official-time validity, conversion
and plausibility counts, event-to-block conservation, bounded peak memory,
manifest lineage, rerun determinism, and comparison with legacy aggregates.
Legacy comparison is evidence only—not an equality oracle.
