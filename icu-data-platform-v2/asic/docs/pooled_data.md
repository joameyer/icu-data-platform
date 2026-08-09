# Pooled ASIC data

This is the stable human-readable reference for the data entering the v2
pipeline. The executable schema is
[`asic/config/pooled/contract.yaml`](../config/pooled/contract.yaml),
currently contract version `1.2` (frozen on 2026-08-02 after complete
production validation).

## Scope and provenance

The pipeline starts from an already-pooled ASIC dataset. No upstream pooling
code, pooling documentation, or unit documentation is known. Consequently,
the pooled schema is treated as a strict observed interface—not as evidence
that cross-hospital names, units, definitions, non-numeric values, or invalid
values have already been harmonized correctly.

The complete accepted production input contained 14,483 static rows and
21,876,966 dynamic rows. The same contract is used in three isolated contexts:

- `production`: complete protected data on the cluster;
- `demo`: an unchanged stay-level subset of production for bounded exploratory
  audits; and
- `mock`: shuffled/remapped development data that preserves the expected
  structure but is not suitable for clinical or statistical conclusions.

Each context follows the same artifact layout:

```text
asic/data/<context>/
├── pooled/
│   ├── static.parquet
│   └── dynamic.parquet
├── translated/
├── cleaned/
└── derived/
```

The pooled files are inputs. Later stages write new sibling directories and
never overwrite pooled data.

## Schema summary

`static.parquet` has 21 columns and one row per ICU stay.
`dynamic.parquet` has 139 columns and longitudinal rows keyed by ICU stay and
minutes since ICU admission. Column names, order, and Arrow types must match
the executable contract exactly; schema drift is blocking.

Production and demo share the production Arrow schema. Mock data has only the
following explicit representation overrides:

| Fields | Production and demo | Mock |
|---|---|---|
| Text fields, including `Pseudo-ID` | `large_string` | `string` |
| Static `Phase` | `int8` | `double` |
| Dynamic `ARDS_Diagnose_App` | `int8` | `double` |

These exceptions do not authorize implicit casting of production data. The
complete ordered source schema is intentionally kept in one place:
[`contract.yaml`](../config/pooled/contract.yaml).

## ICU-stay and hospital identifiers

- One `Pseudo-ID` represents one ICU stay; there is no separate patient-level
  identifier.
- `Pseudo-ID` is the source of `stay_id_global` in the translated layer.
- The suffix after the final colon in `Pseudo-ID` is the authoritative hospital
  code. The prefix is not treated as a patient identifier.
- `hid` is retained independently and must agree with that suffix on every row.
- Missing/malformed IDs, non-integral `hid`, or suffix/`hid` disagreement are
  blocking.
- Static `Pseudo-ID` must be unique. Every static stay must have dynamic rows,
  and every dynamic stay must have a static row.

| Source hospital code | Canonical translated ID |
|---:|---|
| 0 | `asic_UK00` |
| 2 | `asic_UK02` |
| 3 | `asic_UK03` |
| 4 | `asic_UK04` |
| 6 | `asic_UK06` |
| 7 | `asic_UK07` |
| 8 | `asic_UK08` |

No other hospital is expected; in particular, UK01 is absent.

## Time semantics

`Zeit_ab_Aufnahme` is elapsed time since ICU admission in minutes. `timeidx`
encodes the same relative duration as a timestamp anchored to
`2020-01-01 00:00:00`. It is not a real admission or observation date and must
never be interpreted as calendar time.

The pooled audit requires exact agreement between both representations at
zero-second tolerance. Missing/unparseable time, disagreement, or duplicate
`(Pseudo-ID, Zeit_ab_Aufnahme)` keys is blocking.

## Known source semantics

### Mortality

The source carries three related, non-identical fields:

| Source field | Known meaning |
|---|---|
| `KH-Sterblichkeit` | Hospital mortality: string `true`, string `false`, or missing. |
| `Sterblichkeit` | `0` = discharged alive; `ICU` = died in ICU; `KH` = died later in hospital; literal `nan`/null = missing. |
| `Entlassgrund_(verlegt_intern,_verlegt_extern,_verstorben)` | Discharge disposition, including transferred or died. |

Some hospitals supplied death location, while others supplied only hospital
survival. All three sources remain separate in translated and cleaned data.
Consolidation belongs to a derived stage and must expose conflicts rather than
silently choosing one source.

### SOFA and iSOFA

`SOFA`, `Organversagen:_SOFA_Score_ohne_GCS`, the SOFA component/total fields,
and all iSOFA component/total fields are distinct unresolved concepts. The
working hypothesis is that SOFA was hospital-reported and iSOFA was derived
retrospectively, but there is no documentation and the hypothesis is not
treated as fact. iSOFA is known to be populated only for UK00. Nothing in this
group may be merged or declared authoritative yet.

### Driving pressure

Uppercase `DeltaP` was retrospectively calculated as `P_EI - PEEP`. Lowercase
`deltaP` is most likely hospital-reported. They often agree but can differ and
therefore remain separate as `delta_p_computed` and `delta_p_reported`.

## What the pooled audit proves

Run the strict read-only audit with a dataset profile, for example:

```bash
./.venv/bin/python -m asic_pipeline audit-pooled \
  --config asic/config/datasets/demo.yaml
```

The audit checks both files are readable and non-empty; exact columns, order,
and context-specific types; identifier format and cross-field agreement;
expected hospital coverage; static/dynamic stay linkage; exact time
equivalence; unique dynamic stay/time keys; and metadata-versus-scan row
counts. It writes an aggregate JSON report under
`asic/reports/<context>/` without stay identifiers.

Passing this audit proves conformity to the observed input contract. It does
not prove clinical validity, unit consistency, semantic consistency, or that
upstream parsing was lossless.

## Deliberately unresolved at pooled input

- cross-hospital unit and definition discrepancies;
- evidence of upstream unit or semantic corrections;
- whether non-numeric dynamic values were parsed without loss;
- physiological and laboratory invalid-value rules;
- meanings of `Zeit_seit_Studienbeginn`, `Phase`, and `Cluster-ID`;
- ARDS application codes and therapy-review field provenance;
- SOFA/iSOFA provenance and total/component relationships;
- mortality consolidation; and
- the mechanical-ventilation-at-least-24-hours definition.

These uncertainties are preserved explicitly so that a future audit, cleaning
rule, or derived rule cannot be mistaken for an upstream fact.

## Contract history

| Version | Change |
|---|---|
| 1.0 | Initial schema derived from the schema-preserving mock dataset. |
| 1.1 | Production Arrow types became authoritative; explicit mock overrides were recorded. |
| 1.2 | Exact relative/anchored time agreement and unique dynamic stay/time keys became blocking after passing on complete production data. |
