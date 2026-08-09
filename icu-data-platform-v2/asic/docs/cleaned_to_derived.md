# Cleaned to derived

**Implementation status:** design only; no derived generator is implemented  
**Upstream stage:** [`translated_to_cleaned.md`](translated_to_cleaned.md)

`derived/` contains new analytical representations and features created from
an accepted cleaned artifact. It never replaces or modifies cleaned data.

## Boundary and recipe layout

```text
asic/data/<context>/cleaned/
    -> asic/data/<context>/derived/<recipe>/...
```

Time blocking is one recipe:

```text
asic/data/<context>/derived/time_blocking/<resolution>/
```

Each resolution—such as `4h`, `8h`, `12h`, or `24h`—must be generated
independently and directly from cleaned data. A 12-hour artifact must never be
constructed from an 8-hour artifact. This keeps cleaned data reusable and
prevents one analysis choice from silently affecting another.

Every blocking recipe must freeze:

- the ICU-relative alignment origin;
- left/right interval inclusivity and boundary behavior;
- aggregation rules by variable type;
- handling of multiple values, missing intervals, and partially observed
  blocks;
- treatment of static fields;
- output keys and column order; and
- a manifest containing input identity, resolution, rule versions, row counts,
  missingness evidence, and publication status.

Other future recipes may be sibling directories under `derived/` rather than
being embedded in time blocking.

## Mortality derivation

Translated and cleaned data retain `discharge_status`, `death_status`, and
`hospital_mortality_reported` separately. A future derived mortality result
may use these implications:

| Evidence | Hospital mortality | ICU mortality |
|---|---|---|
| `death_status = discharged_alive` | false | false |
| `death_status = died_in_icu` | true | true |
| `death_status = died_in_hospital` | true | false |
| detailed status missing and `hospital_mortality_reported = false` | false | false |
| detailed status missing and `hospital_mortality_reported = true` | true | unknown |

All source columns remain available. If sources disagree, the derived result
must be missing and an explicit conflict flag must be set; no source silently
takes priority. The exact use of `discharge_status` still requires row-level
agreement review.

## Mechanical-ventilation duration

The legacy pipeline excluded a stay unless at least one mechanical-ventilation
episode lasted at least 24 hours. The new pipeline must not apply that cohort
filter. A future derived stay-level flag will instead indicate mechanical
ventilation for at least 24 hours.

Before implementation, the following must be frozen and reviewed:

- marker variables and qualifying modes;
- episode start/end construction;
- allowable observation gaps;
- whether 24 hours is inclusive;
- how multiple episodes are handled; and
- the distinction between `false` and `unknown` under incomplete evidence.

## Other derived fields

If needed, `hours_since_icu_admission` may be derived exactly as
`minutes_since_icu_admission / 60`. It must not replace either source time
representation. Any other analysis-facing score, outcome, feature, or cohort
indicator belongs to a named derived recipe with its own tests and manifest.

No rule in this document is executable yet. Each recipe will be implemented
and approved demo-first with a human checkpoint before production use.
