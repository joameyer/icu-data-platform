# Cross-hospital data-quality audit

**Status:** implemented, read-only; expanded-context and PBW-formula demo rerun pending; production rerun gated  
**Executable policy:** [`asic/config/translated/data_quality_audit.yaml`](../config/translated/data_quality_audit.yaml)  
**Policy version:** `1.10`

This audit answers whether the pooled source appears to have already resolved
cross-hospital unit, scale, definition, numeric-parsing, and invalid-value
problems. It profiles the translated artifact because that layer has stable
English names while retaining pooled values. It never changes a Parquet file.

The JSON report is evidence for human decisions before implementing cleaning
corrections. A warning is expected while known
legacy issues remain unresolved; warnings are not transformations.

## Inputs and outputs

The command reads only:

```text
asic/data/<context>/translated/static.parquet
asic/data/<context>/translated/dynamic.parquet
asic/config/translated/contract.yaml
asic/config/translated/data_quality_audit.yaml
```

It writes one aggregate report under `asic/reports/<context>/` named
`cross_hospital_data_quality_audit_<timestamp>.json`. It does not write to
`pooled/`, `translated/`, `cleaned/`, or `derived/`.

Run the structural translated-input audit first. Data-quality output is not
interpretable if schema, provenance, identifiers, or time keys fail.

## What is measured for every numeric field

Every translated numeric field is profiled except technical hospital codes,
`cluster_id`, and the dynamic relative-time key. The report contains, by table,
field, and hospital:

- row, finite, missing-or-NaN, positive-infinity, and negative-infinity counts;
- finite coverage rate;
- zero, negative, and exact `-1` counts;
- minimum, 1st percentile, Q1, median, Q3, 99th percentile, maximum, mean,
  standard deviation, IQR, and range width; and
- the quantile sample count and whether those quantiles are exact.

Counts, minima, maxima, means, ranges, invalid-value counts, and non-finite
counts use every row. Quantiles use a deterministic bottom-k sample of 20,000
non-missing values per hospital/field. They are exact when no more than 20,000
finite values exist and deterministic estimates otherwise. Sampling uses the
stay/time key, so results are reproducible and memory remains bounded for the
21.9-million-row production table.

## Cross-hospital distribution screen

The legacy algorithm is preserved exactly as a screening rule:

1. Include a hospital/field pair when it has at least 20 finite values.
2. Compare a field only when at least four hospitals are eligible.
3. Across the hospital summaries, calculate Q1 and Q3 for each of `min`,
   `median`, `iqr`, `max`, and `range_width`.
4. Flag hospital metrics outside the 1.5-IQR fences. If the across-hospital IQR
   is zero, only values different from the common value are flagged.

The audit additionally screens finite coverage rate with the same rule.
Distribution flags are review candidates. Case mix, measurement frequency, and
site-specific practice can create real differences, so a flag is not automatic
proof of a unit discrepancy.

## Focused follow-up after the initial demo review

Policy version 1.10 preserves the broad screen and adds exact, bounded evidence
for findings selected during human review of the first demo report. These are
audits, not corrections. Candidate values are calculated in memory, included
only as aggregate profiles in the JSON report, and never written to a data
artifact.

### Candidate scale hypotheses

| Field and hospital | In-memory candidate | Why it is being tested |
|---|---|---|
| `fio2`, UK03 | multiply by 100 | Demo values looked fractional while peers looked percent-scaled |
| `etco2`, UK04 | divide by `7.50062` | Exact conversion retained from the legacy pipeline |
| `norepinephrine_iv_cont`, UK03 | divide by 100 | Demo median then aligned with peer medians; source definition is still unknown |
| `hematocrit`, UK03 | multiply by 100 | Fraction-versus-percent pattern |
| `lymph_pct`, UK00 | multiply by 100 | Fraction-versus-percent pattern |
| `albumin`, UK08 | divide by 100 | Empirical peer-alignment probe only; source and target physical units are unconfirmed |
| `d_dimer`, UK03 and UK08 | multiply by 1000 | Tests the apparent split against UK00, UK02, and UK07 without using the other suspected low-scale site as a peer |
| `troponin`, UK03 | divide by 1000 | Tests an apparent thousand-fold assay/reporting-scale difference |
| `vt_per_kg`, UK06 | divide values above 1000 by 1000 | Exploratory only: this new pooled field matches the legacy per-ideal-body-weight field at UK06, which the old pipeline deemed unrecoverable |
| `vt_per_kg_ideal_body_weight`, UK06 | divide values above 1000 by 1000 | Exploratory only: retains evidence for the exact field that the legacy pipeline masked site-wide as unrecoverable mixed-scale |

Each report entry under `targeted_scale_hypotheses` includes the raw target-site
profile, the hypothetical candidate profile, the explicit peer-hospital scope,
unmodified peer profiles, the exact number of values considered and
numerically changed, and exact
before/after out-of-range counts where a review range is defined. Every entry
has a non-approved status. The albumin probe is explicitly labelled empirical
alignment rather than a unit conversion. Apparent peer alignment does not
approve a cleaning transformation.

The UK06 threshold is retained only as a diagnostic probe. The legacy code
explicitly classified `vt_per_kg_ideal_body_weight` as having “unrecoverable
mixed scales” and set the entire UK06 site/field pair to missing. It contains
no division-by-1000 recovery and no documented comparison of a hypothetically
converted distribution with peers. Therefore the thresholded v2 calculation
is new exploratory evidence, not a recovered legacy correction.

The production audit strengthens the conservative interpretation. For each of
the two UK06 fields, all 99,362 both-present values are exactly equal. The
bucket counts are 492 at or below zero, 43,331 in `(0, 30]`, 8 in `(30, 100]`,
187 in `(100, 1000]`, and 55,344 above 1000. Dividing only values above 1000
produces a hypothetical median of about 8.02, but 434 values remain outside
`[0, 30]` and the maximum remains about 981.1. This does not establish a safe
row-level recovery. Unless a new correction is separately justified and
approved, the legacy site-wide masking decision remains the cleaning
recommendation for UK06 `vt_per_kg_ideal_body_weight`. Because the new pooled
`vt_per_kg` field is identical there, it must also remain unresolved rather
than being automatically recovered.

“UK00's mislabeled tidal-volume field” refers specifically to the pooled source
column `individuelles_Tidalvolumen_pro_kg_idealem_Koerpergewicht`. Policy `1.3`
now moves its unchanged UK00 values to `vt_per_ideal_bw_total` and retains
`vt_per_kg_ideal_body_weight` only for non-UK00 hospitals. The source name says
mL/kg ideal body weight, but 320,028 of 320,030 finite UK00 production values
lie in `(200, 700]`, an
absolute-volume-like scale. This was not a named semantic finding in the legacy
pipeline: the old registry translated the source label literally and its
generic `0–30` mL/kg validity rule would mask almost every UK00 value. The v2
comparison also shows that the field is not simply a duplicate of `vt`: none
of 292,458 both-present rows agree within `0.001`. The defensible conclusion is
that the values are inconsistent with the source label—not that the precise
upstream provenance has been proven. The translated split prevents unlike
scales from sharing a field; it does not rename the values to `vt`, copy them,
or calculate a replacement.

### UK00 predicted-body-weight tidal-volume formula review

The production aggregates provide a specific alternative interpretation that
can be tested. The suspicious field's median, `412.476` mL, equals six times
the NIH ARDS Network predicted body weight for a 173 cm male. Its maximum,
`587.196` mL, similarly equals six times predicted body weight for a 205 cm
male. These aggregate coincidences are strong evidence but are not row-level
proof.

Policy version 1.10 therefore joins UK00's static `sex`, `height_cm`, and
`weight_kg` to dynamic rows by `stay_id_global` and calculates the tested PBW
convention:

```text
male PBW kg   = 50.0 + 0.91 * (height_cm - 152.4)
female PBW kg = 45.5 + 0.91 * (height_cm - 152.4)
```

The formula is from the
[NIH ARDS Network protocol](https://biolincc.nhlbi.nih.gov/media/studies/eden/Protocol%20EDEN%20only.pdf).
It is a review hypothesis, not confirmed documentation of the ASIC source
system.

The report section `predicted_body_weight_tidal_volume_audit` evaluates three
separate identities:

1. `vt_per_ideal_bw_total == 6 * PBW`. Agreement supports the hypothesis
   that the suspicious field is actually an absolute 6 mL/kg PBW target.
2. `vt_per_kg == vt / PBW`. Agreement supports interpreting `vt_per_kg` as
   delivered tidal volume normalized by the tested PBW formula.
3. `vt_per_ideal_bw_total == vt * PBW / weight_kg`. This directly tests
   whether the difference between the target and delivered `vt` is explained
   only by replacing actual weight with predicted weight.

The third identity is deliberately independent of the first two. A calculated
6 mL/kg PBW target can differ from delivered `vt` because of ventilator
settings and clinical management, even when actual body weight plays no role
in that difference.

The audit reports exact comparable, within-tolerance, and outside-tolerance
counts; residual quantiles; PBW and normalized-volume profiles; excluded
height/weight reference counts; and static/dynamic join completeness. Heights
outside 120–220 cm and actual weights outside 20–300 kg are excluded from the
corresponding calculation and counted rather than silently used. It writes no
identifiers and changes no data. The mock artifact must not be interpreted for
this review because independent dynamic-column shuffling destroys these
relationships; use demo first and production only after review.

Even exact formula agreement establishes an arithmetic construction, not
whether the field was prospectively prescribed, retrospectively calculated,
or created for another source-system purpose. The translated split records the
reviewed absolute-volume-like representation only; a more specific clinical
interpretation or any cleaned transformation remains gated on source metadata
or a separate explicit approval.

### Definition and mixed-scale review

`targeted_cross_hospital_comparisons` gives a compact per-hospital profile for
albumin, D-dimer, troponin, sufentanil, propofol, ketanest, morphine,
clonidine, both tidal-volume-per-kilogram fields, the separate UK00
`vt_per_ideal_bw_total` field, urea, and ScvO2. It also
reports the ratio between the largest and smallest nonzero absolute hospital
medians as a
descriptive review aid, not a decision threshold.

The added ScvO2 review was prompted by demo medians near 72.4 at UK00 but near
97 at UK02 and UK07; at UK07 its distribution matched SaO2. The audit now
compares ScvO2 row by row with both SaO2 and SpO2 without assuming equivalence.
UK08 urea is also listed explicitly because its demo median was about 27 versus
roughly 7–10 at peers. No urea conversion is proposed without metadata or
complete production evidence.

Exact bucket counts are reported under `targeted_bucket_audits` for:

- UK06 `vt_per_kg` and `vt_per_kg_ideal_body_weight`;
- UK00 `vt_per_ideal_bw_total`, which contains the unchanged
  absolute-volume-like source values; and
- UK08 `clonidine_iv_cont`, where low and high scales may coexist.

The policy has count guards for all 11 hospital-scale hypotheses, one explicit
row-level scale-entry audit, 13 comparison audits, four bucket audits, and one
cross-table PBW formula audit.
Removing one accidentally is a blocking audit failure.

### General scale-entry candidate discovery

The report screens all 44 dynamic fields whose preserved legacy invalid-value
rule has both a lower and an upper bound. For each finite raw value outside its
inclusive validity window, it tests multiplication and division by 10, 100,
and 1000.

A value is **uniquely recoverable** only when exactly one transformation enters
the field's validity window. If multiple transformations enter the window, it
is **ambiguous** and must not be recovered automatically. If none does, it is
**unrecoverable** by this screen. Existing in-range values are never
candidates.

The `scale_entry_candidate_discovery` report section provides:

- the bounded fields audited, unbounded legacy-rule fields excluded, and all
  other numeric fields excluded for lack of complete approved bounds;
- exact in-range, outside, unique, ambiguous, and unrecoverable counts;
- raw and hypothetical recovered numeric examples without stay identifiers;
- recovered-value profiles by transformation and hospital;
- comparison of each recovered median with the valid same-hospital 1st–99th
  percentile interval; and
- a descriptive pattern classification by hospital.

Pattern classification requires at least 20 finite values. A unique-candidate
fraction at or below 1% is labelled `rare_row_level_pattern`; a fraction at or
above 20% is labelled `possible_hospital_wide_or_systematic_scale_pattern`;
intermediate values remain mixed and unresolved. These configurable thresholds
organize review—they do not approve a correction. Fields with only a one-sided
or no legacy bound, including many long-tailed laboratory and dose variables,
are excluded rather than assigned invented recovery windows.

### Neighboring-measurement and related-field context

For review-selected fields where longitudinal or same-row physiology is useful,
the audit makes one additional bounded pass over `dynamic.parquet`. It evaluates
only uniquely recoverable candidates already found by the general screen. It
does not broaden candidate eligibility and does not correct a value.

The configured scopes are:

| Candidate field | Excluded scope | Same-row supporting field or formula |
|---|---|---|
| `core_temp` | none | none |
| `evlwi` | none | none |
| `ph_art` | none | none |
| `fio2` | UK03 | `fio2_set` |
| `map` | none | `(sbp + 2 * dbp) / 3` and whether recovered MAP lies between DBP and SBP |
| `spo2` | none | `sao2` |
| `sao2` | none | `spo2` |
| `scvo2` | none | none; its separate SaO2 and SpO2 relationship audits remain definition checks, not equivalence assumptions |

UK03 FiO2 is excluded because it is already a known site-wide
fraction-to-percent hypothesis. It remains in `targeted_scale_hypotheses` and
the general discovery counts; it is not misrepresented as an isolated entry
error.

The three newly included fields were selected from the complete production
discovery: `core_temp` had 70 uniquely recoverable candidates, `evlwi` had 32,
and `scvo2` had 5. Their temporal evidence is collected for review only. For
ScvO2, neither SaO2 nor SpO2 is designated as a same-row equivalent because
its site-specific meaning is unresolved; those relationships remain separate
descriptive audits.

For each selected candidate, the second pass finds the nearest strictly earlier
and strictly later valid measurement of the same field and ICU stay. The raw
candidate, all other invalid values, and measurements at the exact candidate
time are excluded as temporal neighbors. Evidence is summarized separately at
1, 4, and 8 hours:

- `recovered_is_closer`: the hypothetical recovered value is mathematically
  closer than the raw value to the median of the available nearest
  previous/next values;
- `recovered_is_not_closer`: temporal context exists but the recovered value is
  not closer; and
- `no_temporal_context`: no valid neighbor exists in that window.

These descriptive labels deliberately avoid saying that recovery is supported.
Being closer does not establish that the transformed value is clinically
credible or that the proposed factor represents the source unit.

Same-row related-field evidence remains a separate comparison, with
`not_applicable` for pH. A recovered MAP is classified as
`contradicts_recovery` whenever it is outside the simultaneous DBP–SBP
interval, even if it is numerically closer to the SBP/DBP-derived MAP than the
raw value. The report records this explicitly as
`recovered_map_outside_dbp_sbp_interval`. No tolerance is invented.

SaO2 and SpO2 remain distinct measurements: proximity never merges their
meanings. The demo candidates include transformations that are mathematically
closer to neighboring saturations but remain far away, such as `4 -> 40` near
a neighboring value around `96`. Such findings currently justify masking the
invalid raw value during cleaning, not automatic scale recovery. A saturation
recovery rule would require separate field-level evidence and explicit human
approval after production review.

The report section `scale_entry_candidate_context` contains aggregate counts by
field, hospital, transformation, and time window plus bounded numeric examples.
It contains no stay identifier or candidate time. Candidate collection is
capped at 100,000 rows for memory safety; reaching the cap, encountering a
candidate without a usable stay/time key, or failing to scan the complete
second pass is a **blocking** completeness failure rather than a partial result
presented as complete. When no selected candidates exist, no second pass is
needed.

The separate `scale_entry_candidate_review_coverage` section maps every
field/hospital scope with at least one uniquely recoverable candidate to one of
three explicit review routes: this context pass, a targeted site-scale audit,
or an explicit row-level scale-entry audit. Any uncovered scope is a blocking
failure. This prevents a candidate that appears only in production from being
silently omitted from human review.

### Explicit isolated scale-entry rules

Hospital-wide unit discrepancies and isolated row-level entry errors are
audited separately. The first row-level rule targets arterial pH:

```text
raw pH is outside [6.8, 7.8]
and raw pH / 100 is inside [6.8, 7.8]
```

For example, `745` is reported as the hypothetical recovery `7.45`. An
already plausible pH is never divided, and an outlier that remains outside the
window after division is reported as unrecoverable. The report section
`row_level_scale_entry_audits` contains exact candidate counts and bounded
raw/recovered numeric examples by hospital. It contains no stay identifiers
and does not modify data.

The pH rule is an audit candidate pending review of demo and production
results. A future cleaning rule may be approved only if the variable has a
strong canonical physiologic window, the factor and direction are explicit,
the raw value is invalid, and exactly one proposed transformation yields a
valid value. Variables with broad or long-tailed distributions must not use
this recovery pattern merely because a transformed value looks common.

## Legacy findings retained explicitly

The policy records its legacy source files and includes count guards that fail
if any recovered item is accidentally removed. It retains 44 legacy
invalid-value rules, expanded to 46 current-field checks, five targeted
semantic findings, and six static `-1` sentinel rules.

### Known site-specific unit and definition findings

| Current field | Hospital | Legacy finding | Legacy action | Current read-only verification |
|---|---|---|---|---|
| `etco2` | UK04 | Values were on a different, Pa-labelled scale | Divide by `7.50062` | Report observed UK04 quantiles, candidate divided quantiles, and peer medians for human review |
| `vt_per_kg_ideal_body_weight` | UK06 | Unrecoverable mixed scales | Set the site/field pair missing | Quantify the still-present modes; do not reinterpret the exploratory `/1000` probe as a legacy correction |
| `norepinephrine_iv_cont` | UK03 | Off-scale; exact source unit/definition unresolved | Set the site/field pair missing | Check whether UK03 is already entirely missing |
| `clonidine_iv_cont` | UK08 | Sharply different from peers; exact source unit/definition unresolved | Set the site/field pair missing | Check whether UK08 is already entirely missing |
| `ie_ratio` | All | I:E versus E:I convention unresolved | Retain pending metadata; exclude from analysis | Report each hospital median and reciprocal median |

The UK06 tidal-volume conclusion comes directly from the legacy harmonization
code. No legacy documentation or implementation supports partial recovery by
division by 1000. The current `vt_per_kg` source was absent from the legacy
translation registry, so its status is inferred from its exact equality with
the legacy-labelled field at UK06 and remains unresolved.

The `etco2` conversion formula is preserved exactly from the old code. The old
label and divisor are not treated as physically validated unit documentation;
no source unit document exists. The observed and candidate distributions must
therefore be reviewed before approving a correction.

### Legacy invalid-value rules

Bounds are inclusive. “Zero invalid” means zero is flagged in addition to the
strict below-minimum and above-maximum checks. Values are counted and reported,
never masked.

| Legacy concept | Current translated field(s) | Allowed bounds | Zero invalid |
|---|---|---:|:---:|
| albumin | `albumin` | no bound | yes |
| cardiac_index_bolus | `cardiac_index_bolus` | 0 to 15 | yes |
| cardiac_index_cont | `cardiac_index_cont` | 0 to 15 | yes |
| cardiac_output_bolus | `cardiac_output_bolus` | 0 to 25 | yes |
| cardiac_output_cont | `cardiac_output_cont` | 0 to 25 | yes |
| compliance | `compliance` | 0 to 300 | yes |
| core_temp | `core_temp` | 25 to 45 | no |
| creatinine | `creatinine` | no bound | yes |
| cvp | `cvp` | -20 to 60 | no |
| dbp | `dbp` | 0 to 200 | yes |
| delta_p | `delta_p_computed`, `delta_p_reported` | 0 to 60 | no |
| evlwi | `evlwi` | 0 to 80 | yes |
| fio2 | `fio2` | 20 to 100 | no |
| gedvi | `gedvi` | 0 to 2000 | yes |
| heart_rate | `heart_rate` | 0 to 250 | yes |
| hematocrit | `hematocrit` | 0 to 80 | yes |
| hemoglobin | `hemoglobin` | 0 to 25 | yes |
| ie_ratio | `ie_ratio` | 0 to 10 | yes |
| inr | `inr` | 0 to 15 | yes |
| insp_pressure | `insp_pressure` | 0 to 80 | no |
| lactate_art | `lactate_art` | 0 to 30 | yes |
| map | `map` | 0 to 250 | yes |
| paco2 | `paco2` | 0 to 150 | yes |
| pao2 | `pao2` | 0 to 760 | yes |
| pap_dias | `dpap` | 0 to 100 | yes |
| pap_mean | `mpap` | 0 to 100 | yes |
| pap_sys | `spap` | 0 to 150 | yes |
| pcwp | `pcwp` | 0 to 50 | yes |
| peep | `peep` | 0 to 30 | no |
| pf_ratio | `pf_ratio` | 0 to 2500 | yes |
| ph_art | `ph_art` | 6.8 to 7.8 | no |
| platelets | `platelets` | 0 to 2000 | yes |
| ptt | `ptt` | 0 to 300 | yes |
| resp_rate | `resp_rate` | 0 to 80 | yes |
| sao2 | `sao2` | 50 to 100 | no |
| sbp | `sbp` | 0 to 300 | yes |
| scvo2 | `scvo2` | 20 to 100 | no |
| sofa | `sofa_score_without_gcs`, `sofa_score_unspecified` | 0 to 24 | no |
| spo2 | `spo2` | 30 to 100 | no |
| stroke_index_cont | `stroke_index_cont` | 0 to 100 | yes |
| stroke_volume_cont | `stroke_volume_cont` | 0 to 300 | yes |
| svri | `svri` | 0 to 10000 | yes |
| vt | `vt`, `vt_per_ideal_bw_total` | 0 to 2000 | yes |
| vt_per_kg_ibw | `vt_per_kg_ideal_body_weight` | 0 to 30 | yes |

For each current field, the report also gives the total invalid count, each
hospital’s share, the dominant hospital, and a legacy-compatible concentration
flag when at least five invalid values exist and one hospital contributes at
least 50%.

The old `sofa` field had combined two German sources. The v2 translated layer
deliberately keeps them separate, so the legacy 0–24 rule is evaluated on both
without merging them. The other SOFA/iSOFA fields remain universally profiled
but do not receive newly invented clinical cutoffs in this policy version.

### Legacy static missing sentinels

The old pipeline interpreted `-1` as missing in:

- `weight_group`;
- `hosp_los`;
- `icu_los`;
- `icu_readmit`;
- `dialysis_free_days`; and
- `vent_free_days`.

Translation policy `1.3` normalizes `weight_group`, `hosp_los`,
`dialysis_free_days`, and `vent_free_days`. The current production input has no
`-1` values in `icu_los` or `icu_readmit`, so those two legacy findings remain
read-only checks rather than active transformations. The audit checks all six
so older information is not lost and reports remaining sentinels by hospital.
It does not normalize any newly detected value.

### Legacy non-numeric parsing

The old pipeline audited raw non-numeric dynamic values. At UK04 it treated
`storniert`, `storno`, `kein material`, `falsches mater.`, `falsches mater`,
`fal.volumen`, `geronnen`, `entfällt`, `entfaellt`, and `folgt` as missing;
converted decimal commas; interpreted `<x` as zero and `>x` as `x`; and parsed
other numeric strings. I:E values could use `a:b` or `a/b`, were calculated as
`a / b`, and became missing for a zero denominator.

The pooled and translated Parquet schemas are already numeric. Consequently,
the new audit can verify physical numeric types, missingness, infinities,
ranges, and distributions, but it cannot reconstruct discarded source strings
or prove which parser created a missing value. The report marks this check
`skipped` and embeds the legacy rules. Re-auditing raw parsing would require the
original hospital-level files or upstream pooling logs, neither of which is
available.

## Additional relationship and availability evidence

The report includes, by hospital:

- `delta_p_computed` versus `insp_pressure - peep` with absolute tolerance
  `0.001`;
- `delta_p_computed` versus `delta_p_reported` with no equivalence assumption;
- `dialysis_free_days` versus `vent_free_days`, because their reviewed
  translation manifests had identical missing-sentinel counts;
- `sofa_score_unspecified` versus `sofa_total_score`;
- reported versus calculated respiratory SOFA;
- iSOFA total versus the sum of six components when all are present;
- `vt_per_kg` versus `vt_per_kg_ideal_body_weight` by hospital;
- UK00 `vt` versus `vt_per_ideal_bw_total`; and
- `scvo2` versus `sao2` and `spo2`, to test the unresolved site-specific
  definition; and
- non-missing iSOFA counts by hospital, with values outside UK00 flagged.

Except for the explicitly computed driving-pressure formula, comparison does
not imply that paired fields should agree. SOFA/iSOFA provenance and definitions
remain unresolved.

## Human-in-the-loop run procedure

Run all commands from the project root on the cluster.

### 1. Test and structurally audit demo

```bash
./.venv/bin/python -m pytest -q

./.venv/bin/python -m asic_pipeline audit-translated \
  --config asic/config/datasets/demo.yaml
```

Every blocking structural check must pass.

### 2. Run the read-only demo quality audit

```bash
./.venv/bin/python -m asic_pipeline audit-cross-hospital \
  --config asic/config/datasets/demo.yaml
```

Review the newest report under `asic/reports/demo/`. In order, inspect:

1. `checks` for blocking failures and advisory warnings;
2. `known_legacy_semantic_findings` for evidence that old corrections are or
   are not already reflected upstream;
3. `legacy_invalid_value_findings`, prioritizing nonzero and concentrated
   findings;
4. `cross_hospital_distribution_issues`, especially median and range-scale
   flags;
5. `targeted_scale_hypotheses`, including exact before/after range counts;
6. `scale_entry_candidate_discovery` for broad bounded-field discovery;
7. `scale_entry_candidate_review_coverage`, confirming that the uncovered
   scope and candidate counts are both zero;
8. `scale_entry_candidate_context` for 1/4/8-hour temporal support and
   same-row physiologic comparisons, confirming `collection_complete: true`;
9. `row_level_scale_entry_audits` for explicit isolated recovery rules;
10. `predicted_body_weight_tidal_volume_audit`, reviewing all three formula
   comparisons and confirming complete static/dynamic linkage;
11. `targeted_bucket_audits` and `targeted_cross_hospital_comparisons` for mixed
   scales and unresolved definitions;
12. `relationship_audits` for driving pressure, tidal-volume fields, ScvO2,
   free days, SOFA, and iSOFA; and
13. `numeric_profiles` for any additional field/hospital requiring deeper
   review.

Stop here for human review. Do not implement corrections from the demo report.

### 3. Run production only after demo review

Production must run on a compute node:

```bash
mkdir -p asic/runs/production
sbatch asic/run_cross_hospital_audit_production.sh
```

The job repeats the structural translated audit, then runs the read-only
cross-hospital audit. Review the Slurm log and newest production JSON. No
cleaning transformation should be implemented until each targeted
correction is explicitly approved.
