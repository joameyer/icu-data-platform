# Pooled to translated

**Implementation status:** human-reviewed and implemented  
**Executable policy:** [`asic/config/pooled_to_translated/policy.yaml`](../config/pooled_to_translated/policy.yaml)  
**Policy version:** `1.3`  
**Pipeline version:** `0.6.3`

This is the single readable reference for every implemented change between the
pooled and translated ASIC layers. The YAML policy is the executable source of
truth. A change is complete only when the policy, this guide, tests, and version
metadata are updated together.

## Stage boundary

```text
asic/data/<context>/pooled/ -> asic/data/<context>/translated/
```

The stage reads contract-valid `static.parquet` and `dynamic.parquet`, streams
the dynamic table in bounded batches, and writes files with the same names plus
`translation_manifest.json`. It performs only the reviewed representation
changes documented here.

It does not filter rows, derive a cohort, consolidate mortality, correct units,
mask general invalid values, impute, clean physiologic values, or build derived
data. Row count and row order are preserved. Every non-empty source concept
remains represented.

## Resulting schema

| Table | Pooled columns | Alias merges | Conditional drops | Added fields | Translated columns |
|---|---:|---:|---:|---:|---:|
| Static | 21 | 0 | 0 | 1 | 22 |
| Dynamic | 139 | 8 | 1 | 2 | 132 |

`hospital_id` is added to both tables by formatting the authoritative
`Pseudo-ID` suffix as `asic_UKNN` after verifying that it agrees with `hid`.
`hid` itself remains available as `hospital_code_source`. Both source time
representations remain unchanged; no hours-based derivative is emitted by this
stage.

The second added dynamic field is `vt_per_ideal_bw_total`. It is a lossless,
hospital-specific semantic split of the pooled source
`individuelles_Tidalvolumen_pro_kg_idealem_Koerpergewicht`:

- at UK00, the unchanged source values are moved to
  `vt_per_ideal_bw_total`, and `vt_per_kg_ideal_body_weight` is missing;
- at every other hospital, the unchanged source values remain in
  `vt_per_kg_ideal_body_weight`, and `vt_per_ideal_bw_total` is missing; and
- the manifest proves that every non-missing source value appears in exactly
  one output field and that neither field contains values outside its approved
  hospital scope.

This split does not calculate tidal volume, predicted body weight, or a unit
conversion. It prevents absolute-volume-like UK00 values from sharing a column
with per-kilogram-labelled values while preserving the source values exactly.
Production review found 320,030 finite UK00 source values: 320,028 were in
`(200, 700]`, and 294,608 of 320,028 formula-comparable rows matched
`6 * predicted body weight` within `0.01` mL. The arithmetic evidence supports
the new representation, but the field's upstream reported-versus-derived
provenance remains unresolved.

## Complete column-name mapping

Repeated output names below occur only for the eight approved alias groups.
They produce one output column according to the merge rules later in this
document. The all-missing duplicate ARDS field is the sole conditional drop.

### Static columns

| Pooled source | Translated output |
|---|---|
| `Pseudo-ID` | `stay_id_global` |
| `Cluster-ID` | `cluster_id` |
| `Zeit_seit_Studienbeginn` | `time_since_study_start` |
| `Phase` | `study_implementation_phase` |
| `clusterGeschlecht` | `sex` |
| `clusterKoerpergewicht` | `weight_group` |
| `clusterKoerpergroesse` | `height_group` |
| `BMI` | `bmi_group` |
| `Liegedauer_KH` | `hosp_los` |
| `Liegedauer_ICU` | `icu_los` |
| `Wiederaufnahme_ICU` | `icu_readmit` |
| `clusterAlter` | `age_group` |
| `Entlassgrund_(verlegt_intern,_verlegt_extern,_verstorben)` | `discharge_status` |
| `Sterblichkeit` | `death_status` |
| `ICD-10_Codes` | `icd10_codes` |
| `weightKg` | `weight_kg` |
| `heightcm` | `height_cm` |
| `KH-Sterblichkeit` | `hospital_mortality_reported` |
| `hid` | `hospital_code_source` |
| `Dialyse_(dialysefreie_Tage)` | `dialysis_free_days` |
| `Beatmungsfreie_Tage` | `vent_free_days` |

### Dynamic columns

| Pooled source | Translated output |
|---|---|
| `24h-Bilanz_(Fluessigkeiten-Einfuhr_vs_-Ausfuhr)` | `fluid_balance_24h` |
| `AF` | `resp_rate` |
| `AF_spontan` | `spont_resp_rate` |
| `ARDS_Diagnose_App` | `ards_diagnosis_app` |
| `ARDS_Diagnose_App_duplicated_0` | *drop after all-missing check* |
| `Albumin` | `albumin` |
| `Amylase` | `amylase` |
| `BE_arteriell` | `base_excess_art` |
| `BNP` | `bnp` |
| `Bicarbonat_arteriell` | `bicarbonate_art` |
| `Bilirubin_ges.` | `bilirubin_total` |
| `CK` | `ck` |
| `CK-MB` | `ck_mb` |
| `CRP` | `crp` |
| `Clonidin_intravenoes_kontinuierlich` | `clonidine_iv_cont` |
| `Compliance` | `compliance` |
| `Creatinkinase` | `ck` |
| `D-Dimere` | `d_dimer` |
| `DAP` | `dbp` |
| `DPAP` | `dpap` |
| `DeltaP` | `delta_p_computed` |
| `Dexamethason_intravenoes_bolusweise` | `dexamethasone_iv_bolus` |
| `Dexmedetomidin_intravenoes_kontinuierlich` | `dexmedetomidine_iv_cont` |
| `Dobutamin_intravenoes_kontinuierlich` | `dobutamine_iv_cont` |
| `ECMO` | `ecmo` |
| `ECMO_FiO2` | `ecmo_o2` |
| `EVLWI` | `evlwi` |
| `Epinephrin_intravenoes_kontinuierlich` | `epinephrine_iv_cont` |
| `Extrakorporaler_Blutfluss` | `extracorp_blood_flow` |
| `Extrakorporaler_Gasfluss_(O2)` | `extracorp_o2_flow` |
| `FeO2` | `feo2` |
| `Fentanyl_intravenoes_kontinuierlich` | `fentanyl_iv_cont` |
| `FiO2` | `fio2` |
| `FiO2_eingestellt` | `fio2_set` |
| `Fludrocortison_peroral_bolusweise` | `fludrocortisone_po_bolus` |
| `Furosemid_intravenoes_kontinuierlich` | `furosemide_iv_cont` |
| `GEDVI` | `gedvi` |
| `GOT` | `ast` |
| `GPT` | `alt` |
| `Gaszusammensetzung_(%O2)` | `ecmo_o2` |
| `HF` | `heart_rate` |
| `HI_(Bolus)` | `cardiac_index_bolus` |
| `HI_(kontinuierlich)` | `cardiac_index_cont` |
| `HZV_(Bolus)` | `cardiac_output_bolus` |
| `HZV_(kontinuierlich)` | `cardiac_output_cont` |
| `Haematokrit` | `hematocrit` |
| `Haemoglobin` | `hemoglobin` |
| `Harnstoff` | `urea` |
| `Horowitz-Quotient_(ohne_Temp-Korrektur)` | `pf_ratio` |
| `Hydrocortison_intravenoes_bolusweise` | `hydrocortisone_iv_bolus` |
| `I:E` | `ie_ratio` |
| `I:E_eingestellt` | `ie_ratio_set` |
| `IL-6` | `il6` |
| `INR` | `inr` |
| `Inhalatives_Iloprost` | `inhaled_iloprost` |
| `Inhalatives_NO` | `inhaled_no` |
| `Interleukin_6` | `il6` |
| `Isofluran_inhalativ` | `isoflurane_inh` |
| `Ketanest_intravenoes_kontinuierlich` | `ketanest_iv_cont` |
| `Koerperkerntemperatur` | `core_temp` |
| `Kreatinin` | `creatinine` |
| `Körpertemperatur` | `core_temp` |
| `LDH` | `ldh` |
| `Lagerungstherapie` | `position_therapy` |
| `Laktat_arteriell` | `lactate_art` |
| `LesebestaetigugnTherapie_utc` | `therapy_read_confirmation_utc` |
| `LesebestaetigungSchweregrad` | `severity_read_confirmation` |
| `LesebestaetigungTherapie_utc` | `therapy_read_confirmation_utc` |
| `Leukozyten` | `wbc` |
| `Levosimendan_intravenoes_kontinuierlich` | `levosimendan_iv_cont` |
| `Lipase` | `lipase` |
| `Lymphocyten` | `lymph_pct` |
| `Lymphozyten_absolut` | `lymph_abs` |
| `Lymphozyten_prozentual` | `lymph_pct` |
| `MAP` | `map` |
| `MPAP` | `mpap` |
| `Midazolam_intravenoes_kontinuierlich` | `midazolam_iv_cont` |
| `Milrinon_intravenoes_kontinuierlich` | `milrinone_iv_cont` |
| `Morphin_intravenoes_kontinuierlich` | `morphine_iv_cont` |
| `NT-proBNP` | `ntprobnp` |
| `NT-pro_BNP` | `ntprobnp` |
| `Norepinephrin_intravenoes_kontinuierlich` | `norepinephrine_iv_cont` |
| `Organversagen:_SOFA_Score_ohne_GCS` | `sofa_score_without_gcs` |
| `PCT` | `pct` |
| `PCWP` | `pcwp` |
| `PEEP` | `peep` |
| `PEEP_eingestellt` | `peep_set` |
| `PVRI` | `pvri` |
| `P_EI` | `insp_pressure` |
| `Prednisolon_intravenoes_bolusweise` | `prednisolone_iv_bolus` |
| `Propofol_intravenoes_kontinuierlich` | `propofol_iv_cont` |
| `Pseudo-ID` | `stay_id_global` |
| `Rocuronium_intravenoes_bolusweise` | `rocuronium_iv_bolus` |
| `SAP` | `sbp` |
| `SOFA` | `sofa_score_unspecified` |
| `SOFA.Punkte_Blut` | `sofa_blood` |
| `SOFA.Punkte_Leber` | `sofa_liver` |
| `SOFA.Punkte_Lunge` | `sofa_respiratory` |
| `SOFA.Punkte_Lunge_(cal.)` | `sofa_respiratory_calculated` |
| `SOFA.Punkte_Niere` | `sofa_renal` |
| `SOFA.Punkte_ZNS` | `sofa_cns` |
| `SOFA.SOFA_Gesamt` | `sofa_total_score` |
| `SPAP` | `spap` |
| `SVI_(Bolus)` | `stroke_index_bolus` |
| `SVI_(kontinuierlich)` | `stroke_index_cont` |
| `SVRI` | `svri` |
| `SV_(Bolus)` | `stroke_volume_bolus` |
| `SV_(kontinuierlich)` | `stroke_volume_cont` |
| `SaO2` | `sao2` |
| `Sevofluran_inhalativ` | `sevoflurane_inh` |
| `SpO2` | `spo2` |
| `Sufentanil_intravenoes_kontinuierlich` | `sufentanil_iv_cont` |
| `SzvO2` | `scvo2` |
| `Terlipressin_intravenoes_bolusweise` | `terlipressin_iv_bolus` |
| `Thrombozyten` | `platelets` |
| `Troponin` | `troponin` |
| `Vasopressin_intravenoes_kontinuierlich` | `vasopressin_iv_cont` |
| `Vt` | `vt` |
| `Vt_per_kg` | `vt_per_kg` |
| `Vt_spontan` | `vt_spontaneous` |
| `ZVD` | `cvp` |
| `Zeit_ab_Aufnahme` | `minutes_since_icu_admission` |
| `deltaP` | `delta_p_reported` |
| `etCO2` | `etco2` |
| `hid` | `hospital_code_source` |
| `iSOFA.HKL` | `isofa_cardiovascular` |
| `iSOFA.Leber` | `isofa_liver` |
| `iSOFA.Lunge` | `isofa_respiratory` |
| `iSOFA.Niere` | `isofa_renal` |
| `iSOFA.Thrombo` | `isofa_thrombocyte` |
| `iSOFA.ZNS` | `isofa_cns` |
| `iSOFA.iSOFA_Gesamt` | `isofa_total_score` |
| `individuelles_Tidalvolumen_pro_kg_idealem_Koerpergewicht` | `vt_per_kg_ideal_body_weight` |
| `pH_Wert_(ohne_Temp-Korrektur)_arteriell` | `ph_art` |
| `pH_arteriell` | `ph_art` |
| `pTT` | `ptt` |
| `paCO2_(ohne_Temp-Korrektur)` | `paco2` |
| `paO2_(ohne_Temp-Korrektur)` | `pao2` |
| `timeidx` | `anchored_time_since_icu_admission` |


## Approved alias merges

| Translated output | Pooled source columns | Rule |
|---|---|---|
| `ck` | `CK`, `Creatinkinase` | Values are equivalent names. `CK` may be non-missing in UK00/03/04/06/07/08 and `Creatinkinase` in UK02. Unequal simultaneous values block publication. |
| `il6` | `IL-6`, `Interleukin_6` | Values are equivalent names. `IL-6` may be non-missing in UK00/03/04/06/07/08 and `Interleukin_6` in UK02. Unequal simultaneous values block publication. |
| `ntprobnp` | `NT-proBNP`, `NT-pro_BNP` | Equivalent spellings. Use is not hospital-exclusive; the demo showed values under both spellings across the prior boundary. Unequal simultaneous values block publication. |
| `therapy_read_confirmation_utc` | `LesebestaetigugnTherapie_utc`, `LesebestaetigungTherapie_utc` | Binary OR. Each source must be `0`, `1`, or missing; a `0/1` disagreement becomes `1`. Non-binary input blocks publication. |
| `core_temp` | `Koerperkerntemperatur`, `Körpertemperatur` | Equivalent names. Unequal simultaneous values block publication. |
| `ecmo_o2` | `ECMO_FiO2`, `Gaszusammensetzung_(%O2)` | `ECMO_FiO2` may be non-missing in UK02 and the alternative in UK00/03/04/06/07/08. Unequal simultaneous values block publication. |
| `lymph_pct` | `Lymphocyten`, `Lymphozyten_prozentual` | Both represent lymphocyte percentage. `Lymphocyten` may be non-missing in UK02 and `Lymphozyten_prozentual` in UK00/03/04/06/07/08. Unequal simultaneous values block publication. |
| `ph_art` | `pH_Wert_(ohne_Temp-Korrektur)_arteriell`, `pH_arteriell` | The qualified name may be non-missing in UK00 and the shorter name in UK02/03/04/06/07/08. Unequal simultaneous values block publication. |

Null and IEEE NaN both count as missing during coalescing. Except for the
explicit binary-OR rule, two unequal non-missing aliases are never silently
resolved. The manifest reports aggregate source/hospital support, overlap,
conflict, resolved-conflict, and disallowed-hospital counts without stay IDs.

The two driving-pressure fields and the two unspecified SOFA concepts are not
alias merges:

- `DeltaP` -> `delta_p_computed` and `deltaP` -> `delta_p_reported`;
- `Organversagen:_SOFA_Score_ohne_GCS` -> `sofa_score_without_gcs` and
  `SOFA` -> `sofa_score_unspecified`.

## Conditional drop

`ARDS_Diagnose_App_duplicated_0` is dropped only after a complete scan proves
that every value is null/NaN. A single non-missing value blocks publication.
`ARDS_Diagnose_App` remains as `ards_diagnosis_app` with its codes unchanged.

## Exact categorical translations

General behavior is strict: strings are trimmed; case is normalized only where
shown; input null remains null; listed missing sentinels become Arrow null; and
an unexpected non-missing value blocks publication rather than passing through
or becoming missing silently.

### `clusterGeschlecht` -> `sex`

Normalization: trim and uppercase. Output type: `large_string`.

| Source | Translated |
|---|---|
| `M` | `male` |
| `W` | `female` |

### `clusterKoerpergewicht` -> `weight_group`

Normalization: trim only. Output type: `large_string`.

| Source | Translated |
|---|---|
| `<65` | `<65` |
| `65-75` | `65-75` |
| `76-250` | `76-250` |
| `-1` | missing |

The ranges are language-neutral. `-1` is an approved missing sentinel.

### `BMI` -> `bmi_group`

Normalization: trim and uppercase. Output type: `large_string`.

| Source | Translated |
|---|---|
| `L` | `underweight` |
| `M` | `normal_weight` |
| `P` | `overweight` |
| `1` | `obesity_class_1` |
| `2` | `obesity_class_2` |
| `3` | `obesity_class_3` |
| `X` | missing |
| literal string `nan` | missing |

### `Entlassgrund_(verlegt_intern,_verlegt_extern,_verstorben)` -> `discharge_status`

Normalization: trim and lowercase. Output type: `large_string`.

| Source | Translated |
|---|---|
| `verlegt` | `transferred` |
| `verstorben` | `died` |

### `Sterblichkeit` -> `death_status`

Normalization: trim and uppercase. Output type: `large_string`.

| Source | Translated |
|---|---|
| `0` | `discharged_alive` |
| `ICU` | `died_in_icu` |
| `KH` | `died_in_hospital` |
| literal string `nan` | missing |

### `KH-Sterblichkeit` -> `hospital_mortality_reported`

Normalization: trim and lowercase. Output type: Arrow `boolean`.

| Source | Translated |
|---|---|
| string `false` | Boolean `false` |
| string `true` | Boolean `true` |

The three mortality-related source concepts are not consolidated in this
stage.

## Approved numeric missing-value sentinels

These are source-system missing-value encodings, not clinically valid negative
measurements. The translated layer replaces only the explicitly approved
sentinel value with an Arrow null and preserves every other numeric value.

| Pooled source | Translated field | Source sentinel | Translated value | Rationale |
|---|---|---:|---|---|
| `Liegedauer_KH` | `hosp_los` | `-1` | missing | Negative hospital length of stay is impossible. The production audit found 5,164 occurrences, all at UK08, where `-1` encodes unavailable hospital length of stay. |
| `Dialyse_(dialysefreie_Tage)` | `dialysis_free_days` | `-1` | missing | A negative number of dialysis-free days is impossible; the old ASIC pipeline also treated `-1` as missing. |
| `Beatmungsfreie_Tage` | `vent_free_days` | `-1` | missing | A negative number of ventilation-free days is impossible; the old ASIC pipeline also treated `-1` as missing. |

The translation manifest records the source non-missing count, the number of
matched `-1` sentinels, and the resulting non-missing count for each field.
Other negative values are preserved. In particular, policy `1.3` does not
silently broaden these field-specific rules into general invalid-value
cleaning.

## Reviewed categorical values retained unchanged

| Translated field | Accepted production values | Current treatment |
|---|---|---|
| `study_implementation_phase` | `0`, `1`, `2` | Preserve codes; meanings are undocumented. |
| `height_group` | `<180`, `180-185`, `>185` | Preserve language-neutral ranges. |
| `icu_readmit` | `0`, `1` | Preserve numeric binary encoding. |
| `age_group` | `<70`, `70-79`, `80-130` | Preserve language-neutral ranges. |
| `ards_diagnosis_app` | `0`, `1`, `2`, `3` | Preserve unresolved application codes. |
| `ecmo` | `0`, `1` | Preserve numeric binary encoding. |
| `position_therapy` | `0`, `1`, `0.5`, `0.3333333432674408`, `0.25`, `0.20000000298023224`, `0.1666666716337204`, `0.1428571492433548`, `0.125` | Preserve until the UK00 fractions/upstream aggregation are understood. |
| `severity_read_confirmation` | no non-missing production values | Retain the all-missing column. |
| `therapy_read_confirmation_utc` | `0`, `1` | Preserve after approved binary-OR merge. |

These were established by a bounded demo inventory followed by a complete
production inventory of 14,483 static and 21,876,966 dynamic rows. All
inventories were complete within the 1,000-value bound. One UK00
therapy-confirmation `0/1` conflict was observed and resolved to `1` under the
approved rule.

## Exact translated column order

The policy groups columns for readability and flattens the groups in the order
shown below. Every output column occurs exactly once. Policy validation blocks
missing, duplicate, or unknown group entries.

### Static order

#### `identifiers_and_study_timing`

```text
stay_id_global
hospital_id
hospital_code_source
cluster_id
time_since_study_start
study_implementation_phase
```

#### `demographics`

```text
sex
age_group
weight_kg
height_cm
weight_group
height_group
bmi_group
```

#### `stay_and_outcomes`

```text
hosp_los
icu_los
icu_readmit
discharge_status
death_status
hospital_mortality_reported
icd10_codes
dialysis_free_days
vent_free_days
```

### Dynamic order

#### `identifiers_and_time`

```text
stay_id_global
hospital_id
hospital_code_source
minutes_since_icu_admission
anchored_time_since_icu_admission
```

#### `physiological_values`

```text
heart_rate
sbp
map
dbp
resp_rate
spont_resp_rate
core_temp
spo2
sao2
scvo2
cvp
spap
mpap
dpap
pcwp
cardiac_output_bolus
cardiac_output_cont
cardiac_index_bolus
cardiac_index_cont
stroke_volume_bolus
stroke_volume_cont
stroke_index_bolus
stroke_index_cont
svri
pvri
gedvi
evlwi
fluid_balance_24h
```

#### `mechanical_ventilation`

```text
fio2
fio2_set
feo2
peep
peep_set
insp_pressure
delta_p_computed
delta_p_reported
compliance
ie_ratio
ie_ratio_set
vt
vt_spontaneous
vt_per_kg
vt_per_kg_ideal_body_weight
vt_per_ideal_bw_total
etco2
pf_ratio
```

#### `laboratory_values`

```text
ph_art
pao2
paco2
bicarbonate_art
base_excess_art
lactate_art
hemoglobin
hematocrit
wbc
platelets
lymph_abs
lymph_pct
inr
ptt
d_dimer
albumin
bilirubin_total
urea
creatinine
ast
alt
ldh
amylase
lipase
ck
ck_mb
troponin
bnp
ntprobnp
crp
pct
il6
```

#### `intravenous_medications`

```text
norepinephrine_iv_cont
epinephrine_iv_cont
dobutamine_iv_cont
milrinone_iv_cont
levosimendan_iv_cont
vasopressin_iv_cont
terlipressin_iv_bolus
propofol_iv_cont
midazolam_iv_cont
dexmedetomidine_iv_cont
clonidine_iv_cont
fentanyl_iv_cont
sufentanil_iv_cont
morphine_iv_cont
ketanest_iv_cont
rocuronium_iv_bolus
furosemide_iv_cont
hydrocortisone_iv_bolus
dexamethasone_iv_bolus
prednisolone_iv_bolus
```

#### `other_therapy_and_review_fields`

```text
ards_diagnosis_app
ecmo
ecmo_o2
extracorp_blood_flow
extracorp_o2_flow
position_therapy
inhaled_no
inhaled_iloprost
isoflurane_inh
sevoflurane_inh
fludrocortisone_po_bolus
therapy_read_confirmation_utc
severity_read_confirmation
```

#### `sofa`

```text
sofa_score_without_gcs
sofa_score_unspecified
sofa_blood
sofa_liver
sofa_respiratory
sofa_respiratory_calculated
sofa_renal
sofa_cns
sofa_total_score
```

#### `isofa`

```text
isofa_cardiovascular
isofa_liver
isofa_respiratory
isofa_renal
isofa_thrombocyte
isofa_cns
isofa_total_score
```


## Manifest and publication checks

Before publishing translated files, the stage verifies the input contract,
row-count/order preservation, exact output schemas and group order, identifier
and time preservation, merge/drop preconditions, and complete categorical
mapping. `translation_manifest.json` records the policy/contract versions,
input/output counts, ordered groups, derived fields, merge evidence, drop
evidence, categorical source/output counts, and hospital-specific semantic
split counts. For each split it records selected and non-selected source
counts, both output counts, scope-leakage checks, and source-value
conservation.

Existing outputs are never replaced unless `--overwrite` is supplied after
human review.

## Commands

Use the same context profile for every operation. Start with demo:

```bash
./.venv/bin/python -m asic_pipeline validate-translation-policy \
  --config asic/config/datasets/demo.yaml

./.venv/bin/python -m asic_pipeline inventory-pooled-categories \
  --config asic/config/datasets/demo.yaml

./.venv/bin/python -m asic_pipeline pooled-to-translated \
  --config asic/config/datasets/demo.yaml
```

After reviewing the demo inventory, translated files, and manifest, use the
mock profile only when mock output is needed. Production must not be run by
substituting `production.yaml` into the foreground command. Follow the
complete human-review and Slurm submission procedure in
[`asic/README.md`](../README.md); the production batch script is
[`asic/run_pooled_to_translated_production.sh`](../run_pooled_to_translated_production.sh).
Categorical inventory is read-only and writes a timestamped aggregate JSON
report under `asic/reports/<context>/`.

## Unresolved semantics carried forward

No assumptions are made about units or clinical validity. The stage preserves
unresolved `Zeit_seit_Studienbeginn`, implementation-phase and cluster codes,
ARDS codes, positioning fractions, severity/therapy confirmation provenance,
SOFA/iSOFA provenance, and computed-versus-reported driving pressure. Unit and
invalid-value handling belongs to
[`translated_to_cleaned.md`](translated_to_cleaned.md). Mortality
consolidation, ventilation duration, and temporal blocking belong to
[`cleaned_to_derived.md`](cleaned_to_derived.md).
