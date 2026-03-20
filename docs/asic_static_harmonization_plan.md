# ASIC Static Harmonization Plan v1

## Goal

Create a first-pass, hospital-agnostic static schema for the ASIC files that is:

- English
- short
- consistent across hospitals
- already cleaned for a few high-value variables before we build the loader

This is still a notebook-stage harmonization plan, but it now includes the first value-level transformations we agreed on.

## Dropped Columns

These raw fields are currently excluded from the harmonized static table:

- `Cluster-ID`
- `Zeit_seit_Studienbeginn`
- `Phase`

Reason: they are not needed for the current static ingestion target.

## Canonical Columns

| Canonical name | Raw source column(s) | Rule |
|---|---|---|
| `hospital_id` | derived from filename | add during ingestion |
| `stay_id` | `PseudoID`, `Pseudo-ID` | keep one canonical ID after equality check |
| `sex` | `clusterGeschlecht`, `ClusterGeschlecht` | map `W -> F`, keep `M -> M` |
| `weight_group` | `clusterKoerpergewicht`, `ClusterKoerperGewicht` | keep categorical bucket |
| `height_group` | `clusterKoerpergroesse`, `ClusterKoerpergroesse` | keep categorical bucket |
| `bmi_group` | `BMI` | map coded BMI classes to English labels |
| `hosp_los` | `Liegedauer_KH` | hospital length of stay |
| `icu_los` | `Liegedauer_ICU` | ICU length of stay |
| `icu_readmit` | `Wiederaufnahme_ICU` | ICU readmission indicator |
| `age_group` | `clusterAlter`, `ClusterAlter` | keep categorical bucket |
| `hosp_mortality` | `Entlassgrund_(verlegt_intern,_verlegt_extern,_verstorben)` | `verstorben -> 1`, `verlegt -> 0` |
| `icu_mortality` | `Sterblichkeit` | `ICU -> 1`, `0/KH -> 0` |
| `icd10_codes` | `ICD-10_Codes` | keep raw comma-separated string |
| `dialysis_free_days` | `Dialyse_(dialysefreie_Tage)` | numeric if valid |
| `vent_free_days` | `Beatmungsfreie_Tage` | numeric if valid |

## First-Pass Harmonization Rules

1. Add `hospital_id` from the filename.
2. Collapse `PseudoID` and `Pseudo-ID` into `stay_id`.
3. Normalize case-only schema variants such as `clusterGeschlecht` and `ClusterGeschlecht`.
4. Drop `cluster_id`, `study_day`, and `phase` from the harmonized output.
5. Convert sex values from German coding to short English coding:
   - `W -> F`
   - `M -> M`
6. Convert discharge status into a binary in-hospital mortality target:
   - `verstorben -> 1`
   - `verlegt -> 0`
7. Convert death status into a binary ICU mortality target:
   - `ICU -> 1`
   - `0 -> 0`
   - `KH -> 0`
8. Translate BMI code values into English labels:
   - `L -> Underweight`
   - `M -> Normal Weight`
   - `P -> Overweight`
   - `1 -> Obesity Class 1`
   - `2 -> Obesity Class 2`
   - `3 -> Obesity Class 3`
   - `X -> missing`
9. Convert `-1` sentinel values to missing where appropriate.
10. Add any missing canonical columns as `NA` so every hospital table has the same layout.

## `-1` Sentinel Handling

In the current sample, `-1` appears to mean missing rather than a real value.

Fields where `-1` should be treated as missing:

- `weight_group`
- `hosp_los`
- `icu_los` if encountered
- `icu_readmit` if encountered
- `dialysis_free_days`
- `vent_free_days`

This rule can be widened later if more sentinel-coded columns appear in the full dataset.

## Known Issues Still Left For The Next Pass

- `bmi_group` is now label-mapped, but it is still not a numeric BMI.
- `age_group`, `weight_group`, and `height_group` remain grouped categories rather than continuous variables.
- `icd10_codes` is still a raw string field and has not been split or normalized yet.
- Some hospitals are missing fields entirely:
  - `asic_UK03` lacks `hosp_los`, `icu_readmit`, `icu_mortality`, `dialysis_free_days`, `vent_free_days`
  - `asic_UK04` lacks `dialysis_free_days`, `vent_free_days`
- `asic_UK00` contains several all-empty fields in this sample.

## Recommended Next Step

Once the notebook output looks right, the next implementation step is:

- move the harmonization logic into an ASIC static loader,
- emit one harmonized combined table,
- emit one QC summary that records missing columns, source mappings, and value recoding counts.
