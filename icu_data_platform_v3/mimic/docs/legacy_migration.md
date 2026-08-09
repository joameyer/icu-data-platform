# Legacy reconstruction and migration matrix

The legacy repository and notebook are evidence, not dependencies or an
authority. The reconstructed behavior below distinguishes intended feature
computations from defects that must not be migrated.

| Area | Reconstructed legacy behavior | Migration |
|---|---|---|
| Inputs | Notebook reads patients, admissions, ICU stays, diagnoses, chart, input, and output events; it does not reliably bind services, lab dictionary metadata, or versioned manifests. | Read only profile-approved MIMIC-IV 3.1 tables/columns and fail on missing contract evidence. |
| Cohort time | Replaces official ICU bounds with first/last selected vital or ventilation-setting time. | Use official `icustays.intime/outtime`. |
| Cohort filters | Requires at least 24 hours and ventilation; lacks the approved service, discharge, and first-stay-per-subject rules. | Apply the frozen cohort contract; no LOS or ventilation requirement. |
| 15-minute blocking | Ceiling assignment, dense nonnegative grid; negative observations dropped. Vital min/max can represent trailing eight-hour windows rather than the 15-minute interval. | Symmetric right-labelled pre/post contract; block-local vital aggregates direct from canonical events. |
| 8-hour blocking | Derived from 15-minute medians. | Independently aggregate canonical events; dependency on 15-minute output is prohibited. |
| Missingness | Later analysis preprocessing carries values forward and imputes/encodes/scales. | Preserve nulls and observation provenance; preprocessing remains analysis-owned. |
| Fluid balance | Calendar-day input totals minus output totals, treating a missing side as zero; not rolling 24 hours. | Disabled until rolling-window semantics, included sources, sign, overlap, and timestamp rules are approved. |
| Arterial labs | Selected lab item IDs can be labelled arterial without proven specimen semantics. | Preserve candidate measurements but keep lactate/PaO2/PaCO2/pH predictor-ineligible until specimen rule is approved. |
| Units | Mixed conversions occur in extraction; BUN-to-urea meaning is not adequately frozen. | Named conversions precede ASIC-equivalent ranges; urea remains gated. |
| BMI | First values and sex-based mean imputation. | Median valid observed height and weight, official ICU interval, continuous BMI, no imputation. |
| ICD | Exact-code filtering can precede prefix matching and `icd_version` is missing, producing incomplete/incorrect phenotypes. | Normalize ICD-10 then prefix-match; keep ICD-9 uncertainty explicit. |
| Ventilation | Ventilator settings help define cohort and recording extent. | Keep selected markers in a separately labelled audit role; never require ventilation and never expose them as predictors. |
| Output | Plain overwrite-prone files without immutable lineage. | Run-scoped immutable artifacts, schemas, counts, hashes, and direct-parent manifests after authorization. |
| QC | Ad hoc plausibility/filtering without complete rejected-row accounting. | Apply ASIC-equivalent rules after unit harmonization and conserve every selected row by disposition. |
| Train/test | Extraction and notebook preprocessing can couple data preparation to modeling splits. | Data layer creates no train/test split and trains no model. |

Unsafe behavior explicitly rejected: inferred ICU bounds, minimum-stay and
ventilation selection, 8h-from-15m aggregation, negative-time loss, trailing
window leakage into nominal blocks, missing-side-as-zero fluid balance,
BMI imputation, ICD-version loss, silent unit assumptions, overwrites, and
row-level evidence export.
