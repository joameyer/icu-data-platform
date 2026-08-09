# Candidate unit decisions for harmonized contract 0.2

## Status and boundary

This document records the complete candidate resolution of the 72 dynamic
variables whose unit is `unresolved` in frozen harmonized contract `0.1`. The
machine-readable source is
[`candidate_unit_decisions_0_2.yaml`](../config/unit_resolution/candidate_unit_decisions_0_2.yaml).

The complete distribution audit finished in run `20260807T083117Z`. The data
owner then approved all nine audited conversions and replaced the nine
candidate cleaning masks with nine value-preserving harmonization semantic
splits. The approved machine-readable amendment is
[`reviewed_unit_decisions_0_2.yaml`](../config/unit_resolution/reviewed_unit_decisions_0_2.yaml).
It authorizes schema planning only. The existing harmonized, cleaned, and
core-derived `0.1` releases remain immutable, and no clinical artifact or
release pointer is changed by either decision register.

## Unit provenance warning

The ASIC source hospitals did not provide a complete unit dictionary. The
units below were assigned by the data owner and pipeline team using:

- the prior ASIC analysis configuration in
  `phase-aware-icu-mortality-prediction/config/variable_configuration.csv`
  (reviewed local SHA-256
  `a116d58c09299e27670afd33b3989c4c5f7253987bc3ab44a9383b98d018d3e7`);
- canonical variable definitions and clinical convention;
- the complete aggregate v3 unit audit `20260807T065722Z`; and
- cross-hospital scale and availability evidence.

The prior analysis repository is evidence only and is not a v3 runtime
dependency. All copied decisions and their provenance are stored in v3 so the
new pipeline remains standalone. Every future variable dictionary must carry
the warning that these units are team-derived assumptions, not
hospital-supplied metadata.

Missingness is an availability property, not a unit. Consequently,
`sofa_score_without_gcs` is an eligible dimensionless score variable even
though its current production column is all missing.

## Complete variable-unit dictionary amendment

### Severity scores

All 16 fields use dimensionless `score_point`:

- `isofa_cardiovascular`, `isofa_cns`, `isofa_liver`, `isofa_renal`,
  `isofa_respiratory`, `isofa_thrombocyte`, `isofa_total_score`;
- `sofa_blood`, `sofa_cns`, `sofa_liver`, `sofa_renal`,
  `sofa_respiratory`, `sofa_respiratory_calculated`,
  `sofa_score_unspecified`, `sofa_score_without_gcs`, and
  `sofa_total_score`.

SOFA, calculated SOFA, source-unspecified SOFA, and iSOFA remain distinct
variables. Resolving their shared unit does not merge their definitions.

### Arterial blood gas and advanced hemodynamics

| Variable | Unit |
|---|---|
| `base_excess_art` | mmol/L |
| `bicarbonate_art` | mmol/L |
| `lactate_art` | mmol/L |
| `evlwi` | mL/kg |
| `gedvi` | mL/m² |
| `pvri` | dyn·s·cm⁻⁵·m² |
| `svri` | dyn·s·cm⁻⁵·m² |

`pvri` remains extremely sparse, but its missingness does not make its unit
unresolved.

### Laboratory and biomarker variables

| Variable | Canonical ASIC unit | Evidence note |
|---|---|---|
| `albumin` | dg/L | Prior ASIC configuration; UK08 conversion below |
| `alt` | U/L | Clinical convention and consistent hospital scales |
| `amylase` | U/L | Clinical convention and consistent hospital scales |
| `ast` | U/L | Prior ASIC configuration |
| `bilirubin_total` | µmol/L | Prior ASIC configuration |
| `bnp` | pg/mL | Clinical convention; numerically equal to ng/L |
| `ck` | U/L | Prior ASIC configuration |
| `ck_mb` | U/L | Clinical convention and hospital distributions |
| `creatinine` | µmol/L | Prior ASIC configuration |
| `crp` | nmol/L | Inferred from aggregate values and clinical scale |
| `d_dimer` | ng/mL | UK03/UK08 conversions below; FEU/DDU unspecified |
| `hemoglobin` | mmol/L | Prior ASIC configuration |
| `il6` | pg/mL | Clinical convention; numerically equal to ng/L |
| `inr` | dimensionless ratio | Prior ASIC configuration and observed domain |
| `ldh` | U/L | Prior ASIC configuration |
| `lipase` | U/L | Prior ASIC configuration |
| `lymph_abs` | 10^9/L | Clinical convention and observed domain |
| `ntprobnp` | pg/mL | Clinical convention; numeric site differences retained |
| `pct` | ng/mL | Clinical convention and observed domain |
| `platelets` | 10^9/L | Prior ASIC configuration |
| `ptt` | s | Data-owner-approved blood/coagulation unit |
| `troponin` | ng/mL | UK03 conversion below; assay subtype unspecified |
| `urea` | mmol/L | Prior ASIC configuration; UK08 conversion below |
| `wbc` | 10^9/L | Prior ASIC configuration |

The albumin evidence supports `mg/L` at UK08 and `dg/L` at the peers. These
are both mass concentration units; this is not a mass-to-molar conversion.
The conversion is nevertheless the intended resolution of the very strong
UK08 albumin scale discrepancy.

### Medication and therapy variables

| Variable | Canonical ASIC unit |
|---|---|
| `clonidine_iv_cont` | mg/h |
| `dexamethasone_iv_bolus` | mg |
| `dexmedetomidine_iv_cont` | µg/kg/h |
| `dobutamine_iv_cont` | µg/kg/min |
| `epinephrine_iv_cont` | µg/kg/min |
| `fentanyl_iv_cont` | µg/kg/h |
| `fludrocortisone_po_bolus` | mg |
| `furosemide_iv_cont` | mg/h |
| `hydrocortisone_iv_bolus` | mg |
| `inhaled_iloprost` | ng/administration |
| `inhaled_no` | ppm |
| `isoflurane_inh` | vol% |
| `ketanest_iv_cont` | mg/h |
| `levosimendan_iv_cont` | µg/kg/min |
| `midazolam_iv_cont` | mg/h |
| `milrinone_iv_cont` | µg/kg/min |
| `morphine_iv_cont` | mg/h |
| `norepinephrine_iv_cont` | µg/kg/min |
| `prednisolone_iv_bolus` | mg |
| `propofol_iv_cont` | mg/h |
| `rocuronium_iv_bolus` | mg |
| `sevoflurane_inh` | vol% |
| `sufentanil_iv_cont` | µg/h |
| `terlipressin_iv_bolus` | mg |
| `vasopressin_iv_cont` | IU/min |

These units are expected representations derived from medication name,
administration mode, clinical dosing convention, and peer-hospital
distributions. They are not claims about unobserved hospital metadata.
Hospital-specific values that cannot be brought to the expected definition
without unavailable patient weight or an unknown time denominator are routed
to explicit parallel variables rather than assigned a misleading unit or
discarded in cleaning.

## Proposed hospital-specific harmonization

The following transformations are deterministic candidate rules. They must be
audited on the cluster before contract `0.2` is frozen.

| Hospital and variable | Source assumption | Operation | Canonical result |
|---|---|---|---|
| UK08 `albumin` | mg/L | × 0.01 | dg/L |
| UK03 `d_dimer` | µg/mL | × 1000 | ng/mL |
| UK08 `d_dimer` | µg/mL | × 1000 | ng/mL |
| UK03 `troponin` | ng/L | × 0.001 | ng/mL |
| UK08 `urea` | BUN mg/dL | ÷ 2.8 | urea mmol/L |
| UK08 `isoflurane_inh` | hundredths of vol% | × 0.01 | vol% |
| UK03 `furosemide_iv_cont` | mg/day | ÷ 24 | mg/h |
| UK08 `vasopressin_iv_cont` | IU/h | ÷ 60 | IU/min |
| UK00 `vasopressin_iv_cont` | IU/kg/min | × static `weight_kg` | IU/min |

The UK00 vasopressin rule must block activation unless exact stay linkage,
weight coverage, finite positive weight, and post-conversion aggregate scale
checks pass. A missing or invalid weight produces a missing converted value and
a rule flag; no value is guessed.

The D-dimer conversion standardizes numeric scale but cannot reconstruct
whether an assay reported FEU or DDU. The canonical dictionary must retain
that caveat so downstream MIMIC integration does not silently treat the assay
definitions as equivalent.

## Approved value-preserving semantic splits

The original candidate proposed masking nine hospital-variable pairs. The
complete audit showed that this would discard 2,518,751 finite values,
including 2,258,449 positive measurements. The data owner instead approved
parallel harmonized variables. For each affected hospital, the source value is
routed unchanged into the parallel target and the ordinary canonical variable
is unavailable at that hospital. No cleaning mask is applied.

| Hospital | Source variable | Parallel target | Unit and eligibility |
|---|---|---|---|
| UK08 | `clonidine_iv_cont` | `clonidine_iv_cont_weight_normalized` | µg/kg/h; eligible |
| UK03 | `epinephrine_iv_cont` | `epinephrine_iv_cont_absolute` | µg/min; eligible |
| UK08 | `hydrocortisone_iv_bolus` | `hydrocortisone_iv_bolus_source_uk08` | unresolved source scale; ineligible pending definition |
| UK08 | `ketanest_iv_cont` | `ketanest_iv_cont_weight_normalized` | mg/kg/h; eligible |
| UK08 | `morphine_iv_cont` | `morphine_iv_cont_weight_normalized` | µg/kg/h; eligible |
| UK03 | `norepinephrine_iv_cont` | `norepinephrine_iv_cont_absolute` | µg/min; eligible |
| UK02 | `prednisolone_iv_bolus` | `prednisolone_iv_bolus_source_uk02` | unresolved source scale; ineligible pending definition |
| UK08 | `propofol_iv_cont` | `propofol_iv_cont_weight_normalized` | mg/kg/h; eligible |
| UK00 | `sufentanil_iv_cont` | `sufentanil_iv_cont_source_uk00` | unresolved source scale; ineligible pending definition |

The six resolved alternate representations are not automatically coalesced
with their ordinary canonical counterparts. The three unresolved source-scale
variables remain accessible in harmonized and cleaned data but cannot be used
as standardized analysis variables until their definitions are established.

## Medication zero and missing semantics

All 25 source medication or therapy variables, and all nine parallel split
variables, preserve an explicit numeric zero separately from null. Null is
unavailable or unobserved and is never assumed to mean zero or inactive
treatment. No harmonization conversion or split may turn zero into null or null
into zero. A downstream exposure candidate may use `value > 0`; carry-forward
or interval imputation requires a separate reviewed contract. See the
[complete medication value-semantics audit](medication_value_semantics.md).

All numeric medication/therapy variables have an inclusive lower domain bound
of zero. A general cleaning-policy `0.2` rule masks finite negative values to
null after harmonization and semantic splitting while preserving those values
in ingestion and harmonization. The current audited effect is 201 UK08
`inhaled_no` values. No absolute-value or zero-clipping correction is allowed.

## Priority range decisions

The 12 legacy-range variables are resolved as follows:

- Mask zero albumin and creatinine after hospital unit harmonization.
- For EVLWI, recover only a unique factor-of-10/100/1000 entry error that
  produces a value in `(0, 80]` mL/kg; mask ambiguous or unrecoverable values.
- Mask GEDVI outside `(0, 2000]` mL/m² and SVRI outside
  `(0, 10000]` dyn·s·cm⁻⁵·m². Do not select among multiple valid factors.
- Mask hemoglobin outside `(0, 25]` mmol/L and platelets outside
  `(0, 2000]` 10^9/L.
- Mask INR outside `(0, 15]`, PTT outside `(0, 300]` s, and arterial lactate
  outside `(0, 30]` mmol/L. Ambiguous power-of-ten candidates are missing,
  not corrected.
- Preserve SOFA values in `[0, 24]` score points. Preserve the
  `sofa_score_without_gcs` schema, unit, and eligibility even while all values
  are missing.

The reviewed aggregate count remains exactly 10,021 legacy-range findings.
No finding is modified by this candidate decision document.

## Required activation sequence

1. Completed: run and review the
   [candidate unit-decision audit](unit_decision_audit.md), including complete
   aggregate distributions and exact UK00 weight-linkage accounting.
2. Completed: approve all nine conversions and replace all nine proposed masks
   with the reviewed semantic splits above.
3. Next: run and review the complete medication zero/missing audit and the
   revised ordered schema/dictionary amendment, including the nine new
   variables and explicit medication semantics.
4. Freeze harmonized schema/dictionary contract `0.2` with the mandatory unit
   provenance note.
5. Build and completely audit a new harmonized candidate; do not overwrite
   release `20260806T111156Z`.
6. Apply cleaning policy `0.2`, including the separately approved 12 range rules,
   and completely audit a new cleaned candidate.
7. Rebuild derived data from the new cleaned release and promote each layer
   only after its separate explicit human approval.
