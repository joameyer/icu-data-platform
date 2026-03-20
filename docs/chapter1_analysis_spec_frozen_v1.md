# Chapter 1 Analysis Specification (Frozen v1)

## Phase
**Phase 1 / Chapter 1**  
**Topic:** Mortality risk structure in ICU data

---

## 1. Chapter 1 scientific question

Chapter 1 examines whether, under the available ICU feature set, documentation process, and temporal aggregation, some in-ICU deaths are preceded by deterioration that is insufficiently captured in the observed data, such that standard near-term risk models assign falsely reassuring risk estimates shortly before death.

This chapter does **not** aim to:
- prove biological death subtypes
- prove irreducible stochastic mortality
- claim that low-predicted fatal cases are fundamentally unobservable rather than unobserved
- treat any two-group decomposition as latent biological ground truth
- make causal claims about why poorly captured deaths occur

Baseline risk models are used as an **operational instrument** to identify deaths whose preceding observed state appears insufficiently informative or falsely reassuring.

---

## 2. Development and validation datasets

### Development dataset
- **ASIC cohort** from five German university hospitals

### External validation dataset
- **MIMIC-IV**

---

## 3. Frozen cohort definition

### Study cohort
The Chapter 1 development cohort consists of **adult ICU patients** in ASIC who received **mechanical ventilation for at least 24 hours**. The external validation cohort in MIMIC-IV will be defined using the same core inclusion criteria as closely as possible.

The primary analysis is restricted to the **first ICU stay per patient**.

### Inclusion criteria
A stay is included if all of the following apply:
1. patient age is **18 years or older**
2. the patient received **mechanical ventilation for at least 24 hours**
3. the stay is the **first ICU stay** for that patient in the dataset
4. the stay has a **valid in-ICU mortality label**
5. the stay contains sufficient observed data to construct **at least one valid prediction instance**

### Exclusion criteria
A stay is excluded if any of the following apply:
1. age is **< 18 years**
2. mechanical ventilation duration is **< 24 hours**
3. the stay is **not the first ICU stay** for that patient
4. the in-ICU mortality label is missing or unusable
5. no valid prediction instance can be constructed from the recorded data
6. the record fails basic preprocessing integrity checks

### Explicit non-exclusions
The following are **not** excluded at cohort construction:
- trauma cases
- discharge against medical advice (AMA)
- hospice / palliative / end-of-life-coded cases

These are instead **flagged explicitly** for later cohort characterization and sensitivity analyses.

### Explicit cohort decision
- **No ICU length-of-stay >= 48h requirement is used**

---

## 4. Frozen outcome definition

### Primary outcome
- **In-ICU mortality**

### Outcome interpretation
The event of interest is death occurring during the ICU stay.  
ICU discharge alive is treated as non-event for the primary Chapter 1 endpoint.  
Deaths occurring after ICU discharge are out of scope for the primary endpoint.

---

## 5. Time representation and prediction target

### Time representation
- Primary temporal representation: **8-hour blocks**

This is a pragmatic aggregation choice and not a claim that the underlying physiological process is naturally 8-hour.

### Prediction target
At each completed 8-hour block, baseline models estimate the probability of **in-ICU death within the next H hours**, conditional on the patient being alive and still in ICU at the prediction time.

### Rationale
The chapter is not primarily a generic mortality prediction study.  
The horizon-based prediction setup is used to identify deaths whose preceding observed state appears falsely reassuring.

A non-horizon target such as eventual ICU death would mix:
- imminent poorly captured deterioration
- longer-run prognosis
- later complications
- later treatment-limitation effects

That is not the primary Chapter 1 target.

---

## 6. Frozen horizon definitions

### Horizon hierarchy
- **Primary horizon:** 24 hours
- **Secondary horizon:** 48 hours
- **Sensitivity horizons:** 8 hours and 16 hours

### Interpretation rule
- **24h** defines the main Chapter 1 narrative
- **48h** is the prespecified main contrast for horizon dependence
- **8h** and **16h** are sensitivity analyses to test whether the apparent under-signaled mortality structure changes under shorter windows

These shorter horizons must be interpreted cautiously because they are more vulnerable to documentation timing, temporal aggregation, and observation-process artifacts.

---

## 7. Frozen unit of analysis and prediction-instance rule

### Unit of analysis
- patient-time prediction instances defined at completed 8-hour blocks within the first ICU stay

### Eligibility for a valid prediction instance
A prediction instance is valid only if:
1. the patient is alive and still in ICU at the prediction time
2. sufficient observed data are available up to the end of the current 8-hour block to construct model inputs
3. the horizon-specific outcome label can be defined unambiguously

All exact implementation details for label construction and valid-instance generation must follow the frozen preprocessing pipeline and must not be revised post hoc based on model results.

---

## 8. Frozen feature-set boundary

Chapter 1 uses routinely recorded ICU variables available through the Chapter 1 preprocessing pipeline and mapped as closely as possible between ASIC and MIMIC-IV.

Eligible feature families may include:
- demographics
- admission/context variables
- vital signs
- laboratory variables
- ventilation-related variables already present in the infrastructure
- simple derived within-block summaries

### Feature boundary rule
Do **not** introduce high-complexity bespoke feature engineering in Sprint 1.

### Observation-process and treatment-limitation variables
Observation-process / missingness variables and treatment-limitation / end-of-life proxies are **mandatory inventory items** for Chapter 1 interpretation and sensitivity work, but they are not required to be part of the primary baseline model definition unless already standard and robustly available.

---

## 9. Frozen preprocessing / missingness policy

The Chapter 1 analysis must use one prespecified preprocessing policy covering:
- block construction
- within-block aggregation
- carry-forward / LOCF behavior
- missingness handling
- static feature joins
- label generation

This policy must be documented before final model fitting.

### Rule
Missingness handling must **not** be revised after inspecting performance results.

---

## 10. Frozen split strategy

### Primary internal split
The ASIC development cohort will be divided into:
- **train:** 70%
- **validation:** 15%
- **test:** 15%

### Split rule
- split at the **patient level**
- because only the first ICU stay is used, this automatically keeps all prediction instances from a patient in the same subset
- stratify by **in-ICU mortality**
- perform the split **within site** to preserve hospital representation across subsets
- pool the resulting subsets across the five ASIC hospitals

### Split interpretation
This is the **primary internal development split** for Chapter 1.

### Site transport sensitivity
A **leave-one-hospital-out evaluation** may be performed as a secondary sensitivity analysis, but it is **not** the primary development split.

### Explicit non-design
The primary Chapter 1 design does **not** include target-site adaptation or hospital-specific tuning using labeled data from a held-out hospital.

---

## 11. Frozen baseline models

The minimum baseline Chapter 1 model set is:
- **logistic regression**
- **XGBoost**

Optional only if very low-friction:
- simple feedforward neural network

### Rule
Do **not** expand the model zoo in Sprint 1.

---

## 12. Frozen evaluation framework

### Mandatory evaluation metrics
For each baseline model and each prespecified horizon, report:
- **AUROC**
- **AUPRC**
- **calibration intercept**
- **calibration slope**

Optional if easy and consistently available:
- **Brier score**

### Mandatory evaluation plots
For each relevant model/horizon combination:
- **reliability plot**
- **observed mortality vs predicted-risk plot**
- **distribution of fatal cases across the predicted-risk spectrum**
- **horizon comparison view**
- **initial site-stratified performance sanity check**

### Central descriptive output
The core descriptive output for Chapter 1 is the relationship between:
- predicted near-term mortality risk
- observed mortality
- the placement of fatal cases within the predicted-risk spectrum

This is the main empirical basis for identifying low-predicted fatal cases.

### Interpretation rule
Calibration is central, not decorative.  
Calibration artifact is a primary interpretive threat and must be evaluated directly.

---

## 13. Reporting template

### Main text structure
1. **Cohort and outcome definition**
   - cohort flow
   - final sample size
   - mortality rate
   - flagged trauma / AMA / hospice prevalence

2. **Primary horizon results (24h)**
   - logistic regression and XGBoost performance
   - discrimination
   - calibration
   - observed mortality vs predicted-risk structure

3. **Secondary horizon results (48h)**
   - same framework as primary horizon
   - explicit comparison with 24h

4. **Sensitivity horizon results (8h, 16h)**
   - compact reporting
   - explicit interpretation as short-window sensitivity tests

5. **Initial site sanity check**
   - confirm whether the main pattern is or is not dominated by one site

### Recommended core tables
**Table 1 — Cohort summary**
- patient count
- in-ICU deaths
- age
- ventilation duration
- flagged trauma / AMA / hospice proportions

**Table 2 — Performance summary**
Rows:
- model × horizon

Columns:
- AUROC
- AUPRC
- calibration intercept
- calibration slope
- event count

### Recommended core figures
**Figure 1 — Calibration/discrimination summary**  
**Figure 2 — Observed mortality vs predicted risk**  
**Figure 3 — Horizon dependence (8h, 16h, 24h, 48h)**  
**Figure 4 — Site sanity check**

---

## 14. Hard-case definition for initial Chapter 1 analysis

For initial Chapter 1 analysis, hard cases are defined operationally as:

> fatal cases that receive low predicted near-term mortality risk from the baseline models at the relevant horizon.

For the initial pass, characterization should be based primarily on:
- risk ranking
- quantiles / percentiles
- descriptive placement in the risk spectrum

A final threshold-based hard-case rule does **not** need to be frozen yet unless required for implementation.

---

## 15. Mandatory interpretive threat checks

The following are prespecified as major interpretive threats and must be considered in later Chapter 1 analysis and discussion:
- calibration artifact
- site / transport artifact
- treatment-limitation / end-of-life confounding
- temporal-resolution artifact
- observation-process artifact
- overinterpretation of latent classes

---

## 16. External validation rule

Chapter 1 is not considered complete without external validation on **MIMIC-IV** using the closest feasible cohort, feature, and outcome alignment.

MIMIC-IV is an external validation dataset, not part of primary model development.

---

## 17. Final freeze summary

### Frozen cohort
- age >= 18
- mechanical ventilation >= 24h
- first ICU stay only
- no ICU LOS >= 48h requirement
- trauma / AMA / hospice flagged, not excluded

### Frozen outcome
- in-ICU mortality

### Frozen time representation
- 8-hour blocks

### Frozen horizon hierarchy
- primary: 24h
- secondary: 48h
- sensitivity: 8h, 16h

### Frozen split
- patient-level
- train / validation / test = 70 / 15 / 15
- mortality-stratified
- performed within site with preserved site proportions
- pooled across ASIC hospitals

### Frozen baseline models
- logistic regression
- XGBoost

### Frozen evaluation framework
- AUROC
- AUPRC
- calibration intercept
- calibration slope
- reliability plots
- observed mortality vs predicted-risk plots
- fatal-case distribution over risk spectrum
- horizon comparison
- initial site-stratified sanity check

---

## 18. Status

This document freezes the Chapter 1 operational specification for Sprint 1 execution.  
It should only be revised if:
- a preprocessing impossibility is discovered
- ASIC–MIMIC harmonization fails for a frozen choice
- a clear leakage or labeling problem is found
- supervisor feedback explicitly requires a justified change
