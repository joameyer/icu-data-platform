# Phase 1 Working Reference — Chapter 1 Mortality Risk Structure

## 1. Chapter Purpose

Chapter 1 tests whether ICU mortality, under the available observational feature set and temporal aggregation, shows heterogeneous predictability structure.

The chapter does not aim to discover biological death subtypes. It aims to determine whether some fatal outcomes remain weakly captured by standard near-term risk models because deterioration is insufficiently captured in the observed data stream under the recorded representation. Baseline risk models are used as an operational instrument to identify low-predicted fatal cases; generic mortality prediction performance is not the scientific endpoint.

The practical purpose of the chapter is fourfold:

1. to describe how mortality predictability is distributed across patients and time horizons,
2. to identify and characterize low-predicted fatal cases under the observed feature set,
3. to test whether any apparent low-information mortality structure is robust or instead largely explained by calibration, case-mix, observation process, or treatment-limitation factors,
4. to determine whether a compact decomposition or weighting scheme is justified for downstream thesis chapters.

This is a bounded empirical chapter about predictability structure under observed ICU data constraints.

---

## 2. Scientific Question

### 2.1 Primary Question

**Does ICU mortality, under the observed feature set and temporal aggregation used in this thesis, exhibit a reproducible poorly captured / low-predicted component relative to standard near-term mortality risk models?**

### 2.2 Secondary Questions

1. How strongly does this pattern depend on prediction horizon?
2. How strongly does it depend on calibration and model class?
3. Are low-predicted fatal cases associated with observation density, missingness, documentation rhythm, treatment-limitation proxies, or site?
4. Is the pattern heterogeneous across clinically plausible disease groups?
5. Does the broad pattern replicate externally in MIMIC-IV?
6. Does a compact decomposition or weighting rule add enough downstream value to justify later use?

---

## 3. Primary Claim and Explicit Non-Claims

### 3.1 Primary Claim

The strongest acceptable claim for Chapter 1 is:

> In ICU observational data, mortality risk estimated from the available feature set shows heterogeneous predictability structure, including a subset of fatal outcomes that remain weakly captured by standard short-horizon risk models. This claim is conditional on the observed variables, charting process, temporal aggregation, prediction horizon, and model class.

A stronger version is acceptable only if supported robustly:

> A compact low-information versus risk-aligned summary of mortality structure provides a useful empirical approximation for downstream analysis.

### 3.2 Explicit Non-Claims

This chapter does **not** establish any of the following:

1. **No claim of irreducible biological randomness.**  
   Unobserved is not the same as unobservable. A poorly predicted death under routine ICU EHR data may become more predictable with richer trajectories, text, waveform data, treatment-goal information, or finer time resolution.

2. **No claim of true biological classes.**  
   Any two-component decomposition is a working approximation. It is not evidence that ICU mortality consists of two natural death types.

3. **No claim of causal attribution.**  
   This chapter does not identify whether low-predicted deaths are driven by disease progression, treatment failure, treatment limitation, data sparsity, case-mix shift, or hidden confounding.

4. **No claim that “sudden” means clinically sudden.**  
   Apparent suddenness may be produced by charting frequency, temporal aggregation, irregular sampling, delayed recording, informative missingness, or omitted variables.

5. **No claim that this chapter identifies which patients are outside the therapeutic control loop.**  
   At most, it identifies where mortality appears more or less aligned with the recorded state trajectory.

6. **No claim that “unexpected death” literature can be transferred directly into ICU mortality decomposition.**  
   That literature is conceptually adjacent, not an operational definition for this chapter.

---

## 4. Why This Chapter Matters for the Thesis

Chapter 1 is not the thesis centerpiece. Its value is boundary-setting.

Its role is to establish whether all ICU mortality should be treated as equally accessible to observational modeling under the shared thesis representation. If a reproducible low-predicted component exists, downstream patient–therapy analyses may benefit from soft weighting, sensitivity restriction, or explicit caution around cases that are poorly captured by observed deterioration.

This chapter contributes to the thesis by:

1. defining the limits of modelability under the available ICU data,
2. linking prior mortality-prediction work to the treatment-focused thesis narrative,
3. testing whether later chapters should treat all deaths symmetrically,
4. remaining publishable even if the result is weak or negative, provided it is externally validated and honestly framed.

This chapter must remain bounded. It is a structural empirical chapter, not a grand theory of death heterogeneity.

---

## 5. Data Sources and Cohort Definition

### 5.1 Primary Dataset

**ASIC** cohort from five German university hospitals.

### 5.2 External Validation Dataset

**MIMIC-IV**

External validation is mandatory. Without it, Chapter 1 is too vulnerable to center-specific artifacts.

### 5.3 Optional Additional Validation

**eICU** only if available at low friction. Not required for the minimum viable chapter.

### 5.4 Working Cohort

Use the ventilated ICU cohort aligned with the thesis infrastructure.

The frozen Chapter 1 cohort definition is:

- age **>= 18 years**
- **mechanical ventilation >= 24 hours**
- **first ICU stay only**
- **no ICU length-of-stay >= 48 hours requirement**
- trauma / AMA / hospice / palliative / end-of-life-coded cases are **flagged, not excluded**
- inclusion additionally requires a valid in-ICU mortality label and at least one valid prediction instance

The final implementation must document:

- index time / observation anchor
- handling of short stays with no valid prediction instance
- handling of transfers if relevant in preprocessing
- discharge/censoring logic
- exact label-construction rule across horizons

### 5.5 Outcome

Primary outcome: **in-ICU mortality**.

This endpoint must be defined closely enough across ASIC and MIMIC-IV to support external validation.

### 5.6 Competing Events / Discharge

Discharge and ICU exit are not incidental. If the analysis is time-updated over stay, informative discharge must be considered explicitly in framing and, where feasible, in sensitivity analysis.

---

## 6. Outcome, Time Axis, Feature Set, and Observation Process

### 6.1 Primary Time Axis

Primary representation: **8-hour blocks**, consistent with current thesis infrastructure.

### 6.2 Mandatory Time Sensitivity

At least one temporal aggregation sensitivity should be tested after the primary 8-hour analysis is established. The preferred comparison is:

- **one finer resolution than 8 hours, if technically feasible**
- **16-hour blocks**
- **24-hour blocks**

This is a secondary sensitivity analysis, not part of Chapter 1 setup. Its purpose is to assess whether the apparent hard-case pattern is robust to alternative temporal aggregation rather than obviously driven by the 8-hour choice alone.

### 6.3 Prediction Horizons

The chapter must freeze a small horizon matrix early.

Frozen hierarchy:

- **primary horizon: 24h**
- **secondary horizon: 48h**
- **sensitivity horizons: 8h, 16h, and 72h**

The chapter should describe predictability structure as a function of horizon, not only as a single pooled target. The shorter horizons test whether the pattern strengthens under tighter near-term windows; the 72-hour horizon tests whether the signal collapses into longer-run prognosis.

### 6.4 Feature Set

Use the shared harmonized ICU feature space from prior work, restricted to routinely recorded variables with acceptable cross-dataset consistency.

Likely feature families include:

- vital signs,
- labs,
- organ dysfunction indicators,
- ventilation-related variables if available in shared representation,
- static context variables,
- established time-aware summaries already available in infrastructure.

Do **not** expand the feature space aggressively for this chapter. The purpose is not to build the best possible mortality model.

### 6.5 Observation Process Variables

Observation process analysis is mandatory.

Where feasible, derive and inspect variables such as:

- number of measurements per block or day,
- missingness burden,
- longest gap between measurements,
- documentation/event density,
- time since last observation,
- simple rhythm irregularity summaries if available at low friction.

These variables are not the main signal of interest, but they are central for interpreting low-predicted fatal cases.

### 6.6 Missingness Handling

Missingness handling must be documented explicitly because low-information mortality may partly reflect observation-process structure.

Record at minimum:

- imputation strategy,
- carry-forward/backfill rules if used,
- missingness indicators if used,
- variables excluded for poor coverage,
- whether missingness is plausibly informative.

---

## 7. Primary Analysis Strategy

### 7.1 Governing Principle

Chapter 1 is **risk-structure-first**, not mixture-first.

The chapter should first characterize where mortality is and is not captured by standard risk models. Any decomposition is secondary and must summarize structure already demonstrated descriptively and analytically.

### 7.2 Stepwise Analysis Logic

The primary analytic order is:

1. build independent mortality risk models,
2. evaluate discrimination and calibration, treating calibration as a gating issue before hard-case interpretation,
3. characterize low-predicted fatal cases,
4. test association of low-predicted fatal cases with observation process, timing, treatment-limitation proxies, subgroup, and site,
5. only then fit a compact decomposition / weighting formulation.

If the decomposition is fragile, the chapter still stands on steps 1–4.

### 7.3 Risk Models

Minimum set:

1. logistic regression,
2. XGBoost.

Optional:
3. simple neural net, only if already low friction.

No architectural novelty is required. Simpler accepted models are preferred.

### 7.4 Evaluation of Risk Models

Required outputs:

- AUROC,
- AUPRC where relevant,
- calibration intercept and slope,
- reliability plots,
- observed mortality vs predicted risk plots,
- horizon-specific performance,
- site-stratified calibration summaries at least within ASIC.

Calibration is central. A decomposition that disappears after recalibration is weak.

---

## 8. Hard-Case Characterization

### 8.1 Purpose

Before any latent summary model, identify fatal cases that are poorly captured by the risk model.

This is the chapter’s first real structural analysis.

### 8.2 Operational Definition

Define a **low-predicted fatal case** using prespecified criteria, such as fatal cases with predicted risk below a horizon-specific threshold or within low-risk quantiles among deaths.

The exact operational definition should be fixed before formal comparison.

### 8.3 Required Comparisons

Compare low-predicted fatal cases against other fatal cases on:

- age, sex, admission type,
- disease group / sepsis / ARDS / pneumonia / surgical status if available,
- ICU timing (early vs late death),
- organ support and organ dysfunction burden if available,
- observation density,
- missingness burden,
- longest measurement gaps,
- site,
- available treatment-limitation or code-status proxies,
- discharge/transfer proximity if relevant.

### 8.4 Interpretation

If low-predicted fatal cases concentrate in sparse-observation, transition, or treatment-limitation contexts, that weakens any ontological interpretation and strengthens the chapter’s measurement-bound framing.

This is not a problem. It is the point.

---

## 9. Decomposition / Weighting Strategy

### 9.1 Role in the Chapter

The decomposition is a **secondary summary device**, not the scientific centerpiece.

Its purpose is to compress observed predictability heterogeneity into an operational form that may be useful downstream.

### 9.2 Preferred Formulation

Start with a simple two-component working approximation:

- **risk-aligned mortality component**
- **low-information mortality component**

This is an analytic convenience only.

### 9.3 Default Output

Preferred output:

- posterior weights / probabilities,
- component proportions with uncertainty,
- smooth weighting rule rather than hard assignment.

### 9.4 Hard Classification

Hard thresholds are allowed only as auxiliary summaries or one downstream sensitivity analysis.

If the decomposition only looks convincing under hard classification, it is too fragile.

### 9.5 Robustness Requirements

The decomposition must be tested for dependence on:

- risk model class,
- recalibration,
- horizon,
- temporal aggregation,
- site,
- main disease strata,
- dataset transfer to MIMIC-IV.

If it fails these checks, it remains a weak descriptive summary and should be written as such.

---

## 10. Sensitivity and Robustness Analyses

The following are mandatory for a defensible chapter.

### 10.1 Model Dependence

Repeat the main structure analysis under at least two distinct risk models.

### 10.2 Calibration Dependence

Assess whether recalibration materially alters the size or interpretation of the low-information component.

### 10.3 Horizon Dependence

Show whether low-predicted mortality shrinks, persists, or changes qualitatively as prediction horizon shortens or lengthens.

### 10.4 Temporal Aggregation Dependence

Repeat the core structure analysis under at least one alternative block size.

### 10.5 Observation Process Dependence

Test whether low-predicted fatal cases are enriched for sparse or irregular observation patterns.

### 10.6 Site Heterogeneity

Check whether the pattern is dominated by one or two hospitals.

### 10.7 Disease-Stratified Analysis

Minimum strata:

- viral pneumonia / viral ARDS if available,
- bacterial infection / sepsis-related cases if available,
- medical vs surgical,
- non-pulmonary comparator group.

### 10.8 Treatment-Limitation / End-of-Life Sensitivity

Inventory and use available proxies for:

- DNR/DNI,
- treatment limitation,
- palliative transition,
- withdrawal / withholding markers,
- comfort-care documentation,
- brain-death pathway if available.

If such variables are absent, this absence must be documented explicitly and carried as a named interpretation limit from the introduction onward.

---

## 11. Validation Plan

### 11.1 Internal Development

Develop the chapter pipeline on ASIC using a prespecified train/validation/test strategy.

### 11.2 External Validation

Apply the same pipeline to MIMIC-IV.

Required external checks:

1. risk model calibration/discrimination,
2. mortality-vs-risk structure,
3. low-predicted fatal-case characterization,
4. decomposition behavior if used,
5. broad direction of key subgroup effects.

### 11.3 Replication Standard

Replication does not require identical parameters.

Replication is sufficient if:

- the broad predictability-heterogeneity pattern persists,
- or the same negative conclusion is reached,
- interpretation does not reverse completely,
- the chapter remains honest about transport limitations.

If MIMIC-IV contradicts ASIC completely, the result becomes cohort-dependent and must be written that way.

---

## 12. Interpretation Rules and Decision Thresholds

### 12.1 Strong Positive Result

A strong result requires:

- reproducible low-predicted fatal cases,
- nontrivial robustness across model class, calibration, and horizon,
- no complete explanation by observation process or treatment-limitation proxies,
- at least partial external replication,
- a useful soft weighting summary for downstream analysis.

### 12.2 Weak but Usable Result

A weak but usable result means:

- predictability heterogeneity exists,
- but it is substantially explained by measurement process, calibration, treatment limitation, or site,
- and the decomposition is only partially stable.

This is still acceptable and probably more scientifically honest.

### 12.3 Negative Result

A negative result means:

- fatal cases are not strongly separable in predictability structure,
- or the apparent low-information component collapses after recalibration / horizon update / observation-process accounting,
- or external validation fails.

This does not damage the thesis. It yields a bounded negative chapter: under the chosen representation, mortality does not support a robust decomposition beyond standard modeling artifacts.

### 12.4 What Is Enough for the Chapter

The chapter is enough if it:

- answers the core question,
- includes external validation,
- includes hard-case characterization,
- handles calibration and observation process seriously,
- and writes limitations honestly.

The chapter is not enough if it relies mostly on a visually attractive mixture fit.

---

## 13. Main Risks and Failure Modes

### 13.1 Ontological Overclaiming

Risk: writing the result as if biological death types were discovered.

### 13.2 Calibration Artifact

Risk: the low-information component is largely a calibration failure.

### 13.3 Horizon Artifact

Risk: the result depends on an arbitrary prediction horizon.

### 13.4 Temporal Aggregation Artifact

Risk: apparent suddenness is induced by block-level aggregation.

### 13.5 Observation-Process Artifact

Risk: irregular sampling, sparse charting, delayed recording, or informative missingness create “poorly predicted” deaths.

### 13.6 Treatment-Limitation Confounding

Risk: a substantial share of low-predicted mortality reflects care-limitation processes rather than sudden physiologic collapse.

### 13.7 Site / Case-Mix Dominance

Risk: the result is mainly a center-composition artifact.

### 13.8 Chapter Inflation

Risk: drifting into subtype clustering, deep phenotyping, or richer-data modeling beyond Chapter 1’s role.

---

## 14. Minimum Viable Chapter Definition

The minimum viable Chapter 1 consists of:

1. frozen cohort, outcome, horizon, and split definitions,
2. at least two independent risk models,
3. calibration and discrimination analysis,
4. mortality-vs-risk plots,
5. formal identification and characterization of low-predicted fatal cases,
6. one observation-process sensitivity,
7. one temporal aggregation or horizon sensitivity,
8. one treatment-limitation / end-of-life sensitivity or explicit documented absence of such variables,
9. disease-stratified comparison for core strata,
10. external validation on MIMIC-IV,
11. optional but preferred compact decomposition / weighting summary.

If the chapter can only exist through the decomposition, it is not robust enough.

---

## 15. Figure and Table Plan

### Core Figures

**Figure 1.** Cohort flowchart for ASIC and MIMIC-IV

**Figure 2.** Calibration and discrimination summary across horizons

**Figure 3.** Observed mortality vs predicted risk in ASIC

**Figure 4.** Characteristics of low-predicted fatal cases vs other fatal cases

**Figure 5.** Observation-process / missingness sensitivity summary

**Figure 6.** Disease-stratified predictability structure

**Figure 7.** MIMIC-IV replication of core structure

**Figure 8.** Optional decomposition / weighting summary with uncertainty

### Core Tables

**Table 1.** Cohort characteristics

**Table 2.** Risk model performance and calibration

**Table 3.** Low-predicted fatal-case comparison

**Table 4.** Stratified results by disease group / site / timing

**Table 5.** External validation comparison

---

## 16. Supervisor Alignment Questions

1. Is the risk-structure-first framing acceptable as the official Chapter 1 framing?
2. Is “low-predicted fatal cases” acceptable terminology for the descriptive core analysis?
3. Is a weak or negative chapter acceptable if external validation is strong and limitations are explicit?
4. Should the decomposition be treated only as a secondary operational summary?
5. Are horizon dependence and observation-process sensitivity mandatory for supervisor acceptance?
6. What treatment-limitation proxies are realistically available?
7. What is the minimum stratification the supervisor considers enough?

---

## 17. Immediate Sprint Tasks

### Sprint 1 Priorities

1. Freeze the Chapter 1 claim and non-claims.
2. Freeze cohort, outcome, and prediction horizons.
3. Inventory ASIC and MIMIC-IV data access and harmonization.
4. Inventory treatment-limitation / end-of-life proxies.
5. Inventory observation-density / missingness / documentation-process variables.
6. Train logistic regression and XGBoost risk models.
7. Generate calibration, discrimination, and mortality-vs-risk plots.
8. Run early hospital-stratified checks.
9. Confirm supervisor alignment.
10. Translate this document into Linear tasks.

### Sprint 2 Priorities

1. Operationally define low-predicted fatal cases.
2. Run descriptive and comparative hard-case analysis.
3. Assess horizon dependence.
4. Assess observation-process dependence.
5. Decide whether decomposition is justified as a next-step summary or already looks too fragile.

---

## 18. Literature Notes / Reading Questions

The literature scan for Chapter 1 should answer:

### Cluster A — Predictability Limits
- Is the real ceiling mainly discrimination, calibration, or transportability?
- Which subgroup calibration failures matter most?

### Cluster B — Alternative Explanations
- How much ICU mortality is shaped by treatment limitation and end-of-life decisions?
- Which decision-process mechanisms could produce low-predicted deaths?

### Cluster C — Time and Measurement Process
- How much does predictability depend on horizon, update frequency, and observation density?
- How much of apparent unpredictability shrinks with richer trajectory or text information?

### Cluster D — Statistical Framing
- What is the safest language for describing heterogeneity without reifying classes?
- What are the main warnings from latent-class / mixture literature?

### Cluster E — External Validation
- How likely is transport failure across case-mix and documentation systems?
- What minimal recalibration logic is necessary?

---

## 19. Working Writing Constraints

1. Never write as if biological death subtypes were discovered.
2. Default interpretation: “poorly predicted under the available data representation.”
3. Keep calibration, horizon, and observation-process dependence visible in the introduction and discussion.
4. Keep treatment-limitation confounding visible from the start, not only in limitations.
5. Treat external validation as part of the claim.
6. Treat the decomposition as optional summary, not proof of ontology.
7. Keep the chapter tied to the thesis through the question: when is mortality meaningfully aligned with observable deterioration under routine ICU data?

---

## 20. Phase 1 Exit Condition

Phase 1 is complete when all of the following are true:

1. Chapter 1 claim, horizon, cohort, and non-claims are frozen.
2. Risk models and calibration analyses are complete.
3. Low-predicted fatal-case characterization is complete.
4. Mandatory sensitivities are complete:
   - horizon or temporal aggregation,
   - observation process,
   - treatment-limitation proxy check or explicit absence.
5. External validation on MIMIC-IV is complete.
6. Optional decomposition / weighting summary is either completed or explicitly judged unnecessary / too fragile.
7. The chapter draft is written and sent to the supervisor.
8. `context.md` is updated for Phase 2 handover.

If these are not met, Phase 1 is not complete.