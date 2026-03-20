# Phase 1 Linear Sprint Plan — Chapter 1 Mortality Risk Structure

This file is optimized for transfer into Linear.

Rules used in this version:
- only **High** and **Low** priority are used
- **High** = must get done in the sprint
- **Low** = stretch issue; only do if time remains
- stretch work is listed **at the end of each sprint** as separate issues
- each issue is written for direct copy into Linear
- issue text is intentionally plain and compact to reduce formatting problems when pasted

---

## Sprint 1 — Framing Lock, Readiness, Baseline Risk Structure Setup

### 1. [High] Lock Chapter 1 operational specification
**Goal**  
Freeze the non-negotiable analysis definitions before modeling starts.

**Scope**
- freeze cohort inclusion/exclusion
- freeze mortality endpoint
- freeze index time / observation anchor
- freeze repeat-stay handling
- freeze short-stay handling
- freeze split strategy
- freeze primary and secondary prediction horizons
- freeze reporting template and evaluation outputs

**Deliverables**
- written Chapter 1 analysis spec
- frozen cohort/outcome/horizon/split definitions
- frozen evaluation framework

**Definition of done**
- there is one authoritative Chapter 1 spec with no unresolved ambiguity

---

### 2. [High] Convert literature into operational Chapter 1 implications
**Goal**  
Turn the completed literature work into design constraints, not narrative summary.

**Details**
Write a short synthesis note that separates literature that supports Chapter 1 from literature that weakens or constrains it. The note should not be a generic review. It should answer: what is defensible, what is overreach, what risks are central, and what analyses are mandatory because of the literature. Explicitly cover calibration, transportability, temporal resolution, observation-process effects, treatment-limitation confounding, and overinterpretation of latent classes.

**Scope**
- extract what literature implies for calibration scrutiny
- extract what literature implies for horizon choice
- extract what literature implies for observation-process sensitivity
- extract what literature implies for treatment-limitation confounding
- extract what literature implies for acceptable wording and negative-result framing

**Deliverables**
- concise literature-to-design memo
- list of mandatory analyses justified by the literature
- list of explicit claim boundaries and overreach risks

**Definition of done**
- a short decision memo exists that directly shapes Sprint 1–5 execution

---

### 3. [High] Prepare major supervisor startup check-in
**Goal**  
Force agreement early on what Chapter 1 is and is not.

**Scope**
- summarize the bounded Chapter 1 purpose and claim
- summarize explicit non-claims
- define what counts as “enough” for the chapter (minimum viable chapter definition)
- prepare decision questions on terminology, endpoint, horizon strategy, acceptability of weak/negative results, and decomposition being secondary only

**Deliverables**
- 1-page supervisor check-in note
- explicit decision questions
- minimum viable chapter summary

**Definition of done**
- the check-in package is ready to send or present

---

### 4. [High] Rebuild and verify Chapter 1 preprocessing and model-ready datasets
**Goal**  
Make Chapter 1 data analysis-ready under the frozen spec.

**Scope**
- rerun or verify cohort extraction
- verify 8-hour block construction
- verify label generation for frozen horizons
  - primary: 24h
  - secondary: 48h
  - sensitivities: 8h, 16h, and 72h
- verify carry-forward / imputation behavior
- generate train/validation/test splits
- confirm ASIC output tables are model-ready
- document MIMIC alignment constraints for later transfer

**Deliverables**
- model-ready ASIC datasets
- horizon-specific targets
- split objects
- compact data-readiness summary with counts and missingness overview

**Definition of done**
- reproducible model-ready Chapter 1 datasets exist and sanity checks are passed
- preprocessing logic is documented

### 5. [High] Inventory treatment-limitation / end-of-life proxies
**Goal**  
Determine whether the data contain usable proxies for treatment-limitation and end-of-life confounding.

**Scope**
- search ASIC and MIMIC for DNR / DNI / palliative / comfort-care / withdrawal/withholding proxies
- document availability, reliability, and cross-dataset harmonization feasibility
- explicitly document absence if direct testing is not possible

**Deliverables**
- inventory of candidate proxy variables
- dataset-specific notes for ASIC and MIMIC
- harmonization feasibility note or absence statement

**Definition of done**
- you know whether treatment-limitation sensitivity is testable directly, indirectly, or not at all

---

### 6. [High] Inventory observation-process and missingness variables
**Goal**  
Freeze a feasible variable set for later hard-case (observation-process and missingness) sensitivity analysis.

**Scope**
- define measurement density variables
- define missingness burden variables
- define time-since-last-measurement or gap variables
- define simple block-level completeness indicators
- keep the set minimal and reproducible

**Deliverables**
- final observation-process variable list
- derivation rules
- ASIC feasibility note
- MIMIC feasibility note

**Definition of done**
- a concrete derivable variable set is frozen for later use

---

### 7. [High] Implement baseline Chapter 1 risk models
**Goal**  
Build the independent risk-modeling pipeline for Chapter 1.

**Scope**
- logistic regression
- XGBoost
- optional simple feedforward net only if friction is very low
- save predictions for all frozen horizons (so they can be reused for hard-case analysis and later external validation)

**Deliverables**
- trained logistic regression baseline
- trained XGBoost baseline
- saved prediction outputs
- reproducible output structure

**Definition of done**
- both baseline models run successfully on the frozen Chapter 1 setup
- prediction outputs are stored and reusable for downstream analysis

---

### 8. [High] Evaluate discrimination, calibration, and mortality-vs-risk structure
**Goal**  
Produce the first core empirical Chapter 1 outputs.

**Details**
Using the baseline model outputs, compute the required discrimination and calibration metrics, generate reliability plots, and produce observed mortality vs predicted-risk plots. Also generate horizon-specific and first site-stratified comparisons. This is the first point where the chapter becomes empirical rather than conceptual.

**Scope**
- AUROC
- AUPRC where relevant
- calibration slope/intercept
- reliability plots
- observed mortality vs predicted-risk plots
- horizon comparison view
- first site-stratified sanity check

**Deliverables**
- first evaluation package
- first mortality-vs-risk figures
- first reviewable interpretation note

**Definition of done**
- First empirical outputs include calibration checks sufficient to determine whether hard-case analysis is interpretable

### 9. [High] Update Linear and `context.md` at sprint close
**Goal**  
Create a clean handover for Sprint 2.

**Scope**
- update issue status in Linear
- record final Sprint 1 decisions
- record frozen definitions
- record dataset status
- record proxy inventory status
- record first model results
- record next sprint focus

**Deliverables**
- updated Linear board
- updated `context.md` so that it is sufficient for Sprint 2 startup without reconstruction work
- concise Sprint 1 closure note

**Definition of done**
- a new Sprint 2 chat could start without reconstructing Sprint 1 from memory

### [Low] Stretch issue — Prepare annotated reading order for Phase 1 core papers
**Goal**
Create a practical reading order for the most important Chapter 1 papers if core sprint work finishes early.

**Details**
Prepare a short annotated list of the 10–15 most important papers to read next, ordered by direct usefulness for the chapter. The point is to make remaining literature work targeted instead of opportunistic.

**Deliverables**
- ranked reading list
- 1–2 sentence note per paper on why it matters

**Definition of done**
- a usable reading order exists for any additional literature time

---

## Sprint 2 — Hard-Case Characterization and Horizon Dependence

### [High] Define low-predicted fatal cases
**Goal**
Freeze the operational definition of hard-case mortality before comparative analysis begins.

**Details**
Define what counts as a low-predicted fatal case for each horizon. Decide whether the primary definition is threshold-based, quantile-based, or another simple prespecified rule. Document the rationale and make sure the definition is set before looking at comparative patterns.

**Deliverables**
- written primary hard-case definition
- written thresholding or quantile rule
- horizon-specific implementation note
- rationale documented

**Definition of done**
- the low-predicted fatal-case definition is fixed
- no post hoc threshold shopping is needed later

### [High] Characterize low-predicted fatal cases vs other fatal cases
**Goal**
Run the first main structural comparison of Chapter 1.

**Details**
Compare low-predicted fatal cases against other fatal cases on demographics, admission type, disease-group composition, ICU timing, site distribution, and organ support or dysfunction proxies if available. Produce one core table and one compact visual output. This issue turns the abstract “low-information component” into something concrete.

**Deliverables**
- comparison table for low-predicted vs other fatal cases
- compact figure or visualization
- summary of the main differences observed

**Definition of done**
- the main comparative analysis is complete
- at least one output is directly usable in the chapter

### [High] Analyze horizon dependence of mortality predictability structure
**Goal**
Test whether the apparent low-information component changes materially by prediction horizon.

**Details**
Repeat the core structure analysis for the frozen horizons. Compare low-predicted death share, mortality-vs-risk shape, and any major pattern changes across horizons. Write a short interpretation of whether hard cases shrink, persist, or change form when the horizon changes.

**Deliverables**
- horizon-specific structure outputs
- comparison of low-predicted death share across horizons
- short interpretation memo

**Definition of done**
- horizon dependence is documented clearly enough to guide later sensitivity interpretation

### [High] Write Sprint 2 viability memo
**Goal**
Decide whether Chapter 1 already stands on descriptive structure or still depends too much on later summary modeling.

**Details**
Summarize the hard-case results and horizon results in one short memo. State whether the chapter already has a defensible descriptive core, whether decomposition should proceed at all, and what the main remaining risks are. The default stance is that Chapter 1 stands as a descriptive hard-case chapter unless decomposition clearly earns its place.

**Deliverables**
- 1-page viability memo
- explicit decomposition go/no-go statement
- main risk summary

**Definition of done**
- a clear viability judgment exists and can be used in Sprint 3 and supervisor discussion

### [High] Run minor supervisor check-in on hard-case results
**Goal**
Validate that the chapter still looks acceptable after the first real structural analysis.

**Details**
Prepare a short supervisor-facing summary of the hard-case findings and horizon dependence. Ask directly whether the chapter framing still looks right and whether the decomposition still deserves to remain in the plan.

**Deliverables**
- short hard-case findings summary
- short horizon summary
- decision question on decomposition role

**Definition of done**
- supervisor check-in is completed or ready to send immediately

### [Low] Stretch issue — Preview temporal aggregation sensitivity
**Goal**
Run an early optional check of whether the Chapter 1 hard-case pattern looks obviously dependent on the 8-hour aggregation choice.

**Details**
If Sprint 2 core work finishes early, rerun the frozen baseline Chapter 1 pipeline under alternative temporal aggregation choices:
- one finer resolution than 8 hours, if technically feasible
- 16-hour blocks
- 24-hour blocks

Keep cohort, endpoint, split, horizons, baseline models, and evaluation framework unchanged as far as possible. Compare calibration, mortality-vs-risk structure, low-predicted fatal-case prevalence, and overlap/stability of identified hard cases across aggregations. Interpret this as a robustness check for aggregation choice, not as proof that deaths are or are not under-signaled.

**Deliverables**
- compact temporal aggregation preview
- short comparison note across block sizes
- note on whether the main pattern looks stable enough to prioritize formal sensitivity work in Sprint 3

**Definition of done**
- an early optional aggregation check exists or is dropped without affecting Sprint 2 completion

### [High] Update Linear and context at Sprint 2 close
**Goal**
Close Sprint 2 cleanly and preserve the hard-case decision state.

**Details**
Update Linear to match actual issue status. Update `context.md` with the hard-case definition, main findings, horizon dependence, and current decomposition go/no-go decision.

**Deliverables**
- Linear statuses updated
- Sprint 2 summary written
- `context.md` updated with hard-case findings and current decision state

**Definition of done**
- Sprint 2 can be reconstructed from `context.md` without digging through chat or notebooks

### [Low] Stretch issue — Add early-vs-late ICU death subgroup split
**Goal**
Explore whether low-predicted fatal cases differ meaningfully between early and later ICU deaths if sprint core work finishes early.

**Details**
Run one additional stratified comparison splitting deaths by ICU timing. This is useful only if the main hard-case analysis is already complete.

**Deliverables**
- early-vs-late subgroup comparison
- short note on whether timing materially changes interpretation

**Definition of done**
- a concise timing subgroup result exists or the issue is dropped without consequence

### [Low] Stretch issue - Temporal aggregation sensitivity across finer and coarser resolutions
**Goal**
Assess whether the Chapter 1 hard-case pattern is robust to alternative temporal aggregation choices. 

**Analysis**
- Rerun the frozen Chapter 1 baseline pipeline using: 
    - **finer resolution:** one block size smaller than 8h, if feasible from the raw data and preprocessing pipeline
    - **primary resolution:** 8h
    - **coarser resolution:** 16h and 24h
  - Keep cohort, endpoint, horizon hierarchy, split strategy, baseline models, and evaluation framework unchanged as far as possible
  - compare:
    - calibration
    - mortality-vs-predicted-risk structure
    - prevalence of low-predicted fatal cases
    - overlap/stability of identified hard cases across resolutions

**Interpretation rule**
- strong instability across resolutions weakens any claim that the observed hard-case structure reflects a robust signal
- broad stability across resolutions suggests the pattern is **not merely an artifact of one specific aggregation choice**
- stability does **not** rule out possible observation-process dependence, omitted variables, or treatment-limitation confounding

**Definition of done**
- a short sensitivity memo states whether the main Chapter 1 pattern is stable, weakened, or substantially altered under alternative temporal resolution

---

## Sprint 3 — Observation Process, Missingness, Treatment-Limitation Sensitivity

### [High] Derive observation-process variables
**Goal**
Build the observation-process dataset needed for Phase 1 sensitivity analysis.

**Details**
Implement the variable derivations chosen in Sprint 1: measurement density, missingness burden, longest gaps, time since last observation, and simple event-density or rhythm variables if feasible. Document each variable clearly enough that it can be described in methods and reproduced later.

**Deliverables**
- observation-process dataset
- variable definition note
- derivation logic note

**Definition of done**
- the observation-process variables are derived and usable in analysis
- variable meaning and derivation are documented clearly

### [High] Test association between low-predicted fatal cases and observation process
**Goal**
Determine whether hard-case deaths are enriched for sparse or irregular monitoring patterns.

**Details**
Compare low-predicted fatal cases and other fatal cases on the derived observation-process variables. Summarize effect sizes and determine whether measurement-process artifacts appear to explain a meaningful share of the hard-case pattern. If feasible, check whether the result is similar across the main horizons.

**Deliverables**
- observation-process comparison analysis
- effect size summary
- short interpretation memo

**Definition of done**
- the likely contribution of observation-process artifacts is documented explicitly
- the result can be incorporated into the central Chapter 1 interpretation

### [High] Run treatment-limitation and end-of-life sensitivity analysis
**Goal**
Test the strongest confounding explanation for low-predicted mortality.

**Details**
Use the available treatment-limitation or end-of-life proxies to compare low-predicted fatal cases and other fatal cases. If proxies are weak or absent, document the limitation explicitly instead of pretending the issue is addressed. If feasible, inspect whether these proxies vary by site.

**Deliverables**
- treatment-limitation sensitivity analysis or explicit absence note
- summary of proxy usefulness and weakness
- site note if feasible

**Definition of done**
- treatment-limitation sensitivity has been analyzed or formally declared untestable with reasons
- the interpretation risk is documented clearly

### [High] Run temporal aggregation sensitivity analysis
**Goal**
Test whether the Chapter 1 structure is materially dependent on temporal aggregation.

**Details**
Rerun the core structure analysis using:
- one finer resolution than 8 hours, if technically feasible
- 16-hour blocks
- 24-hour blocks

Compare calibration, mortality-vs-risk structure, low-predicted fatal-case prevalence, and overlap/stability of identified hard cases across the main and alternative aggregations. Write a short interpretation of whether the main Chapter 1 pattern is stable, weakened, or substantially altered under alternative block definitions.

**Deliverables**
- alternative aggregation datasets or runs
- rerun structure outputs
- aggregation comparison summary
- short interpretation memo

**Definition of done**
- temporal aggregation dependence is explicitly tested and documented

### [High] Update Chapter 1 interpretation after sensitivity analyses
**Goal**
Revise the central Chapter 1 interpretation based on what the sensitivity analyses actually show.

**Details**
Rewrite the bounded claim if necessary. State clearly how much of the hard-case pattern may be explained by observation process, temporal aggregation, or treatment-limitation structure. This is where the scientific story becomes honest and review-resistant.

**Deliverables**
- revised interpretation memo
- revised bounded claim wording if needed
- summary of which alternative explanations matter most

**Definition of done**
- an updated defensible interpretation exists and can be reused in writing and supervisor discussion

### [High] Run minor supervisor check-in on sensitivity findings
**Goal**
Confirm that the revised Chapter 1 interpretation remains acceptable after alternative explanations are tested.

**Details**
Prepare a concise summary of sensitivity findings and the revised bounded claim. Ask directly whether decomposition still adds enough value to justify doing it in Sprint 4.

**Deliverables**
- sensitivity summary for supervisor
- revised claim wording summary
- decomposition value question prepared

**Definition of done**
- supervisor check-in completed or ready to send immediately

### [High] Update Linear and context at Sprint 3 close
**Goal**
Close Sprint 3 cleanly and preserve the revised interpretation state.

**Details**
Update Linear and then update `context.md` with the observation-process results, treatment-limitation sensitivity status, temporal aggregation findings, and revised claim wording.

**Deliverables**
- Linear statuses updated
- Sprint 3 summary written
- `context.md` updated with sensitivity outcomes and revised interpretation

**Definition of done**
- Sprint 3 state is fully preserved for Sprint 4 startup

### [Low] Stretch issue — Fit simple multivariable model for low-predicted fatal-case membership
**Goal**
Run one compact exploratory model to summarize which factors are associated with low-predicted fatal-case membership if sprint core work finishes early.

**Details**
Fit a simple interpretable model using observation-process variables, timing, and any available treatment-limitation proxies. This is only worth doing if the core sensitivity work is already complete.

**Deliverables**
- exploratory membership model
- short summary of strongest associated factors

**Definition of done**
- one compact exploratory summary exists or the issue is dropped with no impact on the sprint

### [Low] Stretch issue - Temporal aggregation sensitivity using coarser block definitions
**Goal**
Assess whether the main Chapter 1 hard-case pattern is robust to alternative aggregation choices.

**Analysis:**


---

## Sprint 4 — Compact Decomposition / Weighting Summary and Stratified Structure

### [High] Decide whether decomposition remains justified
**Goal**
Make an explicit gate decision on whether the decomposition still deserves to be done.

**Details**
Review the Sprint 2 viability memo and Sprint 3 sensitivity findings. Decide whether the decomposition should proceed as a secondary summary, be downgraded, or be dropped entirely. Record the rationale. This issue exists to stop sunk-cost decomposition work.

**Deliverables**
- written decomposition decision
- short rationale note

**Definition of done**
- there is a documented proceed/downgrade/drop decision before any decomposition work starts

### [High] Implement compact decomposition or weighting summary
**Goal**
Fit the secondary summary formulation for mortality predictability heterogeneity.

**Details**
If decomposition remains justified, fit the primary compact summary model and extract posterior weights or probabilities plus uncertainty. Keep the framing operational: this is a summary device, not class discovery.

**Deliverables**
- fitted compact summary model
- posterior weights or probabilities
- uncertainty outputs

**Definition of done**
- the summary outputs exist and are usable for interpretation or later downstream decisions

### [High] Test decomposition robustness
**Goal**
Determine whether the summary model is stable enough to retain in the chapter.

**Details**
Check robustness across the two baseline model classes, recalibration, main horizons, and temporal aggregation. If feasible, inspect site and disease-stratified stability. The purpose is not to rescue the model. It is to decide honestly whether it is robust enough to keep.

**Deliverables**
- robustness summary across model class
- robustness summary across recalibration and horizon
- robustness summary across temporal aggregation
- retain/downgrade/de-emphasize decision

**Definition of done**
- there is a clear judgment on whether the decomposition is robust enough to remain in the chapter

### [High] Run disease-stratified predictability-structure analyses
**Goal**
Assess whether the main Chapter 1 pattern differs across clinically plausible groups.

**Details**
Run the main risk-structure analysis in the key strata that are feasible and relevant, such as viral pneumonia/viral ARDS if available, bacterial infection or sepsis-related cases if available, medical vs surgical, and a non-pulmonary comparator group. Focus on whether the direction and strength of predictability heterogeneity are stable.

**Deliverables**
- stratified analysis outputs
- summary table or figure
- interpretation note on heterogeneity across groups

**Definition of done**
- key disease-stratified comparisons are complete and interpretable

### [High] Decide downstream usability of weighting or decomposition
**Goal**
Decide whether any soft weighting or summary output is credible enough for later thesis chapters.

**Details**
Based on the robustness findings, decide whether the weighting/decomposition output is strong enough to reuse later, weak enough that it should remain only descriptive, or too fragile to use at all.

**Deliverables**
- downstream usability memo
- recommendation for later chapters
- limitation note for downstream use

**Definition of done**
- a clear recommendation exists on whether later chapters should use the summary output

### [High] Freeze Chapter 1 figure and table plan
**Goal**
Lock the chapter figure and table set based on actual results.

**Details**
Choose the final core figures and tables. Prioritize the outputs that carry the scientific argument: mortality-vs-risk, hard-case comparison, sensitivity results, stratified results, and external validation placeholders. Drop decorative or redundant outputs.

**Deliverables**
- final figure list
- final table list
- note on dropped redundant outputs

**Definition of done**
- the Chapter 1 figure/table plan is frozen and aligned with the actual argument of the chapter

### [High] Update Linear and context at Sprint 4 close
**Goal**
Close Sprint 4 cleanly and preserve the near-final Chapter 1 analysis state.

**Details**
Update Linear. Then update `context.md` with the decomposition decision, robustness status, stratified results, downstream usability recommendation, and the final figure/table plan.

**Deliverables**
- Linear statuses updated
- Sprint 4 summary written
- `context.md` updated with near-final Chapter 1 analysis state

**Definition of done**
- Sprint 4 state is preserved cleanly for Sprint 5 startup

### [Low] Stretch issue — Compare one alternative summary formulation
**Goal**
Run one alternative compact summary formulation if core sprint work finishes early.

**Details**
Test one alternative summary method only to check whether the main conclusion is method-specific. This should not become a methods detour.

**Deliverables**
- one alternative summary run
- short comparison note against the primary summary method

**Definition of done**
- one additional method comparison exists or the issue is dropped without impact

---

## Sprint 5 — External Validation, Draft Freeze, Supervisor Delivery

### [High] Reproduce Chapter 1 pipeline on MIMIC-IV
**Goal**
Run the final Chapter 1 pipeline externally in MIMIC-IV.

**Details**
Using the finalized ASIC pipeline, reproduce the baseline risk models, evaluation outputs, hard-case analysis, and required sensitivity components in MIMIC-IV. Include the decomposition only if it survived Sprint 4.

**Deliverables**
- MIMIC baseline model outputs
- MIMIC calibration/discrimination outputs
- MIMIC mortality-vs-risk plots
- MIMIC hard-case characterization
- MIMIC observation-process sensitivity where feasible
- MIMIC decomposition summary if retained

**Definition of done**
- the external validation output package is complete

### [High] Compare ASIC and MIMIC-IV Chapter 1 results
**Goal**
Assess transportability of the main Chapter 1 conclusions.

**Details**
Compare model performance, mortality-vs-risk structure, hard-case characteristics, subgroup effects, and decomposition behavior if retained. Decide whether the result replicates strongly, partially, or fails materially.

**Deliverables**
- ASIC vs MIMIC comparison summary
- replication classification
- interpretation of transport limitations

**Definition of done**
- a clear external comparison judgment exists and can be written into the chapter

### [High] Freeze final Chapter 1 verdict
**Goal**
Lock the final interpretation category for Chapter 1.

**Details**
Classify the chapter result as strong positive, weak but usable, or negative bounded result. Write the justification and state what it means for later chapters.

**Deliverables**
- final verdict memo
- justification note
- downstream implications note

**Definition of done**
- the final interpretation is frozen and ready to be written

### [High] Draft Chapter 1 manuscript package
**Goal**
Convert the completed Phase 1 analysis into a supervisor-ready Chapter 1 draft.

**Details**
Draft the introduction, methods, results, and discussion. Integrate the bounded claim, the literature framing, explicit non-claims, external validation, and named limitations. The package should be complete enough that the supervisor can comment on the actual chapter, not just on notes.

**Deliverables**
- draft introduction
- draft methods
- draft results
- draft discussion
- integrated figures/tables
- integrated limitations and non-claims

**Definition of done**
- a full Chapter 1 draft exists and is ready to send

### [High] Send Chapter 1 package to supervisor
**Goal**
Deliver the completed Chapter 1 package for supervisor review.

**Details**
Prepare a short cover note, assemble the final draft plus figure/table bundle, send the package, and log what was sent and when. Keep this issue focused on actual delivery, not on more writing.

**Deliverables**
- supervisor cover note
- final draft package assembled
- package sent
- send date and contents logged

**Definition of done**
- the Chapter 1 package has been sent to the supervisor

### [High] Update context for Phase 2 handover
**Goal**
Close Phase 1 cleanly so the next chat and sprint can start without ambiguity.

**Details**
Update `context.md` with the final Chapter 1 result, what outputs are reusable downstream, what unresolved risks remain, the supervisor feedback status, and what the next phase depends on.

**Deliverables**
- final Chapter 1 result summary
- reusable downstream outputs summary
- unresolved risks summary
- supervisor feedback status summary
- updated next-phase dependencies in `context.md`

**Definition of done**
- `context.md` is fully updated for Phase 2 startup

### [Low] Stretch issue — Prepare concise abstract and anticipated feedback questions
**Goal**
Prepare a short abstract-style summary and likely feedback questions if core sprint work finishes early.

**Details**
Write a concise abstract or chapter summary paragraph and list the likely supervisor objections or questions. This is useful for faster revision after supervisor feedback but is not required for Sprint 5 completion.

**Deliverables**
- short abstract or chapter summary
- anticipated feedback question list

**Definition of done**
- a concise summary and question list exist or the issue is dropped with no impact
