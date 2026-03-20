# Decomposing Patient-Therapy Dynamics in Ventilated ICU Patients

## Central Question

ICU observational data is the output of a closed-loop system — disease progression and therapeutic control are entangled. Can we structurally separate what the disease does from what therapy does, using multi-center variation as the analytical lever?

---

## Document Purpose

This document defines the full scientific program for the dissertation. It is organized in two layers per analysis:

- **Ideal program** — the methodologically complete version assuming no time constraints (~3 years).
- **Feasible scope** — the reduced version achievable within 12 months given existing infrastructure, data access, and completed work.

The ideal program exists so that if progress exceeds expectations, effort can be directed toward deepening any analysis rather than inventing new scope.

---

## Dissertation Architecture

Four chapters. Each asks one question. Each answer enables the next.

```
Introduction: Frames the closed-loop problem. Cites prior work.
              Defines what "separating disease from therapy" means
              and why it matters.

Chapter 1: Does the therapeutic control loop govern all ICU mortality?
    ↓ defines the domain of influence of the control loop
Chapter 2: What does the controller do, and where do controllers disagree?
    ↓ makes H* explicit, identifies where inter-hospital variation exists
Chapter 3: Can we structurally separate treatment effects from state evolution?
    ↓ the methodological core — builds and evaluates the decomposed model
Chapter 4: Does the decomposition generalize to unseen treatment policies?
    ↓ the empirical test — cross-hospital counterfactual validation

Discussion & Conclusion: Synthesizes findings into a reliability framework.
    What is modelable (Ch1), what the controller does (Ch2), what the model
    captures (Ch3), and where it generalizes (Ch4). Connects back to proposal.
```

**Structural principle:** Every chapter directly addresses the patient-therapy control loop. There is no padding. Chapters 1–2 are analytical prerequisites. Chapters 3–4 are the methodological core and its validation. The discussion serves the synthesis role that was previously a standalone chapter.

**Dependency structure:** Chapters are narratively sequential but partially logically independent. Chapter 2 does not require Chapter 1. Chapters 3–4 require Chapter 2 (for the learned policy H*). Chapter 3 benefits from Chapter 1 (training on the progressive-failure cohort) but can function without it. Chapter 4 requires Chapter 3.

---

## Prior Work: Role in the Introduction

Two completed bodies of work are not thesis chapters but provide important context.

### Phase-Specific ICU Mortality Prediction (Submitted Manuscript)

**Disposition:** Cited in the introduction. Not a thesis chapter.

**Why excluded:** The analysis demonstrates that prediction performance varies by temporal window and that feature importance shifts over the ICU course. However, this is primarily about information availability and prediction deployment — it does not address the patient-therapy control loop. The model deliberately excludes treatment variables. It answers "when can we predict?" not "why does the system behave as it does?"

**How it serves the thesis:** "In prior work [self-cite], we showed that ICU mortality prediction is phase-dependent and that the drivers of risk shift over the course of the stay. However, prediction alone does not explain *why* risk evolves — specifically, what role therapeutic interventions play in shaping patient trajectories. This thesis addresses that question by decomposing the patient-therapy control loop."

**Specific elements reused:**

- The ASIC + MIMIC-IV cross-national validation framework. Describe the data infrastructure in the introduction; no need to re-derive per chapter.
- The feature engineering pipeline (8h blocks, LOCF, harmonization) is shared infrastructure.
- The observation that predictability has a ceiling motivates Chapter 1: why does that ceiling exist?

### Anomaly Detection — Zebra and Viability Hypotheses (Completed, Negative)

**Disposition:** Dropped from the thesis entirely.

**Why excluded:** These analyses answer a different question ("do unsupervised temporal methods capture novel physiological signals?") that does not connect to the control loop storyline. Including them — even as an appendix — dilutes the narrative without strengthening the argument. The negative results are informative but tangential.

**If asked by a committee member:** Mention verbally that unsupervised anomaly detection was explored and found to collapse to nonlinear severity supplements, reinforcing the need for explicit structural decomposition. Do not devote pages to it.

---

## Chapter 1: Does the Therapeutic Control Loop Govern All ICU Mortality?

### Status: EARLY STAGE — requires substantial completion

### The Question

Not all ICU deaths are equally well captured by routinely collected ICU data. Some patients deteriorate along observable dimensions over hours to days — risk accumulates, organs fail sequentially, and therapy either slows or fails to prevent death. Other patients die without a strong detectable deterioration signature in the available feature space — death occurs across the risk spectrum, consistent with events that are not well represented in the observed state vector (for example sudden cardiac arrest, massive PE, undiagnosed pathology, catastrophic events, or clinically relevant signals that were simply not measured).

The working distinction is therefore not "deterministic" versus "random" in any ontological sense. It is between deaths that appear **more aligned** with observable disease-therapy evolution and deaths that appear **less aligned** with it, given the data actually collected. This is a modeling and measurement claim first, and only cautiously a biological one.

This chapter asks: can we quantify the boundary between these regimes in observed ICU data? And does this boundary depend on the underlying disease?

### Scientific Contribution

1. **A finding about the control loop as observed through the available data:** The loop has a measurable domain of influence in the recorded state space. This is not merely a data cleaning step — it is a statement about the structure of the patient-therapy system as represented in ICU data.
2. **Principled domain restriction for downstream modeling:** Chapters 3–4 model treatment effects. Including patients whose outcomes are weakly coupled to the observed state trajectory may inject noise into treatment effect estimates. Restricting or weighting by this structure is justified if the signal is real.
3. **Connection to the proposal:** The proposal describes the patient-therapy interaction as a closed-loop controlled system. This chapter characterizes where that description appears to apply in practice and where it becomes unreliable.
4. **A practical reliability statement:** Even if the interpretation remains agnostic about true biological subtypes, the chapter tells you which deaths are predictable with the data as collected. That matters for any deployed prediction or control-oriented model.

### Ideal Program (~12 months standalone)

### 1.1 Formalize the Decomposition Framework

**Hypothesis:** Observed ICU mortality can be decomposed into a low-information/background component (approximately uniform across risk levels up to a threshold) and a signal component (concentrated at high predicted risk, consistent with progressive organ failure).

**Interpretive stance:** This is a statement about deaths relative to the available feature set, not proof of irreducible biological randomness. Unobserved is not the same as unobservable. Any "stochastic" or "sudden" label in this chapter means poorly explained by the recorded variables at the chosen temporal resolution, not inherently unpredictable in principle.

**Method — Statistical Mixture Decomposition:**

- Develop a population-level mortality risk score using methods independent of the decomposed dynamics model in Chapter 3 (e.g., logistic regression or simple ensemble on admission + early features). This independence is critical to avoid circularity — see Methodological Safeguards.
- Plot observed mortality rate as a function of predicted risk score (binned or smoothed).
- Formally fit a two-component mixture model to the mortality distribution:
    - Component 1 (background / low-information floor): Approximately uniform across risk levels. Interpreted as deaths weakly captured by the observable state vector.
    - Component 2 (progressive failure): Risk-dependent component concentrated at high predicted risk. Interpreted as deaths where observable deterioration is strongly aligned with the fatal outcome.
- Estimate the mixing proportion and threshold via maximum likelihood or Bayesian methods.
- Use posterior probabilities as the primary output. Avoid over-committing to hard binary assignment except where operationally necessary.
- Sensitivity analysis: does the decomposition hold across different risk models (logistic regression, XGBoost, ensemble)?
- Sensitivity analysis: how stable is the decomposition to temporal aggregation (e.g., 4h vs 8h vs 12h blocks, if feasible)? If the apparent "sudden" component changes materially with charting resolution, report that explicitly.

**Why this specific approach:** Preliminary analysis (Schuppert) showed that below ~0.8 predicted risk, deaths are approximately uniformly distributed. Formalizing this as a mixture model gives a principled threshold rather than a visual estimate. The two-component model is a useful approximation for exposition and downstream gating, not a claim that ICU mortality truly consists of exactly two biological classes.

**Key diagnostic:** Check residual autocorrelation in the background component. If deaths classified as "stochastic" show temporal clustering or predictable deterioration in their time series, the decomposition is wrong — those deaths are just poorly predicted under the current feature set.

### 1.2 Biological Validation

**Disease-stratified analysis:**

- Repeat the decomposition separately for:
    - Viral pneumonia / viral ARDS
    - Bacterial pneumonia / bacterial sepsis
    - Non-pulmonary ICU admissions
    - Surgical vs. medical admissions
    - Trauma
- Preliminary findings (viral infections → mostly progressive; non-pulmonary → more stochastic) need formal quantification with confidence intervals.
- Hypothesis: diseases with well-characterized, observable deterioration pathways should show higher progressive-failure fractions. Categories with higher prevalence of sudden, unobservable events should show higher stochastic fractions.

**Temporal validation:**

- Does the stochastic fraction change over the course of the ICU stay? Expected: higher in the first 24–48h (incomplete information, more sudden events relative to accumulated knowledge), lower later.
- This reinterprets the phase-dependent predictability from prior work through the lens of the decomposition — a stronger scientific statement than the original paper makes.

**Clinical plausibility assessment (ideal):**

- Manual chart review of a subsample (n=50–100) of "stochastic deaths" to verify clinical courses are consistent with the interpretation. Labor-intensive, requires clinical collaborator time.

### 1.3 Patient-Level Classification Rule

The mixture model gives a population-level decomposition. For downstream use, you need patient-level assignment or weighting.

**Approach options:**

1. **Posterior probability from the mixture model:** For each death, compute P(progressive | risk score, outcome). Soft assignment.
2. **Threshold-based rule:** Hard cutoff at the identified crossover point. Simple, interpretable, but a simplification.
3. **Trajectory-based classification:** Progressive patients show deteriorating risk trajectories before death; background-component patients show stable or improving trajectories that terminate abruptly. Operationalize via slope of predicted risk in the final 48–72h.
4. **Hybrid of 1 and 3:** Posterior as primary, trajectory slope as secondary discriminator.
5. **Weighting rather than exclusion:** Use posterior probabilities to weight patients in downstream modeling rather than forcing binary membership.

**Preferred stance:** Use soft assignment or weighting unless a hard split is needed for a specific downstream experiment. The continuum interpretation is more plausible than claiming two clean biological types.

**Critical requirement:** The rule must be applicable using only past information. If it requires knowing the outcome, it cannot gate the hybrid model at prediction time. See Safeguard 4.

### 1.4 External Validation

- Develop on ASIC. Validate on MIMIC-IV: same two-component structure? Similar stochastic fraction? Disease-stratified patterns replicate?
- eICU as tertiary validation if available.

### 1.5 Downstream Impact Demonstration

- Show that excluding the stochastic component changes estimated treatment effects, not just AUROC. Run a simple treatment-outcome association (e.g., PEEP levels and oxygenation outcomes) on the full population vs. progressive-failure subpopulation. If the estimated effect changes meaningfully, the decomposition has practical consequences for therapeutic modeling.
- This bridges Chapter 1 to Chapters 2–3.

### Feasible Scope (within dissertation timeline)

**Must deliver:**

- Formalized mixture decomposition with quantified stochastic fraction (1.1 — using 2–3 risk score variants for robustness)
- Disease-stratified analysis with confidence intervals (1.2 — disease stratification only)
- Patient-level classification rule using posterior probability or threshold (1.3 — option 1 or 2)
- External validation on MIMIC-IV (1.4)
- At least one downstream impact demonstration (1.5 — simplified, single therapeutic variable)

**Can skip if needed:**

- Temporal validation of background fraction over ICU stay (1.2 temporal)
- Temporal-resolution sensitivity analysis beyond one alternative aggregation choice
- Trajectory-based classification refinement (1.3 options 3/4)
- Clinical chart review (1.2 clinical)
- eICU tertiary validation

**Estimated effort:** 6–8 weeks.

### What If the Decomposition Fails?

If the stochastic floor turns out to be negligible (<5% of deaths) or the mixture model doesn't fit cleanly, the chapter still contributes: it demonstrates that ICU mortality is overwhelmingly progressive and therefore lies within the control loop's domain. The thesis proceeds with the full population. The chapter becomes shorter but the finding is still valid — you tested a structural hypothesis about the system and got a clear answer.

If the decomposition is real but the stochastic fraction is small (say 10%), the finding stands as a scientific observation about the control loop. Training on the progressive cohort vs. full population may show only small downstream differences, but the structural insight is still worth reporting.

The thesis does not collapse if Chapter 1 produces a negative or weak result. It adjusts.


### Chapter 1 Limitations to State Explicitly

1. **Unobserved is not the same as unobservable.** The chapter can only claim poor predictability relative to the recorded feature set. It cannot distinguish irreducible randomness from predictability with richer measurements.
2. **Two groups are an approximation, not biological ground truth.** The mixture model is a convenient low-dimensional summary. The real structure may be continuous or multi-modal.
3. **Temporal resolution matters.** A "sudden" category may partly be induced by charting frequency and 8h aggregation. This does not negate the practical finding for deployed systems, but it weakens any stronger claim about fundamental predictability.
4. **Interpretation should stay operational.** The safest wording is "background / low-information mortality component" versus "progressive-failure-aligned component," not confident claims about true death subtypes.

---

## Chapter 2: What Does the Controller Do, and Where Do Controllers Disagree?

### Status: NOT STARTED — infrastructure ready

### The Question

The patient-therapy control loop requires a controller — the clinician making treatment decisions based on patient state, history, experience, and guidelines. Before modeling the loop, characterize the controller: what decisions do clinicians make in response to what states, and — critically — how do these decisions differ across hospitals?

ASIC's five-hospital structure is the analytical lever. Same diseases, different controllers. This variation is the closest thing to a natural experiment in observational ICU data.

### Scientific Contribution

1. *Makes H explicit:*Transforms clinician decision-making from an unmeasured confounder into a quantified, learnable function. This directly addresses the proposal's identification problem.
2. **Identifies the variation that powers the counterfactual test:** Chapter 4's cross-hospital counterfactual validation has discriminative power only where hospitals actually diverge in their treatment policies. This chapter maps where that divergence exists.
3. **Provides the treatment input for Chapter 3:** The decomposed dynamics model needs an explicit treatment pathway. The learned policy serves as this input.

### Ideal Program (~8–10 months standalone)

### 2.1 Define the Therapeutic Action Space

**Primary therapeutic levers (ventilated ICU patients):**

- FiO2 (fraction of inspired oxygen) — continuous, frequent
- PEEP (positive end-expiratory pressure) — continuous, less frequent
- Tidal volume / driving pressure — continuous
- Vasopressor dosing (norepinephrine equivalent) — continuous
- Fluid administration volume — continuous, per shift
- Sedation depth (RASS target / propofol-midazolam dosing) — ordinal/continuous

**Secondary levers:**

- Prone positioning (binary, per session)
- Renal replacement therapy initiation/settings
- Antibiotic escalation/de-escalation
- Neuromuscular blockade

**Data representation:** Discretize continuous actions into clinically meaningful bins (e.g., FiO2: ≤0.4, 0.4–0.6, 0.6–0.8, >0.8; PEEP: ≤8, 8–12, 12–16, >16 cmH2O).

**Temporal granularity:** 8h blocks, consistent with prior work.

### 2.2 Learn Hospital-Specific Treatment Policies

**Method:** For each therapeutic lever: P(action_t | state_t, state_{t-1}, ..., state_{t-k}, patient_context)

**Model choices:**

1. **Interpretable baseline:** Decision trees or logistic regression per action. Reveals decision rules directly.
2. **Flexible model:** XGBoost per action, interpretable via SHAP.
3. **Joint policy model (ideal):** Multi-output model capturing correlations between simultaneous decisions (e.g., FiO2 and PEEP adjusted together).

**Training strategy:**

- Hospital-specific models: one per hospital per action. Reveals inter-hospital variation.
- Pooled model with hospital as feature: reveals what is common vs. hospital-specific.
- Agreement quantification: for what fraction of patient-states do all hospitals make the same decision?

### 2.3 Quantify Inter-Hospital Policy Divergence

**Divergence metrics:**

1. **Policy disagreement rate:** Query all 5 hospital models for each patient-state. Fraction of states with disagreement.
2. **State-conditional divergence:** Map *where* in patient-state space hospitals diverge. Hypothesis: agreement on extremes (very sick → aggressive; stable → maintain), divergence on intermediate states.
3. **Temporal divergence:** Does disagreement vary by ICU day? Early (protocol-driven) vs. late (judgment-driven).
4. **Divergence clustering:** Do hospitals cluster into therapy "schools of thought"?

**Deliverable:** A quantified map of (a) which decisions are consensus-driven vs. hospital-specific, (b) which patient states produce the most divergent care, (c) which hospitals are most similar/different.

### 2.4 Validate Policy Models

- **Prediction accuracy:** Accuracy/F1 per action per hospital. If the model can't predict clinical decisions, the learned policy is unreliable.
- **Temporal out-of-sample:** Train on first 80% of time period, test on last 20%. Checks for temporal drift.
- **Cross-hospital transfer:** Train on hospital A, predict hospital B's decisions. Prediction error quantifies policy difference.

### 2.5 Clinical Interpretation

- Identify top 3–5 divergence points (patient states × decisions with highest inter-hospital variation).
- Map to known clinical controversies (PEEP titration in moderate ARDS, liberal vs. conservative fluids, etc.).
- Grounds statistical divergence in clinical reality.

### Feasible Scope (within dissertation timeline)

**Must deliver:**

- Action space definition with clinical justification (2.1 — primary levers only)
- Hospital-specific policy models for 3–4 key actions: FiO2, PEEP, vasopressor dosing, fluid volume (2.2 — interpretable baseline + XGBoost)
- Policy divergence: disagreement rate and state-conditional divergence (2.3 — metrics 1 and 2)
- Prediction accuracy per hospital (2.4 — basic validation)
- Brief clinical interpretation of top divergence points (2.5 — narrative)

**Can skip if needed:**

- Secondary levers (2.1)
- Joint multi-output policy model (2.2)
- Temporal divergence and clustering (2.3)
- Cross-hospital transfer and temporal drift validation (2.4)

**Estimated effort:** 4–6 weeks.

---

## Chapter 3: Can We Structurally Separate Treatment Effects from State Evolution?

### Status: NOT STARTED — partial infrastructure from prior LSTM work

### The Question

Given a patient in state x(t) receiving treatment u(t), can we build a model that separates "what happens because of the disease" from "what changes because of the specific treatment choice" — and does this separation produce treatment effect estimates that are clinically coherent?

### Scientific Contribution

This is the **methodological core** of the thesis. The contribution is an architecture that enforces separation between disease state evolution and therapeutic response through information restriction — not through mechanistic equations or hybrid modeling in the classical sense, but through structural design that prevents the disease pathway from learning treatment-specific dynamics.

### What the Architecture Achieves — and What It Does Not

**What it achieves:** Isolation of *concurrent marginal treatment effects* — the deviation in state transition caused by a specific treatment choice at time t, relative to the expected transition averaged across the training population's treatment distribution.

**What it does not achieve:** Separation of cumulative disease progression from cumulative therapy effects. The patient's state x(t) is already shaped by the entire treatment history. The disease pathway f_disease learns expected transitions given a state that is itself the product of disease + all prior therapy. This is an inherent limitation of the single-step decomposition.

**What multi-step rollout adds:** When the model simulates trajectories forward over multiple steps under a specific policy, the cumulative contribution of that policy propagates through the state. Trajectory divergence between two simulated policies after N steps reflects the accumulated policy effect. This does not achieve full causal identification but provides a structural approximation that a black-box model cannot.

**The thesis must be explicit about these distinctions.** Do not claim the model separates "intrinsic disease dynamics" from "extrinsic therapeutic control" in the absolute sense. Claim it separates expected state evolution from marginal treatment effects, and that multi-step simulation under alternative policies reveals cumulative policy effects through trajectory divergence.

### Ideal Program (~12–18 months standalone)

### 3.1 Specify the State Space

**Patient state vector x(t) (continuous, 8h granularity):**

Core state variables:

- Oxygenation: PaO2/FiO2 ratio (or SpO2/FiO2 as proxy), PaCO2
- Hemodynamics: MAP, heart rate, vasopressor dose (as state, not just action)
- Organ function: lactate, creatinine, bilirubin, platelet count, INR
- Fluid balance: cumulative fluid balance, urine output
- Inflammatory: CRP, PCT (if available), temperature, WBC
- Ventilation state: compliance (Vt / driving pressure), minute ventilation

Context variables (static or slowly changing):

- Age, sex, BMI, admission diagnosis category, Charlson comorbidity index
- APACHE II / SAPS II at admission

**Therapeutic action vector u(t):** From Chapter 2:

- FiO2, PEEP, vasopressor dose change, fluid volume, sedation depth

**State transition target:** x(t+1) — next 8h block.

### 3.2 Two-Pathway Architecture

The model predicts state transitions through two structurally separated pathways:

```
x(t+1) = f_disease(x(t), context) + g_therapy(x(t), u(t))
```

**Disease pathway f_disease:**

- Receives: patient state x(t), static context variables.
- Does NOT receive: therapeutic action u(t). This information restriction is the definitional feature.
- Learns: expected state transitions averaged across the training population's aggregate treatment distribution.
- Interpretation: "what happens next given this state, under average care."

**Therapy pathway g_therapy:**

- Receives: patient state x(t) AND specific therapeutic action u(t).
- Learns: the marginal deviation from expected state evolution caused by a specific treatment choice.
- Interpretation: "how does this specific treatment choice change the expected outcome?"
- Optional enhancement: monotonicity constraints on known directional relationships (FiO2 ↑ → oxygenation should not decrease; vasopressor ↑ → MAP should not decrease). These are not the definitional feature — the information restriction is — but they add physiological plausibility.

**Why additive structure:** The additive decomposition (f + g) enforces that treatment effects are modeled as deviations from a baseline trajectory, not as interactions that reshape the entire dynamics. This is a modeling assumption. Alternatives (multiplicative, gated) exist but are harder to interpret and validate. Start with additive; explore alternatives only if additive demonstrably fails.

**What f_disease actually learns:** It does NOT learn disease progression without treatment. It learns state evolution averaged across whatever treatments were given in the training data. The treatment history is baked into x(t). This is a fundamental limitation. Being explicit about this in the thesis is essential — it is the honest version of the decomposition, and it is still useful for counterfactual reasoning about marginal policy changes.

### 3.3 Network Architecture Options

**Disease pathway f_disease:**

1. **GRU/LSTM:** Processes a lookback window of patient states. Captures temporal patterns in disease evolution.
2. **Simple feedforward:** Takes current state x(t) plus a summary of recent history (e.g., 24h trends). Simpler, more interpretable.

**Therapy pathway g_therapy:**

1. **Feedforward with state conditioning:** Takes concatenated (x(t), u(t)), outputs deviation. Simple baseline.
2. **Interaction architecture:** Separate embeddings for state and action, combined via inner product or attention. Captures state-dependent treatment effects (e.g., FiO2 increase matters more when PaO2/FiO2 is low).
3. **Monotone network:** Constrained architecture ensuring directional correctness on known relationships. Adds physiological plausibility.

**Recommended starting point:** GRU for f_disease (reuse existing infrastructure), feedforward with monotonicity constraints for g_therapy. Simplest architecture that enforces the separation.

### 3.4 Training

**Option 1 — Sequential (recommended for feasible scope):**

- Train f_disease alone: predict x(t+1) from x(t) and context, ignoring treatment. This learns the average-treatment baseline.
- Compute residuals: r(t) = x(t+1) - f_disease(x(t), context).
- Train g_therapy on the residuals: predict r(t) from (x(t), u(t)).
- Advantage: interpretable decomposition. Easy to diagnose.
- Disadvantage: suboptimal — errors in f_disease propagate without correction.

**Option 2 — Joint (ideal):**

- Train f_disease + g_therapy end-to-end to minimize prediction error on x(t+1).
- Regularize: penalty on ||g_therapy|| to prefer disease-pathway explanations over therapy-pathway explanations. Without this, g_therapy may absorb dynamics that should be in f_disease.
- Advantage: better overall prediction.
- Disadvantage: harder to interpret; risk of therapy pathway dominating.

**Option 3 — Hybrid training:**

- Initialize with sequential training. Fine-tune jointly with regularization.
- Gets interpretability of sequential + accuracy of joint.

### 3.5 Training on the Progressive-Failure Cohort

Using the classification from Chapter 1:

- Train the decomposed model on the progressive-failure subpopulation.
- Train a comparison model on the full population.
- Compare: does the progressive-failure model produce sharper, more directionally consistent treatment effect estimates?

If Chapter 1 produced a weak or negative result (small stochastic fraction), train on the full population and skip this comparison. The thesis does not depend on this step.

### 3.6 Treatment Effect Evaluation

**Single-step treatment effects:**

- For a given patient state x(t), compute predicted x(t+1) under action u_A vs. u_B.
- The difference g_therapy(x(t), u_A) - g_therapy(x(t), u_B) gives the estimated marginal treatment effect.
- Evaluate: are the estimated effects directionally consistent with known physiology? Does increasing FiO2 improve oxygenation? Does increasing vasopressor dose increase MAP?
- Clinical coherence is the primary evaluation criterion for the therapy pathway. Prediction accuracy is secondary.

**Multi-step trajectory simulation:**

- Initialize with a patient's admission state.
- Simulate forward using a specific treatment policy (from Chapter 2) over 24–72h.
- Compare predicted trajectory to observed trajectory.
- Evaluate: trajectory RMSE at 1-step (8h), 3-step (24h), 6-step (48h) horizons.

### 3.7 Model Evaluation and Baselines

**Baselines:**

- Persistence: x(t+1) = x(t)
- Linear autoregression: x(t+1) = Ax(t) + Bu(t) + c
- Black-box GRU: x(t+1) = h(x(t), u(t), context) — no structural separation

**Metrics:**

- State transition RMSE per variable (the decomposed model should be competitive with the black box)
- Decomposition quality: what fraction of variance is captured by f_disease vs. g_therapy?
- Treatment effect directional consistency: fraction of known relationships where g_therapy produces the physiologically correct sign
- Multi-step rollout stability: does the model diverge or remain plausible over 6–12 steps?

**Ablations (ideal):**

- Remove g_therapy → how much does prediction degrade? (Quantifies the treatment signal.)
- Remove information restriction (give f_disease access to u(t)) → does the decomposition collapse? (Tests whether the restriction is doing meaningful work.)
- Remove Chapter 1 filtering → how does treatment effect estimation change?

### Feasible Scope (within dissertation timeline)

**Must deliver:**

- State space specification with clinical justification (3.1)
- Two-pathway architecture with information restriction (3.2 — the core contribution)
- GRU for f_disease, feedforward with optional monotonicity for g_therapy (3.3 — simplest viable architecture)
- Sequential training (3.4 option 1)
- Treatment effect evaluation: directional consistency for 2–3 key actions (3.6 — single-step)
- Prediction accuracy vs. baselines (3.7 — persistence, linear AR, black-box GRU)
- Decomposition quality: variance fraction in f vs. g (3.7)

**Can skip if needed:**

- Joint or hybrid training (3.4 options 2, 3) — sequential is sufficient
- Multi-step trajectory simulation (3.6) — nice to have but Chapter 4 covers this
- Progressive-failure cohort comparison (3.5) — depends on Chapter 1 strength
- Full ablation suite (3.7) — do the information-restriction ablation only if time permits

**Estimated effort:** 8–10 weeks. The riskiest chapter. Sequential training de-risks: if the architecture works, you'll know within 3–4 weeks.

---

## Chapter 4: Does the Decomposition Generalize to Unseen Treatment Policies?

### Status: NOT STARTED — depends on Chapters 2 and 3

### The Question

The decomposed model claims to separate disease state evolution from treatment effects. If this separation is real — not just an artifact of the architecture — it should generalize to treatment policies the model has never seen. A black-box model, which entangles disease and treatment, should fail this test.

ASIC's five-hospital structure makes this a natural experiment: train on four hospitals' disease dynamics and treatment policies, then predict what happens to the fifth hospital's patients under the fifth hospital's treatment policy.

### Scientific Contribution

This is the **conditional empirical test** of the thesis. Without this chapter, the decomposition in Chapter 3 remains an architectural claim with limited external stress-testing. With it, the claim becomes testable and falsifiable. But Chapter 4 is only worth running if Chapter 3 already demonstrates that there is something to validate.

### Ideal Program (~6–8 months standalone)

### 4.1 Experimental Design: Leave-One-Hospital-Out

**Protocol:**

- For each hospital h ∈ {1, 2, 3, 4, 5}:
    1. Train the decomposed dynamics model (Chapter 3) on data from hospitals {1,...,5} \ {h}.
    2. Use hospital h's pre-learned policy model (Chapter 2).
    3. For each patient in hospital h:
    a. Initialize with the patient's actual admission state.
    b. Simulate the trajectory using hospital h's learned policy applied to the dynamics model trained on the other hospitals.
    c. Compare predicted trajectory to observed trajectory.
    4. Repeat step 3 using a *different* hospital's policy to generate counterfactual trajectories.

**Key comparison pair:**

- **Reconstruction accuracy:** Predicted trajectories under the held-out hospital's actual policy vs. observed trajectories. Tests whether the dynamics model generalizes across hospitals.
- **Counterfactual plausibility:** Predicted trajectories under an alternative hospital's policy. Tests whether the decomposition enables credible counterfactual reasoning.

### 4.2 Comparison Against Black-Box Baseline

**Critical test:** Run the same leave-one-hospital-out protocol with the black-box GRU from Chapter 3 (no structural separation, treatment as regular input).

**Hypothesis:** The black-box model will perform comparably on reconstruction (same-policy prediction) but degrade more on counterfactual (cross-policy) prediction, because it has learned entangled state-treatment dynamics specific to the training hospitals.

If the decomposed model does NOT outperform the black box on the counterfactual task, the thesis's central architectural claim is not supported. This must be reported honestly. See Risk Assessment.

**Metrics:**

- Multi-step trajectory RMSE (8h, 24h, 48h horizons) — decomposed vs. black box, reconstruction vs. counterfactual
- Clinical coherence: do treatment effect directions match expected physiology?
- Predicted mortality alignment: do trajectories predicted to end in death correspond to observed deaths?

### 4.3 Focus on Key Therapeutic Divergences

Rather than simulating all policy transfers, focus on the 2–3 therapeutic dimensions with greatest inter-hospital divergence (identified in Chapter 2):

- Example: hospitals A and C differ most on PEEP management. Apply hospital A's PEEP policy to hospital C's patients (keep all other actions from hospital C). Predict oxygenation and compliance trajectories. Compare to hospital C's actual trajectories.
- This focused approach is more interpretable and has more statistical power than a full policy swap.

### 4.4 Outcome-Level Validation

- For patients who died in hospital h: does the model predict different trajectories under hospital j's policy?
- For survivors: is the model consistent (doesn't predict death under the alternative policy)?
- Aggregate: does the predicted mortality rate under hospital j's policy approximate hospital j's actual mortality rate for case-mix-adjusted patients?

This is the strongest test but requires careful case-mix adjustment and is most vulnerable to confounding.

### 4.5 Sensitivity to Stochastic Mortality Component

- Repeat the counterfactual analysis separately for progressive-failure cohort and full population.
- Hypothesis: counterfactual predictions are more accurate for the progressive-failure cohort.
- Links Chapter 1 to Chapter 4, demonstrating full pipeline coherence.

### Feasible Scope (within dissertation timeline)

**Prerequisites to proceed with Chapter 4:**

- Chapter 2 shows real, clinically interpretable policy divergence for at least 1–2 therapeutic dimensions.
- Chapter 3 yields stable, directionally coherent treatment-effect estimates for at least 1–2 major actions.
- The decomposed model is at least competitive with the black-box baseline on trajectory prediction; Chapter 4 should not be used to rescue a clearly broken Chapter 3 model.

**Must deliver if those prerequisites are met:**

- Leave-one-hospital-out reconstruction accuracy for at least 3 hospitals (4.1)
- Black-box comparison on reconstruction AND counterfactual tasks (4.2 — this is the central test)
- Focused counterfactual on 1–2 therapeutic dimensions with greatest divergence (4.3)
- Clinical coherence check (4.2 — qualitative)

**Can skip if needed:**

- Full 5-hospital leave-one-out (4.1 — 3 is sufficient)
- Outcome-level validation (4.4 — hardest to do rigorously)
- Sensitivity to stochastic component (4.5 — strengthens narrative but not essential)

**Estimated effort:** 4–6 weeks. Mostly computational once Chapters 2 and 3 are complete.

---

## Summary: Work Status and Dependency Map

| Component | Status | New Work Required | Dependencies | Feasible Effort |
| --- | --- | --- | --- | --- |
| Introduction (prior work framing) | Writing only | Reframe phase paper + anomaly results | None | 1–2 weeks |
| Ch 1: Progressive vs. stochastic mortality | ~20% done | Formalize, validate, classify | None (independent risk model) | 6–8 weeks |
| Ch 2: Treatment policy characterization | 0% (infra ready) | Full analysis | None (benefits from Ch1 but doesn't require it) | 4–6 weeks |
| Ch 3: Decomposed dynamics model | ~15% (LSTM infra) | Architecture, training, evaluation | Ch 2 (for learned H*) | 8–10 weeks |
| Ch 4: Cross-hospital counterfactual | 0% | Full analysis | Ch 2 + Ch 3 | 4–6 weeks |
| Discussion (reliability synthesis) | Conceptual | Synthesis + writing | All preceding | 2–3 weeks |

**Total new work (feasible scope): ~24–35 weeks of analysis + implementation.**
Remaining for writing and iteration: ~17–28 weeks within the 12-month window.

Writing happens in parallel with analysis — each chapter should be drafted within 2 weeks of completing its analysis, while results are fresh. Do not batch all writing to the end.

---

## Circularity and Methodological Safeguards

**Safeguard 1: Independence of mortality decomposition from dynamics model.**
The risk model in Chapter 1 must be methodologically distinct from the decomposed model in Chapter 3. Use a simple logistic regression or the XGBoost from prior work — never the dynamics model's own risk estimates.

**Safeguard 2: Treatment policy models must not leak outcomes.**
The policy models in Chapter 2 predict clinician actions from patient state. They must not include outcome variables (mortality, discharge) as features.

**Safeguard 3: Cross-hospital validation must use held-out data.**
In Chapter 4, the held-out hospital's data cannot appear in any part of model training — not for the dynamics model, not for hyperparameters, not for the stochastic mortality threshold.

**Safeguard 4: Chapter 1 classification must not use future information.**
The progressive/background classification must be applicable at any point during the stay using only past information. If it requires knowing the outcome, it cannot be applied prospectively.

**Safeguard 5: Chapter 1 claims must be bounded by measurement and time resolution.**
Any claim about "stochastic" or "sudden" mortality must be explicitly stated as conditional on the available feature set and temporal aggregation. Sensitivity to charting frequency or block size must be discussed, and ideally tested.

---

## Relationship to Original Proposal

| Proposal Agenda Item | Thesis Coverage | Gap |
| --- | --- | --- |
| 1. Learning scheme for coupled system-control model | Ch 3 (information-restricted two-pathway architecture) | Not a hybrid model in the mechanistic-equation sense. Structural decomposition via architecture, not via coupling ODEs with ML. |
| 2. Software framework (HybridML) | Not addressed | Significant gap. Discuss as future work. |
| 3. Reliability range assessment | Discussion synthesis + contributions from all chapters | Empirical, not theoretical. Formal extrapolation theory is future work. |
| 4. Sensitivity analysis of intervention strategies | Ch 4 (cross-hospital counterfactuals) + Ch 3 (treatment effect isolation) | Reframe as sensitivity analysis in the discussion. |
| 5. Demonstration on ARDS/pandemic data | Entire thesis on ventilated ICU population using ASIC + MIMIC-IV | Covered. |

**Honest assessment:** The thesis addresses the proposal's scientific question (decomposing the system-control interaction) but uses a different methodological approach (information-restricted architecture rather than mechanistic hybrid model). The departure should be discussed explicitly in the introduction: the approach was driven by what the data and problem structure require, not by methodological preference.

---

## Risk Assessment

### Risk 1: The decomposed model doesn't outperform the black box on the counterfactual task

**Probability:** Moderate. Black-box models with explicit treatment inputs can implicitly learn partial decompositions.

**Impact:** High. The thesis's central empirical claim fails.

**Mitigation:** Report honestly. If the decomposed model matches but doesn't beat the black box, pivot the argument to interpretability: the decomposed model provides treatment effect estimates that the black box cannot, even if both predict equally well. If the decomposed model performs *worse* than the black box, that's a strong negative result about the limitations of architectural decomposition — still publishable and informative.

**Decision gate:** Month 7 (October 2026). Proceed to Chapter 4 only if Chapter 2 shows genuine policy divergence and Chapter 3 shows stable, clinically coherent treatment-effect structure with at least competitive predictive performance. If not, activate fallback.

### Risk 2: The spontaneous death decomposition doesn't formalize cleanly

**Probability:** Moderate. The preliminary visual observation may not survive formal statistical testing.

**Impact:** Low-to-moderate. The thesis does not depend on Chapter 1. Chapters 2–4 proceed with the full population. Chapter 1 reports a negative or weak result and becomes shorter.

**Mitigation:** Built into the plan — Chapter 1's contribution is robust to its own outcome. "We tested whether ICU mortality has a stochastic component outside the control loop. The evidence is [strong/weak/absent]." All three answers are publishable.

### Risk 3: Treatment policies don't diverge meaningfully across hospitals

**Probability:** Low. Prior literature and clinical experience strongly suggest inter-hospital variation.

**Impact:** High. If hospitals all do the same thing, there's no natural experiment, and Chapter 4 has no discriminative power.

**Mitigation:** Early detection — Chapter 2 is completed before Chapter 3. If divergence is minimal, you learn this by month 4 and can pivot: either focus on within-hospital temporal variation in policies (less powerful but still usable) or restructure the validation approach.

---

## Fallback Plan

If Chapter 2 shows insufficient policy divergence, or if the decomposed model (Chapter 3) fails to produce clinically coherent treatment effect estimates, or if Chapter 4 (when attempted) shows no advantage over the black box by month 7:

**Fallback thesis: "Preconditions for Modeling Patient-Therapy Dynamics in the ICU"**

1. Progressive vs. stochastic mortality (Chapter 1) — characterizes the control loop's domain
2. Treatment policy characterization (Chapter 2) — characterizes the controller
3. Attempted decomposition + negative result analysis (shortened Chapter 3) — "here's what we tried, why it didn't work, and what it tells us about the problem's difficulty"
4. Expanded discussion: what would be needed to achieve the decomposition (data requirements, architectural alternatives, causal inference framework)

This is less ambitious but defensible. Chapters 1 and 2 stand as independent contributions. The negative result on the decomposition is informative if analyzed properly — it tells the field something about the limits of architectural approaches to causal decomposition in ICU data.

**Decision gate: Month 7 (October 2026).**