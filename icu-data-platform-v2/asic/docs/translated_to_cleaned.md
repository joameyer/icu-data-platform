# Translated to cleaned

**Implementation status:** stage boundary defined; transformation not implemented  
**Input contract:** [`asic/config/translated/contract.yaml`](../config/translated/contract.yaml)  
**Input reference:** [`translated_data.md`](translated_data.md)  
**Diagnostic policy:** [`asic/config/translated/data_quality_audit.yaml`](../config/translated/data_quality_audit.yaml)

This document defines the planned analysis-agnostic cleaning stage. It is
separate from translation, which changes representation, and from derivation,
which creates analysis-facing interpretations or temporal representations.

## Stage boundary

```text
asic/data/<context>/translated/ -> asic/data/<context>/cleaned/
```

The stage will accept only translated artifacts that pass the frozen
translated contract and manifest audit. It must never modify pooled or
translated artifacts.

The cleaned output must:

- retain every translated column;
- preserve static and dynamic row count and row order;
- preserve `stay_id_global`, `hospital_id`, `hospital_code_source`,
  `minutes_since_icu_admission`, and
  `anchored_time_since_icu_admission` exactly;
- change existing values only through explicitly approved, named cleaning
  rules; and
- record every changed or newly flagged value class in a cleaning manifest.

The cleaned stage must not filter rows or stays, select a cohort, perform
temporal blocking, impute missing values, consolidate mortality, construct
mechanical-ventilation episodes, or create analysis-specific features.

## Cleaning-rule categories

Rules may be proposed in four categories:

1. **Unit or representation standardization:** convert a confirmed source unit
   or scale to a documented canonical unit.
2. **Sentinel normalization:** replace confirmed missing-value encodings with
   nulls.
3. **Semantic correction:** apply a confirmed hospital/field-specific
   interpretation without merging unresolved definitions.
4. **Invalid-value and physiologic QC:** mask or flag values that violate a
   reviewed rule, while retaining enough evidence to audit the decision.

An approved cleaning rule may recover an isolated scale-entry error within a
hospital only when the raw value is invalid, an explicit factor and direction
produce a value inside a strong canonical physiologic window, and competing
interpretations have been excluded. The initial read-only audit tests arterial
pH values outside `[6.8, 7.8]` for recovery by division by 100. It does not yet
change them. This mechanism must not be generalized to broad or long-tailed
variables without a separate reviewed rule.

The general read-only discovery screen tests every dynamic variable with both
legacy hard bounds using multiplication and division by 10, 100, and 1000. It
separates uniquely recoverable, ambiguous, and unrecoverable outliers and
describes their frequency by hospital. Discovery output is evidence only: a
field does not become eligible for cleaning until its physiologic window,
factor, direction, ambiguity behavior, and provenance are approved as an
explicit field-level rule.

For the review-selected bounded fields `core_temp`, `evlwi`, `ph_art`, `fio2`,
`map`, `spo2`, `sao2`, and `scvo2`, the audit additionally compares uniquely
recoverable candidates with nearest valid same-stay measurements inside 1-,
4-, and 8-hour windows. Where appropriate, it also compares same-row FiO2 with
`fio2_set`, MAP with the SBP/DBP-derived approximation, and SpO2 with SaO2.
UK03 FiO2 is excluded from this row-level context step because it is a
systematic site-scale hypothesis. ScvO2 has temporal context only: its separate
comparisons with SaO2 and SpO2 do not assume either field is equivalent.
Temporal or related-field agreement is supporting evidence only; it does not
approve recovery, establish a unit, or make the related fields equivalent.
Temporal output therefore says only whether the recovered value is closer to
available neighbors. For MAP, a recovered value outside the simultaneous
DBP–SBP interval is contradictory evidence regardless of its distance from the
derived MAP. Current SaO2/SpO2 scale candidates are eligible for invalid-value
masking only, not automatic recovery; a future recovery rule would require
separate production evidence and explicit approval.

Every field/hospital scope with uniquely recoverable discovery candidates must
be assigned to neighboring-measurement context, a targeted scale audit, or an
explicit row-level scale-entry audit. An uncovered scope is a blocking audit
failure, so production-only candidates cannot bypass review.

If the pH rule is approved after demo and production review, the cleaning
implementation will replace only qualifying cleaned `ph_art` values with
`translated.ph_art / 100`, add a dedicated Boolean correction flag, and record
the exact corrected count by hospital in `cleaning_manifest.json`. Values that
do not satisfy both sides of the rule are not scale-corrected. The immutable
translated artifact remains the source for the original numeric value. The
flag's final column name will be frozen with the cleaned schema rather than
introduced by the read-only audit.

Every executable rule must specify:

- a stable rule ID and clinical description;
- input field and hospital scope;
- source and target unit or meaning;
- exact condition and action;
- behavior at boundaries and for missing values;
- whether the original value is masked, converted, or only flagged;
- demo and production evidence used for approval; and
- affected row/value counts written to `cleaning_manifest.json`.

Distribution alignment alone is not proof of a common unit. No empirical
multiplier may become a cleaning rule without a defensible unit or semantic
interpretation and explicit human approval.

## Required review sequence

For each bounded cleaning rule:

1. preserve the evidence in the read-only audit policy;
2. review the complete demo report;
3. run and review the read-only production report on a compute node;
4. approve or reject the rule explicitly;
5. implement the approved rule with focused tests;
6. build and review cleaned demo output and its manifest; and
7. only then consider a production cleaned build.

No cleaning rule is approved merely because a hypothetical transformed
distribution resembles peers.

## Current evidence and unresolved issues

The read-only audit preserves the complete recovered legacy invalid-value,
sentinel, unit, and semantic findings and adds focused cross-hospital tests.
The current evidence, exact audit outputs, and review order are documented in
[`cross_hospital_data_quality.md`](cross_hospital_data_quality.md).

Important unresolved areas include:

- ScvO2 meaning at hospitals whose values resemble SaO2 or SpO2;
- UK08 urea units;
- empirical albumin and D-dimer scale alignment without confirmed units;
- UK06 tidal-volume-per-kilogram fields: the legacy pipeline classified
  `vt_per_kg_ideal_body_weight` as unrecoverable mixed-scale and masked the
  whole site/field pair; its code did not implement a `/1000` recovery. The new
  `vt_per_kg` field is identical to it on all both-present UK06 production rows,
  so both remain unresolved and the v2 thresholded conversion remains
  diagnostic only;
- UK00 `vt_per_ideal_bw_total`: pooled-to-translated policy `1.3` losslessly
  moves UK00 values out of the misleading per-kilogram field into this separate
  absolute-volume-like field. Production evidence shows that nearly all values
  are close to `6 * PBW`, but the exact upstream reported-versus-derived
  provenance is still unknown. Policy `1.10` continues the row-level PBW and
  `vt` relationship audits; no recalculation or further cleaning is approved;
- I:E versus E:I direction;
- medication dose units and normalization; and
- SOFA/iSOFA provenance and definition differences.

`delta_p_computed` and `delta_p_reported` remain separate. SOFA and iSOFA
fields also remain separate unless a later reviewed rule establishes their
provenance and equivalence.

## Planned output evidence

Every cleaned artifact will contain:

```text
cleaned/
├── static.parquet
├── dynamic.parquet
└── cleaning_manifest.json
```

The manifest must record input contract and artifact identity, cleaning-policy
and generator versions, row/schema preservation checks, rule-by-rule affected
counts, hospital scopes, unit/meaning changes, QC flag counts, and final
publication status. Until that contract and implementation exist, no
`cleaned/` artifact produced by this repository is supported.
