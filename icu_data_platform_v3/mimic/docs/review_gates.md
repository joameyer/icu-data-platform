# Phases and human gates

## Chronological phases

1. **Contract/local synthetic milestone (complete):** profile, selected-source
   registry, cohort/event/block contracts, pure computation functions, and
   synthetic tests. No production data or artifact is created.
2. **Clinical semantic freeze:** resolve all four gates below; update contracts
   and tests; independently review the complete selected-variable mapping.
3. **Production adapter authorization (complete):** bounded Arrow readers,
   immutable manifests, private audit reports, and operator commands are
   implemented and locally verified.
4. **Cluster aggregate evidence (current):** operator runs a
   run-scoped candidate; review aggregate cohort, conversion, conservation,
   performance, and legacy-comparison evidence.
5. **Release gate:** separately approve an immutable canonical/static release,
   then independently generated 15-minute and 8-hour releases.
6. **Analysis handoff:** pin the approved 8-hour manifest in the analysis
   repository and validate its frozen interface. Splitting, preprocessing, and
   modeling remain downstream.

## Open decisions—do not infer

- **Rolling 24-hour fluid balance:** exact input/output event families,
  amount/rate integration, sign, cancelled/revised rows, boundary notation,
  and whether the value is observation-time rolling or evaluated at a grid.
- **Arterial specimen semantics:** the exact specimen evidence and fallback
  policy for lactate, PaO2, PaCO2, and pH; do not infer arterial status from the
  analyte item ID alone.
- **BUN to urea:** approve source-unit eligibility and the exact divisor
  (`2.801` is implemented for testing but disabled) plus handling of conflicting
  or missing units.
- **ICD-9 chronic phenotypes:** approve mappings and coexistence semantics. A
  negative ICD-10 flag is currently unknown when ICD-9 codes are also present.

Resolved decisions: official `icustays.outtime`; ICU-service attribution as the
latest non-future service; ASIC-equivalent plausibility ranges after unit
harmonization; continuous observed-only BMI from median valid ICU height and
weight; common right-labelled convention at both resolutions; fixed 72-hour
prehistory; and an in-platform `mimic/` sibling rather than a new repository.
