# ASIC--MIMIC clinical-variable alignment 0.1

## Outcome and status

Alignment contract `0.1` is implemented as a proposed, platform-level review
artifact. It accounts for all 184 field occurrences in ASIC core-derived `0.2`:
37 static and 147 dynamic. This includes all 162 frozen clinical dictionary
fields, ten table-specific operational provenance fields, and twelve
core-derived additions. No current MIMIC extraction profile, production
candidate, release, or pointer is changed.

The inventory dispositions are:

| Disposition | Field occurrences |
|---|---:|
| aligned | 35 |
| conditionally_aligned | 34 |
| unresolved_blocking | 60 |
| related_but_not_equivalent | 28 |
| no_mimic_equivalent | 5 |
| operational_or_nonclinical_not_in_scope | 22 |
| **Total** | **184** |

The long-form draft has 73 source-table/item/unit rows covering 55 ASIC
concepts: 36 `labevents`, 26 `chartevents`, and 11 direct-table mappings. Of
these, 48 are eligible at the catalog level, 19 remain human-gated, and six are
explicitly ineligible. Catalog eligibility does not mean extraction-profile
membership.

The complete human-readable field inventory is the
[generated alignment table](generated_alignment_table_0_1.md). The CSV is the
machine-readable authority.

## Authoritative lineage

- ASIC harmonized schema/dictionary contract: `0.2`, review run
  `20260807T101321Z`.
- ASIC core-derived contract: `0.2`; release `20260808T074305Z`.
- ASIC variable dictionary SHA-256:
  `98c61eaccf2f7669fc7c59f0315f65584d1dd6686e096a6938d495a17c99bdff`.
- Core-derived contract SHA-256:
  `bfc7599a5eebce483b03ef73d5dd013c41bc9d0dc17aca899d173a43555ef3a6`.
- MIMIC authority: original MIMIC-IV 3.1 tables and dictionaries, the official
  MIT-LCP MIMIC code definitions, and the current v3 MIMIC contracts.

The filename `candidate_unit_decisions_0_2.yaml` is not interpreted as an
unreviewed source. Its contents were audited into
`reviewed_unit_decisions_0_2.yaml` and frozen by the approved dictionary
contract. Its caveat remains material: ASIC units are reviewed team-derived
units because the hospitals did not supply a complete unit dictionary.

## Independent draft and historical regression evidence

The authoritative-input draft was created before opening the historical CSV.
Its combined SHA-256 is
`00bd2c753cffadececce089b6d34abe5c1831d61b2332d3327710d7f58f502ab`.
Only after that hash was recorded was legacy file SHA-256
`a116d58c09299e27670afd33b3989c4c5f7253987bc3ab44a9383b98d018d3e7`
read. The comparison made zero automatic catalog changes.

Agreements include heart rate, pressure, temperature, albumin, bilirubin,
creatinine, hemoglobin, hematocrit, core enzymes, platelets, WBC, and the
arterial analyte identities (while the new catalog adds specimen gates).
Material disagreements are retained for review:

- legacy `sao` conflates arterial saturation with pulse-oximetry SpO2;
- legacy fluid balance does not define a rolling window or missing-side rules;
- legacy `vt` says setting while the frozen ASIC `vt` definition is unresolved;
- legacy ICD scope combines ICD-9 and ICD-10 while the ASIC field is ICD-10;
- legacy BMI is continuous while ASIC core-derived `0.2` contains `bmi_group`;
- legacy BUN/urea treats a quantity change as ordinary unit alignment.

Legacy LOCF windows are recorded only as rejected regression evidence. The
interoperability layer performs no LOCF, imputation, encoding, scaling for
modeling, or time blocking.

## Canonical event boundary

For any enabled MIMIC event, the future canonical-event contract must retain
both representations:

| Source representation | Canonical representation |
|---|---|
| source table and source field | canonical concept ID |
| `d_items`, `d_labitems`, or not applicable | canonical value |
| item ID or not applicable | `shared_canonical_unit` |
| dictionary label, category, and `linksto` | conversion ID and conversion status |
| source value and exact source unit | plausibility status |
| method/specimen evidence | eligibility and rejection reasons |

The strict order is: preserve source evidence, match an explicit item/unit
mapping, apply the registered clinical conversion, then apply plausibility in
the shared canonical unit, then admit only eligible canonical events to 15-
minute or independently generated 8-hour blocking. Blocking cannot repair
units or semantics.

Unknown/missing units, unregistered spellings, contextual conversions without
their required fields, and unavailable conversions fail closed. No item label
or numeric data type is evidence of a unit.

## Confirmed, conditional, rejected, and unavailable overlap

Confirmed item-level overlap includes core vital signs, temperature (including
the affine Fahrenheit conversion), common chemistry, CBC, coagulation, and
selected enzymes. The exact source-unit spelling remains a runtime contract.

Conditional overlap includes:

- arterial lactate, PaO2, PaCO2, pH, base excess, bicarbonate, and SaO2, which
  require reviewed same-specimen semantics through specimen item `52033`;
- FiO2, because item `223835` can contain fraction or percent representations;
- PEEP and tidal-volume fields, because settings and measurements must remain
  distinct;
- troponin, because MIMIC is specifically troponin T while ASIC subtype is
  unspecified;
- height lists, BMI groups, readmission, ICU mortality, and ICD-10, because
  their aggregation/vocabulary contracts must be explicit.

Rejected or unavailable conversion overlap includes:

- MIMIC CK-MB mass concentration versus ASIC CK-MB enzyme activity;
- CRP mass concentration to ASIC molar concentration without an approved
  molecular entity/molar-mass basis;
- D-dimer without FEU/DDU assay evidence; and
- any medication dose/rate/concentration/exposure mapping without an exact item,
  unit, order/administration meaning, and weight context.

Related-but-not-equivalent evidence includes all SOFA/iSOFA variants, ASIC
ventilation-proxy summaries, ARDS application codes, free-day outcomes, and
hospital-scoped unresolved medication representations. These cannot be
coalesced.

## Unit conversion decisions

Registry `unit_conversions_0_1.yaml` contains 34 conversions. Executable
identity, scale, affine, and molar rules have examples and, where meaningful,
inverse/round-trip tests. Examples include:

- inch to cm: `cm = inch * 2.54`;
- Fahrenheit to Celsius: `C = (F - 32) * 5/9` (both scale and offset tested);
- albumin: `dg/L = g/dL * 100`;
- bilirubin: `umol/L = mg/dL * 17.104`;
- creatinine: `umol/L = mg/dL * 88.4`;
- hemoglobin: `mmol/L = g/dL / 1.611`, retaining the stated-equivalent-basis
  caveat;
- counts: `K/uL = 10e9/L` and `#/uL / 1000 = 10e9/L`.

The following are not approved for enablement: contextual FiO2, BUN-to-urea,
CRP molar conversion, CK-MB mass-to-activity, and D-dimer assay conversion.
INR and pH also require the actual MIMIC `valueuom` spellings to be confirmed;
missing units will not be accepted.

## Required aggregate production evidence

The bounded evidence command reads only dictionaries and the `itemid` plus unit
columns of relevant event tables. It emits private item/unit aggregates and a
public counts-only summary. Required evidence is:

1. presence and exact metadata for every proposed `d_items`/`d_labitems` ID;
2. counts for every observed source-unit spelling by table and item;
3. missing and unaccepted-unit counts;
4. current-profile versus catalog mapping counts;
5. same-specimen arterial/venous/capillary/unspecified counts for blood-gas
   items and specimen item `52033`;
6. candidate dictionary matches and unit/context counts for currently unmapped
   medication, hemodynamic, RRT, ECMO, score, and therapy concepts; and
7. amount/rate/concentration/exposure distinctions for medications.

Items 1--4 are implemented in the current report-only runner. Items 5--7 are
explicitly marked as follow-up evidence and must be implemented and
synthetically verified before those semantic gates can close. No decision is
inferred from absence in a label search.

## Validation and conservation invariants

- exactly 184 ASIC occurrences: 37 static and 147 dynamic;
- every occurrence has exactly one allowed disposition and immutable lineage;
- canonical concept IDs and mapping IDs are unique;
- no duplicate concept/table/source-field/item/unit mapping;
- `labevents` uses `d_labitems`; ICU event tables use `d_items`; direct tables
  use neither;
- every mapping has an explicit accepted source unit, including
  `not_applicable` for nonphysical values;
- every conversion is registered and its target equals the concept's
  `preferred_shared_canonical_unit`;
- unknown or missing units preserve raw evidence but produce no canonical value;
- conversion precedes canonical-unit plausibility;
- arterial blood-gas mappings cannot become enabled without reviewed specimen
  semantics;
- deterministic duplicates retain all source rows; blocking selects last
  non-missing only after stable time/source/item/source-row ordering;
- supported inverse conversions pass round trips; affine tests exercise scale
  and offset;
- runtime code imports no analysis repository; and
- public review outputs contain aggregate counts only, no identifiers, source
  values, filenames, private paths, or patient examples.

## Effect on the current MIMIC implementation

The existing canonical-event `0.1` already preserves source table/item/value/
unit and performs conversion before plausibility. It does not yet carry all
alignment fields. A separately reviewed additive canonical-event contract
should add:

- `dictionary_table`, `source_label`, `source_category`, `source_linksto`;
- `canonical_concept_id`, `shared_canonical_unit`, `conversion_status`; and
- explicit method/specimen evidence.

That migration should also split current broad audit names: item `223835` is a
FiO2 setting with a contextual representation; PEEP items are settings in the
official ventilator concept; and items `224684`, `224685`, and `224686` are set,
observed, and spontaneous tidal volume respectively. This contract does not
rewrite the successful candidate `20260808T200651Z` and does not expand the
phase-aware profile. Activation requires its own MIMIC canonical-event/profile
review and a new candidate.

Continuous observed BMI remains the approved MIMIC static representation. It
must be preserved separately from ASIC `bmi_group`; reproducing a category
requires reviewed ASIC category boundaries and cannot replace continuous BMI.

## Human decisions and evidence-dependent reviews

Decisions that can be made now:

1. approve the catalog structure, dispositions, and platform ownership;
2. approve the additive future canonical-event fields;
3. approve or reject the contextual FiO2 rule;
4. define ASIC `vt` as setting, observed measurement, or a separately named
   family and confirm the PEEP/FiO2 setting names;
5. approve or reject BUN-to-urea as an analysis-grade quantity conversion; and
6. define the accepted arterial specimen vocabulary and same-specimen rule.

Reviews that should wait for aggregate cluster evidence:

- exact dictionary labels/categories/`linksto` and source-unit spellings;
- ScvO2, IL-6, procalcitonin, BNP, ECMO/RRT, advanced-hemodynamic, score, and
  medication item candidates;
- D-dimer FEU/DDU availability;
- actual frequency of fraction-versus-percent FiO2; and
- the amount/rate/concentration/exposure context of medication items.

Rolling 24-hour fluid balance, acute versus chronic dialysis, and ICD-9 chronic
phenotypes remain separate reviewed contracts. They are not safe consequences
of this catalog.
