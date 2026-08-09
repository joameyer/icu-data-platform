# Harmonized and cleaned data contracts

## Decision and scope

The first durable dataset after lossless ingestion will be the **harmonized**
layer, not a separately published translated layer. Translation remains a
named, auditable operation inside harmonization. The next durable layer is
**cleaned**, followed by analysis-specific derived data.

This architecture was approved on 2026-08-05. It defines stage boundaries only;
it does not approve parsing rules, variable mappings, units, cleaning ranges,
or production generation. Those decisions require the raw-schema and token
inventories and later human review gates.

## Stage invariants

### Lossless ingestion

The ingested layer preserves raw strings, literal empty cells, positional raw
columns, source identifiers, and deterministic source order. It contains no
clinical interpretation. It remains the immutable source for re-auditing every
later transformation.

### Harmonized

Within one harmonized variable, a value has the same clinical meaning, data
type, unit, missing-value representation, and categorical vocabulary regardless
of source hospital. Harmonization may apply only deterministic, approved rules
based on source provenance, source schema, documented hospital convention, or
unambiguous raw-token syntax.

Harmonization includes:

- raw-to-canonical column naming and approved alias handling;
- categorical-value translation into a common vocabulary;
- approved hospital/variable-specific missing sentinels converted to null;
- direct numeric parsing and unambiguous custom parsing, including decimal
  commas and ratio strings such as `1/2` or `1:2`;
- documented unit conversion, such as percentage to fraction, when the source
  convention and target unit are established;
- documented notation or semantic direction corrections, including I:E versus
  E:I only after direction is established; and
- deterministic hospital-, schema-, or export-version-specific corrections.

Token frequency does not determine the stage. If one hospital contains both
`0.5` and `1/2`, and both representations are unambiguous under an approved
policy, both are harmonized to the same numeric value with distinct parse
statuses. An unresolved or ambiguous non-empty token blocks the harmonized
artifact until reviewed.

Cross-hospital distributions may be required to discover and approve a unit or
semantic discrepancy. After approval, a deterministic hospital- or
schema-scoped conversion is still a harmonization rule. Internal candidate
views and aggregate audits may support that decision without becoming a
published translated dataset.

### Cleaned

The cleaned layer starts from the frozen harmonized contract. It addresses
value-level quality problems whose treatment depends on physiological
plausibility, local context, cross-variable consistency, or empirical
distributions rather than a documented source-system convention.

Cleaning includes:

- masking physiologically impossible values to null under a reviewed policy;
- detecting isolated mixed-scale or transcription errors;
- applying narrowly approved row-level corrections such as division or
  multiplication by `10`, `100`, or `1000` when evidence is sufficient;
- resolving cross-variable contradictions or duplicate observations under
  explicit rules; and
- recording unresolved suspicious values without forcing a correction.

A value should not be rescaled merely because a multiplier makes it plausible.
Every automatic correction requires a rule with an unambiguous trigger,
variable and hospital scope, before/after accounting, and validation against
cross-hospital evidence. The harmonized value remains recoverable, and the
cleaned output records the cleaning status and rule ID. Cleaning masks values;
it does not silently delete source rows or stays. Cohort selection remains a
derived-stage concern.

## Provenance and accounting

Every expected numeric non-empty raw token must satisfy:

```text
raw non-empty
= direct numeric
+ custom parsed
+ approved textual missing
+ unresolved
```

Every harmonization and cleaning rule must have a stable rule ID, version,
scope, rationale, evidence reference, and rule-level counts. Hospital/source
schema and raw column occurrence remain traceable to the canonical variable.
The lossless ingested artifact is never overwritten.

## Variable dictionary and policies

The harmonized contract includes a machine-readable variable dictionary from
which human-readable documentation can be generated. For every canonical
variable it records at least:

- canonical name, definition, data type, and expected unit;
- categorical domain or numeric representation;
- source raw names and occurrences by hospital and schema;
- parsing, missing-sentinel, unit, notation, and alias rule IDs;
- hospital availability and known semantic caveats; and
- contract version and review status.

Physiological validity ranges, scale-correction triggers, and other adjustable
quality decisions belong to a separate versioned cleaning policy linked from
the dictionary. Changing a cleaning threshold must not silently redefine the
harmonized variable.

### Repeated height measurements

Raw-v3 review found that UK00 static height cells contain one or more
approximate measurements for the same stay. Harmonization therefore represents
the canonical `height_measurements_cm` as an Arrow/Parquet `list<double>`:

- numeric element order and duplicates are preserved;
- the approved source `nan` element becomes a null list element;
- no mean, median, first-value selection, or other scalar reduction is applied;
- an unknown or malformed element blocks harmonization; and
- a reviewed scalar source for the same concept may later become a singleton
  list to satisfy the union type.

Cleaning retains the list type and masks physiologically invalid elements to
null under a separately reviewed range policy. Scalar summaries and a selected
height for predicted-body-weight calculations belong to the derived layer. The
exact ingested raw string remains recoverable from the immutable ingested layer.

## All-missing columns

No base-layer column is dropped automatically because it is all missing:

- a column unavailable in one hospital remains in the approved union schema if
  another hospital supplies it;
- a globally all-missing contract variable remains in harmonized and cleaned
  base artifacts and is marked `no_observed_values` and
  `candidate_for_retirement` in the dictionary; and
- physical removal requires an explicit, versioned schema decision with an
  availability report and migration note.

An optional downstream analysis projection may omit globally empty columns for
convenience. That projection is not the canonical harmonized or cleaned
contract.

## Approved stage sequence

```text
raw hospital CSV
  -> lossless per-hospital ingestion
  -> read-only raw schema and parsing-token inventories
  -> reviewed parsing and harmonization registry
  -> hospital-aware harmonization candidates
  -> cross-hospital semantic/unit audit
  -> approved ordered union schema and validated concatenation
  -> pooled harmonized static and dynamic artifacts + variable dictionary
  -> versioned cleaning policy and pooled cleaning
  -> cleaned artifacts
  -> derived datasets and optional time blocking
```

The term `pooled` continues to mean the result of validated concatenation. The
old externally pooled v2 inputs remain untrusted comparison artifacts only.

## Fail-closed bootstrap implementation

The first harmonization implementation slice is intentionally synthetic-only.
`asic/config/harmonization/bootstrap.yaml` loads only active
`human_approved_raw_v3_rule` entries and hard-codes all production, artifact,
concatenation, union-schema, and dictionary gates closed. It contains no output
path and has no CLI or Slurm entry point.

The bootstrap can transform one bounded Arrow raw-string column batch under one
reviewed rule. It supports:

- exact categorical-domain validation;
- direct numeric and scoped decimal-comma parsing;
- scoped numeric missing sentinels;
- exact literal-empty versus source-schema-absent statuses; and
- the repeated-height `list<double>` parser with null-element preservation.

Every output value has a parse status, and every rule reports cell-level and,
where applicable, list-element accounting. An unresolved token yields null plus
an unresolved status and blocks the batch when `require_resolved()` is called;
the exception never includes the protected token. The bootstrap does not yet
rename or merge whole tables, derive identifiers, approve all mappings, write a
manifest, or produce a harmonized artifact.

Evidence-bound partial static decisions are stored in immutable versions.
Registry `reviewed_static_decisions_0_1.yaml` records the initial UK00
`hospital_mortality_reported` nullable Boolean decision. Registry `0.2`
supersedes it as the current partial snapshot and adds:

- UK00 `weight_kg` as `float64` kg using direct and decimal-comma numeric
  grammars;
- UK00 `hosp_los` as `float64` days with exact token `nan` approved as missing
  and all other non-empty tokens blocked because no numeric value was observed;
  and
- preservation of raw `Pseudo-ID`/`PseudoID` as `stay_id_local`, with
  `stay_id_global` derived as `<local>:<numeric hospital suffix>`.

Identifier derivation blocks missing, empty, whitespace-altered,
separator-ambiguous, unknown-hospital, and duplicate-within-hospital static
IDs. The 3,676 audited local IDs shared across hospitals remain distinct after
the hospital suffix is applied. These rules remain synthetic-only and
fail-closed for production; they will be incorporated into the future complete
reviewed registry without rewriting immutable candidate-review evidence.
Reported hospital mortality remains separate from any later derived mortality
consolidation.

Partial registry `0.3` extends `0.2` with hospital-scoped `icu_los`,
`dialysis_free_days`, and `vent_free_days` rules, all represented as nullable
`float64` days. Direct numerics are accepted only where observed and approved.
UK08’s reviewed `-1`/`-1.0` free-day sentinels become null; because UK08
contains no observed direct values for those variables, a newly appearing
numeric value remains blocking. UK01’s unavailable sources likewise block any
new non-empty value pending review.

Reviewed ICD contract `0.1` approves nullable `large_string`
`icd10_codes_source_text`: preserve every non-missing source string exactly,
while literal empty/source absence and the reviewed UK03 token `nan` become
null. Final contract `0.2` represents `icd10_codes` as an ordered unique
`list<large_string>` split on commas with component-edge whitespace removed.
Single codes become one-element lists; case, punctuation, and first-occurrence
order are preserved. Reviewed UK03/UK08 trailing delimiter artifacts are
removed, exact duplicates are removed after the first occurrence, and the one
reviewed incomplete component is removed only through owner-only evidence
binding. Every unreviewed non-code or empty-component pattern blocks. No
ICD-10 diagnosis-semantic validation is part of harmonization.

The contract's component invariant is:

```text
466,002 source components
= 369,470 retained unique valid codes
+ 96,505 duplicate occurrences removed
+ 26 reviewed trailing empty components removed
+ 1 privately reviewed incomplete component removed
```

The Arrow parser returns exact nullable source text, the canonical list, a
row-level parse status, and cell/component accounting. It can execute only on
explicitly supplied bounded batches. A separate full-evidence validator
requires all production counts and hospital-scoped trailing counts to match
before a future writer may proceed. There is currently no production parser
CLI, Slurm job, table writer, concatenation, or schema-freeze entry point.

## Complete candidate-registry review

`review-harmonization-registry` is a report-only bridge between the schema/token
inventory and the future reviewed executable registry. It reads the immutable
policy/artifact `0.3` private bundle for a named run; it never opens ingested
Parquet. Every occurrence receives a private review ID, explicit disposition,
and candidate type/unit/parser/categorical/alias status. Retained occurrences
are grouped into candidate canonical variables.

The candidate contract distinguishes `human_approved_raw_v3`,
`candidate_requires_raw_v3_review`, and explicit unresolved statuses. It never
turns a frozen-v2 decision, source label, or distribution alignment into a v3
approval. The report binds itself to hashes of its immutable evidence inputs.

Registry-review artifact `0.3` separates cross-hospital source-name variants
from true alias/coalescence candidates. Multiple spellings across hospitals
remain mapping evidence; they do not require a merge rule. Alias review is
required only when multiple retained source occurrences within one hospital
and table map to the same target.

Candidate contract `0.3` explicitly surfaces the preserved UK00
`vt_per_ideal_bw_total` semantic split as a review item even though the global
seed mapping still points to the per-kilogram target. This prevents a zero count
of already-divergent targets from being misread as evidence that no semantic
split requires review.

Exact raw headers remain only in private Parquet workbooks. The sanitized report
contains aggregate review counts and must report zero technical blockers before
clinical review. Full operating and privacy details are documented in
`harmonization_registry_review.md`.

The first bounded follow-up is `audit-static-contract`. It reads only eight
selected static candidates, revalidates every ingested static hash and source
binding, and produces owner-only full-scan evidence for mapping, type, unit,
numeric or binary parsing, identifier, and ICD-10 preservation decisions. It is
an evidence stage only: an expected review failure with zero technical blockers
still requires an explicit human decision before any registry status changes.
The operating contract is documented in `static_contract_audit.md`.

The partial raw-v3 contract also approves `icu_readmit` as nullable Boolean:
raw `0` and `0.0` map to false, while `1` and `1.0` map to true. Literal empty
cells and source-schema absence remain missing. It approves `float64` as the
physical representation for `time_since_study_start` with unit `day`. The
associated `study_implementation_phase` remains categorical and maps code 0 to
`calibration`, 1 to `roll_in`, and 2 to `app_implementation`; integer-equivalent
decimal spellings have the same meanings.

The UK00-only `therapy_read_confirmation_utc` candidate is a reviewed
fixed-size list of two nullable Booleans in physical source-column order. Both
source values are preserved, including disagreements. Harmonization and
cleaning must not apply the former v2 binary-OR rule; scalar interpretations
belong only in a later derived or analysis-specific layer.

## Reviewed-registry contract and synthetic table planning

The next result-independent implementation slice defines a strict future
complete-registry format and validates it against the private `R####`
occurrence workbook. Coverage must be exactly one-to-one, all-missing drops must
retain a zero-non-empty precondition, and each canonical target must have one
consistent kind, value type, and unit. Same-target source collisions require an
explicit shared alias group and merge policy.

The table planner examines only synthetic Arrow schemas. Raw clinical fields
must remain strings with lossless raw-name and positional metadata. It rejects
unknown, missing, duplicate, or non-string fields and emits only a candidate
schema marked as not union-approved. It cannot execute transformations. The
complete contract is documented in `harmonization_registry_contract.md`.

## Historical ingestion gate labels

Ingestion manifest `0.2` was frozen before this architecture decision and uses
the strings `raw_to_translated_column_registry_not_reviewed`,
`translated_union_schema_not_approved`, and
`pending_translation_registry_review`. They remain unchanged as immutable
provenance. For downstream interpretation, those labels refer to the now
approved harmonization-registry and harmonized-union-schema gates. Existing
ingestion artifacts do not require regeneration or mutation.
