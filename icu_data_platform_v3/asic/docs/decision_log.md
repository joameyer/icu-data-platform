# Decision log

## 2026-08-06 — Absent migrated range rules receive explicit evidence

- Evidence: consolidated audit `20260806T091317Z` evaluated 46 of 47 migrated
  invalid-range variables. The sole missing variable was `delta_p_computed`,
  which is absent from the complete candidate variable dictionary.
- Classification: this is an audit-accounting omission, not an unresolved raw
  token or a missing patient-level inspection. Harmonization must not
  manufacture a computed field when no source-backed candidate field exists.
- Correction: retain the migrated rule and write an explicit
  `candidate_variable_unavailable_in_v3_union_schema` row. Fields present in a
  union schema but skipped because of an incompatible physical type continue
  to fail technical accounting.
- Boundary: `delta_p_reported` remains separate. Any future computed driving-
  pressure definition belongs to a separately reviewed derived-stage contract;
  this correction transforms no value and changes no candidate artifact.

## 2026-08-06 — V3 made runtime-independent of older project generations

- Status: approved and implemented for the consolidated harmonization audit.
- Finding: the first consolidated-audit policy required a quality-policy file
  and comparison Parquet files from the frozen v2 cluster deployment. That made
  v3 execution depend on retaining an older project generation.
- Decision: selectively migrate the quality-rule knowledge into the v3-owned
  `cross_hospital_quality_audit.yaml`; revalidate those rules against the raw-
  derived v3 candidate; disable older-version comparison by default; and treat
  any future comparison as optional, untrusted, and non-authoritative.
- Runtime contract: loss or deletion of v2 or legacy repositories and cluster
  deployments must not block v3 inventory, ingestion, harmonization audit,
  cleaning, derivation, or publication. Historical provenance may name an
  older source, but no runtime file from that source is required.
- Candidate preservation: completed candidate run `20260806T074852Z` remains
  immutable and does not need to be regenerated for this report-only policy
  correction.

## 2026-08-04 — V3 plan and Phase 1 implementation scope

- Status: approved for local Phase 1 implementation.
- Decision: create the isolated snake-case project `icu_data_platform_v3` and
  implement read-only inventory/reporting only.
- Authority: raw hospital CSVs will replace externally pooled Parquet as the v3
  authoritative boundary after the relevant gates are approved.
- Frozen references: neither legacy nor v2 may be modified.
- Operational constraint: no production cluster access, deployment, or report
  generation was included in this approval.

## Open decision — folder 01 hospital/cohort identity

- Status: blocking; not approved.
- Needed evidence: file schemas and identifier conventions; static/dynamic stay
  consistency; overlap with old pooled stays and pooled `hid` values; duplication
  or complementarity with other raw folders; export/cohort provenance; and
  whether legacy `UK_01` decimal-comma policies apply.
- Prohibited inference: folder `01` must not be silently excluded, mapped to
  `asic_UK01`, or added to a frozen contract without explicit evidence and
  approval.

When evidence is reviewed, append a new dated entry. Do not rewrite this open
decision in place; preserve the chronology and identify the reviewers.

## 2026-08-04 — Private inventory serialization correction

- Status: corrected; production inventory must be rerun under a new immutable
  run ID.
- Finding: artifact version `0.1` built correct in-memory sanitized aggregates,
  but the private `files.parquet` schema was inferred from the first file row.
  Optional CSV, encoding, delimiter, and identifier evidence columns were
  consequently absent when that first row did not contain them.
- Decision: artifact version `0.2` constructs an explicit union of all evidence
  keys before writing Parquet. A regression test reads the written artifact and
  requires the optional evidence columns and populated complete-file records.
- Preservation: the `0.1` run remains immutable as evidence of the first run;
  it must not be overwritten or treated as complete private inventory evidence.

## 2026-08-04 — Raw-anomaly diagnostic extension approved

- Status: approved for read-only implementation and cluster execution.
- Evidence: all 3,676 UK00 dynamic files are rectangular but contain exactly two
  positional `ARDS_Diagnose_App` headers, across two layouts. The unclassified
  UK00 ZIP has 3,678 entries and 3,677 basename overlaps with flat sources, but
  only 16 overlapping byte sizes match.
- Decision: revalidate v2's all-missing duplicate-drop precondition directly
  from raw literal strings, and compare ZIP members with flat files as bytes and
  decoded literal CSV cell streams without extraction.
- Restrictions: neither duplicate occurrence may be dropped or merged; the ZIP
  may be neither ingested nor excluded; and folder `01` remains unmapped until
  the resulting evidence and external provenance are explicitly approved.

## 2026-08-04 — ARDS duplicate resolved; ZIP provisionally excluded

- ARDS decision: approved. Across 3,676 UK00 files and 7,267,221 raw records,
  the second positional `ARDS_Diagnose_App` occurrence was non-empty zero times.
  Both occurrences must be preserved separately at lossless ingestion. The
  second may be omitted only at translation after the all-empty invariant is
  revalidated and recorded.
- ZIP decision: provisionally treat the reviewed archive as an old snapshot and
  exclude it from ingestion while retaining it read-only for comparison. The
  exclusion is scoped by hospital folder, exact byte size, and SHA-256 rather
  than its protected filename.
- Pending: data-owner confirmation may take several days. Until received, the
  archive remains a publication blocker and its provisional classification must
  not be promoted to a final authoritative decision.
- Folder `01`: unchanged and still blocking pending explicit identity/cohort
  provenance.

## 2026-08-04 — Folder 01 mapped and included

- Status: approved.
- Human decision: raw folder `01` represents canonical hospital `asic_UK01`
  and must be included in v3 ingestion.
- Contract effect: the complete approved mapping is `00`, `01`, `02`, `03`,
  `04`, `06`, `07`, and `08` to their correspondingly zero-padded
  `asic_UKNN` identifiers, all with cohort action `include`.
- Provenance: this decision supersedes the earlier open folder-identity gate
  without rewriting it; the chronology above is retained. The omission of
  folder `01` from the old pooled contract is historical comparison evidence,
  not a v3 exclusion rule.
- Boundary: this approval resolves identity and cohort inclusion only. Legacy
  UK01 decimal-comma logic and all hospital-specific parsing, translation, and
  schema decisions still require raw-token evidence and later review.
- Remaining Phase 1 blocker: the reviewed UK00 ZIP stays provisionally excluded
  from ingestion until its owner confirms the legacy-snapshot role.

## 2026-08-04 — Lossless ingestion implementation approved

- Status: approved for implementation and synthetic verification; production
  execution remains a separate per-hospital checkpoint.
- ZIP rule: the exact reviewed archive remains provisionally classified as an
  old snapshot. Ingestion must re-hash it, exclude it from CSV selection, carry
  the owner-confirmation publication blocker, and never treat it as
  authoritative input.
- Scope: one canonical hospital per command, consuming immutable inventory
  artifact `0.4`; atomic `static.parquet`, streamed `dynamic.parquet`, and an
  owner-only ingestion manifest.
- Lossless contract: present cells remain strings, literal empty cells remain
  `""`, and Parquet null represents only a column occurrence absent from a
  source schema. Raw names and duplicate positional occurrences are preserved.
- Provenance: source folder, filename, file ID, file/row/source order,
  filename-derived ID, in-file ID, and schema variant are retained separately.
- Exclusions: no pooled input, checkpoint content, duplicate static
  concatenation, archive member, parsing, translation, cleaning, cohort filter,
  or derived logic is permitted.

## 2026-08-04 — Duplicate ARDS physical names disambiguated

- Status: approved for implementation and UK00 regeneration.
- Finding: ingestion manifest `0.1` preserved both positional
  `ARDS_Diagnose_App` occurrences using duplicate physical Parquet names. The
  low-level `ParquetFile` API could read the file, but the PyArrow dataset API
  used by `pandas.read_parquet()` failed with an ambiguous-field error.
- Decision: ingestion policy and manifest `0.2` store the first raw occurrence
  as `ARDS_Diagnose_App_col1` and the second as
  `ARDS_Diagnose_App_col2`, following original left-to-right position.
- Provenance: both fields retain raw name `ARDS_Diagnose_App`, one-based
  occurrence, and the approved naming-rule ID in Arrow metadata and the
  ingestion manifest. The two fields are positional source occurrences, not
  distinct clinical concepts.
- Clinical scope: no interpretation is required for ingestion. Both value
  columns remain losslessly preserved. Translation may later omit `col2` only
  under the separately approved all-empty precondition.
- Artifact handling: the existing UK00 manifest `0.1` directory must be moved
  intact to the v3 quarantine tree before regeneration. It must not be deleted
  or overwritten.

## 2026-08-04 — Sequential all-hospital ingestion job approved

- Status: approved for implementation and production submission after the
  ingestion `0.2` deployment and UK00 `0.1` quarantine.
- Scope: one Slurm job ingests `asic_UK00`, `asic_UK01`, `asic_UK02`,
  `asic_UK03`, `asic_UK04`, `asic_UK06`, `asic_UK07`, and `asic_UK08` in that
  order using inventory run `0.4` supplied at submission.
- Safety: submission and compute phases validate the dedicated v3 roots,
  private inventory evidence, environment, and config. A full preflight refuses
  the batch if any hospital output or incomplete staging directory exists.
- Execution: hospitals run sequentially and fail-fast. No hospital is skipped,
  resumed, or overwritten automatically.
- Recovery: an interrupted or failed batch may leave completed immutable
  hospital outputs. Those artifacts must remain in place for human review; a
  new recovery decision is required before moving them or resubmitting.
- Publication: successful ingestion of all hospitals still leaves every
  artifact non-publishable pending archive confirmation and downstream parsing,
  translation, and union-schema gates.

## 2026-08-05 — All-hospital ingestion completed; read-only audit approved

- Cluster evidence: the user reported successful completion of the sequential
  all-hospital ingestion job for all eight approved hospitals. Production files
  remain on the authorized cluster; this repository contains code and synthetic
  tests only.
- Status: post-ingestion audit approved for implementation and cluster
  execution before any parsing work.
- Scope: validate the complete hospital set, manifest and policy `0.2`, linkage
  to inventory `20260804T193415Z`, source/file/row conservation, output hashes,
  standard-reader compatibility, raw string schemas, deterministic provenance,
  identifier agreement, the reviewed UK00 two-field storage rule, and
  fail-closed publication state.
- Memory and privacy: hash files as streams and read only the ten provenance
  columns in bounded batches. Keep exact filenames, identifiers, raw tokens,
  headers, paths, and failure locations in owner-only cluster evidence; expose
  aggregates only in the sanitized review report.
- ZIP status: continue excluding the exact reviewed archive. Owner confirmation
  remains a publication blocker, so a technically sound production audit is
  expected to fail only that review gate while confirmation is pending.
- Boundary: this approval does not authorize numeric parsing, translation,
  union-schema concatenation, pooled outputs, cleaning, or derivation. The next
  candidate phase is a separately reviewed read-only raw-schema and parsing-token
  inventory.

## 2026-08-05 — Post-ingestion audit NumPy dependency removed

- Finding: the first deployed audit implementation used
  `Array.to_numpy()` only to locate transitions in the integer source-file-order
  provenance field. The dedicated cluster environment does not include NumPy,
  so two synthetic audit tests failed before any production audit was
  submitted.
- Impact: no ingested Parquet file, manifest, inventory evidence, or production
  report was changed. The failure was confined to the read-only audit code.
- Correction: use PyArrow run-end encoding to identify constant file-order runs
  inside each bounded batch. This preserves the same order and conservation
  checks without adding a dependency or materializing clinical values.
- Verification: the test suite now explicitly rejects `import numpy` and
  `.to_numpy()` in the ingestion-audit implementation.

## 2026-08-05 — Production ingestion audit reviewed

- Evidence: sanitized ingestion-audit artifact `0.1`, run
  `20260805T060046Z`, linked to inventory `20260804T193415Z`.
- Coverage: all 8 approved hospitals and 16 static/dynamic Parquet artifacts;
  16,054 static rows and 24,069,379 dynamic rows were conserved in Parquet
  metadata and streamed provenance.
- Result: all 13 technical checks passed and every output SHA-256 matched its
  ingestion manifest. No additional ingestion or identifier blocker was
  reported.
- Governance state: overall status remains `FAIL` solely for
  `provisional_archive_owner_confirmation`. The exact reviewed ZIP remains
  excluded, read-only, and non-authoritative while owner confirmation is
  pending.
- Decision: ingestion artifacts require no regeneration based on this report.
  They remain non-publishable and no downstream data-layer generation is
  authorized by the audit alone.

## 2026-08-05 — Harmonized layer replaces durable translated layer

- Status: architecture recommendation explicitly approved by the data owner.
- Decision: after lossless ingestion and reviewed token/schema evidence, the
  first durable user-facing dataset is `harmonized`, followed by `cleaned` and
  then `derived`. No separate translated dataset is published.
- Harmonization boundary: canonical names, alias handling, categorical
  vocabularies, missing sentinels, deterministic parsing, expected units,
  notation semantics, and documented hospital/schema/export-specific
  conversions. Translation remains a rule type with provenance inside this
  stage.
- Cleaning boundary: physiologic invalid-value masking and row-level corrections
  whose justification depends on plausibility, context, or distributions rather
  than a documented source convention. Automatic scale correction requires an
  unambiguous reviewed trigger; original harmonized values remain recoverable.
- I:E rule: whether notation appears consistently or only in some rows does not
  determine the stage. Unambiguous `1/2`, `1:2`, and direct numeric tokens are
  harmonized under approved parsing/semantic policies; ambiguous direction or
  unresolved tokens block.
- Dictionary: the harmonized contract includes canonical definitions, data
  types, expected units, category domains, hospital availability, source
  mappings, rule IDs, caveats, and contract version. Adjustable physiologic
  ranges remain in a separately versioned cleaning policy.
- All-missing columns: retain them in canonical harmonized and cleaned base
  contracts, record zero availability and retirement candidacy, and require an
  explicit schema-version decision for physical removal. Convenience analysis
  projections may omit them but are not canonical artifacts.
- Historical compatibility: ingestion manifest `0.2` retains its frozen
  translated-gate labels. They map conceptually to the approved harmonization
  registry and harmonized union-schema gates and do not require mutation or
  regeneration of validated ingestion outputs.
- Implementation boundary: this decision updates architecture only. The next
  code phase remains a separately approved read-only raw-schema and
  parsing-token inventory; no parsing, harmonization, cleaning, or production
  data generation is authorized here.

## 2026-08-05 — Raw-schema and parsing-token inventory approved

- Status: explicitly approved for implementation, synthetic verification, and
  read-only production execution after ingestion audit `20260805T060046Z`.
- Inputs: immutable inventory `20260804T193415Z`, ingestion manifests `0.2`,
  ingested static/dynamic Parquet, and ingestion-audit artifact `0.1`. Every
  Parquet input is re-hashed before scanning.
- Scope: inventory every positional raw field, source-schema availability,
  null/empty/non-empty accounting, candidate canonical mapping, candidate kind,
  and bounded parsing-token evidence. No transformation is applied.
- Candidate provenance: frozen v2 policy `1.3` and legacy mappings/parsers seed
  the registry, but every candidate is explicitly marked as requiring raw-v3
  revalidation. New or unmapped raw columns remain blocking findings.
- Numeric evidence: mutually classify direct numeric, decimal comma, ratio,
  threshold, percentage, textual-missing candidate, whitespace-only, and
  unresolved syntax. Numeric sentinel matches are annotations, not applied
  missingness rules. Candidate numeric summaries are evidence, not conversions.
- Privacy: do not retain identifier or free-text distinct values. Exact raw
  names, bounded raw-token examples, and candidate numeric summaries remain in
  owner-only cluster Parquet reports; sanitized reports expose aggregates only.
- ARDS duplicate: inventory both positional occurrences and block the reviewed
  drop candidate if the second occurrence has any raw non-empty value.
- Output: immutable private and sanitized reports only. Candidate-registry
  approval, parsing, harmonization, concatenation, cleaning, derivation, and
  production data generation remain outside this implementation.

## 2026-08-05 — First partial raw-v3 harmonization rules approved

- Evidence: schema/token inventory `20260805T064135Z`, artifact `0.1`, covering
  934 raw column occurrences, 16,054 static rows, and 24,069,379 dynamic rows.
- Cluster decision: `cluster_id` is a categorical ASIC cluster assignment, not
  the canonical hospital ID. Approve hospital-scoped domains: UK00 `C1`–`C4`,
  UK01 `C5`, UK02 `C7`/`C9`, UK03 `C6`, UK04 `C10`, UK06 `C8`, UK07 `C11`, and
  UK08 `C12`. Canonical `hospital_id` continues to derive independently from
  the approved folder mapping; out-of-domain cluster values block.
- Height decision: approve an anchored singleton-numeric-list parser for UK00
  static `height_cm`; the single contained number is in centimetres. Empty,
  multi-valued, nested, or non-numeric lists remain unresolved and blocking.
- Alias decision: the exact UK01 static dialysis-free-days header ending in
  non-breaking space `U+00A0` maps to `dialysis_free_days`. No global header
  trimming is approved; exact raw-header provenance remains intact.
- Sentinel decision: approve raw numeric `-1`/`-1.0` as missing only for UK08
  static `hosp_los`, `dialysis_free_days`, and `vent_free_days`; the reviewed
  evidence count is 5,164 for each target. No other negative value, variable,
  or hospital is covered.
- Bounded-evidence decision: example truncation is informational only for an
  explicitly approved deterministic grammar at its exact hospital/variable
  scope. The initial decimal-comma scopes are UK01 dynamic `pf_ratio`, `peep`,
  `insp_pressure`, `delta_p_reported`, and
  `vt_per_kg_ideal_body_weight`. Categorical, unknown, unresolved, and
  unreviewed custom-syntax truncation remains blocking.
- ARDS decision: the previously approved second positional UK00 occurrence
  remains a harmonization drop candidate only after its zero-non-empty
  precondition is revalidated; ingestion continues to preserve both fields.
- Implementation boundary: policy/artifact `0.2` performs read-only
  classification and validation against stable rule IDs and evidence counts.
  It writes no harmonized values and does not approve the remaining seed
  registry or the harmonized union schema.

## 2026-08-05 — UK00 height becomes a repeated-measurement list

- Evidence: policy/artifact `0.2` run `20260805T081315Z` resolved the cluster,
  alias, sentinel, and bounded-example findings but retained 249 unresolved
  UK00 height cells. Private review showed multiple approximate measurements
  in a cell and occasional `nan` elements, including mixtures of implausible,
  missing, and plausible values.
- Supersession: rule `HARM-STATIC-HEIGHT-UK00-001`, which classified only a
  singleton list as a scalar `height_cm`, is superseded by
  `HARM-STATIC-HEIGHT-UK00-002` before any harmonized artifact was generated.
- Canonical contract: use `height_measurements_cm` with future Arrow/Parquet
  type `list<double>`. Preserve numeric order and duplicates; convert approved
  `nan` elements to null elements; never reduce the list during harmonization
  or cleaning.
- Accounting: report raw list-cell counts and numeric, approved-missing, and
  unresolved element counts. Unknown text, malformed syntax, nested lists, or
  empty elements remain blocking.
- Cleaning boundary: retain the list and mask physiologically invalid elements
  to null under a separately reviewed range. Do not choose that range in the
  parser.
- Derived boundary: mean, median, first-value, or other scalar selection is an
  explicit derived/analysis rule. Median is a candidate recommendation, not an
  approved transformation.
- Implementation boundary: policy/artifact `0.3` changes read-only evidence
  classification only. Ingested Parquet remains immutable and no harmonized,
  cleaned, or derived artifact is generated.

## 2026-08-05 — Fail-closed harmonization bootstrap implemented

- Authorization: begin harmonization implementation while the policy/artifact
  `0.3` read-only inventory rerun is pending, without assuming its outcome or
  generating production artifacts.
- Scope: load only the 18 active human-approved raw-v3 rules and transform one
  bounded synthetic Arrow raw-string column batch at a time. Implement exact
  categorical domains, direct and scoped decimal-comma numeric parsing, scoped
  sentinels, literal-empty/source-absence distinction, and repeated-height list
  parsing.
- Accounting: emit per-cell parse statuses and conserve input cells across
  direct, custom, sentinel, categorical, missing, and unresolved classes.
  Conserve every repeated-height element across numeric, approved missing, and
  unresolved classes.
- Safety: unknown tokens return null with unresolved status and block on
  `require_resolved()` without including the token in the exception. Only raw
  string Arrow inputs are accepted.
- Closed gates: production reads and execution, artifact writes, table-level
  mapping/merging, hospital concatenation, complete registry, ordered union
  schema, and variable dictionary remain disabled. No CLI or Slurm job exists
  for harmonization.
- V2 treatment: reuse only its general rule/metrics separation and bounded
  Arrow-batch pattern. Do not reuse its pooled input boundary, scalar height
  assumption, or production translation code.

## 2026-08-05 — Schema/token policy 0.3 technical gate passed

- Evidence: production schema/token inventory run `20260805T090942Z`, generated
  at `2026-08-05T09:09:42+00:00`, against ingestion audit
  `20260805T060046Z`.
- Coverage: all 8 approved hospitals and all 16 ingested tables were scanned;
  934 raw-column occurrences were inventoried across 16,054 static and
  24,069,379 dynamic rows.
- Technical result: zero unmapped raw-column occurrences, zero unresolved
  non-empty numeric tokens, zero truncated review-example columns, zero
  reviewed-rule token-domain violations, and zero non-empty values in the
  reviewed UK00 duplicate-ARDS drop candidate.
- Sentinel result: 15,492 numeric missing-sentinel candidates were classified,
  matching the three approved UK08 static scopes at 5,164 rows each.
- Repeated-height result: 3,676 UK00 height-list cells contained 3,947 elements:
  3,944 numeric elements, 3 approved missing elements, and 0 unresolved
  elements. The invariant `3,947 = 3,944 + 3 + 0` holds.
- Gate decision: the technical schema/token findings required for the first
  reviewed-rule harmonization bootstrap are resolved. This does not approve
  the remaining raw-to-canonical mappings, units, notations, aliases, common
  ordered schema, or variable dictionary.
- Remaining blockers: the complete candidate harmonization registry still
  requires raw-v3 review, and the provisionally excluded UK00 archive remains
  a publication blocker pending owner confirmation. Production harmonization,
  concatenation, artifact writes, and publication remain disabled.

## 2026-08-05 — Read-only complete-registry review implemented

- Authorization: after policy/artifact `0.3` passed its technical gate, proceed
  with a read-only review of all raw-to-canonical harmonization candidates.
- Input boundary: consume only the immutable private and sanitized schema/token
  bundle for a named run. Do not reopen ingested Parquet and do not rescan raw
  clinical rows.
- Accounting: assign every raw-column occurrence a stable private `R####`
  review ID and an explicit retain/validate/drop/unresolved disposition. Group
  retained occurrences into private `V####` canonical-variable candidates.
- Candidate metadata: surface proposed names, kinds, physical types, units,
  parser needs, categorical policies, aliases, and semantic splits. Preserve
  frozen-v2 and legacy decisions as proposals unless an active raw-v3 rule
  already approves the exact scope.
- Uncertainty policy: use explicit unresolved unit/type/semantic statuses rather
  than inferring a conversion from a label or distribution. Frozen-v2
  categorical mappings remain candidates pending raw-v3 token review.
- Privacy: exact headers and occurrence-level evidence remain in owner-only
  Parquet workbooks. The sanitized report contains counts and canonical review
  categories only; raw token examples are referenced in place and not copied.
- Closed gates: the command cannot read ingested data, execute harmonization,
  write data artifacts, concatenate hospitals, approve the registry, freeze the
  union schema, or approve the variable dictionary.

## 2026-08-05 — Complete-registry contract bootstrap implemented

- Authorization: continue result-independent implementation while the
  read-only candidate registry-review job runs.
- Scope: define and validate the format of a future complete, human-approved
  raw-v3 registry without creating or approving that registry.
- Completeness: require exactly one rule per private `R####` occurrence and
  exact agreement on hospital, table, raw header, positional occurrence,
  evidence count, and reviewed all-missing drop precondition.
- Canonical invariants: require a consistent kind, Arrow type, and unit for
  each table/target across hospitals. Require an explicit common alias group
  and merge policy when multiple same-hospital sources share a target.
- Synthetic planning: validate lossless Arrow field metadata and exact table
  coverage, then form a candidate output schema without reading values. Unknown,
  missing, duplicate, or non-string raw fields block.
- Closed gates: policy `0.1` permits synthetic schema planning only. No complete
  production registry is shipped; production reads, value transformations,
  artifact writes, concatenation, and union-schema approval remain disabled.

## 2026-08-05 — Registry review 0.1 technically passed and was superseded

- Evidence: production candidate-registry review run `20260805T093848Z` against
  schema/token run `20260805T090942Z` accounted for all 934 occurrences with
  zero technical blockers. It reported 146 current candidate variables, 19
  approved occurrence dispositions, 915 pending mappings, 13 alias groups, and
  the expected human-review gates.
- Gap: `candidate_semantic_split_group_count` was zero because artifact `0.1`
  counted only cases where the current seed registry already mapped one raw
  name to multiple targets. The preserved frozen-v2 UK00 PBW tidal-volume split
  was present only as a unit-status warning and was not a dedicated review item.
- Decision: retain run `20260805T093848Z` as valid immutable occurrence evidence
  but do not use it as the final mapping-approval basis.
- Correction: review/candidate-contract `0.2` explicitly carries the UK00
  candidate split from `vt_per_kg_ideal_body_weight` to the separate
  `vt_per_ideal_bw_total` target with candidate type `float64` and unit `mL`.
  It reports preserved semantic candidates separately from already-observed
  multi-target source groups.
- Boundary: this is review metadata only. It does not approve or apply the
  semantic split and still reads no ingested data or writes any data artifact.

## 2026-08-05 — Registry review 0.2 accepted as the candidate-review basis

- Evidence: production review run `20260805T094801Z`, policy/artifact `0.2`,
  against schema/token run `20260805T090942Z`.
- Technical result: all 934 raw-column occurrences were accounted for and
  `technical_blocking_finding_count` was zero. No production data artifact or
  transformation was generated.
- Candidate scope: 146 current canonical variables plus one explicit additional
  semantic-split target; 19 occurrence dispositions are already approved and
  915 remain pending. One reviewed all-missing drop, 13 alias groups, and one
  known semantic-split candidate are represented.
- Semantic preservation: the report separately records zero already-observed
  multi-target raw-name groups and one preserved known semantic-split candidate,
  preventing the UK00 PBW tidal-volume decision from being lost.
- Review decision: accept this immutable run as the authoritative candidate
  mapping-review basis. It does not approve any of the nine remaining human
  gates. Review will proceed by canonical-variable groups, with every final
  rule still bound one-to-one to its private `R####` occurrence evidence.

## 2026-08-05 — Alias classification narrowed to same-hospital collisions

- Trigger: review of the 20 static candidate variables showed that artifact
  `0.2` labelled a variable as an alias group whenever multiple raw spellings
  occurred anywhere across hospitals. This conflated cross-hospital mapping
  variants with sources that actually require coalescence.
- Human decision: distinguish `cross_hospital_name_variant` from
  `coexisting_alias_group`. A cross-hospital spelling difference still requires
  occurrence-to-target mapping review, but it does not require a merge policy.
- Alias definition: require alias overlap evidence and an explicit merge policy
  only when more than one retained source occurrence maps to the same canonical
  target within the same hospital and table. Duplicate positional occurrences
  are included even when their raw header text is identical.
- Artifact effect: registry-review policy/private/sanitized artifact `0.3`
  replaces the ambiguous alias fields and counts. Immutable run
  `20260805T094801Z` remains valid evidence for its declared `0.2` semantics but
  is superseded as the candidate-review basis after the `0.3` rerun.
- Boundary: this correction reads only the existing schema/token evidence and
  changes review metadata. It does not approve mappings, merge sources, read
  ingested data, or generate harmonized artifacts.

## 2026-08-05 — Static readmission type and study-time representation approved

- Owner-reviewed readmission domain: across the raw-v3 hospitals where the
  field is available, the only non-empty values are the numeric spellings `0`,
  `0.0`, `1`, and `1.0`. Approve canonical `icu_readmit` as nullable Boolean,
  mapping both zero spellings to false and both one spellings to true. Literal
  empty cells and source-schema absence remain missing; no other token is
  approved by this decision.
- Separation: `icu_readmit` remains distinct from discharge and mortality
  fields. The Boolean conversion is a hospital-design harmonization, not a
  derived cohort or outcome rule.
- Owner-reviewed study-time type: approve `time_since_study_start` as
  `float64`. Reviewed values were non-negative and ranged from 0 through 914 in
  the inspected evidence.
- Unresolved unit: do not infer days from the distribution. The exact temporal
  unit and source definition remain blocking for the variable dictionary and
  harmonized contract until documentary or data-owner evidence is recorded.
- Versioning: candidate contract `0.3` carries these partial approvals together
  with registry-review artifact `0.3`; all unrelated mappings and policies
  retain their prior pending status.

## 2026-08-05 — Archive exclusion confirmed and study-time unit made non-blocking

- Archive confirmation: the data owner confirmed that the reviewed UK00
  archive is an old snapshot that was not intended to be present in the raw
  hospital directory.
- Final handling: continue excluding the exact hash-identified archive from
  ingestion and every authoritative v3 input. Retain it in place only as an
  untrusted read-only historical comparison; do not extract, concatenate,
  delete, or relocate it through the pipeline.
- Provenance: inventory, ingestion, audit, and schema/token artifacts generated
  before confirmation remain immutable and correctly retain their historical
  provisional blocker. Registry-review policy `0.3` records the dated owner
  decision, marks the carried finding resolved, and cites it in new review
  manifests.
- Study-time unit: the owner confirmed that the unit is currently undefined and
  must not prevent subsequent procedures. Executable contracts therefore use
  the explicit unit marker `undefined` with approved non-blocking status;
  human-readable dictionaries may render the unit cell blank and must retain a
  caveat that no unit-dependent interpretation is approved.
- Analysis boundary: any later use, conversion, comparison, or interpretation
  of `time_since_study_start` requires a new source-definition decision. Its
  approved `float64` storage type does not imply a temporal unit.

## 2026-08-05 — Study time resolved to days and implementation phases defined

- Supersession: the preceding non-blocking `undefined` unit decision is
  superseded before candidate contract `0.3` was deployed or any harmonized
  artifact was generated.
- Source confirmation: the data owner confirmed that
  `time_since_study_start` is measured in days. Approve canonical type
  `float64` and unit `day` across its reviewed raw-v3 scope.
- Supporting semantic evidence: the reviewed phase-over-time figure identifies
  implementation phase code 0 as Calibration, code 1 as Roll-In, and code 2 as
  App-Implementation, plotted against time since study start. The source image
  remains outside the repository; its SHA-256 is
  `f57caa07375d686542084fc5ec6cd3e6f1712508651b10337238a43ac8db954d`.
- Canonical phase representation: retain `study_implementation_phase` as a
  categorical `large_string`, mapping codes to `calibration`, `roll_in`, and
  `app_implementation`. Accept integer-equivalent source spellings such as `0`
  and `0.0`; unknown, non-integral, or out-of-domain codes remain unresolved.
- Boundary: this resolves the current study-time unit and phase semantics. It
  does not approve unrelated candidate mappings, units, parsers, categorical
  domains, aliases, the union schema, or the variable dictionary.

## 2026-08-05 — Registry review 0.3 accepted as the current review basis

- Evidence: production registry-review run `20260805T104417Z` against immutable
  schema/token run `20260805T090942Z` accounted for all 934 raw-column
  occurrences and reported zero technical blockers. It generated no production
  data artifact.
- Archive resolution: the historical archive-confirmation finding is recorded
  once as resolved and no longer appears as a blocking finding.
- Alias correction result: 13 canonical variables have cross-hospital raw-name
  variants, while only 3 have same-hospital source collisions requiring overlap
  evidence and an explicit merge policy. One known semantic-split candidate
  remains separately pending.
- Partial-decision result: 142 candidate value types, 125 units, 74 unresolved
  units, and 13 categorical policies remain pending after applying the approved
  height, readmission, study-time, and implementation-phase decisions.
- Review decision: supersede run `20260805T094801Z` as the active candidate
  review basis while retaining it as immutable artifact `0.2` history. Continue
  human review from run `20260805T104417Z`; do not interpret its overall `FAIL`
  status as a technical execution failure.

## 2026-08-05 — Two empty occurrences retired and therapy confirmations preserved as a pair

- Evidence: registry-review run `20260805T104417Z` identified three genuine
  same-hospital target collisions. UK00 `ph_art` occurrence `R0129` and UK02
  `ntprobnp` occurrence `R0326` each have a full-scan raw non-empty count of
  zero, while their retained siblings are populated.
- Retirement decision: approve `R0129` and `R0326` as
  `drop_after_all_missing`, with their zero-non-empty evidence revalidated
  before any future table plan. No target variable is retired.
- Therapy decision: do not inherit the frozen-v2 binary-OR merge. Preserve
  UK00 occurrences `R0144` and `R0145` as one canonical
  `therapy_read_confirmation_utc` value with Arrow representation
  `fixed_size_list<bool>[2]`.
- Position contract: position zero and position one follow deterministic
  physical source-column order and remain bound to their private occurrence
  identities. Each row always has two positions; raw 0 becomes false, raw 1
  becomes true, and literal empty or source-schema absence becomes a null
  element. Unknown or non-binary tokens block.
- Information boundary: preserve all nine possible null/false/true pair states,
  including disagreements. Do not apply OR, consensus, source preference, or
  scalar reduction during harmonization or cleaning.
- Validation boundary: policy `0.1` permits only a read-only, bounded two-column
  production scan and private/sanitized report writes. It cannot write
  harmonized data, mutate inputs, concatenate hospitals, or approve the full
  registry or union schema.

## 2026-08-05 — Composite-source audit passed

- Evidence: production composite-source audit run `20260805T111026Z`, bound to
  registry-review run `20260805T104417Z`, scanned all 7,267,221 UK00 dynamic
  rows and reported zero technical or blocking findings. No production data
  artifact was generated.
- Retirement result: both approved all-missing occurrence retirements were
  revalidated successfully.
- Domain result: both therapy-confirmation sources contained only approved
  binary numeric values or source missingness; unresolved row count was zero.
- Conservation: all rows were counted exactly once across the nine fixed-pair
  states. There were 7,265,560 `[null, null]`, 1,315 `[null, false]`, 345
  `[true, null]`, and one `[true, false]` row; every other pair state had count
  zero.
- Decision confirmation: retain the approved `fixed_size_list<bool>[2]`
  representation. The single disagreement remains `[true, false]`; do not
  replace it with binary OR, consensus, or source preference in harmonization
  or cleaning.
- Gate effect: the three same-hospital collision groups identified by registry
  review `0.3` now have reviewed resolutions. This does not resolve the
  separately pending UK00 semantic split or unrelated mappings, types, units,
  parsers, categorical policies, union schema, or variable dictionary.

## 2026-08-05 — First static contract evidence batch authorized

- Scope: collect read-only evidence for `stay_id_global`, `weight_kg`,
  `hosp_los`, `icu_los`, `dialysis_free_days`, `vent_free_days`,
  `hospital_mortality_reported`, and `icd10_codes`.
- Evidence basis: bind the audit to registry-review run `20260805T104417Z`,
  passing composite-audit run `20260805T111026Z`, the immutable post-ingestion
  identifier check, and ingestion-manifest hashes for all selected static
  Parquets.
- Privacy decision: identifier values may be held transiently only to count
  duplicates and cross-hospital raw overlaps. They must never be written to the
  private or sanitized audit bundles. Exact non-identifier token examples stay
  owner-only on the authorized cluster.
- Execution boundary: scan and classify evidence only. Do not convert values,
  approve registry rules, write harmonized artifacts, concatenate hospitals,
  freeze the union schema, or modify any ingested input.
- Human gate: require a new explicit decision after the sanitized and private
  evidence are reviewed. An expected review `FAIL` with zero technical blockers
  is not an execution failure.

## 2026-08-05 — UK00 reported hospital mortality approved as nullable Boolean

- Evidence basis: static-contract audit run `20260805T112746Z`, registry-review
  run `20260805T104417Z`, and passing composite-audit run
  `20260805T111026Z`.
- Approved scope: UK00 static source `KH-Sterblichkeit`, occurrence 1, maps to
  `hospital_mortality_reported` with physical type `bool` and no unit.
- Value policy: after whitespace stripping and case normalization, `false`
  maps to `false` and `true` maps to `true`. Literal empty cells and source
  absence map to null. Any other non-empty token is unresolved and blocks the
  batch without being exposed in an exception or sanitized report.
- Semantic boundary: this remains the source-reported hospital mortality
  variable. It is not consolidated with other mortality fields; any composite
  mortality outcome belongs to a separately reviewed derived-stage decision.
- Implementation boundary: the decision is stored in a versioned partial
  registry with production reads, artifact writes, hospital concatenation, and
  union-schema freezing disabled. Synthetic batch execution is allowed only to
  test the approved mapping and accounting invariant.
- Still pending: the `Pseudo-ID` local/global identifier rewrite, decimal-comma
  handling for `weight_kg`, and UK00 textual `nan` handling for `hosp_los` have
  evidence but are not encoded as approved rules.

## 2026-08-05 — Static identifier, weight, and UK00 hospital-LOS rules approved

- Evidence basis: static-contract audit run `20260805T112746Z`, including the
  previously passing ingestion identifier-agreement check and the reviewed
  static row/uniqueness evidence.
- Identifier correction: the raw `Pseudo-ID`/`PseudoID` value represents
  `stay_id_local`; it must not be renamed directly to `stay_id_global`.
  Harmonization preserves the exact non-empty local value and derives the
  global value as `<stay_id_local>:<hospital-number>`, for example `1234:0` for
  `asic_UK00`. The approved suffixes are `0`, `1`, `2`, `3`, `4`, `6`, `7`,
  and `8` for the eight approved hospitals.
- Identifier safeguards: missing or empty values, surrounding whitespace,
  occurrence of the `:` separator inside the local ID, an unknown hospital,
  or duplicate local IDs within a static hospital table are blocking. The
  audit covered 16,054 non-empty static IDs with zero within-hospital
  duplicates. The 3,676 cross-hospital local-ID overlaps are expected and
  become distinct through the unique hospital suffix.
- Weight policy: UK00 static `weightKg`, occurrence 1, maps to nullable
  `float64` `weight_kg` in kg. Of 3,676 non-empty values, 3,614 follow the
  direct numeric grammar and 62 follow the approved decimal-comma grammar.
  Any other non-empty token remains blocking.
- Hospital-LOS policy: UK00 static `Liegedauer_KH`, occurrence 1, maps to
  nullable `float64` `hosp_los` in days. All 3,676 audited non-empty values are
  the exact reviewed textual-missing token `nan`, which maps to null. This
  missing-token rule is scoped to this hospital and occurrence. Because no
  numeric value was observed for this source, even a syntactically valid future
  numeric token remains blocking until separately reviewed.
- Provenance: registry `0.2` is a complete snapshot of the currently approved
  partial static decisions and supersedes `0.1` without modifying it. The
  earlier candidate mapping `Pseudo-ID -> stay_id_global` remains untouched as
  historical evidence from the pre-correction review and is overridden only
  by the new reviewed contract.
- Execution boundary: transformations remain synthetic-only. Production reads,
  data writes, hospital concatenation, and union-schema freezing stay disabled
  until the complete harmonization registry and later gates are approved.

## 2026-08-05 — Remaining-static decision review authorized

- Scope: re-aggregate immutable static-contract run `20260805T112746Z` for
  `icu_los`, `dialysis_free_days`, `vent_free_days`, and `icd10_codes`.
- Input boundary: read only the existing static-contract private manifest and
  occurrence/variable workbooks, its sanitized JSON, and fail-closed partial
  decision registry `0.2`. Do not reopen raw or ingested data.
- Proposed numeric contract: nullable `float64` days, with direct numeric
  parsing; dialysis- and ventilator-free days also retain already reviewed
  hospital-scoped missing sentinels. Unresolved non-empty values block.
- Proposed ICD-10 contract: preserve each non-empty source cell exactly as one
  nullable `large_string`; do not trim, split, normalize, validate, deduplicate,
  or translate codes in harmonization.
- Privacy: sanitized output may contain canonical targets, hospital IDs, and
  aggregate counts only. Exact raw headers and token examples remain in the
  existing owner-only bundle.
- Human gate: the command proposes but does not approve mappings, types, units,
  parsers, missing rules, or ICD-10 preservation. It cannot read production
  data, transform values, extend the registry, or write harmonized artifacts.

## 2026-08-05 — Remaining numeric static contracts approved; ICD-10 audit authorized

- Evidence: production remaining-static review generated at
  `2026-08-05T12:52:14+00:00`, bound to static-contract run
  `20260805T112746Z`, reported 26 selected occurrences, zero unresolved numeric
  tokens, and zero technical blockers.
- `icu_los`: approve all eight hospital mappings as nullable `float64` days.
  Seven hospitals contain only direct numeric values. UK01 contains no
  non-empty value, so any future non-empty UK01 token remains blocking.
- `dialysis_free_days`: approve UK02, UK06, and UK07 direct numeric values as
  nullable `float64` days. UK01 is unavailable and blocks any future non-empty
  token. UK08 contains only the previously reviewed `-1`/`-1.0` missing
  sentinels; they become null, while a newly appearing UK08 numeric remains
  blocking pending review.
- `vent_free_days`: approve the same type, unit, and evidence-scoped policies as
  dialysis-free days for UK01, UK02, UK06, UK07, and UK08.
- Provenance: partial registry `0.3` extends the immutable, hash-bound `0.2`
  registry with 18 hospital/source rules. Production reads, artifact writes,
  concatenation, and union-schema freezing remain disabled.
- ICD-10 decision: do not yet approve the proposed `icd10_codes` canonical
  string. Authorize a bounded read-only notation audit across the eight static
  source columns. Inspect candidate missing spellings and hospital notation
  patterns before deciding between exact `icd10_codes_source_text` preservation
  and a separately parsed canonical list.
- Privacy and execution: exact ICD-10 examples remain owner-only on the cluster;
  sanitized output contains syntax signatures and counts only. The audit may
  write reports but cannot parse values, approve a contract, or produce data.

## 2026-08-05 — ICD-10 source preservation and missing policy approved

- Evidence: notation-audit run `20260805T131735Z` scanned 16,054 rows across
  all eight hospitals with zero non-string cells and zero technical blockers.
  It classified 16,022 cells as comma-delimited, seven as single-code
  candidates, and 24 UK03 cells as one candidate textual-missing spelling.
- Private review: the data owner inspected that UK03 spelling on the
  authorized cluster and confirmed that all 24 cells contain the literal
  missing placeholder `nan`, not a diagnosis code.
- Source-text decision: approve nullable `large_string`
  `icd10_codes_source_text`. Preserve every other non-empty source string
  exactly. Literal empty cells, source absence, and the hospital-scoped UK03
  `nan` rule map to null; unknown non-empty source values remain blocking.
- Candidate list decision: authorize a final read-only audit of comma splitting
  with component-edge whitespace removal only. Single codes become one-element
  candidate lists. Source order, duplicates, case, and punctuation remain
  unchanged; empty components block and diagnosis semantics are not validated.
- Activation boundary: reviewed ICD contract `0.1` records this approval but
  deliberately leaves `allow_parser_activation: false`. The component audit
  may write private and sanitized reports only. It creates no source-text or
  parsed list field and does not authorize production harmonization.

## 2026-08-05 — ICD-10 component-detail audit authorized

- Evidence: component-audit run `20260805T134245Z` reproduced 16,029
  diagnosis-bearing cells and 466,002 components with zero technical blockers.
  It found 26 empty components, one component outside the structural grammar,
  and 96,505 duplicate occurrences across 9,012 cells.
- Private classification: the data owner inspected the single unexpected
  component on the authorized cluster and classified it as a likely
  incomplete/input-error candidate. Its exact value remains excluded from
  committed configuration, tests, documentation, and sanitized reports.
- Audit decision: authorize a bounded read-only detail scan to classify every
  empty component as leading, trailing, or interior; distinguish adjacent from
  separated duplicates; measure maximum multiplicity; and report
  counterfactual list sizes after empty removal and ordered deduplication.
- Decision boundary: no empty, duplicate, or incomplete component may be
  removed by this audit. Their final policies require another explicit human
  decision after the detail report. Parser activation and every production data
  write remain disabled.

## 2026-08-05 — Final ICD-10 list contract and bounded parser approved

- Evidence: component-detail run `20260805T135859Z` reproduced 466,002 source
  components with zero technical blockers. All 26 empty components are trailing
  delimiters, no cell becomes empty after their removal, duplicate multiplicity
  reaches 48, and 75,088 of 96,505 duplicate occurrences are separated rather
  than adjacent.
- Empty policy: remove one reviewed trailing empty delimiter component in the
  scoped UK03 and UK08 evidence. Any leading or interior empty component, more
  than one trailing empty in a cell, or a trailing empty in another hospital
  remains unresolved and blocks.
- Duplicate policy: construct `icd10_codes` as an ordered unique
  `list<large_string>` by retaining the first occurrence of each exact trimmed
  component. Preserve case and punctuation. Exact multiplicity remains
  recoverable from `icd10_codes_source_text`.
- Incomplete-component policy: remove the single privately reviewed
  incomplete/input-error component only when it matches the owner-only evidence
  for its reviewed hospital and audit run. Its exact value is absent from
  committed configuration. Every other non-empty non-code component blocks.
- Accounting invariant: 466,002 source components equal 369,470 retained
  unique valid components, 96,505 duplicates removed, 26 trailing empty
  components removed, and one reviewed incomplete component removed.
- Activation boundary: contract `0.2` enables only private-evidence reads and
  bounded/synthetic parser execution. Production reads, harmonized artifact
  writes, hospital concatenation, and union-schema freezing remain disabled.
  No CLI or Slurm production harmonization entry point is authorized yet.

## 2026-08-05 — Complete static-registry accounting audit authorized

- Scope: account for all 130 static raw-column occurrences across the eight
  approved hospitals using immutable harmonization registry-review run
  `20260805T104417Z`.
- Decision overlay: combine reviewed raw-v3 schema/token rules, reviewed static
  decision registry `0.3`, final ICD-10 contract `0.2`, and only those target
  representations or categorical domains explicitly marked as human-approved
  raw-v3 decisions. Frozen-v2 and legacy-only mappings remain pending.
- Hospital identifier correction: bind the approved inventory mapping as a
  decision source and include canonical nullable-free `large_string`
  `hospital_id` for every hospital. Do not infer or fabricate the separate
  `hospital_code_source` field when no raw-v3 static occurrence exists.
- Conflict policy: fail technically if approved sources disagree on an
  occurrence target or a canonical physical type/unit. Do not resolve a
  conflict by source precedence.
- Draft outputs: produce owner-only occurrence coverage, variable dictionary,
  and ordered-schema artifacts, plus a sanitized canonical-name/count report.
  Exact raw headers remain private on the authorized cluster.
- Approval boundary: the proposed order and every dictionary definition remain
  unapproved even where a mapping, parser, type, or unit is already executable
  in bounded synthetic tests.
- Execution boundary: report evidence and decision-file reads only. Do not
  read raw or ingested clinical data, transform values, write harmonized data,
  concatenate hospitals, or freeze the static registry/schema.

## 2026-08-06 — Dynamic identifier source contract corrected and approved

- Failure evidence: candidate run `20260806T065514Z` wrote only UK00 static
  output before stopping at UK00 dynamic with the requirement that every table
  contain exactly one raw `Pseudo-ID`/`PseudoID` occurrence. Sanitized registry
  accounting showed one bound static occurrence for every hospital, no dynamic
  occurrence for seven hospitals, and one bound dynamic occurrence for UK04.
- Root cause: the dry-run implementation and its synthetic fixture incorrectly
  modeled a raw identifier column in every dynamic hospital table. That does
  not match the production export and is not an ingestion defect.
- Approved static rule: keep the existing reviewed raw `Pseudo-ID`/`PseudoID`
  source as `stay_id_local`, then derive `stay_id_global` with the approved
  hospital suffix.
- Approved dynamic rule: derive `stay_id_local` from the mandatory
  `__v3_filename_stay_id` lossless-ingestion provenance field. Require every
  available `__v3_in_file_stay_id` to agree. Treat the UK04 raw identifier as
  corroboration that must exactly match the ingested in-file evidence; do not
  prefer or silently discard it.
- Cross-table safeguard: every dynamic local identifier must resolve to the
  same hospital's static stay set. Missing filename provenance, disagreement,
  an unapproved identifier raw name, multiple raw corroborating occurrences,
  or an unknown static stay is blocking without exposing identifier values.
- Privacy: candidate exceptions and sanitized reports must not contain stay
  identifiers. Candidate manifests record source strategies and aggregate
  corroboration counts only.
- Artifact boundary: dry-run policy and candidate artifacts advance to `0.2`.
  The failed `.20260806T065514Z.incomplete` directory remains preserved and is
  never resumed or overwritten; any rerun requires a new run ID.

## 2026-08-06 — UK00 height execution fix and UK03 study-phase mapping approved

- Candidate evidence: non-publishable dry-run `20260806T072541Z` completed all
  eight hospitals with zero technical blockers. Its 5,036 unresolved cells
  were confined to all 3,676 UK00 `height_measurements_cm` cells and all 1,360
  UK03 `study_implementation_phase` cells.
- Height root cause: the candidate Arrow type selector evaluated the generic
  numeric kind before the approved `list_float64` representation. The source
  strings therefore reached scalar numeric parsers instead of the already
  reviewed numeric-list parser. Correct type precedence; retain the approved
  ordered list, omit only approved missing list elements, preserve duplicates,
  and defer physiologic filtering and aggregation to later stages.
- Phase evidence: private inspection established the UK03 source vocabulary as
  the three labels `K`, `RI`, and `QS`, plus the textual missing spelling
  `nan`. Phase-specific time-since-study-start ordering supports `K` as phase 0
  calibration, `RI` as phase 1 roll-in, and `QS` as phase 2 application.
- Phase decision: emit semantic nullable `large_string` values `calibration`,
  `roll_in`, and `app_implementation`; convert only the reviewed `nan` spelling
  to null. Any other non-empty UK03 phase token remains blocking.
- Scope: this is hospital-specific harmonization of a source-system notation,
  not cleaning or derivation. Candidate artifacts advance to version `0.3` and
  remain non-publishable. Run `20260806T072541Z` remains immutable comparison
  evidence and cannot be promoted because it contains unresolved nulls.

## 2026-08-06 — Consolidated unit, semantic, and cleaning-boundary decisions approved

- Evidence: candidate run `20260806T074852Z` and consolidated audit run
  `20260806T092818Z`; the latter completed all 16 hospital tables with zero
  technical blocking findings and did not modify candidate data.
- Unit representation: approve the named units recorded in reviewed decision
  contract `0.1`. Oxygen concentrations and saturations use percentage points.
  I:E fields remain dimensionless but analysis-ineligible until ratio direction
  is established. UK06 per-kilogram tidal-volume fields remain
  analysis-ineligible pending a cleaned-layer mixed-scale decision.
- Approved hospital conversions: UK04 `etco2` divide by `7.50062` to mmHg;
  UK03 `fio2` and `hematocrit` multiply by `100`; UK00 `lymph_pct` multiply by
  `100`. Apply only when building a new candidate. Do not mutate the immutable
  reviewed candidate.
- Unresolved units: retain the harmonized value without conversion, keep unit
  `unresolved`, and set `analysis_eligible=false`. Distribution similarity is
  screening evidence, not proof of a physical unit.
- Semantics: preserve reported and future computed driving pressure separately;
  keep SOFA/iSOFA variants, ScvO2/SaO2/SpO2, dialysis/ventilator-free days, and
  UK00 `vt_per_ideal_bw_total` separate. Treat the two iSOFA sum discrepancies
  at approximately `1.8e-15` as floating-point noise.
- Cleaning boundary: defer all 42 observed invalid-range variable decisions and
  the arterial-pH row-scale rule to the cleaned layer. Apply no physiologic
  masking or row-level power-of-ten repair during harmonization.
- All-missing columns: preserve `feo2`, `severity_read_confirmation`,
  `sofa_score_without_gcs`, and `stroke_volume_bolus` with an explicit
  dictionary flag. Never drop them silently.
- Audit bookkeeping: targeted scale/bucket rules are explicitly dynamic-table
  scoped. Future review counts represent unique decisions, while all hospital
  evidence rows remain private and unchanged.
- Remaining gates: categorical domains, ordered union schemas, variable
  dictionary, and publication. A report-only categorical checkpoint is
  authorized to reuse the immutable consolidated audit without rescanning
  clinical rows.

## 2026-08-06 — Complete categorical contract approved

- Evidence: categorical review run `20260806T100728Z` accounted for 15 scalar
  categorical variables, one fixed-position composite, 304 domain evidence
  rows, and zero truncated variables with no technical blocker.
- Privacy: the approval uses aggregate domain values and counts only. It does
  not expose patient rows, stay identifiers, protected filenames, or row-level
  evidence.
- ARDS: parse integral notation to nullable `int32` and enforce the complete
  hospital-scoped domain. Preserve UK00 codes `10`, `100`, `200`, `1600`,
  `3000`, and `60000` exactly; never reinterpret them as 1–3. Keep the complete
  variable analysis-ineligible until code semantics are documented.
- ECMO: map integral numeric zero/one notation to nullable Boolean. Convert only
  the reviewed UK03 textual missing token to null; the same token at another
  hospital is blocking.
- Position therapy: reclassify from categorical string to nullable `float64`
  dimensionless proportion. Preserve the complete reviewed numeric domain,
  including UK00 fractions, without rounding or Boolean conversion. Document
  the fractional-versus-mostly-binary hospital pattern.
- All missing: retain `severity_read_confirmation` as nullable `large_string`,
  flag it globally all missing, and mark it analysis-ineligible.
- Static domains: retain the reviewed semantic vocabularies for age, BMI,
  cluster, mortality/discharge, sex, height, and weight groups. Convert only
  UK01 height-group `-1` to null. Keep mortality-related variables separate.
- Fail-closed boundary: a new code, new numeric proportion, wrong-hospital
  sentinel, or hospital-domain mismatch blocks rather than being passed through
  or coerced to missing.
- Next gate: build one immutable report-only proposal for the complete ordered
  schemas and variable dictionary. Do not alter the existing candidate or write
  released harmonized data before that proposal is explicitly approved.

## 2026-08-06 — Schema/dictionary proposal 0.1 corrected before approval

- Evidence: report `20260806T102800Z` was technically consistent with its input
  candidate, but it was not approved because two reviewed output decisions were
  represented incompletely.
- Conversion-unit correction: the reviewed UK03 hematocrit and UK00 lymphocyte
  percentage multiplications both produce percentage points. Their common
  dictionary unit is now `percent`, even though those variables were not also
  duplicated in the named-unit section of decision contract `0.1`. Conflicting
  named and conversion-output units are blocking.
- Time representation: raw v3 has `minutes_since_icu_admission` but no raw
  `timeidx` occurrence. Preserve the existing artificial-time decision as the
  explicitly generated `anchored_time_since_icu_admission`, equal to
  `2020-01-01 00:00:00` plus the relative minutes. It is a harmonized notation,
  not a real calendar timestamp and not claimed as raw-source provenance.
- Globally unavailable legacy field: no raw-v3 occurrence maps to `vt_per_kg`.
  Do not fabricate an all-null output and do not merge it with the separately
  source-backed `vt_per_kg_ideal_body_weight`. Historical quality rules may
  retain the absence as unavailable evidence.
- Artifact boundary: schema/dictionary review advances to `0.2`; report
  `20260806T102800Z` remains immutable, superseded evidence. No candidate or
  production data are modified by this correction.

## 2026-08-06 — Ordered harmonized schema and variable dictionary frozen

- Human approval: the data owner explicitly approved and froze the ordered
  harmonized schema and variable dictionary described by review
  `20260806T103811Z`.
- Frozen scope: 153 clinical variables (23 static and 130 dynamic), including
  152 source-backed variables and the reviewed generated artificial anchored
  time; five operational provenance fields remain appended per table.
- Units and eligibility: 72 unresolved-unit variables remain preserved and
  analysis-ineligible. Four globally all-missing variables remain present and
  flagged. This freeze does not guess a unit, activate an ineligible variable,
  or apply cleaning.
- Exact contract: reviewed freeze policy `0.1` records every ordered field,
  physical type, unit, eligibility state, evidence run ID, metric, and the data
  owner's approval statement. The freeze command must also verify the exact
  private dictionary hash before copying it into a versioned v3 contract.
- Authorization boundary: the freeze authorizes implementation of a new
  streaming harmonized build under this exact contract. It does not modify the
  immutable candidate, generate clinical data, apply cleaning or derivation,
  or authorize publication.

## 2026-08-06 — Frozen-contract harmonized build authorized for execution

- Freeze evidence: contract `0.1` passed on the production cluster with zero
  human or technical blockers; schema and dictionary are both frozen. It
  contains 23 static and 130 dynamic clinical fields plus the five operational
  provenance fields on each table.
- Input lineage: the build consumes immutable candidate run
  `20260806T074852Z`, verifies all candidate file hashes and row counts, and
  remains traceable to the lossless per-hospital ingestion boundary. The
  candidate does not become a new authoritative raw input.
- Build scope: pool the eight hospitals in approved deterministic order under
  contract `0.1`; apply only the reviewed categorical contract, four approved
  hospital unit conversions, artificial anchored time, frozen Arrow types and
  metadata, and provenance preservation.
- Excluded scope: do not filter rows or stays, apply physiologic cleaning,
  repair isolated scale errors, derive variables or cohorts, promote a release,
  or publish data.
- Verification: a second independent streaming pass recomputes and compares
  every output cell, including unchanged values, categorical mappings, unit
  conversions, anchored timestamps, identifiers, and provenance. It also
  verifies schemas, hashes, hospital order, conservation, and rule-level
  accounting.
- Human gate: a technically passing audit leaves exactly one blocker—explicit
  approval to promote the run-scoped harmonized candidate. Until then the
  output remains non-publishable under `harmonized_candidates/<run-id>/`.

## 2026-08-06 — Harmonized candidate 20260806T111156Z approved for promotion

- Audit result: technical `PASS` over all 16,054 static rows, 24,069,379
  dynamic rows, and 3,249,815,677 output cells. The frozen schema, unchanged
  values, categorical transformations, four unit conversions, artificial
  anchored time, identifiers, and provenance all agreed exactly.
- Human approval: the data owner stated, “I approve promotion of harmonized
  candidate 20260806T111156Z under frozen contract 0.1.” This resolves the sole
  `harmonized_candidate_release_approved` finding.
- Promotion representation: copy the audited static and dynamic Parquet bytes
  unchanged into immutable release `harmonized/releases/20260806T111156Z`.
  Preserve the candidate and bind the release manifest to the build, audit,
  frozen-contract, promotion-policy, and payload hashes.
- Current release: create `harmonized/current_release.json` once. Do not
  overwrite it automatically; replacing a current release requires a new
  explicit approval and promotion policy.
- Scope: the released harmonized layer is approved as input to cleaning. No
  cleaning, derivation, row/stay filtering, cohort selection, or external data
  export is authorized by this promotion.

## 2026-08-06 — Consolidated cleaned-candidate policy authorized

- Input: use only harmonized release `20260806T111156Z` under contract `0.1`;
  verify its pointer, manifest, schema, rows, and hashes.
- Legacy rules: apply 34 source-backed rules with approved units, preserve and
  audit 12 unresolved-unit targets, and record `delta_p_computed` as unavailable
  until derivation.
- V3 additions: apply explicit resolved-unit/nonnegative rules to six static
  and nine dynamic fields. No unlisted distribution-derived range is invented.
- Entry-error repair: try six power-of-ten factors only for a finite
  out-of-range value with both bounds. Correct a unique in-range result; mask
  no-match and ambiguous results. Record factor-level counts.
- Height and UK06: filter height elements without aggregation; mask the
  irrecoverable UK06 `vt_per_kg_ideal_body_weight` field without creating the
  absent `vt_per_kg` field.
- Preservation: retain all rows, stays, ordered fields, four globally
  all-missing columns, identifiers, and five provenance fields. Apply no
  derivation, cohort logic, time blocking, or external export.
- Gate: build a run-scoped candidate and recompute every output cell in one
  consolidated audit. Promotion still requires explicit human approval.

## 2026-08-06 — Cleaned candidate 20260806T114234Z approved for promotion

- Audit result: technical `PASS` over all 16,054 static rows, 24,069,379
  dynamic rows, and 3,249,815,677 output cells, with zero post-clean range and
  non-finite violations. No row, stay, or column was removed.
- Reviewed changes: 838 uniquely resolvable power-of-ten corrections, 542,477
  range masks, 53 removed height-list elements, and 99,362 values masked under
  the reviewed UK06 mixed-scale rule. The 10,021 unresolved-unit values outside
  legacy ranges remain unchanged and audit-only.
- Human approval: in direct response to the proposed promotion of this exact
  candidate under cleaning policy `0.1`, the data owner stated, “i approve.”
  This resolves `cleaned_candidate_release_approved`.
- Promotion representation: copy both audited Parquet files byte-for-byte into
  immutable release `cleaned/releases/20260806T114234Z`, preserve the candidate
  and evidence, and bind the release manifest to all candidate, audit, policy,
  harmonized-input, promotion-policy, and payload hashes.
- Boundary: create `cleaned/current_release.json` once and approve the released
  layer as input to later derivation. Do not rerun cleaning, overwrite a
  release or pointer, derive variables, filter rows or stays, or authorize
  external export during promotion.

## 2026-08-06 — Consolidated core-derived contract review authorized

- Input: read only cleaned release `20260806T114234Z`; verify its current
  pointer, promotion lineage, manifest, schemas, rows, and hashes before
  scanning any clinical value.
- Consolidated scope: audit exact elapsed hours, computed driving pressure,
  conflict-aware hospital/ICU mortality, and an observed-marker ventilation
  episode summary in one production pass and one human review gate.
- Driving pressure: candidate formula is `insp_pressure - peep`; retain
  `delta_p_reported` and both inputs separately. Audit 0–60 cmH2O candidate
  validity and reported overlap before freezing the out-of-range behavior.
- Mortality: use reviewed detailed status implications and the reported
  hospital fallback. Any disagreement sets both derived outcomes missing and
  exposes a conflict flag. `discharge_status` is audited but does not silently
  affect the output.
- Ventilation: evaluate non-missing FiO2, PEEP, VT, or VT/ideal-body-weight as
  observed support. Gaps up to eight hours and durations at least 24 hours are
  inclusive. This is a transparent proxy, not ground truth and not an
  exclusion rule.
- Episode ordering and ICU window: sort the direct cleaned timestamps within
  each contiguous stay so rare source-order regressions do not alter episode
  boundaries. Report both all-time and nonnegative-time evidence; propose the
  nonnegative ICU-relative window for the flag while preserving negative
  pre-admission rows unchanged.
- Separation: retain all SOFA/iSOFA variants without coalescing or calculating
  a new score. Defer time blocking and analysis cohorts to separate named
  recipes.
- Boundary: the checkpoint writes aggregate reports only, no derived clinical
  data. It filters nothing and has one intended human blocker covering all
  four recipes together.

## 2026-08-06 — Computed driving pressure requires hospital semantic review

- Trigger: the consolidated derivation evidence found 3,625 negative and 11
  above-60 cmH2O results from `insp_pressure - peep`. Only 839,792 of
  3,231,499 rows with separately reported driving pressure agreed with that
  formula within 0.01 cmH2O.
- Interpretation: individually harmonized cmH2O units do not establish whether
  a hospital's inspiratory-pressure field is absolute airway pressure or
  pressure above PEEP. The finding may therefore be semantic rather than a
  missed power-of-ten conversion.
- Decision: do not activate or merely range-mask `delta_p_computed`. First run
  one hospital-level, read-only comparison of reported driving pressure with
  both `insp_pressure - peep` and unchanged `insp_pressure`.
- Evidence boundary: scan only the immutable cleaned dynamic release; write
  exact aggregate hospital counts and errors without identifiers or patient
  rows. Record input and policy hashes automatically in the private manifest;
  do not add another manually supplied hash parameter.
- Human gate: explicitly approve, defer, or reject the interpretation for each
  hospital. No formula, masking, cleaning, derivation, or publication occurs.

## 2026-08-06 — Core-derived contract 0.1 approved

- Human approval: the data owner approved the complete ASIC v3 core-derived
  contract after review `20260806T161232Z` resolved driving-pressure semantics.
- Driving pressure: the primary ASIC protocol defines `P_EI - PEEP`; apply it
  to every hospital. Preserve both inputs and `delta_p_reported`. Retain results
  within 0–60 cmH2O; otherwise set the computed value missing and expose a true
  out-of-range flag. Missing inputs produce a missing value and missing flag.
- Elapsed time: derive exact hours by division by 60 and preserve all negative
  pre-admission rows and the minute field.
- Mortality: combine detailed status, reported hospital mortality, and discharge
  disposition as reviewed implications. Resolve hospital and ICU outcomes
  independently, with separate conflict flags and no silent source priority.
- Ventilation: count all support-marker timestamps; construct episodes from
  sorted nonnegative ICU-relative times, joining gaps up to eight hours and
  qualifying durations of at least 24 hours inclusively. This is a proxy, not
  ground truth or a cohort rule.
- Separation: preserve SOFA/iSOFA variants, do not filter rows or stays, and
  defer time blocking and analysis-specific cohorts.
- Implementation boundary: build a run-scoped non-publishable candidate and
  independently recompute every output cell in one bounded-memory job. A later
  explicit approval is required for byte-preserving promotion.

## 2026-08-06 — Core-derived candidate 20260806T170134Z approved for promotion

- Audit result: technical `PASS` over all 16,054 static rows, 24,069,379
  dynamic rows, and 3,322,168,300 output cells. Every input column was preserved
  exactly and every derived output was independently recomputed exactly.
- Reviewed accounting: 24,069,379 exact elapsed-hour values with 247,977
  negative values retained; 6,383,119 in-range computed driving-pressure
  values and 3,636 masked/flagged out-of-range results; zero hospital- or
  ICU-mortality source conflicts; 53,535 nonnegative-time support episodes and
  12,775 stays meeting the observed-support 24-hour proxy.
- Human approval: the data owner stated, “I approve promotion of core-derived
  candidate 20260806T170134Z under core-derived contract 0.1.” This resolves
  the sole blocker `core_derived_candidate_release_approved`.
- Promotion representation: copy both audited Parquet files byte-for-byte into
  immutable release `derived/releases/20260806T170134Z`, preserve the candidate
  and evidence, and bind the release manifest to candidate, audit, contract,
  cleaned-input, promotion-policy, and payload hashes.
- Boundary: create `derived/current_release.json` once and approve the release
  for internal analysis input. Do not rerun cleaning or derivation, overwrite a
  release or pointer, filter rows or stays, create a cohort, apply time
  blocking, or authorize external export during promotion.

## 2026-08-07 — Consolidated unresolved-unit review authorized

- Human authorization: the data owner approved one consolidated checkpoint for
  all 72 frozen unresolved-unit dynamic variables and the 12-variable subset
  with migrated legacy range rules.
- Input boundary: read only cleaned release `20260806T114234Z`, frozen schema
  and dictionary `0.1`, the exact source occurrence plan from harmonization
  dry-run `20260806T074852Z`, prior consolidated audit `20260806T092818Z`, and
  the v3-owned quality registry. Validate immutable lineage automatically.
- Evidence: create one owner-only decision workbook, hospital numeric profiles,
  source mappings, bounded cross-hospital scale/distribution screens, and exact
  aggregate legacy-range accounting in one streaming pass.
- Decision order: resolve definition and unit before deciding whether a legacy
  range is applicable. Distribution similarity or power-of-ten alignment is a
  screening hypothesis, never automatic unit evidence or conversion approval.
- Safe current action: preserve values and keep affected variables
  analysis-ineligible. The 10,021 values outside legacy ranges remain unchanged
  unless a later unit-aware cleaning decision is explicitly approved.
- Version boundary: a unit conversion requires harmonized contract `0.2` and
  downstream rebuilds; a range-only change requires cleaning policy `0.2` and
  downstream rebuilds. Existing releases remain immutable.
- Output boundary: reports only. Do not modify harmonized, cleaned, or derived
  releases; do not convert, mask, filter, publish, or authorize external export.

## 2026-08-07 — Complete team-derived unit candidate requested

- Data-owner direction: assign concrete expected units to all 72 variables so
  ASIC can later be integrated with MIMIC-IV. Do not treat current all-missing
  status as a reason to leave a unit unresolved or a variable ineligible.
- Explicit confirmations: every SOFA/iSOFA field is a dimensionless score-point
  variable; arterial blood-gas and advanced-hemodynamic units are approved.
- Additional evidence: reuse the `unit_asic` decisions from the prior
  phase-aware mortality configuration by value and provenance, without adding
  a runtime dependency on that repository.
- Provenance boundary: the hospitals supplied no complete unit dictionary.
  Future variable dictionaries and README documentation must say that units
  were derived by the data owner and pipeline team from prior configuration,
  definitions, clinical experience, and aggregate v3 evidence.
- Recovery policy: convert high-confidence hospital scale differences during
  harmonization. If a hospital-specific medication definition cannot be
  reconstructed, retain its source in the ingested layer but mask the affected
  hospital-variable pair in cleaning rather than assigning a misleading value.
- Implementation: create complete candidate register `0.2-candidate.1` with 72
  units, nine proposed conversions, nine proposed site masks, and the complete
  12-variable range dispositions. The candidate cannot modify or supersede any
  `0.1` release and requires explicit review plus a consolidated row-level
  audit before activation.

## 2026-08-07 — Complete candidate unit-decision audit implemented

- Review correction: earlier proposed scale actions relied heavily on
  hospital medians and broad ranges. They remain candidates rather than
  approved transformations.
- Complete evidence: rescan all 24,069,379 released cleaned dynamic rows for
  every variable affected by the nine proposed conversions or nine proposed
  masks. Produce all 128 hospital-variable profiles with exact counts,
  minima, maxima, means, and standard deviations plus deterministic bounded
  Q1/Q5/Q25/median/Q75/Q95/Q99 estimates.
- Comparison: report target distributions before and after every proposed
  conversion alongside unaffected-hospital peer distributions. Do not infer a
  unit or approve a conversion automatically from distribution alignment.
- Weight-linked action: join UK00 vasopressin to static `weight_kg` in memory
  through exact global stay identifiers and record complete aggregate linkage
  accounting. Never write identifiers or patient rows.
- Boundary: reports only. Do not convert or mask persisted values, modify any
  `0.1` release, activate contract `0.2`, filter data, or authorize external
  export. One explicit consolidated human decision remains required.
- First-run correction: the first `0.1` audit report generated at
  `2026-08-07T07:57:28Z` technically completed, but its UK03 furosemide profile
  had two finite values and no sampled median because arbitrary row-position
  sampling missed both. Do not use that report for final approval. Audit policy
  `0.2` samples finite values, guarantees complete quantiles for sparse
  profiles, and adds positive-value distributions to prevent zero-heavy
  medication fields from hiding scale differences.

## 2026-08-07 — Unit conversions approved; masks replaced by semantic splits

- Evidence: corrected unit-decision audit `20260807T083117Z` technically
  passed over all 24,069,379 dynamic rows, 128 hospital-variable profiles,
  nine conversions, and nine proposed mask scopes.
- Human decision: the data owner approved the recommended parallel-variable
  approach and instructed implementation to continue.
- Conversions: approve all nine audited hospital unit conversions, including
  complete UK00 vasopressin-to-weight linkage with 78,770 converted values and
  zero missing or invalid weights.
- Preservation: supersede every proposed cleaning mask. Route the affected
  hospital's source values unchanged into nine parallel harmonized variables;
  keep the ordinary canonical variable unavailable for that hospital.
- Resolved alternates: retain weight-normalized UK08 clonidine, ketanest,
  morphine, and propofol plus absolute-rate UK03 epinephrine and norepinephrine
  as eligible, explicitly unit-labelled variables.
- Unresolved sources: retain UK08 hydrocortisone, UK02 prednisolone, and UK00
  sufentanil as hospital-scoped `float64` source-scale variables that are
  analysis-ineligible pending definition. Do not infer a conversion or discard
  their values.
- Cleaning boundary: the 12 priority range decisions and their 10,021 findings
  remain pending cleaning-policy `0.2` review; this approval does not activate
  them.
- Release boundary: authorize schema/dictionary planning only. Do not freeze a
  schema, build clinical data, modify any `0.1` release, or authorize export.

## 2026-08-07 — Medication zero and missing values remain distinct

- Data-owner question: determine whether medication zeros exist and whether
  zeros or missing cells should be treated as equivalent.
- Decision: preserve every explicit finite numeric zero as zero. Preserve every
  null as unavailable or unobserved. Never impute null as zero, convert zero to
  null, or infer inactive treatment from null during harmonization or cleaning.
- Hospital availability: a variable unavailable for an entire hospital and a
  missing cell in a partially observed hospital-variable series both remain
  null.
- Downstream interpretation: `value > 0` may be used as an explicitly derived
  exposure candidate. Carry-forward and medication-state intervals require a
  separate reviewed derivation contract.
- Evidence: add one complete aggregate scan of 25 source medication/therapy
  variables over all 24,069,379 cleaned dynamic rows, including exact
  hospital-variable counts and conservation checks for four medication unit
  conversions and nine semantic splits.
- Boundary: reports only; no clinical value, release, conversion, split,
  cleaning rule, schema freeze, or external export is changed or activated.

## 2026-08-07 — General nonnegative medication/therapy cleaning domain

- Evidence: all 201 negative medication/therapy values occur in UK08
  `inhaled_no` across seven values from -1.8 to -0.1. They are not one missing
  sentinel and have no safe deterministic correction.
- Data-owner decision: use one general dictionary-driven rule rather than a
  UK08-specific exception. Every numeric field classified as a medication or
  therapy dose, rate, or concentration has an inclusive lower bound of zero.
- Cleaning action: after hospital harmonization and semantic splitting, mask
  each finite value below zero to null and count it by hospital and variable.
- Preservation: retain negative values in ingestion and harmonization; retain
  zero, positive values, and null unchanged. Never take absolute value or clip
  a negative value to zero.
- Extensibility: a future legitimately signed field requires an explicit
  exemption before it may use the medication/therapy classification.
- Activation boundary: approved for cleaning-policy `0.2` implementation but
  not activated against any existing release.

## 2026-08-07 — Harmonized schema and variable dictionary 0.2 frozen

- Approved evidence: exact amendment review `20260807T101321Z`, generated at
  `2026-08-07T10:14:15+00:00`.
- Reference correction: the original approval string `20260807T101415Z` was
  the generation timestamp without punctuation, not the immutable run ID. The
  data owner supplied the sanitized review index linking the timestamp and
  approved metrics uniquely to `20260807T101321Z` and confirmed that contract
  `0.2` had not been created. Preserve both the original statement and this
  correction in the freeze manifest.
- Data-owner decision: approve and freeze harmonized schema and variable
  dictionary contract `0.2`, including the reviewed medication zero/null
  semantics and general negative-medication cleaning rule.
- Contract shape: 23 static and 139 dynamic clinical variables; 72 reviewed
  unit updates; nine semantic splits; 34 medication/therapy variables; three
  explicitly unresolved hospital-scoped source-scale variables.
- Medication semantics: preserve observed zero separately from null; prohibit
  null-to-zero and zero-to-null conversion; preserve negatives in harmonized
  data and mask finite negative medication/therapy values only in cleaning.
- Provenance: the freeze records that units are reviewed team-derived
  assumptions because the hospitals supplied no complete unit dictionary.
- Immutability: copy the three reviewed proposal files byte-for-byte into
  versioned contract `0.2`; preserve contract `0.1`, proposal evidence, and all
  harmonized, cleaned, and derived releases unchanged.
- Activation boundary: the metadata freeze authorizes implementation of a new
  harmonized `0.2` candidate. It does not activate conversions, splits, or
  cleaning; read clinical rows; rebuild a release; publish; or authorize
  external export.

## 2026-08-07 — Harmonized 0.2 candidate build and independent audit implemented

- Authorization: contract `0.2` froze successfully with zero blocking or
  technical findings and explicitly authorizes harmonized-build implementation.
- Source: use immutable harmonized release `20260806T111156Z` for every source
  clinical field. Do not use v2, legacy, or the lossy external pooled files.
- Weight reference: for the reviewed UK00 vasopressin conversion only, use
  immutable cleaned-release `20260806T114234Z` static `weight_kg`, because the
  approved unit audit used that exact corrected weight representation. Read no
  cleaned dynamic value.
- Execution: apply all nine hospital-scoped conversions and all nine semantic
  splits under frozen schema `0.2`, including both `d_dimer` hospital rules and
  both `vasopressin_iv_cont` hospital rules. Preserve row order, every input
  column, all five provenance fields, zero, null, and negative medication
  values independently.
- Audit: re-hash immutable inputs and independently recompute every output cell
  using a separate audit implementation. Require exact schema, cell, rule-level
  accounting, weight-linkage, and zero/null/sign conservation.
- Boundary: candidate output is run-scoped and non-publishable. Do not apply
  cleaning policy `0.2`, derivation, filtering, promotion, pointer updates, or
  external export. An exact post-audit human approval remains required.

## 2026-08-07 — Harmonized 0.2 candidate approved for promotion

- Audit: candidate `20260807T112402Z` passed the complete independent audit
  with zero technical blockers over 16,054 static rows, 24,069,379 dynamic
  rows, and 3,466,440,088 output cells.
- Rule accounting: all nine unit conversions and nine semantic splits were
  independently recomputed; non-missing values and explicit zeros were
  conserved; all 201 negative medication values remained present; UK00
  vasopressin had zero missing or invalid static-weight links.
- Human approval: “I approve promotion of harmonized 0.2 candidate
  20260807T112402Z under frozen contract 0.2.” This resolves the sole blocker
  `harmonized_0_2_candidate_release_approved`.
- Promotion representation: copy the audited static and dynamic Parquet bytes
  unchanged into immutable release `harmonized/releases/20260807T112402Z`.
  Preserve the exact prior current-pointer bytes inside the new release before
  atomically advancing `harmonized/current_release.json`.
- Preservation: keep candidate `20260807T112402Z`, harmonized release
  `20260806T111156Z`, cleaned release `20260806T114234Z`, core-derived release
  `20260806T170134Z`, and all audit evidence unchanged.
- Boundary: promotion does not rerun harmonization, apply cleaning policy
  `0.2`, derive features, filter rows or stays, or authorize external export.

## 2026-08-07 — Harmonized contract 0.2 release promoted

- Data-owner approval: promote harmonized candidate `20260807T112402Z` under
  frozen contract `0.2`.
- Promotion result: PASS with zero blocking or technical findings over 16,054
  static rows, 24,069,379 dynamic rows, and 3,466,440,088 compared cells.
- Immutability: candidate Parquet bytes were copied unchanged; harmonized
  release `20260806T111156Z` and every cleaned/derived `0.1` release remain
  preserved. Only the recoverable current-harmonized pointer advanced.
- Boundary: harmonized `0.2` is approved as cleaning `0.2` input. No cleaning,
  derivation, cohorting, time blocking, or external export occurred.

## 2026-08-07 — Consolidated cleaning policy 0.2 review implemented

- Remaining decision: the general negative-medication rule is already human
  approved, but the 12 unit-aware legacy-range dispositions remain proposed
  rather than activated. Their historical evidence totals 10,021 findings.
- Review design: validate the exact current harmonized release, frozen contract
  `0.2`, cleaning policy `0.1`, reviewed unit decisions, and reviewed medication
  semantics without reading a clinical row.
- Complete proposed policy: replay every `0.1` cleaning rule, add the approved
  dictionary-driven negative-medication rule, and activate all 12 unit-aware
  range dispositions while preserving every row, stay, column, and provenance
  field.
- Important accounting boundary: 10,021 is historical old-range evidence, not
  a promised mask count. The exact post-harmonization effect must be recomputed
  independently by the later cleaned-candidate audit.
- Human gate: one explicit approval of the complete policy is required before
  a cleaned `0.2` candidate may be generated. This checkpoint writes sanitized
  reports only and changes no release or clinical artifact.

## 2026-08-07 — Range-direction evidence required before cleaning 0.2 approval

- Review finding: the 12-rule policy review exposed each inherited total but
  did not separate negative, exact-zero, and above-maximum values. Upper bounds
  for INR, lactate, platelets, PTT, EVLWI, GEDVI, and SVRI may capture genuine
  extremes as well as input errors, so the prior report is insufficient for an
  informed masking decision.
- SOFA correction candidate: ordinary SOFA contains six 0–4 components and has
  a maximum of 24. A field that genuinely excludes the GCS/CNS component has a
  structural five-component maximum of 20; the inherited 24-point rule arose
  from a legacy merge and requires separate accounting.
- Approved next action: run one read-only audit over current harmonized release
  `20260807T112402Z`. Count directions, hospitals, fractional scores, and every
  power-of-ten recovery multiplicity; keep exact aggregate outlier frequencies
  owner-only and exclude identifiers and patient rows.
- Boundary: no prior review is rewritten, no range is approved automatically,
  and no value, row, release, or pointer is changed. A revised consolidated
  cleaning-policy approval remains mandatory before candidate generation.

## 2026-08-07 — Final cleaning 0.2 matrix approved

- Approved evidence: complete hospital-level and decision-relevant aggregate
  evidence from range audit `20260807T133609Z`; no patient rows or identifiers
  were included in the review.
- Accounting clarification: the audit's inherited legacy-rule metric is
  10,565 because it includes 544 valid zero-valued SOFA scores. Cleaning `0.2`
  retains those score values. The remaining 10,021 directional findings are
  9,597 invalid zeros, 50 negatives, and 374 positive upper-tail findings.
- Approved conservative rules: mask invalid nonpositive values where reviewed;
  preserve uncertain positive GEDVI, INR, lactate, PTT, and SVRI extremes with
  aggregate reporting; apply a broad 1000 dg/L albumin ceiling; and enforce
  integer SOFA domains of `[0, 24]` and `[0, 20]` respectively.
- Approved recovery: EVLWI above 80 mL/kg is repaired only when exactly one of
  the reviewed power-of-ten factors produces a value in `(0, 80]`; otherwise
  it becomes null.
- Medication policy: finite negative values become null across 25 source
  medication/therapy fields and nine semantic-split targets. Explicit zero
  remains zero, null remains unavailable, and null-to-zero imputation is
  prohibited.
- Implementation authorization: build one non-publishable cleaned `0.2`
  candidate, generate its cleaned variable dictionaries, and run one complete
  independent audit. Promotion remains a separate exact-candidate human gate.

## 2026-08-08 — Cleaned 0.2 candidate approved for promotion

- Candidate `20260807T154100Z` passed its complete independent audit with zero
  technical blockers over 16,054 static rows, 24,069,379 dynamic rows, and
  3,466,440,088 compared output cells.
- Exact effects: 9,740 priority values masked, 32 unique power-of-ten repairs,
  332 uncertain positive upper-tail values preserved, 201 negative medication
  values masked, and 83 albumin values above 1000 dg/L masked. All post-clean
  governed-range checks passed.
- SOFA clarification: 544 valid zero scores remain present. They were included
  only in the inherited legacy-rule evidence count and were never masked by
  cleaning `0.2`.
- Human approval: “i approve promotion of cleaned 0.2 candidate
  20260807T154100Z under cleaning policy 0.2.” This resolves the sole blocker
  `cleaned_0_2_candidate_release_approved`.
- Promotion contract: copy static and dynamic Parquet plus both cleaned
  variable-dictionary artifacts byte-for-byte; preserve the candidate, prior
  cleaned release, and every harmonized and derived release; snapshot the
  previous current-pointer bytes before atomically advancing the pointer.
- Boundary: promotion reruns neither cleaning nor derivation, filters nothing,
  and does not authorize external data export.

## 2026-08-08 — Core-derived 0.2 implementation authorized

- Trigger: cleaned candidate `20260807T154100Z` was promoted successfully under
  cleaning policy `0.2` with zero blocking or technical findings and is the
  approved derivation input.
- Human instruction: after the proposed next step was described, the data
  owner instructed “proceed.” This authorizes a run-scoped candidate and full
  audit, not release promotion.
- Contract inheritance: core-derived `0.2` makes zero clinical-formula, output
  semantic, derived-output addition, or derived-output removal changes from
  approved contract `0.1`.
- Input change: bind to cleaned release `20260807T154100Z`; preserve all 28
  static and 144 dynamic input columns before appending the same nine static
  and three dynamic derived fields.
- Fail-closed evidence: require the prior reviewed formulas, cleaned `0.2`
  promotion policy, current pointer, release manifest, schemas, row counts,
  hashes, and unchanged exact derivation accounting. Any difference blocks.
- Boundary: write only `derived_0_2_candidates` and build/audit reports. Do not
  move the current derived pointer, alter any release, filter data, make a
  cohort or time block, or authorize external export. Exact-candidate human
  approval remains mandatory before promotion.

## 2026-08-08 — Core-derived 0.2 candidate approved for promotion

- Candidate `20260808T074305Z` passed the complete independent audit with zero
  technical blockers over 16,054 static rows, 24,069,379 dynamic rows, and
  3,538,792,711 compared output cells.
- Conservation: every cleaned `0.2` input column was preserved exactly, every
  derived value was independently recomputed, and no row, stay, cohort, or
  time block was filtered or created.
- Clinical accounting remained identical to the approved `0.1` formulas:
  3,636 computed driving pressures masked and flagged, 4,726 hospital deaths,
  2,146 ICU deaths, 53,535 observed-support episodes, and 12,775 stays with an
  episode lasting at least 24 hours.
- Human approval: “I approve promotion of core-derived 0.2 candidate
  20260808T074305Z under core-derived contract 0.2.” This resolves the sole
  blocker `core_derived_candidate_release_approved`.
- Promotion contract: copy both audited Parquet files byte-for-byte, preserve
  the candidate and derived release `20260806T170134Z`, snapshot the previous
  current-pointer bytes, and atomically advance only the current-derived
  pointer.
- Boundary: rerun neither cleaning nor derivation, modify no clinical value,
  create no cohort or time block, and do not authorize external export.

## 2026-08-08 — Consolidated 8-hour time-blocking evidence authorized

- Input: immutable core-derived release `20260808T074305Z`, contract `0.2`,
  with 16,054 static rows and 24,069,379 dynamic rows. The evidence job must
  resolve it through `derived/current_release.json` and revalidate its hashes,
  schema, and lineage.
- Time convention: preserve every negative row in separately named
  pre-admission windows `[-8,0)`, `[-16,-8)`, and so on; reserve `0h` for the
  singleton `{0}`; use right-labelled post-admission intervals `(0,8]`,
  `(8,16]`, and so on.
- Grid: retain empty intervening blocks and terminal partial blocks. Use
  recording-extent-proxy completeness terminology, omit prediction time, and
  expose nominal versus available-through endpoints in any later contract.
- Last observation: use the exact terms `last_observation_value`,
  `last_observation_time_h`, and `last_observation_age_h`; select the final
  non-missing value within a block using reviewed source-order tie-breaking.
- Medication evidence: preserve zero separately from null. Audit apparent
  bolus repetitions and sums, but do not call a sum administered dose until a
  human confirms distinct-administration semantics. Do not integrate continuous
  rates or apply carry-forward.
- Authorization: implement and run one bounded-memory report-only audit under
  two Arrow batch sizes. Produce sanitized aggregate review evidence and an
  owner-only aggregate bundle. Do not write blocked rows, create a candidate or
  release, filter a cohort, modify a pointer or release, or authorize external
  export.
- Next gate: explicit review and freeze or revision of the exact 8-hour
  contract before blocking-engine implementation.

## 2026-08-08 — Time-blocking evidence schema validator corrected

- First production attempt stopped before scanning rows with
  `Core-derived static validation failed: schema_hash`; no blocked data,
  candidate, release, or pointer modification occurred.
- Cause: core-derived manifests hash the reviewed Arrow schema before Parquet
  serialization, while the evidence validator initially hashed the schema
  reconstructed from Parquet. Parquet uses the equivalent nested-list child
  label `element` where the contract constructor uses `item`; Arrow considers
  the types equal, but their serialized schema bytes differ.
- Correction: restore only that Arrow/Parquet list-child representation before
  recomputing the exact manifest digest. Column order, physical types,
  nullability, field metadata, schema metadata, file hashes, row counts, and
  immutable manifest/pointer bindings remain fail-closed.
- Regression coverage now builds static and dynamic manifests from the
  pre-Parquet contract schemas, including variable- and fixed-size-list fields.
  This does not alter the approved time policy, aggregation proposal, audit
  scope, or production data.

## 2026-08-08 — Time-blocking source-order invariant corrected

- Evidence run `20260808T110427Z` scanned all 24,069,379 dynamic rows under two
  batch sizes and reproduced identical assignments, but correctly remained
  failed because `source_order_is_deterministic` did not satisfy the audit's
  initial global-sequence assertion.
- Root cause is a validator-scope mismatch: ingestion defines
  `__v3_source_order` as a contiguous sequence starting at one independently
  for each hospital/table, and harmonization concatenates hospitals while
  preserving those provenance values. It is neither zero-based nor globally
  unique across the pooled dynamic table.
- Corrected invariant: require all tie-break provenance to be non-null and
  require `__v3_source_order == 1..N` independently within every hospital's
  dynamic rows. Hospital-boundary resets are expected. The sum of hospital
  terminal values must equal the conserved dynamic row count.
- Last-observation implication: stays do not cross hospitals, so elapsed time
  followed by hospital-scoped source order deterministically orders tied rows
  within a stay. File order and source row number remain required provenance;
  no arbitrary row-offset fallback is approved.
- A fresh complete evidence run is required. Run `20260808T110427Z` is retained
  as failed provenance and cannot support contract freeze or implementation.

## 2026-08-08 — Time-blocking evidence unit display corrected

- Evidence run `20260808T113410Z` passed every technical invariant: exact
  release/hash binding, complete row assignment, stay conservation,
  hospital-scoped deterministic ordering, recording-extent reproduction,
  schema/registry agreement, cross-batch reproducibility, and unchanged source
  bytes.
- Review identified a report-only metadata lookup defect before contract
  freeze: profiles read field metadata key `unit`, while the frozen harmonized,
  cleaned, and derived contracts use `asic_v3_unit`. Consequently the report
  displayed `unavailable` even though units remained present and hash-bound in
  the approved schemas.
- Correct the lookup to `asic_v3_unit` and add regression coverage. Preserve
  run `20260808T113410Z` unchanged as technically passing but superseded review
  evidence; require one fresh immutable evidence report before contract freeze.
- This correction changes no schema, clinical value, eligibility decision,
  time assignment, aggregation proposal, source artifact, or release pointer.

## 2026-08-08 — 8-hour time-blocking contract 0.1 frozen for local implementation

- Approval: “I approve the ASIC v3 8-hour time-blocking contract using
  evidence run 20260808T114902Z, with medication dose totals deferred, and
  authorize local implementation and tests only.”
- Evidence binding: run `20260808T114902Z` passed all 14 technical invariants
  over 16,054 stays and 24,069,379 dynamic rows. It assigned all 247,977
  negative rows, retained 10,348 empty grid blocks and 14,855 terminal partial
  blocks, and reproduced assignments across different Arrow batch sizes.
- Frozen time index: pre-admission `[-8,0)`, `[-16,-8)`, and so on; admission
  singleton `{0}`; post-admission `(0,8]`, `(8,16]`, and so on. Retain every
  source row, each stay's admission row, empty intervening blocks, and the
  terminal partial block.
- Frozen aggregation: use explicit registry families and canonical
  `last_observation_value`, `last_observation_time_h`, and
  `last_observation_age_h`. Preserve null, observed zero, and observed false as
  distinct states. Apply no carry-forward, imputation, interpolation, cohort,
  or row filtering.
- Medication boundary: do not emit dose totals, administration counts, or
  time-weighted exposure. Apparent repeated bolus rows do not prove distinct
  administrations.
- Static policy: reference the exact core-derived static table through
  manifest lineage; neither copy it nor repeat static values on blocks.
- Implementation: add a policy-driven, stay-bounded Arrow/Parquet engine and a
  separate direct every-cell audit. Bind the blocked dictionary to the frozen
  harmonized `0.2` definitions and derived-field metadata. Verify output
  identity under different input and output batch sizes.
- Authorization boundary: local demo implementation and tests only. Production
  candidate generation, production audit, Slurm execution, release promotion,
  pointer modification, core-derived modification, and external export remain
  unauthorized and fail closed.

## 2026-08-08 — Production time-blocking workflow implementation authorized

- Approval: “I authorize implementation of the run-scoped ASIC v3 8-hour
  production candidate and independent audit workflow. Do not run it, promote
  a release, or modify any pointer.”
- Contract preservation: frozen time-blocking contract `0.1` and its evidence
  binding remain byte-identical. A separate workflow policy records this later
  implementation authorization.
- Implementation: add one self-submitting Slurm workflow for an immutable
  candidate followed by a separate direct every-cell audit. It requires exact
  candidate and audit run IDs, refuses every existing target, and verifies
  that the core-derived and blocked current-pointer states remain unchanged.
- Execution gate: production policy
  `production_candidate_audit_8h.yaml` has candidate and audit execution set to
  false and references the deliberately absent
  `reviewed_8h_production_execution.yaml`. CLI and Slurm paths fail before
  reading production evidence or clinical data while that file is absent.
- Future authorization: one exact-run approval must bind both run IDs, the
  frozen contract hash, the workflow-policy hash, and core-derived release
  `20260808T074305Z`. It may authorize only candidate construction and the
  independent audit.
- Prohibitions: no production run occurred; no candidate, release, pointer,
  cohort, carry-forward, imputation, medication dose total, or external export
  was created or authorized.

## 2026-08-08 — Exact operator-run production candidate and audit authorized

- Clarification: “Do not run it” prohibits the assistant from accessing or
  executing on the password-protected cluster; it does not prohibit preparing
  the reviewed workflow and operator deployment instructions.
- Exact scope: candidate and independent audit run ID `20260808T130534Z`, both
  bound to core-derived release `20260808T074305Z`, frozen time-blocking
  contract `0.1`, and the production workflow-policy hash.
- Authorized executor: the data owner may deploy and submit the reviewed Slurm
  workflow. The assistant must not connect to or execute on the cluster.
- Still prohibited: release promotion, any current-pointer change,
  core-derived modification, filtering, cohort creation, carry-forward,
  imputation, medication dose totals, and external export.

## 2026-08-08 — Time-blocking provenance-definition lookup corrected

- First exact-run build `20260808T130534Z` stopped with `Source definition is
  unavailable: __v3_source_file_id` before creating a candidate directory,
  build report, release, or pointer.
- Cause: the frozen harmonized dictionary contains clinical variables while
  the five `__v3_source_*` operational provenance columns are appended by the
  reviewed ingestion/harmonization contract and intentionally are not clinical
  dictionary rows.
- Correction: define those five fields explicitly from their authoritative v3
  generation semantics: inventory file identity, one-based hospital/table file
  order, CSV record ordinal, one-based hospital/table row sequence, and
  inventory schema-variant identity. Do not introduce a generic fallback.
- Regression: synthetic source dictionaries now also omit all five operational
  fields, proving that dictionary construction and independent reconstruction
  use the same explicit definitions.
- Rerun boundary: reuse exact approved run ID `20260808T130534Z` only because
  the failure occurred before its immutable candidate/report targets were
  created. Promotion and pointer modification remain prohibited.

## 2026-08-08 — 8-hour time-blocking candidate approved for promotion

- Complete candidate/audit run `20260808T130534Z` conserved 16,054 stays and
  all 24,069,379 source rows, independently compared 777,756 block rows and
  825,976,872 feature cells, and reported zero technical blockers.
- Exact candidate-manifest binding:
  `c7436c8b624e5245ceef49500c4d952798ac5d05ca110393cfdd2c8d8e790882`.
- Approval: “I approve promotion of ASIC v3 8-hour time-blocking candidate
  20260808T130534Z under time-blocking contract 0.1 as immutable release
  20260808T130534Z, and authorize operator-executed promotion and atomic
  creation of only the 8-hour time-blocking current-release pointer. External
  export remains unauthorized.”
- Promotion is byte-preserving: copy the four audited candidate payloads and
  candidate manifest without rerunning time blocking, then create a release
  manifest and atomically create only
  `derived/time_blocking/8h/current_release.json`.
- The initial nested pointer must be absent. Any existing release, staging
  target, promotion report, or pointer blocks execution rather than being
  overwritten.
- The core-derived release and `derived/current_release.json` must remain
  byte-identical. Static data remains a lineage reference and is not copied or
  repeated.
- The release is approved as internal analysis input. No row/stay filtering,
  cohort generation, carry-forward, imputation, medication dose total, or
  external export is authorized.
- The assistant may implement and test locally and provide operator commands,
  but must not connect to or run on the password-protected cluster.

## 2026-08-08 — Time-blocking promotion Parquet-footer OOM corrected

- Operator-submitted promotion job `2847294` was killed by Slurm for exceeding
  its original 2 GiB allocation while executing the promotion command. A
  read-only target-state check is required before retrying; no assumption is
  made here about whether an incomplete staging target remains on the cluster.
- Cause: promotion reopened the extremely wide `blocks.parquet` footer to
  recompute row-count and schema metadata. Its footer scales with Parquet row
  groups multiplied by output columns and is not a bounded-memory operation at
  this width.
- Correction: promotion continues to hash each payload with a bounded streaming
  SHA-256 pass, validates the exact data-owner-approved candidate-manifest hash,
  and uses the row counts and schema hashes cryptographically bound into that
  independently audited manifest. It no longer reopens candidate Parquet
  metadata.
- Defense in depth: the promotion allocation is raised from 2 GiB to 4 GiB.
  This does not authorize a new recipe, candidate, release ID, pointer, export,
  or transformation; the existing approval and byte-preserving promotion
  boundary remain unchanged.
- Regression coverage fails if promotion attempts to reopen candidate Parquet
  metadata. The complete local suite passes after the correction.
