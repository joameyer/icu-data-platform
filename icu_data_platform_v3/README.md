# ICU Data Platform v3

This project owns dataset-isolated, provenance-preserving ICU data pipelines.
The established `asic/` layer uses raw hospital CSV files as its authoritative
input boundary. The new `mimic/` sibling defines a narrow MIMIC-IV 3.1
selected-variable layer for the phase-aware mortality analysis; its current
authorization is an operator-executed, run-scoped production candidate and
independent audit on the protected cluster, with no release or pointer change.

The project is deliberately separate from the frozen references:

- `../hpc-icu-data-platform/` — legacy knowledge, read-only;
- `../icu-data-platform-v2/` — pooled-input implementation, read-only.

The cluster deployment target is
`/hpcwork/jrc_combine/joana/icu_data_platform_v3`. The existing deployment at
`/hpcwork/jrc_combine/joana/icu_data_platform` is v2 and must never be
overwritten by this project.

## Current phase

The immutable harmonized (`20260806T111156Z`), cleaned
(`20260806T114234Z`), and core-derived (`20260806T170134Z`) production releases
have passed their complete audits and explicit promotion gates. The current
follow-up is a report-only resolution of the 72 variables whose frozen
harmonized unit remains unresolved. Twelve of those variables also have legacy
range rules; their 10,021 currently out-of-range values remain preserved until
unit-aware decisions are approved. See
[the consolidated unit-resolution audit](asic/docs/unit_resolution_audit.md)
and [the complete candidate unit decisions](asic/docs/unit_resolution_decisions.md).
The next checkpoint is the
[complete candidate unit-decision audit](asic/docs/unit_decision_audit.md),
which tests every proposed conversion and mask without modifying a release.
That audit has now passed and its human gate is resolved: all nine conversions
are approved, while the nine proposed masks are superseded by value-preserving
semantic splits. Exact amendment review `20260807T101321Z`, generated at
`2026-08-07T10:14:15+00:00`, is approved for
immutable schema/dictionary contract `0.2`. That metadata freeze has now
passed with zero blockers. The next bounded step is the run-scoped
[harmonized 0.2 build and independent audit](asic/docs/harmonized_0_2_build.md),
which activates only the frozen conversions and semantic splits and stops at a
new explicit release gate. Candidate `20260807T112402Z` has passed that audit
and the data owner has approved its byte-preserving promotion. The promotion
implementation preserves the previous pointer bytes and all prior releases;
production execution remains the next checkpoint.

Medication zero and missing semantics are explicit: observed numeric zero is
preserved, null is never assumed to mean zero or inactive treatment, and
carry-forward requires a separate reviewed derivation contract. A complete
aggregate audit covers all medication and therapy fields before contract `0.2`
is frozen. See
[the medication value-semantics review](asic/docs/medication_value_semantics.md).
The reviewed general cleaning rule preserves negatives in harmonized data and
masks finite medication/therapy values below zero only in the cleaned layer;
its current audited effect is 201 UK08 `inhaled_no` values.
See [the contract 0.2 freeze](asic/docs/unit_schema_dictionary_freeze_0_2.md).

### Important unit-provenance warning

The ASIC hospitals did not provide a complete source unit dictionary. The
candidate `0.2` units are assigned by the data owner and pipeline team from a
prior ASIC analysis configuration, canonical variable definitions, clinical
experience, and complete aggregate raw-v3 cross-hospital evidence. They are
explicit reviewed assumptions, not hospital-supplied metadata. The v3-owned
decision register records the evidence basis, confidence, conversion, mask,
and caveat for every formerly unresolved variable; v3 has no runtime
dependency on the older analysis repository.

Phase 1 implements read-only raw-file inventory evidence. Phase 2 implements
the reviewed lossless per-hospital ingestion engine. The all-hospital cluster
run was reported technically successful on 2026-08-05; production artifacts
remain on the authorized cluster and are not present in this repository. The
new post-ingestion audit revalidates those artifacts before any parsing work.
Neither command parses, harmonizes, cleans, derives, concatenates, or publishes
data. The approved folder-to-hospital contract includes raw folder `01` as
`asic_UK01`, with cohort action `include`.

The raw-schema/token inventory, candidate-registry review, reviewed composite
audit, and bounded static-variable evidence audits are implemented. A
separately approved streaming dry run can now apply the accumulated rules to
all eight ingested hospitals and write run-scoped candidate harmonized Parquet.
Those files are deliberately non-publishable: the consolidated profiles,
exceptions, ordered schemas, and variable dictionary still require human
review before a harmonized release can be frozen.

The consolidated harmonization audit now implements that review as one
production-scale, report-only job. It scans all unit policies and all frozen
legacy QC domains through a v3-owned, selectively migrated policy and verifies
candidate/ingestion conservation. It requires no older project at runtime.
Optional older-version comparisons are disabled by default and can never be
authoritative for v3.

Consolidated decisions `0.1` now records the approved units, four
hospital-scoped conversions, semantic separations, all-missing-column policy,
and harmonized-versus-cleaned boundary from audit `20260806T092818Z`. It is a
partial executable contract: it may be applied only when building a new
candidate and cannot mutate the reviewed candidate. Complete categorical
contract `0.1` now resolves all reviewed domains, including fail-closed
preservation of UK00 ARDS codes and numeric `position_therapy`. Ordered
schema/dictionary freezing and publication remain closed. The final
schema/dictionary checkpoint reuses immutable audit workbooks without rescanning
candidate rows.

A read-only remaining-static review re-aggregates that immutable evidence for
`icu_los`, `dialysis_free_days`, `vent_free_days`, and `icd10_codes`. It emits
sanitized per-hospital counts and proposals only; human approval remains
required before the partial executable registry is extended.

The current evidence-bound partial static registry covers UK00 reported
hospital mortality, UK00 decimal-comma weight, UK00 textual-missing hospital
LOS, and local/global stay-ID construction under audit run
`20260805T112746Z`. Registry `0.3` additionally covers hospital-scoped ICU LOS,
dialysis-free days, and ventilator-free days. These rules are executable in
synthetic tests and in the explicitly isolated candidate dry run. Release,
cleaning, derivation, and publication gates remain closed.

ICD-10 remains separately gated: notation and component audits established the
source formats. Reviewed contract `0.2` now enables a bounded Arrow parser with
exact source-text preservation, trailing-delimiter removal, ordered
deduplication, and private-evidence-bound incomplete-component handling.
The reviewed parser may be used by the isolated candidate dry run; a released
harmonized artifact remains disabled.

The read-only static-registry completion audit now accounts for every static
raw-column occurrence against all approved decision sources and produces an
owner-only coverage workbook, draft variable dictionary, and proposed ordered
static schema. These are review artifacts only. Pending v2/legacy candidate
mappings, dictionary approval, and schema freezing still block release of the
candidate as approved harmonized data.

Inventory remains fail-closed. The exact size/SHA-matched ZIP was confirmed by
the data owner as an unintended old snapshot and remains excluded from every
authoritative input. Historical reports retain the provisional finding that
was correct when they were generated; current review policy records its
resolution. Every unreviewed inventory blocker prevents ingestion.

Ingestion contract `0.2` gives the two reviewed UK00 positional ARDS source
occurrences unique physical names (`ARDS_Diagnose_App_col1` and
`ARDS_Diagnose_App_col2`) so standard PyArrow and Pandas readers can open the
Parquet file. Exact raw-name provenance remains embedded in the artifact.

## Local verification

Use an environment containing the dependencies declared in `pyproject.toml`:

```bash
PYTHONDONTWRITEBYTECODE=1 python -m pytest -p no:cacheprovider -q
```

Synthetic tests write only under pytest temporary directories. They never read
the production cluster source.

## Review sequence

1. Review [the Phase 1 inventory design](asic/docs/raw_inventory.md).
2. Review [the raw-anomaly audit](asic/docs/raw_anomaly_audit.md).
3. Review [the lossless ingestion contract](asic/docs/lossless_ingestion.md).
4. Review [the post-ingestion audit](asic/docs/ingestion_audit.md).
5. Review [the harmonized and cleaned contracts](asic/docs/harmonization.md).
6. Review [the schema/token inventory contract](asic/docs/schema_token_inventory.md).
7. Review [the first static contract audit](asic/docs/static_contract_audit.md).
8. Review [the remaining-static contract review](asic/docs/static_completion_review.md).
9. Review [the ICD-10 notation audit](asic/docs/icd10_notation_audit.md).
10. Review [the ICD-10 component audit](asic/docs/icd10_component_audit.md).
11. Review [the ICD-10 component-detail audit](asic/docs/icd10_component_detail_audit.md).
12. Review [the static-registry completion audit](asic/docs/static_registry_completion_audit.md).
13. Review [the candidate harmonization dry run](asic/docs/harmonization_dry_run.md).
14. Review [the consolidated harmonization audit](asic/docs/consolidated_harmonization_audit.md).
15. Review [the categorical contract checkpoint](asic/docs/categorical_contract_review.md).
16. Review [the ordered schema and dictionary checkpoint](asic/docs/schema_dictionary_review.md).
17. Apply [the explicitly approved schema and dictionary freeze](asic/docs/schema_dictionary_freeze.md).
18. Build and completely audit [the frozen-contract harmonized candidate](asic/docs/harmonized_build.md).
19. Promote the explicitly approved, byte-identical harmonized release using
    the same documented frozen-contract workflow.
20. Build and completely audit [the run-scoped cleaned candidate](asic/docs/cleaning.md).
21. Review [the consolidated unresolved-unit and legacy-range evidence](asic/docs/unit_resolution_audit.md).
22. Review [the complete candidate unit decisions](asic/docs/unit_resolution_decisions.md).
23. Run and review [the complete candidate unit-decision audit](asic/docs/unit_decision_audit.md).
24. Review [the ordered schema/dictionary 0.2 amendment](asic/docs/unit_schema_dictionary_amendment.md).
25. Run and review [the medication zero/missing audit](asic/docs/medication_value_semantics.md), which also regenerates the revised schema/dictionary proposal.
26. Review the complete [cleaning policy 0.2 checkpoint](asic/docs/cleaning_0_2_policy_review.md) before rebuilding cleaned data from harmonized contract `0.2`.
27. Before approving its range block, run the consolidated [cleaning 0.2 range-direction audit](asic/docs/cleaning_range_evidence_0_2.md).
28. Apply [the explicitly approved contract 0.2 metadata freeze](asic/docs/unit_schema_dictionary_freeze_0_2.md).
29. Build and independently audit [the harmonized 0.2 candidate](asic/docs/harmonized_0_2_build.md).
30. Build, independently audit, and—after exact human approval—promote [the cleaned 0.2 candidate and cleaned variable dictionary](asic/docs/cleaning_0_2.md).
31. Rebuild and independently audit [core-derived 0.2 from the promoted cleaned 0.2 release](asic/docs/derivation.md).
32. Review [protected-data handling](asic/docs/protected_data.md).
33. Inspect sanitized reports and owner-only cluster evidence.

Translation is retained as an auditable operation inside harmonization; no
separate translated dataset will be published. The candidate dry run is
implementation evidence, not a release. Each later gate requires an explicit
decision.
