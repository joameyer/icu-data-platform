# Candidate harmonization dry run

## Purpose

This stage is the first production-scale execution of harmonization. It turns
the eight immutable per-hospital ingestion outputs into isolated candidate
harmonized Parquet files so mappings, parsers, types, units, categorical
domains, availability, and distributions can be reviewed together.

It does not create the approved harmonized release.

## Inputs

The command is bound to the reviewed inventory, ingestion, schema-token,
registry-review, composite-source, static-decision, and ICD-10 evidence listed
in `asic/config/harmonization/dry_run.yaml`. Decision files are SHA-256 bound.
Every ingested Parquet is re-hashed against its hospital ingestion manifest
before transformation.

## Candidate transformations

The dry run applies deterministic direct-numeric, decimal-comma, ratio,
explicit-percentage, threshold-boundary, reviewed numeric-list, missing-token,
categorical, identifier, composite-source, and ICD-10 operations. It preserves
temporal source strings when their parser remains undecided. The reviewed UK00
tidal-volume semantic split is represented separately as
`vt_per_ideal_bw_total`.

The reviewed UK00 height source is parsed as an ordered nullable
`list<double>` in centimetres. Approved `nan` list elements are omitted while
the remaining measurements, their order, and duplicates are preserved; no
physiologic range cleaning or per-stay aggregation occurs here. The reviewed
UK03 study-phase vocabulary maps `K` to `calibration`, `RI` to `roll_in`, and
`QS` to `app_implementation`; its reviewed `nan` spelling becomes null. This
hospital-scoped mapping is supported by the phase-specific
time-since-study-start ordering and does not alter other categorical fields.

Static local stay identifiers come from the reviewed raw `Pseudo-ID`/
`PseudoID` occurrence. Dynamic local stay identifiers instead come from the
required lossless-ingestion filename provenance. This reflects the production
layout, in which seven dynamic hospital tables have no raw identifier column.
Available in-file identifiers must agree with the filename-derived value. The
UK04 raw dynamic identifier is retained as corroborating evidence and must
agree with the ingested in-file value; it is not silently preferred. Every
dynamic identifier must resolve to its hospital's reviewed static stay set.
Both tables derive `stay_id_global` as
`<stay_id_local>:<approved hospital suffix>`.

No distribution-inferred unit conversion, physiologic range masking,
individual scale-error repair, row filtering, stay filtering, ventilation
cohort, cleaning, or derivation is applied.

## Outputs and isolation

Each invocation receives a unique run ID and writes:

`asic/data/<context>/harmonization_candidates/<run_id>/asic_UKNN/static.parquet`

`asic/data/<context>/harmonization_candidates/<run_id>/asic_UKNN/dynamic.parquet`

All hospitals use identical ordered schemas for a given table. Hospital-source
absence is represented only after the common candidate schema is constructed.
The five lossless source-provenance fields remain row-aligned in every output.
Each hospital and the complete run receive hash- and row-count-bearing
manifests. Those manifests also record sanitized identifier-source and
corroboration counts without storing identifier values.

Exact raw names and token/domain evidence remain in the owner-only private
report bundle. The sanitized review report contains counts and canonical names
only.

The command refuses to overwrite an existing run ID and never writes under
`ingested/`, `cleaned/`, `derived/`, or either frozen project.

## Human review gate

The expected dry-run result is `FAIL` with human-review findings but no
technical failures. That means candidate files were generated successfully and
remain non-publishable while pending mappings, types, parser policies, units,
semantic questions, and categorical domains are reviewed in one consolidated
gate.

After approval, a separate step will freeze the variable dictionary and ordered
union schemas, audit row/schema/hash conservation, and promote the reviewed
candidate to the harmonized release. Cleaning begins only after that promotion.
