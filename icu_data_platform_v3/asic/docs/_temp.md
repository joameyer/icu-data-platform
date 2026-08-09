Recommendation: do not port the legacy implementation. Build a standalone, resolution-general Arrow/Parquet recipe in v3, with an explicitly reviewed `8h` policy. It should reference the immutable core-derived 0.2 release, preserve every non-negative source row in an observed block—including the terminal proxy-partial block—account for negative rows separately, and never apply cohort filtering, carry-forward, or implicit numeric coercion.

This was a read-only local audit. I did not access the production cluster, so the supplied release ID, hashes, and counts remain authoritative inputs to be independently validated at the next gate. No repository files, data, jobs, pointers, or directories were changed.

## 1. Exact legacy-code inventory

### Executable path

The production entry path is:

`run_blocking.sh` → `run_asic_standardized_from_harmonized.py` → CLI `main()` → `build_and_write_asic_standardized_dataset_from_harmonized_outputs()` → generic `build_asic_8h_blocks()`.

Evidence:

- [run_blocking.sh](/Users/joanameyer/repository/icu-data-platform/hpc-icu-data-platform/run_blocking.sh:1): 24 hours, 8 CPUs, 128 GB; CSV only; default chunk size 250,000.
- [run_asic_standardized_from_harmonized.py](/Users/joanameyer/repository/icu-data-platform/hpc-icu-data-platform/run_asic_standardized_from_harmonized.py:1): thin launcher.
- [build_asic_standardized_from_harmonized.py](/Users/joanameyer/repository/icu-data-platform/hpc-icu-data-platform/src/icu_data_platform/pipelines/build_asic_standardized_from_harmonized.py:12): CLI parser and `main()`.
- [pipeline.py](/Users/joanameyer/repository/icu-data-platform/hpc-icu-data-platform/src/icu_data_platform/sources/asic/pipeline.py:449): executable per-hospital CSV pipeline.

### Blocking modules

[blocking.py](/Users/joanameyer/repository/icu-data-platform/hpc-icu-data-platform/src/icu_data_platform/sources/asic/blocking.py:8) is the generic executable implementation:

- Constants and column registries: `BLOCK_SIZE_HOURS`, block/stay/QC/output columns.
- Result: `ASICBlockResult`.
- Construction: `_prepare_stays`, `_dynamic_input`, `_build_block_index`, `_build_blocked_dynamic_features`.
- QC/validation: `_build_negative_dynamic_time_qc`, `_build_validation_table`, `_validate_block_index`, `_build_block_count_distribution_by_hospital`, `_select_example_stays`, `_build_qc_summary`.
- Public entry points: `build_asic_8h_example_stays`, `build_asic_8h_qc_summary`, `build_asic_8h_blocks`.

[blocks.py](/Users/joanameyer/repository/icu-data-platform/hpc-icu-data-platform/src/icu_data_platform/sources/asic/blocks.py:8) is the older Chapter-1 counterpart:

- `ASICChapter1BlockResult`.
- Near-duplicate helpers and aggregation logic.
- Takes a filtered Chapter-1 cohort through `_prepare_retained_stays`.
- Public entry point `build_asic_chapter1_8h_blocks`.

### Stay endpoint and cohort code

[stay_level.py](/Users/joanameyer/repository/icu-data-platform/hpc-icu-data-platform/src/icu_data_platform/sources/asic/stay_level.py:34):

- `build_asic_dynamic_end_time_proxy`: maximum parsed dynamic time per stay.
- `_authoritative_static_stay_level_input`.
- `_build_preprocessing_notes`, summaries, coding distributions.
- `build_asic_stay_level_table_from_dynamic_end_time_proxy`.
- `build_asic_stay_level_table`.

[cohort.py](/Users/joanameyer/repository/icu-data-platform/hpc-icu-data-platform/src/icu_data_platform/sources/asic/cohort.py:494):

- Separate Chapter-1 site eligibility, vital-group coverage, mortality availability, stay exclusions, retained hospitals/stays.
- `_build_chapter1_stay_exclusions` excludes site-ineligible stays, stays without dynamic data, missing readmission status, and readmissions.
- `build_asic_chapter1_cohort` and `build_asic_stay_level_cohort`.

[mech_ventilation.py](/Users/joanameyer/repository/icu-data-platform/hpc-icu-data-platform/src/icu_data_platform/sources/asic/qc/mech_ventilation.py:345):

- Separate observed-support QC deriving episodes and an observed ≥24-hour flag.
- It is not called by the generic blocking function as a filter.
- It documents that it uses direct timestamps, not blocked data.

### Streaming and output code

[pipeline.py](/Users/joanameyer/repository/icu-data-platform/hpc-icu-data-platform/src/icu_data_platform/sources/asic/pipeline.py:331):

- `_partition_harmonized_dynamic_csv_by_hospital`.
- `_dynamic_end_time_proxy_from_lookup`.
- `_cohort_output_paths`, `_blocked_output_paths`.
- `_unlink_if_exists`.
- Generic and Chapter-1 builders/writers.

[common/io.py](/Users/joanameyer/repository/icu-data-platform/hpc-icu-data-platform/src/icu_data_platform/common/io.py:9):

- `ensure_directory`, `prepare_dataframe_for_write`, `write_dataframe`, `read_dataframe`, `append_dataframe_csv`.
- Writes CSV or Parquet in general, although the executable phase-2 CLI restricts both input and output to CSV.

### Generic outputs

Under `cohort/`:

- `stay_level.csv`
- `summary.csv`
- `preprocessing_notes.csv`
- `icu_end_time_proxy_summary_by_hospital.csv`
- `coding_distribution_by_hospital.csv`

Under `blocked/`:

- `asic_8h_block_index.csv`
- `asic_8h_blocked_dynamic_features.csv`
- `asic_8h_stay_block_counts.csv`
- `asic_8h_block_count_distribution_by_hospital.csv`
- `asic_8h_negative_dynamic_time_qc.csv`
- `asic_8h_qc_summary.csv`
- `asic_8h_example_stays.csv`

The Chapter-1 writer produces the analogous `chapter1_*` artifacts under `blocks/`, plus detailed cohort exclusion artifacts.

### Dependencies and state behavior

- Pandas DataFrames and CSV files are the computational/storage model.
- `TemporaryDirectory` holds per-hospital protected dynamic CSV partitions.
- Existing blocked feature output is explicitly unlinked before rebuilding.
- No input manifest validation, immutable candidate directory, schema/dictionary freeze, file hashes, independent audit, or atomic current-release pointer exists.
- No relevant legacy unit or integration tests were found.

## 2. Reconstructed legacy contract

### Confirmed executable behavior

| Concern | Confirmed behavior |
|---|---|
| Resolution | Hard-coded 8 hours. |
| Origin | Requires legacy `icu_admission_time == 0`; block 0 starts at ICU-relative hour 0. |
| Intervals | Effective half-open intervals: `[0,8)`, `[8,16)`, etc. |
| Exact boundary | `floor(time_h / 8)`: hour 8 maps to block 1. |
| Block index | Zero-based integer. |
| Prediction time | `prediction_time_h == block_end_h`. |
| Endpoint | Maximum parsed dynamic `time` per stay, named `icu_end_time_proxy`. It is recording extent, not discharge. |
| Number of blocks | `floor(icu_end_time_proxy_hours / 8)`. |
| Terminal partial | Discarded entirely. |
| Exact endpoint row | At an endpoint such as hour 8, the row maps to the next block, which does not exist unless another full block is completed. It is therefore dropped when extent is exactly 8 hours. |
| Negative time | Excluded from assignment; reported separately. |
| Invalid/unparsed time | Not assignable and not included in negative-time QC; there is no complete conservation equation. |
| Empty blocks | Completed grid blocks are left-joined to aggregates, so gaps appear with row/count fields zero and summaries null. |
| Features | Every dynamic column except listed identifiers/time/order columns is treated as a feature. |
| Coercion | Every feature is passed through `pd.to_numeric(errors="coerce")`. |
| Aggregations | `<variable>_obs_count`, `_mean`, `_median`, `_min`, `_max`, `_last`. |
| Last | Rows are sorted by hospital, stay, block, time, then `source_row_order`; Pandas `groupby.last()` returns the last non-missing value separately for each column. It is not a last-row snapshot. |
| Missingness | Numeric zero is observed; nonnumeric values become missing; null feature counts are zero; aggregate values remain null. |
| Block accounting | `dynamic_row_count`, total numeric non-missing cells, and number of features with at least one numeric value. |
| Output order | Hospital, stay, block. |
| Static behavior | Legacy stay-level attributes are kept separately; they are not repeated on each block. |

The core calculations are visible in [blocking.py](/Users/joanameyer/repository/icu-data-platform/hpc-icu-data-platform/src/icu_data_platform/sources/asic/blocking.py:203) and the universal aggregation in [blocking.py](/Users/joanameyer/repository/icu-data-platform/hpc-icu-data-platform/src/icu_data_platform/sources/asic/blocking.py:255).

### Row-loss consequence

For an endpoint `E`:

- Emitted blocks are indices `0` through `floor(E/8)-1`.
- Non-negative rows whose assigned index is not in that set are discarded.
- If `8 < E < 16`, every row from hour 8 through `E` is discarded.
- If `E == 8`, the hour-8 row is discarded.
- If `E == 0`, no block is emitted and the hour-0 row is discarded.

This is more than “discarding a partial block”: exact-boundary observations can also be lost.

### Cohort dependency

The executed `run_blocking.sh` path uses generic `blocking.py`; it does not call Chapter-1 site/stay exclusions and does not directly apply the ≥24-hour ventilation QC.

However:

- Legacy preprocessing notes say adult eligibility and ≥24-hour ventilation eligibility are inherited from the supplied source cohort.
- Therefore generic blocking avoids a new explicit Chapter-1 filter but still operates inside an already cohort-restricted legacy source boundary.
- The Chapter-1 path remains a separate callable and applies explicit site, readmission, and data-availability exclusions.

The v3 recipe must inherit neither boundary.

### Memory behavior

The monolithic builders materialize all input in Pandas. The production wrapper improves this only partially:

1. Read dynamic CSV in 250,000-row chunks.
2. Filter to static stay IDs.
3. Write complete rows into temporary per-hospital CSV files.
4. Load one entire hospital into Pandas.
5. Create copies for parsing, assignment, sorting, coercion, grouping, and wide aggregation.
6. Retain several combined QC/index tables in memory.

Thus memory is bounded by the largest hospital, not by an Arrow batch. The 128 GB Slurm request and legacy README’s “per-hospital” workaround corroborate this design.

### Privacy behavior

The negative-time QC includes:

- `stay_id_global`
- `hospital_id`
- minimum/maximum negative time
- raw `example_negative_times`

The example-stay report includes stay IDs, recording endpoints, and block boundaries. These are protected row/stay examples and are unsuitable for a publishable v3 review report.

### Confirmed behavior versus inferred intent

Confirmed code behavior is everything above.

Inferred intent, not an approved contract:

- “Completed” appears intended to mean a fully elapsed ICU block, but the endpoint is only maximum observed recording time.
- `prediction_time_h` appears intended for prediction modeling, although the generic recipe has no prediction target or eligibility definition.
- QC text treats negative time as invalid; current v3 intentionally preserves it.
- V2 intended time blocking to be optional and independently generated by resolution, but supplied no executable aggregation contract. See [v2 data README](/Users/joanameyer/repository/icu-data-platform/icu-data-platform-v2/asic/data/README.md:129) and [cleaned-to-derived design](/Users/joanameyer/repository/icu-data-platform/icu-data-platform-v2/asic/docs/cleaned_to_derived.md:16).

## 3. Migration matrix

| Component | Classification | V3 treatment |
|---|---|---|
| ICU-relative fixed blocks | Adapt | General resolution policy; freeze exact 8h rules. |
| Half-open assignment | Adapt | Keep, but calculate from exact minutes to avoid float-boundary ambiguity. |
| Zero-based index | Reuse concept only | Retain subject to approval. |
| `prediction_time_h` | Revalidate against core-derived 0.2 | Do not inherit automatically. |
| Legacy completed-only grid | Retire | It loses partial and boundary rows. |
| Empty intervening blocks | Adapt | Retain with explicit source-row and variable counts. |
| Universal numeric aggregation | Retire | Replace with a complete reviewed registry. |
| Pandas `last` | Rewrite | Deterministic last-nonmissing contract with tie-breaking and age. |
| `icu_end_time_proxy` construction | Adapt | Use/revalidate `icu_recording_extent_hours`; name it as a proxy. |
| Legacy `icu_admission_time` | Retire | V3 already supplies exact ICU-relative minutes/hours and artificial time. |
| Generic `blocking.py` | Rewrite | No runtime import or code copy. |
| Chapter-1 `blocks.py` | Retire | No cohort-specific blocking implementation. |
| Chapter-1 cohort/site exclusions | Retire | Explicitly prohibited. |
| Legacy ≥24h ventilation QC | Documentation reference only | V3 static proxy remains descriptive, never a filter. |
| Per-hospital CSV staging | Retire | Arrow/Parquet batch streaming with bounded spill only if required. |
| Legacy QC/example outputs | Retire | Aggregate sanitized reports; owner-only evidence separately. |
| Legacy CSV writers/overwrite behavior | Retire | Immutable run-scoped candidates and byte-preserving promotion. |
| V2 resolution directory idea | Adapt | Preserve resolution independence within v3 release conventions. |
| V2 documentation | Documentation reference only | No runtime dependency. |
| V3 release loader/hash validation | Adapt | Bind the exact core-derived pointer, manifest, files, and schema. |
| V3 candidate/audit/promotion model | Adapt | Apply as a separate named recipe. |
| V3 privacy guard | Adapt | Expand prohibited report fields and add recipe-specific checks. |
| Existing core-derived tests | Reuse concept only | Mirror synthetic, independent-audit, immutability, and promotion tests. |

## 4. Legacy weaknesses and v3 incompatibilities

- Cohort leakage: legacy input is documented as inheriting adult and ≥24-hour ventilation selection; Chapter-1 code adds site/readmission exclusions. None belongs in time blocking.
- Incorrect “completion” language: maximum observed time does not establish that the patient remained in the ICU continuously or until block end.
- Row loss: terminal-partial rows and some exact-boundary rows disappear.
- Negative-time handling: rows are excluded and labeled invalid rather than fully conserved and audited.
- Incomplete accounting: unparseable times and rows dropped by the block lookup have no complete conservation invariant.
- Lossy coercion: strings, booleans, lists, composites, and provenance-like fields are forced through numeric conversion.
- Unsuitable aggregation: means and medians are blindly applied to boluses, scores, rolling values, category codes, and therapy records.
- Medication semantics: no explicit protection of observed zero versus null; no variable-level eligibility treatment.
- “Last” ambiguity: last non-missing is an undocumented Pandas side effect; no observation time or age is exposed.
- Composite destruction: `therapy_read_confirmation_utc` cannot survive numeric coercion.
- Variant leakage: score variants and reported/computed driving pressure need explicit separation, not datatype-driven aggregation.
- Privacy: QC writes identifiers and raw examples.
- Scalability: largest-hospital Pandas materialization and wide intermediate copies are unsafe for 24,069,379 rows.
- Immutability: outputs may be overwritten; no candidate manifest, hashes, independent audit, promotion approval, or atomic pointer.
- Runtime independence: legacy and v2 cannot be dependencies because v3 must survive their deletion.

## 5. Proposed exact v3 artifact tree

The proposed nested layout is appropriate, provided it is treated as a separate recipe and never confused with `derived/current_release.json`.

```text
icu_data_platform_v3/
├── asic/
│   ├── config/time_blocking/
│   │   ├── contract_evidence_8h.yaml
│   │   ├── reviewed_time_blocking_8h_contract_0_1.yaml
│   │   └── reviewed_time_blocking_8h_promotion_<release_id>.yaml
│   ├── data/production/derived/time_blocking/8h/
│   │   ├── candidates/<run_id>/
│   │   │   ├── blocks.parquet
│   │   │   ├── stay_block_summary.parquet
│   │   │   ├── blocked_variable_dictionary.parquet
│   │   │   ├── blocked_variable_dictionary.md
│   │   │   ├── time_blocking_contract.yaml
│   │   │   └── candidate_manifest.json
│   │   ├── releases/<release_id>/
│   │   │   ├── blocks.parquet
│   │   │   ├── stay_block_summary.parquet
│   │   │   ├── blocked_variable_dictionary.parquet
│   │   │   ├── blocked_variable_dictionary.md
│   │   │   ├── time_blocking_contract.yaml
│   │   │   ├── candidate_manifest.json
│   │   │   ├── release_manifest.json
│   │   │   └── previous_current_release.json  # only when a prior pointer exists
│   │   └── current_release.json
│   ├── reports/production/time_blocking/8h/
│   │   ├── contract_evidence/<run_id>.{json,md}
│   │   ├── candidate_build/<run_id>.{json,md}
│   │   ├── candidate_audit/<run_id>.{json,md}
│   │   └── promotion/<release_id>.{json,md}
│   ├── reports/production/private/time_blocking/8h/
│   │   ├── contract_evidence/<run_id>/
│   │   │   ├── private_manifest.json
│   │   │   └── aggregate_evidence.parquet
│   │   └── candidate_audit/<run_id>/
│   │       ├── private_manifest.json
│   │       └── audit_evidence.parquet
│   └── slurm/
│       ├── run_time_blocking_8h_contract_evidence_production.sh
│       ├── run_time_blocking_8h_build_and_audit_production.sh
│       └── run_promote_time_blocking_8h_release_production.sh
├── src/asic_pipeline/time_blocking/
│   ├── __init__.py
│   ├── policy.py
│   ├── registry.py
│   ├── engine.py
│   ├── audit.py
│   └── promotion.py
└── tests/
    ├── test_time_blocking_policy.py
    ├── test_time_blocking_registry.py
    ├── test_time_blocking_engine.py
    ├── test_time_blocking_audit.py
    └── test_time_blocking_promotion.py
```

CLI surface:

- `audit-time-blocking-contract --resolution 8h`
- `build-time-blocking-candidate --resolution 8h`
- `audit-time-blocking-candidate --resolution 8h --run-id ...`
- `promote-time-blocking-release --resolution 8h --release-id ...`

The manifest must bind:

- Core-derived release ID, contract version, pointer hash, release-manifest hash.
- Static and dynamic Parquet hashes, schemas, row counts, and column counts.
- Reviewed time-blocking contract and registry hashes.
- Engine/artifact versions, resolution, origin, interval policy, ordering policy.
- Output file hashes, rows, schemas, Parquet metadata, compression, row-group policy.
- Build/audit report hashes and independent-audit status.
- Explicit statements: no cohort filtering, no input modification, no imputation/carry-forward, no external export.

This follows the immutable candidate/release principles documented for v3 in [data README](/Users/joanameyer/repository/icu-data-platform/icu_data_platform_v3/asic/data/README.md:33) and byte-preserving promotion in [core promotion](/Users/joanameyer/repository/icu-data-platform/icu_data_platform_v3/src/asic_pipeline/derivation/promotion.py:486).

## 6. Proposed time-index contract

Every row below requires explicit approval at the contract-freeze gate.

| Item | Recommended contract |
|---|---|
| Resolution | 480 exact minutes / 8 hours. Engine accepts other reviewed resolutions independently. |
| Origin | ICU admission at exact elapsed minute 0. |
| Interval notation | `[block_start, block_end)`. |
| Index formula | For `minutes >= 0`, `floor(minutes_since_icu_admission / 480)`. Do not compute boundaries using floating-point modulo on hours. |
| Boundary assignment | Minute 480/hour 8 belongs to block 1. |
| Block index | Zero-based signed integer, but this recipe emits only non-negative indices. |
| Label | `icu_8h_block_<index:06d>`; numeric index/start/end remain authoritative. |
| Negative time | Exclude from ICU-aligned aggregates, but conserve and audit every row and per-variable non-missing value. Do not silently discard or create negative indices. A future pre-admission recipe must be separately named and approved. |
| Emitted grid | For each stay with at least one non-negative row, emit blocks 0 through the block containing the maximum non-negative observation, including empty intervening blocks. |
| Terminal partial | Emit it in the same table. Do not create a second lossy dataset. |
| Completeness | `fully_covered_by_recording_extent_proxy = recording_extent >= block_end`. This is explicitly proxy-based, not confirmed ICU completion. |
| Exact terminal boundary | If the maximum row is exactly at hour 8, block 0 is proxy-full and block 1 is emitted as the terminal proxy-partial block containing that row. |
| Empty block | `source_row_count == 0`; all variable non-missing counts zero; values null. |
| Source rows but missing variable | `source_row_count > 0`, variable count zero, aggregate null. |
| Observed zero/false | Counts as a real observation and remains distinct from null. |
| Recording extent | Revalidate `icu_recording_extent_hours` against maximum dynamic elapsed time. Never call it discharge or true length of stay. |
| Prediction time | Do not emit `prediction_time_h` in this representation layer. `block_end_h` is the nominal boundary; a prediction recipe may later assign meaning to it. |
| Time columns | `block_start_h`, `block_end_h`, and corresponding artificial anchored timestamps based on 2020-01-01. No real calendar interpretation. |
| Ordering | `(hospital_id, stay_id_global, block_index)` for block rows. |
| Last ordering | Elapsed minutes ascending, then `__v3_source_order`, with file order/row number as validated fallbacks. Fail if the final tie key is not unique. |
| Last null policy | Last non-missing observation for that variable, not last physical row. |
| Last age | Emit last observation time and `block_end_h - last_time_h`; null when the variable has no observation. |
| Carry-forward | Prohibited. Last values stay within their source block only. |

The artificial timestamp must remain the reviewed `2020-01-01 + ICU-relative elapsed time` representation, not a patient timestamp.

## 7. Complete aggregation-registry design

The registry must contain exactly one disposition for every one of the 147 dynamic columns. Build and audit must fail on any missing, duplicate, or extra field. Unit, definition, physical type, and eligibility are inherited from the exact core-derived lineage—not inferred from Arrow type.

### Keys, time, and provenance: 11 fields

- Keys: `stay_id_global`, `stay_id_local`, `hospital_id`.
  - `stay_id_global` and `hospital_id` are block keys.
  - `stay_id_local` is excluded from feature aggregation and retained only through source lineage/private data where needed.
- Time: `minutes_since_icu_admission`, `hours_since_icu_admission`, `anchored_time_since_icu_admission`.
  - Used to assign blocks and derive block times; never summarized as clinical features.
- Provenance: `__v3_source_file_id`, `__v3_source_file_order`, `__v3_source_row_number`, `__v3_source_order`, `__v3_source_schema_variant_id`.
  - Excluded from blocked features.
  - Used for deterministic ordering, lineage, and aggregate accounting.
  - No provenance values appear in sanitized reports.

### Numeric measurement policy: 79 fields

Policy:

- Outputs: `__n`, `__mean`, `__median`, `__min`, `__max`, `__last`, `__last_time_h`, `__age_at_block_end_h`.
- Count: non-nullable `int64`.
- Value summaries: nullable `float64`, same unit as source.
- Time/age: nullable `float64`, unit hours.
- No interpolation, weighting, or carry-forward.

Fields:

`albumin`, `alt`, `amylase`, `ast`, `base_excess_art`, `bicarbonate_art`, `bilirubin_total`, `bnp`, `cardiac_index_bolus`, `cardiac_index_cont`, `cardiac_output_bolus`, `cardiac_output_cont`, `ck`, `ck_mb`, `compliance`, `core_temp`, `creatinine`, `crp`, `cvp`, `d_dimer`, `dbp`, `delta_p_reported`, `dpap`, `ecmo_o2`, `etco2`, `evlwi`, `extracorp_blood_flow`, `extracorp_o2_flow`, `feo2`, `fio2`, `fio2_set`, `gedvi`, `heart_rate`, `hematocrit`, `hemoglobin`, `ie_ratio`, `ie_ratio_set`, `il6`, `inr`, `insp_pressure`, `lactate_art`, `ldh`, `lipase`, `lymph_abs`, `lymph_pct`, `map`, `mpap`, `ntprobnp`, `paco2`, `pao2`, `pct`, `pcwp`, `peep`, `peep_set`, `pf_ratio`, `ph_art`, `platelets`, `ptt`, `pvri`, `resp_rate`, `sao2`, `sbp`, `scvo2`, `spap`, `spo2`, `spont_resp_rate`, `stroke_index_bolus`, `stroke_index_cont`, `stroke_volume_bolus`, `stroke_volume_cont`, `svri`, `troponin`, `urea`, `vt`, `vt_per_ideal_bw_total`, `vt_per_kg_ideal_body_weight`, `vt_spontaneous`, `wbc`, and `delta_p_computed`.

Eligibility overrides:

- `feo2`, `ie_ratio`, `ie_ratio_set`, and `stroke_volume_bolus`: exclude value aggregates; retain aggregate input accounting and dictionary rows as analysis-ineligible.
- `vt_per_kg_ideal_body_weight`: preserve its conditionally-ineligible state; recommended default is no analysis-feature output until its condition is formally resolved.
- `delta_p_reported` and `delta_p_computed` remain separate and are never coalesced.
- `delta_p_computed_out_of_range` is handled as a boolean below.

### Medication and therapy policy: 34 fields

Source variables:

`clonidine_iv_cont`, `dexamethasone_iv_bolus`, `dexmedetomidine_iv_cont`, `dobutamine_iv_cont`, `epinephrine_iv_cont`, `fentanyl_iv_cont`, `fludrocortisone_po_bolus`, `furosemide_iv_cont`, `hydrocortisone_iv_bolus`, `inhaled_iloprost`, `inhaled_no`, `isoflurane_inh`, `ketanest_iv_cont`, `levosimendan_iv_cont`, `midazolam_iv_cont`, `milrinone_iv_cont`, `morphine_iv_cont`, `norepinephrine_iv_cont`, `prednisolone_iv_bolus`, `propofol_iv_cont`, `rocuronium_iv_bolus`, `sevoflurane_inh`, `sufentanil_iv_cont`, `terlipressin_iv_bolus`, `vasopressin_iv_cont`.

Semantic-split variables:

`clonidine_iv_cont_weight_normalized`, `epinephrine_iv_cont_absolute`, `hydrocortisone_iv_bolus_source_uk08`, `ketanest_iv_cont_weight_normalized`, `morphine_iv_cont_weight_normalized`, `norepinephrine_iv_cont_absolute`, `prednisolone_iv_bolus_source_uk02`, `propofol_iv_cont_weight_normalized`, `sufentanil_iv_cont_source_uk00`.

Recommended observed-value outputs:

- `__n`, `__n_zero`, `__n_positive`
- `__mean`, `__median`, `__min`, `__max`
- `__last`, `__last_time_h`, `__age_at_block_end_h`

These are summaries of observations only:

- No sum interpreted as total dose.
- No time-weighted exposure.
- No interval extension.
- No carry-forward.
- Zero is observed zero.
- Null is unavailable/unobserved.
- Missing does not imply inactive.

The three unresolved-source-scale fields are excluded from value aggregates and remain analysis-ineligible:

- `hydrocortisone_iv_bolus_source_uk08`
- `prednisolone_iv_bolus_source_uk02`
- `sufentanil_iv_cont_source_uk00`

Their aggregate non-missing/zero/positive counts belong in audit evidence only.

### Score policy: 16 fields

Fields:

- iSOFA: `isofa_cardiovascular`, `isofa_cns`, `isofa_liver`, `isofa_renal`, `isofa_respiratory`, `isofa_thrombocyte`, `isofa_total_score`.
- SOFA: `sofa_blood`, `sofa_cns`, `sofa_liver`, `sofa_renal`, `sofa_respiratory`, `sofa_respiratory_calculated`, `sofa_score_unspecified`, `sofa_score_without_gcs`, `sofa_total_score`.

Outputs:

- `__n`, `__median`, `__min`, `__max`, `__last`, `__last_time_h`, `__age_at_block_end_h`.
- No mean by default.
- No recomputation, coalescence, or preference between variants.
- Physical summary type `float64`; count `int64`; unit score points.

### Rolling/cumulative policy

`fluid_balance_24h`:

- Outputs `__n`, `__last`, `__last_time_h`, `__age_at_block_end_h`.
- No sum: summing repeated 24-hour rolling balances would double-count.
- No mean unless separately justified.

### Boolean policy

`ecmo`, `delta_p_computed_out_of_range`:

- `__n`, `__n_true`, `__n_false`, `__any_true`, `__all_true`, `__last`, `__last_time_h`, `__age_at_block_end_h`.
- Counts `int64`; logical outputs nullable `bool`.
- Observed false remains distinct from null.

### Categorical and composite policy

- `ards_diagnosis_app`: categorical code; analysis-ineligible because site-scoped meanings remain unresolved. Exclude feature values; aggregate accounting only.
- `severity_read_confirmation`: string, currently unavailable/all missing and analysis-ineligible. Exclude feature values; accounting only.
- `position_therapy`: numeric dimensionless proportion, not boolean. Proposed outputs: `__n`, `__median`, `__min`, `__max`, `__last`, last time/age. No time-weighted exposure and no boolean coercion.
- `therapy_read_confirmation_utc`: fixed-size nullable boolean pair.
  - Preserve positions independently.
  - For each position: observed/true/false counts, last, time, age.
  - Also count all nine pair states: `ff`, `ft`, `f_null`, `tf`, `tt`, `t_null`, `null_f`, `null_t`, `null_null`.
  - No OR, consensus, source preference, or list-to-number coercion.

### Static-only fields

All 37 static columns are excluded from dynamic aggregation. In particular:

- Mortality: `hospital_mortality`, `icu_mortality`, and both conflict flags.
- Ventilation proxy summaries: supported timestamp count, episode count, maximum episode hours, observed ≥24h proxy.
- `icu_recording_extent_hours`.

They remain available through exact release lineage and are not repeated per block.

## 8. Static-table policy

Recommendation: reference the exact core-derived static table through manifest lineage.

Do not:

- Copy `static.parquet` into every time-blocked release.
- Repeat static values on every block.
- Create a modified static table masquerading as the source static table.

`stay_block_summary.parquet` should contain only:

- `stay_id_global`, `hospital_id`
- whether a non-negative block was emitted
- non-negative, negative, invalid-time, and total source-row counts
- first/last emitted block indices
- full/partial/empty block counts
- validated recording-extent proxy bookkeeping

Trade-offs:

- Reference: least storage, no divergent copy, strongest provenance; consumers must perform a lineage-aware join.
- Byte-for-byte static copy: convenient but duplicates protected data and complicates retention.
- Repetition per block: largest storage, highest leakage risk, and multiple inconsistent copies; reject.

## 9. Blocked variable dictionary

One row per emitted output field, plus disposition-only rows for excluded source variables.

Required columns:

- `source_variable`
- `source_definition`
- `source_physical_type`
- `source_unit`
- `source_eligibility`
- `disposition`
- `aggregation_operation`
- `output_name`
- `output_physical_type`
- `output_unit`
- `interval_origin`
- `interval_semantics`
- `last_value_semantics`
- `missingness_meaning`
- `zero_meaning`
- `false_meaning`
- `carry_forward_allowed`
- `analysis_eligibility`
- `caveats`
- `source_core_derived_contract_version`
- `source_core_derived_release_id`
- `source_release_manifest_sha256`
- `time_blocking_contract_version`
- `registry_entry_version`

The dictionary must explicitly say that an output such as `heart_rate__mean` is the arithmetic mean of non-missing observations in `[start,end)`, not a time-weighted mean.

## 10. Conservation and audit invariants

A build or audit fails unless all applicable invariants pass:

1. `derived/current_release.json` resolves to release `20260808T074305Z`.
2. Pointer hash, release-manifest hash, and both input Parquet hashes match.
3. Input contract is core-derived 0.2 with lineage to cleaned `20260807T154100Z` and harmonized `20260807T112402Z`.
4. Input counts are exactly 16,054 static rows, 24,069,379 dynamic rows, 37 static columns, and 147 dynamic columns.
5. Static and dynamic stay relationships match the approved release.
6. Every static stay appears in `stay_block_summary.parquet`; absence of an emitted block has an explicit reason.
7. Dynamic row conservation:

   `total input rows = negative-time rows + invalid/missing-time rows + assigned non-negative rows`.

8. Assigned non-negative rows equal the sum of block `source_row_count`.
9. The expected 247,977 negative rows are independently recounted and never enter non-negative block features.
10. Exact multiples of 480 minutes are counted and verified in the following block.
11. Every unassigned row has exactly one enumerated reason.
12. Full, proxy-partial, and empty block counts reconcile by stay and globally.
13. No block outside the reviewed grid is emitted.
14. `source_row_count == 0` iff the block has no assigned source row.
15. For every variable: input non-missing counts reconcile across negative, invalid-time, assigned, and deliberately excluded categories.
16. Observed zero and false counts reconcile independently from null.
17. Every emitted `__n` equals independently recomputed non-missing values.
18. Every last value, last timestamp, and age is independently recomputed using the frozen tie-break.
19. No last value is sourced from an earlier block.
20. The same input processed with multiple Arrow batch sizes produces identical schemas, row order, values, accounting, and canonical content digest.
21. Registry field set equals the 147-column source schema exactly.
22. Output schema equals the dictionary exactly.
23. Reported/computed driving pressure and all SOFA/iSOFA variants remain separate.
24. No row or stay is filtered for site, readmission, mortality, ventilation, or analysis cohort criteria.
25. Core-derived files and manifests hash identically before and after the run.
26. No forward filling, carry-forward, interpolation, null-to-zero conversion, or other imputation occurs.
27. Sanitized reports contain no identifiers, filenames, provenance tokens, patient rows, or raw examples.
28. Promotion copies every audited payload byte unchanged and modifies only the recipe-specific pointer.

## 11. Consolidated production evidence plan

Run one bounded-memory Slurm contract-evidence job before implementation. It may perform multiple streaming passes internally but must produce no blocked candidate or release.

The job should:

1. Resolve and validate the authoritative core-derived pointer, manifest, file hashes, lineage, schema, and counts.
2. Compile the proposed registry and prove coverage of all 147 columns.
3. Profile time validity, negative rows, exact 480-minute boundaries, per-stay extents, timestamp duplicates/regressions, and tie-key uniqueness.
4. Simulate aggregate counts for:
   - completed-only blocks;
   - all observed blocks;
   - the recommended superset with proxy-completeness flags.
5. Quantify rows and non-missing values that each alternative would lose.
6. Count full, partial, exact-boundary-terminal, empty-intervening, and zero-block stays.
7. Profile each variable’s non-missing, zero, positive, true/false, categorical domain, list state, and eligibility.
8. Verify medication null/zero behavior and the three unresolved-source-scale fields.
9. Compare last-row versus last-nonmissing outcomes and age distributions without exposing values.
10. Measure maximum rows per stay/block and calculate bounded-memory/spill requirements.
11. Execute assignment/tie-selection digest logic under at least two different batch sizes.
12. Revalidate `icu_recording_extent_hours` against maximum dynamic elapsed time.
13. Emit one sanitized aggregate JSON/Markdown report and one owner-only aggregate evidence package.

Privacy rules:

- Public review report: aggregate counts/distributions only.
- Private directory: mode 0700; files 0600; manifest-hashed.
- No raw identifiers, filenames, raw tokens, or patient examples by default, even privately.
- If a future technical blocker truly requires row-level owner evidence, that must be separately approved and remain cluster-only.

V3 already has a review-payload guard in [privacy/redaction.py](/Users/joanameyer/repository/icu-data-platform/icu_data_platform_v3/src/asic_pipeline/privacy/redaction.py:11); the new recipe should extend its protected-key vocabulary.

## 12. Tests

### Time and grid unit tests

- Time 0 → block 0.
- Just before 8 hours → block 0.
- Exactly 8 hours → block 1.
- Just after 8 hours → block 1.
- Negative time → audited, not assigned.
- Exact maximum at 8 hours → block 0 proxy-full plus block 1 terminal proxy-partial containing the boundary row.
- Partial endpoint at 10 hours → blocks 0 and 1; block 1 partial.
- Empty gap between observed blocks → empty block retained.
- Stay with no non-negative time → stay summary only, explicit reason.
- Invalid/null time → explicit accounting reason.
- Large block indices and exact integer-minute arithmetic.

### Aggregation tests

- Numeric zero versus null.
- Boolean false versus null.
- Source rows present but a variable entirely null.
- Entire block without source rows.
- Duplicate timestamps with unique source order.
- Duplicate timestamps and duplicate source order → hard failure.
- Last physical row null but earlier value non-null → last-nonmissing chosen.
- Last age is calculated from the selected observation.
- Medication values do not carry into later blocks.
- No medication sum or time-weighted exposure.
- SOFA/iSOFA variants remain separate.
- Reported/computed driving pressure remain separate.
- Rolling `fluid_balance_24h` is not summed.
- `position_therapy` fractions are not boolean-coerced.
- `therapy_read_confirmation_utc` preserves position and all pair states.
- Ineligible fields produce accounting and dictionary entries but no eligible feature output.

### Integration and reproducibility tests

- Synthetic immutable core-derived release with pointer, manifest, schemas, hashes, and lineage.
- Input hash mismatch, pointer mismatch, row-count mismatch, or schema drift fails before writing a candidate.
- Stay split across Arrow batch boundaries.
- Same data under multiple batch sizes and row-group sizes yields identical canonical output.
- Spill/no-spill execution yields identical results.
- Independent audit detects a changed value, count, last timestamp, dictionary row, or schema field.
- Candidate rerun cannot overwrite an existing run ID.
- Promotion refuses unaudited/unapproved candidates.
- Promotion preserves all payload hashes and leaves candidate intact.
- Existing recipe release cannot be overwritten.
- Pointer remains unchanged on every failure.
- Sanitized-report tests reject identifiers, filenames, raw times/examples, and provenance values.

## 13. Chronological implementation phases and human gates

1. **Legacy audit and design — complete in this response.**  
   No implementation or production action.

2. **Consolidated contract evidence.**  
   One read-only cluster job; aggregate/public and owner-only evidence only. No blocked candidate.

3. **Explicit 8-hour contract freeze.**  
   Human approval of time-index, terminal, negative-time, aggregation, dictionary, privacy, and artifact policies. Freeze a reviewed YAML with its hash.

4. **Local implementation and tests.**  
   Standalone v3 Arrow/Parquet engine using synthetic data only. Legacy and v2 unavailable at runtime.

5. **Run-scoped production candidate.**  
   Build only `candidates/<run_id>/`; validate input hashes before and after; never update a pointer.

6. **Complete independent candidate audit.**  
   Separate read path and independently recomputed assignment/aggregates. Produce sanitized and private evidence.

7. **Explicit human promotion approval.**  
   Approval must identify the exact run ID, candidate hashes, contract hash, input release, and successful audit.

8. **Byte-preserving immutable release promotion.**  
   Copy audited bytes unchanged, preserve the candidate and previous pointer bytes, create a release manifest, then atomically update only `derived/time_blocking/8h/current_release.json`.

Any failed gate leaves the core-derived release and both current-release pointers unchanged.

## 14. Recommendation and decisions needed from you

I recommend approving the architecture for evidence collection, but not yet approving implementation. The central contract recommendation is a single superset `blocks.parquet` containing all observed non-negative blocks, empty intervening blocks, and a proxy-completeness flag. That preserves information while allowing analysts to select proxy-full blocks without maintaining a second lossy artifact.

Decisions that require your explicit approval:

1. Approve excluding negative rows from ICU-aligned blocks while fully auditing them, with no negative indices.
2. Approve emitting the terminal observed block and empty intervening blocks, rather than completed-only output.
3. Approve the exact-boundary consequence: an observation at hour 8 belongs to block 1, which is emitted even when it is the terminal proxy-partial block.
4. Approve the name and interpretation `fully_covered_by_recording_extent_proxy`, explicitly not ICU-discharge completion.
5. Approve omitting `prediction_time_h` from this representation recipe.
6. Approve last-nonmissing semantics, deterministic source-order tie-breaking, and last-observation time/age outputs.
7. Approve the proposed aggregation families, including no medication carry-forward, no dose summation, no score means, and last-only handling for `fluid_balance_24h`.
8. Approve excluding the nine analysis-ineligible variables’ value aggregates and keeping `vt_per_kg_ideal_body_weight` non-default while conditionally ineligible.
9. Approve referencing core-derived static data through exact lineage instead of copying or repeating it.
10. Approve the proposed candidate/release/report/private-evidence tree and resolution-general engine.
11. Approve one consolidated cluster contract-evidence audit as the next action; this approval would not authorize implementation, candidate generation, promotion, or external export.