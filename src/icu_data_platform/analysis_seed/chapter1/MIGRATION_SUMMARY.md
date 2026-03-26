# Chapter 1 Migration Summary

## Remains In `icu-data-platform`

- ASIC extraction from raw source tables
- Harmonization of static and dynamic ASIC tables
- Generic stay-ID QC and generic semantic cleaning
- Generic stay-level ASIC table construction
- Generic 8-hour block construction and generic blocked dynamic feature aggregation

## Extracted Into The Chapter 1 Seed

- Chapter 1 site-level eligibility rules
- Chapter 1 stay-level exclusions
- Readmission-based first-stay proxy handling
- Chapter 1 valid-instance selection on top of generic blocks
- Chapter 1 feature-set configuration
- Chapter 1 terminal-label assembly for valid instances
- Chapter 1 model-ready dataset assembly
- Chapter 1 readiness summaries

## Still Mixed / Follow-Up Cleanup

- `src/icu_data_platform/sources/asic/cohort.py` still contains legacy Chapter 1 logic and compatibility paths
- `src/icu_data_platform/sources/asic/blocks.py` still contains legacy Chapter 1-specific block code
- `src/icu_data_platform/sources/asic/harmonize/dynamic.py` still exposes `chapter1_recommendation` inside an upstream harmonization QC artifact
- The portable seed currently uses terminal ICU mortality labels reused across horizons because true event-time horizon labels are not available in the standardized upstream ASIC artifacts
