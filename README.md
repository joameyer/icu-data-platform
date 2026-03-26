# icu-data-platform

## Purpose
This repository contains the reusable ICU data ingestion and harmonization layer for the PhD project.

It is responsible for:
- dataset-specific extraction
- source-specific harmonization
- hospital-specific ASIC ingestion differences
- common schemas
- reusable validation / QC

It is not responsible for:
- chapter-specific cohort definitions
- prediction horizon labels
- block construction
- model-ready analysis datasets
- chapter-specific evaluation

## Supported datasets
- ASIC
- MIMIC-IV
- later potentially eICU

## Output contract
This repo produces harmonized dataset-level tables that can be consumed by downstream chapter-specific analysis repositories.

For ASIC, the upstream harmonized build also emits reusable QC artifacts, including an
observed mechanical-ventilation `>=24h` verification derived directly from harmonized
dynamic timestamps. See `docs/asic_mech_vent_ge_24h_qc.md`.

## Data policy
No real patient-level data is committed to this repository.
Only schemas, synthetic examples, and safe metadata/QC outputs may be stored here.

```
icu-data-platform/
├── src/
│   └── icu_data_platform/
│       ├── schemas/
│       │   ├── patients.py
│       │   ├── icu_stays.py
│       │   ├── ventilation.py
│       │   ├── death_disposition.py
│       │   └── events.py
│       │
│       ├── sources/
│       │   ├── asic/
│       │   │   ├── extract/
│       │   │   ├── harmonize/
│       │   │   ├── qc/
│       │   │   └── site_configs/
│       │   │
│       │   ├── mimic/
│       │   │   ├── extract/
│       │   │   ├── harmonize/
│       │   │   └── qc/
│       │   │
│       │   └── eicu/
│       │       ├── extract/
│       │       ├── harmonize/
│       │       └── qc/
│       │
│       ├── common/
│       │   ├── ids.py
│       │   ├── time.py
│       │   ├── units.py
│       │   ├── validation.py
│       │   └── io.py
│       │
│       └── pipelines/
│           ├── build_asic_harmonized.py
│           ├── build_mimic_harmonized.py
│           └── build_eicu_harmonized.py
```
