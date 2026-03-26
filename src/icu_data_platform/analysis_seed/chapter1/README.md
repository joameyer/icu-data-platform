# Chapter 1 Analysis Seed

This package is the portable Chapter 1 analytic preprocessing layer.

It assumes standardized upstream ASIC artifacts already exist:

- harmonized ASIC static data
- harmonized ASIC dynamic data
- generic 8-hour blocked ASIC data

It owns only Chapter 1-specific logic:

- site and stay exclusions
- readmission-based first-stay proxy handling
- valid-instance logic
- Chapter 1 feature selection
- terminal label assembly
- model-ready dataset construction
- readiness summaries

The intended migration target is a standalone Chapter 1 repository.
