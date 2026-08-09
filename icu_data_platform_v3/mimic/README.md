# MIMIC-IV selected-variable pipeline

This directory owns the MIMIC-IV 3.1 contracts for the narrow
`phase_aware_mortality_0_1` profile. It is a sibling of `asic/`, while its
implementation is isolated in `src/mimic_iv_pipeline/`.

The current milestone authorizes an operator-executed, run-scoped production
candidate and independent audit on the protected cluster. It does not authorize
a release, pointer change, or public row-level output. The frozen analysis
repository is an interface consumer and is not a runtime dependency.

The pipeline has one canonical timestamp-level selected-event boundary.
Fifteen-minute and eight-hour blocks are each generated directly from that
boundary. Neither blocking path performs carry-forward, imputation, encoding,
or scaling, and the eight-hour path must never read the fifteen-minute output.

See [architecture](docs/architecture.md), [legacy migration](docs/legacy_migration.md),
[review gates](docs/review_gates.md), and the operator-only
[cluster runbook](docs/cluster_runbook.md).
