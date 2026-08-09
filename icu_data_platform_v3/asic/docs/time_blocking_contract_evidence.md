# 8-hour time-blocking contract evidence

The first v3 time-blocking action was one report-only production audit over
immutable core-derived release `20260808T074305Z`. Evidence run
`20260808T114902Z` passed every technical invariant and was explicitly approved
by the data owner as the basis for contract `0.1`, with medication dose totals
deferred and local implementation/tests only.

The human-reviewed candidate time convention is:

- pre-admission windows `[-8,0)`, `[-16,-8)`, and so on;
- an admission singleton `{0}` labelled `0h`;
- post-admission right-labelled windows `(0,8]`, `(8,16]`, and so on;
- retained empty intervening blocks and a retained terminal partial block;
- recording-extent-proxy terminology rather than ICU-discharge completion;
- no prediction-time field, carry-forward, imputation, or cohort filtering.

The audit validates the exact current pointer, release manifest, Parquet
hashes, schemas, row counts, and reviewed v3 lineage contracts. It then runs
the assignment scan under two Arrow batch sizes and profiles every one of the
147 dynamic fields. Evidence includes negative, admission, positive,
exact-boundary, empty-block, terminal-partial, tie, recording-extent,
medication-zero/null, categorical/composite, and apparent bolus-repeat
accounting.

Deterministic ordering follows the frozen provenance scope:
`__v3_source_order` must be non-null and contiguous from one separately within
each hospital's dynamic table. The harmonized file concatenates hospitals, so
the value is expected to reset at a hospital boundary and is not a global row
number. Within a stay, elapsed time followed by this hospital-scoped source
order gives a deterministic chronological tie-break; file order and source row
number must also remain non-null provenance.

Manifest schema digests bind the reviewed Arrow schemas before Parquet
serialization. During validation, nested list child labels are canonicalized
from Parquet's equivalent `element` representation back to the contract's
`item` representation before recomputing the exact digest. Column order,
physical types, nullability, field metadata, and schema metadata remain
hash-bound.

Variable-profile units are read from the frozen v3 field-metadata key
`asic_v3_unit`; operational provenance fields without clinical-unit metadata
are reported as `unavailable`.

The canonical last-observation terminology is:

- `last_observation_value`;
- `last_observation_time_h`;
- `last_observation_age_h`.

An apparent bolus value sum is evidence only. It cannot become an
`observed_dose_total` unless the evidence and a later human contract freeze
establish that repeated rows are distinct administrations. Continuous rate
integration remains a separate future exposure contract.

Run the self-submitting production wrapper from the cluster login frontend:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
unset SLURM_JOB_ID SLURM_STEP_ID
bash asic/slurm/run_time_blocking_8h_contract_evidence_production.sh
```

The expected technically successful command exit is `2`, meaning the evidence
is complete but the exact 8-hour contract still requires human review. The job
writes only owner-only aggregate evidence under
`asic/reports/production/private/time_blocking_8h_contract_evidence/` and a
sanitized JSON/Markdown review under
`asic/reports/production/review/time_blocking_8h_contract_evidence/`.

The evidence-review gate is complete. See
[the frozen 8-hour contract](time_blocking_8h_contract.md). Production blocked
workflow implementation is now authorized, but candidate and audit execution
is limited to the separately approved exact run ID `20260808T130534Z`.
