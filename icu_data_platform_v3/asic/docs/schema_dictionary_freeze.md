# Frozen harmonized schema and variable dictionary

## Approval

The data owner explicitly approved and froze schema/dictionary review
`20260806T103811Z` on 2026-08-06. Reviewed freeze policy `0.1` records the exact
approval statement, lineage IDs, metrics, all 153 ordered clinical fields,
their physical types, units, analysis eligibility, and the five appended
provenance fields.

The freeze resolves only these two human findings from the source report:

- `ordered_harmonized_union_schema_approved`; and
- `variable_dictionary_approved`.

It does not reinterpret the 72 unresolved units. Those values remain preserved
and analysis-ineligible under the previously approved fail-closed policy.

## Operation

`freeze-schema-dictionary` reads no candidate clinical rows. It verifies:

- the exact approved review run and evidence lineage;
- every approved metric and schema row;
- the source private-manifest version and variable-dictionary hash;
- non-empty dictionary definitions and provenance strategies;
- exact agreement between every dictionary row and the approved schema; and
- that all source dictionary rows remain non-publishable.

It then copies the exact reviewed dictionary and writes ordered static and
dynamic schema JSON into the versioned v3 contract directory:

`asic/data/production/contracts/harmonized_schema_dictionary/0.1/`

The write is exclusive and staged. Existing final or incomplete output blocks
a retry rather than being overwritten.

## Cluster invocation

Run this small metadata-only operation on the frontend:

```bash
cd /hpcwork/jrc_combine/joana/icu_data_platform_v3
unset SLURM_JOB_ID SLURM_STEP_ID

.venv/bin/asic-pipeline freeze-schema-dictionary \
  --config asic/config/datasets/production.yaml
```

The expected exit status is `0`, with no blocking findings and both frozen
flags true. No clinical production data are generated and publication remains
false. The frozen contract authorizes implementation of the streaming
harmonized build; it does not itself authorize publication, cleaning, or
derivation.
