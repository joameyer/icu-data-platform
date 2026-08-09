# Frozen harmonized schema and variable dictionary 0.2

## Approval

The data owner explicitly approved and froze the amendment review generated at
`2026-08-07T10:14:15+00:00`. The immutable run ID is
`20260807T101321Z`. The approval includes the complete ordered static and
dynamic schemas, all 162 clinical dictionary rows, the reviewed medication
zero/null semantics, and the general negative-medication cleaned-layer rule.

The original approval referred to `20260807T101415Z`, which was the report's
generation timestamp written without punctuation, not its run ID. The data
owner subsequently supplied the sanitized review index. It uniquely linked
that timestamp and the approved metrics to run `20260807T101321Z`, while also
confirming that contract `0.2` had not been created. This correction is stored
in the freeze policy and manifest; the original approval text is preserved.

The freeze policy binds that approval to:

- the corrected exact review ID and original generation timestamp;
- the exact amendment, reviewed-unit, and reviewed-medication policy hashes;
- all review metrics, including 23 static and 139 dynamic variables;
- the private proposal manifest and its three output hashes;
- frozen baseline contract `0.1` and the reviewed unit-audit lineage; and
- all 34 dictionary-classified medication or therapy variables.

Any missing, changed, mismatched, or already-frozen evidence stops the command.
The command never selects the latest report and never overwrites an existing
contract.

## What the freeze writes

The command copies the three approved files byte-for-byte into:

`asic/data/production/contracts/harmonized_schema_dictionary/0.2/`

The directory contains:

- `static_schema.json`;
- `dynamic_schema.json`;
- `variable_dictionary.parquet`; and
- `freeze_manifest.json`.

A sanitized PASS record is written as
`asic/reports/production/review/schema_dictionary_freeze/0.2.md` and `.json`.
Contract `0.1`, all proposal evidence, and all released clinical data remain
unchanged.

## Activation boundary

This is a metadata contract freeze. It reads zero clinical rows and does not:

- apply any of the nine unit conversions;
- create any of the nine semantic-split columns in clinical data;
- apply the negative-medication cleaning rule;
- rebuild harmonized, cleaned, or derived data;
- change a current-release pointer;
- authorize publication; or
- authorize external export.

It authorizes the implementation of a separately run and completely audited
harmonized `0.2` candidate. Cleaning policy `0.2` and all downstream rebuilds
remain later, explicit checkpoints.
