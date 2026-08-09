# Static-registry completion audit

## Purpose

This audit is the first complete static-table accounting gate. It does not
harmonize values. It joins the immutable harmonization registry-review
workbooks with every currently approved static decision source and answers:

- is every static raw-column occurrence represented exactly once;
- which occurrence mappings and parsers are approved versus still inherited
  only as v2/legacy candidates;
- whether approved sources conflict on a target, physical type, or unit;
- which canonical variables are available at which hospitals;
- what the complete ordered static schema would be; and
- which dictionary definitions, units, caveats, and provenance still require
  review.

The production evidence basis is harmonization registry-review run
`20260805T104417Z`. The audit expects 130 static raw-column occurrences across
the eight approved hospitals.

## Decision sources

All decision sources are immutable and SHA-256 bound in
`asic/config/harmonization/static_registry_completion_audit.yaml`:

- reviewed raw-v3 schema/token rules, including cluster, height-list, and
  hospital-scoped missing-sentinel decisions;
- the approved inventory folder-to-canonical-hospital mapping, which supplies
  `hospital_id` independently of any optional in-file hospital code;
- reviewed static decision registry `0.3`, including identifiers, weight,
  lengths of stay, dialysis-/ventilation-free days, and reported mortality;
- reviewed ICD-10 contract `0.2`; and
- the candidate contract's explicitly human-approved raw-v3 target
  representations and categorical domains.

Frozen-v2 or legacy-only proposals remain pending. An English candidate name
does not become approved merely because it appears in the draft schema.

The proposed schema always includes canonical `hospital_id` as an approved
`large_string` output derived from the inventory mapping. A source
`hospital_code_source` field is included only if it is actually observed in
the raw-v3 occurrence evidence; it is never fabricated from `hospital_id`.

## Outputs

The owner-only directory is:

`asic/reports/<context>/private/static_registry_completion_audit/<run-id>/`

It contains:

- `occurrence_coverage.parquet` — exact raw occurrence identity, approved
  decision sources, resolved target, and mapping/parser status;
- `variable_dictionary.parquet` — draft definitions, type/unit status,
  hospital availability, exact source-name provenance, and caveats;
- `ordered_schema.json` — the complete proposed static column order; and
- `static_registry_completion_audit_manifest.json` — immutable input hashes,
  decision hashes, counts, and checks.

The sanitized review report contains canonical English names, hospital-level
counts, statuses, and the proposed order. It excludes exact raw headers,
tokens, filenames, and stay identifiers.

## Fail-closed behavior

Technical failures include changed or incomplete input evidence, changed
decision hashes, an unaccounted static occurrence, conflicting approved
decisions, an unknown hospital, or a proposed schema that fails to account for
every target. Human findings include pending mappings, types, units, parser or
categorical policies, unresolved aliases, and the intentionally unapproved
ordered schema and dictionary.

The audit returns exit code 2 for expected human-review findings and exit code
1 for technical failure. The Slurm wrapper converts exit code 2 into a
successful batch completion while preserving `Overall status: FAIL` in the
review report.

## Execution boundary

The command reads only immutable report evidence and configuration/decision
files. It does not read raw CSV or ingested Parquet, parse clinical values,
write harmonized data, concatenate hospitals, freeze a registry or schema, or
approve the dictionary.

Run from a login shell:

```bash
export REGISTRY_REVIEW_RUN_ID=20260805T104417Z
bash asic/slurm/run_static_registry_completion_audit_production.sh
```

After the report is reviewed, new approvals must be recorded in a new
versioned decision source. The audit must then be rerun; historical bundles
are never overwritten.
