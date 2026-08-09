# Complete reviewed harmonization-registry contract

## Current status

The contract validator and table planner are implemented for synthetic schemas
only. No complete production registry exists yet, and this stage cannot read or
transform production data.

The executable contract policy is
`asic/config/harmonization/registry_contract_policy.yaml`. All production,
value-transformation, artifact-writing, concatenation, and union-schema gates
are false.

## Complete registry requirements

A future complete registry must be explicitly marked
`human_approved_complete_raw_v3_registry` and cite:

- the exact schema/token inventory run;
- the exact candidate registry-review run;
- reviewer role and approval date; and
- the approved total occurrence count.

It must contain exactly one rule for every private `R####` occurrence. Each rule
uses the exact hospital, table, raw header, and positional occurrence from its
owner-only evidence row. Rule IDs, review IDs, and scopes must all be unique.

Every rule must choose one action:

- `retain`, with an explicit canonical target, kind, Arrow value type, unit,
  and any parser/missing/categorical/unit/semantic rule IDs; or
- `drop_after_all_missing`, with no target contract and an approved evidence
  count of zero.

There is no generic `drop`, inferred mapping, or implicit missing policy.

## Cross-rule invariants

Within a table, one canonical target must have one consistent kind, value type,
and unit across every hospital and source. Hospital-specific semantic
differences that cannot satisfy that invariant require distinct canonical
targets or an approved deterministic conversion.

If two raw sources at one hospital/table map to the same target, every source
must name the same alias group and merge policy. Allowed policies are narrowly
enumerated by the contract. A target-name collision is never treated as an
implicit coalesce.

The registry is validated against the private occurrence workbook before a
table may be planned. Review IDs, exact scopes, evidence counts, and all-missing
drop preconditions must agree exactly.

## Synthetic table planner

The planner accepts an Arrow schema, not a table or data file. Every clinical
field must still be string-typed and carry lossless-ingestion metadata for the
exact raw name and occurrence. Provenance fields are supplied separately.

The planner rejects:

- an unknown source field;
- a missing reviewed field;
- duplicate raw scopes;
- non-string clinical inputs;
- incomplete field metadata;
- unapproved target types; and
- source/registry coverage differences.

It can produce only a candidate output schema marked
`candidate_not_union_approved`. It performs no parsing, renaming, merging,
dropping, or value transformation. `require_execution_ready()` always blocks
under policy `0.1`.

## Next approval-dependent work

After the candidate registry-review report is examined, reviewed rules can be
added in small clinical groups. The complete registry must not be marked
approved until every occurrence is resolved and the variable dictionary and
ordered union schema are separately reviewed. Only then may a later policy
version consider enabling bounded production transformation.

