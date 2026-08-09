# Reviewed composite-source audit

This read-only stage validates the owner-approved representation of the two
UK00 therapy-read-confirmation sources. It also revalidates the two approved
all-missing occurrence retirements from immutable registry-review evidence.

The proposed canonical representation is a fixed-size list of two nullable
Booleans. Positions follow the physical source-column order in lossless
ingestion and are bound to the exact private `R####` occurrence evidence. Raw
numeric zero maps to false, raw numeric one maps to true, and literal empty or
source-schema absence maps to a null element. Every other non-empty token is
unresolved and blocks the audit.

Every resolved row is counted in exactly one of nine pair patterns. Conflicting
pairs are preserved. The audit applies no OR, consensus, preferred-source rule,
or scalar reduction and writes no harmonized data.

Private output remains under:

```text
asic/reports/<context>/private/composite_source_audit/<run_id>/
```

Sanitized JSON and Markdown are written under the corresponding
`review/composite_source_audit/` directory. Exact source headers and any
unresolved token examples remain private on the authorized cluster.

The policy is bound to registry-review run `20260805T104417Z`. A different
review basis requires a new reviewed policy version rather than silently
reusing these occurrence IDs.
