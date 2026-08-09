# Candidate harmonization-registry review

## Purpose

This stage turns the immutable schema/token inventory into a complete,
owner-reviewable candidate registry. It does not harmonize values and does not
approve its own proposals.

The command consumes exactly one policy/artifact `0.3` schema/token run. It
requires zero unmapped occurrences, unresolved numeric tokens, truncated review
classes, reviewed-domain violations, non-empty reviewed drop candidates, and
unresolved numeric-list elements. Its only allowed inherited findings are the
still-unapproved complete registry and provisional archive confirmation.

## Candidate evidence

Every raw-column occurrence receives one stable `R####` ID and records:

- hospital, table, exact raw and physical names, and positional occurrence;
- source availability, row counts, and token-class accounting;
- candidate canonical target, kind, and explicit disposition;
- candidate type and unit with separate approval status;
- parser, missing-value, categorical, alias, and semantic-split review needs;
- active raw-v3 rule and evidence IDs where available; and
- all-missing evidence without automatically dropping the field.

Every retained table/target pair receives one `V####` group in the variable
workbook. These groups expose hospital coverage, exact contributing raw names,
cross-hospital name variants, same-hospital alias candidates, type/unit
proposals, and pending review counts. Different raw spellings used by different
hospitals are mapping variants, not merge instructions. Alias/coalescence review
is required only when more than one retained source occurrence maps to the same
target within one hospital and table.

Candidate names come from the frozen-v2 and legacy registries plus the active
raw-v3 overrides. Candidate types use conservative Arrow representations.
Candidate units are supplied only where there is a defensible prior convention;
otherwise the unit remains `unresolved`. Even a supplied unit remains pending
unless an exact raw-v3 rule has approved it.

Policy/artifact `0.3` with candidate contract `0.3` also carries explicit known
semantic-split candidates
whose seed mapping has not yet diverged by hospital. In particular, the frozen
v2 UK00 PBW-related tidal-volume decision is represented as a proposed separate
absolute-volume target with unit `mL`. It remains unapproved and is never
applied by this command. The sanitized report separately counts observed
multi-target source groups and preserved known semantic-split candidates.

## Outputs and privacy

Owner-only output:

```text
asic/reports/<context>/private/harmonization_registry_review/<run_id>/
├── harmonization_registry_review_manifest.json
├── occurrences.parquet
├── variables.parquet
└── README.md
```

The private manifest binds the review to SHA-256 hashes of the source manifest,
sanitized review, column evidence, and bounded token evidence. Raw token examples
remain in the referenced schema/token bundle and are not duplicated.

Sanitized output:

```text
asic/reports/<context>/review/harmonization_registry_review/<run_id>.json
asic/reports/<context>/review/harmonization_registry_review/<run_id>.md
```

The sanitized report excludes exact raw headers, raw tokens, source filenames,
and stay identifiers. It contains only review counts, canonical review classes,
hospital-level totals, and generic blocking checks. Private files remain mode
`0600` in a mode-`0700` directory.

## Human-review sequence

1. Verify `technical_blocking_finding_count=0` in the sanitized report.
2. Review `variables.parquet` for canonical names, types, units, definitions to
   be drafted, alias groups, semantic splits, and hospital availability.
3. Use each variable's source rows in `occurrences.parquet` to review exact
   headers, parsing classes, missing policies, and active rule coverage.
4. Consult bounded examples only in the referenced private
   `schema_token_inventory/.../tokens.parquet`; never print or copy them off the
   cluster.
5. Record approvals or revisions in a new versioned reviewed registry. Do not
   edit generated workbooks or treat them as executable policy.
6. Freeze the ordered union schema and variable dictionary only after every
   occurrence has a reviewed disposition and every applicable unit, parser,
   categorical policy, alias, and semantic split is approved.

Historical input artifacts retain the archive-confirmation blocker that was
correct when they were generated. Review policy `0.3` records the subsequent
data-owner confirmation that the archive is an old snapshot, keeps it excluded
from authoritative inputs, and reports the carried finding as resolved rather
than blocking.
