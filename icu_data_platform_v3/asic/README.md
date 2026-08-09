# ASIC v3

ASIC v3 uses the following stage order:

1. raw inventory and hospital mapping;
2. lossless per-hospital ingestion;
3. raw-schema and parsing-token inventories;
4. auditable hospital-aware harmonization;
5. approved union schema and validated concatenation;
6. pooled harmonized contract and variable dictionary;
7. cross-hospital and row-level clinical cleaning;
8. derived datasets and optional time blocking.

Steps 1 and 2 are implemented. Step 2 operates on one explicitly selected
hospital per command and refuses to overwrite an existing artifact. A separate
complete-set post-ingestion audit now verifies all hospital outputs before the
parsing gate. It hashes each Parquet file and streams provenance only; it never
materializes clinical columns. The word *pooled* is reserved for the result of
validated concatenation. The old external pooled Parquet files are an
untrusted, read-only comparison source and never an authoritative v3 input.
Translation remains traceable inside harmonization rather than being published
as a separate durable dataset.

Step 3 is implemented as an immutable read-only report stage. Candidate v2 and
legacy names are evidence seeds only; the command does not apply them. Exact
raw headers and token examples remain in owner-only cluster reports.

The first part of Step 4 is implemented as a read-only candidate-registry
review. It accounts for every raw occurrence and proposes dispositions, types,
units, parser needs, categorical policies, aliases, and semantic splits in
owner-only workbooks. A separately bounded static-contract audit re-hashes and
streams only eight selected static candidates to collect full-scan evidence for
their first human-review batch. Neither command can approve a registry or
generate released harmonized artifacts.

The second part of Step 4 is an explicitly approved streaming dry run. It reads
only immutable lossless-ingestion artifacts and evidence-bound decision files,
then writes one run-scoped candidate directory. Every hospital receives the
same ordered static schema and the same ordered dynamic schema. The command
preserves row provenance and row counts, refuses overwrite, writes private
token/domain evidence separately, and marks all outputs non-publishable. It
does not clean, filter, derive, concatenate hospitals, or advance a release
pointer.

The following consolidated audit is also implemented. One read-only Slurm job
re-hashes and profiles every candidate hospital and covers every unresolved unit
policy and selectively migrated legacy-QC domain using a v3-owned rule registry.
It has no runtime dependency on v2. An older-version comparison can be enabled
separately as optional, untrusted context, but it is disabled in the shipped
configuration. The job writes an owner-only decision register and sanitized
review report, but cannot mutate or release harmonized data.

After promotion of the immutable harmonized, cleaned, and core-derived `0.1`
releases, one additional consolidated unit-resolution audit profiles all 72
analysis-ineligible unresolved-unit variables and fully accounts for the
12-variable legacy-range subset. It writes reports and an owner-only decision
workbook only; distributions cannot establish units, and no value or release is
changed.

Complete candidate unit decisions `0.2-candidate.1` now assign an expected
unit to all 72 variables and keep availability separate from unit eligibility.
The register records which decisions came from a prior ASIC analysis
configuration, data-owner confirmation, or clinical/distribution inference.
Because the hospitals supplied no complete unit dictionary, every future
dictionary must state that these are team-derived assumptions. Nine proposed
hospital conversions and nine unrecoverable site masks remain inactive until
the data owner approves the complete register and a consolidated row-level
audit passes. Existing `0.1` releases remain immutable.

That consolidated unit-decision audit is now implemented as one low-resource,
read-only production job. It profiles the 16 affected variables at all eight
hospitals, evaluates every conversion before and after against unaffected-peer
distributions, and validates exact UK00 vasopressin-to-static-weight linkage.
It writes aggregate review evidence only and cannot activate contract `0.2` or
change any released value.

Corrected audit run `20260807T083117Z` passed, and the data owner approved its
nine conversions. The original nine masking proposals are superseded by nine
harmonization semantic splits: six resolved alternate-unit variables and three
explicitly unresolved hospital-scoped source variables. All source values are
retained through cleaning. Reviewed unit decision contract `0.2` authorizes
schema planning but not schema freeze, clinical builds, release changes, or
external export.

Medication values additionally retain explicit zero separately from null
across all 25 source medication/therapy variables and nine semantic-split
targets. Null is never inferred as zero or inactive treatment. The combined
medication-semantics and schema-review job performs complete aggregate
zero/null/sign accounting and writes reports only before contract `0.2` can be
frozen. See
[the medication value-semantics contract](docs/medication_value_semantics.md).
The same reviewed contract assigns a nonnegative domain to these fields and
authorizes a general cleaned-layer negative-value mask for cleaning policy
`0.2`; it does not change an existing release.

Frozen contract `0.2` is now the exact target of a bounded harmonized rebuild.
The new builder reads immutable harmonized release `20260806T111156Z` and, only
for the audited UK00 weight-linked vasopressin conversion, immutable cleaned
static `weight_kg` from release `20260806T114234Z`. It applies all nine
hospital-scoped conversions and nine semantic splits in bounded Arrow batches.
A separately implemented audit recomputes every output cell. The candidate
remains non-publishable until an exact human release approval. Candidate
`20260807T112402Z` has now passed the complete audit and received that exact
approval. The bounded promotion copies audited bytes unchanged, snapshots the
previous current pointer, preserves release `20260806T111156Z`, and advances
only the recoverable current pointer. See
[the harmonized 0.2 build contract](docs/harmonized_0_2_build.md).

Cleaning `0.2` is now implemented as one bounded candidate build followed by
an independent complete audit. It replays the approved `0.1` policy, replaces
the 12 formerly unit-unresolved rules with the reviewed unit-aware matrix, and
masks finite negative values across all 25 source medication fields and nine
semantic-split targets while preserving explicit zero separately from null.
The candidate includes machine-readable and Markdown cleaned-variable
dictionaries. Candidate `20260807T154100Z` passed its complete audit and has
received exact human promotion approval; the approval-bound promotion copies
all four audited payloads byte-for-byte while preserving the prior cleaned
release. See [the cleaned 0.2 contract](docs/cleaning_0_2.md).

Core-derived contract `0.2` retargets the already reviewed `0.1` recipes to
cleaned release `20260807T154100Z` without changing a formula or output
semantic. Candidate `20260808T074305Z` passed its full audit while preserving
all cleaned `0.2` columns and received exact data-owner promotion approval.
The immutable promotion preserves derived release `20260806T170134Z` and
advances only the recoverable current pointer. See
[the derivation contract](docs/derivation.md).

The optional 8-hour time-blocking contract `0.1` is frozen against technically
passing evidence run `20260808T114902Z`. A policy-driven bounded-memory engine
and separate every-cell audit are implemented. Candidate and audit run
`20260808T130534Z` passed the complete independent comparison with zero
technical blockers and received exact immutable promotion approval. The recipe
retains negative pre-admission windows, an exact-admission
singleton,
right-labelled post-admission windows, empty blocks, and terminal partial
blocks. Medication dose totals remain deferred. The promotion copies the
audited candidate bytes unchanged, references static data through core-derived
lineage, and may create only the nested 8-hour current-release pointer. See
[the frozen 8-hour contract](docs/time_blocking_8h_contract.md) and
[its source evidence](docs/time_blocking_contract_evidence.md).

Reviewed consolidated decisions `0.1` bind the technically complete candidate
and audit runs to the owner-approved unit, conversion, semantic, all-missing,
and cleaning-boundary policies. The separate categorical checkpoint reads only
immutable audit evidence; it does not rescan candidate data. Complete
categorical contract `0.1` is now approved. Ordered-schema, dictionary, and
publication decisions remain explicit gates and are presented together by one
final report-only checkpoint.

A synthetic-only complete-registry contract and Arrow-schema planner are also
implemented. They prove exact occurrence coverage and schema invariants but do
not ship a complete registry or enable transformations.

Partial static decision registry `0.2` records the approved UK00 mortality,
weight, and hospital-LOS rules plus local/global static stay-ID construction.
It supports synthetic value-level tests and the explicitly bounded candidate
dry run; release concatenation and union-schema approval remain disabled.

Registry `0.3` extends that immutable base with the approved hospital-scoped
ICU length-of-stay, dialysis-free-day, and ventilator-free-day rules. Empty-only
and sentinel-only hospital sources remain evidence-bound: a newly appearing
numeric value blocks rather than being accepted automatically.

The remaining-static review reads only the immutable first static-audit bundle
and proposes contracts for ICU length of stay, dialysis-free days,
ventilator-free days, and ICD-10 source strings. It writes sanitized review
reports only and cannot approve or execute those proposals.

The follow-up ICD-10 notation audit scans only the selected static source-text
column, stores exact examples privately, and reports syntax-feature counts
without parsing or altering codes.

After notation review, the ICD-10 component audit verifies the approved UK03
`nan` missing rule and accounts for every candidate comma-separated list
component. It remains report-only; parser activation and harmonized data writes
stay disabled pending review.

The component-detail audit then classifies empty delimiter positions and
duplicate structure and computes counterfactual ordered-deduplication counts.
Exact anomaly examples remain private, and no candidate operation is applied.

After explicit review, ICD-10 contract `0.2` activates those operations only in
the bounded Arrow parser. It emits exact nullable source text, an ordered unique
code list, parse status, and conserved rule counts. The candidate dry run may
execute it; no released harmonized artifact writer is enabled.

Configuration is separated by dataset context. Production output paths point
only into v3-owned `asic/data/`, `asic/reports/`, and `asic/runs/` trees.
Generated or protected content is excluded from the source repository.
