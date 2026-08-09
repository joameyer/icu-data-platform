# Medication zero and missing-value semantics

## Decision

Medication and inhaled-therapy values use three distinct states:

- an explicit finite numeric zero is preserved as an observed zero;
- a null means unavailable or unobserved and is never imputed as zero; and
- a positive value is a candidate observed exposure, without changing the
  underlying value.

An explicit zero is not automatically labelled as an infusion stop. A null is
not evidence that treatment was inactive. Hospitals that do not supply a
variable and missing cells in hospitals that sometimes supply it both remain
null. Harmonization conversions and semantic splits must conserve zero and
null independently.

Downstream bolus or infusion exposure may use `value > 0` as an explicitly
derived candidate. Carry-forward, interval construction, or an assumption that
a previously observed infusion continues requires a separate reviewed
derivation contract. None is activated here.

## Complete audit

The production audit scans all 24,069,379 released cleaned dynamic rows for 25
source medication or therapy variables. It creates exact aggregate counts for
every one of the 200 hospital-variable combinations:

- null;
- finite zero;
- finite positive;
- finite negative; and
- non-finite.

It also classifies all-missing hospital-variable combinations, profiles where
zero and positive values coexist, the four approved medication conversion
scopes, and all nine semantic splits. The UK00 weight-dependent vasopressin
conversion verifies that every nonzero value has a valid linked weight; zero
remains zero even though multiplication by weight is unnecessary to establish
that result.

The private workbooks contain aggregate hospital-variable counts only. The
sanitized report contains variable-level totals. Neither contains patient rows,
stay identifiers, raw tokens, or filenames.

## Boundary

The audit writes reports only. It does not change a clinical value, impute a
null, remove a zero, infer medication state, apply carry-forward, activate unit
conversions or semantic splits, freeze contract `0.2`, or modify an existing
release.

## Reviewed general negative-value rule

The complete audit found 201 finite negative values, all in UK08
`inhaled_no`, across seven distinct values from -1.8 to -0.1. This is not a
single missing sentinel and there is no deterministic recovery.

The data owner approved one dictionary-driven cleaning rule for all 34 numeric
medication/therapy variables in proposed contract `0.2`:

- preserve finite negative values in ingestion and harmonization;
- after hospital harmonization and semantic splitting, set every finite value
  below zero to null in the cleaned layer;
- preserve zero, positive values, and existing nulls unchanged;
- report counts by hospital and variable;
- never use absolute value or clip a negative value to zero; and
- require an explicit exemption before any future legitimately signed variable
  can be classified as a medication/therapy field.

The current expected cleaning count is 201. The rule is approved for cleaning
policy `0.2` implementation but is not activated against any existing release.
