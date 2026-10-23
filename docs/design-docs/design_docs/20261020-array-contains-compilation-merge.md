# ARRAY contains compilation merge for Milvus 2.6

## Context and scope

[Issue #51996](https://github.com/milvus-io/milvus/issues/51996) and
[master PR #52138](https://github.com/milvus-io/milvus/pull/52138) introduce
physical ARRAY predicate merging. The implementation was already backported to
3.0 by [PR #52365](https://github.com/milvus-io/milvus/pull/52365). This change
backports that rule to 2.6 and adapts the rewrite traversal for long filters.
It changes expression compilation; it does not change the query protocol or
require client-side filter rewriting.

A synthetic filter with 2,216 same-field `ARRAY_CONTAINS` leaves can otherwise
retain thousands of executable predicates. Recursively rewriting and then
flattening every prefix of a left-deep chain also repeats work quadratically.
The goal is one compatible Any/All predicate and no repeated prefix rewrite.

## Rules and identity

On a physical ARRAY column:

- `contains(a, x) OR contains(a, y)` becomes `contains_any(a, [x, y])`.
- `contains(a, x) AND contains(a, y)` becomes `contains_all(a, [x, y])`.
- Compatible existing Any/All leaves join the corresponding group. At least two
  leaves must qualify; incompatible operands remain in the expression.

Field ID and nested path locate candidate groups. Complete protobuf
`ColumnInfo` equality is then required, including element type and nullability;
conflicting metadata is preserved without merging. JSON columns are excluded.
The merged node occupies its first source position. Values retain first-seen
order and are deduplicated using the executor's FLOAT/DOUBLE conversions while
retaining the first literal representation. Unknown, nil, array-valued, and NaN
elements are not merged.

## Traversal and templates

The visitor first flattens the entire same-operator chain, then visits each
operand once. If simplifying a child exposes another chain of the same
operator, that result is flattened without revisiting its operands. Existing
2.6 rewrite-pass ordering is retained. Remaining operands are folded into a
balanced tree with their order and template flags preserved. NOT, opposite
logical operators, and non-binary wrappers remain boundaries; this is not a
rewrite across arbitrary Boolean expressions.

The production `ParseExpr` path fills template values successfully before
rewriting. In 2.6, `FillJSONContainsExpressionValue` retains the template name and
`IsTemplate` flag. An unfilled Any/All placeholder has no elements and
`ElementsSameType=false`; filling even an empty list sets that field to `true`.
The merge explicitly rejects the pending-list shape. Pending scalar Contains
is rejected by the one-element arity requirement. Filled merged predicates
clear consumed template metadata. Tests exercise this specific 2.6 contract
rather than assuming that a retained template name means an unfilled value.

## NULL and empty arrays

For nonempty candidate lists, merging preserves nullable membership semantics,
including unknown results under an outer NOT. A valid empty ARRAY remains
distinct from a NULL row. The executor treats an empty Any list as false and an
empty All list as true, including its constant-result path for NULL rows; these
are respectively the OR and AND identities when combined with other leaves.
The rewrite does not introduce additional domain-wide true/false folds.

## Compatibility and validation

The backport retains the upstream Python query-equivalence case. Go imports
use v2; the upstream BytesVal and struct-array test cases are omitted because
those APIs are absent from this 2.6 branch. No runtime dependency is added.

Coverage includes typed numeric deduplication, different fields and paths,
conflicting column metadata, pending and filled templates, NULL and empty-list
truth tables, NOT/mixed operators, newly exposed same-operator chains, and
synthetic 2,164/2,216-leaf parsed filters. A distinct-field case checks that
unmergeable leaves remain ordered and the output stays balanced.

The full `internal/parser/planparserv2/...` regression suite runs with
`-tags dynamic,test -gcflags='all=-N -l'`. The standalone rewrite benchmark uses
1,000/2,164/2,216 leaves and reports time, allocations, and output leaf count on
matched base and patched builds. It measures compilation work, not end-to-end
search throughput or production memory savings. The Python test has passed an
AST check and remains identical to the upstream case; cluster e2e execution is
not claimed by this backport's local validation.

## Rollout

The rule follows the existing `common.enabledOptimizeExpr` switch and its
current default. Disabling optional expression optimization preserves the
original contains leaves. Release review should retain the existing parser
regressions and query-equivalence coverage when moving the backport to a new
2.6 base.
