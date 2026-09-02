# Correlated Multi-Column IN: `tuple_term` / `[a, b] in [[v1,w1], ...]`

- **Issue**: [#52832](https://github.com/milvus-io/milvus/issues/52832)
- **Status**: Draft
- **Date**: 2026-09-01
- **Related**: [20260707-bloom-filter-expression.md](./20260707-bloom-filter-expression.md),
  [20260714-roaring-exact-membership-expression.md](./20260714-roaring-exact-membership-expression.md) —
  precedent for a dedicated plan node with a v1/future-work split between a
  correct primitive and later index acceleration.

## TL;DR

Extend the existing `IN` operator to accept a **list of columns** on the
left-hand side and a **list of tuples** on the right-hand side:

```
[source_review_id, source_response_id] in [["id1","pid1"], ["id2","pid2"]]
[a, b] not in [[1, "x"], [2, "y"]]
```

A row matches iff the tuple formed by reading the listed columns, in order,
equals one of the right-hand tuples **position-for-position**. This is
correlated matching: it is not equivalent to independent per-column `IN`
combined with `AND`, which computes a Cartesian product and admits
combinations the caller never asked for.

New plan node `TupleTermExpr` (`Expr` oneof field 24). Grammar is unchanged —
`Plan.g4`'s `Term` rule already parses an `Array` on either side of `IN`; the
block is today's Go-level rejection in `VisitTerm`
(`internal/parser/planparserv2/parser_visitor.go:1170-1173`,
`"'term' can only be used on single field"`).

**v1 scope, stated up front:** correctness only. Stage-2 exact tuple recheck
via per-row raw-data probe, no stage-1 index-accelerated coarse filter. This
mirrors how `bloom_match` shipped its data-path probe first and deferred
"Index-enumeration probe" to Future Work — same split here, see
[Non-goals](#non-goals--future-work).

## Motivation

Milvus's `IN` is single-column. Combining two of them with `AND` to emulate a
correlated multi-column filter is wrong:

```
source_review_id in ["id1","id2"] and source_response_id in ["pid1","pid2"]
```

matches `("id1","pid2")` too, a pairing the caller never listed. The only
correct workarounds today are a verbose `OR` of `AND`-ed equalities (does not
scale past a few dozen pairs) or concatenating the columns into one synthetic
key column (forces a schema change). Neither is a native, general answer, and
this is a common shape: correlated multi-field keys (e.g.
`(source_review_id, source_response_id)`, `(tenant_id, external_id)`) appear
whenever an external system's composite key is mirrored into Milvus for
filtering.

## Syntax and semantics

```
[<field>, <field>, ...] in [[<value>, <value>, ...], ...]
[<field>, ...] not in [[<value>, ...], ...]
```

- **Negation must be written `[...] not in [...]`** (`NOT` immediately
  before `IN`), matching how single-column `field not in [...]` already
  works. The prefix form `not [...] in [...]` is NOT equivalent and is
  invalid here: this grammar's generic unary `NOT`
  (`op=(ADD|SUB|BNOT|NOT) expr # Unary`) binds to the bracketed array as
  its own primary operand first, under standard operator-precedence
  parsing — so `not [a, b] in [...]` parses as `(not [a, b]) in [...]`,
  not as negation of the whole tuple-membership expression. This is not
  a new limitation this feature introduces: single-column `IN` has the
  identical precedence behavior today (`not Int64Field in [1,2]` is
  likewise invalid; only `Int64Field not in [1,2]` works) — this section
  just makes it explicit for the new bracketed-LHS form, where it is
  easier to write by reflex and get wrong.
- **v1 fields**: top-level scalar fields only (no JSON path, no nested/array
  path, no dynamic field, no `$meta`). Matches the existing per-column `IN`'s
  supported type set (see `isTermExprTargetSupported`).
  JSON-path/array-element tuple members are explicit v1 non-goals (below).
- **Arity**: every right-hand tuple MUST have exactly as many elements as
  there are left-hand columns. A mismatch is a parse-time `InputError`.
- **Columns MUST be pairwise distinct.** `[a, a] in [[1,1]]` is rejected at
  parse time — it is either a caller mistake or means "a in {1}", which the
  existing single-column `IN` already expresses better.
- **Typing**: each tuple's element *i* is cast against column *i*'s declared
  data type at parse time, reusing `castValue` — identical to how each value
  in today's single-column `IN` list is cast.
- **NULL, three-valued**: if *any* participating column is NULL for a row,
  that row does not match, under **both** polarities. `[...] not in [...]`
  does not "recover" NULL rows, exactly like `NOT IN` and
  `not roaring_match(...)` today (`res = valid = false`). Negation is not a
  property of `TupleTermExpr` itself — the existing generic
  `UnaryExpr{Op: Not}` wraps it, unchanged from how single-column `Term` is
  negated (`parser_visitor.go:1229-1239`), so this NULL contract is enforced
  once, in the physical expression's own valid-bitmap handling, not
  duplicated for each polarity.
- **Empty tuple list**: `[a,b] in []` matches nothing, matching
  `IN []` today.
- **Delete expressions**: **allowed**. Unlike `bloom_match`, this predicate is
  exact (no false positives), so it does not need the
  `PlanContainsMembershipFilterUnsafeForDelete` guard
  (`internal/proxy/task_delete.go:359-362`) that keeps approximate Bloom
  filters out of deletes.
- **`element_filter` / `MATCH_*`**: rejected in v1, for the same structural
  reason `roaring_match`/`bloom_match` are — those executors supply
  element-level offsets where a row-offset prober expects segment row
  offsets. Revisit together with any future element-level membership work,
  not as a one-off carve-out here.

## Plan IR

```proto
message TupleTermExpr {
  // Every column MUST be a top-level scalar field, pairwise distinct.
  repeated ColumnInfo columns = 1;
  // Each Array is one candidate tuple; Array.array MUST have exactly
  // len(columns) elements, already cast to each column's declared type.
  repeated Array tuples = 2;
}
```

added to `Expr.oneof expr` as field 24 (next after `roaring_filter_expr = 23`).
Reuses the existing `Array`/`GenericValue` messages already used by
`TermExpr.values` and already capable of nesting (`GenericValue.array_val`
recurses) — no new leaf value type. No `is_not` flag: negation reuses the
existing generic `UnaryExpr` wrapper, so nothing about tuple negation needs
representing on this message at all.

No `milvus-proto` (client-facing) change: `plan.proto` is local to this repo
(`pkg/proto/plan.proto`), and both the literal-array RHS above and a future
`{template}`-based RHS are already representable — `schemapb.TemplateArrayValue`
already has a recursive `ArrayData` variant, and
`internal/parser/planparserv2/convert_field_data_to_generic_value.go:73-74`
already lowers it into a nested `planpb.GenericValue`. Template-value RHS is
not implemented in v1 (see Non-goals) but nothing blocks it later.

## Parser changes (Go)

All in `internal/parser/planparserv2/`:

1. `VisitTerm` (`parser_visitor.go:1159`): dispatch on the raw parse tree
   *before* generically visiting the LHS — `ctx.Expr(0)`'s underlying type is
   checked for `*parser.ArrayContext` whose elements are all plain
   `*parser.IdentifierContext` (`isTupleTermLHS`), and only then routed to
   the new builder. This has to happen before the existing
   `ctx.Expr(0).Accept(v)` call (not after, and not via the existing
   `toColumnInfo(childExpr) == nil` check at line 1170-1173): `VisitArray`
   unconditionally rejects any non-constant element
   ("array element type must be generic value"), so `[a, b]` would already
   fail inside `VisitArray` before `VisitTerm`'s own single-column check
   ever ran. Single-column behavior is untouched; a non-identifier bracketed
   LHS (e.g. `[1, 2] in [...]`) still falls through to today's existing
   error, unchanged.
2. New `tuple_term_filter.go` (mirrors the shape of `membership_filter.go`):
   `isTupleTermLHS` (the parse-tree-level guard above) and
   `(*ParserVisitor).buildTupleTermExpr(ctx, arrayCtx)` — resolves each
   LHS identifier to a `ColumnInfo`, validates column distinctness and
   per-tuple arity, casts each tuple element against its column's type
   (reusing `castValue`), and emits `planpb.Expr_TupleTermExpr`. The
   existing `ctx.GetOp() != nil` → `UnaryExpr{Not}` wrapping in `VisitTerm`
   covers `[...] NOT IN [...]` unchanged (NOT must be written immediately
   before IN, see the syntax note above — `not [...] in [...]` is a
   different, invalid parse under this grammar's precedence).
3. `plan_redact.go`: no change. It only elides template-variable-bearing
   slots, and `TupleTermExpr` has no template variable in v1 (literal arrays
   only) — nothing to redact. Revisit if/when a templated tuple-list RHS
   (see Non-goals) is added.
4. `internal/proxy/task_delete.go`: no code change — confirming (and
   asserting in tests) that `TupleTermExpr` is simply not one of the cases
   `PlanContainsMembershipFilterUnsafeForDelete` matches, so it is delete-eligible
   by construction, not by an added carve-out.

## Execution changes (C++ segcore)

New `exec/expression/TupleTermExpr.{h,cpp}`: `PhyTupleTermFilterExpr`, a
`SegmentExpr` reading each participating column's raw value + validity per
row (batched, following the same contract `PhyMembershipFilterExpr` uses in
`MembershipFilterExpr.{h,cpp}`) and testing the row's tuple against a
canonically-encoded hash set built once from `TupleTermExpr.tuples` at parse
time. New IR class `expr::TupleTermFilterExpr` in `ITypeExpr.h`, next to
`TermFilterExpr`/`RoaringFilterExpr`/`BloomFilterExpr`. Wired into
`query/PlanProto.cpp` (`ParseTupleTermFilterExprs`, `case
ppe::kTupleTermExpr:`) and `exec/expression/Expr.cpp` (factory dispatch +
the same post-selective optimizer tier Bloom/Roaring already join, so cheaper
indexed predicates prune `bitmap_input` first).

v1 requires raw field data for every participating column — a sealed segment
loaded index-only for any of them fails with a clear `FieldNotLoaded`-style
error rather than attempting an unindexed reverse lookup. Growing segments
and sealed segments with raw data work identically (both are raw-data
probes).

## Non-goals / Future work

- **Stage-1 index-accelerated coarse filter.** The issue's own proposal is a
  two-stage design (per-column index bitmap intersection, then exact
  recheck). v1 ships stage 2 only — a correct brute-force per-row probe —
  exactly as `bloom_match`'s data-path-only v1 deferred its
  "Index-enumeration probe" to Future Work. Given this predicate's brute-force
  path is already exact, the future stage-1 optimization is a pure
  performance improvement over a correct baseline, not a correctness
  prerequisite, so it is safe to ship independently once index-capability
  negotiation (which columns have a fast, reverse-lookup-capable index) is
  designed.
- **JSON paths, array elements, dynamic fields** as tuple members — out of
  scope, same reasoning `bloom_match`/`roaring_match` used for their own
  scoped field-type non-goals.
- **Template-value RHS** (`[a,b] in {tuples}`, for large tuple counts without
  inflating the expression string) — structurally unblocked (see Plan IR
  above) but not implemented in v1; literal arrays only.
- **`element_filter` / `MATCH_*` support** — deferred with the rest of this
  codebase's element-level membership gap, not specific to this feature.

## Testing

- `internal/parser/planparserv2/tuple_term_filter_test.go`: arity mismatch,
  duplicate LHS column, per-position type-cast failure, NULL in a tuple
  literal, single-column `IN` still takes the old path unchanged, `NOT
  [...] IN [...]`, delete-expression eligibility.
- `internal/core/src/exec/expression/ExprTupleTermTest.cpp`: 2- and
  3-column exact match; a row matching some-but-not-all columns must NOT
  match (proves this isn't independent per-column `IN`); NULL in one
  participating column excluded under both polarities; empty tuple set;
  growing segment; sealed segment with raw data; sealed index-only segment
  fails closed with a clear error.
- Full `make test-go` once the wire/plan contract lands, per this repo's
  contract-change testing rule.
