# Correlated Multi-Column IN, v1: Lowering to AND/OR/Equality

- **Issue**: [#52832](https://github.com/milvus-io/milvus/issues/52832)
- **Status**: Draft
- **Date**: 2026-10-12
- **Supersedes, for v1 only**: [20260901-tuple-term-membership-expression.md](./20260901-tuple-term-membership-expression.md),
  which designed a dedicated `TupleTermExpr` plan node and segcore execution
  operator. That design is not wrong -- it is scoped as the natural follow-up
  once a workload exists that needs it (see Non-goals / Future work below) --
  but per discussion with @xiaofan-luan on the issue, v1 ships as pure
  syntactic sugar instead, with no new plan node and no new segcore code.

## TL;DR

```
[source_review_id, source_response_id] in [["id1","pid1"], ["id2","pid2"]]
```

lowers, entirely at Go parse time, into exactly what a caller would otherwise
have to write by hand:

```
(source_review_id == "id1" AND source_response_id == "pid1")
  OR
(source_review_id == "id2" AND source_response_id == "pid2")
```

No new `Expr` oneof field, no new C++ execution class. The only change is in
`internal/parser/planparserv2/`: `VisitTerm` dispatches a bracketed,
multi-identifier left-hand side to a new builder
(`tuple_term_filter.go`) that emits a tree of the plan's existing
`UnaryRangeExpr` (`column == literal`) and `BinaryExpr`
(`LogicalAnd`/`LogicalOr`) nodes, reusing the same folding helpers
(`foldAndOperands`/`foldOrOperands`) the grammar's own `AND`/`OR` rules
already use.

## Motivation

Recap from the issue: `a in [...] and b in [...]` computes the Cartesian
product of the two independent sets, not the correlated pairs the caller
actually meant -- it wrongly matches `(id1, pid2)` when only `(id1,pid1)` and
`(id2,pid2)` were asked for. The only correct workaround today is writing the
OR-of-ANDs form out by hand, which does not scale past a few dozen pairs to
type. This feature makes the correlated form a first-class, validated syntax;
v1 answers *how it executes*, not *whether the syntax exists* -- that part is
unchanged from the original design.

## Why lowering instead of a dedicated operator, for v1

Milvus's execution engine (segcore) has several storage-layout-specific code
paths for reading a column's raw data -- growing vs. sealed, chunked vs. not,
mmap vs. not -- and these paths are genuinely easy to get subtly wrong
without a full local C++ build to test against. This is not hypothetical: an
earlier attempt at the dedicated-operator version of this feature shipped a
real bug doing exactly this (reading a VARCHAR column on a sealed/chunked
segment the same way as a fixed-width numeric column), caught only by CI.

`==`, `AND`, and `OR` are the most exercised code paths in the entire engine.
They already handle every one of those storage layouts correctly, because
ordinary `WHERE` filtering depends on it. Lowering to them means v1 inherits
that correctness instead of re-deriving it in new code, at the cost of two
specific, bounded tradeoffs (below) -- not a free lunch, a deliberate trade.

### What lowering gets for free, beyond "less code to write"

- **Three-valued NULL handling, and arguably more precise than a hand-rolled
  version would get it.** `ConjunctExpr.cpp`'s `BuildActiveBitmap` implements
  genuine three-valued AND/OR (`NULL AND FALSE = FALSE`, `NULL OR TRUE =
  TRUE`). A row with one NULL column is not automatically "unknown": if the
  other columns already rule out every candidate tuple, the row correctly
  resolves to definite `false`; "unknown" is only produced when a NULL
  column creates genuine per-tuple ambiguity. A hand-rolled operator that
  takes the simpler shortcut "any participating column NULL => whole row
  unknown" (which is what the superseded design did, matching
  `roaring_match`'s convention) is *more conservative* than this, not more
  correct -- SQL's actual row-value-constructor NULL semantics are the
  finer-grained behavior the lowering produces automatically.
- **Partial, per-branch index use for free.** Each `column == literal`
  equality is an ordinary indexed-equality predicate -- identical in shape to
  any other `WHERE column = 'x'` -- so `ReorderConjunctExpr` (`Expr.cpp`)
  already buckets it as `indexed_expr` when the column has a scalar index,
  running it early and pruning `bitmap_input` for later predicates. This is
  not the issue's proposed stage-1 coarse filter (there is no single
  intersected candidate set shared across all tuples), but it is a real,
  automatic, per-column approximation of it.
- **Delete-expression eligibility for free.** Ordinary filter expressions are
  already usable in delete expressions; there is no need to separately audit
  and prove the new syntax isn't caught by a membership-filter-specific
  delete-safety guard (`PlanContainsMembershipFilterUnsafeForDelete`), the
  way the Bloom-adjacent dedicated-operator design would have needed to.
- **Almost all of the validation work carries over unchanged.** LHS
  detection (`isTupleTermLHS`), per-column resolution, arity/distinctness/
  type-cast validation, and their rejection-path tests are identical to the
  superseded design; only the final step -- which plan node(s) to emit --
  changes.

### What lowering costs -- stated plainly, not glossed over

- **Plan-size / wire-size growth with tuple count is not eliminated, only
  relocated.** The user's query text stays compact, but the expanded plan
  sent to every QueryNode contains N (tuples) x M (columns) comparison nodes.
  For small-to-moderate tuple counts this is irrelevant; for the "thousands
  of pairs" scale the original issue's two-stage design was aimed at, this
  reintroduces the same cost at the plan level that the dedicated operator
  was meant to avoid at the query-text level.
- **No cross-tuple sharing of work.** A single-column `IN [v1..vN]` probes a
  hash set once per row, O(1) regardless of N. N lowered AND-branches have no
  such sharing: in the worst case, matching against the k-th tuple costs k
  comparisons, with no early exit across branches (only within one AND
  clause). This is the concrete performance gap a dedicated tuple-membership
  operator (hash-set probe) would close.
- **One explicit edge case, not automatic:** `[a,b] in []` lowers to an OR
  with zero disjuncts, which is not a valid tree shape on its own. v1 folds
  this directly to the same constant-false node the parser's existing
  boolean-literal folding already produces for `false and expr`
  (`alwaysFalseExpr()` in `utils.go`) -- a deliberate, tested special case,
  not an oversight left for later.

## Syntax and semantics (unchanged from the superseded design)

```
[<field>, <field>, ...] in [[<value>, <value>, ...], ...]
[<field>, ...] not in [[<value>, ...], ...]
```

- Negation must be written `[...] not in [...]` (NOT immediately before IN),
  matching single-column `field not in [...]` and for the identical grammar-
  precedence reason: the generic unary `NOT` binds to the bracketed array
  first (`(not [a,b]) in [...]`), not to the whole `in` expression.
- v1 fields: top-level scalar fields only (no JSON path, no nested/array
  path, no dynamic field) -- `typeutil.IsPrimitiveType`.
- Columns must be pairwise distinct; every tuple's arity must match the
  column count; each tuple element is cast against its column's declared
  type (`castValue`), identical to single-column `IN`.
- Delete expressions: allowed (see above).

## Plan IR

None. `[a,b] in [[v1,w1],...]` lowers to the same `Expr` oneof cases every
other filter expression already uses:

```
BinaryExpr{ Op: LogicalOr,
  Left:  BinaryExpr{ Op: LogicalAnd,
           Left:  UnaryRangeExpr{ ColumnInfo: a, Op: Equal, Value: v1 },
           Right: UnaryRangeExpr{ ColumnInfo: b, Op: Equal, Value: w1 } },
  Right: BinaryExpr{ Op: LogicalAnd, ... for [v2,w2] ... },
}
```

A single tuple lowers directly to one `BinaryExpr{LogicalAnd}` (no 1-way OR
wrapper) -- `foldOrOperands` already returns its single operand unchanged
when given one. An empty tuple list lowers to `UnaryExpr{Op: Not, Child:
AlwaysTrueExpr{}}`, matching `alwaysFalseExpr()`.

No `milvus-proto` (client-facing) or `pkg/proto/plan.proto` change at all --
every node this lowering emits already exists on the wire today.

## Parser changes (Go)

All in `internal/parser/planparserv2/`:

1. `VisitTerm` (`parser_visitor.go`): dispatch on the raw parse tree, before
   generically visiting the LHS, exactly as before -- `ctx.Expr(0)`'s
   underlying type is checked for a bracketed list of two or more plain
   identifiers (`isTupleTermLHS`) before falling through to the existing
   single-column path. `VisitArray` would otherwise reject `[a, b]` outright
   ("array element type must be generic value"), before `VisitTerm`'s
   single-column check ever ran.
2. `tuple_term_filter.go`: `isTupleTermLHS` (unchanged) and
   `buildTupleTermExpr`, whose tail now builds the lowered OR-of-ANDs tree
   from the validated `columns`/`tuples` instead of a `TupleTermExpr` plan
   node, using `castValue` (per-element casting, unchanged) and
   `foldAndOperands`/`foldOrOperands` (the same helpers `AND`/`OR` grammar
   rules use) to assemble it. `NOT` continues to reuse the generic
   `UnaryExpr{Op: Not}` wrap, applied to the finished lowered tree.
3. No C++ change. No `plan_redact.go` change (nothing new carries a template
   variable or blob to redact). No `task_delete.go` change (nothing to
   carve out; see Motivation above).

## Non-goals / Future work

- **A dedicated tuple-membership operator** (hash-set probe, O(1) per row
  regardless of tuple count), with a stage-1 scalar-index coarse filter
  ahead of it -- the issue's original two-stage proposal. Real, scoped
  follow-up work once a workload exists where the plan-size/evaluation-cost
  growth this doc states above actually matters. An earlier, CI-validated
  (after one real bug fix) implementation of the dedicated operator exists
  on a separate branch and is a reasonable starting point for that
  follow-up, not discarded.
- JSON paths, array elements, dynamic fields as tuple members -- same v1
  non-goal as the superseded design, for the same reason (scope).
- A `{template}` right-hand side for very large tuple counts -- unneeded
  while the lowering approach is in effect (there is no compact wire
  representation to template into; a dedicated operator would be the point
  at which this becomes relevant again).

## Test plan

- `internal/parser/planparserv2/tuple_term_filter_test.go`: structural
  assertions that 2+ tuples lower to `BinaryExpr{LogicalOr}` of
  `BinaryExpr{LogicalAnd}` of `UnaryRangeExpr{Op: Equal}` (not just "parses
  without error"); a single tuple lowers to a bare AND with no OR wrapper;
  the empty-list case lowers to `UnaryExpr{Not, AlwaysTrueExpr}`; `NOT`
  wraps the finished lowered tree; the prefix-NOT grammar-precedence trap;
  every arity/distinctness/type-cast/JSON-path/non-primitive rejection path;
  delete-expression eligibility.
- **Revised per PR review**: "no new C++ code" is true, but it does not mean
  "no new C++ test needed." Each primitive (`AND`, `OR`, `UnaryRangeExpr`) is
  individually well-tested in isolation, but the specific *composition* this
  feature introduces -- an OR over several multi-column AND branches, each
  potentially touching a NULL column in a different tuple -- is new, and the
  NULL-semantics claims above were originally verified only by reading
  `ConjunctExpr.cpp`'s logic, not by running it. Three-valued-logic
  propagation is notoriously easy to get subtly wrong by hand-tracing,
  including by the person making the claim.
  `internal/core/src/exec/expression/TupleTermLoweringExprTest.cpp` closes
  this gap: it builds the exact lowered IR tree directly (bypassing the Go
  parser and plan-proto text), runs it against a real nullable growing
  segment, and asserts the specific truth-table claims above end to end --
  including the sharp case that distinguishes genuine ambiguity from a
  definite answer that merely involves a NULL column
  (`(NULL, 3)` against `[[1,1],[2,2]]` is definite `FALSE` for `IN`/`TRUE`
  for `NOT IN`, not `UNKNOWN`), and a multi-tuple case showing that adding a
  tuple can turn a definite `FALSE` into a genuine `UNKNOWN`.
  The empty-tuple-list case is not included in that C++ test: it folds to a
  pre-existing constant-`AlwaysTrueExpr`/`NOT` evaluation path (already
  covered structurally by the Go-level parser test), not the new AND/OR
  composition this file is about.
