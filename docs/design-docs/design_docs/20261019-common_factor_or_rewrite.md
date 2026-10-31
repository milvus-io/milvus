# MEP: Common-factor extraction from OR branches in the expression rewriter

- **Created:** 2026-10-19
- **Author(s):** @pardeep-singh
- **Status:** Draft
- **Component:** Proxy (`internal/parser/planparserv2/rewriter`)
- **Related Issues:** #54388 (EX01)

## Summary

Add a rule to the Go expression rewriter that pulls predicates shared by
every branch of an OR out in front of it:

```text
(tenant == 7 AND price > 100) OR (tenant == 7 AND rating > 4)
  →  tenant == 7 AND (price > 100 OR rating > 4)

tenant == 7 OR (tenant == 7 AND price > 100)
  →  tenant == 7
```

The rule runs only when `common.enabledOptimizeExpr` is on. It is bounded,
deterministic and keeps the TRUE/FALSE/UNKNOWN result of every row. This is
the Stanford CS146S catalog project EX01. No plan format, segcore or public
API change is needed.

## Motivation

Filters assembled from several alternatives often repeat a tenant, status or
access-scope condition in every alternative. The rewriter already merges
same-column predicates, but nothing removes a predicate repeated across OR
branches, so the plan carries one copy per branch.

Segcore evaluates an AND/OR chain as a `PhyConjunctFilterExpr`. It runs
column predicates before nested conjunctions (`ReorderConjunctExpr`) and only
evaluates later operands on rows the earlier ones did not decide. With the
factor pulled out, the shared predicate is one plan node that runs once per
batch, and the residual OR only runs on rows where it is not FALSE. With the
factor repeated, each branch evaluates its own copy.

## Rule

For one flattened OR chain with operands `P1 ... Pn`:

1. Split every `Pi` into its AND conjuncts. An operand that is not an AND is
   a branch with one conjunct.
2. Compute a structural key for every eligible conjunct (see Eligibility).
   A key repeated inside one branch counts once.
3. `F` is the set of keys present in every branch. If `F` is empty, the OR is
   unchanged.
4. If some branch consists only of conjuncts in `F`, the result is `AND(F)`
   (absorption).
5. Otherwise, the result is `AND(F, OR(R1 ... Rn))`, where `Ri` is branch `i`
   without the conjuncts in `F`.

Factors keep the order of their first occurrence in the first branch;
residuals keep branch order. Only factors shared by all branches are
extracted. Nothing is distributed into other branches, and no DNF/CNF
expansion is attempted.

## Eligibility and structural equality

A conjunct can be a factor only if it is one of:

- `UnaryRangeExpr` with `==`, `!=`, `<`, `<=`, `>`, `>=`, no extra values and a
  concrete bool/int64/float/string literal;
- `BinaryRangeExpr` with concrete scalar bounds;
- `TermExpr` (`IN`) with a non-empty value list and `is_in_field = false`;

and its column is an ordinary top-level scalar field: data type bool,
int8–int64, float, double, string or varchar, no `nested_path`, and
`is_element_level = false`. This excludes JSON paths, the dynamic field, array
elements and element-level struct fields.

Everything else stays in its branch and never crosses the OR: `NOT`,
`IS NULL`, LIKE/regex/text/phrase match, `MATCH_*`, `element_filter`, JSON and
array contains, arithmetic, column comparisons, GIS, membership filters and
calls. An ineligible conjunct does not block factoring of other conjuncts in
the same branch.

The key encodes the whole `ColumnInfo` (field ID, data type, element type,
nullable, primary/auto-ID/partition/clustering flags), the operator or the
inclusivity flags, and each literal with its `GenericValue` kind. So
`int64 7` and `float 7.0` differ, and `"7"` differs from `7`. Floats are
encoded by bit pattern: `0.0` and `-0.0` are treated as different (a missed
factor, never a wrong one). Strings carry a length prefix. IN value lists are
already sorted and deduplicated by the visitor, so equal sets give equal keys.

The existing `CheckPredicatesIdentical` helper is in the parent package (it
would be an import cycle) and compares a display form rather than typed
literals, so the rule uses its own key. `equalsGeneric` in `util.go` already
distinguishes value kinds, but a key lets the rule intersect branches with one
map instead of pairwise comparisons.

Template variable names are not part of the key. The rewriter runs after
template values are filled, so the bound literal is what is evaluated:
`tenant == {a}` and `tenant == {b}` are factored when both bind to `7` and are
not when they bind to different values.

## Placement

The rule runs in the OR branch of `visitBinaryExpr`, through a new
`mergeOrParts`:

1. Flatten the OR chain and visit each operand. Visiting an AND operand runs
   all AND rules on it (including range tightening), so each branch is
   already in its merged form.
2. Run the existing OR same-column merges on the chain.
3. Drop constant `false` operands; a constant `true` operand decides the OR.
4. Extract common factors.
5. Fold the result.

Running after the same-column merges is deliberate. Branches are compared in
their merged, canonical form, so `tenant IN [8, 7]` and `tenant IN [7, 8, 7]`
match, and a single-value `IN [7]` already became `== 7`. The cost is that a
predicate absorbed into a range merge inside one branch is no longer visible
as a factor. For example, in `(a > 5 AND a < 10 AND b) OR (a > 5 AND c)` the
first branch becomes `5 < a < 10 AND b`, so `a > 5` is not factored. That is
a missed opportunity, not a wrong result. Factoring before the merges would
compare unnormalized operands and need a second normalization of every branch.

Dropping constants first matters for idempotence: otherwise
`false OR (A AND B) OR (A AND C)` would only be factored on a second rewrite,
after the fold removed `false`.

After factoring, the residual OR is passed through `mergeOrParts` again
(residual atoms on one column can now merge: `(t == 7 AND r > 4) OR
(t == 7 AND r > 3)` becomes `t == 7 AND r > 3`), and the new AND goes through
`mergeAndParts`. A residual of two or more conjuncts is also merged again.
Removing a factor can change a same-column group, and the merge fixes the
operand order a second rewrite would produce.

The AND and OR branches of the visitor now splice visited operands that
became same-operator chains. An OR inside an AND that is factored returns an
AND, and its conjuncts must meet the parent's AND rules. For example,
`rating > 1 AND ((rating > 4 AND t == 7) OR (rating > 4 AND p > 100))`
becomes `rating > 4 AND (t == 7 OR p > 100)` in one pass. Without the splice,
the second rewrite would produce that and the first would not.

`MatchExpr` predicates are not visited by optional rewrites today, so the
rule never runs inside them. `RandomSampleExpr` and `ElementFilterExpr`
predicates are visited as before. In `element_filter`, the element
expression's columns are element-level, so they are never factors.

## Budgets

Budgets are constants; there are no new configuration parameters.

| Budget | Value | Counts |
|---|---|---|
| `maxCommonFactorBranches` | 64 | operands of one OR chain |
| `maxCommonFactorNodes` | 1024 | AND nodes and conjuncts visited while splitting branches |
| `maxCommonFactorWork` | 16384 | key encoding: one per candidate conjunct plus one per IN value |

When a budget is exceeded the rule returns without a change, and the OR is
kept exactly as the same-column merges produced it. Branches are scanned in
order, and the scan stops as soon as the running intersection is empty. An OR
without a common factor therefore usually costs two branch splits and their
keys.

## Three-valued logic

Segcore's AND/OR (`ThreeValuedLogicOp` in `ConjunctExpr`) and NOT are Kleene
three-valued logic: UNKNOWN is a NULL comparison, `FALSE AND UNKNOWN = FALSE`,
`TRUE OR UNKNOWN = TRUE`, `NOT UNKNOWN = UNKNOWN`. Kleene logic is a
distributive lattice with absorption:

- `(A ∧ B) ∨ (A ∧ C) = A ∧ (B ∨ C)`;
- `A ∨ (A ∧ B) = A`;
- with more branches and factors, by induction.

Both sides agree on all `3^k` assignments, not only on rows where the result
is TRUE. So the rewrite is also safe under an outer `NOT` and as an operand
of any larger expression. The tests check this exhaustively instead of
assuming it (see Testing). Duplicate factors inside a branch are idempotent
(`A ∧ A = A`), so counting them once is sound.

## Ownership and determinism

The rule allocates new `BinaryExpr` nodes and reuses the operand nodes. It
does not modify any input node, and the dropped copies of a factor are not
referenced. The output depends only on the input: factor order comes from the
first branch, residual order from branch order, and keys are strings, not
pointers.

## Testing

`rewriter/common_factor_test.go` and `rewriter/common_factor_budget_test.go`:

- **Oracle.** A Kleene evaluator over the planpb tree. Atoms are evaluated
  against a row in which every nullable field can be NULL, so a comparison is
  TRUE, FALSE or UNKNOWN. A second evaluator models segcore's operand
  reordering and short circuit, and must agree with the first.
- **Parser cases.** 27 expressions over a nullable schema. Each is checked
  differentially against the unrewritten parse on all 486 rows of the
  enumerated domain, checked for idempotence, and compared with a
  hand-factored expected form or an atom count. The cases cover: the issue
  example, absorption, identical and nonmatching branches, a factor missing
  from one branch, several factors in different orders, a factor repeated
  within a branch, IN with differently ordered values, BinaryRange,
  operator/inclusivity/column differences, `!=` on a nullable field, bool and
  string factors, outer NOT, nested factoring, a factored result meeting a
  parent AND, and unsupported `IS NULL`/NOT/LIKE conjuncts.
- **Random differential.** 3000 random trees (NOT, nested AND/OR,
  factorable ORs with repeats, absorption branches and blocking branches)
  over four nullable fields. The domain `{NULL, 0, 1, 2}` makes every atom
  take all three values, and for the `> 0` atoms the 256 rows cover all `3^4`
  truth assignments. Both the optimized and the disabled rewrite are
  compared.
- **Idempotence.** Checked on the parser cases and on 3000 random trees. The
  random pool is limited to `>`/BinaryRange atoms because the existing rules
  are not idempotent for some mixes of `==`/`!=`/IN on one column (see Open
  questions).
- **Typed literals and metadata.** int vs float, string vs int, data type,
  nullability, nested path, element level, BinaryRange bound kind, IN value
  kind and IN value set.
- **Unsupported columns and expressions.** JSON path, dynamic field, array
  element, `array_contains`, `text_match`, arithmetic, column comparison, and
  a `MatchExpr` boundary.
- **Templates.** Same and different bound values.
- **Optimizer disabled, budget exhaustion and boundaries, no input
  mutation.**

The benchmark `BenchmarkRewriteCommonFactor` measures the rewrite time and
the plan node count. It also reports per-row leaf predicate evaluations and
filter time from the segcore model, for 2/4/8 repeated branches with a common
factor, without one, with a highly selective residual, and for large
expressions (16/64 branches of 8 conjuncts):

```
go test -run '^$' -bench BenchmarkRewriteCommonFactor ./internal/parser/planparserv2/rewriter/
```

The evaluation numbers come from the Go model, not from segcore. This change
does not include a segcore end-to-end benchmark.

## Open questions

- Should the rule also see predicates that a branch's own range merge
  absorbed (factoring before the merges)? That needs a second normalization
  pass for every branch.
- The existing AND rules are not idempotent for some mixes on one column. For
  example, `a != 1 AND a != 1` becomes `NOT (a IN [1])` and then `a != 1` on a
  second rewrite. `combineAndRangePredicates` also emits merged groups in map
  order. This rule does not change that.
