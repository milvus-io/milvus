## Expression Rewriter (planparserv2/rewriter)

This module performs rule-based logical rewrites on parsed `planpb.Expr` trees right after template value filling and before planning/execution.

### Entry
- `RewriteExpr(*planpb.Expr) *planpb.Expr` (in `entry.go`)
  - Applies an ordered registry of composable rewrite rules with a post-order traversal.
  - Uses global configuration from `paramtable.Get().CommonCfg.EnabledOptimizeExpr`
- `RewriteExprWithConfig(*planpb.Expr, bool) *planpb.Expr` (in `entry.go`)
  - Same as `RewriteExpr` but allows custom configuration for testing or special cases.

### Configuration

The rewriter can be configured via the following parameter (refreshable at runtime):

| Parameter | Default | Description |
|-----------|---------|-------------|
| `common.enabledOptimizeExpr` | `true` | Enable query expression optimization including ARRAY contains merge, range simplification, IN/NOT IN merge, TEXT_MATCH merge, and all other optimizations |

**IMPORTANT**: IN/NOT IN value list sorting and deduplication **always** runs regardless of this configuration setting, because the execution engine depends on sorted value lists.

### Rewrite Architecture

- The `rewriter` package owns the public entry points and post-order DFS traversal. The `rewriter/rules` package owns the Rule contract, default registry, concrete rules, correctness normalizers, and rule-specific helper algorithms.
- Rewrite behavior is organized as an ordered rule registry. Registry order is part of the rewrite contract: a new rule should be inserted at the point where its input and output forms compose with the surrounding rules.
- The rewriter builds the active registry from configuration: Term sorting/deduplication is always registered, while canonicalization and optimization rules are appended only when expression optimization is enabled. Individual rules do not inspect the feature flag.
- Each node rule implements `rules.Rule` through `Match` and `Apply`. The rewriter invokes `Apply` only after `Match` accepts the current expression. Logical sub-rules follow the same two-stage contract for flattened operands.
- `Match` performs the inexpensive structural eligibility check. `Apply` performs deeper grouping and type checks, then explicitly reports whether it changed the expression. A rule that reports no change must not rebuild an equivalent node.
- The rewriter uses a standard explicit post-order DFS stack:
  1. Push a post-visit task for the current node, then push its children.
  2. Process every child completely before applying rules to the parent.
  3. Scan the current node's rules in registry order.
  4. If a rule changes the node, replace it in its owning slot and traverse that replacement subtree again from its children upward.
  5. If a complete rule scan makes no change, return to the already-waiting parent task.
- The child slot is stored as `**planpb.Expr`, so a replacement is written directly into the root variable or the corresponding parent field. No parent pointer, ancestor queue, or stable-node cache is required.
- A rewrite-application budget is only a defensive guard against mutually inverse or continually expanding custom rules. Normal rules should terminate without reaching it.

### Implemented Rules

1) IN / NOT IN normalization and merges (`rules/term_in.go`)
- OR-equals to IN (same column):
  - `a == v1 OR a == v2 ...` → `a IN (v1, v2, ...)`
  - At least two compatible equality predicates are required.
- AND-not-equals to NOT IN (same column):
  - `a != v1 AND a != v2 ...` → `NOT (a IN (v1, v2, ...))`
  - At least two compatible not-equal predicates are required.
- IN vs Equal redundancy elimination (same column):
  - AND: `(a ∈ S) AND (a = v)`:
    - if `v ∈ S` → `a = v`
    - if `v ∉ S` → contradiction → constant `false`
  - OR:  `(a ∈ S) OR (a = v)` → `a ∈ (S ∪ {v})` (always union)
- IN with IN union:
  - OR: `(a ∈ S1) OR (a ∈ S2)` → `a ∈ (S1 ∪ S2)` with sorting/dedup
  - AND: `(a ∈ S1) AND (a ∈ S2)` → `a ∈ (S1 ∩ S2)`; empty intersection → constant `false`
- Sort and deduplicate `IN` / `NOT IN` value lists (supported types: bool, int64, float64, string).

2) TEXT_MATCH OR merge (`rules/text_match.go`)
- Merge ORs of `TEXT_MATCH(field, "literal")` on the same column (no options):
  - Concatenate literals with a single space in the order they appear; no tokenization, deduplication, or sorting is performed.
  - Example: `TEXT_MATCH(f, "A C") OR TEXT_MATCH(f, "B D")` → `TEXT_MATCH(f, "A C B D")`
- If any `TEXT_MATCH` in the group has options (e.g., `minimum_should_match`), this optimization is skipped for that group.

3) ARRAY contains merge (`rules/array_contains.go`)
- OR on the same physical ARRAY column:
  - `array_contains(a, x) OR array_contains(a, y)` → `array_contains_any(a, [x, y])`
  - Existing `array_contains_any` nodes are absorbed, so arbitrarily long and nested OR chains close into one node.
- AND on the same physical ARRAY column:
  - `array_contains(a, x) AND array_contains(a, y)` → `array_contains_all(a, [x, y])`
  - Existing `array_contains_all` nodes are absorbed, so arbitrarily long and nested AND chains close into one node.
- At least two compatible source nodes are required. Values retain first-encounter order, duplicates are removed without sorting, and the merged node is emitted at the group's first position.
- The rule is keyed by `ColumnInfo`, including nested path and element-level identity. Different fields and the opposite Any/All operator remain separate.
- Only `ColumnInfo.DataType == Array` participates. JSON columns remain unchanged even though ARRAY and JSON predicates share `JSONContainsExpr` and either function spelling may be used on an ARRAY column.
- Nil, array-valued, unknown, and NaN elements are excluded from merging. `ElementsSameType` is recomputed and consumed template metadata is cleared on the merged node.

4) Range predicate simplification (`rules/range.go`)
- AND tighten (same column):
  - Lower bounds: `a > 10 AND a > 20` → `a > 20` (pick strongest lower)
  - Upper bounds: `a < 50 AND a < 60` → `a < 50` (pick strongest upper)
  - Mixed lower and upper: `a > 10 AND a < 50` → `10 < a < 50` (BinaryRangeExpr)
  - Inclusion respected (>, >=, <, <=). On ties, exclusive is considered stronger than inclusive for tightening.
- OR weaken (same column, same direction):
  - Lower bounds: `a > 10 OR a > 20` → `a > 10` (pick weakest lower)
  - Upper bounds: `a < 10 OR a < 20` → `a < 20` (pick weakest upper)
  - Inclusion respected, preferring inclusive for weakening in ties.
- Mixed-direction OR (lower vs upper) is not merged.
- Equivalent-bound collapses (same column, same value):
  - AND: `a ≥ x AND a > x` → `a > x`; `a ≤ y AND a < y` → `a < y`
  - OR:  `a ≥ x OR a > x` → `a ≥ x`; `a ≤ y OR a < y` → `a ≤ y`
  - Symmetric dedup: `a > 10 AND a ≥ 10` → `a > 10`; `a < 5 OR a ≤ 5` → `a ≤ 5`
- IN ∩ range filtering:
  - AND: `(a ∈ {…}) AND (range)` → keep only values in the set that satisfy the range
    - e.g., `{1,3,5} AND a > 3` → `{5}`
- Supported columns for range optimization:
  - Scalar: Int8/Int16/Int32/Int64, Float/Double, VarChar
  - Array element access: when indexing an element (e.g., `ArrayInt[0]`), the element type above applies
  - JSON/dynamic fields with nested paths (e.g., `JSONField["price"]`, `$meta["age"]`) are range-optimized
    - Type determined from literal value (int, float, string)
    - Numeric types (int and float) are compatible and normalized to Double for merging
    - Different type categories are not merged (e.g., `json["a"] > 10` and `json["a"] > "hello"` remain separate)
    - Bool literals are not optimized (no meaningful ranges)
- Literal compatibility:
  - Integer columns require integer literals (e.g., `Int64Field > 10`)
  - Float/Double columns accept both integer and float literals (e.g., `FloatField > 10` or `> 10.5`)
- Column identity:
  - Merges only happen within the same `ColumnInfo` (including nested path and element index). For example, `ArrayInt[0]` and `ArrayInt[1]` are different columns and are not merged with each other.
- BinaryRangeExpr merging:
  - AND: Merge multiple `BinaryRangeExpr` nodes on the same column to compute intersection (max lower, min upper)
    - `(10 < x < 50) AND (20 < x < 40)` → `(20 < x < 40)`
    - Empty intersection → constant `false`
  - AND with UnaryRangeExpr: Update appropriate bound of `BinaryRangeExpr`
    - `(10 < x < 50) AND (x > 30)` → `(30 < x < 50)`
  - OR: Merge overlapping or adjacent `BinaryRangeExpr` nodes into wider interval
    - `(10 < x < 25) OR (20 < x < 40)` → `(10 < x < 40)` (overlapping)
    - `(10 < x <= 20) OR (20 <= x < 30)` → `(10 < x < 30)` (adjacent with inclusive)
    - Disjoint intervals remain separate: `(10 < x < 20) OR (30 < x < 40)` → remains as OR
  - Inclusivity handling: AND prefers exclusive on equal bounds (stronger), OR prefers inclusive (weaker)

### General Notes
- All merges require operands to target the same column (same `ColumnInfo`, including nested path/element type).
- Rewrite runs after template value filling; template placeholders do not appear here.
- Optional optimizer traversal remains limited to `BinaryExpr`, `UnaryExpr`, `TermExpr`, and `ValueExpr`, so it does not descend into `MatchExpr` or `ElementFilterExpr` predicates. The always-on correctness normalizers retain their broader traversal policy.
- Sorting/dedup for IN/NOT IN is deterministic; duplicates are removed post-sort.
- Nullable fields keep contradiction/tautology predicates instead of folding to valid `true`/`false`, because NULL must remain unknown under outer logical operators such as `NOT`. Fixed JSON/array paths also avoid domain-wide folds that assume every path/index exists.
- **Known limitation / TODO**: grouped predicate emission in `rules/term_in.go`, `rules/range.go`, and `rules/text_match.go` is still map-based, so independent groups may not preserve first-occurrence order. Preserving encounter order is intentionally left for a follow-up.

### Ordered Rule Registry

The logical pipelines below define the registry order for each logical node. Children finish first. If the logical node is rewritten, its replacement subtree is processed again before the waiting parent is allowed to run.

- OR branch:
  1. Flatten
  2. ARRAY `Contains` / `ContainsAny` → `ContainsAny`
  3. OR `==` → IN
  4. TEXT_MATCH merge (no options)
  5. Range weaken (same-direction bounds)
  6. BinaryRangeExpr merge (overlapping/adjacent intervals)
  7. IN with `!=` short-circuiting
  8. IN ∪ IN union
  9. IN vs Equal redundancy elimination
  10. Fold back to BinaryExpr
- AND branch:
  1. Flatten
  2. ARRAY `Contains` / `ContainsAll` → `ContainsAll`
  3. Range tighten / interval construction
  4. BinaryRangeExpr merge (intersection, also with UnaryRangeExpr)
  5. IN ∪ IN intersection (if any)
  6. IN with `!=` filtering
  7. IN ∩ range filtering
  8. IN vs Equal redundancy elimination
  9. AND `!=` → NOT IN
  10. Fold back to BinaryExpr

Each construction of IN will be normalized (sorted and deduplicated). TEXT_MATCH OR merge concatenates literals with a single space; no tokenization, deduplication, or sorting is performed.

### File Structure
- `entry.go`      — public rewrite entry and configuration lookup
- `rewriter.go`   — explicit post-order stack and rewrite-application guard
- `api.go`        — compatibility wrappers for public helper APIs
- `rules/rule.go` — public Rule interface and shared logical-rule contracts
- `rules/rule_registry.go` — ordered default node and logical rule registries
- `rules/rule_*.go` — one concrete rule type with its `Match` and `Apply` implementations per file
- `rules/util.go` — shared helpers (column keying, value classification, sorting/dedup, constructors)
- `rules/array_contains.go` — physical ARRAY contains Any/All merges
- `rules/term_in.go` — IN/NOT IN normalization and conversions
- `rules/text_match.go` — TEXT_MATCH OR merge (no options)
- `rules/range.go` — range tightening/weakening and interval construction

### Future Extensions
- More IN-range algebra (e.g., `IN` vs exact equality propagation across subtrees).
- Merging phrase_match or other string ops with clearly-defined token rules.
- More algebraic simplifications around equality and null checks:
  - Contradiction detection: `(a == 1) AND (a == 2)` → `false`; `(a > 10) AND (a == 5)` → `false`
  - Tautology detection: `(a > 10) OR (a <= 10)` → `true` (for non-NULL values)
  - Absorption laws: `(a > 10) OR ((a > 10) AND (b > 20))` → `a > 10`
- Advanced BinaryRangeExpr merging:
  - OR with unbounded + bounded: Currently skipped. Could optimize `(x > 10) OR (5 < x < 15)` → `x > 5`.
