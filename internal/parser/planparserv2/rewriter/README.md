## Expression Rewriter (planparserv2/rewriter)

This module performs rule-based logical rewrites on parsed `planpb.Expr` trees right after template value filling and before planning/execution.

### Entry
- `RewriteExpr(*planpb.Expr) *planpb.Expr` (in `entry.go`)
  - Recursively visits the expression tree and applies a set of composable, side-effect-free rewrite rules.

### Implemented Rules

1) IN / NOT IN normalization and merges (`term_in.go`)
- OR-equals to IN (same column):
  - `a == v1 OR a == v2 ...` → `a IN (v1, v2, ...)`
  - Numeric columns only merge when count > threshold (default 150); others when count > 1.
- AND-not-equals to NOT IN (same column):
  - `a != v1 AND a != v2 ...` → `NOT (a IN (v1, v2, ...))`
  - Same thresholds as above.
- IN vs Equal redundancy elimination (same column):
  - AND: `(a ∈ S) AND (a = v)`:
    - if `v ∈ S` → `a = v`
    - if `v ∉ S` → contradiction → constant `false`
  - OR:  `(a ∈ S) OR (a = v)` → `a ∈ (S ∪ {v})` (always union)
- IN with IN union:
  - OR: `(a ∈ S1) OR (a ∈ S2)` → `a ∈ (S1 ∪ S2)` with sorting/dedup
  - AND: `(a ∈ S1) AND (a ∈ S2)` → `a ∈ (S1 ∩ S2)`; empty intersection → constant `false`
- Sort and deduplicate `IN` / `NOT IN` value lists (supported types: bool, int64, float64, string).

2) TEXT_MATCH OR merge (`text_match.go`)
- Merge ORs of `TEXT_MATCH(field, "literal")` on the same column (no options):
  - Concatenate literals with a single space in the order they appear; no tokenization, deduplication, or sorting is performed.
  - Example: `TEXT_MATCH(f, "A C") OR TEXT_MATCH(f, "B D")` → `TEXT_MATCH(f, "A C B D")`
- If any `TEXT_MATCH` in the group has options (e.g., `minimum_should_match`), this optimization is skipped for that group.

3) Range predicate simplification (`range.go`)
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
  - JSON/dynamic fields are not range-optimized
- Literal compatibility:
  - Integer columns require integer literals (e.g., `Int64Field > 10`)
  - Float/Double columns accept both integer and float literals (e.g., `FloatField > 10` or `> 10.5`)
- Column identity:
  - Merges only happen within the same `ColumnInfo` (including nested path and element index). For example, `ArrayInt[0]` and `ArrayInt[1]` are different columns and are not merged with each other.
- BinaryRangeExpr scope:
  - Once a `BinaryRangeExpr` is constructed, multiple `BinaryRangeExpr` nodes are not merged further.

### General Notes
- All merges require operands to target the same column (same `ColumnInfo`, including nested path/element type).
- Rewrite runs after template value filling; template placeholders do not appear here.
- Sorting/dedup for IN/NOT IN is deterministic; duplicates are removed post-sort.
- Numeric-threshold for OR→IN / AND≠→NOT IN is defined in `util.go` (`defaultConvertOrToInNumericLimit`, default 150).

### Pass Ordering (current)
- OR branch:
  1. Flatten
  2. OR `==` → IN
  3. TEXT_MATCH merge (no options)
  4. Range weaken (same-direction bounds)
  5. IN with `!=` short-circuiting
  6. IN ∪ IN union
  7. IN vs Equal redundancy elimination
  8. Fold back to BinaryExpr
- AND branch:
  1. Flatten
  2. Range tighten / interval construction
  3. IN ∪ IN intersection (if any)
  4. IN with `!=` filtering
  5. IN ∩ range filtering
  6. IN vs Equal redundancy elimination
  7. AND `!=` → NOT IN
  8. Fold back to BinaryExpr

Each construction of IN will be normalized (sorted and deduplicated). TEXT_MATCH OR merge concatenates literals with a single space; no tokenization, deduplication, or sorting is performed.

### File Structure
- `entry.go`      — rewrite entry and visitor orchestration
- `util.go`       — shared helpers (column keying, value classification, sorting/dedup, constructors)
- `term_in.go`    — IN/NOT IN normalization and conversions
- `text_match.go` — TEXT_MATCH OR merge (no options)
- `range.go`      — range tightening/weakening and interval construction

### Future Extensions
- More IN-range algebra (e.g., `IN` vs exact equality propagation across subtrees).
- Merging phrase_match or other string ops with clearly-defined token rules.
- More algebraic simplifications around equality and null checks.
- Range optimization support for JSON/dynamic fields (define comparison semantics and safe gates).
- Merge/union logic for BinaryRangeExpr nodes (e.g., coalescing intervals across AND/OR when compatible).


