# Nested Arithmetic in Filter Expressions (Depth-2)

**Author:** Ayush Kashyap
**Date:** 2026-08-12
**Issue:** https://github.com/milvus-io/milvus/issues/51269
**Follows:** [Bitwise Filter Operators (`&`, `|`, `^`)](20260620-bitwise-filter-operators.md),
[Bitwise Shift and NOT Operators (`<<`, `>>`, `~`)](20260703-bitwise-shift-and-not-operators.md) —
supersedes the "Non-goal: nested arithmetic" section of the latter
**Status:** Implementation drafted, unverified — no C++/proto build toolchain was available in the
authoring environment; `make generated-proto-without-cpp` and a full C++ build + test run are
still required before this can be considered complete (see Testing).

---

## Background

Milvus filter expressions support exactly one arithmetic operation before a
comparison: `(column OP constant) CMP constant`. The prior bitwise-operator
work explicitly called out that **two** arithmetic ops before the comparison
— e.g. `(flags >> 2) & 1 == 1` — are rejected with `"complicated arithmetic
operations are not supported"`, and listed lifting that limitation as a
separate follow-up:

> The `BinaryArithOpEvalRangeExpr` execution model fuses exactly one
> arithmetic op and one comparison... Expressions that require two arithmetic
> operations before the comparison — e.g. `(flags >> 2) & 1 == 1`... are not
> supported... Lifting it would require nested-arithmetic support in the
> executor and is left as a separate follow-up.
> — [Bitwise Shift and NOT Operators, "Non-goal: nested arithmetic"](20260703-bitwise-shift-and-not-operators.md#non-goal-nested-arithmetic)

This is exactly [issue #51269](https://github.com/milvus-io/milvus/issues/51269),
whose motivating use case is bitmask extraction:

```
(flags >> 2) & 1 == 1     -- extract and test a single bit
(flags >> 4) & 7 == 5     -- extract and test a 3-bit field
```

This change lifts that limitation for exactly **one** additional level of
nesting.

---

## Design Overview

**Scope: depth-2 only** — `((column op1 a) op2 b) CMP value`. General N-deep
nesting is explicitly out of scope (see Non-Goals).

This scope was chosen after inspecting the actual execution model rather than
assumed up front. The raw-scalar hot path fuses one arithmetic op with the
comparison in a single call:

```cpp
res.inplace_arith_compare<T, arith_op_cvt, cmp_op_cvt>(src, right_operand, val, size);
```

A general N-deep chain would require materializing an intermediate buffer via
a runtime transform loop before this final comparison — introducing both
runtime overhead and a departure from the compile-time-dispatched shape every
other arithmetic op already uses. Depth-2 avoids this: the second op becomes
an additional *compile-time* template parameter (`arith_op1`, `arith_op2`)
rather than a runtime chain, so the raw-scalar and index paths remain fully
compile-time dispatched, with no intermediate materialization pass.

**A verified finding that shapes the SIMD scope decision below:** the AVX2
kernel (and AVX-512/NEON/SVE analogously) already `return false` — i.e. "no
vectorized kernel, fall back to scalar" — for `Div`, `Mod`, `BitAnd`, `BitOr`,
`BitXor`, `Shl`, and `Shr`, for the *existing single-op* case:

```cpp
// avx2-impl.h
if constexpr (AOp == ArithOpType::Div || AOp == ArithOpType::Mod ||
              AOp == ArithOpType::BitAnd || AOp == ArithOpType::BitOr ||
              AOp == ArithOpType::BitXor || AOp == ArithOpType::Shl ||
              AOp == ArithOpType::Shr) {
    return false;
}
```

Only `Add`/`Sub`/`Mul` (integer) and `Add`/`Sub`/`Mul`/`Div` (float/double)
have real SIMD kernels today. The issue's own motivating example — a shift
followed by a bitmask — was therefore **never SIMD-accelerated to begin
with**, even at depth-1. Extending it to depth-2 costs zero SIMD regression
relative to today.

This splits the SIMD-layer work into two tiers:

- **Tier 1 (this change):** a generic, non-vectorized two-op scalar
  comparison path, mirroring the shape of the existing single-op scalar
  fallback exactly. Correct for every op-pair, zero regression, and — since
  the motivating bitmask case was already scalar-only — sufficient for the
  issue as filed.
- **Tier 2 (explicit follow-up, not in this change):** true SIMD-fused
  kernels for chained `Add`/`Sub`/`Mul`/`Div`, factoring "apply-only"
  primitives out of the existing fused compare kernels across
  AVX2/AVX-512/NEON/SVE. Deferred pending further review of the platform
  code; see Non-Goals.

The grammar-level `BinaryArithExpr` proto message (`{ Expr left; Expr right;
ArithOpType op; }`) is already fully recursive — the ANTLR visitors already
build correct, correctly-typed nested trees today (each grammar level
independently runs `calcDataType`/`checkValidModArith` against its operands'
already-computed types). The entire gap is downstream, in the one-shot
flattening step (`handleBinaryArithExpr`) and the **flat**
`BinaryArithOpEvalRangeExpr` plan node it produces, which carries exactly one
`(op, operand)` pair end to end through Go, proto, and the C++ executor.

---

## What Changed

### 1. Protobuf Schema (`pkg/proto/plan.proto`)

`BinaryArithOpEvalRangeExpr` gained an optional second op/operand triple:

```protobuf
message BinaryArithOpEvalRangeExpr {
  // existing fields unchanged ...
  ArithOpType arith_op2 = 8;
  GenericValue right_operand2 = 9;
  string operand2_template_variable_name = 10;
}
```

`ArithOpType_Unknown` (the proto3 default, `0`) is the "no second op"
sentinel — every existing depth-1 producer is unaffected, and old serialized
plans deserialize as depth-1 automatically. The existing `BinaryArithOp`
message (`{ column_info; arith_op; right_operand; }`) was considered as a
possible carrier for the second op but is a different, incompatible shape
(no comparison `op`/`value`) and was left untouched.

### 2. Go Parser (`internal/parser/planparserv2/`)

**`utils.go`** — `handleBinaryArithExpr`'s existing leaf-matching logic
(`column op const` / `const op column`) was factored into `resolveArithLeaf`,
reused for both the outer op and, when it fails to match, an inner nested
operand. A new `combineNestedBinaryArithExpr` resolves the nested side
through the same `resolveArithLeaf` and builds the two-op flat node:

- If the inner side is itself nested (depth-3+) or field-to-field,
  `resolveArithLeaf` reports "not a leaf" and the existing
  `"complicated arithmetic operations are not supported"` rejection fires —
  the depth cap falls out of the existing leaf-matching logic with no new
  depth-tracking code.
- `array_length(...)` is explicitly excluded from participating as either
  op — it has no `right_operand` and doesn't fit the flat two-op shape, so it
  stays fully separate via the existing `combineArrayLengthExpr`.
- Div/mod-by-zero and shift-range `[0, 64)` validation
  (`validateAndCastArithOperand`, factored out of the former
  `combineBinaryArithExpr`) run independently for op1 and op2, each against
  its own operand.

**`fill_expression_value.go`** — `FillBinaryArithOpEvalRangeExpressionValue`
gained a second block handling `arith_op2`/`right_operand2`'s template-value
fill and re-validation, run after op1's block: the intermediate type left by
op1's cast becomes op2's cast target (chaining `getTargetType` one level
deeper, mirroring how the plan-time path chains `calcDataType`), and the
final comparison value is cast against whatever type is left after op2 runs.

**`parser_visitor.go`** — No behavioral changes; the ANTLR visitors already
built correct nested trees. One stale comment was corrected: the `BNOT`
(`~x` → `x ^ -1`) rewrite produces an ordinary `BinaryArithExpr` node, so it
already composed with one further level of nesting (`(~x) & 3`, `~(x >> 2)`)
once depth-2 support landed — a prior comment claiming this was unsupported
was updated.

### 3. C++ Logical Expr (`internal/core/src/expr/ITypeExpr.h`)

`expr::BinaryArithOpEvalRangeExpr` gained `arith_op_type2_`/`right_operand2_`
members (defaulted to `Unknown`/empty, so every existing single-op
constructor call site keeps compiling unchanged) and a `has_second_op()`
accessor.

### 4. Proto → C++ Translation (`internal/core/src/query/PlanProto.cpp`)

`ParseBinaryArithOpEvalRangeExprs` threads `expr_pb.arith_op2()` /
`expr_pb.right_operand2()` into the constructor — a direct pass-through,
backward compatible via the same `Unknown` sentinel.

### 5. C++ Executor (`internal/core/src/exec/expression/`)

**`BinaryArithOpEvalRangeExpr.h`** — New sibling functors
`ArithOpElementFunc2<T, cmp_op, arith_op1, arith_op2, filter_type>` and
`ArithOpIndexFunc2<...>`, added alongside (not replacing) the existing
single-op `ArithOpElementFunc`/`ArithOpIndexFunc` — a smaller, additive diff
that leaves the existing hot path's codegen untouched. Each composes op1 and
op2 via a shared `ApplyArithOp` helper, then either loops per-element (the
`FilterType::random` / iterative-filter fallback, matching the existing
single-op shape) or calls the new `inplace_arith_compare2` bitset entry point
for the batch/sequential path. `PhyBinaryArithOpEvalRangeExpr` gained a
`right_operand2_arg_` member, initialized only when `has_second_op()`.

**`BinaryArithOpEvalRangeExpr.cpp`** — Four execution paths, two different
strategies:

- **Raw scalar (index-backed and data-backed).** The existing single-op
  dispatch is a hand-written `switch(op_type){switch(arith_type)}` (66 arms).
  A literal two-op equivalent would be ~600 arms across the two paths — too
  large and too error-prone to hand-author (and, in this authoring
  environment, impossible to compile-verify). Instead, a small set of
  macros (`DISPATCH_ARITH2_*_CMP` / `_ARITH1` / `_LEAF`, `#undef`'d after
  use) mechanically *generate* the full `(cmp × arith1 × arith2)`
  compile-time-dispatched switch, mirroring the `DECLARE_PARTIAL_*` macro
  pattern already used in the bitset platform headers. Op1's captured value
  and op2's captured value/type are aliased into the same local names the
  existing switch already references, so the switch body itself needed no
  duplication — only routing to it changed. Two correctness issues were
  found and fixed while wiring this up: the two-op branch was initially
  returning early and skipping the shared valid-mask post-processing that
  runs after the switch (fixed by falling through to the shared tail
  instead of returning), and the `SkipIndex`-based chunk-pruning helper
  (`CanSkipBinaryArithRange`) only reasons about a single op against
  per-chunk min/max stats — it is now unconditionally disabled
  (never-skip) whenever `has_second_op()`, rather than risk incorrectly
  pruning rows that would still match after the second op applies.
- **JSON and Array fields.** These paths are already a sequential per-element
  loop over `simdjson`/array accessors (no SIMD to lose), so no combinatorial
  expansion was needed at all: a small `ApplyJsonArithOp` helper
  pre-transforms the extracted value through op1, then the *existing,
  unmodified* `switch(arith_type)` block runs for op2 against the
  transformed value (via the same local-name-aliasing trick used in the
  scalar paths).
- **VectorArray** is `array_length`-only and unaffected, since `array_length`
  cannot participate in a chain.

Div/mod-by-zero for op2 is validated once per batch (matching where op1's
existing check already sits), not per-element.

### 6. C++ Bitset Layer — Tier 1 (`internal/core/src/bitset/`)

A depth-2 counterpart was added at every layer of the existing single-op
stack, always resolving to the generic scalar path (no dedicated SIMD kernel
yet, per the Design Overview):

- `common.h`: `ArithApplyOperator<AOp>` (a single-op "apply, don't compare"
  primitive, kept independent of `ArithCompareOperator`'s inlined logic so
  the existing single-op path is untouched) and `ArithCompareOperator2<AOp1,
  AOp2, CmpOp>`, which composes two `ArithApplyOperator` calls before the
  final compare.
- `detail/element_wise.h`: `ElementWiseBitsetPolicy::op_arith_compare2`.
- `detail/element_vectorized.h`: `VectorizedElementWisePolicy::op_arith_compare2`,
  same baseline/vectorized dispatch shape as `op_arith_compare` — the
  vectorized lambda always returns `false` today (see below), so this always
  falls back to the scalar baseline.
- `bitset.h`: `BitsetBase::inplace_arith_compare2`, the public entry point
  `ArithOpElementFunc2`/`ArithOpIndexFunc2` call.
- `detail/platform/vectorized_ref.h` and all four platform forwarding
  classes (`VectorizedAvx2`, `VectorizedAvx512`, `VectorizedNeon`,
  `VectorizedSve`) plus `VectorizedDynamic`: a direct `op_arith_compare2`
  stub that always returns `false`. Unlike the single-op path's
  `OpArithCompareImpl<T, AOp, CmpOp>` machinery (which is `decl`/`impl`/`inst`
  split across translation units to support real per-type, per-op generated
  SIMD instructions), Tier 1's answer doesn't depend on `T`/`AOp1`/`AOp2`/
  `CmpOp` at all — it's unconditionally `false` — so no combinatorial
  per-type instantiation split was needed, just one small direct method per
  platform file.

---

## Type and Value Restrictions

- Same integer-only rules as the existing single-op bitwise/shift/mod family
  apply **independently to op1 and op2** — each op's type check runs at its
  own grammar level during parsing (unchanged machinery), so e.g.
  `(FloatField + 1) & 1` is rejected because `&`'s own operand isn't
  integer-convertible, exactly as it would be at depth-1.
- Div/mod-by-zero and shift-range `[0, 64)` are validated independently for
  op1 and op2, both at plan time (constant operands) and at fill time
  (templated operands, once the placeholder value is known).
- `array_length(...)` cannot participate as op1 or op2 of a nested chain.
- Depth-3+ nesting and field-to-field arithmetic nested inside a second op
  (e.g. `((a + b) >> 1) == 4`) are rejected with the same messages used for
  the equivalent depth-1 shapes.

---

## Testing

**Not yet run — no build toolchain in the authoring environment.** The
changes below are hand-reviewed for correctness (including two bugs caught
during review, noted in §5) but unverified by compilation or execution. Before
this can be merged:

- Run `make generated-proto-without-cpp` and confirm the generated Go/C++
  proto bindings match what this doc assumes.
- Build and run `go test -tags dynamic,test -gcflags="all=-N -l" -count=1
  ./internal/parser/planparserv2/...`.
- Build `internal/core` and run the updated `ExprArithOpTest`/`BitsetTest`
  suites.
- Run the updated `tests/python_client/milvus_client/expressions` matrix.

Test coverage added in this change:

- **Go** (`plan_parser_v2_test.go`): plan-structure assertions
  (`TestExpr_NestedBinaryArith`) for the issue's own bitmask examples, plain
  arithmetic chains, the `~x` rewrite composing with a further op, and the
  reversed-nesting (`const op2 (nested)`, Add/Mul only) form; rejections for
  depth-3+, field-to-field-nested, div/mod-by-zero and out-of-range shift on
  op2, and `array_length` in a chain. Two pre-existing cases in
  `TestExpr_BinaryArith`'s "unsupported" list (`(Int64Field >> 2) * 2 == 4`,
  `(~Int64Field) + 1 == 0`) were moved to the valid list, since this change
  makes them supported — leaving them in place would have made an existing
  test assert the old, now-incorrect behavior.
  `fill_expression_value_test.go` gained template-fill cases for op2's
  operand and re-validation cases for op2 division-by-zero and out-of-range
  shift via a templated value.
- **C++** (`ExprArithOpTest.cpp`): end-to-end cases added to the existing
  `TestBinaryArithOpEvalRange` table covering the issue's own bitmask
  examples plus a pure-arithmetic and a modulo chain, across `int16`/`int32`/
  `int64` and both the index-backed and data-backed raw-scalar paths.
- **Python** (`filtering_case_matrix.py`): two new negative cases
  (depth-3+ rejection, field-to-field-nested rejection) added to
  `NEGATIVE_FILTER_ERROR_CASES`.

**Known gaps in this change's test coverage** (flagged rather than silently
skipped): no direct C++-level tests for the JSON/Array execution paths'
two-op handling, no C++-level tests constructing the two-op proto directly
to exercise op2 div/mod-by-zero or shift-range validation at the executor
layer, no new `BitsetTest.cpp` coverage for `inplace_arith_compare2`, and no
new Python **positive** case (computing correct expected row IDs requires
the test fixture's underlying data, which wasn't available to compute
against safely in this session — adding one with fabricated expected values
would be worse than not adding it).

---

## Non-Goals

- **General N-deep nesting.** Only depth-2 is supported; `((a op1 b) op2 c)
  op3 d` is rejected the same way depth-3+ always was. See Design Overview
  for why this boundary was chosen.
- **Tier 2: true SIMD fusion for chained `Add`/`Sub`/`Mul`/`Div`.** Tier 1
  (generic scalar, this change) is correct and sufficient for the issue's
  motivating bitmask case, which was never SIMD-accelerated even at depth-1.
  A follow-up could factor "apply-only" SIMD primitives out of the existing
  fused compare kernels (`ArithHelper*<AOp, CmpOp>::op` in each platform's
  `-impl.h`) to fuse chained pure-arithmetic ops across AVX2/AVX-512/NEON/SVE,
  reusing the existing single-op fused kernel for the final op.
- **`array_length(...)` in a chain.** Structurally incompatible with the flat
  two-op shape (no `right_operand`); stays a fully separate code path.
