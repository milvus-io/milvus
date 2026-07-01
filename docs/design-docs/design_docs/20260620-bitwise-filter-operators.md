# Bitwise Filter Operators (&, |, ^)

**Author:** Ayush Kashyap
**Date:** 2026-06-20
**Issue:** https://github.com/milvus-io/milvus/issues/24490
**Status:** Implementation Complete

---

## Background

Milvus filter expressions support arithmetic operators (`+`, `-`, `*`, `/`, `%`) in `BinaryArithOpEvalRangeExpr` for range comparisons on scalar fields. However, bitwise operators (`&`, `|`, `^`) were not supported, making it impossible to perform bitmask-style filtering — a common pattern when integer fields encode flags or permission bits.

Example use-case: given a `flags` Int64 field where bit 2 encodes "is_active", users expect to write:

```
(flags & 4) == 4
```

This PR adds full support for bitwise AND (`&`), OR (`|`), and XOR (`^`) across all scalar execution paths.

---

## What Changed

### 1. Protobuf Schema (`pkg/proto/plan.proto`)

Three new values were added to the `ArithOpType` enum:

```protobuf
enum ArithOpType {
  // existing ...
  ArrayLength = 6;
  BitAnd      = 7;   // new: bitwise AND
  BitOr       = 8;   // new: bitwise OR
  BitXor      = 9;   // new: bitwise XOR
}
```

`pkg/proto/planpb/plan.pb.go` was regenerated from this proto (not hand-edited).

### 2. Go Parser (`internal/parser/planparserv2/`)

**`operators.go`** — Extended `arithExprMap` to map the ANTLR grammar tokens `BAND`, `BOR`, `BXOR` to the new `ArithOpType` values. Constant-folding functions (`BitAnd`, `BitOr`, `BitXor`) were added for compile-time evaluation of expressions involving two integer literals.

**`utils.go`** — Extended `checkValidModArith` (renamed conceptually to cover all restricted ops) to reject bitwise operators on non-integer field types (`Float`, `Double`, etc.) at parse time, matching the existing behavior for `%`.

**`parser_visitor.go`** — Implemented `VisitBitAnd`, `VisitBitOr`, `VisitBitXor` via a shared `visitBitwiseBinaryOp` helper, mirroring the existing pattern for `VisitMul`, `VisitDiv`, etc.

**Operator precedence note:** In the Milvus filter grammar, `==` has higher precedence than `&`, so expressions must use parentheses: `(flags & 4) == 4`, not `flags & 4 == 4`.

### 3. C++ Executor (`internal/core/src/exec/expression/`)

**`BinaryArithOpEvalRangeExpr.h`** — Added `ArithOpHelper<T, BitAnd/BitOr/BitXor>` template specializations that implement the bitwise compute:

```cpp
template <typename T>
struct ArithOpHelper<T, proto::plan::ArithOpType::BitAnd> {
    static T apply(T a, T b) { return a & b; }
};
```

Extended `ArithOpElementFunc` and `ArithOpIndexFunc` template dispatch arms to cover all six comparison ops (`EQ`, `NE`, `GT`, `GE`, `LT`, `LE`) for each new bitwise op.

**`BinaryArithOpEvalRangeExpr.cpp`** — Added dispatch cases across all four execution paths:

| Path | Description |
|------|-------------|
| JSON field | `int64_t(json_v) & int64_t(right_operand)` with all 6 compare ops |
| Array field | `int64_t(value) & int64_t(right_operand)` with all 6 compare ops |
| Scalar data | Template instantiation via `ArithOpElementFunc<T, OpType, ArithOpType::BitAnd, filter_type>` |
| Index path | Template instantiation via `ArithOpIndexFunc<T, OpType, ArithOpType::BitAnd, filter_type>` |

### 4. C++ Bitset Layer (`internal/core/src/bitset/`)

**`common.h`** — Added `BitAnd = 5`, `BitOr = 6`, `BitXor = 7` to the `ArithOpType` enum (the bitset-layer enum does not include `ArrayLength`, so numbering differs from the proto enum). Added compute arms in `ArithCompareOperator::compare()`.

**`bitset.h`** — Extended the runtime dispatch block in `inplace_arith_compare` with three new `else if` branches for BitAnd/BitOr/BitXor, each dispatching to the six typed template instantiations.

**Platform instantiation files** — Extended `ALL_ARITH_CMP_OPS` macro in all five platform files to cover the new ops × 6 compare ops = 18 new explicit template instantiations per file:

- `detail/platform/dynamic.cpp`
- `detail/platform/x86/avx2-inst.cpp`
- `detail/platform/x86/avx512-inst.cpp`
- `detail/platform/arm/neon-inst.cpp`
- `detail/platform/arm/sve-inst.cpp`

**SIMD impl headers** — Extended early-exit fallback guards in all four SIMD impl headers so bitwise ops fall through to the scalar reference path (SIMD vectorization of bitwise ops is not implemented; this avoids linker errors from missing specializations):

- `detail/platform/x86/avx2-impl.h`
- `detail/platform/x86/avx512-impl.h`
- `detail/platform/arm/neon-impl.h`
- `detail/platform/arm/sve-impl.h`

---

## Type Restrictions

Bitwise operators are restricted to integer scalar fields (`Int8`, `Int16`, `Int32`, `Int64`) at parse time. Attempting to use them on `Float` or `Double` fields returns a parse error:

```
'bitand' cannot perform arithmetic between Float field and integer
```

JSON and Array fields accept bitwise ops via `int64_t` casting (consistent with how `%` is handled there).

---

## Testing

- Go parser tests added in `internal/parser/planparserv2/plan_parser_v2_test.go` covering valid expressions (`(Int64Field & 4) == 4`, `(Int32Field | 2) != 0`, `(Int64Field ^ 7) == 0`, etc.) and invalid type-mismatch cases (`(FloatField & 1) == 1` → error).

---

## Non-Goals

- SIMD-accelerated bitwise evaluation (scalar fallback used; can be a follow-up)
- Bitwise NOT (`~`) operator (unary; out of scope for this PR)
- Bitwise shift operators (`<<`, `>>`)
