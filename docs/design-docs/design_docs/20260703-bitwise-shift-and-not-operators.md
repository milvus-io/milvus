# Bitwise Shift and NOT Operators (`<<`, `>>`, `~`)

**Author:** Ayush Kashyap
**Date:** 2026-07-03
**Issue:** https://github.com/milvus-io/milvus/issues/50964
**Follows:** [Bitwise Filter Operators (`&`, `|`, `^`)](20260620-bitwise-filter-operators.md) — PR #50666
**Status:** Implementation Complete

---

## Background

PR #50666 added the binary bitwise operators `&`, `|`, `^` to Milvus filter
expressions and explicitly listed the remaining bitwise operators as non-goals:
bitwise NOT (`~`) and the shift operators (`<<`, `>>`). The grammar tokens
already exist in `Plan.g4` (`SHL`, `SHR`, `BNOT`), but the visitors and
constant-folding helpers returned "unsupported" errors.

This change completes the bitwise operator family. It enables bitmask
expressions such as:

```
(flags << 1) == 4        -- shift left
(flags >> 2) == 1        -- shift right
~flags == 0              -- bitwise NOT
```

on integer, integer-JSON and integer-array-element fields, matching the
coverage established for `&`, `|`, `^`.

---

## Design Overview

The three operators split into two shapes:

- **`<<` and `>>` are binary** and reuse the entire `&`/`|`/`^` machinery — a
  new `ArithOpType` value, the shared `visitBitwiseBinaryOp` helper, and a new
  `ArithOpHelper` specialization plus executor dispatch on every path.
- **`~` is unary.** The executor has no unary-arithmetic node (even `-field` is
  unsupported), so instead of introducing one, `~x` is **rewritten at parse
  time to `x ^ -1`** — an exact identity in two's-complement arithmetic
  (`~x == x ^ -1` for all `int64`). This reuses the existing `BitXor` execution
  path on every field type with **no new proto value and no new executor code**.

### Non-goal: nested arithmetic

The `BinaryArithOpEvalRangeExpr` execution model fuses exactly one arithmetic
op and one comparison: `(column OP constant) CMP constant`. Expressions that
require **two** arithmetic operations before the comparison — e.g.
`(flags >> 2) & 1 == 1` or `(~flags) & 3 == 0` — are **not** supported. This is
a pre-existing limitation shared by all arithmetic operators (`(a + b) / 2 == 3`
fails the same way) and is orthogonal to this change. Lifting it would require
nested-arithmetic support in the executor and is left as a separate follow-up.

---

## What Changed

### 1. Protobuf Schema (`pkg/proto/plan.proto`)

Two new values were added to the `ArithOpType` enum:

```protobuf
enum ArithOpType {
  // existing ...
  BitAnd = 7;
  BitOr  = 8;
  BitXor = 9;
  Shl    = 10;   // new: shift left  (<<)
  Shr    = 11;   // new: shift right (>>)
}
```

No enum value is added for `~`: the XOR rewrite means it never reaches the
plan as a distinct operator. `pkg/proto/planpb/plan.pb.go` is regenerated from
this proto (`make generated-proto-without-cpp`), not hand-edited.

### 2. Go Parser (`internal/parser/planparserv2/`)

**`operators.go`** — Mapped the `SHL`/`SHR` tokens to `ArithOpType_Shl`/`_Shr`
in `arithExprMap`/`arithNameMap`. Implemented the constant-folding functions
`ShiftLeft`, `ShiftRight` (integer-only, with shift-amount validation) and
`BitNot` (`~a == ^a` for an integer literal).

**`utils.go`** — Extended `checkValidModArith` to reject `Shl`/`Shr` on
non-integer field types (same integer-only rule as `&`, `|`, `^`, `%`). Added a
shift-amount guard in `combineBinaryArithExpr` so a field-path shift with a
constant amount outside `[0, 64)` is rejected at plan time.

**`parser_visitor.go`** —
- `VisitShift` dispatches to the shared `visitBitwiseBinaryOp` helper, using
  `ctx.GetOp()` to distinguish `<<` from `>>` (both live under one grammar rule).
- `visitBitwiseBinaryOp` gained `SHL`/`SHR` constant-folding arms.
- `VisitUnary` gained a `BNOT` arm in both branches: constant operands fold via
  `BitNot`; a field operand is rewritten to a `BinaryArithExpr{Left: col,
  Right: -1, Op: BitXor}`, with the same integer-only type check as the binary
  bitwise ops.

**Shift-amount contract:** a negative or `>= 64` shift amount is undefined
behavior in both Go and the C++ executor, so it is rejected at plan time. This
is enforced identically in the constant-folding path (`ShiftLeft`/`ShiftRight`)
and the field path (`combineBinaryArithExpr`).

### 3. C++ Executor (`internal/core/src/exec/expression/`)

**`BinaryArithOpEvalRangeExpr.h`** — Added `ArithOpHelper<Shl>`/`<Shr>`
specializations and extended `ArithOpElementFunc` / `ArithOpIndexFunc` dispatch
arms to cover all six comparison ops for the two shift ops.

**`BinaryArithOpEvalRangeExpr.cpp`** — Added `Shl`/`Shr` dispatch cases across
all four execution paths (JSON field, Array field, scalar data, index), mirroring
the `BitXor` cases:

| Path | Shift compute |
|------|---------------|
| JSON field  | `int64_t(json_v) << int64_t(right_operand)` (and `>>`) |
| Array field | `int64_t(value) << int64_t(right_operand)` (and `>>`) |
| Scalar data | `ArithOpElementFunc<T, OpType, ArithOpType::Shl, filter_type>` |
| Index path  | `ArithOpIndexFunc<T, OpType, ArithOpType::Shl, filter_type>` |

`~` needs **no** change here: it is emitted as `BitXor` with operand `-1` and
rides the existing `BitXor` cases on every path.

### 4. C++ Bitset Layer (`internal/core/src/bitset/`)

**`common.h`** — Added `Shl`, `Shr` to the bitset-layer `ArithOpType` enum and
the corresponding compute arms in `ArithCompareOperator::compare()`
(`long(left) << long(right)` / `>>`).

**`bitset.h`** — Extended the `inplace_arith_compare` runtime dispatch with
`Shl`/`Shr` branches, each dispatching to the six typed comparison
instantiations.

**Platform instantiation files** — Extended the `ALL_ARITH_CMP_OPS` macro with
`Shl`/`Shr` × 6 compare ops = 12 new explicit instantiations per file:

- `detail/platform/dynamic.cpp`
- `detail/platform/x86/avx2-inst.cpp`
- `detail/platform/x86/avx512-inst.cpp`
- `detail/platform/arm/neon-inst.cpp`
- `detail/platform/arm/sve-inst.cpp`

**SIMD impl headers** — Added `Shl`/`Shr` to the early-exit fallback guards so
shifts fall through to the scalar reference path (no SIMD vectorization of
shifts), avoiding missing-specialization linker errors:

- `detail/platform/x86/avx2-impl.h`
- `detail/platform/x86/avx512-impl.h`
- `detail/platform/arm/neon-impl.h`
- `detail/platform/arm/sve-impl.h`

### 5. Diagnostics (`internal/core/src/expr/ITypeExpr.h`)

Added `Shl`/`Shr` cases to the `fmt::formatter<ArithOpType>` so the operators
render by name in logs and error messages.

---

## Type and Value Restrictions

- **Integer-only.** `<<`, `>>`, and `~` are restricted to integer scalar fields,
  integer JSON values, and integer array elements at parse time. Use on `Float`
  / `Double` returns `bitwise operations can only apply on integer types`.
- **Shift amount in `[0, 64)`.** A constant shift amount outside this range is
  rejected at plan time (`shift amount must be in range [0, 64)`). A templated
  shift amount is validated when the placeholder value is filled.
- **`>>` is arithmetic** (sign-preserving) on signed 64-bit values, consistent
  between the Go constant-folding path and the C++ executor.

JSON and array-element fields accept these ops via `int64_t` casting, consistent
with `&`, `|`, `^`, `%`.

---

## Testing

- **Go unit tests** (`plan_parser_v2_test.go`, `utils_test.go`,
  `fill_expression_value_test.go`): valid shift/`~` expressions on scalar / JSON
  / array-element fields, plan-fusion assertions (`<<` → `Shl`, `>>` → `Shr`,
  `~x` → `BitXor` with operand `-1`), constant folding, and rejection of
  non-integer operands, out-of-range shift amounts, field-to-field shifts, and
  nested arithmetic.
- **C++ unit / e2e tests** (`ExprArithOpTest.cpp`, `ExprArrayTest.cpp`): shift
  filtering across the execution paths.
- **Integration test** (`tests/integration/expression/expression_test.go`):
  `searchWithShiftNotExpression` asserts exact match counts for `<<`, `>>`, and
  `~` filters against known data, exercising the `~ → ^ -1` rewrite end-to-end.

---

## Non-Goals

- Nested arithmetic in a single predicate (e.g. `(flags >> 2) & 1 == 1`,
  `(~flags) & 3 == 0`) — a pre-existing executor limitation shared by all
  arithmetic operators; separate follow-up.
- SIMD-accelerated shift evaluation (scalar fallback used, as for `&`/`|`/`^`).
- A first-class `BitNot` plan operator (the `x ^ -1` rewrite is exact and
  reuses the `BitXor` path with no new executor code).
