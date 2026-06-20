# Bitwise Filter Operators (&, |, ^)

## Summary

This change adds support for bitwise AND (`&`), OR (`|`), and XOR (`^`) arithmetic operators in Milvus filter expressions. These operators allow users to perform bitmask-style filtering on integer fields, e.g. `(flags & 4) == 4` to check whether a specific bit is set. The implementation extends the existing `BinaryArithOpEvalRangeExpr` pipeline: three new `ArithOpType` enum values (`BitAnd=7`, `BitOr=8`, `BitXor=9`) are added to the protobuf schema, wired through the Go ANTLR parser visitor, and dispatched in the C++ executor across all four execution paths (JSON field, Array field, scalar data, and index). Bitwise operations are restricted to integer types at parse time.

## Related Issue

https://github.com/milvus-io/milvus/issues/24490
