// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cmath>
#include <string>
#include <string_view>
#include <fmt/core.h>

#include "bitset/common.h"
#include "common/EasyAssert.h"
#include "common/Types.h"
#include "common/Vector.h"
#include "exec/expression/Expr.h"
#include "segcore/SegmentInterface.h"
#include "exec/expression/Element.h"
#include "index/SkipIndex.h"

namespace milvus {
namespace exec {

namespace {

template <typename T, typename U>
decltype(auto)
safe_mod(T a, U b) {
    if (b == 0) {
        ThrowInfo(ErrorCode::ExprInvalid,
                  "modulus by zero in arithmetic expression");
    }
    if constexpr (std::is_floating_point_v<T> || std::is_floating_point_v<U>) {
        return std::fmod(a, b);
    } else {
        return a % b;
    }
}

template <proto::plan::OpType cmp_op>
struct CmpOpHelper {
    using op = void;
};
template <>
struct CmpOpHelper<proto::plan::OpType::Equal> {
    static constexpr auto op = milvus::bitset::CompareOpType::EQ;
};
template <>
struct CmpOpHelper<proto::plan::OpType::GreaterEqual> {
    static constexpr auto op = milvus::bitset::CompareOpType::GE;
};
template <>
struct CmpOpHelper<proto::plan::OpType::GreaterThan> {
    static constexpr auto op = milvus::bitset::CompareOpType::GT;
};
template <>
struct CmpOpHelper<proto::plan::OpType::LessEqual> {
    static constexpr auto op = milvus::bitset::CompareOpType::LE;
};
template <>
struct CmpOpHelper<proto::plan::OpType::LessThan> {
    static constexpr auto op = milvus::bitset::CompareOpType::LT;
};
template <>
struct CmpOpHelper<proto::plan::OpType::NotEqual> {
    static constexpr auto op = milvus::bitset::CompareOpType::NE;
};

template <proto::plan::ArithOpType arith_op>
struct ArithOpHelper {
    using op = void;
};
template <>
struct ArithOpHelper<proto::plan::ArithOpType::Add> {
    static constexpr auto op = milvus::bitset::ArithOpType::Add;
};
template <>
struct ArithOpHelper<proto::plan::ArithOpType::Sub> {
    static constexpr auto op = milvus::bitset::ArithOpType::Sub;
};
template <>
struct ArithOpHelper<proto::plan::ArithOpType::Mul> {
    static constexpr auto op = milvus::bitset::ArithOpType::Mul;
};
template <>
struct ArithOpHelper<proto::plan::ArithOpType::Div> {
    static constexpr auto op = milvus::bitset::ArithOpType::Div;
};
template <>
struct ArithOpHelper<proto::plan::ArithOpType::Mod> {
    static constexpr auto op = milvus::bitset::ArithOpType::Mod;
};
template <>
struct ArithOpHelper<proto::plan::ArithOpType::BitAnd> {
    static constexpr auto op = milvus::bitset::ArithOpType::BitAnd;
};
template <>
struct ArithOpHelper<proto::plan::ArithOpType::BitOr> {
    static constexpr auto op = milvus::bitset::ArithOpType::BitOr;
};
template <>
struct ArithOpHelper<proto::plan::ArithOpType::BitXor> {
    static constexpr auto op = milvus::bitset::ArithOpType::BitXor;
};
template <>
struct ArithOpHelper<proto::plan::ArithOpType::Shl> {
    static constexpr auto op = milvus::bitset::ArithOpType::Shl;
};
template <>
struct ArithOpHelper<proto::plan::ArithOpType::Shr> {
    static constexpr auto op = milvus::bitset::ArithOpType::Shr;
};

}  // namespace

namespace binary_arith {

// Runtime -> compile-time dispatch, executed once per sub-batch. The row loop
// (or the SIMD call) lives inside `f`, so no per-row switch is introduced.
template <typename F>
inline void
DispatchCompareOp(proto::plan::OpType cmp_op,
                  std::string_view expr_kind,
                  F&& f) {
    switch (cmp_op) {
        case proto::plan::OpType::Equal:
            return f.template operator()<proto::plan::OpType::Equal>();
        case proto::plan::OpType::NotEqual:
            return f.template operator()<proto::plan::OpType::NotEqual>();
        case proto::plan::OpType::GreaterThan:
            return f.template operator()<proto::plan::OpType::GreaterThan>();
        case proto::plan::OpType::GreaterEqual:
            return f.template operator()<proto::plan::OpType::GreaterEqual>();
        case proto::plan::OpType::LessThan:
            return f.template operator()<proto::plan::OpType::LessThan>();
        case proto::plan::OpType::LessEqual:
            return f.template operator()<proto::plan::OpType::LessEqual>();
        default:
            ThrowInfo(UnexpectedError,
                      "unsupported operator type for {}: {}",
                      expr_kind,
                      cmp_op);
    }
}

// Outer switch on the compare op, inner on the arith op, so an unsupported
// compare op is reported before an unsupported arith op.
template <typename F>
inline void
DispatchArithCompareOp(proto::plan::ArithOpType arith_op,
                       proto::plan::OpType cmp_op,
                       F&& f) {
    DispatchCompareOp(
        cmp_op, "binary arithmetic eval expr", [&]<proto::plan::OpType C>() {
            using AT = proto::plan::ArithOpType;
            switch (arith_op) {
                case AT::Add:
                    return f.template operator()<AT::Add, C>();
                case AT::Sub:
                    return f.template operator()<AT::Sub, C>();
                case AT::Mul:
                    return f.template operator()<AT::Mul, C>();
                case AT::Div:
                    return f.template operator()<AT::Div, C>();
                case AT::Mod:
                    return f.template operator()<AT::Mod, C>();
                case AT::BitAnd:
                    return f.template operator()<AT::BitAnd, C>();
                case AT::BitOr:
                    return f.template operator()<AT::BitOr, C>();
                case AT::BitXor:
                    return f.template operator()<AT::BitXor, C>();
                case AT::Shl:
                    return f.template operator()<AT::Shl, C>();
                case AT::Shr:
                    return f.template operator()<AT::Shr, C>();
                default:
                    ThrowInfo(UnexpectedError,
                              "unsupported arith type for binary arithmetic "
                              "eval expr: {}",
                              arith_op);
            }
        });
}

template <proto::plan::OpType C, typename L, typename V>
inline bool
CompareBy(const L& l, const V& v) {
    return milvus::bitset::CompareOperator<CmpOpHelper<C>::op>::compare(l, v);
}

// Scalar columns and scalar ARRAY elements: the SIMD fallback formula from
// bitset/common.h; Mod and bitwise ops go through long().
template <proto::plan::ArithOpType A, proto::plan::OpType C, typename T>
inline bool
ScalarArithCompare(const T& x,
                   const milvus::bitset::ArithHighPrecisionType<T>& right,
                   const milvus::bitset::ArithHighPrecisionType<T>& value) {
    return milvus::bitset::
        ArithCompareOperator<ArithOpHelper<A>::op, CmpOpHelper<C>::op>::compare(
            x, right, value);
}

// JSON and ARRAY-subscript operands: Mod uses safe_mod (fmod for floating
// operands); bitwise/shift cast both to int64_t.
template <proto::plan::ArithOpType A, typename X, typename R>
inline auto
NestedArith(const X& x, const R& r) {
    using AT = proto::plan::ArithOpType;
    if constexpr (A == AT::Add) {
        return x + r;
    } else if constexpr (A == AT::Sub) {
        return x - r;
    } else if constexpr (A == AT::Mul) {
        return x * r;
    } else if constexpr (A == AT::Div) {
        return x / r;
    } else if constexpr (A == AT::Mod) {
        return safe_mod(x, r);
    } else if constexpr (A == AT::BitAnd) {
        return int64_t(x) & int64_t(r);
    } else if constexpr (A == AT::BitOr) {
        return int64_t(x) | int64_t(r);
    } else if constexpr (A == AT::BitXor) {
        return int64_t(x) ^ int64_t(r);
    } else if constexpr (A == AT::Shl) {
        return int64_t(x) << int64_t(r);
    } else if constexpr (A == AT::Shr) {
        return int64_t(x) >> int64_t(r);
    } else {
        static_assert(sizeof(X) == 0, "NestedArith: unsupported arith op");
    }
}

// Rows a kernel may leave untouched: KernelAdapter and EvalKernel overwrite
// NULL and non-candidate rows whatever the kernel wrote.
template <typename T>
inline bool
RowExcluded(const CandidateBatch<T>& b, size_t i) {
    return (b.validity && !b.validity[i]) ||
           (!b.candidates.empty() && !b.candidates[i]);
}

}  // namespace binary_arith

// Raw scalar column, and scalar ARRAY elements at element level.
template <typename T>
struct BinaryArithScalarKernel {
    using HighPrecision = milvus::bitset::ArithHighPrecisionType<T>;

    proto::plan::OpType cmp_op;
    proto::plan::ArithOpType arith_op;
    HighPrecision value;
    HighPrecision right_operand;
    milvus::OpContext* op_ctx;

    template <FilterType filter_type>
    void
    Eval(const CandidateBatch<T>& b, TriStateOut out) const {
        // Checked on every kernel invocation.
        if ((arith_op == proto::plan::ArithOpType::Div ||
             arith_op == proto::plan::ArithOpType::Mod) &&
            right_operand == 0) {
            ThrowInfo(ErrorCode::ExprInvalid,
                      "division or modulus by zero in arithmetic expression");
        }
        if constexpr (filter_type == FilterType::sequential) {
            // Contiguous rows: one dispatch, then the SIMD kernel writes the
            // whole match range. KernelAdapter folds NULL rows.
            binary_arith::DispatchArithCompareOp(
                arith_op,
                cmp_op,
                [&]<proto::plan::ArithOpType A, proto::plan::OpType C>() {
                    out.match.inplace_arith_compare<T,
                                                    ArithOpHelper<A>::op,
                                                    CmpOpHelper<C>::op>(
                        b.data, right_operand, value, b.size);
                });
        } else {
            binary_arith::DispatchArithCompareOp(
                arith_op,
                cmp_op,
                [&]<proto::plan::ArithOpType A, proto::plan::OpType C>() {
                    for (size_t i = 0; i < b.size; ++i) {
                        out.match[i] = binary_arith::ScalarArithCompare<A, C>(
                            b.data[i], right_operand, value);
                    }
                });
        }
    }

    bool
    CanSkip(const SkipIndex& skip_index,
            FieldId field_id,
            int64_t chunk_id) const {
        return skip_index.CanSkipBinaryArithRange<T>(
            op_ctx, field_id, chunk_id, cmp_op, arith_op, value, right_operand);
    }
};

static_assert(KernelCanSkip<BinaryArithScalarKernel<int64_t>>);

// Row-level JSON column. ValueType ∈ {bool, int64_t, double}.
template <typename ValueType>
struct BinaryArithJsonKernel {
    proto::plan::OpType cmp_op;
    proto::plan::ArithOpType arith_op;  // may be ArrayLength
    ValueType value;
    ValueType right_operand;  // ValueType{} for ArrayLength
    std::string pointer;

    template <FilterType filter_type>
    void
    Eval(const CandidateBatch<milvus::Json>& b, TriStateOut out) const {
        // Checked before any row is read.
        if ((arith_op == proto::plan::ArithOpType::Div ||
             arith_op == proto::plan::ArithOpType::Mod) &&
            right_operand == 0) {
            ThrowInfo(ErrorCode::ExprInvalid,
                      "division or modulus by zero in JSON field arithmetic "
                      "expression");
        }
        if (arith_op == proto::plan::ArithOpType::ArrayLength) {
            binary_arith::DispatchCompareOp(
                cmp_op,
                "binary arithmetic eval expr",
                [&]<proto::plan::OpType C>() {
                    for (size_t i = 0; i < b.size; ++i) {
                        if (binary_arith::RowExcluded(b, i)) {
                            continue;
                        }
                        // `doc` must outlive count_elements() (ondemand).
                        auto doc = b.data[i].doc();
                        auto array = doc.at_pointer(pointer).get_array();
                        if (array.error()) {
                            out.SetUnknown(i);  // #50979
                            continue;
                        }
                        int array_length = array.count_elements();
                        out.match[i] =
                            binary_arith::CompareBy<C>(array_length, value);
                    }
                });
            return;
        }
        binary_arith::DispatchArithCompareOp(
            arith_op,
            cmp_op,
            [&]<proto::plan::ArithOpType A, proto::plan::OpType C>() {
                for (size_t i = 0; i < b.size; ++i) {
                    if (binary_arith::RowExcluded(b, i)) {
                        continue;
                    }
                    if constexpr (std::is_same_v<ValueType, int64_t>) {
                        // One parse for any JSON number; int64 keeps precision,
                        // uint64/double fall back to double.
                        auto x = b.data[i].at_numeric(pointer);
                        if (x.error()) {
                            out.SetUnknown(i);  // #50979
                            continue;
                        }
                        auto n = x.value();
                        if (n.is_int64()) {
                            out.match[i] = binary_arith::CompareBy<C>(
                                binary_arith::NestedArith<A>(n.get_int64(),
                                                             right_operand),
                                value);
                        } else {
                            const double json_v =
                                n.is_uint64()
                                    ? static_cast<double>(n.get_uint64())
                                    : n.get_double();
                            out.match[i] = binary_arith::CompareBy<C>(
                                binary_arith::NestedArith<A>(json_v,
                                                             right_operand),
                                value);
                        }
                    } else {
                        auto x = b.data[i].template at<ValueType>(pointer);
                        if (x.error()) {
                            out.SetUnknown(i);
                            continue;
                        }
                        out.match[i] = binary_arith::CompareBy<C>(
                            binary_arith::NestedArith<A>(x.value(),
                                                         right_operand),
                            value);
                    }
                }
            });
    }
};

// Row-level ARRAY subscript `arr[index] <arith> right <cmp> value`.
// ValueType ∈ {int64_t, double}; never ArrayLength.
template <typename ValueType>
struct BinaryArithArrayKernel {
    proto::plan::OpType cmp_op;
    proto::plan::ArithOpType arith_op;
    ValueType value;
    ValueType right_operand;
    int index;  // nested_path_[0], -1 when absent

    template <FilterType filter_type>
    void
    Eval(const CandidateBatch<milvus::ArrayView>& b, TriStateOut out) const {
        // The divisor check comes before the nested-path assert.
        if ((arith_op == proto::plan::ArithOpType::Div ||
             arith_op == proto::plan::ArithOpType::Mod) &&
            right_operand == 0) {
            ThrowInfo(ErrorCode::ExprInvalid,
                      "division or modulus by zero in Array field arithmetic "
                      "expression");
        }
        AssertInfo(index >= 0,
                   "array arithmetic predicate requires nested path");
        binary_arith::DispatchArithCompareOp(
            arith_op,
            cmp_op,
            [&]<proto::plan::ArithOpType A, proto::plan::OpType C>() {
                for (size_t i = 0; i < b.size; ++i) {
                    if (binary_arith::RowExcluded(b, i)) {
                        continue;
                    }
                    const auto& row = b.data[i];
                    if (index >= row.length()) {
                        out.SetUnknown(i);  // #50979
                        continue;
                    }
                    out.match[i] = binary_arith::CompareBy<C>(
                        binary_arith::NestedArith<A>(
                            row.get_data<ValueType>(index), right_operand),
                        value);
                }
            });
    }
};

// array_length(col) <cmp> value over ArrayView / ArrayValueView /
// VectorArrayView rows, or ArrayValueView children at element level.
template <typename ArrayType, typename ValueType>
struct ArrayLengthKernel {
    proto::plan::OpType cmp_op;
    ValueType value;

    template <FilterType filter_type>
    void
    Eval(const CandidateBatch<ArrayType>& b, TriStateOut out) const {
        // Dispatches the compare op once per batch.
        binary_arith::DispatchCompareOp(
            cmp_op, "ARRAY length expression", [&]<proto::plan::OpType C>() {
                for (size_t i = 0; i < b.size; ++i) {
                    // Do not touch the payload of a NULL row.
                    if (b.validity && !b.validity[i]) {
                        continue;
                    }
                    out.match[i] = binary_arith::CompareBy<C>(
                        static_cast<int64_t>(
                            SegmentExpr::GetArrayRowSize(b.data[i])),
                        value);
                }
            });
    }
};

template <typename T,
          proto::plan::OpType cmp_op,
          proto::plan::ArithOpType arith_op,
          FilterType filter_type>
struct ArithOpIndexFunc {
    typedef std::conditional_t<std::is_integral_v<T> &&
                                   !std::is_same_v<bool, T>,
                               int64_t,
                               T>
        HighPrecisonType;
    using Index = index::ScalarIndex<T>;
    TargetBitmap
    operator()(Index* index,
               size_t size,
               HighPrecisonType val,
               HighPrecisonType right_operand,
               const int32_t* offsets = nullptr) {
        // Validate divisor for division/modulo operations
        if constexpr (arith_op == proto::plan::ArithOpType::Div ||
                      arith_op == proto::plan::ArithOpType::Mod) {
            if (right_operand == 0) {
                ThrowInfo(
                    ErrorCode::ExprInvalid,
                    "division or modulus by zero in arithmetic expression");
            }
        }

        TargetBitmap res(size);
        for (size_t i = 0; i < size; ++i) {
            auto offset = i;
            if constexpr (filter_type == FilterType::random) {
                offset = (offsets) ? offsets[i] : i;
            }
            auto raw = index->Reverse_Lookup(offset);
            if (!raw.has_value()) {
                res[i] = false;
                continue;
            }
            if constexpr (cmp_op == proto::plan::OpType::Equal) {
                if constexpr (arith_op == proto::plan::ArithOpType::Add) {
                    res[i] = (raw.value() + right_operand) == val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Sub) {
                    res[i] = (raw.value() - right_operand) == val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mul) {
                    res[i] = (raw.value() * right_operand) == val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Div) {
                    res[i] = (raw.value() / right_operand) == val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mod) {
                    res[i] = (long(raw.value()) % long(right_operand)) == val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::BitAnd) {
                    res[i] = (long(raw.value()) & long(right_operand)) == val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::BitOr) {
                    res[i] = (long(raw.value()) | long(right_operand)) == val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::BitXor) {
                    res[i] = (long(raw.value()) ^ long(right_operand)) == val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Shl) {
                    res[i] = (long(raw.value()) << long(right_operand)) == val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Shr) {
                    res[i] = (long(raw.value()) >> long(right_operand)) == val;
                } else {
                    ThrowInfo(
                        UnexpectedError,
                        fmt::format(
                            "unsupported arith type:{} for ArithOpElementFunc",
                            arith_op));
                }
            } else if constexpr (cmp_op == proto::plan::OpType::NotEqual) {
                if constexpr (arith_op == proto::plan::ArithOpType::Add) {
                    res[i] = (raw.value() + right_operand) != val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Sub) {
                    res[i] = (raw.value() - right_operand) != val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mul) {
                    res[i] = (raw.value() * right_operand) != val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Div) {
                    res[i] = (raw.value() / right_operand) != val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mod) {
                    res[i] = (long(raw.value()) % long(right_operand)) != val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::BitAnd) {
                    res[i] = (long(raw.value()) & long(right_operand)) != val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::BitOr) {
                    res[i] = (long(raw.value()) | long(right_operand)) != val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::BitXor) {
                    res[i] = (long(raw.value()) ^ long(right_operand)) != val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Shl) {
                    res[i] = (long(raw.value()) << long(right_operand)) != val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Shr) {
                    res[i] = (long(raw.value()) >> long(right_operand)) != val;
                } else {
                    ThrowInfo(
                        UnexpectedError,
                        fmt::format(
                            "unsupported arith type:{} for ArithOpElementFunc",
                            arith_op));
                }
            } else if constexpr (cmp_op == proto::plan::OpType::GreaterThan) {
                if constexpr (arith_op == proto::plan::ArithOpType::Add) {
                    res[i] = (raw.value() + right_operand) > val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Sub) {
                    res[i] = (raw.value() - right_operand) > val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mul) {
                    res[i] = (raw.value() * right_operand) > val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Div) {
                    res[i] = (raw.value() / right_operand) > val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mod) {
                    res[i] = (long(raw.value()) % long(right_operand)) > val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::BitAnd) {
                    res[i] = (long(raw.value()) & long(right_operand)) > val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::BitOr) {
                    res[i] = (long(raw.value()) | long(right_operand)) > val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::BitXor) {
                    res[i] = (long(raw.value()) ^ long(right_operand)) > val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Shl) {
                    res[i] = (long(raw.value()) << long(right_operand)) > val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Shr) {
                    res[i] = (long(raw.value()) >> long(right_operand)) > val;
                } else {
                    ThrowInfo(
                        UnexpectedError,
                        fmt::format(
                            "unsupported arith type:{} for ArithOpElementFunc",
                            arith_op));
                }
            } else if constexpr (cmp_op == proto::plan::OpType::GreaterEqual) {
                if constexpr (arith_op == proto::plan::ArithOpType::Add) {
                    res[i] = (raw.value() + right_operand) >= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Sub) {
                    res[i] = (raw.value() - right_operand) >= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mul) {
                    res[i] = (raw.value() * right_operand) >= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Div) {
                    res[i] = (raw.value() / right_operand) >= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mod) {
                    res[i] = (long(raw.value()) % long(right_operand)) >= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::BitAnd) {
                    res[i] = (long(raw.value()) & long(right_operand)) >= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::BitOr) {
                    res[i] = (long(raw.value()) | long(right_operand)) >= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::BitXor) {
                    res[i] = (long(raw.value()) ^ long(right_operand)) >= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Shl) {
                    res[i] = (long(raw.value()) << long(right_operand)) >= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Shr) {
                    res[i] = (long(raw.value()) >> long(right_operand)) >= val;
                } else {
                    ThrowInfo(
                        UnexpectedError,
                        fmt::format(
                            "unsupported arith type:{} for ArithOpElementFunc",
                            arith_op));
                }
            } else if constexpr (cmp_op == proto::plan::OpType::LessThan) {
                if constexpr (arith_op == proto::plan::ArithOpType::Add) {
                    res[i] = (raw.value() + right_operand) < val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Sub) {
                    res[i] = (raw.value() - right_operand) < val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mul) {
                    res[i] = (raw.value() * right_operand) < val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Div) {
                    res[i] = (raw.value() / right_operand) < val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mod) {
                    res[i] = (long(raw.value()) % long(right_operand)) < val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::BitAnd) {
                    res[i] = (long(raw.value()) & long(right_operand)) < val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::BitOr) {
                    res[i] = (long(raw.value()) | long(right_operand)) < val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::BitXor) {
                    res[i] = (long(raw.value()) ^ long(right_operand)) < val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Shl) {
                    res[i] = (long(raw.value()) << long(right_operand)) < val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Shr) {
                    res[i] = (long(raw.value()) >> long(right_operand)) < val;
                } else {
                    ThrowInfo(
                        UnexpectedError,
                        fmt::format(
                            "unsupported arith type:{} for ArithOpElementFunc",
                            arith_op));
                }
            } else if constexpr (cmp_op == proto::plan::OpType::LessEqual) {
                if constexpr (arith_op == proto::plan::ArithOpType::Add) {
                    res[i] = (raw.value() + right_operand) <= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Sub) {
                    res[i] = (raw.value() - right_operand) <= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mul) {
                    res[i] = (raw.value() * right_operand) <= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Div) {
                    res[i] = (raw.value() / right_operand) <= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mod) {
                    res[i] = (long(raw.value()) % long(right_operand)) <= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::BitAnd) {
                    res[i] = (long(raw.value()) & long(right_operand)) <= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::BitOr) {
                    res[i] = (long(raw.value()) | long(right_operand)) <= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::BitXor) {
                    res[i] = (long(raw.value()) ^ long(right_operand)) <= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Shl) {
                    res[i] = (long(raw.value()) << long(right_operand)) <= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Shr) {
                    res[i] = (long(raw.value()) >> long(right_operand)) <= val;
                } else {
                    ThrowInfo(
                        UnexpectedError,
                        fmt::format(
                            "unsupported arith type:{} for ArithOpElementFunc",
                            arith_op));
                }
            }
        }
        return res;
    }
};

class PhyTimestamptzArithCompareExpr;
class PhyBinaryArithOpEvalRangeExpr : public SegmentExpr {
    friend class PhyTimestamptzArithCompareExpr;

 public:
    PhyBinaryArithOpEvalRangeExpr(
        const std::vector<std::shared_ptr<Expr>>& input,
        const std::shared_ptr<const milvus::expr::BinaryArithOpEvalRangeExpr>&
            expr,
        const std::string& name,
        milvus::OpContext* op_ctx,
        const segcore::SegmentInternalInterface* segment,
        int64_t active_count,
        int64_t batch_size,
        int32_t consistency_level)
        : SegmentExpr(std::move(input),
                      name,
                      op_ctx,
                      segment,
                      expr->column_.field_id_,
                      expr->column_.nested_path_,
                      DataType::NONE,
                      active_count,
                      batch_size,
                      consistency_level),
          expr_(expr) {
        // DetermineExecPath();
    }

    void
    Eval(EvalCtx& context, VectorPtr& result) override;

    void
    DetermineExecPath() override {
        SegmentExpr::DetermineExecPath();
        if (exec_path_ != ExprExecPath::ScalarIndex) {
            return;
        }

        auto data_type = expr_->column_.data_type_;
        if (expr_->column_.element_level_) {
            data_type = expr_->column_.element_type_;
        }

        // JSON, ARRAY and VECTOR_ARRAY types cannot use index for arith ops.
        if (data_type == DataType::JSON || data_type == DataType::ARRAY ||
            data_type == DataType::VECTOR_ARRAY) {
            exec_path_ = ExprExecPath::RawData;
            return;
        }

        // for basic types, need index raw data for arith evaluation
        bool has_raw = false;
        switch (data_type) {
            case DataType::BOOL:
                has_raw = IndexHasRawData<bool>();
                break;
            case DataType::INT8:
                has_raw = IndexHasRawData<int8_t>();
                break;
            case DataType::INT16:
                has_raw = IndexHasRawData<int16_t>();
                break;
            case DataType::INT32:
                has_raw = IndexHasRawData<int32_t>();
                break;
            case DataType::INT64:
                has_raw = IndexHasRawData<int64_t>();
                break;
            case DataType::FLOAT:
                has_raw = IndexHasRawData<float>();
                break;
            case DataType::DOUBLE:
                has_raw = IndexHasRawData<double>();
                break;
            default:
                has_raw = false;
        }
        if (!has_raw) {
            exec_path_ = ExprExecPath::RawData;
        }
    }

    std::string
    ToString() const override {
        return fmt::format("{}", expr_->ToString());
    }

    bool
    IsSource() const override {
        return true;
    }

    std::optional<milvus::expr::ColumnInfo>
    GetColumnInfo() const override {
        return expr_->column_;
    }

    bool
    IsElementLevelExpression() const override {
        return expr_->column_.element_level_;
    }

    void
    PrefetchRawData() override;

    template <typename T>
    void
    PrefetchRawData();

 private:
    template <typename T>
    VectorPtr
    ExecRangeVisitorImpl(EvalCtx& context);

    template <typename T>
    VectorPtr
    ExecRangeVisitorImplForIndex(OffsetVector* input = nullptr);

    template <typename T>
    VectorPtr
    ExecRangeVisitorImplForData(EvalCtx& context);

    template <typename ValueType>
    VectorPtr
    ExecRangeVisitorImplForJson(EvalCtx& context);

    template <typename ValueType>
    VectorPtr
    ExecRangeVisitorImplForArray(EvalCtx& context);

    template <typename ArrayType, typename ValueType, bool ElementLevel>
    VectorPtr
    ExecArrayLength(EvalCtx& context);

 private:
    std::shared_ptr<const milvus::expr::BinaryArithOpEvalRangeExpr> expr_;
    SingleElement right_operand_arg_;
    SingleElement value_arg_;
    bool arg_inited_{false};
};

}  //namespace exec
}  // namespace milvus
