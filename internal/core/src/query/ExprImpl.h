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

#include <tuple>
#include <vector>
#include <boost/container/vector.hpp>

#include "Expr.h"

namespace milvus::query {

template <typename T>
struct TermExprImpl : TermExpr {
    const std::vector<T> terms_;

    TermExprImpl(const FieldOffset field_offset, const DataType data_type, const std::vector<T>& terms)
        : TermExpr(field_offset, data_type), terms_(terms) {
    }
};

template <typename T>
struct BinaryArithOpEvalRangeExprImpl : BinaryArithOpEvalRangeExpr {
    const T right_operand_;
    const T value_;

    BinaryArithOpEvalRangeExprImpl(const FieldOffset field_offset,
                                   const DataType data_type,
                                   const ArithOpType arith_op,
                                   const T right_operand,
                                   const OpType op_type,
                                   const T value)
        : BinaryArithOpEvalRangeExpr(field_offset, data_type, op_type, arith_op),
          right_operand_(right_operand),
          value_(value) {
    }
};

template <typename T>
struct UnaryRangeExprImpl : UnaryRangeExpr {
    const T value_;

    UnaryRangeExprImpl(const FieldOffset field_offset, const DataType data_type, const OpType op_type, const T value)
        : UnaryRangeExpr(field_offset, data_type, op_type), value_(value) {
    }
};

template <typename T>
struct BinaryRangeExprImpl : BinaryRangeExpr {
    const T lower_value_;
    const T upper_value_;

    BinaryRangeExprImpl(const FieldOffset field_offset,
                        const DataType data_type,
                        const bool lower_inclusive,
                        const bool upper_inclusive,
                        const T lower_value,
                        const T upper_value)
        : BinaryRangeExpr(field_offset, data_type, lower_inclusive, upper_inclusive),
          lower_value_(lower_value),
          upper_value_(upper_value) {
    }
};

}  // namespace milvus::query
