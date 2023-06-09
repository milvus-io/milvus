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
#include <utility>
#include <vector>
#include <boost/container/vector.hpp>

#include "Expr.h"
#include "pb/plan.pb.h"

namespace milvus::query {

template <typename T>
struct TermExprImpl : TermExpr {
    const std::vector<T> terms_;

    TermExprImpl(ColumnInfo column,
                 const std::vector<T>& terms,
                 const proto::plan::GenericValue::ValCase val_case,
                 const bool is_in_field = false)
        : TermExpr(std::forward<ColumnInfo>(column), val_case, is_in_field),
          terms_(terms) {
    }
};

template <typename T>
struct BinaryArithOpEvalRangeExprImpl : BinaryArithOpEvalRangeExpr {
    const T right_operand_;
    const T value_;

    BinaryArithOpEvalRangeExprImpl(
        ColumnInfo column,
        const proto::plan::GenericValue::ValCase val_case,
        const ArithOpType arith_op,
        const T right_operand,
        const OpType op_type,
        const T value)
        : BinaryArithOpEvalRangeExpr(
              std::forward<ColumnInfo>(column), val_case, op_type, arith_op),
          right_operand_(right_operand),
          value_(value) {
    }
};

template <typename T>
struct UnaryRangeExprImpl : UnaryRangeExpr {
    const T value_;

    UnaryRangeExprImpl(ColumnInfo column,
                       const OpType op_type,
                       const T value,
                       const proto::plan::GenericValue::ValCase val_case)
        : UnaryRangeExpr(std::forward<ColumnInfo>(column), op_type, val_case),
          value_(value) {
    }
};

template <typename T>
struct BinaryRangeExprImpl : BinaryRangeExpr {
    const T lower_value_;
    const T upper_value_;

    BinaryRangeExprImpl(ColumnInfo column,
                        const proto::plan::GenericValue::ValCase val_case,
                        const bool lower_inclusive,
                        const bool upper_inclusive,
                        const T lower_value,
                        const T upper_value)
        : BinaryRangeExpr(std::forward<ColumnInfo>(column),
                          val_case,
                          lower_inclusive,
                          upper_inclusive),
          lower_value_(lower_value),
          upper_value_(upper_value) {
    }
};

struct ExistsExprImpl : ExistsExpr {
    ExistsExprImpl(ColumnInfo column)
        : ExistsExpr(std::forward<ColumnInfo>(column)) {
    }
};

}  // namespace milvus::query
