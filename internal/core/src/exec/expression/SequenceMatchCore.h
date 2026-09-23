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

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <numeric>
#include <optional>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

#include "common/EasyAssert.h"
#include "pb/plan.pb.h"

namespace milvus::exec::sequence {

// Values are owned because a string returned by a chunk accessor can borrow a
// pinned chunk which is released before another field is read.
using Value = std::variant<bool, int64_t, double, std::string>;
using Cell = std::optional<Value>;
using Row = std::unordered_map<int64_t, std::vector<Cell>>;

inline Cell
Read(const Row& row, int64_t field, size_t element) {
    auto it = row.find(field);
    AssertInfo(it != row.end() && element < it->second.size(),
               "SEQUENCE_MATCH field {} element {} is unavailable",
               field,
               element);
    return it->second[element];
}

inline Cell
Constant(const proto::plan::GenericValue& value) {
    switch (value.val_case()) {
        case proto::plan::GenericValue::kBoolVal:
            return Value(value.bool_val());
        case proto::plan::GenericValue::kInt64Val:
            return Value(value.int64_val());
        case proto::plan::GenericValue::kFloatVal:
            return Value(value.float_val());
        case proto::plan::GenericValue::kStringVal:
            return Value(value.string_val());
        default:
            ThrowInfo(ExprInvalid, "unsupported SEQUENCE_MATCH constant");
    }
}

inline int
Compare(const Value& a, const Value& b) {
    if (a.index() == b.index()) {
        if (a == b) {
            return 0;
        }
        return a < b ? -1 : 1;
    }
    // A numeric literal is encoded as int64 or double; permit comparison
    // across these representations without coercing string/bool values.
    const auto is_number = [](const Value& v) {
        return std::holds_alternative<int64_t>(v) ||
               std::holds_alternative<double>(v);
    };
    if (is_number(a) && is_number(b)) {
        const auto n = [](const Value& v) {
            return std::holds_alternative<int64_t>(v)
                       ? static_cast<long double>(std::get<int64_t>(v))
                       : static_cast<long double>(std::get<double>(v));
        };
        auto lhs = n(a);
        auto rhs = n(b);
        return lhs < rhs ? -1 : lhs > rhs ? 1 : 0;
    }
    ThrowInfo(ExprInvalid, "incompatible SEQUENCE_MATCH value types");
}

inline bool
ApplyOp(proto::plan::OpType op, const Cell& lhs, const Cell& rhs) {
    if (!lhs || !rhs) {
        return false;
    }
    if ((std::holds_alternative<double>(*lhs) &&
         std::isnan(std::get<double>(*lhs))) ||
        (std::holds_alternative<double>(*rhs) &&
         std::isnan(std::get<double>(*rhs)))) {
        return op == proto::plan::OpType::NotEqual;
    }
    const int cmp = Compare(*lhs, *rhs);
    switch (op) {
        case proto::plan::OpType::Equal:
            return cmp == 0;
        case proto::plan::OpType::NotEqual:
            return cmp != 0;
        case proto::plan::OpType::GreaterThan:
            return cmp > 0;
        case proto::plan::OpType::GreaterEqual:
            return cmp >= 0;
        case proto::plan::OpType::LessThan:
            return cmp < 0;
        case proto::plan::OpType::LessEqual:
            return cmp <= 0;
        default:
            ThrowInfo(ExprInvalid, "unsupported SEQUENCE_MATCH comparison");
    }
}

inline Cell
Field(const Row& row,
      const proto::plan::ColumnInfo& column,
      size_t current,
      const std::vector<size_t>& bindings) {
    size_t element = current;
    if (column.has_sequence_step_index()) {
        auto step = column.sequence_step_index();
        AssertInfo(step < bindings.size(),
                   "SEQUENCE_MATCH refers to unbound step {}",
                   step);
        element = bindings[step];
    }
    return Read(row, column.field_id(), element);
}

inline bool
Predicate(const proto::plan::Expr& expr,
          const Row& row,
          size_t current,
          const std::vector<size_t>& bindings) {
    switch (expr.expr_case()) {
        case proto::plan::Expr::kBinaryExpr: {
            const auto& binary = expr.binary_expr();
            if (binary.op() == proto::plan::BinaryExpr::LogicalAnd) {
                return Predicate(binary.left(), row, current, bindings) &&
                       Predicate(binary.right(), row, current, bindings);
            }
            if (binary.op() == proto::plan::BinaryExpr::LogicalOr) {
                return Predicate(binary.left(), row, current, bindings) ||
                       Predicate(binary.right(), row, current, bindings);
            }
            break;
        }
        case proto::plan::Expr::kUnaryRangeExpr: {
            const auto& unary = expr.unary_range_expr();
            return ApplyOp(unary.op(),
                           Field(row, unary.column_info(), current, bindings),
                           Constant(unary.value()));
        }
        case proto::plan::Expr::kCompareExpr: {
            const auto& compare = expr.compare_expr();
            AssertInfo(compare.op() == proto::plan::OpType::Equal ||
                           compare.op() == proto::plan::OpType::NotEqual,
                       "SEQUENCE_MATCH only supports direct ==/!= field "
                       "bindings");
            return ApplyOp(
                compare.op(),
                Field(row, compare.left_column_info(), current, bindings),
                Field(row, compare.right_column_info(), current, bindings));
        }
        case proto::plan::Expr::kAlwaysTrueExpr:
            return true;
        default:
            break;
    }
    ThrowInfo(ExprInvalid, "unsupported SEQUENCE_MATCH step predicate");
}

inline bool
Window(const proto::plan::SequenceWindow& window,
       const Row& row,
       size_t current,
       const std::vector<size_t>& bindings) {
    AssertInfo(window.prior_step_index() < bindings.size(),
               "SEQUENCE_MATCH window refers to unbound step");
    auto now = Read(row, window.current_field_id(), current);
    auto prior =
        Read(row, window.prior_field_id(), bindings[window.prior_step_index()]);
    if (!now || !prior) {
        return false;
    }
    AssertInfo(std::holds_alternative<int64_t>(*now) &&
                   std::holds_alternative<int64_t>(*prior),
               "SEQUENCE_MATCH window requires int64 timestamps");
    const auto delta = static_cast<__int128>(std::get<int64_t>(*now)) -
                       static_cast<__int128>(std::get<int64_t>(*prior));
    return delta >= window.min_ms() && delta <= window.max_ms();
}

inline bool
Match(const proto::plan::SequenceMatchExpr& spec,
      const Row& row,
      size_t count,
      size_t candidate_budget = 1'000'000) {
    if (count < static_cast<size_t>(spec.steps_size())) {
        return false;
    }
    std::vector<size_t> ordered;
    ordered.reserve(count);
    for (size_t element = 0; element < count; ++element) {
        if (Read(row, spec.order_time_field_id(), element) &&
            Read(row, spec.tie_field_id(), element)) {
            ordered.push_back(element);
        }
    }
    if (ordered.size() < static_cast<size_t>(spec.steps_size())) {
        return false;
    }
    std::stable_sort(ordered.begin(), ordered.end(), [&](size_t a, size_t b) {
        auto left_time = Read(row, spec.order_time_field_id(), a);
        auto right_time = Read(row, spec.order_time_field_id(), b);
        auto cmp = Compare(*left_time, *right_time);
        if (cmp != 0) {
            return cmp < 0;
        }
        auto left_tie = Read(row, spec.tie_field_id(), a);
        auto right_tie = Read(row, spec.tie_field_id(), b);
        return Compare(*left_tie, *right_tie) < 0;
    });

    std::vector<size_t> bindings;
    size_t examined = 0;
    const auto visit = [&](auto&& self, size_t step, size_t next_pos) -> bool {
        if (step == static_cast<size_t>(spec.steps_size())) {
            return true;
        }
        const auto& candidate_step = spec.steps(static_cast<int>(step));
        for (size_t pos = next_pos; pos < ordered.size(); ++pos) {
            AssertInfo(++examined <= candidate_budget,
                       "SEQUENCE_MATCH candidate budget exceeded; narrow "
                       "the parent filter or shorten the sequence");
            const auto element = ordered[pos];
            if (candidate_step.has_window() &&
                !Window(candidate_step.window(), row, element, bindings)) {
                continue;
            }
            if (!Predicate(
                    candidate_step.predicate(), row, element, bindings)) {
                continue;
            }
            bindings.push_back(element);
            const bool found = self(self, step + 1, pos + 1);
            bindings.pop_back();
            if (found) {
                return true;
            }
        }
        return false;
    };
    return visit(visit, 0, 0);
}

}  // namespace milvus::exec::sequence
