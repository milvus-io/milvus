// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <optional>
#include <boost/dynamic_bitset.hpp>
#include <utility>
#include <deque>
#include "segcore/SegmentGrowingImpl.h"
#include "query/ExprImpl.h"
#include "query/generated/ExecExprVisitor.h"

namespace milvus::query {
#if 1
// THIS CONTAINS EXTRA BODY FOR VISITOR
// WILL BE USED BY GENERATOR
namespace impl {
class ExecExprVisitor : ExprVisitor {
 public:
    using RetType = std::deque<boost::dynamic_bitset<>>;
    ExecExprVisitor(const segcore::SegmentGrowingImpl& segment, int64_t row_count)
        : segment_(segment), row_count_(row_count) {
    }
    RetType
    call_child(Expr& expr) {
        Assert(!ret_.has_value());
        expr.accept(*this);
        Assert(ret_.has_value());
        auto ret = std::move(ret_);
        ret_ = std::nullopt;
        return std::move(ret.value());
    }

 public:
    template <typename T, typename IndexFunc, typename ElementFunc>
    auto
    ExecRangeVisitorImpl(RangeExprImpl<T>& expr, IndexFunc func, ElementFunc element_func) -> RetType;

    template <typename T>
    auto
    ExecRangeVisitorDispatcher(RangeExpr& expr_raw) -> RetType;

    template <typename T>
    auto
    ExecTermVisitorImpl(TermExpr& expr_raw) -> RetType;

 private:
    const segcore::SegmentInternalInterface& segment_;
    int64_t row_count_;
    std::optional<RetType> ret_;
};
}  // namespace impl
#endif

void
ExecExprVisitor::visit(BoolUnaryExpr& expr) {
    using OpType = BoolUnaryExpr::OpType;
    auto vec = call_child(*expr.child_);
    RetType ret;
    for (int chunk_id = 0; chunk_id < vec.size(); ++chunk_id) {
        auto chunk = vec[chunk_id];
        switch (expr.op_type_) {
            case OpType::LogicalNot: {
                chunk.flip();
                break;
            }
            default: {
                PanicInfo("Invalid OpType");
            }
        }
        ret.emplace_back(std::move(chunk));
    }
    ret_ = std::move(ret);
}

void
ExecExprVisitor::visit(BoolBinaryExpr& expr) {
    using OpType = BoolBinaryExpr::OpType;
    RetType ret;
    auto left = call_child(*expr.left_);
    auto right = call_child(*expr.right_);
    Assert(left.size() == right.size());

    for (int chunk_id = 0; chunk_id < left.size(); ++chunk_id) {
        boost::dynamic_bitset<> chunk_res;
        auto left_chunk = std::move(left[chunk_id]);
        auto right_chunk = std::move(right[chunk_id]);
        chunk_res = std::move(left_chunk);
        switch (expr.op_type_) {
            case OpType::LogicalAnd: {
                chunk_res &= right_chunk;
                break;
            }
            case OpType::LogicalOr: {
                chunk_res |= right_chunk;
                break;
            }
            case OpType::LogicalXor: {
                chunk_res ^= right_chunk;
                break;
            }
            case OpType::LogicalMinus: {
                chunk_res -= right_chunk;
                break;
            }
        }
        ret.emplace_back(std::move(chunk_res));
    }
    ret_ = std::move(ret);
}

template <typename T, typename IndexFunc, typename ElementFunc>
auto
ExecExprVisitor::ExecRangeVisitorImpl(RangeExprImpl<T>& expr, IndexFunc index_func, ElementFunc element_func)
    -> RetType {
    auto data_type = expr.data_type_;
    auto& schema = segment_.get_schema();
    auto field_offset = expr.field_offset_;
    auto& field_meta = schema[field_offset];
    // auto vec_ptr = records.get_entity<T>(field_offset);
    // auto& vec = *vec_ptr;
    // const segcore::ScalarIndexingEntry<T>& entry = indexing_record.get_scalar_entry<T>(field_offset);

    // RetType results(vec.num_chunk());
    auto indexing_barrier = segment_.num_chunk_index_safe(field_offset);
    auto chunk_size = segment_.chunk_size();
    auto num_chunk = upper_div(row_count_, chunk_size);
    RetType results;

    using Index = knowhere::scalar::StructuredIndex<T>;
    for (auto chunk_id = 0; chunk_id < indexing_barrier; ++chunk_id) {
        // auto& result = results[chunk_id];
        const Index& indexing = segment_.chunk_scalar_index<T>(field_offset, chunk_id);
        // NOTE: knowhere is not const-ready
        // This is a dirty workaround
        auto data = index_func(const_cast<Index*>(&indexing));
        Assert(data->size() == chunk_size);
        results.emplace_back(std::move(*data));
    }

    for (auto chunk_id = indexing_barrier; chunk_id < num_chunk; ++chunk_id) {
        boost::dynamic_bitset<> result(chunk_size);
        // auto& result = results[chunk_id];
        result.resize(chunk_size);
        auto chunk = segment_.chunk_data<T>(field_offset, chunk_id);
        const T* data = chunk.data();
        for (int index = 0; index < chunk_size; ++index) {
            result[index] = element_func(data[index]);
        }
        Assert(result.size() == chunk_size);
        results.emplace_back(std::move(result));
    }
    return results;
}
#pragma clang diagnostic push
#pragma ide diagnostic ignored "Simplify"
template <typename T>
auto
ExecExprVisitor::ExecRangeVisitorDispatcher(RangeExpr& expr_raw) -> RetType {
    auto& expr = static_cast<RangeExprImpl<T>&>(expr_raw);
    auto conditions = expr.conditions_;
    std::sort(conditions.begin(), conditions.end());
    using OpType = RangeExpr::OpType;
    using Index = knowhere::scalar::StructuredIndex<T>;
    using Operator = knowhere::scalar::OperatorType;
    if (conditions.size() == 1) {
        auto cond = conditions[0];
        // auto [op, val] = cond; // strange bug on capture
        auto op = std::get<0>(cond);
        auto val = std::get<1>(cond);
        switch (op) {
            case OpType::Equal: {
                auto index_func = [val](Index* index) { return index->In(1, &val); };
                return ExecRangeVisitorImpl(expr, index_func, [val](T x) { return (x == val); });
            }

            case OpType::NotEqual: {
                auto index_func = [val](Index* index) { return index->NotIn(1, &val); };
                return ExecRangeVisitorImpl(expr, index_func, [val](T x) { return (x != val); });
            }

            case OpType::GreaterEqual: {
                auto index_func = [val](Index* index) { return index->Range(val, Operator::GE); };
                return ExecRangeVisitorImpl(expr, index_func, [val](T x) { return (x >= val); });
            }

            case OpType::GreaterThan: {
                auto index_func = [val](Index* index) { return index->Range(val, Operator::GT); };
                return ExecRangeVisitorImpl(expr, index_func, [val](T x) { return (x > val); });
            }

            case OpType::LessEqual: {
                auto index_func = [val](Index* index) { return index->Range(val, Operator::LE); };
                return ExecRangeVisitorImpl(expr, index_func, [val](T x) { return (x <= val); });
            }

            case OpType::LessThan: {
                auto index_func = [val](Index* index) { return index->Range(val, Operator::LT); };
                return ExecRangeVisitorImpl(expr, index_func, [val](T x) { return (x < val); });
            }
            default: {
                PanicInfo("unsupported range node");
            }
        }
    } else if (conditions.size() == 2) {
        OpType op1, op2;
        T val1, val2;
        std::tie(op1, val1) = conditions[0];
        std::tie(op2, val2) = conditions[1];
        Assert(val1 <= val2);
        auto ops = std::make_tuple(op1, op2);
        if (false) {
        } else if (ops == std::make_tuple(OpType::GreaterThan, OpType::LessThan)) {
            auto index_func = [val1, val2](Index* index) { return index->Range(val1, false, val2, false); };
            return ExecRangeVisitorImpl(expr, index_func, [val1, val2](T x) { return (val1 < x && x < val2); });
        } else if (ops == std::make_tuple(OpType::GreaterThan, OpType::LessEqual)) {
            auto index_func = [val1, val2](Index* index) { return index->Range(val1, false, val2, true); };
            return ExecRangeVisitorImpl(expr, index_func, [val1, val2](T x) { return (val1 < x && x <= val2); });
        } else if (ops == std::make_tuple(OpType::GreaterEqual, OpType::LessThan)) {
            auto index_func = [val1, val2](Index* index) { return index->Range(val1, true, val2, false); };
            return ExecRangeVisitorImpl(expr, index_func, [val1, val2](T x) { return (val1 <= x && x < val2); });
        } else if (ops == std::make_tuple(OpType::GreaterEqual, OpType::LessEqual)) {
            auto index_func = [val1, val2](Index* index) { return index->Range(val1, true, val2, true); };
            return ExecRangeVisitorImpl(expr, index_func, [val1, val2](T x) { return (val1 <= x && x <= val2); });
        } else {
            PanicInfo("unsupported range node");
        }
    } else {
        PanicInfo("unsupported range node");
    }
}
#pragma clang diagnostic pop

void
ExecExprVisitor::visit(RangeExpr& expr) {
    auto& field_meta = segment_.get_schema()[expr.field_offset_];
    Assert(expr.data_type_ == field_meta.get_data_type());
    RetType ret;
    switch (expr.data_type_) {
        case DataType::BOOL: {
            ret = ExecRangeVisitorDispatcher<bool>(expr);
            break;
        }
        case DataType::INT8: {
            ret = ExecRangeVisitorDispatcher<int8_t>(expr);
            break;
        }
        case DataType::INT16: {
            ret = ExecRangeVisitorDispatcher<int16_t>(expr);
            break;
        }
        case DataType::INT32: {
            ret = ExecRangeVisitorDispatcher<int32_t>(expr);
            break;
        }
        case DataType::INT64: {
            ret = ExecRangeVisitorDispatcher<int64_t>(expr);
            break;
        }
        case DataType::FLOAT: {
            ret = ExecRangeVisitorDispatcher<float>(expr);
            break;
        }
        case DataType::DOUBLE: {
            ret = ExecRangeVisitorDispatcher<double>(expr);
            break;
        }
        default:
            PanicInfo("unsupported");
    }
    ret_ = std::move(ret);
}

template <typename T>
auto
ExecExprVisitor::ExecTermVisitorImpl(TermExpr& expr_raw) -> RetType {
    auto& expr = static_cast<TermExprImpl<T>&>(expr_raw);
    // auto& records = segment_.get_insert_record();
    auto data_type = expr.data_type_;
    auto& schema = segment_.get_schema();

    auto field_offset = expr_raw.field_offset_;
    auto& field_meta = schema[field_offset];
    // auto vec_ptr = records.get_entity<T>(field_offset);
    // auto& vec = *vec_ptr;
    auto chunk_size = segment_.chunk_size();
    auto num_chunk = upper_div(row_count_, chunk_size);
    RetType bitsets;

    // auto N = records.ack_responder_.GetAck();
    // TODO: enable index for term

    for (int64_t chunk_id = 0; chunk_id < num_chunk; ++chunk_id) {
        Span<T> chunk = segment_.chunk_data<T>(field_offset, chunk_id);

        auto size = chunk_id == num_chunk - 1 ? row_count_ - chunk_id * chunk_size : chunk_size;

        boost::dynamic_bitset<> bitset(chunk_size);
        for (int i = 0; i < size; ++i) {
            auto value = chunk.data()[i];
            bool is_in = std::binary_search(expr.terms_.begin(), expr.terms_.end(), value);
            bitset[i] = is_in;
        }
        bitsets.emplace_back(std::move(bitset));
    }
    return bitsets;
}

void
ExecExprVisitor::visit(TermExpr& expr) {
    auto& field_meta = segment_.get_schema()[expr.field_offset_];
    Assert(expr.data_type_ == field_meta.get_data_type());
    RetType ret;
    switch (expr.data_type_) {
        case DataType::BOOL: {
            ret = ExecTermVisitorImpl<bool>(expr);
            break;
        }
        case DataType::INT8: {
            ret = ExecTermVisitorImpl<int8_t>(expr);
            break;
        }
        case DataType::INT16: {
            ret = ExecTermVisitorImpl<int16_t>(expr);
            break;
        }
        case DataType::INT32: {
            ret = ExecTermVisitorImpl<int32_t>(expr);
            break;
        }
        case DataType::INT64: {
            ret = ExecTermVisitorImpl<int64_t>(expr);
            break;
        }
        case DataType::FLOAT: {
            ret = ExecTermVisitorImpl<float>(expr);
            break;
        }
        case DataType::DOUBLE: {
            ret = ExecTermVisitorImpl<double>(expr);
            break;
        }
        default:
            PanicInfo("unsupported");
    }
    ret_ = std::move(ret);
}
}  // namespace milvus::query
