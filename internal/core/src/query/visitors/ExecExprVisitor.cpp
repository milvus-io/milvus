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
#include <boost/variant.hpp>
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
    ExecExprVisitor(const segcore::SegmentInternalInterface& segment, int64_t row_count, Timestamp timestamp)
        : segment_(segment), row_count_(row_count), timestamp_(timestamp) {
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

    template <typename CmpFunc>
    auto
    ExecCompareExprDispatcher(CompareExpr& expr, CmpFunc cmp_func) -> RetType;

 private:
    const segcore::SegmentInternalInterface& segment_;
    int64_t row_count_;
    std::optional<RetType> ret_;
    Timestamp timestamp_;
};
}  // namespace impl
#endif

void
ExecExprVisitor::visit(LogicalUnaryExpr& expr) {
    using OpType = LogicalUnaryExpr::OpType;
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
ExecExprVisitor::visit(LogicalBinaryExpr& expr) {
    using OpType = LogicalBinaryExpr::OpType;
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
    auto& schema = segment_.get_schema();
    auto field_offset = expr.field_offset_;
    auto& field_meta = schema[field_offset];
    auto indexing_barrier = segment_.num_chunk_index(field_offset);
    auto size_per_chunk = segment_.size_per_chunk();
    auto num_chunk = upper_div(row_count_, size_per_chunk);
    RetType results;

    using Index = knowhere::scalar::StructuredIndex<T>;
    for (auto chunk_id = 0; chunk_id < indexing_barrier; ++chunk_id) {
        const Index& indexing = segment_.chunk_scalar_index<T>(field_offset, chunk_id);
        // NOTE: knowhere is not const-ready
        // This is a dirty workaround
        auto data = index_func(const_cast<Index*>(&indexing));
        Assert(data->size() == size_per_chunk);
        results.emplace_back(std::move(*data));
    }

    for (auto chunk_id = indexing_barrier; chunk_id < num_chunk; ++chunk_id) {
        boost::dynamic_bitset<> result(size_per_chunk);
        result.resize(size_per_chunk);
        auto chunk = segment_.chunk_data<T>(field_offset, chunk_id);
        const T* data = chunk.data();
        for (int index = 0; index < size_per_chunk; ++index) {
            result[index] = element_func(data[index]);
        }
        Assert(result.size() == size_per_chunk);
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
        // TODO: disable check?
        if (val1 > val2) {
            // Empty
            auto size_per_chunk = segment_.size_per_chunk();
            auto num_chunk = upper_div(row_count_, size_per_chunk);
            RetType ret(num_chunk, boost::dynamic_bitset<>(size_per_chunk));
            return ret;
        }
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

template <typename Op>
struct relational {
    template <typename T, typename U>
    bool
    operator()(T const& a, U const& b) const {
        return Op{}(a, b);
    }
    template <typename... T>
    bool
    operator()(T const&...) const {
        PanicInfo("incompatible operands");
    }
};

using number = boost::variant<bool, int8_t, int16_t, int32_t, int64_t, float, double>;

template <typename Op>
auto
ExecExprVisitor::ExecCompareExprDispatcher(CompareExpr& expr, Op op) -> RetType {
    auto size_per_chunk = segment_.size_per_chunk();
    auto num_chunk = upper_div(row_count_, size_per_chunk);
    RetType bitsets;
    for (int64_t chunk_id = 0; chunk_id < num_chunk; ++chunk_id) {
        auto size = chunk_id == num_chunk - 1 ? row_count_ - chunk_id * size_per_chunk : size_per_chunk;

        auto getChunkData = [&, chunk_id](DataType type, FieldOffset offset) -> std::function<const number(int)> {
            switch (type) {
                case DataType::BOOL: {
                    auto chunk = segment_.chunk_data<bool>(offset, chunk_id);
                    return [chunk](int i) -> const number { return chunk.data()[i]; };
                }
                case DataType::INT8: {
                    auto chunk = segment_.chunk_data<int8_t>(offset, chunk_id);
                    return [chunk](int i) -> const number { return chunk.data()[i]; };
                }
                case DataType::INT16: {
                    auto chunk = segment_.chunk_data<int16_t>(offset, chunk_id);
                    return [chunk](int i) -> const number { return chunk.data()[i]; };
                }
                case DataType::INT32: {
                    auto chunk = segment_.chunk_data<int32_t>(offset, chunk_id);
                    return [chunk](int i) -> const number { return chunk.data()[i]; };
                }
                case DataType::INT64: {
                    auto chunk = segment_.chunk_data<int64_t>(offset, chunk_id);
                    return [chunk](int i) -> const number { return chunk.data()[i]; };
                }
                case DataType::FLOAT: {
                    auto chunk = segment_.chunk_data<float>(offset, chunk_id);
                    return [chunk](int i) -> const number { return chunk.data()[i]; };
                }
                case DataType::DOUBLE: {
                    auto chunk = segment_.chunk_data<double>(offset, chunk_id);
                    return [chunk](int i) -> const number { return chunk.data()[i]; };
                }
                default:
                    PanicInfo("unsupported datatype");
            }
        };
        auto left = getChunkData(expr.data_types_[0], expr.field_offsets_[0]);
        auto right = getChunkData(expr.data_types_[1], expr.field_offsets_[1]);

        boost::dynamic_bitset<> bitset(size_per_chunk);
        for (int i = 0; i < size; ++i) {
            bool is_in = boost::apply_visitor(relational<decltype(op)>{}, left(i), right(i));
            bitset[i] = is_in;
        }
        bitsets.emplace_back(std::move(bitset));
    }
    return bitsets;
}

void
ExecExprVisitor::visit(CompareExpr& expr) {
    Assert(expr.data_types_.size() == expr.field_offsets_.size());
    Assert(expr.data_types_.size() == 2);
    auto& schema = segment_.get_schema();

    for (auto i = 0; i < expr.field_offsets_.size(); i++) {
        auto& field_meta = schema[expr.field_offsets_[i]];
        Assert(expr.data_types_[i] == field_meta.get_data_type());
    }

    RetType ret;
    switch (expr.op) {
        case OpType::Equal: {
            ret = ExecCompareExprDispatcher(expr, std::equal_to<>{});
            break;
        }
        case OpType::NotEqual: {
            ret = ExecCompareExprDispatcher(expr, std::not_equal_to<>{});
            break;
        }
        case OpType::GreaterEqual: {
            ret = ExecCompareExprDispatcher(expr, std::greater_equal<>{});
            break;
        }
        case OpType::GreaterThan: {
            ret = ExecCompareExprDispatcher(expr, std::greater<>{});
            break;
        }
        case OpType::LessEqual: {
            ret = ExecCompareExprDispatcher(expr, std::less_equal<>{});
            break;
        }
        case OpType::LessThan: {
            ret = ExecCompareExprDispatcher(expr, std::less<>{});
            break;
        }
        default: {
            PanicInfo("unsupported optype");
        }
    }
    ret_ = std::move(ret);
}

template <typename T>
auto
ExecExprVisitor::ExecTermVisitorImpl(TermExpr& expr_raw) -> RetType {
    auto& expr = static_cast<TermExprImpl<T>&>(expr_raw);
    auto& schema = segment_.get_schema();

    auto field_offset = expr_raw.field_offset_;
    auto& field_meta = schema[field_offset];
    auto size_per_chunk = segment_.size_per_chunk();
    auto num_chunk = upper_div(row_count_, size_per_chunk);
    RetType bitsets;
    for (int64_t chunk_id = 0; chunk_id < num_chunk; ++chunk_id) {
        Span<T> chunk = segment_.chunk_data<T>(field_offset, chunk_id);

        auto size = chunk_id == num_chunk - 1 ? row_count_ - chunk_id * size_per_chunk : size_per_chunk;

        boost::dynamic_bitset<> bitset(size_per_chunk);
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
