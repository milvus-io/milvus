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

#include "query/generated/ExecExprVisitor.h"

#include <boost/variant.hpp>
#include <boost/utility/binary.hpp>
#include <cmath>
#include <cstdint>
#include <ctime>
#include <deque>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_set>
#include <utility>

#include "arrow/type_fwd.h"
#include "common/Json.h"
#include "common/Types.h"
#include "exceptions/EasyAssert.h"
#include "pb/plan.pb.h"
#include "query/ExprImpl.h"
#include "query/Relational.h"
#include "query/Utils.h"
#include "segcore/SegmentGrowingImpl.h"
#include "simdjson/error.h"
#include "query/PlanProto.h"
namespace milvus::query {
// THIS CONTAINS EXTRA BODY FOR VISITOR
// WILL BE USED BY GENERATOR
namespace impl {
class ExecExprVisitor : ExprVisitor {
 public:
    ExecExprVisitor(const segcore::SegmentInternalInterface& segment,
                    int64_t row_count,
                    Timestamp timestamp)
        : segment_(segment), row_count_(row_count), timestamp_(timestamp) {
    }

    BitsetType
    call_child(Expr& expr) {
        AssertInfo(!bitset_opt_.has_value(),
                   "[ExecExprVisitor]Bitset already has value before accept");
        expr.accept(*this);
        AssertInfo(bitset_opt_.has_value(),
                   "[ExecExprVisitor]Bitset doesn't have value after accept");
        auto res = std::move(bitset_opt_);
        bitset_opt_ = std::nullopt;
        return std::move(res.value());
    }

 public:
    template <typename T, typename IndexFunc, typename ElementFunc>
    auto
    ExecRangeVisitorImpl(FieldId field_id,
                         IndexFunc func,
                         ElementFunc element_func) -> BitsetType;

    template <typename T>
    auto
    ExecUnaryRangeVisitorDispatcherImpl(UnaryRangeExpr& expr_raw) -> BitsetType;

    template <typename T>
    auto
    ExecUnaryRangeVisitorDispatcher(UnaryRangeExpr& expr_raw) -> BitsetType;

    template <typename T>
    auto
    ExecBinaryArithOpEvalRangeVisitorDispatcher(
        BinaryArithOpEvalRangeExpr& expr_raw) -> BitsetType;

    template <typename T>
    auto
    ExecBinaryRangeVisitorDispatcher(BinaryRangeExpr& expr_raw) -> BitsetType;

    template <typename T>
    auto
    ExecTermVisitorImpl(TermExpr& expr_raw) -> BitsetType;

    template <typename T>
    auto
    ExecTermVisitorImplTemplate(TermExpr& expr_raw) -> BitsetType;

    template <typename CmpFunc>
    auto
    ExecCompareExprDispatcher(CompareExpr& expr, CmpFunc cmp_func)
        -> BitsetType;

 private:
    const segcore::SegmentInternalInterface& segment_;
    int64_t row_count_;
    Timestamp timestamp_;
    BitsetTypeOpt bitset_opt_;
};
}  // namespace impl

void
ExecExprVisitor::visit(LogicalUnaryExpr& expr) {
    using OpType = LogicalUnaryExpr::OpType;
    auto child_res = call_child(*expr.child_);
    BitsetType res = std::move(child_res);
    switch (expr.op_type_) {
        case OpType::LogicalNot: {
            res.flip();
            break;
        }
        default: {
            PanicInfo("Invalid Unary Op");
        }
    }
    AssertInfo(res.size() == row_count_,
               "[ExecExprVisitor]Size of results not equal row count");
    bitset_opt_ = std::move(res);
}

void
ExecExprVisitor::visit(LogicalBinaryExpr& expr) {
    using OpType = LogicalBinaryExpr::OpType;
    auto left = call_child(*expr.left_);
    auto right = call_child(*expr.right_);
    AssertInfo(left.size() == right.size(),
               "[ExecExprVisitor]Left size not equal to right size");
    auto res = std::move(left);
    switch (expr.op_type_) {
        case OpType::LogicalAnd: {
            res &= right;
            break;
        }
        case OpType::LogicalOr: {
            res |= right;
            break;
        }
        case OpType::LogicalXor: {
            res ^= right;
            break;
        }
        case OpType::LogicalMinus: {
            res -= right;
            break;
        }
        default: {
            PanicInfo("Invalid Binary Op");
        }
    }
    AssertInfo(res.size() == row_count_,
               "[ExecExprVisitor]Size of results not equal row count");
    bitset_opt_ = std::move(res);
}

static auto
Assemble(const std::deque<BitsetType>& srcs) -> BitsetType {
    BitsetType res;

    if (srcs.size() == 1) {
        return srcs[0];
    }

    int64_t total_size = 0;
    for (auto& chunk : srcs) {
        total_size += chunk.size();
    }
    res.resize(total_size);

    int64_t counter = 0;
    for (auto& chunk : srcs) {
        for (int64_t i = 0; i < chunk.size(); ++i) {
            res[counter + i] = chunk[i];
        }
        counter += chunk.size();
    }
    return res;
}

void
AppendOneChunk(BitsetType& result, const FixedVector<bool>& chunk_res) {
    // Append a value once instead of BITSET_BLOCK_BIT_SIZE times.
    auto AppendBlock = [&result](const bool* ptr, int n) {
        for (int i = 0; i < n; ++i) {
            BitSetBlockType val = 0;
            // This can use CPU SIMD optimzation
            uint8_t vals[BITSET_BLOCK_SIZE] = {0};
            for (size_t j = 0; j < 8; ++j) {
                for (size_t k = 0; k < BITSET_BLOCK_SIZE; ++k) {
                    vals[k] |= uint8_t(*(ptr + k * 8 + j)) << j;
                }
            }
            for (size_t j = 0; j < BITSET_BLOCK_SIZE; ++j) {
                val |= BitSetBlockType(vals[j]) << (8 * j);
            }
            result.append(val);
            ptr += BITSET_BLOCK_SIZE * 8;
        }
    };
    // Append bit for these bits that can not be union as a block
    // Usually n less than BITSET_BLOCK_BIT_SIZE.
    auto AppendBit = [&result](const bool* ptr, int n) {
        for (int i = 0; i < n; ++i) {
            bool bit = *ptr++;
            result.push_back(bit);
        }
    };

    size_t res_len = result.size();
    size_t chunk_len = chunk_res.size();
    const bool* chunk_ptr = chunk_res.data();

    int n_prefix =
        res_len % BITSET_BLOCK_BIT_SIZE == 0
            ? 0
            : std::min(BITSET_BLOCK_BIT_SIZE - res_len % BITSET_BLOCK_BIT_SIZE,
                       chunk_len);

    AppendBit(chunk_ptr, n_prefix);

    if (n_prefix == chunk_len)
        return;

    size_t n_block = (chunk_len - n_prefix) / BITSET_BLOCK_BIT_SIZE;
    size_t n_suffix = (chunk_len - n_prefix) % BITSET_BLOCK_BIT_SIZE;

    AppendBlock(chunk_ptr + n_prefix, n_block);

    AppendBit(chunk_ptr + n_prefix + n_block * BITSET_BLOCK_BIT_SIZE, n_suffix);

    return;
}

BitsetType
AssembleChunk(const std::vector<FixedVector<bool>>& results) {
    BitsetType assemble_result;
    for (auto& result : results) {
        AppendOneChunk(assemble_result, result);
    }
    return assemble_result;
}

template <typename T, typename IndexFunc, typename ElementFunc>
auto
ExecExprVisitor::ExecRangeVisitorImpl(FieldId field_id,
                                      IndexFunc index_func,
                                      ElementFunc element_func) -> BitsetType {
    auto& schema = segment_.get_schema();
    auto& field_meta = schema[field_id];
    auto indexing_barrier = segment_.num_chunk_index(field_id);
    auto size_per_chunk = segment_.size_per_chunk();
    auto num_chunk = upper_div(row_count_, size_per_chunk);
    std::vector<FixedVector<bool>> results;
    results.reserve(num_chunk);

    typedef std::
        conditional_t<std::is_same_v<T, std::string_view>, std::string, T>
            IndexInnerType;
    using Index = index::ScalarIndex<IndexInnerType>;
    for (auto chunk_id = 0; chunk_id < indexing_barrier; ++chunk_id) {
        const Index& indexing =
            segment_.chunk_scalar_index<IndexInnerType>(field_id, chunk_id);
        // NOTE: knowhere is not const-ready
        // This is a dirty workaround
        auto data = index_func(const_cast<Index*>(&indexing));
        AssertInfo(data.size() == size_per_chunk,
                   "[ExecExprVisitor]Data size not equal to size_per_chunk");
        results.emplace_back(std::move(data));
    }
    for (auto chunk_id = indexing_barrier; chunk_id < num_chunk; ++chunk_id) {
        auto this_size = chunk_id == num_chunk - 1
                             ? row_count_ - chunk_id * size_per_chunk
                             : size_per_chunk;
        FixedVector<bool> chunk_res(this_size);
        auto chunk = segment_.chunk_data<T>(field_id, chunk_id);
        const T* data = chunk.data();
        // Can use CPU SIMD optimazation to speed up
        for (int index = 0; index < this_size; ++index) {
            chunk_res[index] = element_func(data[index]);
        }
        results.emplace_back(std::move(chunk_res));
    }
    auto final_result = AssembleChunk(results);
    AssertInfo(final_result.size() == row_count_,
               "[ExecExprVisitor]Final result size not equal to row count");
    return final_result;
}

template <typename T, typename IndexFunc, typename ElementFunc>
auto
ExecExprVisitor::ExecDataRangeVisitorImpl(FieldId field_id,
                                          IndexFunc index_func,
                                          ElementFunc element_func)
    -> BitsetType {
    auto& schema = segment_.get_schema();
    auto& field_meta = schema[field_id];
    auto size_per_chunk = segment_.size_per_chunk();
    auto num_chunk = upper_div(row_count_, size_per_chunk);
    auto indexing_barrier = segment_.num_chunk_index(field_id);
    auto data_barrier = segment_.num_chunk_data(field_id);
    AssertInfo(std::max(data_barrier, indexing_barrier) == num_chunk,
               "max(data_barrier, index_barrier) not equal to num_chunk");
    std::vector<FixedVector<bool>> results;
    results.reserve(num_chunk);

    // for growing segment, indexing_barrier will always less than data_barrier
    // so growing segment will always execute expr plan using raw data
    // if sealed segment has loaded raw data on this field, then index_barrier = 0 and data_barrier = 1
    // in this case, sealed segment execute expr plan using raw data
    for (auto chunk_id = 0; chunk_id < data_barrier; ++chunk_id) {
        auto this_size = chunk_id == num_chunk - 1
                             ? row_count_ - chunk_id * size_per_chunk
                             : size_per_chunk;
        FixedVector<bool> result(this_size);
        auto chunk = segment_.chunk_data<T>(field_id, chunk_id);
        const T* data = chunk.data();
        for (int index = 0; index < this_size; ++index) {
            result[index] = element_func(data[index]);
        }
        AssertInfo(result.size() == this_size,
                   "[ExecExprVisitor]Chunk result size not equal to "
                   "expected size");
        results.emplace_back(std::move(result));
    }

    // if sealed segment has loaded scalar index for this field, then index_barrier = 1 and data_barrier = 0
    // in this case, sealed segment execute expr plan using scalar index
    typedef std::
        conditional_t<std::is_same_v<T, std::string_view>, std::string, T>
            IndexInnerType;
    using Index = index::ScalarIndex<IndexInnerType>;
    for (auto chunk_id = data_barrier; chunk_id < indexing_barrier;
         ++chunk_id) {
        auto& indexing =
            segment_.chunk_scalar_index<IndexInnerType>(field_id, chunk_id);
        auto this_size = const_cast<Index*>(&indexing)->Count();
        FixedVector<bool> result(this_size);
        for (int offset = 0; offset < this_size; ++offset) {
            result[offset] = index_func(const_cast<Index*>(&indexing), offset);
        }
        results.emplace_back(std::move(result));
    }

    auto final_result = AssembleChunk(results);
    AssertInfo(final_result.size() == row_count_,
               "[ExecExprVisitor]Final result size not equal to row count");
    return final_result;
}

#pragma clang diagnostic push
#pragma ide diagnostic ignored "Simplify"
template <typename T>
auto
ExecExprVisitor::ExecUnaryRangeVisitorDispatcherImpl(UnaryRangeExpr& expr_raw)
    -> BitsetType {
    typedef std::
        conditional_t<std::is_same_v<T, std::string_view>, std::string, T>
            IndexInnerType;
    using Index = index::ScalarIndex<IndexInnerType>;
    auto& expr = static_cast<UnaryRangeExprImpl<IndexInnerType>&>(expr_raw);

    auto op = expr.op_type_;
    auto val = IndexInnerType(expr.value_);
    auto field_id = expr.column_.field_id;
    switch (op) {
        case OpType::Equal: {
            auto index_func = [&](Index* index) { return index->In(1, &val); };
            auto elem_func = [&](MayConstRef<T> x) { return (x == val); };
            return ExecRangeVisitorImpl<T>(field_id, index_func, elem_func);
        }
        case OpType::NotEqual: {
            auto index_func = [&](Index* index) {
                return index->NotIn(1, &val);
            };
            auto elem_func = [&](MayConstRef<T> x) { return (x != val); };
            return ExecRangeVisitorImpl<T>(field_id, index_func, elem_func);
        }
        case OpType::GreaterEqual: {
            auto index_func = [&](Index* index) {
                return index->Range(val, OpType::GreaterEqual);
            };
            auto elem_func = [&](MayConstRef<T> x) { return (x >= val); };
            return ExecRangeVisitorImpl<T>(field_id, index_func, elem_func);
        }
        case OpType::GreaterThan: {
            auto index_func = [&](Index* index) {
                return index->Range(val, OpType::GreaterThan);
            };
            auto elem_func = [&](MayConstRef<T> x) { return (x > val); };
            return ExecRangeVisitorImpl<T>(field_id, index_func, elem_func);
        }
        case OpType::LessEqual: {
            auto index_func = [&](Index* index) {
                return index->Range(val, OpType::LessEqual);
            };
            auto elem_func = [&](MayConstRef<T> x) { return (x <= val); };
            return ExecRangeVisitorImpl<T>(field_id, index_func, elem_func);
        }
        case OpType::LessThan: {
            auto index_func = [&](Index* index) {
                return index->Range(val, OpType::LessThan);
            };
            auto elem_func = [&](MayConstRef<T> x) { return (x < val); };
            return ExecRangeVisitorImpl<T>(field_id, index_func, elem_func);
        }
        case OpType::PrefixMatch: {
            auto index_func = [&](Index* index) {
                auto dataset = std::make_unique<Dataset>();
                dataset->Set(milvus::index::OPERATOR_TYPE, OpType::PrefixMatch);
                dataset->Set(milvus::index::PREFIX_VALUE, val);
                return index->Query(std::move(dataset));
            };
            auto elem_func = [&](MayConstRef<T> x) {
                return Match(x, val, op);
            };
            return ExecRangeVisitorImpl<T>(field_id, index_func, elem_func);
        }
        // TODO: PostfixMatch
        default: {
            PanicInfo("unsupported range node");
        }
    }
}
#pragma clang diagnostic pop

template <typename T>
auto
ExecExprVisitor::ExecUnaryRangeVisitorDispatcher(UnaryRangeExpr& expr_raw)
    -> BitsetType {
    // bool type is integral but will never be overflowed,
    // the check method may evaluate it out of range with bool type,
    // exclude bool type here
    if constexpr (std::is_integral_v<T> && !std::is_same_v<T, bool>) {
        auto& expr = static_cast<UnaryRangeExprImpl<int64_t>&>(expr_raw);
        auto val = expr.value_;

        if (!out_of_range<T>(val)) {
            return ExecUnaryRangeVisitorDispatcherImpl<T>(expr_raw);
        }

        // see also: https://github.com/milvus-io/milvus/issues/23646.
        switch (expr.op_type_) {
            case proto::plan::GreaterThan:
            case proto::plan::GreaterEqual: {
                BitsetType r(row_count_);
                if (lt_lb<T>(val)) {
                    r.set();
                }
                return r;
            }

            case proto::plan::LessThan:
            case proto::plan::LessEqual: {
                BitsetType r(row_count_);
                if (gt_ub<T>(val)) {
                    r.set();
                }
                return r;
            }

            case proto::plan::Equal: {
                BitsetType r(row_count_);
                r.reset();
                return r;
            }

            case proto::plan::NotEqual: {
                BitsetType r(row_count_);
                r.set();
                return r;
            }

            default: {
                PanicInfo("unsupported range node");
            }
        }
    }
    return ExecUnaryRangeVisitorDispatcherImpl<T>(expr_raw);
}

template <typename ExprValueType>
auto
ExecExprVisitor::ExecUnaryRangeVisitorDispatcherJson(UnaryRangeExpr& expr_raw)
    -> BitsetType {
    using Index = index::ScalarIndex<milvus::Json>;
    auto& expr = static_cast<UnaryRangeExprImpl<ExprValueType>&>(expr_raw);

    auto op = expr.op_type_;
    auto val = expr.value_;
    auto pointer = milvus::Json::pointer(expr.column_.nested_path);
    auto field_id = expr.column_.field_id;
    auto index_func = [=](Index* index) { return TargetBitmap{}; };
    using GetType =
        std::conditional_t<std::is_same_v<ExprValueType, std::string>,
                           std::string_view,
                           ExprValueType>;

#define UnaryRangeJSONCompare(cmp)                            \
    do {                                                      \
        auto x = json.template at<GetType>(pointer);          \
        if (x.error()) {                                      \
            if constexpr (std::is_same_v<GetType, int64_t>) { \
                auto x = json.template at<double>(pointer);   \
                return !x.error() && (cmp);                   \
            }                                                 \
            return false;                                     \
        }                                                     \
        return (cmp);                                         \
    } while (false)

#define UnaryRangeJSONCompareNotEqual(cmp)                    \
    do {                                                      \
        auto x = json.template at<GetType>(pointer);          \
        if (x.error()) {                                      \
            if constexpr (std::is_same_v<GetType, int64_t>) { \
                auto x = json.template at<double>(pointer);   \
                return x.error() || (cmp);                    \
            }                                                 \
            return true;                                      \
        }                                                     \
        return (cmp);                                         \
    } while (false)

    switch (op) {
        case OpType::Equal: {
            auto elem_func = [&](const milvus::Json& json) {
                UnaryRangeJSONCompare(x.value() == val);
            };
            return ExecRangeVisitorImpl<milvus::Json>(
                field_id, index_func, elem_func);
        }
        case OpType::NotEqual: {
            auto elem_func = [&](const milvus::Json& json) {
                UnaryRangeJSONCompareNotEqual(x.value() != val);
            };
            return ExecRangeVisitorImpl<milvus::Json>(
                field_id, index_func, elem_func);
        }
        case OpType::GreaterEqual: {
            auto elem_func = [&](const milvus::Json& json) {
                UnaryRangeJSONCompare(x.value() >= val);
            };
            return ExecRangeVisitorImpl<milvus::Json>(
                field_id, index_func, elem_func);
        }
        case OpType::GreaterThan: {
            auto elem_func = [&](const milvus::Json& json) {
                UnaryRangeJSONCompare(x.value() > val);
            };
            return ExecRangeVisitorImpl<milvus::Json>(
                field_id, index_func, elem_func);
        }
        case OpType::LessEqual: {
            auto elem_func = [&](const milvus::Json& json) {
                UnaryRangeJSONCompare(x.value() <= val);
            };
            return ExecRangeVisitorImpl<milvus::Json>(
                field_id, index_func, elem_func);
        }
        case OpType::LessThan: {
            auto elem_func = [&](const milvus::Json& json) {
                UnaryRangeJSONCompare(x.value() < val);
            };
            return ExecRangeVisitorImpl<milvus::Json>(
                field_id, index_func, elem_func);
        }
        case OpType::PrefixMatch: {
            auto elem_func = [&](const milvus::Json& json) {
                UnaryRangeJSONCompare(Match(ExprValueType(x.value()), val, op));
            };
            return ExecRangeVisitorImpl<milvus::Json>(
                field_id, index_func, elem_func);
        }
        // TODO: PostfixMatch
        default: {
            PanicInfo("unsupported range node");
        }
    }
}

#pragma clang diagnostic push
#pragma ide diagnostic ignored "Simplify"
template <typename T>
auto
ExecExprVisitor::ExecBinaryArithOpEvalRangeVisitorDispatcher(
    BinaryArithOpEvalRangeExpr& expr_raw) -> BitsetType {
    // see also: https://github.com/milvus-io/milvus/issues/23646.
    typedef std::conditional_t<std::is_integral_v<T> &&
                                   !std::is_same_v<bool, T>,
                               int64_t,
                               T>
        HighPrecisionType;

    auto& expr =
        static_cast<BinaryArithOpEvalRangeExprImpl<HighPrecisionType>&>(
            expr_raw);
    using Index = index::ScalarIndex<T>;
    auto arith_op = expr.arith_op_;
    auto right_operand = expr.right_operand_;
    auto op = expr.op_type_;
    auto val = expr.value_;

    switch (op) {
        case OpType::Equal: {
            switch (arith_op) {
                case ArithOpType::Add: {
                    auto index_func = [val, right_operand](Index* index,
                                                           size_t offset) {
                        auto x = index->Reverse_Lookup(offset);
                        return (x + right_operand) == val;
                    };
                    auto elem_func = [val, right_operand](MayConstRef<T> x) {
                        return ((x + right_operand) == val);
                    };
                    return ExecDataRangeVisitorImpl<T>(
                        expr.column_.field_id, index_func, elem_func);
                }
                case ArithOpType::Sub: {
                    auto index_func = [val, right_operand](Index* index,
                                                           size_t offset) {
                        auto x = index->Reverse_Lookup(offset);
                        return (x - right_operand) == val;
                    };
                    auto elem_func = [val, right_operand](MayConstRef<T> x) {
                        return ((x - right_operand) == val);
                    };
                    return ExecDataRangeVisitorImpl<T>(
                        expr.column_.field_id, index_func, elem_func);
                }
                case ArithOpType::Mul: {
                    auto index_func = [val, right_operand](Index* index,
                                                           size_t offset) {
                        auto x = index->Reverse_Lookup(offset);
                        return (x * right_operand) == val;
                    };
                    auto elem_func = [val, right_operand](MayConstRef<T> x) {
                        return ((x * right_operand) == val);
                    };
                    return ExecDataRangeVisitorImpl<T>(
                        expr.column_.field_id, index_func, elem_func);
                }
                case ArithOpType::Div: {
                    auto index_func = [val, right_operand](Index* index,
                                                           size_t offset) {
                        auto x = index->Reverse_Lookup(offset);
                        return (x / right_operand) == val;
                    };
                    auto elem_func = [val, right_operand](MayConstRef<T> x) {
                        return ((x / right_operand) == val);
                    };
                    return ExecDataRangeVisitorImpl<T>(
                        expr.column_.field_id, index_func, elem_func);
                }
                case ArithOpType::Mod: {
                    auto index_func = [val, right_operand](Index* index,
                                                           size_t offset) {
                        auto x = index->Reverse_Lookup(offset);
                        return static_cast<T>(fmod(x, right_operand)) == val;
                    };
                    auto elem_func = [val, right_operand](MayConstRef<T> x) {
                        return (static_cast<T>(fmod(x, right_operand)) == val);
                    };
                    return ExecDataRangeVisitorImpl<T>(
                        expr.column_.field_id, index_func, elem_func);
                }
                default: {
                    PanicInfo("unsupported arithmetic operation");
                }
            }
        }
        case OpType::NotEqual: {
            switch (arith_op) {
                case ArithOpType::Add: {
                    auto index_func = [val, right_operand](Index* index,
                                                           size_t offset) {
                        auto x = index->Reverse_Lookup(offset);
                        return (x + right_operand) != val;
                    };
                    auto elem_func = [val, right_operand](MayConstRef<T> x) {
                        return ((x + right_operand) != val);
                    };
                    return ExecDataRangeVisitorImpl<T>(
                        expr.column_.field_id, index_func, elem_func);
                }
                case ArithOpType::Sub: {
                    auto index_func = [val, right_operand](Index* index,
                                                           size_t offset) {
                        auto x = index->Reverse_Lookup(offset);
                        return (x - right_operand) != val;
                    };
                    auto elem_func = [val, right_operand](MayConstRef<T> x) {
                        return ((x - right_operand) != val);
                    };
                    return ExecDataRangeVisitorImpl<T>(
                        expr.column_.field_id, index_func, elem_func);
                }
                case ArithOpType::Mul: {
                    auto index_func = [val, right_operand](Index* index,
                                                           size_t offset) {
                        auto x = index->Reverse_Lookup(offset);
                        return (x * right_operand) != val;
                    };
                    auto elem_func = [val, right_operand](MayConstRef<T> x) {
                        return ((x * right_operand) != val);
                    };
                    return ExecDataRangeVisitorImpl<T>(
                        expr.column_.field_id, index_func, elem_func);
                }
                case ArithOpType::Div: {
                    auto index_func = [val, right_operand](Index* index,
                                                           size_t offset) {
                        auto x = index->Reverse_Lookup(offset);
                        return (x / right_operand) != val;
                    };
                    auto elem_func = [val, right_operand](MayConstRef<T> x) {
                        return ((x / right_operand) != val);
                    };
                    return ExecDataRangeVisitorImpl<T>(
                        expr.column_.field_id, index_func, elem_func);
                }
                case ArithOpType::Mod: {
                    auto index_func = [val, right_operand](Index* index,
                                                           size_t offset) {
                        auto x = index->Reverse_Lookup(offset);
                        return static_cast<T>(fmod(x, right_operand)) != val;
                    };
                    auto elem_func = [val, right_operand](MayConstRef<T> x) {
                        return (static_cast<T>(fmod(x, right_operand)) != val);
                    };
                    return ExecDataRangeVisitorImpl<T>(
                        expr.column_.field_id, index_func, elem_func);
                }
                default: {
                    PanicInfo("unsupported arithmetic operation");
                }
            }
        }
        default: {
            PanicInfo("unsupported range node with arithmetic operation");
        }
    }
}
#pragma clang diagnostic pop

template <typename ExprValueType>
auto
ExecExprVisitor::ExecBinaryArithOpEvalRangeVisitorDispatcherJson(
    BinaryArithOpEvalRangeExpr& expr_raw) -> BitsetType {
    auto& expr =
        static_cast<BinaryArithOpEvalRangeExprImpl<ExprValueType>&>(expr_raw);
    using Index = index::ScalarIndex<milvus::Json>;
    using GetType =
        std::conditional_t<std::is_same_v<ExprValueType, std::string>,
                           std::string_view,
                           ExprValueType>;

    auto arith_op = expr.arith_op_;
    auto right_operand = expr.right_operand_;
    auto op = expr.op_type_;
    auto val = expr.value_;
    auto pointer = milvus::Json::pointer(expr.column_.nested_path);

#define BinaryArithRangeJSONCompare(cmp)                      \
    do {                                                      \
        auto x = json.template at<GetType>(pointer);          \
        if (x.error()) {                                      \
            if constexpr (std::is_same_v<GetType, int64_t>) { \
                auto x = json.template at<double>(pointer);   \
                return !x.error() && (cmp);                   \
            }                                                 \
            return false;                                     \
        }                                                     \
        return (cmp);                                         \
    } while (false)

#define BinaryArithRangeJSONCompareNotEqual(cmp)              \
    do {                                                      \
        auto x = json.template at<GetType>(pointer);          \
        if (x.error()) {                                      \
            if constexpr (std::is_same_v<GetType, int64_t>) { \
                auto x = json.template at<double>(pointer);   \
                return x.error() || (cmp);                    \
            }                                                 \
            return true;                                      \
        }                                                     \
        return (cmp);                                         \
    } while (false)

    switch (op) {
        case OpType::Equal: {
            switch (arith_op) {
                case ArithOpType::Add: {
                    auto index_func = [val, right_operand](Index* index,
                                                           size_t offset) {
                        return false;
                    };
                    auto elem_func = [&](const milvus::Json& json) {
                        BinaryArithRangeJSONCompare(x.value() + right_operand ==
                                                    val);
                    };
                    return ExecDataRangeVisitorImpl<milvus::Json>(
                        expr.column_.field_id, index_func, elem_func);
                }
                case ArithOpType::Sub: {
                    auto index_func = [val, right_operand](Index* index,
                                                           size_t offset) {
                        return false;
                    };
                    auto elem_func = [&](const milvus::Json& json) {
                        BinaryArithRangeJSONCompare(x.value() - right_operand ==
                                                    val);
                    };
                    return ExecDataRangeVisitorImpl<milvus::Json>(
                        expr.column_.field_id, index_func, elem_func);
                }
                case ArithOpType::Mul: {
                    auto index_func = [val, right_operand](Index* index,
                                                           size_t offset) {
                        return false;
                    };
                    auto elem_func = [&](const milvus::Json& json) {
                        BinaryArithRangeJSONCompare(x.value() * right_operand ==
                                                    val);
                    };
                    return ExecDataRangeVisitorImpl<milvus::Json>(
                        expr.column_.field_id, index_func, elem_func);
                }
                case ArithOpType::Div: {
                    auto index_func = [val, right_operand](Index* index,
                                                           size_t offset) {
                        return false;
                    };
                    auto elem_func = [&](const milvus::Json& json) {
                        BinaryArithRangeJSONCompare(x.value() / right_operand ==
                                                    val);
                    };
                    return ExecDataRangeVisitorImpl<milvus::Json>(
                        expr.column_.field_id, index_func, elem_func);
                }
                case ArithOpType::Mod: {
                    auto index_func = [val, right_operand](Index* index,
                                                           size_t offset) {
                        return false;
                    };
                    auto elem_func = [&](const milvus::Json& json) {
                        BinaryArithRangeJSONCompare(
                            static_cast<ExprValueType>(
                                fmod(x.value(), right_operand)) == val);
                    };
                    return ExecDataRangeVisitorImpl<milvus::Json>(
                        expr.column_.field_id, index_func, elem_func);
                }
                default: {
                    PanicInfo("unsupported arithmetic operation");
                }
            }
        }
        case OpType::NotEqual: {
            switch (arith_op) {
                case ArithOpType::Add: {
                    auto index_func = [val, right_operand](Index* index,
                                                           size_t offset) {
                        return false;
                    };
                    auto elem_func = [&](const milvus::Json& json) {
                        BinaryArithRangeJSONCompareNotEqual(
                            x.value() + right_operand != val);
                    };
                    return ExecDataRangeVisitorImpl<milvus::Json>(
                        expr.column_.field_id, index_func, elem_func);
                }
                case ArithOpType::Sub: {
                    auto index_func = [val, right_operand](Index* index,
                                                           size_t offset) {
                        return false;
                    };
                    auto elem_func = [&](const milvus::Json& json) {
                        BinaryArithRangeJSONCompareNotEqual(
                            x.value() - right_operand != val);
                    };
                    return ExecDataRangeVisitorImpl<milvus::Json>(
                        expr.column_.field_id, index_func, elem_func);
                }
                case ArithOpType::Mul: {
                    auto index_func = [val, right_operand](Index* index,
                                                           size_t offset) {
                        return false;
                    };
                    auto elem_func = [&](const milvus::Json& json) {
                        BinaryArithRangeJSONCompareNotEqual(
                            x.value() * right_operand != val);
                    };
                    return ExecDataRangeVisitorImpl<milvus::Json>(
                        expr.column_.field_id, index_func, elem_func);
                }
                case ArithOpType::Div: {
                    auto index_func = [val, right_operand](Index* index,
                                                           size_t offset) {
                        return false;
                    };
                    auto elem_func = [&](const milvus::Json& json) {
                        BinaryArithRangeJSONCompareNotEqual(
                            x.value() / right_operand != val);
                    };
                    return ExecDataRangeVisitorImpl<milvus::Json>(
                        expr.column_.field_id, index_func, elem_func);
                }
                case ArithOpType::Mod: {
                    auto index_func = [val, right_operand](Index* index,
                                                           size_t offset) {
                        return false;
                    };
                    auto elem_func = [&](const milvus::Json& json) {
                        BinaryArithRangeJSONCompareNotEqual(
                            static_cast<ExprValueType>(
                                fmod(x.value(), right_operand)) != val);
                    };
                    return ExecDataRangeVisitorImpl<milvus::Json>(
                        expr.column_.field_id, index_func, elem_func);
                }
                default: {
                    PanicInfo("unsupported arithmetic operation");
                }
            }
        }
        default: {
            PanicInfo("unsupported range node with arithmetic operation");
        }
    }
}  // namespace milvus::query

#pragma clang diagnostic push
#pragma ide diagnostic ignored "Simplify"
template <typename T>
auto
ExecExprVisitor::ExecBinaryRangeVisitorDispatcher(BinaryRangeExpr& expr_raw)
    -> BitsetType {
    typedef std::
        conditional_t<std::is_same_v<T, std::string_view>, std::string, T>
            IndexInnerType;
    using Index = index::ScalarIndex<IndexInnerType>;

    // see also: https://github.com/milvus-io/milvus/issues/23646.
    typedef std::conditional_t<std::is_integral_v<IndexInnerType> &&
                                   !std::is_same_v<bool, T>,
                               int64_t,
                               IndexInnerType>
        HighPrecisionType;
    auto& expr = static_cast<BinaryRangeExprImpl<HighPrecisionType>&>(expr_raw);

    bool lower_inclusive = expr.lower_inclusive_;
    bool upper_inclusive = expr.upper_inclusive_;
    auto val1 = static_cast<HighPrecisionType>(expr.lower_value_);
    auto val2 = static_cast<HighPrecisionType>(expr.upper_value_);

    if constexpr (std::is_integral_v<T> && !std::is_same_v<bool, T>) {
        if (gt_ub<T>(val1)) {
            BitsetType r(row_count_);
            r.reset();
            return r;
        } else if (lt_lb<T>(val1)) {
            val1 = std::numeric_limits<T>::min();
            lower_inclusive = true;
        }

        if (gt_ub<T>(val2)) {
            val2 = std::numeric_limits<T>::max();
            upper_inclusive = true;
        } else if (lt_lb<T>(val2)) {
            BitsetType r(row_count_);
            r.reset();
            return r;
        }
    }

    auto index_func = [=](Index* index) {
        return index->Range(val1, lower_inclusive, val2, upper_inclusive);
    };

    if (lower_inclusive && upper_inclusive) {
        auto elem_func = [val1, val2](MayConstRef<T> x) {
            return (val1 <= x && x <= val2);
        };
        return ExecRangeVisitorImpl<T>(
            expr.column_.field_id, index_func, elem_func);
    } else if (lower_inclusive && !upper_inclusive) {
        auto elem_func = [val1, val2](MayConstRef<T> x) {
            return (val1 <= x && x < val2);
        };
        return ExecRangeVisitorImpl<T>(
            expr.column_.field_id, index_func, elem_func);
    } else if (!lower_inclusive && upper_inclusive) {
        auto elem_func = [val1, val2](MayConstRef<T> x) {
            return (val1 < x && x <= val2);
        };
        return ExecRangeVisitorImpl<T>(
            expr.column_.field_id, index_func, elem_func);
    } else {
        auto elem_func = [val1, val2](MayConstRef<T> x) {
            return (val1 < x && x < val2);
        };
        return ExecRangeVisitorImpl<T>(
            expr.column_.field_id, index_func, elem_func);
    }
}
#pragma clang diagnostic pop

template <typename ExprValueType>
auto
ExecExprVisitor::ExecBinaryRangeVisitorDispatcherJson(BinaryRangeExpr& expr_raw)
    -> BitsetType {
    using Index = index::ScalarIndex<milvus::Json>;
    using GetType =
        std::conditional_t<std::is_same_v<ExprValueType, std::string>,
                           std::string_view,
                           ExprValueType>;

    auto& expr = static_cast<BinaryRangeExprImpl<ExprValueType>&>(expr_raw);
    bool lower_inclusive = expr.lower_inclusive_;
    bool upper_inclusive = expr.upper_inclusive_;
    ExprValueType val1 = expr.lower_value_;
    ExprValueType val2 = expr.upper_value_;
    auto pointer = milvus::Json::pointer(expr.column_.nested_path);

    // no json index now
    auto index_func = [=](Index* index) { return TargetBitmap{}; };

#define BinaryRangeJSONCompare(cmp)                           \
    do {                                                      \
        auto x = json.template at<GetType>(pointer);          \
        if (x.error()) {                                      \
            if constexpr (std::is_same_v<GetType, int64_t>) { \
                auto x = json.template at<double>(pointer);   \
                if (!x.error()) {                             \
                    auto value = x.value();                   \
                    return (cmp);                             \
                }                                             \
            }                                                 \
            return false;                                     \
        }                                                     \
        auto value = x.value();                               \
        return (cmp);                                         \
    } while (false)

    if (lower_inclusive && upper_inclusive) {
        auto elem_func = [&](const milvus::Json& json) {
            BinaryRangeJSONCompare(val1 <= value && value <= val2);
        };
        return ExecRangeVisitorImpl<milvus::Json>(
            expr.column_.field_id, index_func, elem_func);
    } else if (lower_inclusive && !upper_inclusive) {
        auto elem_func = [&](const milvus::Json& json) {
            BinaryRangeJSONCompare(val1 <= value && value < val2);
        };
        return ExecRangeVisitorImpl<milvus::Json>(
            expr.column_.field_id, index_func, elem_func);
    } else if (!lower_inclusive && upper_inclusive) {
        auto elem_func = [&](const milvus::Json& json) {
            BinaryRangeJSONCompare(val1 < value && value <= val2);
        };
        return ExecRangeVisitorImpl<milvus::Json>(
            expr.column_.field_id, index_func, elem_func);
    } else {
        auto elem_func = [&](const milvus::Json& json) {
            BinaryRangeJSONCompare(val1 < value && value < val2);
        };
        return ExecRangeVisitorImpl<milvus::Json>(
            expr.column_.field_id, index_func, elem_func);
    }
}

void
ExecExprVisitor::visit(UnaryRangeExpr& expr) {
    auto& field_meta = segment_.get_schema()[expr.column_.field_id];
    AssertInfo(expr.column_.data_type == field_meta.get_data_type(),
               "[ExecExprVisitor]DataType of expr isn't field_meta data type");
    BitsetType res;
    switch (expr.column_.data_type) {
        case DataType::BOOL: {
            res = ExecUnaryRangeVisitorDispatcher<bool>(expr);
            break;
        }
        case DataType::INT8: {
            res = ExecUnaryRangeVisitorDispatcher<int8_t>(expr);
            break;
        }
        case DataType::INT16: {
            res = ExecUnaryRangeVisitorDispatcher<int16_t>(expr);
            break;
        }
        case DataType::INT32: {
            res = ExecUnaryRangeVisitorDispatcher<int32_t>(expr);
            break;
        }
        case DataType::INT64: {
            res = ExecUnaryRangeVisitorDispatcher<int64_t>(expr);
            break;
        }
        case DataType::FLOAT: {
            res = ExecUnaryRangeVisitorDispatcher<float>(expr);
            break;
        }
        case DataType::DOUBLE: {
            res = ExecUnaryRangeVisitorDispatcher<double>(expr);
            break;
        }
        case DataType::VARCHAR: {
            if (segment_.type() == SegmentType::Growing) {
                res = ExecUnaryRangeVisitorDispatcher<std::string>(expr);
            } else {
                res = ExecUnaryRangeVisitorDispatcher<std::string_view>(expr);
            }
            break;
        }
        case DataType::JSON: {
            switch (expr.val_case_) {
                case proto::plan::GenericValue::ValCase::kBoolVal:
                    res = ExecUnaryRangeVisitorDispatcherJson<bool>(expr);
                    break;
                case proto::plan::GenericValue::ValCase::kInt64Val:
                    res = ExecUnaryRangeVisitorDispatcherJson<int64_t>(expr);
                    break;
                case proto::plan::GenericValue::ValCase::kFloatVal:
                    res = ExecUnaryRangeVisitorDispatcherJson<double>(expr);
                    break;
                case proto::plan::GenericValue::ValCase::kStringVal:
                    res =
                        ExecUnaryRangeVisitorDispatcherJson<std::string>(expr);
                    break;
                default:
                    PanicInfo(
                        fmt::format("unknown data type: {}", expr.val_case_));
            }
            break;
        }
        default:
            PanicInfo(fmt::format("unsupported data type: {}",
                                  expr.column_.data_type));
    }
    AssertInfo(res.size() == row_count_,
               "[ExecExprVisitor]Size of results not equal row count");
    bitset_opt_ = std::move(res);
}

void
ExecExprVisitor::visit(BinaryArithOpEvalRangeExpr& expr) {
    auto& field_meta = segment_.get_schema()[expr.column_.field_id];
    AssertInfo(expr.column_.data_type == field_meta.get_data_type(),
               "[ExecExprVisitor]DataType of expr isn't field_meta data type");
    BitsetType res;
    switch (expr.column_.data_type) {
        case DataType::INT8: {
            res = ExecBinaryArithOpEvalRangeVisitorDispatcher<int8_t>(expr);
            break;
        }
        case DataType::INT16: {
            res = ExecBinaryArithOpEvalRangeVisitorDispatcher<int16_t>(expr);
            break;
        }
        case DataType::INT32: {
            res = ExecBinaryArithOpEvalRangeVisitorDispatcher<int32_t>(expr);
            break;
        }
        case DataType::INT64: {
            res = ExecBinaryArithOpEvalRangeVisitorDispatcher<int64_t>(expr);
            break;
        }
        case DataType::FLOAT: {
            res = ExecBinaryArithOpEvalRangeVisitorDispatcher<float>(expr);
            break;
        }
        case DataType::DOUBLE: {
            res = ExecBinaryArithOpEvalRangeVisitorDispatcher<double>(expr);
            break;
        }
        case DataType::JSON: {
            switch (expr.val_case_) {
                case proto::plan::GenericValue::ValCase::kBoolVal: {
                    res = ExecBinaryArithOpEvalRangeVisitorDispatcherJson<bool>(
                        expr);
                    break;
                }
                case proto::plan::GenericValue::ValCase::kInt64Val: {
                    res = ExecBinaryArithOpEvalRangeVisitorDispatcherJson<
                        int64_t>(expr);
                    break;
                }
                case proto::plan::GenericValue::ValCase::kFloatVal: {
                    res =
                        ExecBinaryArithOpEvalRangeVisitorDispatcherJson<double>(
                            expr);
                    break;
                }
                default: {
                    PanicInfo(
                        fmt::format("unsupported value type {} in expression",
                                    expr.val_case_));
                }
            }
            break;
        }
        default:
            PanicInfo(fmt::format("unsupported data type: {}",
                                  expr.column_.data_type));
    }
    AssertInfo(res.size() == row_count_,
               "[ExecExprVisitor]Size of results not equal row count");
    bitset_opt_ = std::move(res);
}

void
ExecExprVisitor::visit(BinaryRangeExpr& expr) {
    auto& field_meta = segment_.get_schema()[expr.column_.field_id];
    AssertInfo(expr.column_.data_type == field_meta.get_data_type(),
               "[ExecExprVisitor]DataType of expr isn't field_meta data type");
    BitsetType res;
    switch (expr.column_.data_type) {
        case DataType::BOOL: {
            res = ExecBinaryRangeVisitorDispatcher<bool>(expr);
            break;
        }
        case DataType::INT8: {
            res = ExecBinaryRangeVisitorDispatcher<int8_t>(expr);
            break;
        }
        case DataType::INT16: {
            res = ExecBinaryRangeVisitorDispatcher<int16_t>(expr);
            break;
        }
        case DataType::INT32: {
            res = ExecBinaryRangeVisitorDispatcher<int32_t>(expr);
            break;
        }
        case DataType::INT64: {
            res = ExecBinaryRangeVisitorDispatcher<int64_t>(expr);
            break;
        }
        case DataType::FLOAT: {
            res = ExecBinaryRangeVisitorDispatcher<float>(expr);
            break;
        }
        case DataType::DOUBLE: {
            res = ExecBinaryRangeVisitorDispatcher<double>(expr);
            break;
        }
        case DataType::VARCHAR: {
            if (segment_.type() == SegmentType::Growing) {
                res = ExecBinaryRangeVisitorDispatcher<std::string>(expr);
            } else {
                res = ExecBinaryRangeVisitorDispatcher<std::string_view>(expr);
            }
            break;
        }
        case DataType::JSON: {
            switch (expr.val_case_) {
                case proto::plan::GenericValue::ValCase::kBoolVal: {
                    res = ExecBinaryRangeVisitorDispatcherJson<bool>(expr);
                    break;
                }
                case proto::plan::GenericValue::ValCase::kInt64Val: {
                    res = ExecBinaryRangeVisitorDispatcherJson<int64_t>(expr);
                    break;
                }
                case proto::plan::GenericValue::ValCase::kFloatVal: {
                    res = ExecBinaryRangeVisitorDispatcherJson<double>(expr);
                    break;
                }
                case proto::plan::GenericValue::ValCase::kStringVal: {
                    res =
                        ExecBinaryRangeVisitorDispatcherJson<std::string>(expr);
                    break;
                }
                default: {
                    PanicInfo(
                        fmt::format("unsupported value type {} in expression",
                                    expr.val_case_));
                }
            }
            break;
        }
        default:
            PanicInfo(fmt::format("unsupported data type: {}",
                                  expr.column_.data_type));
    }
    AssertInfo(res.size() == row_count_,
               "[ExecExprVisitor]Size of results not equal row count");
    bitset_opt_ = std::move(res);
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

template <typename T, typename U, typename CmpFunc>
TargetBitmap
ExecExprVisitor::ExecCompareRightType(const T* left_raw_data,
                                      const FieldId& right_field_id,
                                      const int64_t current_chunk_id,
                                      CmpFunc cmp_func) {
    auto size_per_chunk = segment_.size_per_chunk();
    auto num_chunks = upper_div(row_count_, size_per_chunk);
    auto size = current_chunk_id == num_chunks - 1
                    ? row_count_ - current_chunk_id * size_per_chunk
                    : size_per_chunk;

    TargetBitmap result(size);
    const U* right_raw_data =
        segment_.chunk_data<U>(right_field_id, current_chunk_id).data();

    for (int i = 0; i < size; ++i) {
        result[i] = cmp_func(left_raw_data[i], right_raw_data[i]);
    }

    return result;
}

template <typename T, typename CmpFunc>
BitsetType
ExecExprVisitor::ExecCompareLeftType(const FieldId& left_field_id,
                                     const FieldId& right_field_id,
                                     const DataType& right_field_type,
                                     CmpFunc cmp_func) {
    auto size_per_chunk = segment_.size_per_chunk();
    auto num_chunks = upper_div(row_count_, size_per_chunk);
    std::vector<FixedVector<bool>> results;
    results.reserve(num_chunks);

    for (int64_t chunk_id = 0; chunk_id < num_chunks; ++chunk_id) {
        FixedVector<bool> result;
        const T* left_raw_data =
            segment_.chunk_data<T>(left_field_id, chunk_id).data();

        switch (right_field_type) {
            case DataType::BOOL:
                result = ExecCompareRightType<T, bool, CmpFunc>(
                    left_raw_data, right_field_id, chunk_id, cmp_func);
                break;
            case DataType::INT8:
                result = ExecCompareRightType<T, int8_t, CmpFunc>(
                    left_raw_data, right_field_id, chunk_id, cmp_func);
                break;
            case DataType::INT16:
                result = ExecCompareRightType<T, int16_t, CmpFunc>(
                    left_raw_data, right_field_id, chunk_id, cmp_func);
                break;
            case DataType::INT32:
                result = ExecCompareRightType<T, int32_t, CmpFunc>(
                    left_raw_data, right_field_id, chunk_id, cmp_func);
                break;
            case DataType::INT64:
                result = ExecCompareRightType<T, int64_t, CmpFunc>(
                    left_raw_data, right_field_id, chunk_id, cmp_func);
                break;
            case DataType::FLOAT:
                result = ExecCompareRightType<T, float, CmpFunc>(
                    left_raw_data, right_field_id, chunk_id, cmp_func);
                break;
            case DataType::DOUBLE:
                result = ExecCompareRightType<T, double, CmpFunc>(
                    left_raw_data, right_field_id, chunk_id, cmp_func);
                break;
            default:
                PanicInfo("unsupported left datatype of compare expr");
        }
        results.push_back(result);
    }
    auto final_result = AssembleChunk(results);
    AssertInfo(final_result.size() == row_count_,
               "[ExecExprVisitor]Size of results not equal row count");
    return final_result;
}

template <typename CmpFunc>
BitsetType
ExecExprVisitor::ExecCompareExprDispatcherForNonIndexedSegment(
    CompareExpr& expr, CmpFunc cmp_func) {
    switch (expr.left_data_type_) {
        case DataType::BOOL:
            return ExecCompareLeftType<bool, CmpFunc>(expr.left_field_id_,
                                                      expr.right_field_id_,
                                                      expr.right_data_type_,
                                                      cmp_func);
        case DataType::INT8:
            return ExecCompareLeftType<int8_t, CmpFunc>(expr.left_field_id_,
                                                        expr.right_field_id_,
                                                        expr.right_data_type_,
                                                        cmp_func);
        case DataType::INT16:
            return ExecCompareLeftType<int16_t, CmpFunc>(expr.left_field_id_,
                                                         expr.right_field_id_,
                                                         expr.right_data_type_,
                                                         cmp_func);
        case DataType::INT32:
            return ExecCompareLeftType<int32_t, CmpFunc>(expr.left_field_id_,
                                                         expr.right_field_id_,
                                                         expr.right_data_type_,
                                                         cmp_func);
        case DataType::INT64:
            return ExecCompareLeftType<int64_t, CmpFunc>(expr.left_field_id_,
                                                         expr.right_field_id_,
                                                         expr.right_data_type_,
                                                         cmp_func);
        case DataType::FLOAT:
            return ExecCompareLeftType<float, CmpFunc>(expr.left_field_id_,
                                                       expr.right_field_id_,
                                                       expr.right_data_type_,
                                                       cmp_func);
        case DataType::DOUBLE:
            return ExecCompareLeftType<double, CmpFunc>(expr.left_field_id_,
                                                        expr.right_field_id_,
                                                        expr.right_data_type_,
                                                        cmp_func);
        default:
            PanicInfo("unsupported right datatype of compare expr");
    }
}

template <typename Op>
auto
ExecExprVisitor::ExecCompareExprDispatcher(CompareExpr& expr, Op op)
    -> BitsetType {
    using number = boost::variant<bool,
                                  int8_t,
                                  int16_t,
                                  int32_t,
                                  int64_t,
                                  float,
                                  double,
                                  std::string>;
    auto is_string_expr = [&expr]() -> bool {
        return expr.left_data_type_ == DataType::VARCHAR ||
               expr.right_data_type_ == DataType::VARCHAR;
    };

    auto size_per_chunk = segment_.size_per_chunk();
    auto num_chunk = upper_div(row_count_, size_per_chunk);
    std::deque<BitsetType> bitsets;

    // check for sealed segment, load either raw field data or index
    auto left_indexing_barrier = segment_.num_chunk_index(expr.left_field_id_);
    auto left_data_barrier = segment_.num_chunk_data(expr.left_field_id_);
    AssertInfo(std::max(left_data_barrier, left_indexing_barrier) == num_chunk,
               "max(left_data_barrier, left_indexing_barrier) not equal to "
               "num_chunk");

    auto right_indexing_barrier =
        segment_.num_chunk_index(expr.right_field_id_);
    auto right_data_barrier = segment_.num_chunk_data(expr.right_field_id_);
    AssertInfo(
        std::max(right_data_barrier, right_indexing_barrier) == num_chunk,
        "max(right_data_barrier, right_indexing_barrier) not equal to "
        "num_chunk");

    // For segment both fields has no index, can use SIMD to speed up.
    // Avoiding too much call stack that blocks SIMD.
    if (left_indexing_barrier == 0 && right_indexing_barrier == 0 &&
        !is_string_expr()) {
        return ExecCompareExprDispatcherForNonIndexedSegment<Op>(expr, op);
    }

    // TODO: refactoring the code that contains too much call stack.
    for (int64_t chunk_id = 0; chunk_id < num_chunk; ++chunk_id) {
        auto size = chunk_id == num_chunk - 1
                        ? row_count_ - chunk_id * size_per_chunk
                        : size_per_chunk;
        auto getChunkData =
            [&, chunk_id](DataType type, FieldId field_id, int64_t data_barrier)
            -> std::function<const number(int)> {
            switch (type) {
                case DataType::BOOL: {
                    if (chunk_id < data_barrier) {
                        auto chunk_data =
                            segment_.chunk_data<bool>(field_id, chunk_id)
                                .data();
                        return [chunk_data](int i) -> const number {
                            return chunk_data[i];
                        };
                    } else {
                        // for case, sealed segment has loaded index for scalar field instead of raw data
                        auto& indexing = segment_.chunk_scalar_index<bool>(
                            field_id, chunk_id);
                        return [&indexing](int i) -> const number {
                            return indexing.Reverse_Lookup(i);
                        };
                    }
                }
                case DataType::INT8: {
                    if (chunk_id < data_barrier) {
                        auto chunk_data =
                            segment_.chunk_data<int8_t>(field_id, chunk_id)
                                .data();
                        return [chunk_data](int i) -> const number {
                            return chunk_data[i];
                        };
                    } else {
                        // for case, sealed segment has loaded index for scalar field instead of raw data
                        auto& indexing = segment_.chunk_scalar_index<int8_t>(
                            field_id, chunk_id);
                        return [&indexing](int i) -> const number {
                            return indexing.Reverse_Lookup(i);
                        };
                    }
                }
                case DataType::INT16: {
                    if (chunk_id < data_barrier) {
                        auto chunk_data =
                            segment_.chunk_data<int16_t>(field_id, chunk_id)
                                .data();
                        return [chunk_data](int i) -> const number {
                            return chunk_data[i];
                        };
                    } else {
                        // for case, sealed segment has loaded index for scalar field instead of raw data
                        auto& indexing = segment_.chunk_scalar_index<int16_t>(
                            field_id, chunk_id);
                        return [&indexing](int i) -> const number {
                            return indexing.Reverse_Lookup(i);
                        };
                    }
                }
                case DataType::INT32: {
                    if (chunk_id < data_barrier) {
                        auto chunk_data =
                            segment_.chunk_data<int32_t>(field_id, chunk_id)
                                .data();
                        return [chunk_data](int i) -> const number {
                            return chunk_data[i];
                        };
                    } else {
                        // for case, sealed segment has loaded index for scalar field instead of raw data
                        auto& indexing = segment_.chunk_scalar_index<int32_t>(
                            field_id, chunk_id);
                        return [&indexing](int i) -> const number {
                            return indexing.Reverse_Lookup(i);
                        };
                    }
                }
                case DataType::INT64: {
                    if (chunk_id < data_barrier) {
                        auto chunk_data =
                            segment_.chunk_data<int64_t>(field_id, chunk_id)
                                .data();
                        return [chunk_data](int i) -> const number {
                            return chunk_data[i];
                        };
                    } else {
                        // for case, sealed segment has loaded index for scalar field instead of raw data
                        auto& indexing = segment_.chunk_scalar_index<int64_t>(
                            field_id, chunk_id);
                        return [&indexing](int i) -> const number {
                            return indexing.Reverse_Lookup(i);
                        };
                    }
                }
                case DataType::FLOAT: {
                    if (chunk_id < data_barrier) {
                        auto chunk_data =
                            segment_.chunk_data<float>(field_id, chunk_id)
                                .data();
                        return [chunk_data](int i) -> const number {
                            return chunk_data[i];
                        };
                    } else {
                        // for case, sealed segment has loaded index for scalar field instead of raw data
                        auto& indexing = segment_.chunk_scalar_index<float>(
                            field_id, chunk_id);
                        return [&indexing](int i) -> const number {
                            return indexing.Reverse_Lookup(i);
                        };
                    }
                }
                case DataType::DOUBLE: {
                    if (chunk_id < data_barrier) {
                        auto chunk_data =
                            segment_.chunk_data<double>(field_id, chunk_id)
                                .data();
                        return [chunk_data](int i) -> const number {
                            return chunk_data[i];
                        };
                    } else {
                        // for case, sealed segment has loaded index for scalar field instead of raw data
                        auto& indexing = segment_.chunk_scalar_index<double>(
                            field_id, chunk_id);
                        return [&indexing](int i) -> const number {
                            return indexing.Reverse_Lookup(i);
                        };
                    }
                }
                case DataType::VARCHAR: {
                    if (chunk_id < data_barrier) {
                        if (segment_.type() == SegmentType::Growing) {
                            auto chunk_data =
                                segment_
                                    .chunk_data<std::string>(field_id, chunk_id)
                                    .data();
                            return [chunk_data](int i) -> const number {
                                return chunk_data[i];
                            };
                        } else {
                            auto chunk_data = segment_
                                                  .chunk_data<std::string_view>(
                                                      field_id, chunk_id)
                                                  .data();
                            return [chunk_data](int i) -> const number {
                                return std::string(chunk_data[i]);
                            };
                        }
                    } else {
                        // for case, sealed segment has loaded index for scalar field instead of raw data
                        auto& indexing =
                            segment_.chunk_scalar_index<std::string>(field_id,
                                                                     chunk_id);
                        return [&indexing](int i) -> const number {
                            return indexing.Reverse_Lookup(i);
                        };
                    }
                }
                default:
                    PanicInfo(fmt::format("unsupported data type: {}", type));
            }
        };
        auto left = getChunkData(
            expr.left_data_type_, expr.left_field_id_, left_data_barrier);
        auto right = getChunkData(
            expr.right_data_type_, expr.right_field_id_, right_data_barrier);

        BitsetType bitset(size);
        for (int i = 0; i < size; ++i) {
            bool is_in = boost::apply_visitor(
                Relational<decltype(op)>{}, left(i), right(i));
            bitset[i] = is_in;
        }
        bitsets.emplace_back(std::move(bitset));
    }
    auto final_result = Assemble(bitsets);
    AssertInfo(final_result.size() == row_count_,
               "[ExecExprVisitor]Size of results not equal row count");
    return final_result;
}

void
ExecExprVisitor::visit(CompareExpr& expr) {
    auto& schema = segment_.get_schema();
    auto& left_field_meta = schema[expr.left_field_id_];
    auto& right_field_meta = schema[expr.right_field_id_];
    AssertInfo(expr.left_data_type_ == left_field_meta.get_data_type(),
               "[ExecExprVisitor]Left data type not equal to left field "
               "meta type");
    AssertInfo(expr.right_data_type_ == right_field_meta.get_data_type(),
               "[ExecExprVisitor]right data type not equal to right field "
               "meta type");

    BitsetType res;
    switch (expr.op_type_) {
        case OpType::Equal: {
            res = ExecCompareExprDispatcher(expr, std::equal_to<>{});
            break;
        }
        case OpType::NotEqual: {
            res = ExecCompareExprDispatcher(expr, std::not_equal_to<>{});
            break;
        }
        case OpType::GreaterEqual: {
            res = ExecCompareExprDispatcher(expr, std::greater_equal<>{});
            break;
        }
        case OpType::GreaterThan: {
            res = ExecCompareExprDispatcher(expr, std::greater<>{});
            break;
        }
        case OpType::LessEqual: {
            res = ExecCompareExprDispatcher(expr, std::less_equal<>{});
            break;
        }
        case OpType::LessThan: {
            res = ExecCompareExprDispatcher(expr, std::less<>{});
            break;
        }
        case OpType::PrefixMatch: {
            res =
                ExecCompareExprDispatcher(expr, MatchOp<OpType::PrefixMatch>{});
            break;
        }
            // case OpType::PostfixMatch: {
            // }
        default: {
            PanicInfo("unsupported optype");
        }
    }
    AssertInfo(res.size() == row_count_,
               "[ExecExprVisitor]Size of results not equal row count");
    bitset_opt_ = std::move(res);
}

template <typename T>
auto
ExecExprVisitor::ExecTermVisitorImpl(TermExpr& expr_raw) -> BitsetType {
    auto& expr = static_cast<TermExprImpl<T>&>(expr_raw);
    auto& schema = segment_.get_schema();
    auto primary_filed_id = schema.get_primary_field_id();
    auto field_id = expr_raw.column_.field_id;
    auto& field_meta = schema[field_id];

    bool use_pk_index = false;
    if (primary_filed_id.has_value()) {
        use_pk_index = primary_filed_id.value() == field_id &&
                       IsPrimaryKeyDataType(field_meta.get_data_type());
    }

    if (use_pk_index) {
        auto id_array = std::make_unique<IdArray>();
        switch (field_meta.get_data_type()) {
            case DataType::INT64: {
                auto dst_ids = id_array->mutable_int_id();
                for (const auto& id : expr.terms_) {
                    dst_ids->add_data((int64_t&)id);
                }
                break;
            }
            case DataType::VARCHAR: {
                auto dst_ids = id_array->mutable_str_id();
                for (const auto& id : expr.terms_) {
                    dst_ids->add_data((std::string&)id);
                }
                break;
            }
            default: {
                PanicInfo("unsupported type");
            }
        }

        auto [uids, seg_offsets] = segment_.search_ids(*id_array, timestamp_);
        BitsetType bitset(row_count_);
        std::vector<int64_t> cached_offsets;
        for (const auto& offset : seg_offsets) {
            auto _offset = (int64_t)offset.get();
            bitset[_offset] = true;
            cached_offsets.push_back(_offset);
        }
        // If enable plan_visitor pk index cache, pass offsets to it
        if (plan_visitor_ != nullptr) {
            plan_visitor_->SetExprUsePkIndex(true);
            plan_visitor_->SetExprCacheOffsets(std::move(cached_offsets));
        }
        AssertInfo(bitset.size() == row_count_,
                   "[ExecExprVisitor]Size of results not equal row count");
        return bitset;
    }

    return ExecTermVisitorImplTemplate<T>(expr_raw);
}

template <>
auto
ExecExprVisitor::ExecTermVisitorImpl<std::string>(TermExpr& expr_raw)
    -> BitsetType {
    return ExecTermVisitorImplTemplate<std::string>(expr_raw);
}

template <>
auto
ExecExprVisitor::ExecTermVisitorImpl<std::string_view>(TermExpr& expr_raw)
    -> BitsetType {
    return ExecTermVisitorImplTemplate<std::string_view>(expr_raw);
}

template <typename T>
auto
ExecExprVisitor::ExecTermVisitorImplTemplate(TermExpr& expr_raw) -> BitsetType {
    typedef std::
        conditional_t<std::is_same_v<T, std::string_view>, std::string, T>
            IndexInnerType;
    using Index = index::ScalarIndex<IndexInnerType>;
    auto& expr = static_cast<TermExprImpl<IndexInnerType>&>(expr_raw);
    const std::vector<IndexInnerType> terms(expr.terms_.begin(),
                                            expr.terms_.end());
    auto n = terms.size();
    std::unordered_set<T> term_set(expr.terms_.begin(), expr.terms_.end());

    auto index_func = [&terms, n](Index* index) {
        return index->In(n, terms.data());
    };
    auto elem_func = [&terms, &term_set](MayConstRef<T> x) {
        //// terms has already been sorted.
        // return std::binary_search(terms.begin(), terms.end(), x);
        return term_set.find(x) != term_set.end();
    };

    return ExecRangeVisitorImpl<T>(
        expr.column_.field_id, index_func, elem_func);
}

// TODO: bool is so ugly here.
template <>
auto
ExecExprVisitor::ExecTermVisitorImplTemplate<bool>(TermExpr& expr_raw)
    -> BitsetType {
    using T = bool;
    auto& expr = static_cast<TermExprImpl<T>&>(expr_raw);
    using Index = index::ScalarIndex<T>;
    const auto& terms = expr.terms_;
    auto n = terms.size();
    std::unordered_set<T> term_set(expr.terms_.begin(), expr.terms_.end());

    auto index_func = [&terms, n](Index* index) {
        auto bool_arr_copy = new bool[terms.size()];
        int it = 0;
        for (auto elem : terms) {
            bool_arr_copy[it++] = elem;
        }
        auto bitset = index->In(n, bool_arr_copy);
        delete[] bool_arr_copy;
        return bitset;
    };

    auto elem_func = [&terms, &term_set](MayConstRef<T> x) {
        //// terms has already been sorted.
        // return std::binary_search(terms.begin(), terms.end(), x);
        return term_set.find(x) != term_set.end();
    };

    return ExecRangeVisitorImpl<T>(
        expr.column_.field_id, index_func, elem_func);
}

template <typename ExprValueType>
auto
ExecExprVisitor::ExecTermJsonFieldInVariable(TermExpr& expr_raw) -> BitsetType {
    using Index = index::ScalarIndex<milvus::Json>;
    auto& expr = static_cast<TermExprImpl<ExprValueType>&>(expr_raw);
    auto pointer = milvus::Json::pointer(expr.column_.nested_path);
    auto index_func = [](Index* index) { return TargetBitmap{}; };

    std::unordered_set<ExprValueType> term_set(expr.terms_.begin(),
                                               expr.terms_.end());

    if (term_set.empty()) {
        auto elem_func = [=](const milvus::Json& json) { return false; };
        return ExecRangeVisitorImpl<milvus::Json>(
            expr.column_.field_id, index_func, elem_func);
    }

    auto elem_func = [&term_set, &pointer](const milvus::Json& json) {
        using GetType =
            std::conditional_t<std::is_same_v<ExprValueType, std::string>,
                               std::string_view,
                               ExprValueType>;
        auto x = json.template at<GetType>(pointer);
        if (x.error()) {
            if constexpr (std::is_same_v<GetType, std::int64_t>) {
                auto x = json.template at<double>(pointer);
                if (x.error()) {
                    return false;
                }

                auto value = x.value();
                // if the term set is {1}, and the value is 1.1, we should not return true.
                return std::floor(value) == value &&
                       term_set.find(ExprValueType(value)) != term_set.end();
            }
            return false;
        }
        return term_set.find(ExprValueType(x.value())) != term_set.end();
    };

    return ExecRangeVisitorImpl<milvus::Json>(
        expr.column_.field_id, index_func, elem_func);
}

template <typename ExprValueType>
auto
ExecExprVisitor::ExecTermJsonVariableInField(TermExpr& expr_raw) -> BitsetType {
    using Index = index::ScalarIndex<milvus::Json>;
    auto& expr = static_cast<TermExprImpl<ExprValueType>&>(expr_raw);
    auto pointer = milvus::Json::pointer(expr.column_.nested_path);
    auto index_func = [](Index* index) { return TargetBitmap{}; };

    AssertInfo(expr.terms_.size() == 1,
               "element length in json array must be one");
    ExprValueType target_val = expr.terms_[0];

    auto elem_func = [&target_val, &pointer](const milvus::Json& json) {
        using GetType =
            std::conditional_t<std::is_same_v<ExprValueType, std::string>,
                               std::string_view,
                               ExprValueType>;
        auto doc = json.doc();
        auto array = doc.at_pointer(pointer).get_array();
        if (array.error())
            return false;
        for (auto it = array.begin(); it != array.end(); ++it) {
            auto val = (*it).template get<GetType>();
            if (val.error()) {
                return false;
            }
            if (val.value() == target_val)
                return true;
        }
        return false;
    };

    return ExecRangeVisitorImpl<milvus::Json>(
        expr.column_.field_id, index_func, elem_func);
}

template <typename ExprValueType>
auto
ExecExprVisitor::ExecTermVisitorImplTemplateJson(TermExpr& expr_raw)
    -> BitsetType {
    if (expr_raw.is_in_field_) {
        return ExecTermJsonVariableInField<ExprValueType>(expr_raw);
    } else {
        return ExecTermJsonFieldInVariable<ExprValueType>(expr_raw);
    }
}

void
ExecExprVisitor::visit(TermExpr& expr) {
    auto& field_meta = segment_.get_schema()[expr.column_.field_id];
    AssertInfo(expr.column_.data_type == field_meta.get_data_type(),
               "[ExecExprVisitor]DataType of expr isn't field_meta "
               "data type ");
    BitsetType res;
    switch (expr.column_.data_type) {
        case DataType::BOOL: {
            res = ExecTermVisitorImpl<bool>(expr);
            break;
        }
        case DataType::INT8: {
            res = ExecTermVisitorImpl<int8_t>(expr);
            break;
        }
        case DataType::INT16: {
            res = ExecTermVisitorImpl<int16_t>(expr);
            break;
        }
        case DataType::INT32: {
            res = ExecTermVisitorImpl<int32_t>(expr);
            break;
        }
        case DataType::INT64: {
            res = ExecTermVisitorImpl<int64_t>(expr);
            break;
        }
        case DataType::FLOAT: {
            res = ExecTermVisitorImpl<float>(expr);
            break;
        }
        case DataType::DOUBLE: {
            res = ExecTermVisitorImpl<double>(expr);
            break;
        }
        case DataType::VARCHAR: {
            if (segment_.type() == SegmentType::Growing) {
                res = ExecTermVisitorImpl<std::string>(expr);
            } else {
                res = ExecTermVisitorImpl<std::string_view>(expr);
            }
            break;
        }
        case DataType::JSON: {
            switch (expr.val_case_) {
                case proto::plan::GenericValue::ValCase::kBoolVal:
                    res = ExecTermVisitorImplTemplateJson<bool>(expr);
                    break;
                case proto::plan::GenericValue::ValCase::kInt64Val:
                    res = ExecTermVisitorImplTemplateJson<int64_t>(expr);
                    break;
                case proto::plan::GenericValue::ValCase::kFloatVal:
                    res = ExecTermVisitorImplTemplateJson<double>(expr);
                    break;
                case proto::plan::GenericValue::ValCase::kStringVal:
                    res = ExecTermVisitorImplTemplateJson<std::string>(expr);
                    break;
                case proto::plan::GenericValue::ValCase::VAL_NOT_SET:
                    res = ExecTermVisitorImplTemplateJson<bool>(expr);
                    break;
                default:
                    PanicInfo(
                        fmt::format("unknown data type: {}", expr.val_case_));
            }
            break;
        }
        default:
            PanicInfo(fmt::format("unsupported data type: {}",
                                  expr.column_.data_type));
    }
    AssertInfo(res.size() == row_count_,
               "[ExecExprVisitor]Size of results not equal row count");
    bitset_opt_ = std::move(res);
}

void
ExecExprVisitor::visit(ExistsExpr& expr) {
    auto& field_meta = segment_.get_schema()[expr.column_.field_id];
    AssertInfo(expr.column_.data_type == field_meta.get_data_type(),
               "[ExecExprVisitor]DataType of expr isn't field_meta data type");
    BitsetType res;
    auto pointer = milvus::Json::pointer(expr.column_.nested_path);
    switch (expr.column_.data_type) {
        case DataType::JSON: {
            using Index = index::ScalarIndex<milvus::Json>;
            auto index_func = [&](Index* index) { return TargetBitmap{}; };
            auto elem_func = [&](const milvus::Json& json) {
                auto x = json.exist(pointer);
                return x;
            };
            res = ExecRangeVisitorImpl<milvus::Json>(
                expr.column_.field_id, index_func, elem_func);
            break;
        }
        default:
            PanicInfo(fmt::format("unsupported data type {}",
                                  expr.column_.data_type));
    }
    AssertInfo(res.size() == row_count_,
               "[ExecExprVisitor]Size of results not equal row count");
    bitset_opt_ = std::move(res);
}

}  // namespace milvus::query
