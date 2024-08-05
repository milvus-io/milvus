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
#include "common/EasyAssert.h"
#include "fmt/core.h"
#include "pb/plan.pb.h"
#include "query/ExprImpl.h"
#include "query/Relational.h"
#include "query/Utils.h"
#include "segcore/SegmentGrowingImpl.h"
#include "simdjson/error.h"
#include "query/PlanProto.h"
#include "index/SkipIndex.h"
#include "index/Meta.h"

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
    template <typename T,
              typename IndexFunc,
              typename ElementFunc,
              typename SkipIndexFunc>
    auto
    ExecRangeVisitorImpl(FieldId field_id,
                         IndexFunc func,
                         ElementFunc element_func,
                         SkipIndexFunc skip_index_func) -> BitsetType;

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
            PanicInfo(OpTypeInvalid, "Invalid Unary Op {}", expr.op_type_);
        }
    }
    AssertInfo(res.size() == row_count_,
               "[ExecExprVisitor]Size of results not equal row count");
    bitset_opt_ = std::move(res);
}

void
ExecExprVisitor::visit(LogicalBinaryExpr& expr) {
    using OpType = LogicalBinaryExpr::OpType;
    auto skip_right_expr = [](const BitsetType& left_res,
                              const OpType& op_type) -> bool {
        return (op_type == OpType::LogicalAnd && left_res.none()) ||
               (op_type == OpType::LogicalOr && left_res.all());
    };

    auto left = call_child(*expr.left_);
    // skip execute right node for some situations
    if (skip_right_expr(left, expr.op_type_)) {
        AssertInfo(left.size() == row_count_,
                   "[ExecExprVisitor]Size of results not equal row count");
        bitset_opt_ = std::move(left);
        return;
    }
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
            PanicInfo(OpTypeInvalid, "Invalid Binary Op {}", expr.op_type_);
        }
    }
    AssertInfo(res.size() == row_count_,
               "[ExecExprVisitor]Size of results not equal row count");
    bitset_opt_ = std::move(res);
}

static auto
Assemble(const std::deque<BitsetType>& srcs) -> BitsetType {
    BitsetType res;

    int64_t total_size = 0;
    for (auto& chunk : srcs) {
        total_size += chunk.size();
    }
    res.reserve(total_size);

    for (auto& chunk : srcs) {
        res.append(chunk);
    }
    return res;
}

void
AppendOneChunk(BitsetType& result, const TargetBitmapView chunk_res) {
    result.append(chunk_res);
}

BitsetType
AssembleChunk(const std::vector<TargetBitmap>& results) {
    BitsetType assemble_result;
    for (auto& result : results) {
        AppendOneChunk(assemble_result, result.view());
    }
    return assemble_result;
}

template <typename T,
          typename IndexFunc,
          typename ElementFunc,
          typename SkipIndexFunc>
auto
ExecExprVisitor::ExecRangeVisitorImpl(FieldId field_id,
                                      IndexFunc index_func,
                                      ElementFunc element_func,
                                      SkipIndexFunc skip_index_func)
    -> BitsetType {
    auto& schema = segment_.get_schema();
    auto& field_meta = schema[field_id];
    auto indexing_barrier = segment_.num_chunk_index(field_id);
    auto size_per_chunk = segment_.size_per_chunk();
    auto num_chunk = upper_div(row_count_, size_per_chunk);
    std::vector<TargetBitmap> results;
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
        TargetBitmap chunk_res(this_size);
        //check possible chunk metrics
        auto& skipIndex = segment_.GetSkipIndex();
        if (skip_index_func(skipIndex, field_id, chunk_id)) {
            results.emplace_back(std::move(chunk_res));
            continue;
        }
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
    std::vector<TargetBitmap> results;
    results.reserve(num_chunk);

    // for growing segment, indexing_barrier will always less than data_barrier
    // so growing segment will always execute expr plan using raw data
    // if sealed segment has loaded raw data on this field, then index_barrier = 0 and data_barrier = 1
    // in this case, sealed segment execute expr plan using raw data
    for (auto chunk_id = 0; chunk_id < data_barrier; ++chunk_id) {
        auto this_size = chunk_id == num_chunk - 1
                             ? row_count_ - chunk_id * size_per_chunk
                             : size_per_chunk;
        TargetBitmap result(this_size);
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
        TargetBitmap result(this_size);
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
    auto default_skip_index_func = [&](const SkipIndex& skipIndex,
                                       FieldId fieldId,
                                       int64_t chunkId) { return false; };
    switch (op) {
        case OpType::Equal: {
            auto index_func = [&](Index* index) { return index->In(1, &val); };
            auto elem_func = [&](MayConstRef<T> x) { return (x == val); };
            auto skip_index_func = [&](const SkipIndex& skipIndex,
                                       FieldId fieldId,
                                       int64_t chunkId) {
                return skipIndex.CanSkipUnaryRange<T>(
                    fieldId, chunkId, OpType::Equal, val);
            };
            return ExecRangeVisitorImpl<T>(
                field_id, index_func, elem_func, skip_index_func);
        }
        case OpType::NotEqual: {
            auto index_func = [&](Index* index) {
                return index->NotIn(1, &val);
            };
            auto elem_func = [&](MayConstRef<T> x) { return (x != val); };
            return ExecRangeVisitorImpl<T>(
                field_id, index_func, elem_func, default_skip_index_func);
        }
        case OpType::GreaterEqual: {
            auto index_func = [&](Index* index) {
                return index->Range(val, OpType::GreaterEqual);
            };
            auto elem_func = [&](MayConstRef<T> x) { return (x >= val); };
            auto skip_index_func = [&](const SkipIndex& skipIndex,
                                       FieldId fieldId,
                                       int64_t chunkId) {
                return skipIndex.CanSkipUnaryRange<T>(
                    fieldId, chunkId, OpType::GreaterEqual, val);
            };

            return ExecRangeVisitorImpl<T>(
                field_id, index_func, elem_func, skip_index_func);
        }
        case OpType::GreaterThan: {
            auto index_func = [&](Index* index) {
                return index->Range(val, OpType::GreaterThan);
            };
            auto elem_func = [&](MayConstRef<T> x) { return (x > val); };
            auto skip_index_func = [&](const SkipIndex& skipIndex,
                                       FieldId fieldId,
                                       int64_t chunkId) {
                return skipIndex.CanSkipUnaryRange<T>(
                    fieldId, chunkId, OpType::GreaterThan, val);
            };
            return ExecRangeVisitorImpl<T>(
                field_id, index_func, elem_func, skip_index_func);
        }
        case OpType::LessEqual: {
            auto index_func = [&](Index* index) {
                return index->Range(val, OpType::LessEqual);
            };
            auto elem_func = [&](MayConstRef<T> x) { return (x <= val); };
            auto skip_index_func = [&](const SkipIndex& skipIndex,
                                       FieldId fieldId,
                                       int64_t chunkId) {
                return skipIndex.CanSkipUnaryRange<T>(
                    fieldId, chunkId, OpType::LessEqual, val);
            };
            return ExecRangeVisitorImpl<T>(
                field_id, index_func, elem_func, skip_index_func);
        }
        case OpType::LessThan: {
            auto index_func = [&](Index* index) {
                return index->Range(val, OpType::LessThan);
            };
            auto elem_func = [&](MayConstRef<T> x) { return (x < val); };
            auto skip_index_func = [&](const SkipIndex& skipIndex,
                                       FieldId fieldId,
                                       int64_t chunkId) {
                return skipIndex.CanSkipUnaryRange<T>(
                    fieldId, chunkId, OpType::LessThan, val);
            };
            return ExecRangeVisitorImpl<T>(
                field_id, index_func, elem_func, skip_index_func);
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
            return ExecRangeVisitorImpl<T>(
                field_id, index_func, elem_func, default_skip_index_func);
        }
        // TODO: PostfixMatch
        default: {
            PanicInfo(OpTypeInvalid, "unsupported range node {}", op);
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
                PanicInfo(
                    OpTypeInvalid,
                    fmt::format("unsupported range node {}", expr.op_type_));
            }
        }
    }
    return ExecUnaryRangeVisitorDispatcherImpl<T>(expr_raw);
}

template <typename T>
bool
CompareTwoJsonArray(T arr1, const proto::plan::Array& arr2) {
    int json_array_length = 0;
    if constexpr (std::is_same_v<
                      T,
                      simdjson::simdjson_result<simdjson::ondemand::array>>) {
        json_array_length = arr1.count_elements();
    }
    if constexpr (std::is_same_v<T,
                                 std::vector<simdjson::simdjson_result<
                                     simdjson::ondemand::value>>>) {
        json_array_length = arr1.size();
    }
    if (arr2.array_size() != json_array_length) {
        return false;
    }
    int i = 0;
    for (auto&& it : arr1) {
        switch (arr2.array(i).val_case()) {
            case proto::plan::GenericValue::kBoolVal: {
                auto val = it.template get<bool>();
                if (val.error() || val.value() != arr2.array(i).bool_val()) {
                    return false;
                }
                break;
            }
            case proto::plan::GenericValue::kInt64Val: {
                auto val = it.template get<int64_t>();
                if (val.error() || val.value() != arr2.array(i).int64_val()) {
                    return false;
                }
                break;
            }
            case proto::plan::GenericValue::kFloatVal: {
                auto val = it.template get<double>();
                if (val.error() || val.value() != arr2.array(i).float_val()) {
                    return false;
                }
                break;
            }
            case proto::plan::GenericValue::kStringVal: {
                auto val = it.template get<std::string_view>();
                if (val.error() || val.value() != arr2.array(i).string_val()) {
                    return false;
                }
                break;
            }
            default:
                PanicInfo(DataTypeInvalid,
                          "unsupported data type {}",
                          arr2.array(i).val_case());
        }
        i++;
    }
    return true;
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
    auto default_skip_index_func = [&](const SkipIndex& skipIndex,
                                       FieldId fieldId,
                                       int64_t chunkId) { return false; };
    switch (op) {
        case OpType::Equal: {
            auto elem_func = [&](const milvus::Json& json) {
                if constexpr (std::is_same_v<GetType, proto::plan::Array>) {
                    auto doc = json.doc();
                    auto array = doc.at_pointer(pointer).get_array();
                    if (array.error()) {
                        return false;
                    }
                    return CompareTwoJsonArray(array, val);
                } else {
                    UnaryRangeJSONCompare(x.value() == val);
                }
            };
            return ExecRangeVisitorImpl<milvus::Json>(
                field_id, index_func, elem_func, default_skip_index_func);
        }
        case OpType::NotEqual: {
            auto elem_func = [&](const milvus::Json& json) {
                if constexpr (std::is_same_v<GetType, proto::plan::Array>) {
                    auto doc = json.doc();
                    auto array = doc.at_pointer(pointer).get_array();
                    if (array.error()) {
                        return false;
                    }
                    return !CompareTwoJsonArray(array, val);
                } else {
                    UnaryRangeJSONCompareNotEqual(x.value() != val);
                }
            };
            return ExecRangeVisitorImpl<milvus::Json>(
                field_id, index_func, elem_func, default_skip_index_func);
        }
        case OpType::GreaterEqual: {
            auto elem_func = [&](const milvus::Json& json) {
                if constexpr (std::is_same_v<GetType, proto::plan::Array>) {
                    return false;
                } else {
                    UnaryRangeJSONCompare(x.value() >= val);
                }
            };
            return ExecRangeVisitorImpl<milvus::Json>(
                field_id, index_func, elem_func, default_skip_index_func);
        }
        case OpType::GreaterThan: {
            auto elem_func = [&](const milvus::Json& json) {
                if constexpr (std::is_same_v<GetType, proto::plan::Array>) {
                    return false;
                } else {
                    UnaryRangeJSONCompare(x.value() > val);
                }
            };
            return ExecRangeVisitorImpl<milvus::Json>(
                field_id, index_func, elem_func, default_skip_index_func);
        }
        case OpType::LessEqual: {
            auto elem_func = [&](const milvus::Json& json) {
                if constexpr (std::is_same_v<GetType, proto::plan::Array>) {
                    return false;
                } else {
                    UnaryRangeJSONCompare(x.value() <= val);
                }
            };
            return ExecRangeVisitorImpl<milvus::Json>(
                field_id, index_func, elem_func, default_skip_index_func);
        }
        case OpType::LessThan: {
            auto elem_func = [&](const milvus::Json& json) {
                if constexpr (std::is_same_v<GetType, proto::plan::Array>) {
                    return false;
                } else {
                    UnaryRangeJSONCompare(x.value() < val);
                }
            };
            return ExecRangeVisitorImpl<milvus::Json>(
                field_id, index_func, elem_func, default_skip_index_func);
        }
        case OpType::PrefixMatch: {
            auto elem_func = [&](const milvus::Json& json) {
                if constexpr (std::is_same_v<GetType, proto::plan::Array>) {
                    return false;
                } else {
                    UnaryRangeJSONCompare(
                        Match(ExprValueType(x.value()), val, op));
                }
            };
            return ExecRangeVisitorImpl<milvus::Json>(
                field_id, index_func, elem_func, default_skip_index_func);
        }
        // TODO: PostfixMatch
        default: {
            PanicInfo(OpTypeInvalid, "unsupported range node {}", op);
        }
    }
}

template <typename ExprValueType>
auto
ExecExprVisitor::ExecUnaryRangeVisitorDispatcherArray(UnaryRangeExpr& expr_raw)
    -> BitsetType {
    using Index = index::ScalarIndex<milvus::ArrayView>;
    auto& expr = static_cast<UnaryRangeExprImpl<ExprValueType>&>(expr_raw);

    auto op = expr.op_type_;
    auto val = expr.value_;
    auto field_id = expr.column_.field_id;
    auto index_func = [=](Index* index) { return TargetBitmap{}; };
    int index = -1;
    if (expr.column_.nested_path.size() > 0) {
        index = std::stoi(expr.column_.nested_path[0]);
    }
    using GetType =
        std::conditional_t<std::is_same_v<ExprValueType, std::string>,
                           std::string_view,
                           ExprValueType>;
    auto default_skip_index_func = [&](const SkipIndex& skipIndex,
                                       FieldId fieldId,
                                       int64_t chunkId) { return false; };
    switch (op) {
        case OpType::Equal: {
            auto elem_func = [&](const milvus::ArrayView& array) {
                if constexpr (std::is_same_v<GetType, proto::plan::Array>) {
                    return array.is_same_array(val);
                } else {
                    if (index >= array.length()) {
                        return false;
                    }
                    auto array_data = array.template get_data<GetType>(index);
                    return array_data == val;
                }
            };
            return ExecRangeVisitorImpl<milvus::ArrayView>(
                field_id, index_func, elem_func, default_skip_index_func);
        }
        case OpType::NotEqual: {
            auto elem_func = [&](const milvus::ArrayView& array) {
                if constexpr (std::is_same_v<GetType, proto::plan::Array>) {
                    return !array.is_same_array(val);
                } else {
                    if (index >= array.length()) {
                        return false;
                    }
                    auto array_data = array.template get_data<GetType>(index);
                    return array_data != val;
                }
            };
            return ExecRangeVisitorImpl<milvus::ArrayView>(
                field_id, index_func, elem_func, default_skip_index_func);
        }
        case OpType::GreaterEqual: {
            auto elem_func = [&](const milvus::ArrayView& array) {
                if constexpr (std::is_same_v<GetType, proto::plan::Array>) {
                    return false;
                } else {
                    if (index >= array.length()) {
                        return false;
                    }
                    auto array_data = array.template get_data<GetType>(index);
                    return array_data >= val;
                }
            };
            return ExecRangeVisitorImpl<milvus::ArrayView>(
                field_id, index_func, elem_func, default_skip_index_func);
        }
        case OpType::GreaterThan: {
            auto elem_func = [&](const milvus::ArrayView& array) {
                if constexpr (std::is_same_v<GetType, proto::plan::Array>) {
                    return false;
                } else {
                    if (index >= array.length()) {
                        return false;
                    }
                    auto array_data = array.template get_data<GetType>(index);
                    return array_data > val;
                }
            };
            return ExecRangeVisitorImpl<milvus::ArrayView>(
                field_id, index_func, elem_func, default_skip_index_func);
        }
        case OpType::LessEqual: {
            auto elem_func = [&](const milvus::ArrayView& array) {
                if constexpr (std::is_same_v<GetType, proto::plan::Array>) {
                    return false;
                } else {
                    if (index >= array.length()) {
                        return false;
                    }
                    auto array_data = array.template get_data<GetType>(index);
                    return array_data <= val;
                }
            };
            return ExecRangeVisitorImpl<milvus::ArrayView>(
                field_id, index_func, elem_func, default_skip_index_func);
        }
        case OpType::LessThan: {
            auto elem_func = [&](const milvus::ArrayView& array) {
                if constexpr (std::is_same_v<GetType, proto::plan::Array>) {
                    return false;
                } else {
                    if (index >= array.length()) {
                        return false;
                    }
                    auto array_data = array.template get_data<GetType>(index);
                    return array_data < val;
                }
            };
            return ExecRangeVisitorImpl<milvus::ArrayView>(
                field_id, index_func, elem_func, default_skip_index_func);
        }
        case OpType::PrefixMatch: {
            auto elem_func = [&](const milvus::ArrayView& array) {
                if constexpr (std::is_same_v<GetType, proto::plan::Array>) {
                    return false;
                } else {
                    if (index >= array.length()) {
                        return false;
                    }
                    auto array_data = array.template get_data<GetType>(index);
                    return Match(array_data, val, op);
                }
            };
            return ExecRangeVisitorImpl<milvus::ArrayView>(
                field_id, index_func, elem_func, default_skip_index_func);
        }
        // TODO: PostfixMatch
        default: {
            PanicInfo(OpTypeInvalid, "unsupported range node {}", op);
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
                    PanicInfo(
                        OpTypeInvalid,
                        fmt::format("unsupported arithmetic operation {}", op));
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
                    PanicInfo(
                        OpTypeInvalid,
                        fmt::format("unsupported arithmetic operation {}", op));
                }
            }
        }
        default: {
            PanicInfo(
                OpTypeInvalid,
                fmt::format(
                    "unsupported range node with arithmetic operation {}", op));
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
                case ArithOpType::ArrayLength: {
                    auto index_func = [val, right_operand](Index* index,
                                                           size_t offset) {
                        return false;
                    };
                    auto elem_func = [&](const milvus::Json& json) {
                        int array_length = 0;
                        auto doc = json.doc();
                        auto array = doc.at_pointer(pointer).get_array();
                        if (!array.error()) {
                            array_length = array.count_elements();
                        }
                        return array_length == val;
                    };
                    return ExecDataRangeVisitorImpl<milvus::Json>(
                        expr.column_.field_id, index_func, elem_func);
                }
                default: {
                    PanicInfo(
                        OpTypeInvalid,
                        fmt::format("unsupported arithmetic operation {}", op));
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
                case ArithOpType::ArrayLength: {
                    auto index_func = [val, right_operand](Index* index,
                                                           size_t offset) {
                        return false;
                    };
                    auto elem_func = [&](const milvus::Json& json) {
                        int array_length = 0;
                        auto doc = json.doc();
                        auto array = doc.at_pointer(pointer).get_array();
                        if (!array.error()) {
                            array_length = array.count_elements();
                        }
                        return array_length != val;
                    };
                    return ExecDataRangeVisitorImpl<milvus::Json>(
                        expr.column_.field_id, index_func, elem_func);
                }
                default: {
                    PanicInfo(
                        OpTypeInvalid,
                        fmt::format("unsupported arithmetic operation {}", op));
                }
            }
        }
        default: {
            PanicInfo(
                OpTypeInvalid,
                fmt::format(
                    "unsupported range node with arithmetic operation {}", op));
        }
    }
}  // namespace milvus::query

template <typename ExprValueType>
auto
ExecExprVisitor::ExecBinaryArithOpEvalRangeVisitorDispatcherArray(
    BinaryArithOpEvalRangeExpr& expr_raw) -> BitsetType {
    auto& expr =
        static_cast<BinaryArithOpEvalRangeExprImpl<ExprValueType>&>(expr_raw);
    using Index = index::ScalarIndex<milvus::ArrayView>;

    auto arith_op = expr.arith_op_;
    auto right_operand = expr.right_operand_;
    auto op = expr.op_type_;
    auto val = expr.value_;
    int index = -1;
    if (expr.column_.nested_path.size() > 0) {
        index = std::stoi(expr.column_.nested_path[0]);
    }
    using GetType =
        std::conditional_t<std::is_same_v<ExprValueType, std::string>,
                           std::string_view,
                           ExprValueType>;

    switch (op) {
        case OpType::Equal: {
            switch (arith_op) {
                case ArithOpType::Add: {
                    auto index_func = [val, right_operand](Index* index,
                                                           size_t offset) {
                        return false;
                    };
                    auto elem_func = [&](const milvus::ArrayView& array) {
                        if (index >= array.length()) {
                            return false;
                        }
                        auto value = array.get_data<GetType>(index);
                        return value + right_operand == val;
                    };
                    return ExecDataRangeVisitorImpl<milvus::ArrayView>(
                        expr.column_.field_id, index_func, elem_func);
                }
                case ArithOpType::Sub: {
                    auto index_func = [val, right_operand](Index* index,
                                                           size_t offset) {
                        return false;
                    };
                    auto elem_func = [&](const milvus::ArrayView& array) {
                        if (index >= array.length()) {
                            return false;
                        }
                        auto value = array.get_data<GetType>(index);
                        return value - right_operand == val;
                    };
                    return ExecDataRangeVisitorImpl<milvus::ArrayView>(
                        expr.column_.field_id, index_func, elem_func);
                }
                case ArithOpType::Mul: {
                    auto index_func = [val, right_operand](Index* index,
                                                           size_t offset) {
                        return false;
                    };
                    auto elem_func = [&](const milvus::ArrayView& array) {
                        if (index >= array.length()) {
                            return false;
                        }
                        auto value = array.get_data<GetType>(index);
                        return value * right_operand == val;
                    };
                    return ExecDataRangeVisitorImpl<milvus::ArrayView>(
                        expr.column_.field_id, index_func, elem_func);
                }
                case ArithOpType::Div: {
                    auto index_func = [val, right_operand](Index* index,
                                                           size_t offset) {
                        return false;
                    };
                    auto elem_func = [&](const milvus::ArrayView& array) {
                        if (index >= array.length()) {
                            return false;
                        }
                        auto value = array.get_data<GetType>(index);
                        return value / right_operand == val;
                    };
                    return ExecDataRangeVisitorImpl<milvus::ArrayView>(
                        expr.column_.field_id, index_func, elem_func);
                }
                case ArithOpType::Mod: {
                    auto index_func = [val, right_operand](Index* index,
                                                           size_t offset) {
                        return false;
                    };
                    auto elem_func = [&](const milvus::ArrayView& array) {
                        if (index >= array.length()) {
                            return false;
                        }
                        auto value = array.get_data<GetType>(index);
                        return static_cast<ExprValueType>(
                                   fmod(value, right_operand)) == val;
                    };
                    return ExecDataRangeVisitorImpl<milvus::ArrayView>(
                        expr.column_.field_id, index_func, elem_func);
                }
                case ArithOpType::ArrayLength: {
                    auto index_func = [val, right_operand](Index* index,
                                                           size_t offset) {
                        return false;
                    };
                    auto elem_func = [&](const milvus::ArrayView& array) {
                        return array.length() == val;
                    };
                    return ExecDataRangeVisitorImpl<milvus::ArrayView>(
                        expr.column_.field_id, index_func, elem_func);
                }
                default: {
                    PanicInfo(
                        OpTypeInvalid,
                        fmt::format("unsupported arithmetic operation {}", op));
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
                    auto elem_func = [&](const milvus::ArrayView& array) {
                        if (index >= array.length()) {
                            return false;
                        }
                        auto value = array.get_data<GetType>(index);
                        return value + right_operand != val;
                    };
                    return ExecDataRangeVisitorImpl<milvus::ArrayView>(
                        expr.column_.field_id, index_func, elem_func);
                }
                case ArithOpType::Sub: {
                    auto index_func = [val, right_operand](Index* index,
                                                           size_t offset) {
                        return false;
                    };
                    auto elem_func = [&](const milvus::ArrayView& array) {
                        if (index >= array.length()) {
                            return false;
                        }
                        auto value = array.get_data<GetType>(index);
                        return value - right_operand != val;
                    };
                    return ExecDataRangeVisitorImpl<milvus::ArrayView>(
                        expr.column_.field_id, index_func, elem_func);
                }
                case ArithOpType::Mul: {
                    auto index_func = [val, right_operand](Index* index,
                                                           size_t offset) {
                        return false;
                    };
                    auto elem_func = [&](const milvus::ArrayView& array) {
                        if (index >= array.length()) {
                            return false;
                        }
                        auto value = array.get_data<GetType>(index);
                        return value * right_operand != val;
                    };
                    return ExecDataRangeVisitorImpl<milvus::ArrayView>(
                        expr.column_.field_id, index_func, elem_func);
                }
                case ArithOpType::Div: {
                    auto index_func = [val, right_operand](Index* index,
                                                           size_t offset) {
                        return false;
                    };
                    auto elem_func = [&](const milvus::ArrayView& array) {
                        if (index >= array.length()) {
                            return false;
                        }
                        auto value = array.get_data<GetType>(index);
                        return value / right_operand != val;
                    };
                    return ExecDataRangeVisitorImpl<milvus::ArrayView>(
                        expr.column_.field_id, index_func, elem_func);
                }
                case ArithOpType::Mod: {
                    auto index_func = [val, right_operand](Index* index,
                                                           size_t offset) {
                        return false;
                    };
                    auto elem_func = [&](const milvus::ArrayView& array) {
                        if (index >= array.length()) {
                            return false;
                        }
                        auto value = array.get_data<GetType>(index);
                        return static_cast<ExprValueType>(
                                   fmod(value, right_operand)) != val;
                    };
                    return ExecDataRangeVisitorImpl<milvus::ArrayView>(
                        expr.column_.field_id, index_func, elem_func);
                }
                case ArithOpType::ArrayLength: {
                    auto index_func = [val, right_operand](Index* index,
                                                           size_t offset) {
                        return false;
                    };
                    auto elem_func = [&](const milvus::ArrayView& array) {
                        return array.length() != val;
                    };
                    return ExecDataRangeVisitorImpl<milvus::ArrayView>(
                        expr.column_.field_id, index_func, elem_func);
                }
                default: {
                    PanicInfo(
                        OpTypeInvalid,
                        fmt::format("unsupported arithmetic operation {}", op));
                }
            }
        }
        default: {
            PanicInfo(
                OpTypeInvalid,
                fmt::format(
                    "unsupported range node with arithmetic operation {}", op));
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
        auto skip_index_func = [&](const SkipIndex& skip_index,
                                   FieldId field_id,
                                   int64_t chunk_id) {
            return skip_index.CanSkipBinaryRange<T>(
                field_id, chunk_id, val1, val2, true, true);
        };
        return ExecRangeVisitorImpl<T>(
            expr.column_.field_id, index_func, elem_func, skip_index_func);
    } else if (lower_inclusive && !upper_inclusive) {
        auto elem_func = [val1, val2](MayConstRef<T> x) {
            return (val1 <= x && x < val2);
        };
        auto skip_index_func = [&](const SkipIndex& skip_index,
                                   FieldId field_id,
                                   int64_t chunk_id) {
            return skip_index.CanSkipBinaryRange<T>(
                field_id, chunk_id, val1, val2, true, false);
        };
        return ExecRangeVisitorImpl<T>(
            expr.column_.field_id, index_func, elem_func, skip_index_func);
    } else if (!lower_inclusive && upper_inclusive) {
        auto elem_func = [val1, val2](MayConstRef<T> x) {
            return (val1 < x && x <= val2);
        };
        auto skip_index_func = [&](const SkipIndex& skip_index,
                                   FieldId field_id,
                                   int64_t chunk_id) {
            return skip_index.CanSkipBinaryRange<T>(
                field_id, chunk_id, val1, val2, false, true);
        };
        return ExecRangeVisitorImpl<T>(
            expr.column_.field_id, index_func, elem_func, skip_index_func);
    } else {
        auto elem_func = [val1, val2](MayConstRef<T> x) {
            return (val1 < x && x < val2);
        };
        auto skip_index_func = [&](const SkipIndex& skip_index,
                                   FieldId field_id,
                                   int64_t chunk_id) {
            return skip_index.CanSkipBinaryRange<T>(
                field_id, chunk_id, val1, val2, false, false);
        };
        return ExecRangeVisitorImpl<T>(
            expr.column_.field_id, index_func, elem_func, skip_index_func);
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
    auto default_skip_index_func = [&](const SkipIndex& skipIndex,
                                       FieldId fieldId,
                                       int64_t chunkId) { return false; };

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
        return ExecRangeVisitorImpl<milvus::Json>(expr.column_.field_id,
                                                  index_func,
                                                  elem_func,
                                                  default_skip_index_func);
    } else if (lower_inclusive && !upper_inclusive) {
        auto elem_func = [&](const milvus::Json& json) {
            BinaryRangeJSONCompare(val1 <= value && value < val2);
        };
        return ExecRangeVisitorImpl<milvus::Json>(expr.column_.field_id,
                                                  index_func,
                                                  elem_func,
                                                  default_skip_index_func);
    } else if (!lower_inclusive && upper_inclusive) {
        auto elem_func = [&](const milvus::Json& json) {
            BinaryRangeJSONCompare(val1 < value && value <= val2);
        };
        return ExecRangeVisitorImpl<milvus::Json>(expr.column_.field_id,
                                                  index_func,
                                                  elem_func,
                                                  default_skip_index_func);
    } else {
        auto elem_func = [&](const milvus::Json& json) {
            BinaryRangeJSONCompare(val1 < value && value < val2);
        };
        return ExecRangeVisitorImpl<milvus::Json>(expr.column_.field_id,
                                                  index_func,
                                                  elem_func,
                                                  default_skip_index_func);
    }
}

template <typename ExprValueType>
auto
ExecExprVisitor::ExecBinaryRangeVisitorDispatcherArray(
    BinaryRangeExpr& expr_raw) -> BitsetType {
    using Index = index::ScalarIndex<milvus::ArrayView>;
    using GetType =
        std::conditional_t<std::is_same_v<ExprValueType, std::string>,
                           std::string_view,
                           ExprValueType>;

    auto& expr = static_cast<BinaryRangeExprImpl<ExprValueType>&>(expr_raw);
    bool lower_inclusive = expr.lower_inclusive_;
    bool upper_inclusive = expr.upper_inclusive_;
    ExprValueType val1 = expr.lower_value_;
    ExprValueType val2 = expr.upper_value_;
    int index = -1;
    if (expr.column_.nested_path.size() > 0) {
        index = std::stoi(expr.column_.nested_path[0]);
    }

    // no json index now
    auto index_func = [=](Index* index) { return TargetBitmap{}; };
    auto default_skip_index_func = [&](const SkipIndex& skipIndex,
                                       FieldId fieldId,
                                       int64_t chunkId) { return false; };

    if (lower_inclusive && upper_inclusive) {
        auto elem_func = [&](const milvus::ArrayView& array) {
            if (index >= array.length()) {
                return false;
            }
            auto value = array.get_data<GetType>(index);
            return val1 <= value && value <= val2;
        };
        return ExecRangeVisitorImpl<milvus::ArrayView>(expr.column_.field_id,
                                                       index_func,
                                                       elem_func,
                                                       default_skip_index_func);
    } else if (lower_inclusive && !upper_inclusive) {
        auto elem_func = [&](const milvus::ArrayView& array) {
            if (index >= array.length()) {
                return false;
            }
            auto value = array.get_data<GetType>(index);
            return val1 <= value && value < val2;
        };
        return ExecRangeVisitorImpl<milvus::ArrayView>(expr.column_.field_id,
                                                       index_func,
                                                       elem_func,
                                                       default_skip_index_func);
    } else if (!lower_inclusive && upper_inclusive) {
        auto elem_func = [&](const milvus::ArrayView& array) {
            if (index >= array.length()) {
                return false;
            }
            auto value = array.get_data<GetType>(index);
            return val1 < value && value <= val2;
        };
        return ExecRangeVisitorImpl<milvus::ArrayView>(expr.column_.field_id,
                                                       index_func,
                                                       elem_func,
                                                       default_skip_index_func);
    } else {
        auto elem_func = [&](const milvus::ArrayView& array) {
            if (index >= array.length()) {
                return false;
            }
            auto value = array.get_data<GetType>(index);
            return val1 < value && value < val2;
        };
        return ExecRangeVisitorImpl<milvus::ArrayView>(expr.column_.field_id,
                                                       index_func,
                                                       elem_func,
                                                       default_skip_index_func);
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
                case proto::plan::GenericValue::ValCase::kArrayVal:
                    res =
                        ExecUnaryRangeVisitorDispatcherJson<proto::plan::Array>(
                            expr);
                    break;
                default:
                    PanicInfo(
                        DataTypeInvalid,
                        fmt::format("unknown data type: {}", expr.val_case_));
            }
            break;
        }
        case DataType::ARRAY: {
            switch (expr.val_case_) {
                case proto::plan::GenericValue::ValCase::kBoolVal:
                    res = ExecUnaryRangeVisitorDispatcherArray<bool>(expr);
                    break;
                case proto::plan::GenericValue::ValCase::kInt64Val:
                    res = ExecUnaryRangeVisitorDispatcherArray<int64_t>(expr);
                    break;
                case proto::plan::GenericValue::ValCase::kFloatVal:
                    res = ExecUnaryRangeVisitorDispatcherArray<double>(expr);
                    break;
                case proto::plan::GenericValue::ValCase::kStringVal:
                    res =
                        ExecUnaryRangeVisitorDispatcherArray<std::string>(expr);
                    break;
                case proto::plan::GenericValue::ValCase::kArrayVal:
                    res = ExecUnaryRangeVisitorDispatcherArray<
                        proto::plan::Array>(expr);
                    break;
                default:
                    PanicInfo(
                        DataTypeInvalid,
                        fmt::format("unknown data type: {}", expr.val_case_));
            }
            break;
        }
        default:
            PanicInfo(DataTypeInvalid,
                      "unsupported data type: {}",
                      expr.column_.data_type);
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
                        DataTypeInvalid,
                        fmt::format("unsupported value type {} in expression",
                                    expr.val_case_));
                }
            }
            break;
        }
        case DataType::ARRAY: {
            switch (expr.val_case_) {
                case proto::plan::GenericValue::ValCase::kInt64Val: {
                    res = ExecBinaryArithOpEvalRangeVisitorDispatcherArray<
                        int64_t>(expr);
                    break;
                }
                case proto::plan::GenericValue::ValCase::kFloatVal: {
                    res = ExecBinaryArithOpEvalRangeVisitorDispatcherArray<
                        double>(expr);
                    break;
                }
                default: {
                    PanicInfo(
                        DataTypeInvalid,
                        fmt::format("unsupported value type {} in expression",
                                    expr.val_case_));
                }
            }
            break;
        }
        default:
            PanicInfo(DataTypeInvalid,
                      "unsupported data type: {}",
                      expr.column_.data_type);
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
                        DataTypeInvalid,
                        fmt::format("unsupported value type {} in expression",
                                    expr.val_case_));
                }
            }
            break;
        }
        case DataType::ARRAY: {
            switch (expr.val_case_) {
                case proto::plan::GenericValue::ValCase::kInt64Val: {
                    res = ExecBinaryRangeVisitorDispatcherArray<int64_t>(expr);
                    break;
                }
                case proto::plan::GenericValue::ValCase::kFloatVal: {
                    res = ExecBinaryRangeVisitorDispatcherArray<double>(expr);
                    break;
                }
                case proto::plan::GenericValue::ValCase::kStringVal: {
                    res = ExecBinaryRangeVisitorDispatcherArray<std::string>(
                        expr);
                    break;
                }
                default: {
                    PanicInfo(
                        DataTypeInvalid,
                        fmt::format("unsupported value type {} in expression",
                                    expr.val_case_));
                }
            }
            break;
        }
        default:
            PanicInfo(DataTypeInvalid,
                      "unsupported data type: {}",
                      expr.column_.data_type);
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
        PanicInfo(OpTypeInvalid, "incompatible operands");
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
    std::vector<TargetBitmap> results;
    results.reserve(num_chunks);

    for (int64_t chunk_id = 0; chunk_id < num_chunks; ++chunk_id) {
        TargetBitmap result;
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
                PanicInfo(
                    DataTypeInvalid,
                    fmt::format("unsupported right datatype {} of compare expr",
                                right_field_type));
        }
        results.push_back(std::move(result));
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
            PanicInfo(
                DataTypeInvalid,
                fmt::format("unsupported right datatype {} of compare expr",
                            expr.left_data_type_));
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
                        if (indexing.HasRawData()) {
                            return [&indexing](int i) -> const number {
                                return indexing.Reverse_Lookup(i);
                            };
                        }
                        auto chunk_data =
                            segment_.chunk_data<bool>(field_id, chunk_id)
                                .data();
                        return [chunk_data](int i) -> const number {
                            return chunk_data[i];
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
                        if (indexing.HasRawData()) {
                            return [&indexing](int i) -> const number {
                                return indexing.Reverse_Lookup(i);
                            };
                        }
                        auto chunk_data =
                            segment_.chunk_data<int8_t>(field_id, chunk_id)
                                .data();
                        return [chunk_data](int i) -> const number {
                            return chunk_data[i];
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
                        if (indexing.HasRawData()) {
                            return [&indexing](int i) -> const number {
                                return indexing.Reverse_Lookup(i);
                            };
                        }
                        auto chunk_data =
                            segment_.chunk_data<int16_t>(field_id, chunk_id)
                                .data();
                        return [chunk_data](int i) -> const number {
                            return chunk_data[i];
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
                        if (indexing.HasRawData()) {
                            return [&indexing](int i) -> const number {
                                return indexing.Reverse_Lookup(i);
                            };
                        }
                        auto chunk_data =
                            segment_.chunk_data<int32_t>(field_id, chunk_id)
                                .data();
                        return [chunk_data](int i) -> const number {
                            return chunk_data[i];
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
                        if (indexing.HasRawData()) {
                            return [&indexing](int i) -> const number {
                                return indexing.Reverse_Lookup(i);
                            };
                        }
                        auto chunk_data =
                            segment_.chunk_data<int64_t>(field_id, chunk_id)
                                .data();
                        return [chunk_data](int i) -> const number {
                            return chunk_data[i];
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
                        if (indexing.HasRawData()) {
                            return [&indexing](int i) -> const number {
                                return indexing.Reverse_Lookup(i);
                            };
                        }
                        auto chunk_data =
                            segment_.chunk_data<float>(field_id, chunk_id)
                                .data();
                        return [chunk_data](int i) -> const number {
                            return chunk_data[i];
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
                        if (indexing.HasRawData()) {
                            return [&indexing](int i) -> const number {
                                return indexing.Reverse_Lookup(i);
                            };
                        }
                        auto chunk_data =
                            segment_.chunk_data<double>(field_id, chunk_id)
                                .data();
                        return [chunk_data](int i) -> const number {
                            return chunk_data[i];
                        };
                    }
                }
                case DataType::VARCHAR: {
                    if (chunk_id < data_barrier) {
                        if (segment_.type() == SegmentType::Growing &&
                            !storage::MmapManager::GetInstance()
                                 .GetMmapConfig()
                                 .growing_enable_mmap) {
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
                        if (indexing.HasRawData()) {
                            return [&indexing](int i) -> const number {
                                return indexing.Reverse_Lookup(i);
                            };
                        }
                        auto chunk_data =
                            segment_.chunk_data<std::string>(field_id, chunk_id)
                                .data();
                        return [chunk_data](int i) -> const number {
                            return chunk_data[i];
                        };
                    }
                }
                default:
                    PanicInfo(
                        DataTypeInvalid, "unsupported data type {}", type);
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
            PanicInfo(OpTypeInvalid, "unsupported optype {}", expr.op_type_);
        }
    }
    AssertInfo(res.size() == row_count_,
               "[ExecExprVisitor]Size of results not equal row count");
    bitset_opt_ = std::move(res);
}

template <typename T>
auto
ExecExprVisitor::ExecTermVisitorImpl(TermExpr& expr_raw) -> BitsetType {
    typedef std::
        conditional_t<std::is_same_v<T, std::string_view>, std::string, T>
            InnerType;
    auto& expr = static_cast<TermExprImpl<InnerType>&>(expr_raw);
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
                PanicInfo(
                    DataTypeInvalid,
                    fmt::format("unsupported data type {}", expr.val_case_));
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
        // If enable plan_visitor pk index cache, pass offsets_ to it
        if (plan_visitor_ != nullptr) {
            plan_visitor_->SetExprUsePkIndex(true);
        }
        AssertInfo(bitset.size() == row_count_,
                   "[ExecExprVisitor]Size of results not equal row count");
        return bitset;
    }

    return ExecTermVisitorImplTemplate<T>(expr_raw);
}

template <typename T>
auto
ExecExprVisitor::ExecTermVisitorImplTemplate(TermExpr& expr_raw) -> BitsetType {
    typedef std::
        conditional_t<std::is_same_v<T, std::string_view>, std::string, T>
            IndexInnerType;
    using Index = index::ScalarIndex<IndexInnerType>;
    auto& expr = static_cast<TermExprImpl<IndexInnerType>&>(expr_raw);
    const auto& terms = expr.terms_;
    auto n = terms.size();
    std::unordered_set<T> term_set(expr.terms_.begin(), expr.terms_.end());

    auto index_func = [&terms, n](Index* index) {
        return index->In(n, terms.data());
    };

    auto elem_func = [&term_set](MayConstRef<T> x) {
        return term_set.find(x) != term_set.end();
    };

    auto default_skip_index_func = [&](const SkipIndex& skipIndex,
                                       FieldId fieldId,
                                       int64_t chunkId) { return false; };
    return ExecRangeVisitorImpl<T>(
        expr.column_.field_id, index_func, elem_func, default_skip_index_func);
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
    auto skip_index_func =
        [&](const SkipIndex& skipIndex, FieldId fieldId, int64_t chunkId) {
            for (const auto& term : term_set) {
                if (!skipIndex.CanSkipUnaryRange(
                        fieldId, chunkId, OpType::Equal, term)) {
                    return false;
                }
            }
            return true;
        };

    return ExecRangeVisitorImpl<T>(
        expr.column_.field_id, index_func, elem_func, skip_index_func);
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

    auto default_skip_index_func = [&](const SkipIndex& skipIndex,
                                       FieldId fieldId,
                                       int64_t chunkId) { return false; };
    if (term_set.empty()) {
        auto elem_func = [=](const milvus::Json& json) { return false; };
        return ExecRangeVisitorImpl<milvus::Json>(expr.column_.field_id,
                                                  index_func,
                                                  elem_func,
                                                  default_skip_index_func);
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
        expr.column_.field_id, index_func, elem_func, default_skip_index_func);
}

template <typename ExprValueType>
auto
ExecExprVisitor::ExecTermArrayFieldInVariable(TermExpr& expr_raw)
    -> BitsetType {
    using Index = index::ScalarIndex<milvus::ArrayView>;
    auto& expr = static_cast<TermExprImpl<ExprValueType>&>(expr_raw);
    auto index_func = [](Index* index) { return TargetBitmap{}; };
    auto default_skip_index_func = [&](const SkipIndex& skipIndex,
                                       FieldId fieldId,
                                       int64_t chunkId) { return false; };
    int index = -1;
    if (expr.column_.nested_path.size() > 0) {
        index = std::stoi(expr.column_.nested_path[0]);
    }
    std::unordered_set<ExprValueType> term_set(expr.terms_.begin(),
                                               expr.terms_.end());
    using GetType =
        std::conditional_t<std::is_same_v<ExprValueType, std::string>,
                           std::string_view,
                           ExprValueType>;

    if (term_set.empty()) {
        auto elem_func = [=](const milvus::ArrayView& array) { return false; };
        return ExecRangeVisitorImpl<milvus::ArrayView>(expr.column_.field_id,
                                                       index_func,
                                                       elem_func,
                                                       default_skip_index_func);
    }

    auto elem_func = [&term_set, &index](const milvus::ArrayView& array) {
        if (index >= array.length()) {
            return false;
        }
        auto value = array.get_data<GetType>(index);
        return term_set.find(ExprValueType(value)) != term_set.end();
    };
    return ExecRangeVisitorImpl<milvus::ArrayView>(
        expr.column_.field_id, index_func, elem_func, default_skip_index_func);
}

template <typename ExprValueType>
auto
ExecExprVisitor::ExecTermJsonVariableInField(TermExpr& expr_raw) -> BitsetType {
    using Index = index::ScalarIndex<milvus::Json>;
    auto& expr = static_cast<TermExprImpl<ExprValueType>&>(expr_raw);
    auto pointer = milvus::Json::pointer(expr.column_.nested_path);
    auto index_func = [](Index* index) { return TargetBitmap{}; };
    auto default_skip_index_func = [&](const SkipIndex& skipIndex,
                                       FieldId fieldId,
                                       int64_t chunkId) { return false; };

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
            if (val.value() == target_val) {
                return true;
            }
        }
        return false;
    };
    return ExecRangeVisitorImpl<milvus::Json>(
        expr.column_.field_id, index_func, elem_func, default_skip_index_func);
}

template <typename ExprValueType>
auto
ExecExprVisitor::ExecTermArrayVariableInField(TermExpr& expr_raw)
    -> BitsetType {
    using Index = index::ScalarIndex<milvus::ArrayView>;
    auto& expr = static_cast<TermExprImpl<ExprValueType>&>(expr_raw);
    auto index_func = [](Index* index) { return TargetBitmap{}; };

    AssertInfo(expr.terms_.size() == 1,
               "element length in json array must be one");
    ExprValueType target_val = expr.terms_[0];
    auto default_skip_index_func = [&](const SkipIndex& skipIndex,
                                       FieldId fieldId,
                                       int64_t chunkId) { return false; };
    auto elem_func = [&target_val](const milvus::ArrayView& array) {
        using GetType =
            std::conditional_t<std::is_same_v<ExprValueType, std::string>,
                               std::string_view,
                               ExprValueType>;
        for (int i = 0; i < array.length(); i++) {
            auto val = array.template get_data<GetType>(i);
            if (val == target_val) {
                return true;
            }
        }
        return false;
    };

    return ExecRangeVisitorImpl<milvus::ArrayView>(
        expr.column_.field_id, index_func, elem_func, default_skip_index_func);
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

template <typename ExprValueType>
auto
ExecExprVisitor::ExecTermVisitorImplTemplateArray(TermExpr& expr_raw)
    -> BitsetType {
    if (expr_raw.is_in_field_) {
        return ExecTermArrayVariableInField<ExprValueType>(expr_raw);
    } else {
        return ExecTermArrayFieldInVariable<ExprValueType>(expr_raw);
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
                    PanicInfo(DataTypeInvalid,
                              "unsupported data type {}",
                              expr.val_case_);
            }
            break;
        }
        case DataType::ARRAY: {
            switch (expr.val_case_) {
                case proto::plan::GenericValue::ValCase::kBoolVal:
                    res = ExecTermVisitorImplTemplateArray<bool>(expr);
                    break;
                case proto::plan::GenericValue::ValCase::kInt64Val:
                    res = ExecTermVisitorImplTemplateArray<int64_t>(expr);
                    break;
                case proto::plan::GenericValue::ValCase::kFloatVal:
                    res = ExecTermVisitorImplTemplateArray<double>(expr);
                    break;
                case proto::plan::GenericValue::ValCase::kStringVal:
                    res = ExecTermVisitorImplTemplateArray<std::string>(expr);
                    break;
                case proto::plan::GenericValue::ValCase::VAL_NOT_SET:
                    res = ExecTermVisitorImplTemplateArray<bool>(expr);
                    break;
                default:
                    PanicInfo(
                        Unsupported,
                        fmt::format("unknown data type: {}", expr.val_case_));
            }
            break;
        }
        default:
            PanicInfo(DataTypeInvalid,
                      "unsupported data type {}",
                      expr.column_.data_type);
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
            auto default_skip_index_func = [&](const SkipIndex& skipIndex,
                                               FieldId fieldId,
                                               int64_t chunkId) {
                return false;
            };
            res = ExecRangeVisitorImpl<milvus::Json>(expr.column_.field_id,
                                                     index_func,
                                                     elem_func,
                                                     default_skip_index_func);
            break;
        }
        default:
            PanicInfo(DataTypeInvalid,
                      "unsupported data type {}",
                      expr.column_.data_type);
    }
    AssertInfo(res.size() == row_count_,
               "[ExecExprVisitor]Size of results not equal row count");
    bitset_opt_ = std::move(res);
}

void
ExecExprVisitor::visit(AlwaysTrueExpr& expr) {
    BitsetType res(row_count_);
    res.set();
    bitset_opt_ = std::move(res);
}

template <typename ExprValueType>
auto
ExecExprVisitor::ExecJsonContains(JsonContainsExpr& expr_raw) -> BitsetType {
    using Index = index::ScalarIndex<milvus::Json>;
    auto& expr = static_cast<JsonContainsExprImpl<ExprValueType>&>(expr_raw);
    auto pointer = milvus::Json::pointer(expr.column_.nested_path);
    auto index_func = [](Index* index) { return TargetBitmap{}; };
    using GetType =
        std::conditional_t<std::is_same_v<ExprValueType, std::string>,
                           std::string_view,
                           ExprValueType>;
    std::unordered_set<GetType> elements;
    for (auto const& element : expr.elements_) {
        elements.insert(element);
    }
    auto elem_func = [&elements, &pointer](const milvus::Json& json) {
        auto doc = json.doc();
        auto array = doc.at_pointer(pointer).get_array();
        if (array.error()) {
            return false;
        }
        for (auto&& it : array) {
            auto val = it.template get<GetType>();
            if (val.error()) {
                continue;
            }
            if (elements.count(val.value()) > 0) {
                return true;
            }
        }
        return false;
    };
    auto default_skip_index_func = [&](const SkipIndex& skipIndex,
                                       FieldId fieldId,
                                       int64_t chunkId) { return false; };
    return ExecRangeVisitorImpl<milvus::Json>(
        expr.column_.field_id, index_func, elem_func, default_skip_index_func);
}

template <typename ExprValueType>
auto
ExecExprVisitor::ExecArrayContains(JsonContainsExpr& expr_raw) -> BitsetType {
    using Index = index::ScalarIndex<milvus::ArrayView>;
    auto& expr = static_cast<JsonContainsExprImpl<ExprValueType>&>(expr_raw);
    AssertInfo(expr.column_.nested_path.size() == 0,
               "[ExecArrayContains]nested path must be null");
    auto index_func = [](Index* index) { return TargetBitmap{}; };
    using GetType =
        std::conditional_t<std::is_same_v<ExprValueType, std::string>,
                           std::string_view,
                           ExprValueType>;
    std::unordered_set<GetType> elements;
    for (auto const& element : expr.elements_) {
        elements.insert(element);
    }
    auto elem_func = [&elements](const milvus::ArrayView& array) {
        for (int i = 0; i < array.length(); ++i) {
            if (elements.count(array.template get_data<GetType>(i)) > 0) {
                return true;
            }
        }
        return false;
    };

    auto default_skip_index_func = [&](const SkipIndex& skipIndex,
                                       FieldId fieldId,
                                       int64_t chunkId) { return false; };
    return ExecRangeVisitorImpl<milvus::ArrayView>(
        expr.column_.field_id, index_func, elem_func, default_skip_index_func);
}

auto
ExecExprVisitor::ExecJsonContainsArray(JsonContainsExpr& expr_raw)
    -> BitsetType {
    using Index = index::ScalarIndex<milvus::Json>;
    auto& expr =
        static_cast<JsonContainsExprImpl<proto::plan::Array>&>(expr_raw);
    auto pointer = milvus::Json::pointer(expr.column_.nested_path);
    auto index_func = [](Index* index) { return TargetBitmap{}; };
    auto& elements = expr.elements_;
    auto elem_func = [&elements, &pointer](const milvus::Json& json) {
        auto doc = json.doc();
        auto array = doc.at_pointer(pointer).get_array();
        if (array.error()) {
            return false;
        }
        for (auto&& it : array) {
            auto val = it.get_array();
            if (val.error()) {
                continue;
            }
            std::vector<simdjson::simdjson_result<simdjson::ondemand::value>>
                json_array;
            json_array.reserve(val.count_elements());
            for (auto&& e : val) {
                json_array.emplace_back(e);
            }
            for (auto const& element : elements) {
                if (CompareTwoJsonArray(json_array, element)) {
                    return true;
                }
            }
        }
        return false;
    };
    auto default_skip_index_func = [&](const SkipIndex& skipIndex,
                                       FieldId fieldId,
                                       int64_t chunkId) { return false; };
    return ExecRangeVisitorImpl<milvus::Json>(
        expr.column_.field_id, index_func, elem_func, default_skip_index_func);
}

auto
ExecExprVisitor::ExecJsonContainsWithDiffType(JsonContainsExpr& expr_raw)
    -> BitsetType {
    using Index = index::ScalarIndex<milvus::Json>;
    auto& expr =
        static_cast<JsonContainsExprImpl<proto::plan::GenericValue>&>(expr_raw);
    auto pointer = milvus::Json::pointer(expr.column_.nested_path);
    auto index_func = [](Index* index) { return TargetBitmap{}; };
    auto& elements = expr.elements_;
    auto elem_func = [&elements, &pointer](const milvus::Json& json) {
        auto doc = json.doc();
        auto array = doc.at_pointer(pointer).get_array();
        if (array.error()) {
            return false;
        }
        // Note: array can only be iterated once
        for (auto&& it : array) {
            for (auto const& element : elements) {
                switch (element.val_case()) {
                    case proto::plan::GenericValue::kBoolVal: {
                        auto val = it.template get<bool>();
                        if (val.error()) {
                            continue;
                        }
                        if (val.value() == element.bool_val()) {
                            return true;
                        }
                        break;
                    }
                    case proto::plan::GenericValue::kInt64Val: {
                        auto val = it.template get<int64_t>();
                        if (val.error()) {
                            continue;
                        }
                        if (val.value() == element.int64_val()) {
                            return true;
                        }
                        break;
                    }
                    case proto::plan::GenericValue::kFloatVal: {
                        auto val = it.template get<double>();
                        if (val.error()) {
                            continue;
                        }
                        if (val.value() == element.float_val()) {
                            return true;
                        }
                        break;
                    }
                    case proto::plan::GenericValue::kStringVal: {
                        auto val = it.template get<std::string_view>();
                        if (val.error()) {
                            continue;
                        }
                        if (val.value() == element.string_val()) {
                            return true;
                        }
                        break;
                    }
                    case proto::plan::GenericValue::kArrayVal: {
                        auto val = it.get_array();
                        if (val.error()) {
                            continue;
                        }
                        if (CompareTwoJsonArray(val, element.array_val())) {
                            return true;
                        }
                        break;
                    }
                    default:
                        PanicInfo(DataTypeInvalid,
                                  "unsupported data type {}",
                                  element.val_case());
                }
            }
        }
        return false;
    };
    auto default_skip_index_func = [&](const SkipIndex& skipIndex,
                                       FieldId fieldId,
                                       int64_t chunkId) { return false; };

    return ExecRangeVisitorImpl<milvus::Json>(
        expr.column_.field_id, index_func, elem_func, default_skip_index_func);
}

template <typename ExprValueType>
auto
ExecExprVisitor::ExecJsonContainsAll(JsonContainsExpr& expr_raw) -> BitsetType {
    using Index = index::ScalarIndex<milvus::Json>;
    auto& expr = static_cast<JsonContainsExprImpl<ExprValueType>&>(expr_raw);
    auto pointer = milvus::Json::pointer(expr.column_.nested_path);
    auto index_func = [](Index* index) { return TargetBitmap{}; };
    using GetType =
        std::conditional_t<std::is_same_v<ExprValueType, std::string>,
                           std::string_view,
                           ExprValueType>;

    std::unordered_set<GetType> elements;
    for (auto const& element : expr.elements_) {
        elements.insert(element);
    }
    //    auto elements = expr.elements_;
    auto elem_func = [&elements, &pointer](const milvus::Json& json) {
        auto doc = json.doc();
        auto array = doc.at_pointer(pointer).get_array();
        if (array.error()) {
            return false;
        }
        std::unordered_set<GetType> tmp_elements(elements);
        // Note: array can only be iterated once
        for (auto&& it : array) {
            auto val = it.template get<GetType>();
            if (val.error()) {
                continue;
            }
            tmp_elements.erase(val.value());
            if (tmp_elements.size() == 0) {
                return true;
            }
        }
        return tmp_elements.size() == 0;
    };
    auto default_skip_index_func = [&](const SkipIndex& skipIndex,
                                       FieldId fieldId,
                                       int64_t chunkId) { return false; };

    return ExecRangeVisitorImpl<milvus::Json>(
        expr.column_.field_id, index_func, elem_func, default_skip_index_func);
}

template <typename ExprValueType>
auto
ExecExprVisitor::ExecArrayContainsAll(JsonContainsExpr& expr_raw)
    -> BitsetType {
    using Index = index::ScalarIndex<milvus::ArrayView>;
    auto& expr = static_cast<JsonContainsExprImpl<ExprValueType>&>(expr_raw);
    AssertInfo(expr.column_.nested_path.size() == 0,
               "[ExecArrayContains]nested path must be null");
    auto index_func = [](Index* index) { return TargetBitmap{}; };
    using GetType =
        std::conditional_t<std::is_same_v<ExprValueType, std::string>,
                           std::string_view,
                           ExprValueType>;

    std::unordered_set<GetType> elements;
    for (auto const& element : expr.elements_) {
        elements.insert(element);
    }
    //    auto elements = expr.elements_;
    auto elem_func = [&elements](const milvus::ArrayView& array) {
        std::unordered_set<GetType> tmp_elements(elements);
        // Note: array can only be iterated once
        for (int i = 0; i < array.length(); ++i) {
            tmp_elements.erase(array.template get_data<GetType>(i));
            if (tmp_elements.size() == 0) {
                return true;
            }
        }
        return tmp_elements.size() == 0;
    };
    auto default_skip_index_func = [&](const SkipIndex& skipIndex,
                                       FieldId fieldId,
                                       int64_t chunkId) { return false; };
    return ExecRangeVisitorImpl<milvus::ArrayView>(
        expr.column_.field_id, index_func, elem_func, default_skip_index_func);
}

auto
ExecExprVisitor::ExecJsonContainsAllArray(JsonContainsExpr& expr_raw)
    -> BitsetType {
    using Index = index::ScalarIndex<milvus::Json>;
    auto& expr =
        static_cast<JsonContainsExprImpl<proto::plan::Array>&>(expr_raw);
    auto pointer = milvus::Json::pointer(expr.column_.nested_path);
    auto index_func = [](Index* index) { return TargetBitmap{}; };
    auto& elements = expr.elements_;

    auto elem_func = [&elements, &pointer](const milvus::Json& json) {
        auto doc = json.doc();
        auto array = doc.at_pointer(pointer).get_array();
        if (array.error()) {
            return false;
        }
        std::unordered_set<int> exist_elements_index;
        for (auto&& it : array) {
            auto val = it.get_array();
            if (val.error()) {
                continue;
            }
            std::vector<simdjson::simdjson_result<simdjson::ondemand::value>>
                json_array;
            json_array.reserve(val.count_elements());
            for (auto&& e : val) {
                json_array.emplace_back(e);
            }
            for (int index = 0; index < elements.size(); ++index) {
                if (CompareTwoJsonArray(json_array, elements[index])) {
                    exist_elements_index.insert(index);
                }
            }
            if (exist_elements_index.size() == elements.size()) {
                return true;
            }
        }
        return exist_elements_index.size() == elements.size();
    };

    auto default_skip_index_func = [&](const SkipIndex& skipIndex,
                                       FieldId fieldId,
                                       int64_t chunkId) { return false; };

    return ExecRangeVisitorImpl<milvus::Json>(
        expr.column_.field_id, index_func, elem_func, default_skip_index_func);
}

auto
ExecExprVisitor::ExecJsonContainsAllWithDiffType(JsonContainsExpr& expr_raw)
    -> BitsetType {
    using Index = index::ScalarIndex<milvus::Json>;
    auto& expr =
        static_cast<JsonContainsExprImpl<proto::plan::GenericValue>&>(expr_raw);
    auto pointer = milvus::Json::pointer(expr.column_.nested_path);
    auto index_func = [](Index* index) { return TargetBitmap{}; };

    auto elements = expr.elements_;
    std::unordered_set<int> elements_index;
    int i = 0;
    for (auto& element : expr.elements_) {
        elements_index.insert(i);
        i++;
    }
    auto elem_func =
        [&elements, &elements_index, &pointer](const milvus::Json& json) {
            auto doc = json.doc();
            auto array = doc.at_pointer(pointer).get_array();
            if (array.error()) {
                return false;
            }
            std::unordered_set<int> tmp_elements_index(elements_index);
            for (auto&& it : array) {
                int i = -1;
                for (auto& element : elements) {
                    i++;
                    switch (element.val_case()) {
                        case proto::plan::GenericValue::kBoolVal: {
                            auto val = it.template get<bool>();
                            if (val.error()) {
                                continue;
                            }
                            if (val.value() == element.bool_val()) {
                                tmp_elements_index.erase(i);
                            }
                            break;
                        }
                        case proto::plan::GenericValue::kInt64Val: {
                            auto val = it.template get<int64_t>();
                            if (val.error()) {
                                continue;
                            }
                            if (val.value() == element.int64_val()) {
                                tmp_elements_index.erase(i);
                            }
                            break;
                        }
                        case proto::plan::GenericValue::kFloatVal: {
                            auto val = it.template get<double>();
                            if (val.error()) {
                                continue;
                            }
                            if (val.value() == element.float_val()) {
                                tmp_elements_index.erase(i);
                            }
                            break;
                        }
                        case proto::plan::GenericValue::kStringVal: {
                            auto val = it.template get<std::string_view>();
                            if (val.error()) {
                                continue;
                            }
                            if (val.value() == element.string_val()) {
                                tmp_elements_index.erase(i);
                            }
                            break;
                        }
                        case proto::plan::GenericValue::kArrayVal: {
                            auto val = it.get_array();
                            if (val.error()) {
                                continue;
                            }
                            if (CompareTwoJsonArray(val, element.array_val())) {
                                tmp_elements_index.erase(i);
                            }
                            break;
                        }
                        default:
                            PanicInfo(DataTypeInvalid,
                                      "unsupported data type {}",
                                      element.val_case());
                    }
                    if (tmp_elements_index.size() == 0) {
                        return true;
                    }
                }
                if (tmp_elements_index.size() == 0) {
                    return true;
                }
            }
            return tmp_elements_index.size() == 0;
        };
    auto default_skip_index_func = [&](const SkipIndex& skipIndex,
                                       FieldId fieldId,
                                       int64_t chunkId) { return false; };

    return ExecRangeVisitorImpl<milvus::Json>(
        expr.column_.field_id, index_func, elem_func, default_skip_index_func);
}

void
ExecExprVisitor::visit(JsonContainsExpr& expr) {
    auto& field_meta = segment_.get_schema()[expr.column_.field_id];
    AssertInfo(
        expr.column_.data_type == DataType::JSON ||
            expr.column_.data_type == DataType::ARRAY,
        "[ExecExprVisitor]DataType of JsonContainsExpr isn't json data type");
    BitsetType res;
    auto data_type = expr.column_.data_type;
    switch (expr.op_) {
        case proto::plan::JSONContainsExpr_JSONOp_Contains:
        case proto::plan::JSONContainsExpr_JSONOp_ContainsAny: {
            if (IsArrayDataType(data_type)) {
                switch (expr.val_case_) {
                    case proto::plan::GenericValue::kBoolVal: {
                        res = ExecArrayContains<bool>(expr);
                        break;
                    }
                    case proto::plan::GenericValue::kInt64Val: {
                        res = ExecArrayContains<int64_t>(expr);
                        break;
                    }
                    case proto::plan::GenericValue::kFloatVal: {
                        res = ExecArrayContains<double>(expr);
                        break;
                    }
                    case proto::plan::GenericValue::kStringVal: {
                        res = ExecArrayContains<std::string>(expr);
                        break;
                    }
                    default:
                        PanicInfo(DataTypeInvalid,
                                  "unsupported data type {}",
                                  expr.val_case_);
                }
            } else {
                if (expr.same_type_) {
                    switch (expr.val_case_) {
                        case proto::plan::GenericValue::kBoolVal: {
                            res = ExecJsonContains<bool>(expr);
                            break;
                        }
                        case proto::plan::GenericValue::kInt64Val: {
                            res = ExecJsonContains<int64_t>(expr);
                            break;
                        }
                        case proto::plan::GenericValue::kFloatVal: {
                            res = ExecJsonContains<double>(expr);
                            break;
                        }
                        case proto::plan::GenericValue::kStringVal: {
                            res = ExecJsonContains<std::string>(expr);
                            break;
                        }
                        case proto::plan::GenericValue::kArrayVal: {
                            res = ExecJsonContainsArray(expr);
                            break;
                        }
                        default:
                            PanicInfo(Unsupported,
                                      "unsupported value type {}",
                                      expr.val_case_);
                    }
                } else {
                    res = ExecJsonContainsWithDiffType(expr);
                }
            }
            break;
        }
        case proto::plan::JSONContainsExpr_JSONOp_ContainsAll: {
            if (IsArrayDataType(data_type)) {
                switch (expr.val_case_) {
                    case proto::plan::GenericValue::kBoolVal: {
                        res = ExecArrayContainsAll<bool>(expr);
                        break;
                    }
                    case proto::plan::GenericValue::kInt64Val: {
                        res = ExecArrayContainsAll<int64_t>(expr);
                        break;
                    }
                    case proto::plan::GenericValue::kFloatVal: {
                        res = ExecArrayContainsAll<double>(expr);
                        break;
                    }
                    case proto::plan::GenericValue::kStringVal: {
                        res = ExecArrayContainsAll<std::string>(expr);
                        break;
                    }
                    default:
                        PanicInfo(DataTypeInvalid,
                                  "unsupported data type {}",
                                  expr.val_case_);
                }
            } else {
                if (expr.same_type_) {
                    switch (expr.val_case_) {
                        case proto::plan::GenericValue::kBoolVal: {
                            res = ExecJsonContainsAll<bool>(expr);
                            break;
                        }
                        case proto::plan::GenericValue::kInt64Val: {
                            res = ExecJsonContainsAll<int64_t>(expr);
                            break;
                        }
                        case proto::plan::GenericValue::kFloatVal: {
                            res = ExecJsonContainsAll<double>(expr);
                            break;
                        }
                        case proto::plan::GenericValue::kStringVal: {
                            res = ExecJsonContainsAll<std::string>(expr);
                            break;
                        }
                        case proto::plan::GenericValue::kArrayVal: {
                            res = ExecJsonContainsAllArray(expr);
                            break;
                        }
                        default:
                            PanicInfo(
                                Unsupported,
                                fmt::format(
                                    "unsupported value type {} in expression",
                                    expr.val_case_));
                    }
                } else {
                    res = ExecJsonContainsAllWithDiffType(expr);
                }
            }
            break;
        }
        default:
            PanicInfo(DataTypeInvalid,
                      "unsupported json contains type {}",
                      expr.val_case_);
    }
    AssertInfo(res.size() == row_count_,
               "[ExecExprVisitor]Size of results not equal row count");
    bitset_opt_ = std::move(res);
}

}  // namespace milvus::query
