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

#include "JsonContainsExpr.h"

#include <algorithm>
#include <cstdint>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <variant>

#include "boost/container/vector.hpp"
#include "boost/cstdint.hpp"
#include "common/Array.h"
#include "common/Json.h"
#include "common/Tracer.h"
#include "common/Types.h"
#include "common/type_c.h"
#include "common/ScopedTimer.h"
#include "exec/expression/EvalCtx.h"
#include "fmt/core.h"
#include "folly/FBVector.h"
#include "monitor/Monitor.h"
#include "index/ScalarIndex.h"
#include "index/json_stats/JsonKeyStats.h"
#include "index/json_stats/utils.h"
#include "opentelemetry/trace/span.h"
#include "segcore/SegmentInterface.h"
#include "segcore/SegmentSealed.h"

namespace milvus {
namespace exec {

void
PhyJsonContainsFilterExpr::Eval(EvalCtx& context, VectorPtr& result) {
    WaitPrefetch();
    tracer::AutoSpan span(
        "PhyJsonContainsFilterExpr::Eval", tracer::GetRootSpan(), true);
    span.SetAttribute("data_type", static_cast<int>(expr_->column_.data_type_));
    span.SetAttribute("json_filter_expr_type", "json_contains");

    auto input = context.get_offset_input();
    SetHasOffsetInput((input != nullptr));
    const bool is_element_level_array =
        expr_->column_.data_type_ == DataType::ARRAY &&
        expr_->column_.element_level_;
    if (expr_->vals_.empty() && !is_element_level_array) {
        auto real_batch_size =
            has_offset_input_ ? input->size() : GetNextBatchSize();
        if (real_batch_size == 0) {
            result = nullptr;
            return;
        }

        if (expr_->column_.data_type_ == DataType::ARRAY &&
            expr_->column_.nullable_) {
            auto valid_result =
                has_offset_input_
                    ? ProcessChunksForValidByOffsets<ArrayView>(false, *input)
                    : ProcessDataChunksForValid<ArrayView>();

            const bool empty_matches =
                expr_->op_ == proto::plan::JSONContainsExpr_JSONOp_ContainsAll;
            TargetBitmap value_result(real_batch_size, empty_matches);
            value_result &= valid_result;
            result = std::make_shared<ColumnVector>(std::move(value_result),
                                                    std::move(valid_result));
            return;
        }

        if (expr_->op_ == proto::plan::JSONContainsExpr_JSONOp_ContainsAll) {
            result = std::make_shared<ColumnVector>(
                TargetBitmap(real_batch_size, true),
                TargetBitmap(real_batch_size, true));
        } else {
            result = std::make_shared<ColumnVector>(
                TargetBitmap(real_batch_size, false),
                TargetBitmap(real_batch_size, true));
        }
        MoveCursor();
        return;
    }

    switch (expr_->column_.data_type_) {
        case DataType::ARRAY: {
            if (exec_path_ == ExprExecPath::ScalarIndex && !has_offset_input_) {
                result = EvalArrayContainsForIndexSegment(
                    expr_->column_.element_type_);
            } else {
                result = EvalJsonContainsForDataSegment(context);
            }
            break;
        }
        case DataType::JSON: {
            if (exec_path_ == ExprExecPath::ScalarIndex && !has_offset_input_) {
                if (value_type_ == DataType::INT64 && PinnedJsonIndexIsFlat()) {
                    result = EvalArrayContainsForIndexSegment(DataType::INT64);
                } else {
                    result = EvalArrayContainsForIndexSegment(
                        value_type_ == DataType::INT64 ? DataType::DOUBLE
                                                       : value_type_);
                }
            } else {
                result = EvalJsonContainsForDataSegment(context);
            }
            break;
        }
        default:
            ThrowInfo(UnexpectedError,
                      "unsupported data type: {}",
                      expr_->column_.data_type_);
    }
}

VectorPtr
PhyJsonContainsFilterExpr::EvalJsonContainsForDataSegment(EvalCtx& context) {
    auto data_type = expr_->column_.data_type_;
    switch (expr_->op_) {
        case proto::plan::JSONContainsExpr_JSONOp_Contains:
        case proto::plan::JSONContainsExpr_JSONOp_ContainsAny: {
            if (IsArrayDataType(data_type)) {
                auto val_type = expr_->column_.element_type_;
                switch (val_type) {
                    case DataType::BOOL: {
                        return ExecArrayContains<bool>(context);
                    }
                    case DataType::INT8:
                    case DataType::INT16:
                    case DataType::INT32:
                    case DataType::INT64: {
                        return ExecArrayContains<int64_t>(context);
                    }
                    case DataType::FLOAT: {
                        return ExecArrayContains<float>(context);
                    }
                    case DataType::DOUBLE: {
                        return ExecArrayContains<double>(context);
                    }
                    case DataType::STRING:
                    case DataType::VARCHAR: {
                        return ExecArrayContains<std::string>(context);
                    }
                    default:
                        ThrowInfo(UnexpectedError,
                                  "unsupported array sub element type {}",
                                  val_type);
                }
            } else {
                if (expr_->same_type_) {
                    auto val_type = expr_->vals_[0].val_case();
                    switch (val_type) {
                        case proto::plan::GenericValue::kBoolVal: {
                            return ExecJsonContains<bool>(context);
                        }
                        case proto::plan::GenericValue::kInt64Val: {
                            return ExecJsonContains<int64_t>(context);
                        }
                        case proto::plan::GenericValue::kFloatVal: {
                            return ExecJsonContains<double>(context);
                        }
                        case proto::plan::GenericValue::kStringVal: {
                            return ExecJsonContains<std::string>(context);
                        }
                        case proto::plan::GenericValue::kArrayVal: {
                            return ExecJsonContainsArray(context);
                        }
                        default:
                            ThrowInfo(UnexpectedError,
                                      "unsupported data type:{}",
                                      val_type);
                    }
                } else {
                    return ExecJsonContainsWithDiffType(context);
                }
            }
        }
        case proto::plan::JSONContainsExpr_JSONOp_ContainsAll: {
            if (IsArrayDataType(data_type)) {
                auto val_type = expr_->column_.element_type_;
                switch (val_type) {
                    case DataType::BOOL: {
                        return ExecArrayContainsAll<bool>(context);
                    }
                    case DataType::INT8:
                    case DataType::INT16:
                    case DataType::INT32:
                    case DataType::INT64: {
                        return ExecArrayContainsAll<int64_t>(context);
                    }
                    case DataType::FLOAT: {
                        return ExecArrayContainsAll<float>(context);
                    }
                    case DataType::DOUBLE: {
                        return ExecArrayContainsAll<double>(context);
                    }
                    case DataType::STRING:
                    case DataType::VARCHAR: {
                        return ExecArrayContainsAll<std::string>(context);
                    }
                    default:
                        ThrowInfo(UnexpectedError,
                                  "unsupported array sub element type {}",
                                  val_type);
                }
            } else {
                if (expr_->same_type_) {
                    auto val_type = expr_->vals_[0].val_case();
                    switch (val_type) {
                        case proto::plan::GenericValue::kBoolVal: {
                            return ExecJsonContainsAll<bool>(context);
                        }
                        case proto::plan::GenericValue::kInt64Val: {
                            return ExecJsonContainsAll<int64_t>(context);
                        }
                        case proto::plan::GenericValue::kFloatVal: {
                            return ExecJsonContainsAll<double>(context);
                        }
                        case proto::plan::GenericValue::kStringVal: {
                            return ExecJsonContainsAll<std::string>(context);
                        }
                        case proto::plan::GenericValue::kArrayVal: {
                            return ExecJsonContainsAllArray(context);
                        }
                        default:
                            ThrowInfo(UnexpectedError,
                                      "unsupported data type:{}",
                                      val_type);
                    }
                } else {
                    return ExecJsonContainsAllWithDiffType(context);
                }
            }
        }
        default:
            ThrowInfo(UnexpectedError,
                      "unsupported json contains type {}",
                      proto::plan::JSONContainsExpr_JSONOp_Name(expr_->op_));
    }
}

template <typename ExprValueType>
VectorPtr
PhyJsonContainsFilterExpr::ExecArrayContains(EvalCtx& context) {
    if (expr_->column_.element_level_) {
        return ExecArrayContainsImpl<ArrayValueView, ExprValueType, true>(
            context);
    }
    return ExecArrayContainsImpl<ArrayView, ExprValueType, false>(context);
}

template <typename ArrayType, typename ExprValueType, bool ElementLevel>
VectorPtr
PhyJsonContainsFilterExpr::ExecArrayContainsImpl(EvalCtx& context) {
    using GetType =
        std::conditional_t<std::is_same_v<ExprValueType, std::string>,
                           std::string_view,
                           ExprValueType>;

    // Typed cached set used directly inside the array scan loop, mirroring
    // the pattern in ExecArrayContainsAll. Skips the MultiElement variant
    // round-trip, virtual dispatch and runtime type checks in In().
    //   string: owning std::string set with transparent hash, so string_view
    //           lookups are zero-copy and the set never holds dangling views.
    //   bool:   std::unordered_set<bool>, since std::hash<bool> is safe.
    //           ankerl::unordered_dense::set<bool> is avoided for the same
    //           reason as SetElement<bool> in Element.h (wyhash 8-byte read).
    //   other:  ankerl::unordered_dense::set<ExprValueType>.
    using TypedSet = std::conditional_t<
        std::is_same_v<ExprValueType, std::string>,
        ankerl::unordered_dense::set<std::string, StringHash, std::equal_to<>>,
        std::conditional_t<std::is_same_v<ExprValueType, bool>,
                           std::unordered_set<bool>,
                           ankerl::unordered_dense::set<ExprValueType>>>;

    AssertInfo(expr_->column_.element_level_ == ElementLevel,
               "ARRAY contains element-level mismatch: plan={}, executor={}",
               expr_->column_.element_level_,
               ElementLevel);
    AssertInfo(expr_->column_.nested_path_.size() == 0,
               "[ExecArrayContains]nested path must be null");

    if (!arg_inited_) {
        auto elements = std::make_shared<TypedSet>();
        elements->max_load_factor(0.5f);
        for (const auto& val : expr_->vals_) {
            elements->insert(GetValueWithCastNumber<ExprValueType>(val));
        }
        arg_cached_set_ = elements;
        arg_inited_ = true;
    }
    auto elements = std::static_pointer_cast<TypedSet>(arg_cached_set_);

    return EvalKernel<ArrayType>(
        context,
        ArrayContainsAnyKernel<ArrayType, GetType, TypedSet>{elements.get()},
        ElementLevel);
}

template <typename ExprValueType>
VectorPtr
PhyJsonContainsFilterExpr::ExecJsonContains(EvalCtx& context) {
    using GetType =
        std::conditional_t<std::is_same_v<ExprValueType, std::string>,
                           std::string_view,
                           ExprValueType>;
    if (!has_offset_input_ && exec_path_ == ExprExecPath::JsonStats) {
        milvus::ScopedTimer timer("json_contains_by_stats", [this](double us) {
            json_filter_stats_latency_us_ += us;
        });
        return ExecJsonContainsByStats<ExprValueType>();
    }

    milvus::ScopedTimer timer("json_contains_bruteforce", [this](double us) {
        json_filter_bruteforce_latency_us_ += us;
    });

    if (!arg_inited_) {
        arg_set_ = std::make_shared<SetElement<GetType>>(expr_->vals_);
        arg_inited_ = true;
    }
    return EvalKernel<milvus::Json>(
        context,
        JsonContainsAnyKernel<GetType>{
            milvus::Json::pointer(expr_->column_.nested_path_), arg_set_.get()},
        false);
}

// JsonStats compute shared by the six json_contains variants: one ARRAY
// shredded column at the pointer, then shared BSON data unless every row's
// ARRAY value is shredded. Factories run only on a cache miss.
template <typename MakeShreddingExecutor, typename MakeSharedExecutor>
VectorPtr
PhyJsonContainsFilterExpr::ScanArrayPathByStats(
    std::string_view shredding_timer_name,
    std::string_view shared_timer_name,
    MakeShreddingExecutor&& make_shredding_executor,
    MakeSharedExecutor&& make_shared_executor) {
    return EvalByStats([&](TriStateOut out) {
        auto pointer = milvus::Json::pointer(expr_->column_.nested_path_);
        auto* segment = dynamic_cast<const segcore::SegmentSealed*>(segment_);
        auto index = segment->GetJsonStats(op_ctx_, expr_->column_.field_id_);
        Assert(index.get() != nullptr);
        {
            milvus::ScopedTimer timer(shredding_timer_name, [this](double us) {
                json_stats_shredding_latency_us_ += us;
            });
            auto target_field = index->GetShreddingField(
                pointer, milvus::index::JSONType::ARRAY);
            if (!target_field.empty()) {
                OrShreddingColumn<std::string_view>(
                    *index.get(), target_field, make_shredding_executor(), out);
            }
        }
        if (!index->HasAllShreddingFields(pointer,
                                          {milvus::index::JSONType::ARRAY})) {
            milvus::ScopedTimer timer(shared_timer_name, [this](double us) {
                json_stats_shared_latency_us_ += us;
            });
            index->ExecuteForSharedData(
                op_ctx_, bson_index_, pointer, make_shared_executor(out));
        }
    });
}

template <typename ExprValueType>
VectorPtr
PhyJsonContainsFilterExpr::ExecJsonContainsByStats() {
    using GetType =
        std::conditional_t<std::is_same_v<ExprValueType, std::string>,
                           std::string_view,
                           ExprValueType>;
    if (!arg_inited_) {
        arg_set_ = std::make_shared<SetElement<GetType>>(expr_->vals_);
        arg_inited_ = true;
    }
    return ScanArrayPathByStats(
        "json_contains_stats_shredding_data",
        "json_contains_stats_shared_data",
        [this]() {
            return ShreddingArrayBsonContainsAnyExecutor<GetType>(arg_set_);
        },
        [this](TriStateOut out) {
            return [this, out](milvus::BsonView bson,
                               uint32_t row_offset,
                               uint32_t value_offset) mutable {
                auto val = bson.ParseAsArrayAtOffset(value_offset);
                if (!val.has_value()) {
                    return;
                }
                out.known[row_offset] = true;
                for (const auto& element : val.value()) {
                    if constexpr (std::is_same_v<GetType, int64_t> ||
                                  std::is_same_v<GetType, double>) {
                        auto value =
                            GetBsonNumberExact<GetType>(element.get_value());
                        if (value.has_value() && this->arg_set_->In(*value)) {
                            out.match[row_offset] = true;
                            return;
                        }
                    } else {
                        auto value =
                            milvus::BsonView::GetValueFromBsonView<GetType>(
                                element.get_value());
                        if (value.has_value() &&
                            this->arg_set_->In(value.value())) {
                            out.match[row_offset] = true;
                            return;
                        }
                    }
                }
            };
        });
}

VectorPtr
PhyJsonContainsFilterExpr::ExecJsonContainsArray(EvalCtx& context) {
    if (!has_offset_input_ && exec_path_ == ExprExecPath::JsonStats) {
        milvus::ScopedTimer timer(
            "json_contains_array_by_stats",
            [this](double us) { json_filter_stats_latency_us_ += us; });
        return ExecJsonContainsArrayByStats();
    }

    milvus::ScopedTimer timer(
        "json_contains_array_bruteforce",
        [this](double us) { json_filter_bruteforce_latency_us_ += us; });

    if (!arg_inited_) {
        auto elements = std::make_shared<std::vector<proto::plan::Array>>();
        for (auto const& element : expr_->vals_) {
            elements->emplace_back(
                GetValueFromProto<proto::plan::Array>(element));
        }
        arg_cached_set_ = elements;
        arg_inited_ = true;
    }
    auto elements = std::static_pointer_cast<std::vector<proto::plan::Array>>(
        arg_cached_set_);
    return EvalKernel<milvus::Json>(
        context,
        JsonContainsArrayKernel{
            milvus::Json::pointer(expr_->column_.nested_path_), elements.get()},
        false);
}

VectorPtr
PhyJsonContainsFilterExpr::ExecJsonContainsArrayByStats() {
    std::vector<proto::plan::Array> elements;
    elements.reserve(expr_->vals_.size());
    for (auto const& element : expr_->vals_) {
        elements.emplace_back(GetValueFromProto<proto::plan::Array>(element));
    }
    return ScanArrayPathByStats(
        "json_contains_array_stats_shredding_data",
        "json_contains_array_stats_shared_data",
        [&elements]() {
            return ShreddingArrayBsonContainsArrayExecutor(elements);
        },
        [&elements](TriStateOut out) {
            return [&elements, out](milvus::BsonView bson,
                                    uint32_t row_offset,
                                    uint32_t value_offset) mutable {
                auto array = bson.ParseAsArrayAtOffset(value_offset);
                if (!array.has_value()) {
                    return;
                }
                out.known[row_offset] = true;
                for (const auto& sub_value : array.value()) {
                    auto sub_array = milvus::BsonView::GetValueFromBsonView<
                        milvus::bson::array_view>(sub_value.get_value());
                    if (!sub_array.has_value()) {
                        continue;
                    }
                    for (const auto& element : elements) {
                        if (CompareTwoJsonArray(sub_array.value(), element)) {
                            out.match[row_offset] = true;
                            return;
                        }
                    }
                }
                out.match[row_offset] = false;
            };
        });
}

template <typename ExprValueType>
VectorPtr
PhyJsonContainsFilterExpr::ExecArrayContainsAll(EvalCtx& context) {
    if (expr_->column_.element_level_) {
        return ExecArrayContainsAllImpl<ArrayValueView, ExprValueType, true>(
            context);
    }
    return ExecArrayContainsAllImpl<ArrayView, ExprValueType, false>(context);
}

template <typename ArrayType, typename ExprValueType, bool ElementLevel>
VectorPtr
PhyJsonContainsFilterExpr::ExecArrayContainsAllImpl(EvalCtx& context) {
    using GetType =
        std::conditional_t<std::is_same_v<ExprValueType, std::string>,
                           std::string_view,
                           ExprValueType>;
    AssertInfo(expr_->column_.nested_path_.size() == 0,
               "[ExecArrayContainsAll]nested path must be null");
    AssertInfo(expr_->column_.element_level_ == ElementLevel,
               "ARRAY contains-all element-level mismatch: plan={}, "
               "executor={}",
               expr_->column_.element_level_,
               ElementLevel);

    if (!arg_inited_) {
        auto elements = std::make_shared<std::set<GetType>>();
        for (auto const& element : expr_->vals_) {
            elements->insert(GetValueWithCastNumber<GetType>(element));
        }
        arg_cached_set_ = elements;
        arg_inited_ = true;
    }
    auto elements =
        std::static_pointer_cast<std::set<GetType>>(arg_cached_set_);
    return EvalKernel<ArrayType>(
        context,
        ArrayContainsAllKernel<ArrayType, GetType>(*elements),
        ElementLevel);
}

template <typename ExprValueType>
VectorPtr
PhyJsonContainsFilterExpr::ExecJsonContainsAll(EvalCtx& context) {
    using GetType =
        std::conditional_t<std::is_same_v<ExprValueType, std::string>,
                           std::string_view,
                           ExprValueType>;
    if (!has_offset_input_ && exec_path_ == ExprExecPath::JsonStats) {
        milvus::ScopedTimer timer(
            "json_contains_all_by_stats",
            [this](double us) { json_filter_stats_latency_us_ += us; });
        return ExecJsonContainsAllByStats<ExprValueType>();
    }

    milvus::ScopedTimer timer(
        "json_contains_all_bruteforce",
        [this](double us) { json_filter_bruteforce_latency_us_ += us; });

    if (!arg_inited_) {
        auto elements = std::make_shared<std::set<GetType>>();
        for (auto const& element : expr_->vals_) {
            elements->insert(GetValueFromProto<GetType>(element));
        }
        arg_cached_set_ = elements;
        arg_inited_ = true;
    }
    auto elements =
        std::static_pointer_cast<std::set<GetType>>(arg_cached_set_);
    return EvalKernel<milvus::Json>(
        context,
        JsonContainsAllKernel<GetType>(
            milvus::Json::pointer(expr_->column_.nested_path_), *elements),
        false);
}

template <typename ExprValueType>
VectorPtr
PhyJsonContainsFilterExpr::ExecJsonContainsAllByStats() {
    using GetType =
        std::conditional_t<std::is_same_v<ExprValueType, std::string>,
                           std::string_view,
                           ExprValueType>;
    if (!arg_inited_) {
        auto elements = std::make_shared<std::set<GetType>>();
        for (auto const& element : expr_->vals_) {
            elements->insert(GetValueFromProto<GetType>(element));
        }
        arg_cached_set_ = elements;
        arg_inited_ = true;
    }
    auto elements =
        std::static_pointer_cast<std::set<GetType>>(arg_cached_set_);
    return ScanArrayPathByStats(
        "json_contains_all_stats_shredding_data",
        "json_contains_all_stats_shared_data",
        [&elements]() {
            return ShreddingArrayBsonContainsAllExecutor<GetType>(*elements);
        },
        [&elements](TriStateOut out) {
            ContainsAllMatcher<GetType> matcher(*elements);
            std::vector<uint64_t> found_large(
                matcher.use_small() ? 0 : matcher.num_words());
            return [out,
                    matcher = std::move(matcher),
                    found_large = std::move(found_large)](
                       milvus::BsonView bson,
                       uint32_t row_offset,
                       uint32_t value_offset) mutable {
                auto val = bson.ParseAsArrayAtOffset(value_offset);
                if (!val.has_value()) {
                    return;
                }
                out.known[row_offset] = true;
                auto element_value =
                    [](const auto& element) -> std::optional<GetType> {
                    if constexpr (std::is_same_v<GetType, int64_t> ||
                                  std::is_same_v<GetType, double>) {
                        return GetBsonNumberExact<GetType>(element.get_value());
                    } else {
                        return milvus::BsonView::GetValueFromBsonView<GetType>(
                            element.get_value());
                    }
                };
                if (matcher.use_small()) {
                    uint64_t found = 0;
                    for (const auto& element : val.value()) {
                        auto value = element_value(element);
                        if (!value.has_value()) {
                            continue;
                        }
                        if (matcher.set_if_found(value.value(), found)) {
                            out.match[row_offset] = true;
                            return;
                        }
                    }
                    out.match[row_offset] = (found == matcher.full_mask());
                } else {
                    std::fill(found_large.begin(), found_large.end(), 0);
                    size_t remaining = matcher.target_count();
                    for (const auto& element : val.value()) {
                        auto value = element_value(element);
                        if (!value.has_value()) {
                            continue;
                        }
                        if (matcher.set_if_found(
                                value.value(), found_large, remaining)) {
                            out.match[row_offset] = true;
                            return;
                        }
                    }
                    out.match[row_offset] = (remaining == 0);
                }
            };
        });
}

VectorPtr
PhyJsonContainsFilterExpr::ExecJsonContainsAllWithDiffType(EvalCtx& context) {
    if (!has_offset_input_ && exec_path_ == ExprExecPath::JsonStats) {
        milvus::ScopedTimer timer(
            "json_contains_all_difftype_by_stats",
            [this](double us) { json_filter_stats_latency_us_ += us; });
        return ExecJsonContainsAllWithDiffTypeByStats();
    }

    milvus::ScopedTimer timer(
        "json_contains_all_difftype_bruteforce",
        [this](double us) { json_filter_bruteforce_latency_us_ += us; });

    const auto& elements = expr_->vals_;
    std::unordered_set<int> elements_index;
    for (int i = 0; i < static_cast<int>(elements.size()); i++) {
        elements_index.insert(i);
    }
    return EvalKernel<milvus::Json>(
        context,
        JsonContainsAllWithDiffTypeKernel{
            milvus::Json::pointer(expr_->column_.nested_path_),
            &elements,
            std::move(elements_index)},
        false);
}

VectorPtr
PhyJsonContainsFilterExpr::ExecJsonContainsAllWithDiffTypeByStats() {
    const auto& elements = expr_->vals_;
    std::set<int> elements_index;
    for (int i = 0; i < static_cast<int>(elements.size()); i++) {
        elements_index.insert(i);
    }
    return ScanArrayPathByStats(
        "json_contains_all_difftype_stats_shredding_data",
        "json_contains_all_difftype_stats_shared_data",
        [&elements, &elements_index]() {
            return ShreddingArrayBsonContainsAllWithDiffTypeExecutor(
                elements, elements_index);
        },
        [&elements, &elements_index](TriStateOut out) {
            return [&elements, &elements_index, out](
                       milvus::BsonView bson,
                       uint32_t row_offset,
                       uint32_t value_offset) mutable {
                std::set<int> tmp_elements_index(elements_index);
                auto array = bson.ParseAsArrayAtOffset(value_offset);
                if (!array.has_value()) {
                    return;
                }
                out.known[row_offset] = true;
                for (const auto& sub_value : array.value()) {
                    int i = -1;
                    for (auto& element : elements) {
                        i++;
                        switch (element.val_case()) {
                            case proto::plan::GenericValue::kBoolVal: {
                                auto val =
                                    milvus::BsonView::GetValueFromBsonView<
                                        bool>(sub_value.get_value());
                                if (!val.has_value()) {
                                    continue;
                                }
                                if (val.value() == element.bool_val()) {
                                    tmp_elements_index.erase(i);
                                }
                                break;
                            }
                            case proto::plan::GenericValue::kInt64Val:
                            case proto::plan::GenericValue::kFloatVal: {
                                auto comparison = CompareBsonNumberToBound(
                                    sub_value.get_value(), element);
                                if (comparison.has_value() &&
                                    *comparison == 0) {
                                    tmp_elements_index.erase(i);
                                }
                                break;
                            }
                            case proto::plan::GenericValue::kStringVal: {
                                auto val =
                                    milvus::BsonView::GetValueFromBsonView<
                                        std::string>(sub_value.get_value());
                                if (!val.has_value()) {
                                    continue;
                                }
                                if (val.value() == element.string_val()) {
                                    tmp_elements_index.erase(i);
                                }
                                break;
                            }
                            case proto::plan::GenericValue::kArrayVal: {
                                auto val =
                                    milvus::BsonView::GetValueFromBsonView<
                                        milvus::bson::array_view>(
                                        sub_value.get_value());
                                if (!val.has_value()) {
                                    continue;
                                }
                                if (CompareTwoJsonArray(val.value(),
                                                        element.array_val())) {
                                    tmp_elements_index.erase(i);
                                }
                                break;
                            }
                            default:
                                ThrowInfo(UnexpectedError,
                                          "unsupported data type {}",
                                          element.val_case());
                        }
                        if (tmp_elements_index.size() == 0) {
                            out.match[row_offset] = true;
                            return;
                        }
                    }
                    if (tmp_elements_index.size() == 0) {
                        out.match[row_offset] = true;
                        return;
                    }
                }
                out.match[row_offset] = tmp_elements_index.size() == 0;
            };
        });
}

VectorPtr
PhyJsonContainsFilterExpr::ExecJsonContainsAllArray(EvalCtx& context) {
    if (!has_offset_input_ && exec_path_ == ExprExecPath::JsonStats) {
        milvus::ScopedTimer timer(
            "json_contains_all_array_by_stats",
            [this](double us) { json_filter_stats_latency_us_ += us; });
        return ExecJsonContainsAllArrayByStats();
    }

    milvus::ScopedTimer timer(
        "json_contains_all_array_bruteforce",
        [this](double us) { json_filter_bruteforce_latency_us_ += us; });

    std::vector<proto::plan::Array> elements;
    elements.reserve(expr_->vals_.size());
    for (auto const& element : expr_->vals_) {
        elements.emplace_back(GetValueFromProto<proto::plan::Array>(element));
    }
    return EvalKernel<milvus::Json>(
        context,
        JsonContainsAllArrayKernel{
            milvus::Json::pointer(expr_->column_.nested_path_),
            std::move(elements)},
        false);
}

VectorPtr
PhyJsonContainsFilterExpr::ExecJsonContainsAllArrayByStats() {
    std::vector<proto::plan::Array> elements;
    elements.reserve(expr_->vals_.size());
    for (auto const& element : expr_->vals_) {
        elements.emplace_back(GetValueFromProto<proto::plan::Array>(element));
    }
    return ScanArrayPathByStats(
        "json_contains_all_array_stats_shredding_data",
        "json_contains_all_array_stats_shared_data",
        [&elements]() {
            return ShreddingArrayBsonContainsAllArrayExecutor(elements);
        },
        [&elements](TriStateOut out) {
            return [&elements, out](milvus::BsonView bson,
                                    uint32_t row_offset,
                                    uint32_t value_offset) mutable {
                auto array = bson.ParseAsArrayAtOffset(value_offset);
                if (!array.has_value()) {
                    return;
                }
                out.known[row_offset] = true;
                std::set<int> exist_elements_index;
                for (const auto& sub_value : array.value()) {
                    auto sub_array = milvus::BsonView::GetValueFromBsonView<
                        milvus::bson::array_view>(sub_value.get_value());
                    if (!sub_array.has_value()) {
                        continue;
                    }
                    for (int index = 0; index < elements.size(); ++index) {
                        if (CompareTwoJsonArray(sub_array.value(),
                                                elements[index])) {
                            exist_elements_index.insert(index);
                        }
                    }
                    if (exist_elements_index.size() == elements.size()) {
                        out.match[row_offset] = true;
                        return;
                    }
                }
                out.match[row_offset] =
                    exist_elements_index.size() == elements.size();
            };
        });
}

VectorPtr
PhyJsonContainsFilterExpr::ExecJsonContainsWithDiffType(EvalCtx& context) {
    if (!has_offset_input_ && exec_path_ == ExprExecPath::JsonStats) {
        milvus::ScopedTimer timer(
            "json_contains_difftype_by_stats",
            [this](double us) { json_filter_stats_latency_us_ += us; });
        return ExecJsonContainsWithDiffTypeByStats();
    }

    milvus::ScopedTimer timer(
        "json_contains_difftype_bruteforce",
        [this](double us) { json_filter_bruteforce_latency_us_ += us; });

    return EvalKernel<milvus::Json>(
        context,
        JsonContainsAnyWithDiffTypeKernel{
            milvus::Json::pointer(expr_->column_.nested_path_), &expr_->vals_},
        false);
}

VectorPtr
PhyJsonContainsFilterExpr::ExecJsonContainsWithDiffTypeByStats() {
    const auto& elements = expr_->vals_;
    return ScanArrayPathByStats(
        "json_contains_difftype_stats_shredding_data",
        "json_contains_difftype_stats_shared_data",
        [&elements]() {
            return ShreddingArrayBsonContainsAnyWithDiffTypeExecutor(elements);
        },
        [&elements](TriStateOut out) {
            return [&elements, out](milvus::BsonView bson,
                                    uint32_t row_offset,
                                    uint32_t value_offset) mutable {
                auto array = bson.ParseAsArrayAtOffset(value_offset);
                if (!array.has_value()) {
                    return;
                }
                out.known[row_offset] = true;
                for (const auto& sub_value : array.value()) {
                    for (auto const& element : elements) {
                        switch (element.val_case()) {
                            case proto::plan::GenericValue::kBoolVal: {
                                auto val =
                                    milvus::BsonView::GetValueFromBsonView<
                                        bool>(sub_value.get_value());
                                if (!val.has_value()) {
                                    continue;
                                }
                                if (val.value() == element.bool_val()) {
                                    out.match[row_offset] = true;
                                    return;
                                }
                                break;
                            }
                            case proto::plan::GenericValue::kInt64Val:
                            case proto::plan::GenericValue::kFloatVal: {
                                auto comparison = CompareBsonNumberToBound(
                                    sub_value.get_value(), element);
                                if (comparison.has_value() &&
                                    *comparison == 0) {
                                    out.match[row_offset] = true;
                                    return;
                                }
                                break;
                            }
                            case proto::plan::GenericValue::kStringVal: {
                                auto val =
                                    milvus::BsonView::GetValueFromBsonView<
                                        std::string>(sub_value.get_value());
                                if (!val.has_value()) {
                                    continue;
                                }
                                if (val.value() == element.string_val()) {
                                    out.match[row_offset] = true;
                                    return;
                                }
                                break;
                            }
                            case proto::plan::GenericValue::kArrayVal: {
                                auto val =
                                    milvus::BsonView::GetValueFromBsonView<
                                        milvus::bson::array_view>(
                                        sub_value.get_value());
                                if (!val.has_value()) {
                                    continue;
                                }
                                if (CompareTwoJsonArray(val.value(),
                                                        element.array_val())) {
                                    out.match[row_offset] = true;
                                    return;
                                }
                                break;
                            }
                            default:
                                ThrowInfo(UnexpectedError,
                                          "unsupported data type {}",
                                          element.val_case());
                        }
                    }
                }
            };
        });
}

VectorPtr
PhyJsonContainsFilterExpr::EvalArrayContainsForIndexSegment(
    DataType data_type) {
    switch (data_type) {
        case DataType::BOOL: {
            return ExecArrayContainsForIndexSegmentImpl<bool>();
        }
        case DataType::INT8: {
            return ExecArrayContainsForIndexSegmentImpl<int8_t>();
        }
        case DataType::INT16: {
            return ExecArrayContainsForIndexSegmentImpl<int16_t>();
        }
        case DataType::INT32: {
            return ExecArrayContainsForIndexSegmentImpl<int32_t>();
        }
        case DataType::INT64: {
            return ExecArrayContainsForIndexSegmentImpl<int64_t>();
        }
        case DataType::FLOAT: {
            return ExecArrayContainsForIndexSegmentImpl<float>();
        }
        case DataType::DOUBLE: {
            return ExecArrayContainsForIndexSegmentImpl<double>();
        }
        case DataType::VARCHAR:
        case DataType::STRING: {
            return ExecArrayContainsForIndexSegmentImpl<std::string>();
        }
        default:
            ThrowInfo(UnexpectedError,
                      fmt::format("unsupported data type for "
                                  "ExecArrayContainsForIndexSegmentImpl: {}",
                                  expr_->column_.element_type_));
    }
}

template <typename ExprValueType>
VectorPtr
PhyJsonContainsFilterExpr::ExecArrayContainsForIndexSegmentImpl() {
    typedef std::conditional_t<std::is_same_v<ExprValueType, std::string_view>,
                               std::string,
                               ExprValueType>
        GetType;
    using Index = index::ScalarIndex<GetType>;
    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    std::unordered_set<GetType> elements;
    for (auto const& element : expr_->vals_) {
        elements.insert(GetValueWithCastNumber<GetType>(element));
    }
    boost::container::vector<GetType> elems(elements.begin(), elements.end());

    // Get array offsets for nested index (needed for element-to-row conversion)
    auto array_offsets = segment_->GetArrayOffsets(expr_->column_.field_id_);

    auto execute_sub_batch =
        [this, &array_offsets](
            Index* index_ptr,
            const boost::container::vector<GetType>& vals) -> TargetBitmap {
        // Query helper: for nested index, convert element-level to row-level
        auto query_in = [&](size_t n, const GetType* data) -> TargetBitmap {
            auto element_bitset = index_ptr->In(n, data);
            if (!index_ptr->IsNestedIndex()) {
                return element_bitset;
            }
            AssertInfo(array_offsets != nullptr,
                       "array offsets not found for field {}",
                       expr_->column_.field_id_.get());
            return array_offsets->ForEachRowElementRange(
                [&element_bitset](int32_t elem_start, int32_t elem_end) {
                    for (int32_t i = elem_start; i < elem_end; ++i) {
                        if (element_bitset[i]) {
                            return true;
                        }
                    }
                    return false;
                },
                0,
                active_count_);
        };

        switch (expr_->op_) {
            case proto::plan::JSONContainsExpr_JSONOp_Contains:
            case proto::plan::JSONContainsExpr_JSONOp_ContainsAny:
                return query_in(vals.size(), vals.data());

            case proto::plan::JSONContainsExpr_JSONOp_ContainsAll: {
                TargetBitmap result(active_count_);
                result.set();
                for (size_t i = 0; i < vals.size(); i++) {
                    result &= query_in(1, &vals[i]);
                }
                return result;
            }
            default:
                ThrowInfo(
                    UnexpectedError,
                    "unsupported array contains type {}",
                    proto::plan::JSONContainsExpr_JSONOp_Name(expr_->op_));
        }
    };

    // Use WithRowLevel version since func handles element-to-row conversion for nested index
    auto validity_mode = field_type_ == DataType::JSON
                             ? IndexValidityMode::JsonExactPath
                             : IndexValidityMode::Default;
    auto res = ProcessIndexChunksWithRowLevel<GetType>(
        execute_sub_batch, validity_mode, elems);
    AssertInfo(res->size() == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               res->size(),
               real_batch_size);
    return res;
}

}  //namespace exec
}  // namespace milvus
