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

#include <simdjson.h>
#include <algorithm>
#include <cmath>
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

// Replaces per-row std::set copy with a value->bit-index map built once.
// For <= 64 targets uses uint64_t bitmask (zero heap alloc per row).
// For > 64 targets uses vector<uint64_t> dynamic bitset.
template <typename T>
class ContainsAllMatcher {
 public:
    explicit ContainsAllMatcher(const std::set<T>& targets) {
        target_count_ = targets.size();
        use_small_ = (target_count_ <= 64);
        uint32_t idx = 0;
        for (const auto& t : targets) {
            value_to_bit_[t] = idx++;
        }
        if (use_small_) {
            full_mask_ = (target_count_ == 64)
                             ? ~uint64_t(0)
                             : (uint64_t(1) << target_count_) - 1;
        } else {
            num_words_ = (target_count_ + 63) / 64;
        }
    }

    // Small path: look up a value and set its bit. Returns true when all
    // targets have been found.
    bool
    set_if_found(const T& val, uint64_t& found) const {
        auto it = value_to_bit_.find(val);
        if (it != value_to_bit_.end()) {
            found |= (uint64_t(1) << it->second);
            return found == full_mask_;
        }
        return false;
    }

    // Large path: returns true when all targets found.
    bool
    set_if_found(const T& val,
                 std::vector<uint64_t>& found,
                 size_t& remaining) const {
        auto it = value_to_bit_.find(val);
        if (it != value_to_bit_.end()) {
            uint32_t idx = it->second;
            uint64_t bit = uint64_t(1) << (idx % 64);
            uint64_t& word = found[idx / 64];
            if (!(word & bit)) {
                word |= bit;
                return --remaining == 0;
            }
        }
        return false;
    }

    bool
    use_small() const {
        return use_small_;
    }
    size_t
    target_count() const {
        return target_count_;
    }
    uint64_t
    full_mask() const {
        return full_mask_;
    }
    size_t
    num_words() const {
        return num_words_;
    }

 private:
    ankerl::unordered_dense::map<T, uint32_t> value_to_bit_;
    size_t target_count_{0};
    bool use_small_{true};
    uint64_t full_mask_{0};
    size_t num_words_{0};
};

void
PhyJsonContainsFilterExpr::Eval(EvalCtx& context, VectorPtr& result) {
    WaitPrefetch();
    tracer::AutoSpan span(
        "PhyJsonContainsFilterExpr::Eval", tracer::GetRootSpan(), true);
    span.GetSpan()->SetAttribute("data_type",
                                 static_cast<int>(expr_->column_.data_type_));
    span.GetSpan()->SetAttribute("json_filter_expr_type", "json_contains");

    auto input = context.get_offset_input();
    SetHasOffsetInput((input != nullptr));
    if (expr_->vals_.empty()) {
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
            const auto has_array_literal =
                std::any_of(expr_->vals_.begin(),
                            expr_->vals_.end(),
                            [](const auto& value) {
                                return value.val_case() ==
                                       proto::plan::GenericValue::kArrayVal;
                            });
            if (exec_path_ == ExprExecPath::ScalarIndex && expr_->same_type_ &&
                !has_array_literal && !has_offset_input_) {
                const auto has_unsafe_int_literal =
                    std::any_of(expr_->vals_.begin(),
                                expr_->vals_.end(),
                                [this](const auto& value) {
                                    return value.has_int64_val() &&
                                           !IsInt64SafeForJsonDoubleIndex(
                                               value.int64_val());
                                });
                const auto all_int_literals = std::all_of(
                    expr_->vals_.begin(),
                    expr_->vals_.end(),
                    [](const auto& value) { return value.has_int64_val(); });
                if (all_int_literals && PinnedJsonIndexIsFlat()) {
                    result = EvalArrayContainsForIndexSegment(DataType::INT64);
                } else if (has_unsafe_int_literal) {
                    result = EvalJsonContainsForDataSegment(context);
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
            ThrowInfo(DataTypeInvalid,
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
                        ThrowInfo(DataTypeInvalid,
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
                            ThrowInfo(DataTypeInvalid,
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
                        ThrowInfo(DataTypeInvalid,
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
                            ThrowInfo(DataTypeInvalid,
                                      "unsupported data type:{}",
                                      val_type);
                    }
                } else {
                    return ExecJsonContainsAllWithDiffType(context);
                }
            }
        }
        default:
            ThrowInfo(ExprInvalid,
                      "unsupported json contains type {}",
                      proto::plan::JSONContainsExpr_JSONOp_Name(expr_->op_));
    }
}

template <typename ExprValueType>
VectorPtr
PhyJsonContainsFilterExpr::ExecArrayContains(EvalCtx& context) {
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

    auto* input = context.get_offset_input();
    const auto& bitmap_input = context.get_bitmap_input();
    auto real_batch_size =
        has_offset_input_ ? input->size() : GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }
    AssertInfo(expr_->column_.nested_path_.size() == 0,
               "[ExecArrayContains]nested path must be null");

    auto res_vec =
        std::make_shared<ColumnVector>(TargetBitmap(real_batch_size, false),
                                       TargetBitmap(real_batch_size, true));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);

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

    int processed_cursor = 0;
    auto execute_sub_batch =
        [&processed_cursor, &
         bitmap_input ]<FilterType filter_type = FilterType::sequential>(
            const milvus::ArrayView* data,
            const bool* valid_data,
            const int32_t* offsets,
            const int size,
            TargetBitmapView res,
            TargetBitmapView valid_res,
            const TypedSet& elements) {
        // If data is nullptr, this chunk was skipped by SkipIndex.
        // We only need to update processed_cursor for bitmap_input indexing.
        if (data == nullptr) {
            processed_cursor += size;
            return;
        }
        auto executor = [&](size_t i) {
            const auto& array = data[i];
            for (int j = 0; j < array.length(); ++j) {
                if (elements.find(array.template get_data<GetType>(j)) !=
                    elements.end()) {
                    return true;
                }
            }
            return false;
        };
        bool has_bitmap_input = !bitmap_input.empty();
        for (int i = 0; i < size; ++i) {
            auto offset = i;
            if constexpr (filter_type == FilterType::random) {
                offset = (offsets) ? offsets[i] : i;
            }
            if (valid_data != nullptr && !valid_data[offset]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            if (has_bitmap_input && !bitmap_input[processed_cursor + i]) {
                continue;
            }
            res[i] = executor(offset);
        }
        processed_cursor += size;
    };

    int64_t processed_size;
    if (has_offset_input_) {
        processed_size =
            ProcessDataByOffsets<milvus::ArrayView>(execute_sub_batch,
                                                    std::nullptr_t{},
                                                    input,
                                                    res,
                                                    valid_res,
                                                    *elements);
    } else {
        processed_size = ProcessDataChunks<milvus::ArrayView>(
            execute_sub_batch, std::nullptr_t{}, res, valid_res, *elements);
    }
    AssertInfo(processed_size == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
}

template <typename ExprValueType>
VectorPtr
PhyJsonContainsFilterExpr::ExecJsonContains(EvalCtx& context) {
    using GetType =
        std::conditional_t<std::is_same_v<ExprValueType, std::string>,
                           std::string_view,
                           ExprValueType>;
    auto* input = context.get_offset_input();
    const auto& bitmap_input = context.get_bitmap_input();

    FieldId field_id = expr_->column_.field_id_;
    if (!has_offset_input_ && exec_path_ == ExprExecPath::JsonStats) {
        milvus::ScopedTimer timer("json_contains_by_stats", [this](double us) {
            json_filter_stats_latency_us_ += us;
        });
        return ExecJsonContainsByStats<ExprValueType>();
    }

    milvus::ScopedTimer timer("json_contains_bruteforce", [this](double us) {
        json_filter_bruteforce_latency_us_ += us;
    });

    auto real_batch_size =
        has_offset_input_ ? input->size() : GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    auto res_vec =
        std::make_shared<ColumnVector>(TargetBitmap(real_batch_size, false),
                                       TargetBitmap(real_batch_size, true));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);

    auto pointer = milvus::Json::pointer(expr_->column_.nested_path_);
    if (!arg_inited_) {
        arg_set_ = std::make_shared<SetElement<GetType>>(expr_->vals_);
        arg_inited_ = true;
    }

    size_t processed_cursor = 0;
    auto execute_sub_batch =
        [&processed_cursor, &
         bitmap_input ]<FilterType filter_type = FilterType::sequential>(
            const milvus::Json* data,
            const bool* valid_data,
            const int32_t* offsets,
            const int size,
            TargetBitmapView res,
            TargetBitmapView valid_res,
            const std::string& pointer,
            const std::shared_ptr<MultiElement>& elements) {
        // If data is nullptr, this chunk was skipped by SkipIndex.
        // We only need to update processed_cursor for bitmap_input indexing.
        if (data == nullptr) {
            processed_cursor += size;
            return;
        }
        auto executor = [&](size_t i) {
            auto doc = data[i].doc();
            auto array = doc.at_pointer(pointer).get_array();
            if (array.error()) {
                return std::make_pair(false, false);
            }
            for (auto&& it : array) {
                auto val = it.template get<GetType>();
                if (val.error()) {
                    if constexpr (std::is_same_v<GetType, int64_t>) {
                        auto double_val = it.template get<double>();
                        if (!double_val.error() &&
                            double_val.value() ==
                                std::floor(double_val.value())) {
                            if (elements->In(static_cast<int64_t>(
                                    double_val.value())) > 0) {
                                return std::make_pair(true, true);
                            }
                        }
                    }
                    continue;
                }
                if (elements->In(val.value()) > 0) {
                    return std::make_pair(true, true);
                }
            }
            return std::make_pair(true, false);
        };
        bool has_bitmap_input = !bitmap_input.empty();
        for (size_t i = 0; i < size; ++i) {
            auto offset = i;
            if constexpr (filter_type == FilterType::random) {
                offset = (offsets) ? offsets[i] : i;
            }
            if (valid_data != nullptr && !valid_data[offset]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            if (has_bitmap_input && !bitmap_input[processed_cursor + i]) {
                continue;
            }
            auto [valid, matched] = executor(offset);
            if (!valid) {
                res[i] = valid_res[i] = false;
                continue;
            }
            res[i] = matched;
        }
        processed_cursor += size;
    };

    int64_t processed_size;
    if (has_offset_input_) {
        processed_size = ProcessDataByOffsets<Json>(execute_sub_batch,
                                                    std::nullptr_t{},
                                                    input,
                                                    res,
                                                    valid_res,
                                                    pointer,
                                                    arg_set_);
    } else {
        processed_size = ProcessDataChunks<Json>(execute_sub_batch,
                                                 std::nullptr_t{},
                                                 res,
                                                 valid_res,
                                                 pointer,
                                                 arg_set_);
    }
    AssertInfo(processed_size == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
}

template <typename ExprValueType>
VectorPtr
PhyJsonContainsFilterExpr::ExecJsonContainsByStats() {
    using GetType =
        std::conditional_t<std::is_same_v<ExprValueType, std::string>,
                           std::string_view,
                           ExprValueType>;
    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }
    std::unordered_set<GetType> elements;
    auto pointer = milvus::Json::pointer(expr_->column_.nested_path_);
    if (!arg_inited_) {
        arg_set_ = std::make_shared<SetElement<GetType>>(expr_->vals_);
        arg_inited_ = true;
    }

    if (arg_set_->Empty()) {
        MoveCursor();
        return std::make_shared<ColumnVector>(
            TargetBitmap(real_batch_size, false),
            TargetBitmap(real_batch_size, true));
    }

    if (cached_index_chunk_id_ != 0 && TryCacheGet()) {
        // Cache hit — skip Stats computation.
    } else if (cached_index_chunk_id_ != 0 &&
               segment_->type() == SegmentType::Sealed) {
        auto cache_compute_start = CacheClock::now();
        auto* segment = dynamic_cast<const segcore::SegmentSealed*>(segment_);
        auto field_id = expr_->column_.field_id_;
        auto index = segment->GetJsonStats(op_ctx_, field_id);
        Assert(index.get() != nullptr);

        cached_index_chunk_res_ = std::make_shared<TargetBitmap>(active_count_);
        cached_index_chunk_valid_res_ =
            std::make_shared<TargetBitmap>(active_count_);
        TargetBitmapView res_view(*cached_index_chunk_res_);
        TargetBitmapView valid_res_view(*cached_index_chunk_valid_res_);
        // process shredding data for ARRAY type (non-shared)
        {
            milvus::ScopedTimer timer(
                "json_contains_stats_shredding_data",
                [this](double us) { json_stats_shredding_latency_us_ += us; });
            auto target_field = index->GetShreddingField(
                pointer, milvus::index::JSONType::ARRAY);
            if (!target_field.empty()) {
                TargetBitmap target_res(active_count_, false);
                TargetBitmapView target_res_view(target_res);
                TargetBitmap target_valid(active_count_, true);
                TargetBitmapView target_valid_view(target_valid);
                ShreddingArrayBsonContainsAnyExecutor<GetType> executor(
                    arg_set_);

                index->ExecutorForShreddingData<std::string_view>(
                    op_ctx_,
                    target_field,
                    executor,
                    nullptr,
                    target_res_view,
                    target_valid_view);
                res_view.inplace_or_with_count(target_res_view, active_count_);
                valid_res_view.inplace_or_with_count(target_valid_view,
                                                     active_count_);
            }
        }
        // process shared data
        auto shared_executor = [this, &res_view, &valid_res_view](
                                   milvus::BsonView bson,
                                   uint32_t row_offset,
                                   uint32_t value_offset) {
            auto val = bson.ParseAsArrayAtOffset(value_offset);

            if (!val.has_value()) {
                return;
            }
            valid_res_view[row_offset] = true;

            for (const auto& element : val.value()) {
                if constexpr (std::is_same_v<GetType, int64_t> ||
                              std::is_same_v<GetType, double>) {
                    auto value =
                        GetBsonNumberExact<GetType>(element.get_value());
                    if (value.has_value() && this->arg_set_->In(*value)) {
                        res_view[row_offset] = true;
                        return;
                    }
                } else {
                    auto value =
                        milvus::BsonView::GetValueFromBsonView<GetType>(
                            element.get_value());
                    if (value.has_value() &&
                        this->arg_set_->In(value.value())) {
                        res_view[row_offset] = true;
                        return;
                    }
                }
            }
        };
        {
            milvus::ScopedTimer timer(
                "json_contains_stats_shared_data",
                [this](double us) { json_stats_shared_latency_us_ += us; });
            index->ExecuteForSharedData(
                op_ctx_, bson_index_, pointer, shared_executor);
        }
        cached_index_chunk_id_ = 0;
        CachePut(CacheElapsedUs(cache_compute_start));
    }

    auto res = MoveOrSliceBitmap(*cached_index_chunk_res_,
                                 *cached_index_chunk_valid_res_,
                                 current_data_global_pos_,
                                 real_batch_size);
    MoveCursor();
    return res;
}

VectorPtr
PhyJsonContainsFilterExpr::ExecJsonContainsArray(EvalCtx& context) {
    auto* input = context.get_offset_input();
    const auto& bitmap_input = context.get_bitmap_input();
    FieldId field_id = expr_->column_.field_id_;
    if (!has_offset_input_ && exec_path_ == ExprExecPath::JsonStats) {
        milvus::ScopedTimer timer(
            "json_contains_array_by_stats",
            [this](double us) { json_filter_stats_latency_us_ += us; });
        return ExecJsonContainsArrayByStats();
    }

    milvus::ScopedTimer timer(
        "json_contains_array_bruteforce",
        [this](double us) { json_filter_bruteforce_latency_us_ += us; });

    auto real_batch_size =
        has_offset_input_ ? input->size() : GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    auto res_vec =
        std::make_shared<ColumnVector>(TargetBitmap(real_batch_size, false),
                                       TargetBitmap(real_batch_size, true));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);

    auto pointer = milvus::Json::pointer(expr_->column_.nested_path_);
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
    size_t processed_cursor = 0;
    auto execute_sub_batch =
        [&processed_cursor, &
         bitmap_input ]<FilterType filter_type = FilterType::sequential>(
            const milvus::Json* data,
            const bool* valid_data,
            const int32_t* offsets,
            const int size,
            TargetBitmapView res,
            TargetBitmapView valid_res,
            const std::string& pointer,
            const std::vector<proto::plan::Array>& elements) {
        // If data is nullptr, this chunk was skipped by SkipIndex.
        // We only need to update processed_cursor for bitmap_input indexing.
        if (data == nullptr) {
            processed_cursor += size;
            return;
        }
        auto executor = [&](size_t i) {
            auto doc = data[i].doc();
            auto array = doc.at_pointer(pointer).get_array();
            if (array.error()) {
                return std::make_pair(false, false);
            }
            for (auto&& it : array) {
                auto val = it.get_array();
                if (val.error()) {
                    continue;
                }
                std::vector<
                    simdjson::simdjson_result<simdjson::ondemand::value>>
                    json_array;
                json_array.reserve(val.count_elements());
                for (auto&& e : val) {
                    json_array.emplace_back(e);
                }
                for (auto const& element : elements) {
                    if (CompareTwoJsonArray(json_array, element)) {
                        return std::make_pair(true, true);
                    }
                }
            }
            return std::make_pair(true, false);
        };
        bool has_bitmap_input = !bitmap_input.empty();
        for (size_t i = 0; i < size; ++i) {
            auto offset = i;
            if constexpr (filter_type == FilterType::random) {
                offset = (offsets) ? offsets[i] : i;
            }
            if (valid_data != nullptr && !valid_data[offset]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            if (has_bitmap_input && !bitmap_input[processed_cursor + i]) {
                continue;
            }
            auto [valid, matched] = executor(offset);
            if (!valid) {
                res[i] = valid_res[i] = false;
                continue;
            }
            res[i] = matched;
        }
        processed_cursor += size;
    };

    int64_t processed_size;
    if (has_offset_input_) {
        processed_size = ProcessDataByOffsets<milvus::Json>(execute_sub_batch,
                                                            std::nullptr_t{},
                                                            input,
                                                            res,
                                                            valid_res,
                                                            pointer,
                                                            *elements);
    } else {
        processed_size = ProcessDataChunks<milvus::Json>(execute_sub_batch,
                                                         std::nullptr_t{},
                                                         res,
                                                         valid_res,
                                                         pointer,
                                                         *elements);
    }
    AssertInfo(processed_size == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
}

VectorPtr
PhyJsonContainsFilterExpr::ExecJsonContainsArrayByStats() {
    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }
    std::vector<proto::plan::Array> elements;
    elements.reserve(expr_->vals_.size());
    auto pointer = milvus::Json::pointer(expr_->column_.nested_path_);
    for (auto const& element : expr_->vals_) {
        elements.emplace_back(GetValueFromProto<proto::plan::Array>(element));
    }
    if (elements.empty()) {
        MoveCursor();
        return std::make_shared<ColumnVector>(
            TargetBitmap(real_batch_size, false),
            TargetBitmap(real_batch_size, true));
    }

    if (cached_index_chunk_id_ != 0 && TryCacheGet()) {
        // Cache hit — skip Stats computation.
    } else if (cached_index_chunk_id_ != 0 &&
               segment_->type() == SegmentType::Sealed) {
        auto cache_compute_start = CacheClock::now();
        auto* segment = dynamic_cast<const segcore::SegmentSealed*>(segment_);
        auto field_id = expr_->column_.field_id_;
        auto index = segment->GetJsonStats(op_ctx_, field_id);
        Assert(index.get() != nullptr);

        cached_index_chunk_res_ = std::make_shared<TargetBitmap>(active_count_);
        cached_index_chunk_valid_res_ =
            std::make_shared<TargetBitmap>(active_count_);
        TargetBitmapView res_view(*cached_index_chunk_res_);
        TargetBitmapView valid_res_view(*cached_index_chunk_valid_res_);

        // process shredding data for ARRAY type (non-shared)
        {
            milvus::ScopedTimer timer(
                "json_contains_array_stats_shredding_data",
                [this](double us) { json_stats_shredding_latency_us_ += us; });
            auto target_field = index->GetShreddingField(
                pointer, milvus::index::JSONType::ARRAY);
            if (!target_field.empty()) {
                TargetBitmap target_res(active_count_, false);
                TargetBitmapView target_res_view(target_res);
                TargetBitmap target_valid(active_count_, true);
                TargetBitmapView target_valid_view(target_valid);
                ShreddingArrayBsonContainsArrayExecutor executor(elements);
                index->ExecutorForShreddingData<std::string_view>(
                    op_ctx_,
                    target_field,
                    executor,
                    nullptr,
                    target_res_view,
                    target_valid_view);
                res_view.inplace_or_with_count(target_res_view, active_count_);
                valid_res_view.inplace_or_with_count(target_valid_view,
                                                     active_count_);
            }
        }

        auto shared_executor = [&elements, &res_view, &valid_res_view](
                                   milvus::BsonView bson,
                                   uint32_t row_offset,
                                   uint32_t value_offset) {
            auto array = bson.ParseAsArrayAtOffset(value_offset);

            if (!array.has_value()) {
                return;
            }
            valid_res_view[row_offset] = true;

            for (const auto& sub_value : array.value()) {
                auto sub_array = milvus::BsonView::GetValueFromBsonView<
                    milvus::bson::array_view>(sub_value.get_value());

                if (!sub_array.has_value())
                    continue;

                for (const auto& element : elements) {
                    if (CompareTwoJsonArray(sub_array.value(), element)) {
                        res_view[row_offset] = true;
                        return;
                    }
                }
            }
            res_view[row_offset] = false;
        };
        {
            milvus::ScopedTimer timer(
                "json_contains_array_stats_shared_data",
                [this](double us) { json_stats_shared_latency_us_ += us; });
            index->ExecuteForSharedData(
                op_ctx_, bson_index_, pointer, shared_executor);
        }
        cached_index_chunk_id_ = 0;
        CachePut(CacheElapsedUs(cache_compute_start));
    }

    auto res = MoveOrSliceBitmap(*cached_index_chunk_res_,
                                 *cached_index_chunk_valid_res_,
                                 current_data_global_pos_,
                                 real_batch_size);
    MoveCursor();
    return res;
}

template <typename ExprValueType>
VectorPtr
PhyJsonContainsFilterExpr::ExecArrayContainsAll(EvalCtx& context) {
    using GetType =
        std::conditional_t<std::is_same_v<ExprValueType, std::string>,
                           std::string_view,
                           ExprValueType>;
    auto* input = context.get_offset_input();
    const auto& bitmap_input = context.get_bitmap_input();
    AssertInfo(expr_->column_.nested_path_.size() == 0,
               "[ExecArrayContainsAll]nested path must be null");
    auto real_batch_size =
        has_offset_input_ ? input->size() : GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    auto res_vec =
        std::make_shared<ColumnVector>(TargetBitmap(real_batch_size, false),
                                       TargetBitmap(real_batch_size, true));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);

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
    int processed_cursor = 0;
    ContainsAllMatcher<GetType> matcher(*elements);
    std::vector<uint64_t> found_large(
        matcher.use_small() ? 0 : matcher.num_words());
    auto execute_sub_batch =
        [&processed_cursor, &bitmap_input, &matcher, &
         found_large ]<FilterType filter_type = FilterType::sequential>(
            const milvus::ArrayView* data,
            const bool* valid_data,
            const int32_t* offsets,
            const int size,
            TargetBitmapView res,
            TargetBitmapView valid_res,
            const std::set<GetType>& elements) {
        // If data is nullptr, this chunk was skipped by SkipIndex.
        // We only need to update processed_cursor for bitmap_input indexing.
        if (data == nullptr) {
            processed_cursor += size;
            return;
        }
        auto executor = [&](size_t i) {
            if (static_cast<size_t>(data[i].length()) <
                matcher.target_count()) {
                return false;
            }
            if (matcher.use_small()) {
                uint64_t found = 0;
                for (int j = 0; j < data[i].length(); ++j) {
                    if (matcher.set_if_found(
                            data[i].template get_data<GetType>(j), found)) {
                        return true;
                    }
                }
                return found == matcher.full_mask();
            } else {
                std::fill(found_large.begin(), found_large.end(), 0);
                size_t remaining = matcher.target_count();
                for (int j = 0; j < data[i].length(); ++j) {
                    if (matcher.set_if_found(
                            data[i].template get_data<GetType>(j),
                            found_large,
                            remaining)) {
                        return true;
                    }
                }
                return remaining == 0;
            }
        };
        bool has_bitmap_input = !bitmap_input.empty();
        for (int i = 0; i < size; ++i) {
            auto offset = i;
            if constexpr (filter_type == FilterType::random) {
                offset = (offsets) ? offsets[i] : i;
            }
            if (valid_data != nullptr && !valid_data[offset]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            if (has_bitmap_input && !bitmap_input[processed_cursor + i]) {
                continue;
            }
            res[i] = executor(offset);
        }
        processed_cursor += size;
    };
    int64_t processed_size;
    if (has_offset_input_) {
        processed_size =
            ProcessDataByOffsets<milvus::ArrayView>(execute_sub_batch,
                                                    std::nullptr_t{},
                                                    input,
                                                    res,
                                                    valid_res,
                                                    *elements);
    } else {
        processed_size = ProcessDataChunks<milvus::ArrayView>(
            execute_sub_batch, std::nullptr_t{}, res, valid_res, *elements);
    }
    AssertInfo(processed_size == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
}

template <typename ExprValueType>
VectorPtr
PhyJsonContainsFilterExpr::ExecJsonContainsAll(EvalCtx& context) {
    using GetType =
        std::conditional_t<std::is_same_v<ExprValueType, std::string>,
                           std::string_view,
                           ExprValueType>;
    auto* input = context.get_offset_input();
    const auto& bitmap_input = context.get_bitmap_input();

    FieldId field_id = expr_->column_.field_id_;
    if (!has_offset_input_ && exec_path_ == ExprExecPath::JsonStats) {
        milvus::ScopedTimer timer(
            "json_contains_all_by_stats",
            [this](double us) { json_filter_stats_latency_us_ += us; });
        return ExecJsonContainsAllByStats<ExprValueType>();
    }

    milvus::ScopedTimer timer(
        "json_contains_all_bruteforce",
        [this](double us) { json_filter_bruteforce_latency_us_ += us; });

    auto real_batch_size =
        has_offset_input_ ? input->size() : GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    auto res_vec =
        std::make_shared<ColumnVector>(TargetBitmap(real_batch_size, false),
                                       TargetBitmap(real_batch_size, true));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);

    auto pointer = milvus::Json::pointer(expr_->column_.nested_path_);
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
    int processed_cursor = 0;
    ContainsAllMatcher<GetType> matcher(*elements);
    std::vector<uint64_t> found_large(
        matcher.use_small() ? 0 : matcher.num_words());
    auto execute_sub_batch =
        [&processed_cursor, &bitmap_input, &matcher, &
         found_large ]<FilterType filter_type = FilterType::sequential>(
            const milvus::Json* data,
            const bool* valid_data,
            const int32_t* offsets,
            const int size,
            TargetBitmapView res,
            TargetBitmapView valid_res,
            const std::string& pointer,
            const std::set<GetType>& elements) {
        // If data is nullptr, this chunk was skipped by SkipIndex.
        // We only need to update processed_cursor for bitmap_input indexing.
        if (data == nullptr) {
            processed_cursor += size;
            return;
        }
        auto executor = [&](const size_t i) {
            auto doc = data[i].doc();
            auto array = doc.at_pointer(pointer).get_array();
            if (array.error()) {
                return std::make_pair(false, false);
            }
            if (matcher.use_small()) {
                uint64_t found = 0;
                for (auto&& it : array) {
                    auto val = it.template get<GetType>();
                    if (val.error()) {
                        if constexpr (std::is_same_v<GetType, int64_t>) {
                            auto double_val = it.template get<double>();
                            if (!double_val.error() &&
                                double_val.value() ==
                                    std::floor(double_val.value())) {
                                if (matcher.set_if_found(
                                        static_cast<int64_t>(
                                            double_val.value()),
                                        found)) {
                                    return std::make_pair(true, true);
                                }
                            }
                        }
                        continue;
                    }
                    if (matcher.set_if_found(val.value(), found)) {
                        return std::make_pair(true, true);
                    }
                }
                return std::make_pair(true, found == matcher.full_mask());
            } else {
                std::fill(found_large.begin(), found_large.end(), 0);
                size_t remaining = matcher.target_count();
                for (auto&& it : array) {
                    auto val = it.template get<GetType>();
                    if (val.error()) {
                        if constexpr (std::is_same_v<GetType, int64_t>) {
                            auto double_val = it.template get<double>();
                            if (!double_val.error() &&
                                double_val.value() ==
                                    std::floor(double_val.value())) {
                                if (matcher.set_if_found(
                                        static_cast<int64_t>(
                                            double_val.value()),
                                        found_large,
                                        remaining)) {
                                    return std::make_pair(true, true);
                                }
                            }
                        }
                        continue;
                    }
                    if (matcher.set_if_found(
                            val.value(), found_large, remaining)) {
                        return std::make_pair(true, true);
                    }
                }
                return std::make_pair(true, remaining == 0);
            }
        };
        bool has_bitmap_input = !bitmap_input.empty();
        for (size_t i = 0; i < size; ++i) {
            auto offset = i;
            if constexpr (filter_type == FilterType::random) {
                offset = (offsets) ? offsets[i] : i;
            }
            if (valid_data != nullptr && !valid_data[offset]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            if (has_bitmap_input && !bitmap_input[processed_cursor + i]) {
                continue;
            }
            auto [valid, matched] = executor(offset);
            if (!valid) {
                res[i] = valid_res[i] = false;
                continue;
            }
            res[i] = matched;
        }
        processed_cursor += size;
    };

    int64_t processed_size;
    if (has_offset_input_) {
        processed_size = ProcessDataByOffsets<Json>(execute_sub_batch,
                                                    std::nullptr_t{},
                                                    input,
                                                    res,
                                                    valid_res,
                                                    pointer,
                                                    *elements);
    } else {
        processed_size = ProcessDataChunks<Json>(execute_sub_batch,
                                                 std::nullptr_t{},
                                                 res,
                                                 valid_res,
                                                 pointer,
                                                 *elements);
    }
    AssertInfo(processed_size == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
}

template <typename ExprValueType>
VectorPtr
PhyJsonContainsFilterExpr::ExecJsonContainsAllByStats() {
    using GetType =
        std::conditional_t<std::is_same_v<ExprValueType, std::string>,
                           std::string_view,
                           ExprValueType>;
    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }
    auto pointer = milvus::Json::pointer(expr_->column_.nested_path_);
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
    if (elements->empty()) {
        MoveCursor();
        return std::make_shared<ColumnVector>(
            TargetBitmap(real_batch_size, false),
            TargetBitmap(real_batch_size, true));
    }

    if (cached_index_chunk_id_ != 0 && TryCacheGet()) {
        // Cache hit — skip Stats computation.
    } else if (cached_index_chunk_id_ != 0 &&
               segment_->type() == SegmentType::Sealed) {
        auto cache_compute_start = CacheClock::now();
        auto* segment = dynamic_cast<const segcore::SegmentSealed*>(segment_);
        auto field_id = expr_->column_.field_id_;
        auto index = segment->GetJsonStats(op_ctx_, field_id);
        Assert(index.get() != nullptr);

        cached_index_chunk_res_ = std::make_shared<TargetBitmap>(active_count_);
        cached_index_chunk_valid_res_ =
            std::make_shared<TargetBitmap>(active_count_);
        TargetBitmapView res_view(*cached_index_chunk_res_);
        TargetBitmapView valid_res_view(*cached_index_chunk_valid_res_);
        // process shredding data for ARRAY type (non-shared)
        {
            milvus::ScopedTimer timer(
                "json_contains_all_stats_shredding_data",
                [this](double us) { json_stats_shredding_latency_us_ += us; });
            auto target_field = index->GetShreddingField(
                pointer, milvus::index::JSONType::ARRAY);
            if (!target_field.empty()) {
                TargetBitmap target_res(active_count_, false);
                TargetBitmapView target_res_view(target_res);
                TargetBitmap target_valid(active_count_, true);
                TargetBitmapView target_valid_view(target_valid);
                ShreddingArrayBsonContainsAllExecutor<GetType> executor(
                    *elements);

                index->ExecutorForShreddingData<std::string_view>(
                    op_ctx_,
                    target_field,
                    executor,
                    nullptr,
                    target_res_view,
                    target_valid_view);
                res_view.inplace_or_with_count(target_res_view, active_count_);
                valid_res_view.inplace_or_with_count(target_valid_view,
                                                     active_count_);
            }
        }
        // process shared data
        ContainsAllMatcher<GetType> shared_matcher(*elements);
        std::vector<uint64_t> shared_found_large(
            shared_matcher.use_small() ? 0 : shared_matcher.num_words());
        auto shared_executor = [&shared_matcher,
                                &res_view,
                                &valid_res_view,
                                &shared_found_large](milvus::BsonView bson,
                                                     uint32_t row_offset,
                                                     uint32_t value_offset) {
            auto val = bson.ParseAsArrayAtOffset(value_offset);

            if (!val.has_value()) {
                return;
            }
            valid_res_view[row_offset] = true;

            if (shared_matcher.use_small()) {
                uint64_t found = 0;
                for (const auto& element : val.value()) {
                    auto value = [&]() -> std::optional<GetType> {
                        if constexpr (std::is_same_v<GetType, int64_t> ||
                                      std::is_same_v<GetType, double>) {
                            return GetBsonNumberExact<GetType>(
                                element.get_value());
                        } else {
                            return milvus::BsonView::GetValueFromBsonView<
                                GetType>(element.get_value());
                        }
                    }();
                    if (!value.has_value()) {
                        continue;
                    }
                    if (shared_matcher.set_if_found(value.value(), found)) {
                        res_view[row_offset] = true;
                        return;
                    }
                }
                res_view[row_offset] = (found == shared_matcher.full_mask());
            } else {
                std::fill(
                    shared_found_large.begin(), shared_found_large.end(), 0);
                size_t remaining = shared_matcher.target_count();
                for (const auto& element : val.value()) {
                    auto value = [&]() -> std::optional<GetType> {
                        if constexpr (std::is_same_v<GetType, int64_t> ||
                                      std::is_same_v<GetType, double>) {
                            return GetBsonNumberExact<GetType>(
                                element.get_value());
                        } else {
                            return milvus::BsonView::GetValueFromBsonView<
                                GetType>(element.get_value());
                        }
                    }();
                    if (!value.has_value()) {
                        continue;
                    }
                    if (shared_matcher.set_if_found(
                            value.value(), shared_found_large, remaining)) {
                        res_view[row_offset] = true;
                        return;
                    }
                }
                res_view[row_offset] = (remaining == 0);
            }
        };
        {
            milvus::ScopedTimer timer(
                "json_contains_all_stats_shared_data",
                [this](double us) { json_stats_shared_latency_us_ += us; });
            index->ExecuteForSharedData(
                op_ctx_, bson_index_, pointer, shared_executor);
        }
        cached_index_chunk_id_ = 0;
        CachePut(CacheElapsedUs(cache_compute_start));
    }

    auto res = MoveOrSliceBitmap(*cached_index_chunk_res_,
                                 *cached_index_chunk_valid_res_,
                                 current_data_global_pos_,
                                 real_batch_size);
    MoveCursor();
    return res;
}

VectorPtr
PhyJsonContainsFilterExpr::ExecJsonContainsAllWithDiffType(EvalCtx& context) {
    auto* input = context.get_offset_input();
    const auto& bitmap_input = context.get_bitmap_input();
    FieldId field_id = expr_->column_.field_id_;
    if (!has_offset_input_ && exec_path_ == ExprExecPath::JsonStats) {
        milvus::ScopedTimer timer(
            "json_contains_all_difftype_by_stats",
            [this](double us) { json_filter_stats_latency_us_ += us; });
        return ExecJsonContainsAllWithDiffTypeByStats();
    }

    milvus::ScopedTimer timer(
        "json_contains_all_difftype_bruteforce",
        [this](double us) { json_filter_bruteforce_latency_us_ += us; });

    auto real_batch_size =
        has_offset_input_ ? input->size() : GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }
    auto res_vec =
        std::make_shared<ColumnVector>(TargetBitmap(real_batch_size, false),
                                       TargetBitmap(real_batch_size, true));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);

    auto pointer = milvus::Json::pointer(expr_->column_.nested_path_);

    const auto& elements = expr_->vals_;
    std::unordered_set<int> elements_index;
    for (int i = 0; i < static_cast<int>(elements.size()); i++) {
        elements_index.insert(i);
    }

    int processed_cursor = 0;
    auto execute_sub_batch =
        [&processed_cursor, &
         bitmap_input ]<FilterType filter_type = FilterType::sequential>(
            const milvus::Json* data,
            const bool* valid_data,
            const int32_t* offsets,
            const int size,
            TargetBitmapView res,
            TargetBitmapView valid_res,
            const std::string& pointer,
            const std::vector<proto::plan::GenericValue>& elements,
            const std::unordered_set<int>& elements_index) {
        // If data is nullptr, this chunk was skipped by SkipIndex.
        // We only need to update processed_cursor for bitmap_input indexing.
        if (data == nullptr) {
            processed_cursor += size;
            return;
        }
        auto executor = [&](size_t i) {
            const auto& json = data[i];
            auto doc = json.dom_doc();
            auto array = doc.at_pointer(pointer).get_array();
            if (array.error()) {
                return std::make_pair(false, false);
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
                                auto double_val = it.template get<double>();
                                if (!double_val.error() &&
                                    double_val.value() == element.int64_val()) {
                                    tmp_elements_index.erase(i);
                                }
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
                            ThrowInfo(DataTypeInvalid,
                                      "unsupported data type {}",
                                      element.val_case());
                    }
                    if (tmp_elements_index.size() == 0) {
                        return std::make_pair(true, true);
                    }
                }
                if (tmp_elements_index.size() == 0) {
                    return std::make_pair(true, true);
                }
            }
            return std::make_pair(true, tmp_elements_index.size() == 0);
        };
        bool has_bitmap_input = !bitmap_input.empty();
        for (size_t i = 0; i < size; ++i) {
            auto offset = i;
            if constexpr (filter_type == FilterType::random) {
                offset = (offsets) ? offsets[i] : i;
            }
            if (valid_data != nullptr && !valid_data[offset]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            if (has_bitmap_input && !bitmap_input[processed_cursor + i]) {
                continue;
            }

            auto [valid, matched] = executor(offset);
            if (!valid) {
                res[i] = valid_res[i] = false;
                continue;
            }
            res[i] = matched;
        }
        processed_cursor += size;
    };

    int64_t processed_size;
    if (has_offset_input_) {
        processed_size = ProcessDataByOffsets<Json>(execute_sub_batch,
                                                    std::nullptr_t{},
                                                    input,
                                                    res,
                                                    valid_res,
                                                    pointer,
                                                    elements,
                                                    elements_index);
    } else {
        processed_size = ProcessDataChunks<Json>(execute_sub_batch,
                                                 std::nullptr_t{},
                                                 res,
                                                 valid_res,
                                                 pointer,
                                                 elements,
                                                 elements_index);
    }
    AssertInfo(processed_size == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
}

VectorPtr
PhyJsonContainsFilterExpr::ExecJsonContainsAllWithDiffTypeByStats() {
    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }
    auto pointer = milvus::Json::pointer(expr_->column_.nested_path_);
    const auto& elements = expr_->vals_;
    std::set<int> elements_index;
    for (int i = 0; i < static_cast<int>(elements.size()); i++) {
        elements_index.insert(i);
    }
    if (elements.empty()) {
        MoveCursor();
        return std::make_shared<ColumnVector>(
            TargetBitmap(real_batch_size, false),
            TargetBitmap(real_batch_size, true));
    }

    if (cached_index_chunk_id_ != 0 && TryCacheGet()) {
        // Cache hit — skip Stats computation.
    } else if (cached_index_chunk_id_ != 0 &&
               segment_->type() == SegmentType::Sealed) {
        auto cache_compute_start = CacheClock::now();
        auto* segment = dynamic_cast<const segcore::SegmentSealed*>(segment_);
        auto field_id = expr_->column_.field_id_;
        auto index = segment->GetJsonStats(op_ctx_, field_id);
        Assert(index.get() != nullptr);

        cached_index_chunk_res_ = std::make_shared<TargetBitmap>(active_count_);
        cached_index_chunk_valid_res_ =
            std::make_shared<TargetBitmap>(active_count_);
        TargetBitmapView res_view(*cached_index_chunk_res_);
        TargetBitmapView valid_res_view(*cached_index_chunk_valid_res_);

        // process shredding data for ARRAY type (non-shared)
        {
            milvus::ScopedTimer timer(
                "json_contains_all_difftype_stats_shredding_data",
                [this](double us) { json_stats_shredding_latency_us_ += us; });
            auto target_field = index->GetShreddingField(
                pointer, milvus::index::JSONType::ARRAY);
            if (!target_field.empty()) {
                TargetBitmap target_res(active_count_, false);
                TargetBitmapView target_res_view(target_res);
                TargetBitmap target_valid(active_count_, true);
                TargetBitmapView target_valid_view(target_valid);
                ShreddingArrayBsonContainsAllWithDiffTypeExecutor executor(
                    elements, elements_index);
                index->ExecutorForShreddingData<std::string_view>(
                    op_ctx_,
                    target_field,
                    executor,
                    nullptr,
                    target_res_view,
                    target_valid_view);
                res_view.inplace_or_with_count(target_res_view, active_count_);
                valid_res_view.inplace_or_with_count(target_valid_view,
                                                     active_count_);
            }
        }

        auto shared_executor = [&elements,
                                &elements_index,
                                &res_view,
                                &valid_res_view](milvus::BsonView bson,
                                                 uint32_t row_offset,
                                                 uint32_t value_offset) {
            std::set<int> tmp_elements_index(elements_index);
            auto array = bson.ParseAsArrayAtOffset(value_offset);
            if (!array.has_value()) {
                return;
            }
            valid_res_view[row_offset] = true;

            for (const auto& sub_value : array.value()) {
                int i = -1;
                for (auto& element : elements) {
                    i++;
                    switch (element.val_case()) {
                        case proto::plan::GenericValue::kBoolVal: {
                            auto val =
                                milvus::BsonView::GetValueFromBsonView<bool>(
                                    sub_value.get_value());
                            if (!val.has_value()) {
                                continue;
                            }
                            if (val.value() == element.bool_val()) {
                                tmp_elements_index.erase(i);
                            }
                            break;
                        }
                        case proto::plan::GenericValue::kInt64Val: {
                            auto comparison = CompareBsonNumberToBound(
                                sub_value.get_value(), element);
                            if (comparison.has_value() && *comparison == 0) {
                                tmp_elements_index.erase(i);
                            }
                            break;
                        }
                        case proto::plan::GenericValue::kFloatVal: {
                            auto comparison = CompareBsonNumberToBound(
                                sub_value.get_value(), element);
                            if (comparison.has_value() && *comparison == 0) {
                                tmp_elements_index.erase(i);
                            }
                            break;
                        }
                        case proto::plan::GenericValue::kStringVal: {
                            auto val = milvus::BsonView::GetValueFromBsonView<
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
                            auto val = milvus::BsonView::GetValueFromBsonView<
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
                            ThrowInfo(DataTypeInvalid,
                                      "unsupported data type {}",
                                      element.val_case());
                    }
                    if (tmp_elements_index.size() == 0) {
                        res_view[row_offset] = true;
                        return;
                    }
                }
                if (tmp_elements_index.size() == 0) {
                    res_view[row_offset] = true;
                    return;
                }
            }
            res_view[row_offset] = tmp_elements_index.size() == 0;
        };
        {
            milvus::ScopedTimer timer(
                "json_contains_all_difftype_stats_shared_data",
                [this](double us) { json_stats_shared_latency_us_ += us; });
            index->ExecuteForSharedData(
                op_ctx_, bson_index_, pointer, shared_executor);
        }
        cached_index_chunk_id_ = 0;
        CachePut(CacheElapsedUs(cache_compute_start));
    }

    auto res = MoveOrSliceBitmap(*cached_index_chunk_res_,
                                 *cached_index_chunk_valid_res_,
                                 current_data_global_pos_,
                                 real_batch_size);
    MoveCursor();
    return res;
}

VectorPtr
PhyJsonContainsFilterExpr::ExecJsonContainsAllArray(EvalCtx& context) {
    auto* input = context.get_offset_input();
    const auto& bitmap_input = context.get_bitmap_input();
    FieldId field_id = expr_->column_.field_id_;
    if (!has_offset_input_ && exec_path_ == ExprExecPath::JsonStats) {
        milvus::ScopedTimer timer(
            "json_contains_all_array_by_stats",
            [this](double us) { json_filter_stats_latency_us_ += us; });
        return ExecJsonContainsAllArrayByStats();
    }

    milvus::ScopedTimer timer(
        "json_contains_all_array_bruteforce",
        [this](double us) { json_filter_bruteforce_latency_us_ += us; });

    auto real_batch_size =
        has_offset_input_ ? input->size() : GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    auto res_vec =
        std::make_shared<ColumnVector>(TargetBitmap(real_batch_size, false),
                                       TargetBitmap(real_batch_size, true));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);

    auto pointer = milvus::Json::pointer(expr_->column_.nested_path_);

    std::vector<proto::plan::Array> elements;
    elements.reserve(expr_->vals_.size());
    for (auto const& element : expr_->vals_) {
        elements.emplace_back(GetValueFromProto<proto::plan::Array>(element));
    }

    size_t processed_cursor = 0;
    auto execute_sub_batch =
        [&processed_cursor, &
         bitmap_input ]<FilterType filter_type = FilterType::sequential>(
            const milvus::Json* data,
            const bool* valid_data,
            const int32_t* offsets,
            const int size,
            TargetBitmapView res,
            TargetBitmapView valid_res,
            const std::string& pointer,
            const std::vector<proto::plan::Array>& elements) {
        // If data is nullptr, this chunk was skipped by SkipIndex.
        // We only need to update processed_cursor for bitmap_input indexing.
        if (data == nullptr) {
            processed_cursor += size;
            return;
        }
        auto executor = [&](const size_t i) {
            auto doc = data[i].doc();
            auto array = doc.at_pointer(pointer).get_array();
            if (array.error()) {
                return std::make_pair(false, false);
            }
            std::unordered_set<int> exist_elements_index;
            for (auto&& it : array) {
                auto val = it.get_array();
                if (val.error()) {
                    continue;
                }
                std::vector<
                    simdjson::simdjson_result<simdjson::ondemand::value>>
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
                    return std::make_pair(true, true);
                }
            }
            return std::make_pair(
                true, exist_elements_index.size() == elements.size());
        };
        bool has_bitmap_input = !bitmap_input.empty();
        for (size_t i = 0; i < size; ++i) {
            auto offset = i;
            if constexpr (filter_type == FilterType::random) {
                offset = (offsets) ? offsets[i] : i;
            }
            if (valid_data != nullptr && !valid_data[offset]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            if (has_bitmap_input && !bitmap_input[processed_cursor + i]) {
                continue;
            }

            auto [valid, matched] = executor(offset);
            if (!valid) {
                res[i] = valid_res[i] = false;
                continue;
            }
            res[i] = matched;
        }
        processed_cursor += size;
    };

    int64_t processed_size;
    if (has_offset_input_) {
        processed_size = ProcessDataByOffsets<Json>(execute_sub_batch,
                                                    std::nullptr_t{},
                                                    input,
                                                    res,
                                                    valid_res,
                                                    pointer,
                                                    elements);
    } else {
        processed_size = ProcessDataChunks<Json>(execute_sub_batch,
                                                 std::nullptr_t{},
                                                 res,
                                                 valid_res,
                                                 pointer,
                                                 elements);
    }
    AssertInfo(processed_size == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
}

VectorPtr
PhyJsonContainsFilterExpr::ExecJsonContainsAllArrayByStats() {
    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }
    auto pointer = milvus::Json::pointer(expr_->column_.nested_path_);
    std::vector<proto::plan::Array> elements;
    elements.reserve(expr_->vals_.size());
    for (auto const& element : expr_->vals_) {
        elements.emplace_back(GetValueFromProto<proto::plan::Array>(element));
    }
    if (elements.empty()) {
        MoveCursor();
        return std::make_shared<ColumnVector>(
            TargetBitmap(real_batch_size, false),
            TargetBitmap(real_batch_size, true));
    }

    if (cached_index_chunk_id_ != 0 && TryCacheGet()) {
        // Cache hit — skip Stats computation.
    } else if (cached_index_chunk_id_ != 0 &&
               segment_->type() == SegmentType::Sealed) {
        auto cache_compute_start = CacheClock::now();
        auto* segment = dynamic_cast<const segcore::SegmentSealed*>(segment_);
        auto field_id = expr_->column_.field_id_;
        auto index = segment->GetJsonStats(op_ctx_, field_id);
        Assert(index.get() != nullptr);

        cached_index_chunk_res_ = std::make_shared<TargetBitmap>(active_count_);
        cached_index_chunk_valid_res_ =
            std::make_shared<TargetBitmap>(active_count_);
        TargetBitmapView res_view(*cached_index_chunk_res_);
        TargetBitmapView valid_res_view(*cached_index_chunk_valid_res_);

        // process shredding data for ARRAY type (non-shared)
        {
            milvus::ScopedTimer timer(
                "json_contains_all_array_stats_shredding_data",
                [this](double us) { json_stats_shredding_latency_us_ += us; });
            auto target_field = index->GetShreddingField(
                pointer, milvus::index::JSONType::ARRAY);
            if (!target_field.empty()) {
                TargetBitmap target_res(active_count_, false);
                TargetBitmapView target_res_view(target_res);
                TargetBitmap target_valid(active_count_, true);
                TargetBitmapView target_valid_view(target_valid);
                ShreddingArrayBsonContainsAllArrayExecutor executor(elements);
                index->ExecutorForShreddingData<std::string_view>(
                    op_ctx_,
                    target_field,
                    executor,
                    nullptr,
                    target_res_view,
                    target_valid_view);
                res_view.inplace_or_with_count(target_res_view, active_count_);
                valid_res_view.inplace_or_with_count(target_valid_view,
                                                     active_count_);
            }
        }

        auto shared_executor = [&elements, &res_view, &valid_res_view](
                                   milvus::BsonView bson,
                                   uint32_t row_offset,
                                   uint32_t value_offset) {
            auto array = bson.ParseAsArrayAtOffset(value_offset);
            if (!array.has_value()) {
                return;
            }
            valid_res_view[row_offset] = true;

            std::set<int> exist_elements_index;
            for (const auto& sub_value : array.value()) {
                auto sub_array = milvus::BsonView::GetValueFromBsonView<
                    milvus::bson::array_view>(sub_value.get_value());

                if (!sub_array.has_value())
                    continue;

                for (int index = 0; index < elements.size(); ++index) {
                    if (CompareTwoJsonArray(sub_array.value(),
                                            elements[index])) {
                        exist_elements_index.insert(index);
                    }
                }
                if (exist_elements_index.size() == elements.size()) {
                    res_view[row_offset] = true;
                    return;
                }
            }
            res_view[row_offset] =
                exist_elements_index.size() == elements.size();
        };
        {
            milvus::ScopedTimer timer(
                "json_contains_all_array_stats_shared_data",
                [this](double us) { json_stats_shared_latency_us_ += us; });
            index->ExecuteForSharedData(
                op_ctx_, bson_index_, pointer, shared_executor);
        }
        cached_index_chunk_id_ = 0;
        CachePut(CacheElapsedUs(cache_compute_start));
    }

    auto res = MoveOrSliceBitmap(*cached_index_chunk_res_,
                                 *cached_index_chunk_valid_res_,
                                 current_data_global_pos_,
                                 real_batch_size);
    MoveCursor();
    return res;
}

VectorPtr
PhyJsonContainsFilterExpr::ExecJsonContainsWithDiffType(EvalCtx& context) {
    auto* input = context.get_offset_input();
    const auto& bitmap_input = context.get_bitmap_input();
    FieldId field_id = expr_->column_.field_id_;
    if (!has_offset_input_ && exec_path_ == ExprExecPath::JsonStats) {
        milvus::ScopedTimer timer(
            "json_contains_difftype_by_stats",
            [this](double us) { json_filter_stats_latency_us_ += us; });
        return ExecJsonContainsWithDiffTypeByStats();
    }

    milvus::ScopedTimer timer(
        "json_contains_difftype_bruteforce",
        [this](double us) { json_filter_bruteforce_latency_us_ += us; });

    auto real_batch_size =
        has_offset_input_ ? input->size() : GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    auto res_vec =
        std::make_shared<ColumnVector>(TargetBitmap(real_batch_size, false),
                                       TargetBitmap(real_batch_size, true));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);

    auto pointer = milvus::Json::pointer(expr_->column_.nested_path_);

    const auto& elements = expr_->vals_;

    size_t processed_cursor = 0;
    auto execute_sub_batch =
        [&processed_cursor, &
         bitmap_input ]<FilterType filter_type = FilterType::sequential>(
            const milvus::Json* data,
            const bool* valid_data,
            const int32_t* offsets,
            const int size,
            TargetBitmapView res,
            TargetBitmapView valid_res,
            const std::string& pointer,
            const std::vector<proto::plan::GenericValue>& elements) {
        // If data is nullptr, this chunk was skipped by SkipIndex.
        // We only need to update processed_cursor for bitmap_input indexing.
        if (data == nullptr) {
            processed_cursor += size;
            return;
        }
        auto executor = [&](const size_t i) {
            auto& json = data[i];
            auto doc = json.dom_doc();
            auto array = doc.at_pointer(pointer).get_array();
            if (array.error()) {
                return std::make_pair(false, false);
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
                                return std::make_pair(true, true);
                            }
                            break;
                        }
                        case proto::plan::GenericValue::kInt64Val: {
                            auto val = it.template get<int64_t>();
                            if (val.error()) {
                                auto double_val = it.template get<double>();
                                if (!double_val.error() &&
                                    double_val.value() == element.int64_val()) {
                                    return std::make_pair(true, true);
                                }
                                continue;
                            }
                            if (val.value() == element.int64_val()) {
                                return std::make_pair(true, true);
                            }
                            break;
                        }
                        case proto::plan::GenericValue::kFloatVal: {
                            auto val = it.template get<double>();
                            if (val.error()) {
                                continue;
                            }
                            if (val.value() == element.float_val()) {
                                return std::make_pair(true, true);
                            }
                            break;
                        }
                        case proto::plan::GenericValue::kStringVal: {
                            auto val = it.template get<std::string_view>();
                            if (val.error()) {
                                continue;
                            }
                            if (val.value() == element.string_val()) {
                                return std::make_pair(true, true);
                            }
                            break;
                        }
                        case proto::plan::GenericValue::kArrayVal: {
                            auto val = it.get_array();
                            if (val.error()) {
                                continue;
                            }
                            if (CompareTwoJsonArray(val, element.array_val())) {
                                return std::make_pair(true, true);
                            }
                            break;
                        }
                        default:
                            ThrowInfo(DataTypeInvalid,
                                      "unsupported data type {}",
                                      element.val_case());
                    }
                }
            }
            return std::make_pair(true, false);
        };
        bool has_bitmap_input = !bitmap_input.empty();
        for (size_t i = 0; i < size; ++i) {
            auto offset = i;
            if constexpr (filter_type == FilterType::random) {
                offset = (offsets) ? offsets[i] : i;
            }
            if (valid_data != nullptr && !valid_data[offset]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            if (has_bitmap_input && !bitmap_input[processed_cursor + i]) {
                continue;
            }

            auto [valid, matched] = executor(offset);
            if (!valid) {
                res[i] = valid_res[i] = false;
                continue;
            }
            res[i] = matched;
        }
        processed_cursor += size;
    };

    int64_t processed_size;
    if (has_offset_input_) {
        processed_size = ProcessDataByOffsets<Json>(execute_sub_batch,
                                                    std::nullptr_t{},
                                                    input,
                                                    res,
                                                    valid_res,
                                                    pointer,
                                                    elements);
    } else {
        processed_size = ProcessDataChunks<Json>(execute_sub_batch,
                                                 std::nullptr_t{},
                                                 res,
                                                 valid_res,
                                                 pointer,
                                                 elements);
    }
    AssertInfo(processed_size == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
}

VectorPtr
PhyJsonContainsFilterExpr::ExecJsonContainsWithDiffTypeByStats() {
    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }
    auto pointer = milvus::Json::pointer(expr_->column_.nested_path_);
    const auto& elements = expr_->vals_;
    if (elements.empty()) {
        MoveCursor();
        return std::make_shared<ColumnVector>(
            TargetBitmap(real_batch_size, false),
            TargetBitmap(real_batch_size, true));
    }

    if (cached_index_chunk_id_ != 0 && TryCacheGet()) {
        // Cache hit — skip Stats computation.
    } else if (cached_index_chunk_id_ != 0 &&
               segment_->type() == SegmentType::Sealed) {
        auto cache_compute_start = CacheClock::now();
        auto* segment = dynamic_cast<const segcore::SegmentSealed*>(segment_);
        auto field_id = expr_->column_.field_id_;
        auto index = segment->GetJsonStats(op_ctx_, field_id);
        Assert(index.get() != nullptr);

        cached_index_chunk_res_ = std::make_shared<TargetBitmap>(active_count_);
        cached_index_chunk_valid_res_ =
            std::make_shared<TargetBitmap>(active_count_);
        TargetBitmapView res_view(*cached_index_chunk_res_);
        TargetBitmapView valid_res_view(*cached_index_chunk_valid_res_);

        // process shredding data for ARRAY type (non-shared)
        {
            milvus::ScopedTimer timer(
                "json_contains_difftype_stats_shredding_data",
                [this](double us) { json_stats_shredding_latency_us_ += us; });
            auto target_field = index->GetShreddingField(
                pointer, milvus::index::JSONType::ARRAY);
            if (!target_field.empty()) {
                TargetBitmap target_res(active_count_, false);
                TargetBitmapView target_res_view(target_res);
                TargetBitmap target_valid(active_count_, true);
                TargetBitmapView target_valid_view(target_valid);
                ShreddingArrayBsonContainsAnyWithDiffTypeExecutor executor(
                    elements);
                index->ExecutorForShreddingData<std::string_view>(
                    op_ctx_,
                    target_field,
                    executor,
                    nullptr,
                    target_res_view,
                    target_valid_view);
                res_view.inplace_or_with_count(target_res_view, active_count_);
                valid_res_view.inplace_or_with_count(target_valid_view,
                                                     active_count_);
            }
        }

        auto shared_executor = [&elements, &res_view, &valid_res_view](
                                   milvus::BsonView bson,
                                   uint32_t row_offset,
                                   uint32_t value_offset) {
            auto array = bson.ParseAsArrayAtOffset(value_offset);
            if (!array.has_value()) {
                return;
            }
            valid_res_view[row_offset] = true;

            for (const auto& sub_value : array.value()) {
                for (auto const& element : elements) {
                    switch (element.val_case()) {
                        case proto::plan::GenericValue::kBoolVal: {
                            auto val =
                                milvus::BsonView::GetValueFromBsonView<bool>(
                                    sub_value.get_value());
                            if (!val.has_value()) {
                                continue;
                            }
                            if (val.value() == element.bool_val()) {
                                res_view[row_offset] = true;
                                return;
                            }
                            break;
                        }
                        case proto::plan::GenericValue::kInt64Val: {
                            auto comparison = CompareBsonNumberToBound(
                                sub_value.get_value(), element);
                            if (comparison.has_value() && *comparison == 0) {
                                res_view[row_offset] = true;
                                return;
                            }
                            break;
                        }
                        case proto::plan::GenericValue::kFloatVal: {
                            auto comparison = CompareBsonNumberToBound(
                                sub_value.get_value(), element);
                            if (comparison.has_value() && *comparison == 0) {
                                res_view[row_offset] = true;
                                return;
                            }
                            break;
                        }
                        case proto::plan::GenericValue::kStringVal: {
                            auto val = milvus::BsonView::GetValueFromBsonView<
                                std::string>(sub_value.get_value());
                            if (!val.has_value()) {
                                continue;
                            }
                            if (val.value() == element.string_val()) {
                                res_view[row_offset] = true;
                                return;
                            }
                            break;
                        }
                        case proto::plan::GenericValue::kArrayVal: {
                            auto val = milvus::BsonView::GetValueFromBsonView<
                                milvus::bson::array_view>(
                                sub_value.get_value());
                            if (!val.has_value()) {
                                continue;
                            }
                            if (CompareTwoJsonArray(val.value(),
                                                    element.array_val())) {
                                res_view[row_offset] = true;
                                return;
                            }
                            break;
                        }
                        default:
                            ThrowInfo(DataTypeInvalid,
                                      "unsupported data type {}",
                                      element.val_case());
                    }
                }
            }
        };
        {
            milvus::ScopedTimer timer(
                "json_contains_difftype_stats_shared_data",
                [this](double us) { json_stats_shared_latency_us_ += us; });
            index->ExecuteForSharedData(
                op_ctx_, bson_index_, pointer, shared_executor);
        }
        cached_index_chunk_id_ = 0;
        CachePut(CacheElapsedUs(cache_compute_start));
    }

    auto res = MoveOrSliceBitmap(*cached_index_chunk_res_,
                                 *cached_index_chunk_valid_res_,
                                 current_data_global_pos_,
                                 real_batch_size);
    MoveCursor();
    return res;
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
            ThrowInfo(DataTypeInvalid,
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
                    ExprInvalid,
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
