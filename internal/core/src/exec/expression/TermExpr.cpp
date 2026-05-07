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

#include "TermExpr.h"

#include <math.h>
#include <simdjson.h>
#include <algorithm>
#include <cstdint>
#include <memory>
#include <utility>
#include <variant>

#include "bitset/bitset.h"
#include "common/Array.h"
#include "common/EasyAssert.h"
#include "common/Json.h"
#include "common/Tracer.h"
#include "common/bson_view.h"
#include "common/type_c.h"
#include "common/ScopedTimer.h"
#include "exec/expression/EvalCtx.h"
#include "exec/expression/Utils.h"
#include "folly/FBVector.h"
#include "glog/logging.h"
#include "monitor/Monitor.h"
#include "index/json_stats/JsonKeyStats.h"
#include "index/json_stats/utils.h"
#include "log/Log.h"
#include "opentelemetry/trace/span.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"
#include "segcore/SegmentInterface.h"
#include "segcore/SegmentSealed.h"
#include "storage/MmapManager.h"
#include "storage/Types.h"
#include "query/Utils.h"

namespace milvus {
class SkipIndex;

namespace exec {

namespace {

template <typename T>
struct TermSubBatchExecutor {
    int& processed_cursor;
    const TargetBitmap& bitmap_input;
    const std::function<void(const void*, int, TargetBitmapView)>&
        simd_filter_fn;
    SetElement<std::string>* str_set_elem;

    template <FilterType filter_type = FilterType::sequential>
    void
    operator()(const T* data,
               const bool* valid_data,
               const int32_t* offsets,
               const int size,
               TargetBitmapView res,
               TargetBitmapView valid_res,
               const std::shared_ptr<MultiElement>& vals) {
        if (data == nullptr) {
            processed_cursor += size;
            return;
        }
        bool has_bitmap_input = !bitmap_input.empty();

        // ── Path 1: SIMD batch (numeric, sequential, within threshold) ──
        if constexpr (filter_type == FilterType::sequential) {
            if (simd_filter_fn) {
                simd_filter_fn(data, size, res);
                // Apply validity mask
                if (valid_data != nullptr) {
                    for (int i = 0; i < size; ++i) {
                        if (!valid_data[i]) {
                            res[i] = valid_res[i] = false;
                        }
                    }
                }
                // Apply bitmap mask
                if (has_bitmap_input) {
                    for (int i = 0; i < size; ++i) {
                        if (!bitmap_input[i + processed_cursor]) {
                            res[i] = false;
                        }
                    }
                }
                processed_cursor += size;
                return;
            }
        }

        // ── Path 2: Per-row (string, bool, large numeric, random access) ──
        // Check validity and bitmap first, then direct lookup without variant.
        for (int i = 0; i < size; ++i) {
            auto offset = i;
            if constexpr (filter_type == FilterType::random) {
                offset = (offsets) ? offsets[i] : i;
            }
            if (valid_data != nullptr && !valid_data[offset]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            if (has_bitmap_input && !bitmap_input[i + processed_cursor]) {
                continue;
            }
            // Direct lookup: skip variant construction
            if constexpr (std::is_same_v<T, std::string> ||
                          std::is_same_v<T, std::string_view>) {
                if (str_set_elem) {
                    // Hash lookup via string_view (zero copy)
                    res[i] = str_set_elem->values_.find(std::string_view(
                                 data[offset])) != str_set_elem->values_.end();
                } else {
                    // FlatVectorElement path (small IN ≤4)
                    res[i] = vals->In(MultiElement::ValueType(
                        std::string_view(data[offset])));
                }
            } else {
                res[i] = vals->In(MultiElement::ValueType(std::in_place_type<T>,
                                                          data[offset]));
            }
        }
        processed_cursor += size;
    }
};

}  // namespace

void
PhyTermFilterExpr::Eval(EvalCtx& context, VectorPtr& result) {
    tracer::AutoSpan span(
        "PhyTermFilterExpr::Eval", tracer::GetRootSpan(), true);
    span.GetSpan()->SetAttribute("data_type",
                                 static_cast<int>(expr_->column_.data_type_));

    auto input = context.get_offset_input();
    SetHasOffsetInput((input != nullptr));
    if (exec_path_ == ExprExecPath::PkIndex && !has_offset_input_) {
        result = ExecPkTermImpl();
        return;
    }
    auto data_type = expr_->column_.data_type_;
    if (expr_->column_.element_level_ &&
        expr_->column_.data_type_ != DataType::JSON) {
        data_type = expr_->column_.element_type_;
    }
    switch (data_type) {
        case DataType::BOOL: {
            result = ExecVisitorImpl<bool>(context);
            break;
        }
        case DataType::INT8: {
            result = ExecVisitorImpl<int8_t>(context);
            break;
        }
        case DataType::INT16: {
            result = ExecVisitorImpl<int16_t>(context);
            break;
        }
        case DataType::INT32: {
            result = ExecVisitorImpl<int32_t>(context);
            break;
        }
        case DataType::INT64:
        case DataType::TIMESTAMPTZ: {
            result = ExecVisitorImpl<int64_t>(context);
            break;
        }
        case DataType::FLOAT: {
            result = ExecVisitorImpl<float>(context);
            break;
        }
        case DataType::DOUBLE: {
            result = ExecVisitorImpl<double>(context);
            break;
        }
        case DataType::VARCHAR: {
            if (segment_->type() == SegmentType::Growing &&
                !storage::MmapManager::GetInstance()
                     .GetMmapConfig()
                     .growing_enable_mmap) {
                result = ExecVisitorImpl<std::string>(context);
            } else {
                result = ExecVisitorImpl<std::string_view>(context);
            }
            break;
        }
        case DataType::JSON: {
            span.GetSpan()->SetAttribute("json_filter_expr_type", "term");
            if (expr_->vals_.size() == 0) {
                result = ExecVisitorImplTemplateJson<bool>(context);
                break;
            }
            auto type = expr_->vals_[0].val_case();
            switch (type) {
                case proto::plan::GenericValue::ValCase::kBoolVal:
                    result = ExecVisitorImplTemplateJson<bool>(context);
                    break;
                case proto::plan::GenericValue::ValCase::kInt64Val:
                    result = ExecVisitorImplTemplateJson<int64_t>(context);
                    break;
                case proto::plan::GenericValue::ValCase::kFloatVal:
                    result = ExecVisitorImplTemplateJson<double>(context);
                    break;
                case proto::plan::GenericValue::ValCase::kStringVal:
                    result = ExecVisitorImplTemplateJson<std::string>(context);
                    break;
                default:
                    ThrowInfo(DataTypeInvalid, "unknown data type: {}", type);
            }
            break;
        }
        case DataType::ARRAY: {
            if (expr_->vals_.size() == 0) {
                result = ExecVisitorImplTemplateArray<bool>(context);
                break;
            }
            auto type = expr_->vals_[0].val_case();
            switch (type) {
                case proto::plan::GenericValue::ValCase::kBoolVal:
                    result = ExecVisitorImplTemplateArray<bool>(context);
                    break;
                case proto::plan::GenericValue::ValCase::kInt64Val:
                    result = ExecVisitorImplTemplateArray<int64_t>(context);
                    break;
                case proto::plan::GenericValue::ValCase::kFloatVal:
                    result = ExecVisitorImplTemplateArray<double>(context);
                    break;
                case proto::plan::GenericValue::ValCase::kStringVal:
                    result = ExecVisitorImplTemplateArray<std::string>(context);
                    break;
                default:
                    ThrowInfo(DataTypeInvalid, "unknown data type: {}", type);
            }
            break;
        }
        default:
            ThrowInfo(DataTypeInvalid,
                      "unsupported data type: {}",
                      expr_->column_.data_type_);
    }
}

template <typename T>
bool
PhyTermFilterExpr::CanSkipSegment() {
    const auto& skip_index = segment_->GetSkipIndex();
    T min, max;
    for (auto i = 0; i < expr_->vals_.size(); i++) {
        auto val = GetValueFromProto<T>(expr_->vals_[i]);
        max = i == 0 ? val : std::max(val, max);
        min = i == 0 ? val : std::min(val, min);
    }
    auto can_skip = [&]() -> bool {
        bool res = false;
        for (int i = 0; i < num_data_chunk_; ++i) {
            if (!skip_index.CanSkipBinaryRange<T>(
                    field_id_, i, min, max, true, true)) {
                return false;
            } else {
                res = true;
            }
        }
        return res;
    };

    // using skip index to help skipping this segment
    if (segment_->type() == SegmentType::Sealed && can_skip()) {
        cached_bits_.resize(active_count_, false);
        cached_bits_inited_ = true;
        return true;
    }
    return false;
}

void
PhyTermFilterExpr::InitPkCacheOffset() {
    auto id_array = std::make_unique<IdArray>();
    switch (pk_type_) {
        case DataType::INT64: {
            if (CanSkipSegment<int64_t>()) {
                return;
            }
            auto dst_ids = id_array->mutable_int_id();
            for (const auto& id : expr_->vals_) {
                dst_ids->add_data(GetValueFromProto<int64_t>(id));
            }
            break;
        }
        case DataType::VARCHAR: {
            if (CanSkipSegment<std::string>()) {
                return;
            }
            auto dst_ids = id_array->mutable_str_id();
            for (const auto& id : expr_->vals_) {
                dst_ids->add_data(GetValueFromProto<std::string>(id));
            }
            break;
        }
        default: {
            ThrowInfo(DataTypeInvalid, "unsupported data type {}", pk_type_);
        }
    }

    cached_bits_.resize(active_count_, false);
    segment_->search_ids(cached_bits_, *id_array);
    cached_bits_inited_ = true;
}

VectorPtr
PhyTermFilterExpr::ExecPkTermImpl() {
    if (!cached_bits_inited_) {
        InitPkCacheOffset();
    }

    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    auto res = MoveOrSliceBitmap(
        cached_bits_, current_data_global_pos_, real_batch_size);
    MoveCursor();
    return res;
}

template <typename ValueType>
VectorPtr
PhyTermFilterExpr::ExecVisitorImplTemplateJson(EvalCtx& context) {
    if (expr_->is_in_field_) {
        return ExecTermJsonVariableInField<ValueType>(context);
    } else {
        if (exec_path_ == ExprExecPath::ScalarIndex && !has_offset_input_) {
            // we create double index for json int64 field for now
            using GetType =
                std::conditional_t<std::is_same_v<ValueType, int64_t>,
                                   double,
                                   ValueType>;
            return ExecVisitorImplForIndex<GetType>();
        } else {
            return ExecTermJsonFieldInVariable<ValueType>(context);
        }
    }
}

template <typename ValueType>
VectorPtr
PhyTermFilterExpr::ExecVisitorImplTemplateArray(EvalCtx& context) {
    if (expr_->is_in_field_) {
        return ExecTermArrayVariableInField<ValueType>(context);
    } else {
        return ExecTermArrayFieldInVariable<ValueType>(context);
    }
}

template <typename ValueType>
VectorPtr
PhyTermFilterExpr::ExecTermArrayVariableInField(EvalCtx& context) {
    using GetType = std::conditional_t<std::is_same_v<ValueType, std::string>,
                                       std::string_view,
                                       ValueType>;
    auto* input = context.get_offset_input();
    const auto& bitmap_input = context.get_bitmap_input();
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

    AssertInfo(expr_->vals_.size() == 1,
               "element length in json array must be one");
    if (!arg_inited_) {
        arg_val_.SetValue<ValueType>(expr_->vals_[0]);
        arg_inited_ = true;
    }
    auto target_val = arg_val_.GetValue<ValueType>();

    int processed_cursor = 0;
    auto execute_sub_batch =
        [&processed_cursor, &
         bitmap_input ]<FilterType filter_type = FilterType::sequential>(
            const ArrayView* data,
            const bool* valid_data,
            const int32_t* offsets,
            const int size,
            TargetBitmapView res,
            TargetBitmapView valid_res,
            const ValueType& target_val) {
        // If data is nullptr, this chunk was skipped by SkipIndex.
        // We only need to update processed_cursor for bitmap_input indexing.
        if (data == nullptr) {
            processed_cursor += size;
            return;
        }
        auto executor = [&](size_t offset) {
            for (int i = 0; i < data[offset].length(); i++) {
                auto val = data[offset].template get_data<GetType>(i);
                if (val == target_val) {
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
                                                    target_val);
    } else {
        processed_size = ProcessDataChunks<milvus::ArrayView>(
            execute_sub_batch, std::nullptr_t{}, res, valid_res, target_val);
    }
    AssertInfo(processed_size == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
}

template <typename ValueType>
VectorPtr
PhyTermFilterExpr::ExecTermArrayFieldInVariable(EvalCtx& context) {
    using GetType = std::conditional_t<std::is_same_v<ValueType, std::string>,
                                       std::string_view,
                                       ValueType>;

    auto* input = context.get_offset_input();
    const auto& bitmap_input = context.get_bitmap_input();
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

    int index = -1;
    if (expr_->column_.nested_path_.size() > 0) {
        index = std::stoi(expr_->column_.nested_path_[0]);
    }
    if (!arg_inited_) {
        arg_set_ = std::make_shared<SetElement<ValueType>>(expr_->vals_);
        arg_inited_ = true;
    }

    if (arg_set_->Empty()) {
        res.reset();
        MoveCursor();
        return res_vec;
    }

    int processed_cursor = 0;
    auto execute_sub_batch =
        [&processed_cursor, &
         bitmap_input ]<FilterType filter_type = FilterType::sequential>(
            const ArrayView* data,
            const bool* valid_data,
            const int32_t* offsets,
            const int size,
            TargetBitmapView res,
            TargetBitmapView valid_res,
            int index,
            const std::shared_ptr<MultiElement>& term_set) {
        // If data is nullptr, this chunk was skipped by SkipIndex.
        // We only need to update processed_cursor for bitmap_input indexing.
        if (data == nullptr) {
            processed_cursor += size;
            return;
        }
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
            if (term_set->Empty() || index >= data[offset].length()) {
                res[i] = false;
                continue;
            }
            if (has_bitmap_input && !bitmap_input[processed_cursor + i]) {
                continue;
            }
            auto value = data[offset].get_data<GetType>(index);
            res[i] = term_set->In(ValueType(value));
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
                                                    index,
                                                    arg_set_);
    } else {
        processed_size = ProcessDataChunks<milvus::ArrayView>(execute_sub_batch,
                                                              std::nullptr_t{},
                                                              res,
                                                              valid_res,
                                                              index,
                                                              arg_set_);
    }
    AssertInfo(processed_size == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
}

template <typename ValueType>
VectorPtr
PhyTermFilterExpr::ExecTermJsonVariableInField(EvalCtx& context) {
    using GetType = std::conditional_t<std::is_same_v<ValueType, std::string>,
                                       std::string_view,
                                       ValueType>;
    auto* input = context.get_offset_input();
    const auto& bitmap_input = context.get_bitmap_input();
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

    AssertInfo(expr_->vals_.size() == 1,
               "element length in json array must be one");
    if (!arg_inited_) {
        arg_val_.SetValue<ValueType>(expr_->vals_[0]);
        arg_inited_ = true;
    }
    auto val = arg_val_.GetValue<ValueType>();

    auto pointer = milvus::Json::pointer(expr_->column_.nested_path_);

    int processed_cursor = 0;
    auto execute_sub_batch =
        [&processed_cursor, &
         bitmap_input ]<FilterType filter_type = FilterType::sequential>(
            const Json* data,
            const bool* valid_data,
            const int32_t* offsets,
            const int size,
            TargetBitmapView res,
            TargetBitmapView valid_res,
            const std::string& pointer,
            const ValueType& target_val) {
        // If data is nullptr, this chunk was skipped by SkipIndex.
        // We only need to update processed_cursor for bitmap_input indexing.
        if (data == nullptr) {
            processed_cursor += size;
            return;
        }
        auto executor = [&](size_t i) {
            auto doc = data[i].doc();
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
            res[i] = executor(offset);
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
                                                            val);
    } else {
        processed_size = ProcessDataChunks<milvus::Json>(
            execute_sub_batch, std::nullptr_t{}, res, valid_res, pointer, val);
    }
    AssertInfo(processed_size == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
}

template <typename ValueType>
VectorPtr
PhyTermFilterExpr::ExecJsonInVariableByStats() {
    using GetType = std::conditional_t<std::is_same_v<ValueType, std::string>,
                                       std::string_view,
                                       ValueType>;
    auto real_batch_size = GetNextBatchSize();

    auto pointer = milvus::index::JsonPointer(expr_->column_.nested_path_);
    if (!arg_inited_) {
        arg_set_ = std::make_shared<SetElement<ValueType>>(expr_->vals_);
        if constexpr (std::is_same_v<GetType, int64_t>) {
            // for int64_t, we need to a double vector to store the values
            auto int_arg_set =
                std::static_pointer_cast<SetElement<int64_t>>(arg_set_);
            std::vector<double> double_vals;
            for (const auto& val : int_arg_set->GetElements()) {
                double_vals.emplace_back(static_cast<double>(val));
            }
            arg_set_double_ = std::make_shared<SetElement<double>>(double_vals);
        } else if constexpr (std::is_same_v<GetType, double>) {
            arg_set_double_ = arg_set_;
        }
        arg_inited_ = true;
    }

    if (arg_set_->Empty()) {
        MoveCursor();
        return std::make_shared<ColumnVector>(
            TargetBitmap(real_batch_size, false),
            TargetBitmap(real_batch_size, true));
    }

    if (cached_index_chunk_id_ != 0 &&
        segment_->type() == SegmentType::Sealed) {
        auto segment = dynamic_cast<const segcore::SegmentSealed*>(segment_);
        auto field_id = expr_->column_.field_id_;
        auto index = segment->GetJsonStats(op_ctx_, field_id);
        Assert(index.get() != nullptr);

        cached_index_chunk_res_ = std::make_shared<TargetBitmap>(active_count_);
        cached_index_chunk_valid_res_ =
            std::make_shared<TargetBitmap>(active_count_, true);
        TargetBitmapView res_view(*cached_index_chunk_res_);
        TargetBitmapView valid_res_view(*cached_index_chunk_valid_res_);

        // process shredding data
        auto try_execute = [&](milvus::index::JSONType json_type,
                               TargetBitmapView& res_view,
                               TargetBitmapView& valid_res_view,
                               auto GetType) {
            auto target_field = index->GetShreddingField(pointer, json_type);
            if (!target_field.empty()) {
                using ColType = decltype(GetType);
                auto shredding_executor = [this](const ColType* src,
                                                 const bool* valid,
                                                 size_t size,
                                                 TargetBitmapView res,
                                                 TargetBitmapView valid_res) {
                    for (size_t i = 0; i < size; ++i) {
                        if (valid != nullptr && !valid[i]) {
                            res[i] = valid_res[i] = false;
                            continue;
                        }
                        if constexpr (std::is_same_v<ColType, double>) {
                            res[i] = this->arg_set_double_->In(src[i]);
                        } else {
                            res[i] = this->arg_set_->In(src[i]);
                        }
                    }
                };
                index->ExecutorForShreddingData<ColType>(op_ctx_,
                                                         target_field,
                                                         shredding_executor,
                                                         nullptr,
                                                         res_view,
                                                         valid_res_view);
                LOG_DEBUG("using shredding data's field: {} count {}",
                          target_field,
                          res_view.count());
            }
        };

        {
            milvus::ScopedTimer timer(
                "term_json_stats_shredding_data",
                [this](double us) { json_stats_shredding_latency_us_ += us; });

            if constexpr (std::is_same_v<GetType, bool>) {
                try_execute(milvus::index::JSONType::BOOL,
                            res_view,
                            valid_res_view,
                            bool{});
            } else if constexpr (std::is_same_v<GetType, int64_t>) {
                try_execute(milvus::index::JSONType::INT64,
                            res_view,
                            valid_res_view,
                            int64_t{});
                // and double compare
                TargetBitmap res_double(active_count_, false);
                TargetBitmapView res_double_view(res_double);
                TargetBitmap res_double_valid(active_count_, true);
                TargetBitmapView valid_res_double_view(res_double_valid);
                try_execute(milvus::index::JSONType::DOUBLE,
                            res_double_view,
                            valid_res_double_view,
                            double{});
                res_view.inplace_or_with_count(res_double_view, active_count_);
                valid_res_view.inplace_or_with_count(valid_res_double_view,
                                                     active_count_);

            } else if constexpr (std::is_same_v<GetType, double>) {
                try_execute(milvus::index::JSONType::DOUBLE,
                            res_view,
                            valid_res_view,
                            double{});
                // and int64 compare
                TargetBitmap res_int64(active_count_, false);
                TargetBitmapView res_int64_view(res_int64);
                TargetBitmap res_int64_valid(active_count_, true);
                TargetBitmapView valid_res_int64_view(res_int64_valid);
                try_execute(milvus::index::JSONType::INT64,
                            res_int64_view,
                            valid_res_int64_view,
                            int64_t{});
                res_view.inplace_or_with_count(res_int64_view, active_count_);
                valid_res_view.inplace_or_with_count(valid_res_int64_view,
                                                     active_count_);
            } else if constexpr (std::is_same_v<GetType, std::string_view> ||
                                 std::is_same_v<GetType, std::string>) {
                try_execute(milvus::index::JSONType::STRING,
                            res_view,
                            valid_res_view,
                            std::string_view{});
            }
        }

        // process shared data
        auto shared_executor = [this, &res_view](milvus::BsonView bson,
                                                 uint32_t row_offset,
                                                 uint32_t value_offset) {
            if constexpr (std::is_same_v<GetType, int64_t> ||
                          std::is_same_v<GetType, double>) {
                auto get_value =
                    bson.ParseAsValueAtOffset<double>(value_offset);
                if (get_value.has_value()) {
                    res_view[row_offset] =
                        this->arg_set_double_->In(get_value.value());
                }
                return;
            } else {
                auto get_value =
                    bson.ParseAsValueAtOffset<GetType>(value_offset);
                if (get_value.has_value()) {
                    res_view[row_offset] =
                        this->arg_set_->In(get_value.value());
                }
                return;
            }
        };

        {
            milvus::ScopedTimer timer(
                "term_json_stats_shared_data",
                [this](double us) { json_stats_shared_latency_us_ += us; });

            index->ExecuteForSharedData(
                op_ctx_, bson_index_, pointer, shared_executor);
        }
        cached_index_chunk_id_ = 0;
    }

    auto res = MoveOrSliceBitmap(
        *cached_index_chunk_res_, current_data_global_pos_, real_batch_size);
    MoveCursor();
    return res;
}

template <typename ValueType>
VectorPtr
PhyTermFilterExpr::ExecTermJsonFieldInVariable(EvalCtx& context) {
    using GetType = std::conditional_t<std::is_same_v<ValueType, std::string>,
                                       std::string_view,
                                       ValueType>;
    auto* input = context.get_offset_input();
    const auto& bitmap_input = context.get_bitmap_input();
    FieldId field_id = expr_->column_.field_id_;
    if (!has_offset_input_ && exec_path_ == ExprExecPath::JsonStats) {
        milvus::ScopedTimer timer("term_json_by_stats", [this](double us) {
            json_filter_stats_latency_us_ += us;
        });
        return ExecJsonInVariableByStats<ValueType>();
    }

    milvus::ScopedTimer timer("term_json_bruteforce", [this](double us) {
        json_filter_bruteforce_latency_us_ += us;
    });

    auto pointer = milvus::Json::pointer(expr_->column_.nested_path_);
    if (expr_->column_.element_level_) {
        AssertInfo(has_offset_input_ && input != nullptr,
                   "JSON element-level term filtering requires row offsets");
        using ElementValueType =
            std::conditional_t<std::is_same_v<ValueType, int64_t> ||
                                   std::is_same_v<ValueType, double>,
                               double,
                               ValueType>;
        if (!arg_inited_) {
            arg_set_ =
                std::make_shared<SetElement<ElementValueType>>(expr_->vals_);
            arg_inited_ = true;
        }

        TargetBitmap json_res;
        TargetBitmap json_valid_res;
        int processed_cursor = 0;
        auto execute_sub_batch =
            TermSubBatchExecutor<ElementValueType>{processed_cursor,
                                                   bitmap_input,
                                                   cached_filter_chunk_,
                                                   cached_str_set_elem_};

        int64_t processed_size = 0;
        FixedVector<ElementValueType> element_values;
        FixedVector<bool> element_valid;
        VisitJsonRowsByOffsets(input, [&](const Json& json, bool row_valid) {
            auto elem_count = ExtractJsonElementValues<ElementValueType>(
                json, row_valid, pointer, element_values, element_valid);
            if (elem_count == 0) {
                return;
            }

            auto old_size = json_res.size();
            json_res.resize(old_size + elem_count, false);
            json_valid_res.resize(old_size + elem_count, true);

            TargetBitmapView res_view(json_res);
            TargetBitmapView valid_res_view(json_valid_res);
            execute_sub_batch.template operator()<FilterType::sequential>(
                element_values.data(),
                element_valid.data(),
                nullptr,
                static_cast<int>(elem_count),
                res_view + old_size,
                valid_res_view + old_size,
                arg_set_);
            processed_size += elem_count;
        });
        AssertInfo(processed_size == static_cast<int64_t>(json_res.size()),
                   "internal error: expr processed JSON elements {} not equal "
                   "result size {}",
                   processed_size,
                   json_res.size());
        return std::make_shared<ColumnVector>(std::move(json_res),
                                              std::move(json_valid_res));
    }

    if (!arg_inited_) {
        arg_set_ = std::make_shared<SetElement<ValueType>>(expr_->vals_);
        arg_inited_ = true;
    }

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

    if (arg_set_->Empty()) {
        res.reset();
        MoveCursor();
        return res_vec;
    }

    int processed_cursor = 0;
    auto execute_sub_batch =
        [&processed_cursor, &
         bitmap_input ]<FilterType filter_type = FilterType::sequential>(
            const Json* data,
            const bool* valid_data,
            const int32_t* offsets,
            const int size,
            TargetBitmapView res,
            TargetBitmapView valid_res,
            const std::string& pointer,
            const std::shared_ptr<MultiElement>& terms) {
        // If data is nullptr, this chunk was skipped by SkipIndex.
        // We only need to update processed_cursor for bitmap_input indexing.
        if (data == nullptr) {
            processed_cursor += size;
            return;
        }
        auto executor = [&](size_t i) {
            if constexpr (std::is_same_v<GetType, std::int64_t>) {
                auto x_num = data[i].at_numeric(pointer);
                if (x_num.error()) {
                    return false;
                }
                auto n = x_num.value();
                if (n.is_int64()) {
                    return terms->In(ValueType(n.get_int64()));
                }
                // uint64 or double → compare as double, consistent with
                // index/stats paths.
                auto dval = n.is_uint64() ? static_cast<double>(n.get_uint64())
                                          : n.get_double();
                // if the term set is {1}, and the value is 1.1, we should
                // not return true.
                return std::floor(dval) == dval && terms->In(ValueType(dval));
            } else {
                auto x = data[i].template at<GetType>(pointer);
                if (x.error()) {
                    return false;
                }
                return terms->In(ValueType(x.value()));
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
            if (terms->Empty()) {
                res[i] = false;
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
        processed_size = ProcessDataByOffsets<milvus::Json>(execute_sub_batch,
                                                            std::nullptr_t{},
                                                            input,
                                                            res,
                                                            valid_res,
                                                            pointer,
                                                            arg_set_);
    } else {
        processed_size = ProcessDataChunks<milvus::Json>(execute_sub_batch,
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

template <typename T>
VectorPtr
PhyTermFilterExpr::ExecVisitorImpl(EvalCtx& context) {
    if (exec_path_ == ExprExecPath::ScalarIndex && !has_offset_input_) {
        return ExecVisitorImplForIndex<T>();
    } else {
        return ExecVisitorImplForData<T>(context);
    }
}

template <typename T>
VectorPtr
PhyTermFilterExpr::ExecVisitorImplForIndex() {
    typedef std::
        conditional_t<std::is_same_v<T, std::string_view>, std::string, T>
            IndexInnerType;
    using Index = index::ScalarIndex<IndexInnerType>;
    auto real_batch_size =
        GetNextRealBatchSize(nullptr, expr_->column_.element_level_);
    if (real_batch_size == 0) {
        return nullptr;
    }

    if (!arg_inited_) {
        std::vector<IndexInnerType> vals;
        for (auto& val : expr_->vals_) {
            if constexpr (std::is_same_v<T, double>) {
                if (val.has_int64_val()) {
                    // only json field will cast int to double because other fields are casted in proxy
                    vals.emplace_back(static_cast<double>(val.int64_val()));
                    continue;
                }
            }

            // Generic overflow handling for all types
            bool overflowed = false;
            auto converted_val =
                GetValueFromProtoWithOverflow<T>(val, overflowed);
            if (!overflowed) {
                vals.emplace_back(converted_val);
            }
        }
        arg_set_ = std::make_shared<FlatVectorElement<IndexInnerType>>(vals);
        arg_inited_ = true;
    }
    auto execute_sub_batch = [](Index* index_ptr,
                                const std::vector<IndexInnerType>& vals) {
        TermIndexFunc<T> func;
        return func(index_ptr, vals.size(), vals.data());
    };
    auto args =
        std::dynamic_pointer_cast<FlatVectorElement<IndexInnerType>>(arg_set_);
    auto res = ProcessIndexChunks<T>(execute_sub_batch, args->values_);
    AssertInfo(res->size() == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               res->size(),
               real_batch_size);
    return res;
}

template <>
VectorPtr
PhyTermFilterExpr::ExecVisitorImplForIndex<bool>() {
    using Index = index::ScalarIndex<bool>;
    auto real_batch_size =
        GetNextRealBatchSize(nullptr, expr_->column_.element_level_);
    if (real_batch_size == 0) {
        return nullptr;
    }

    if (!arg_inited_) {
        std::vector<uint8_t> vals;
        for (auto& val : expr_->vals_) {
            vals.emplace_back(GetValueFromProto<bool>(val) ? 1 : 0);
        }
        arg_set_ = std::make_shared<FlatVectorElement<uint8_t>>(vals);
        arg_inited_ = true;
    }
    auto execute_sub_batch = [](Index* index_ptr,
                                const std::vector<uint8_t>& vals) {
        TermIndexFunc<bool> func;
        return func(index_ptr, vals.size(), (bool*)vals.data());
    };
    auto args = std::dynamic_pointer_cast<FlatVectorElement<uint8_t>>(arg_set_);
    auto res = ProcessIndexChunks<bool>(execute_sub_batch, args->values_);
    return res;
}

template <typename T>
VectorPtr
PhyTermFilterExpr::ExecVisitorImplForData(EvalCtx& context) {
    auto* input = context.get_offset_input();
    const auto& bitmap_input = context.get_bitmap_input();
    auto real_batch_size =
        GetNextRealBatchSize(input, expr_->column_.element_level_);
    if (real_batch_size == 0) {
        return nullptr;
    }

    if (!arg_inited_) {
        std::vector<T> vals;
        for (auto& val : expr_->vals_) {
            bool overflowed = false;
            auto converted_val =
                GetValueFromProtoWithOverflow<T>(val, overflowed);
            if (!overflowed) {
                vals.emplace_back(converted_val);
            }
        }

        if constexpr (std::is_same_v<T, std::string> ||
                      std::is_same_v<T, std::string_view>) {
            std::vector<std::string> str_vals;
            str_vals.reserve(vals.size());
            for (const auto& v : vals) {
                str_vals.emplace_back(v);
            }
            constexpr size_t kLinearScanThreshold = 4;
            if (str_vals.size() <= kLinearScanThreshold) {
                // Small IN: linear scan with length check + memcmp
                arg_set_ =
                    std::make_shared<FlatVectorElement<std::string>>(str_vals);
            } else {
                arg_set_ = std::make_shared<SetElement<std::string>>(str_vals);
            }
        } else if constexpr (std::is_same_v<T, bool>) {
            // Bool: use specialized SetElement<bool> (two flags, O(1)).
            arg_set_ = std::make_shared<SetElement<T>>(vals);
        } else {
            // All numeric types: SIMD for small IN, hash for large IN.
            // Crossover benchmarked with ankerl hash at load_factor=0.5.
            // Automatically adapts to all architectures:
            //   AVX512 int32 (kLanes=16): threshold=128
            //   AVX2   int32 (kLanes=8):  threshold=64
            //   AVX2   int64 (kLanes=4):  threshold=32
            //   NEON   int32 (kLanes=4):  threshold=32
            //   NEON   int64 (kLanes=2):  threshold=16
            const size_t kSimdThreshold =
                static_cast<size_t>(simdLaneCount<T>()) * 8;
            if (vals.size() <= kSimdThreshold) {
                arg_set_ = std::make_shared<SimdBatchElement<T>>(vals);
            } else {
                arg_set_ = std::make_shared<SetElement<T>>(vals);
            }
        }

        // Cache SIMD FilterChunk dispatch (numeric types only).
        // SIMD path runs batch filter first, then applies validity/bitmap
        // masks — avoids per-row variant construction entirely.
        if constexpr (!std::is_same_v<T, bool> &&
                      !std::is_same_v<T, std::string> &&
                      !std::is_same_v<T, std::string_view>) {
            if (auto simd_elem =
                    std::dynamic_pointer_cast<SimdBatchElement<T>>(arg_set_)) {
                cached_filter_chunk_ = [simd_elem](const void* data,
                                                   int size,
                                                   TargetBitmapView res) {
                    simd_elem->FilterChunk(
                        static_cast<const T*>(data), size, res);
                };
            }
        }
        // Cache string SetElement pointer for per-row direct lookup.
        if constexpr (std::is_same_v<T, std::string> ||
                      std::is_same_v<T, std::string_view>) {
            cached_str_set_elem_ =
                dynamic_cast<SetElement<std::string>*>(arg_set_.get());
        }
        // Cache element values for skip_index (avoids per-chunk copy)
        cached_skip_elements_ = GetElementValues<T>(arg_set_);
        arg_inited_ = true;
    }

    int processed_cursor = 0;
    auto execute_sub_batch = TermSubBatchExecutor<T>{processed_cursor,
                                                     bitmap_input,
                                                     cached_filter_chunk_,
                                                     cached_str_set_elem_};

    auto skip_index_func =
        [&cached_elements = cached_skip_elements_](
            const SkipIndex& skip_index, FieldId field_id, int64_t chunk_id) {
            auto* elements = std::any_cast<std::vector<T>>(&cached_elements);
            if (elements == nullptr) {
                return false;
            }
            return skip_index.CanSkipInQuery<T>(field_id, chunk_id, *elements);
        };

    auto res_vec =
        std::make_shared<ColumnVector>(TargetBitmap(real_batch_size, false),
                                       TargetBitmap(real_batch_size, true));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);

    int64_t processed_size;
    if (has_offset_input_) {
        if (expr_->column_.element_level_) {
            // For element-level filtering with offset input
            processed_size = ProcessElementLevelByOffsets<T>(execute_sub_batch,
                                                             skip_index_func,
                                                             input,
                                                             res,
                                                             valid_res,
                                                             arg_set_);
        } else {
            processed_size = ProcessDataByOffsets<T>(execute_sub_batch,
                                                     skip_index_func,
                                                     input,
                                                     res,
                                                     valid_res,
                                                     arg_set_);
        }
    } else {
        if (expr_->column_.element_level_) {
            // For element-level filtering without offset input (brute force)
            processed_size = ProcessDataChunksForElementLevel<T>(
                execute_sub_batch, skip_index_func, res, valid_res, arg_set_);
        } else {
            processed_size = ProcessDataChunks<T>(
                execute_sub_batch, skip_index_func, res, valid_res, arg_set_);
        }
    }
    AssertInfo(processed_size == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
}

void
PhyTermFilterExpr::DetermineExecPath() {
    // PkIndex
    if (is_pk_field_) {
        exec_path_ = ExprExecPath::PkIndex;
        return;
    }

    // JsonStats
    if (CanUseJsonStatsAtInit()) {
        exec_path_ = ExprExecPath::JsonStats;
        return;
    }

    SegmentExpr::DetermineExecPath();
    if (exec_path_ != ExprExecPath::ScalarIndex) {
        return;
    }

    auto data_type = expr_->column_.data_type_;
    if (expr_->column_.element_level_) {
        data_type = expr_->column_.element_type_;
    }

    // ARRAY type cannot use scalar index
    if (data_type == DataType::ARRAY) {
        exec_path_ = ExprExecPath::RawData;
    }
}

}  //namespace exec
}  // namespace milvus
