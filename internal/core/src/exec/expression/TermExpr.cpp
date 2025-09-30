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
#include <memory>
#include <utility>
#include "log/Log.h"
#include "query/Utils.h"
namespace milvus {
namespace exec {

void
PhyTermFilterExpr::Eval(EvalCtx& context, VectorPtr& result) {
    auto input = context.get_offset_input();
    SetHasOffsetInput((input != nullptr));
    if (is_pk_field_ && !has_offset_input_) {
        result = ExecPkTermImpl();
        return;
    }
    switch (expr_->column_.data_type_) {
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
        case DataType::INT64: {
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
                SetNotUseIndex();
                result = ExecVisitorImplTemplateArray<bool>(context);
                break;
            }
            auto type = expr_->vals_[0].val_case();
            switch (type) {
                case proto::plan::GenericValue::ValCase::kBoolVal:
                    SetNotUseIndex();
                    result = ExecVisitorImplTemplateArray<bool>(context);
                    break;
                case proto::plan::GenericValue::ValCase::kInt64Val:
                    SetNotUseIndex();
                    result = ExecVisitorImplTemplateArray<int64_t>(context);
                    break;
                case proto::plan::GenericValue::ValCase::kFloatVal:
                    SetNotUseIndex();
                    result = ExecVisitorImplTemplateArray<double>(context);
                    break;
                case proto::plan::GenericValue::ValCase::kStringVal:
                    SetNotUseIndex();
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

    auto seg_offsets = segment_->search_ids(*id_array, query_timestamp_);
    cached_bits_.resize(active_count_, false);
    for (const auto& offset : seg_offsets) {
        auto _offset = (int64_t)offset.get();
        cached_bits_[_offset] = true;
    }
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

    TargetBitmap result;
    result.append(cached_bits_, current_data_global_pos_, real_batch_size);
    MoveCursor();
    return std::make_shared<ColumnVector>(std::move(result),
                                          TargetBitmap(real_batch_size, true));
}

template <typename ValueType>
VectorPtr
PhyTermFilterExpr::ExecVisitorImplTemplateJson(EvalCtx& context) {
    if (expr_->is_in_field_) {
        return ExecTermJsonVariableInField<ValueType>(context);
    } else {
        if (SegmentExpr::CanUseIndex() && !has_offset_input_) {
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
            const std::string pointer,
            const ValueType& target_val) {
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
        auto vals = expr_->vals_;
        pinned_json_stats_ = segment->GetJsonStats(op_ctx_, field_id);
        auto* index = pinned_json_stats_.get();
        Assert(index != nullptr);

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
                index->template ExecutorForShreddingData<ColType>(
                    op_ctx_,
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

        // process shared data
        auto shared_executor = [this, &res_view](milvus::BsonView bson,
                                                 uint32_t row_offset,
                                                 uint32_t value_offset) {
            auto get_value = bson.ParseAsValueAtOffset<GetType>(value_offset);

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
        if (!index->CanSkipShared(pointer)) {
            index->ExecuteForSharedData(op_ctx_, pointer, shared_executor);
        }
        cached_index_chunk_id_ = 0;
    }

    TargetBitmap result;
    result.append(
        *cached_index_chunk_res_, current_data_global_pos_, real_batch_size);
    MoveCursor();
    return std::make_shared<ColumnVector>(std::move(result),
                                          TargetBitmap(real_batch_size, true));
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
    if (!has_offset_input_ && CanUseJsonStats(context, field_id)) {
        return ExecJsonInVariableByStats<ValueType>();
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

    auto pointer = milvus::Json::pointer(expr_->column_.nested_path_);
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
            const Json* data,
            const bool* valid_data,
            const int32_t* offsets,
            const int size,
            TargetBitmapView res,
            TargetBitmapView valid_res,
            const std::string pointer,
            const std::shared_ptr<MultiElement>& terms) {
        auto executor = [&](size_t i) {
            auto x = data[i].template at<GetType>(pointer);
            if (x.error()) {
                if constexpr (std::is_same_v<GetType, std::int64_t>) {
                    auto x = data[i].template at<double>(pointer);
                    if (x.error()) {
                        return false;
                    }

                    auto value = x.value();
                    // if the term set is {1}, and the value is 1.1, we should not return true.
                    return std::floor(value) == value &&
                           terms->In(ValueType(x.value()));
                }
                return false;
            }
            return terms->In(ValueType(x.value()));
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
    if (SegmentExpr::CanUseIndex() && !has_offset_input_) {
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
    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

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
        auto converted_val = GetValueFromProtoWithOverflow<T>(val, overflowed);
        if (!overflowed) {
            vals.emplace_back(converted_val);
        }
    }
    auto execute_sub_batch = [](Index* index_ptr,
                                const std::vector<IndexInnerType>& vals) {
        TermIndexFunc<T> func;
        return func(index_ptr, vals.size(), vals.data());
    };
    auto res = ProcessIndexChunks<T>(execute_sub_batch, vals);
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
    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    std::vector<uint8_t> vals;
    for (auto& val : expr_->vals_) {
        vals.emplace_back(GetValueFromProto<bool>(val) ? 1 : 0);
    }
    auto execute_sub_batch = [](Index* index_ptr,
                                const std::vector<uint8_t>& vals) {
        TermIndexFunc<bool> func;
        return std::move(func(index_ptr, vals.size(), (bool*)vals.data()));
    };
    auto res = ProcessIndexChunks<bool>(execute_sub_batch, vals);
    return res;
}

template <typename T>
VectorPtr
PhyTermFilterExpr::ExecVisitorImplForData(EvalCtx& context) {
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

    if (!arg_inited_) {
        std::vector<T> vals;
        for (auto& val : expr_->vals_) {
            // Integral overflow process
            bool overflowed = false;
            auto converted_val =
                GetValueFromProtoWithOverflow<T>(val, overflowed);
            if (!overflowed) {
                vals.emplace_back(converted_val);
            }
        }
        arg_set_ = std::make_shared<SetElement<T>>(vals);
        arg_inited_ = true;
    }

    int processed_cursor = 0;
    auto execute_sub_batch =
        [&processed_cursor, &
         bitmap_input ]<FilterType filter_type = FilterType::sequential>(
            const T* data,
            const bool* valid_data,
            const int32_t* offsets,
            const int size,
            TargetBitmapView res,
            TargetBitmapView valid_res,
            const std::shared_ptr<MultiElement>& vals) {
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
            if (has_bitmap_input && !bitmap_input[i + processed_cursor]) {
                continue;
            }
            res[i] = vals->In(data[offset]);
        }
        processed_cursor += size;
    };
    int64_t processed_size;
    if (has_offset_input_) {
        processed_size = ProcessDataByOffsets<T>(execute_sub_batch,
                                                 std::nullptr_t{},
                                                 input,
                                                 res,
                                                 valid_res,
                                                 arg_set_);
    } else {
        processed_size = ProcessDataChunks<T>(
            execute_sub_batch, std::nullptr_t{}, res, valid_res, arg_set_);
    }
    AssertInfo(processed_size == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
}

}  //namespace exec
}  // namespace milvus
