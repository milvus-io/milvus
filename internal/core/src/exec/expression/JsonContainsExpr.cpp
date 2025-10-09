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
#include <utility>
#include "common/Types.h"

namespace milvus {
namespace exec {

void
PhyJsonContainsFilterExpr::Eval(EvalCtx& context, VectorPtr& result) {
    auto input = context.get_offset_input();
    SetHasOffsetInput((input != nullptr));
    if (expr_->vals_.empty()) {
        auto real_batch_size = has_offset_input_
                                   ? context.get_offset_input()->size()
                                   : GetNextBatchSize();
        if (real_batch_size == 0) {
            result = nullptr;
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
            if (SegmentExpr::CanUseIndex() && !has_offset_input_) {
                result = EvalArrayContainsForIndexSegment(
                    expr_->column_.element_type_);
            } else {
                result = EvalJsonContainsForDataSegment(context);
            }
            break;
        }
        case DataType::JSON: {
            if (SegmentExpr::CanUseIndex() && !has_offset_input_) {
                result = EvalArrayContainsForIndexSegment(
                    value_type_ == DataType::INT64 ? DataType::DOUBLE
                                                   : value_type_);
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
                    case DataType::FLOAT:
                    case DataType::DOUBLE: {
                        return ExecArrayContains<double>(context);
                    }
                    case DataType::STRING:
                    case DataType::VARCHAR: {
                        return ExecArrayContains<std::string>(context);
                    }
                    default:
                        ThrowInfo(
                            DataTypeInvalid,
                            fmt::format("unsupported array sub element type {}",
                                        val_type));
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
                    case DataType::FLOAT:
                    case DataType::DOUBLE: {
                        return ExecArrayContainsAll<double>(context);
                    }
                    case DataType::STRING:
                    case DataType::VARCHAR: {
                        return ExecArrayContainsAll<std::string>(context);
                    }
                    default:
                        ThrowInfo(
                            DataTypeInvalid,
                            fmt::format("unsupported array sub element type {}",
                                        val_type));
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
        arg_set_ = std::make_shared<SortVectorElement<GetType>>(expr_->vals_);
        arg_inited_ = true;
    }

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
            const std::shared_ptr<MultiElement>& elements) {
        auto executor = [&](size_t i) {
            const auto& array = data[i];
            for (int j = 0; j < array.length(); ++j) {
                if (elements->In(array.template get_data<GetType>(j))) {
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
                                                    arg_set_);
    } else {
        processed_size = ProcessDataChunks<milvus::ArrayView>(
            execute_sub_batch, std::nullptr_t{}, res, valid_res, arg_set_);
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
    if (!has_offset_input_ && CanUseJsonStats(context, field_id)) {
        return ExecJsonContainsByStats<ExprValueType>();
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
        arg_set_ = std::make_shared<SortVectorElement<GetType>>(expr_->vals_);
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
        auto executor = [&](size_t i) {
            auto doc = data[i].doc();
            auto array = doc.at_pointer(pointer).get_array();
            if (array.error()) {
                return false;
            }
            for (auto&& it : array) {
                auto val = it.template get<GetType>();
                if (val.error()) {
                    continue;
                }
                if (elements->In(val.value()) > 0) {
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
        arg_set_ = std::make_shared<SortVectorElement<GetType>>(expr_->vals_);
        if constexpr (std::is_same_v<GetType, int64_t>) {
            // for int64_t, we need to a double vector to store the values
            auto sort_arg_set =
                std::dynamic_pointer_cast<SortVectorElement<int64_t>>(arg_set_);
            std::vector<double> double_vals;
            for (const auto& val : sort_arg_set->GetElements()) {
                double_vals.emplace_back(static_cast<double>(val));
            }
            arg_set_double_ =
                std::make_shared<SortVectorElement<double>>(double_vals);
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
        auto* segment = dynamic_cast<const segcore::SegmentSealed*>(segment_);
        auto field_id = expr_->column_.field_id_;
        pinned_json_stats_ = segment->GetJsonStats(op_ctx_, field_id);
        auto* index = pinned_json_stats_.get();
        Assert(index != nullptr);

        cached_index_chunk_res_ = std::make_shared<TargetBitmap>(active_count_);
        cached_index_chunk_valid_res_ =
            std::make_shared<TargetBitmap>(active_count_, true);
        TargetBitmapView res_view(*cached_index_chunk_res_);
        TargetBitmapView valid_res_view(*cached_index_chunk_valid_res_);
        // process shredding data for ARRAY type (non-shared)
        {
            auto target_field = index->GetShreddingField(
                pointer, milvus::index::JSONType::ARRAY);
            if (!target_field.empty()) {
                ShreddingArrayBsonContainsAnyExecutor<GetType> executor(
                    arg_set_, arg_set_double_);

                index->ExecutorForShreddingData<std::string_view>(
                    op_ctx_,
                    target_field,
                    executor,
                    nullptr,
                    res_view,
                    valid_res_view);
            }
        }
        // process shared data
        auto shared_executor = [this, &res_view](milvus::BsonView bson,
                                                 uint32_t row_offset,
                                                 uint32_t value_offset) {
            auto val = bson.ParseAsArrayAtOffset(value_offset);

            if (!val.has_value()) {
                res_view[row_offset] = false;
                return;
            }

            for (const auto& element : val.value()) {
                if constexpr (std::is_same_v<GetType, int64_t> ||
                              std::is_same_v<GetType, double>) {
                    auto value = milvus::BsonView::GetValueFromBsonView<double>(
                        element.get_value());
                    if (value.has_value() &&
                        this->arg_set_double_->In(value.value())) {
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

VectorPtr
PhyJsonContainsFilterExpr::ExecJsonContainsArray(EvalCtx& context) {
    auto* input = context.get_offset_input();
    const auto& bitmap_input = context.get_bitmap_input();
    FieldId field_id = expr_->column_.field_id_;
    if (!has_offset_input_ && CanUseJsonStats(context, field_id)) {
        return ExecJsonContainsArrayByStats();
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
    std::vector<proto::plan::Array> elements;
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
        auto executor = [&](size_t i) -> bool {
            auto doc = data[i].doc();
            auto array = doc.at_pointer(pointer).get_array();
            if (array.error()) {
                return false;
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
                        return true;
                    }
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
                                                            elements);
    } else {
        processed_size = ProcessDataChunks<milvus::Json>(execute_sub_batch,
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
PhyJsonContainsFilterExpr::ExecJsonContainsArrayByStats() {
    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }
    std::vector<proto::plan::Array> elements;
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

    if (cached_index_chunk_id_ != 0 &&
        segment_->type() == SegmentType::Sealed) {
        auto* segment = dynamic_cast<const segcore::SegmentSealed*>(segment_);
        auto field_id = expr_->column_.field_id_;
        pinned_json_stats_ = segment->GetJsonStats(op_ctx_, field_id);
        auto* index = pinned_json_stats_.get();
        Assert(index != nullptr);

        cached_index_chunk_res_ = std::make_shared<TargetBitmap>(active_count_);
        cached_index_chunk_valid_res_ =
            std::make_shared<TargetBitmap>(active_count_, true);
        TargetBitmapView res_view(*cached_index_chunk_res_);
        TargetBitmapView valid_res_view(*cached_index_chunk_valid_res_);

        // process shredding data for ARRAY type (non-shared)
        {
            auto target_field = index->GetShreddingField(
                pointer, milvus::index::JSONType::ARRAY);
            if (!target_field.empty()) {
                ShreddingArrayBsonContainsArrayExecutor executor(elements);
                index->ExecutorForShreddingData<std::string_view>(
                    op_ctx_,
                    target_field,
                    executor,
                    nullptr,
                    res_view,
                    valid_res_view);
            }
        }

        auto shared_executor = [&elements, &res_view](milvus::BsonView bson,
                                                      uint32_t row_offset,
                                                      uint32_t value_offset) {
            auto array = bson.ParseAsArrayAtOffset(value_offset);

            if (!array.has_value()) {
                res_view[row_offset] = false;
            }

            for (const auto& sub_value : array.value()) {
                auto sub_array = milvus::BsonView::GetValueFromBsonView<
                    bsoncxx::array::view>(sub_value.get_value());

                if (!sub_array.has_value())
                    continue;

                for (const auto& element : elements) {
                    if (CompareTwoJsonArray(sub_array.value(), element)) {
                        return true;
                    }
                }
            }
            return false;
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

    std::set<GetType> elements;
    for (auto const& element : expr_->vals_) {
        elements.insert(GetValueWithCastNumber<GetType>(element));
    }

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
            const std::set<GetType>& elements) {
        auto executor = [&](size_t i) {
            std::set<GetType> tmp_elements(elements);
            // Note: array can only be iterated once
            for (int j = 0; j < data[i].length(); ++j) {
                tmp_elements.erase(data[i].template get_data<GetType>(j));
                if (tmp_elements.size() == 0) {
                    return true;
                }
            }
            return tmp_elements.size() == 0;
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
                                                    elements);
    } else {
        processed_size = ProcessDataChunks<milvus::ArrayView>(
            execute_sub_batch, std::nullptr_t{}, res, valid_res, elements);
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
    if (!has_offset_input_ && CanUseJsonStats(context, field_id)) {
        return ExecJsonContainsAllByStats<ExprValueType>();
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
    std::set<GetType> elements;
    for (auto const& element : expr_->vals_) {
        elements.insert(GetValueFromProto<GetType>(element));
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
            const std::set<GetType>& elements) {
        auto executor = [&](const size_t i) -> bool {
            auto doc = data[i].doc();
            auto array = doc.at_pointer(pointer).get_array();
            if (array.error()) {
                return false;
            }
            std::set<GetType> tmp_elements(elements);
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
    std::set<GetType> elements;
    auto pointer = milvus::Json::pointer(expr_->column_.nested_path_);
    for (auto const& element : expr_->vals_) {
        elements.insert(GetValueFromProto<GetType>(element));
    }
    if (elements.empty()) {
        MoveCursor();
        return std::make_shared<ColumnVector>(
            TargetBitmap(real_batch_size, false),
            TargetBitmap(real_batch_size, true));
    }

    if (cached_index_chunk_id_ != 0 &&
        segment_->type() == SegmentType::Sealed) {
        auto* segment = dynamic_cast<const segcore::SegmentSealed*>(segment_);
        auto field_id = expr_->column_.field_id_;
        pinned_json_stats_ = segment->GetJsonStats(op_ctx_, field_id);
        auto* index = pinned_json_stats_.get();
        Assert(index != nullptr);

        cached_index_chunk_res_ = std::make_shared<TargetBitmap>(active_count_);
        cached_index_chunk_valid_res_ =
            std::make_shared<TargetBitmap>(active_count_, true);
        TargetBitmapView res_view(*cached_index_chunk_res_);
        TargetBitmapView valid_res_view(*cached_index_chunk_valid_res_);
        // process shredding data for ARRAY type (non-shared)
        {
            auto target_field = index->GetShreddingField(
                pointer, milvus::index::JSONType::ARRAY);
            if (!target_field.empty()) {
                ShreddingArrayBsonContainsAllExecutor<GetType> executor(
                    elements);

                index->ExecutorForShreddingData<std::string_view>(
                    op_ctx_,
                    target_field,
                    executor,
                    nullptr,
                    res_view,
                    valid_res_view);
            }
        }
        // process shared data
        auto shared_executor = [this, &elements, &res_view](
                                   milvus::BsonView bson,
                                   uint32_t row_offset,
                                   uint32_t value_offset) {
            auto val = bson.ParseAsArrayAtOffset(value_offset);

            if (!val.has_value()) {
                res_view[row_offset] = false;
                return;
            }

            std::set<GetType> tmp_elements(elements);
            for (const auto& element : val.value()) {
                auto value = milvus::BsonView::GetValueFromBsonView<GetType>(
                    element.get_value());
                if (!value.has_value()) {
                    continue;
                }
                tmp_elements.erase(value.value());
                if (tmp_elements.size() == 0) {
                    res_view[row_offset] = true;
                    return;
                }
            }
            res_view[row_offset] = tmp_elements.empty();
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

VectorPtr
PhyJsonContainsFilterExpr::ExecJsonContainsAllWithDiffType(EvalCtx& context) {
    auto* input = context.get_offset_input();
    const auto& bitmap_input = context.get_bitmap_input();
    FieldId field_id = expr_->column_.field_id_;
    if (!has_offset_input_ && CanUseJsonStats(context, field_id)) {
        return ExecJsonContainsAllWithDiffTypeByStats();
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

    auto elements = expr_->vals_;
    std::unordered_set<int> elements_index;
    int i = 0;
    for (auto& element : elements) {
        elements_index.insert(i);
        i++;
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
            const std::unordered_set<int> elements_index) {
        auto executor = [&](size_t i) -> bool {
            const auto& json = data[i];
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
                            ThrowInfo(DataTypeInvalid,
                                      fmt::format("unsupported data type {}",
                                                  element.val_case()));
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
    auto elements = expr_->vals_;
    std::set<int> elements_index;
    int i = 0;
    for (auto& element : elements) {
        elements_index.insert(i);
        i++;
    }
    if (elements.empty()) {
        MoveCursor();
        return std::make_shared<ColumnVector>(
            TargetBitmap(real_batch_size, false),
            TargetBitmap(real_batch_size, true));
    }

    if (cached_index_chunk_id_ != 0 &&
        segment_->type() == SegmentType::Sealed) {
        auto* segment = dynamic_cast<const segcore::SegmentSealed*>(segment_);
        auto field_id = expr_->column_.field_id_;
        pinned_json_stats_ = segment->GetJsonStats(op_ctx_, field_id);
        auto* index = pinned_json_stats_.get();
        Assert(index != nullptr);

        cached_index_chunk_res_ = std::make_shared<TargetBitmap>(active_count_);
        cached_index_chunk_valid_res_ =
            std::make_shared<TargetBitmap>(active_count_, true);
        TargetBitmapView res_view(*cached_index_chunk_res_);
        TargetBitmapView valid_res_view(*cached_index_chunk_valid_res_);

        // process shredding data for ARRAY type (non-shared)
        {
            auto target_field = index->GetShreddingField(
                pointer, milvus::index::JSONType::ARRAY);
            if (!target_field.empty()) {
                ShreddingArrayBsonContainsAllWithDiffTypeExecutor executor(
                    elements, elements_index);
                index->ExecutorForShreddingData<std::string_view>(
                    op_ctx_,
                    target_field,
                    executor,
                    nullptr,
                    res_view,
                    valid_res_view);
            }
        }

        auto shared_executor = [&elements, &elements_index, &res_view](
                                   milvus::BsonView bson,
                                   uint32_t row_offset,
                                   uint32_t value_offset) {
            std::set<int> tmp_elements_index(elements_index);
            auto array = bson.ParseAsArrayAtOffset(value_offset);
            if (!array.has_value()) {
                res_view[row_offset] = false;
                return;
            }

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
                            auto val =
                                milvus::BsonView::GetValueFromBsonView<int64_t>(
                                    sub_value.get_value());
                            if (!val.has_value()) {
                                continue;
                            }
                            if (val.value() == element.int64_val()) {
                                tmp_elements_index.erase(i);
                            }
                            break;
                        }
                        case proto::plan::GenericValue::kFloatVal: {
                            auto val =
                                milvus::BsonView::GetValueFromBsonView<double>(
                                    sub_value.get_value());
                            if (!val.has_value()) {
                                continue;
                            }
                            if (val.value() == element.float_val()) {
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
                                bsoncxx::array::view>(sub_value.get_value());
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
                                      fmt::format("unsupported data type {}",
                                                  element.val_case()));
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

VectorPtr
PhyJsonContainsFilterExpr::ExecJsonContainsAllArray(EvalCtx& context) {
    auto* input = context.get_offset_input();
    const auto& bitmap_input = context.get_bitmap_input();
    FieldId field_id = expr_->column_.field_id_;
    if (!has_offset_input_ && CanUseJsonStats(context, field_id)) {
        return ExecJsonContainsAllArrayByStats();
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

    std::vector<proto::plan::Array> elements;
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
        auto executor = [&](const size_t i) {
            auto doc = data[i].doc();
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
                    return true;
                }
            }
            return exist_elements_index.size() == elements.size();
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
    for (auto const& element : expr_->vals_) {
        elements.emplace_back(GetValueFromProto<proto::plan::Array>(element));
    }
    if (elements.empty()) {
        MoveCursor();
        return std::make_shared<ColumnVector>(
            TargetBitmap(real_batch_size, false),
            TargetBitmap(real_batch_size, true));
    }

    if (cached_index_chunk_id_ != 0 &&
        segment_->type() == SegmentType::Sealed) {
        auto* segment = dynamic_cast<const segcore::SegmentSealed*>(segment_);
        auto field_id = expr_->column_.field_id_;
        pinned_json_stats_ = segment->GetJsonStats(op_ctx_, field_id);
        auto* index = pinned_json_stats_.get();
        Assert(index != nullptr);

        cached_index_chunk_res_ = std::make_shared<TargetBitmap>(active_count_);
        cached_index_chunk_valid_res_ =
            std::make_shared<TargetBitmap>(active_count_, true);
        TargetBitmapView res_view(*cached_index_chunk_res_);
        TargetBitmapView valid_res_view(*cached_index_chunk_valid_res_);

        // process shredding data for ARRAY type (non-shared)
        {
            auto target_field = index->GetShreddingField(
                pointer, milvus::index::JSONType::ARRAY);
            if (!target_field.empty()) {
                ShreddingArrayBsonContainsAllArrayExecutor executor(elements);
                index->ExecutorForShreddingData<std::string_view>(
                    op_ctx_,
                    target_field,
                    executor,
                    nullptr,
                    res_view,
                    valid_res_view);
            }
        }

        auto shared_executor = [&elements, &res_view](milvus::BsonView bson,
                                                      uint32_t row_offset,
                                                      uint32_t value_offset) {
            auto array = bson.ParseAsArrayAtOffset(value_offset);
            if (!array.has_value()) {
                res_view[row_offset] = false;
                return;
            }

            std::set<int> exist_elements_index;
            for (const auto& sub_value : array.value()) {
                auto sub_array = milvus::BsonView::GetValueFromBsonView<
                    bsoncxx::array::view>(sub_value.get_value());

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

VectorPtr
PhyJsonContainsFilterExpr::ExecJsonContainsWithDiffType(EvalCtx& context) {
    auto* input = context.get_offset_input();
    const auto& bitmap_input = context.get_bitmap_input();
    FieldId field_id = expr_->column_.field_id_;
    if (!has_offset_input_ && CanUseJsonStats(context, field_id)) {
        return ExecJsonContainsWithDiffTypeByStats();
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

    auto elements = expr_->vals_;
    std::unordered_set<int> elements_index;
    int i = 0;
    for (auto& element : elements) {
        elements_index.insert(i);
        i++;
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
            const std::vector<proto::plan::GenericValue>& elements) {
        auto executor = [&](const size_t i) {
            auto& json = data[i];
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
                            ThrowInfo(DataTypeInvalid,
                                      fmt::format("unsupported data type {}",
                                                  element.val_case()));
                    }
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
    auto elements = expr_->vals_;
    if (elements.empty()) {
        MoveCursor();
        return std::make_shared<ColumnVector>(
            TargetBitmap(real_batch_size, false),
            TargetBitmap(real_batch_size, true));
    }

    if (cached_index_chunk_id_ != 0 &&
        segment_->type() == SegmentType::Sealed) {
        auto* segment = dynamic_cast<const segcore::SegmentSealed*>(segment_);
        auto field_id = expr_->column_.field_id_;
        pinned_json_stats_ = segment->GetJsonStats(op_ctx_, field_id);
        auto* index = pinned_json_stats_.get();
        Assert(index != nullptr);

        cached_index_chunk_res_ = std::make_shared<TargetBitmap>(active_count_);
        cached_index_chunk_valid_res_ =
            std::make_shared<TargetBitmap>(active_count_, true);
        TargetBitmapView res_view(*cached_index_chunk_res_);
        TargetBitmapView valid_res_view(*cached_index_chunk_valid_res_);

        // process shredding data for ARRAY type (non-shared)
        {
            auto target_field = index->GetShreddingField(
                pointer, milvus::index::JSONType::ARRAY);
            if (!target_field.empty()) {
                ShreddingArrayBsonContainsAnyWithDiffTypeExecutor executor(
                    elements);
                index->ExecutorForShreddingData<std::string_view>(
                    op_ctx_,
                    target_field,
                    executor,
                    nullptr,
                    res_view,
                    valid_res_view);
            }
        }

        auto shared_executor = [&elements, &res_view](milvus::BsonView bson,
                                                      uint32_t row_offset,
                                                      uint32_t value_offset) {
            auto array = bson.ParseAsArrayAtOffset(value_offset);
            if (!array.has_value()) {
                res_view[row_offset] = false;
                return;
            }

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
                            auto val =
                                milvus::BsonView::GetValueFromBsonView<int64_t>(
                                    sub_value.get_value());
                            if (!val.has_value()) {
                                continue;
                            }
                            if (val.value() == element.int64_val()) {
                                res_view[row_offset] = true;
                                return;
                            }
                            break;
                        }
                        case proto::plan::GenericValue::kFloatVal: {
                            auto val =
                                milvus::BsonView::GetValueFromBsonView<double>(
                                    sub_value.get_value());
                            if (!val.has_value()) {
                                continue;
                            }
                            if (val.value() == element.float_val()) {
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
                                bsoncxx::array::view>(sub_value.get_value());
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
                                      fmt::format("unsupported data type {}",
                                                  element.val_case()));
                    }
                }
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
    auto execute_sub_batch =
        [this](Index* index_ptr,
               const boost::container::vector<GetType>& vals) {
            switch (expr_->op_) {
                case proto::plan::JSONContainsExpr_JSONOp_Contains:
                case proto::plan::JSONContainsExpr_JSONOp_ContainsAny: {
                    return index_ptr->In(vals.size(), vals.data());
                }
                case proto::plan::JSONContainsExpr_JSONOp_ContainsAll: {
                    TargetBitmap result(index_ptr->Count());
                    result.set();
                    for (size_t i = 0; i < vals.size(); i++) {
                        auto sub = index_ptr->In(1, &vals[i]);
                        result &= sub;
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
    auto res = ProcessIndexChunks<GetType>(execute_sub_batch, elems);
    AssertInfo(res->size() == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               res->size(),
               real_batch_size);
    return res;
}
}  //namespace exec
}  // namespace milvus
