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
    switch (expr_->column_.data_type_) {
        case DataType::ARRAY: {
            if (is_index_mode_ && !has_offset_input_) {
                result = EvalArrayContainsForIndexSegment();
            } else {
                result = EvalJsonContainsForDataSegment(input);
            }
            break;
        }
        case DataType::JSON: {
            if (is_index_mode_ && !has_offset_input_) {
                PanicInfo(
                    ExprInvalid,
                    "exists expr for json or array index mode not supported");
            }
            result = EvalJsonContainsForDataSegment(input);
            break;
        }
        default:
            PanicInfo(DataTypeInvalid,
                      "unsupported data type: {}",
                      expr_->column_.data_type_);
    }
}

VectorPtr
PhyJsonContainsFilterExpr::EvalJsonContainsForDataSegment(OffsetVector* input) {
    auto data_type = expr_->column_.data_type_;
    switch (expr_->op_) {
        case proto::plan::JSONContainsExpr_JSONOp_Contains:
        case proto::plan::JSONContainsExpr_JSONOp_ContainsAny: {
            if (IsArrayDataType(data_type)) {
                auto val_type = expr_->vals_[0].val_case();
                switch (val_type) {
                    case proto::plan::GenericValue::kBoolVal: {
                        return ExecArrayContains<bool>(input);
                    }
                    case proto::plan::GenericValue::kInt64Val: {
                        return ExecArrayContains<int64_t>(input);
                    }
                    case proto::plan::GenericValue::kFloatVal: {
                        return ExecArrayContains<double>(input);
                    }
                    case proto::plan::GenericValue::kStringVal: {
                        return ExecArrayContains<std::string>(input);
                    }
                    default:
                        PanicInfo(
                            DataTypeInvalid,
                            fmt::format("unsupported data type {}", val_type));
                }
            } else {
                if (expr_->same_type_) {
                    auto val_type = expr_->vals_[0].val_case();
                    switch (val_type) {
                        case proto::plan::GenericValue::kBoolVal: {
                            return ExecJsonContains<bool>(input);
                        }
                        case proto::plan::GenericValue::kInt64Val: {
                            return ExecJsonContains<int64_t>(input);
                        }
                        case proto::plan::GenericValue::kFloatVal: {
                            return ExecJsonContains<double>(input);
                        }
                        case proto::plan::GenericValue::kStringVal: {
                            return ExecJsonContains<std::string>(input);
                        }
                        case proto::plan::GenericValue::kArrayVal: {
                            return ExecJsonContainsArray(input);
                        }
                        default:
                            PanicInfo(DataTypeInvalid,
                                      "unsupported data type:{}",
                                      val_type);
                    }
                } else {
                    return ExecJsonContainsWithDiffType(input);
                }
            }
        }
        case proto::plan::JSONContainsExpr_JSONOp_ContainsAll: {
            if (IsArrayDataType(data_type)) {
                auto val_type = expr_->vals_[0].val_case();
                switch (val_type) {
                    case proto::plan::GenericValue::kBoolVal: {
                        return ExecArrayContainsAll<bool>(input);
                    }
                    case proto::plan::GenericValue::kInt64Val: {
                        return ExecArrayContainsAll<int64_t>(input);
                    }
                    case proto::plan::GenericValue::kFloatVal: {
                        return ExecArrayContainsAll<double>(input);
                    }
                    case proto::plan::GenericValue::kStringVal: {
                        return ExecArrayContainsAll<std::string>(input);
                    }
                    default:
                        PanicInfo(
                            DataTypeInvalid,
                            fmt::format("unsupported data type {}", val_type));
                }
            } else {
                if (expr_->same_type_) {
                    auto val_type = expr_->vals_[0].val_case();
                    switch (val_type) {
                        case proto::plan::GenericValue::kBoolVal: {
                            return ExecJsonContainsAll<bool>(input);
                        }
                        case proto::plan::GenericValue::kInt64Val: {
                            return ExecJsonContainsAll<int64_t>(input);
                        }
                        case proto::plan::GenericValue::kFloatVal: {
                            return ExecJsonContainsAll<double>(input);
                        }
                        case proto::plan::GenericValue::kStringVal: {
                            return ExecJsonContainsAll<std::string>(input);
                        }
                        case proto::plan::GenericValue::kArrayVal: {
                            return ExecJsonContainsAllArray(input);
                        }
                        default:
                            PanicInfo(DataTypeInvalid,
                                      "unsupported data type:{}",
                                      val_type);
                    }
                } else {
                    return ExecJsonContainsAllWithDiffType(input);
                }
            }
        }
        default:
            PanicInfo(ExprInvalid,
                      "unsupported json contains type {}",
                      proto::plan::JSONContainsExpr_JSONOp_Name(expr_->op_));
    }
}

template <typename ExprValueType>
VectorPtr
PhyJsonContainsFilterExpr::ExecArrayContains(OffsetVector* input) {
    using GetType =
        std::conditional_t<std::is_same_v<ExprValueType, std::string>,
                           std::string_view,
                           ExprValueType>;
    auto real_batch_size =
        has_offset_input_ ? input->size() : GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }
    AssertInfo(expr_->column_.nested_path_.size() == 0,
               "[ExecArrayContains]nested path must be null");

    auto res_vec = std::make_shared<ColumnVector>(
        TargetBitmap(real_batch_size), TargetBitmap(real_batch_size));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);
    valid_res.set();

    std::unordered_set<GetType> elements;
    for (auto const& element : expr_->vals_) {
        elements.insert(GetValueFromProto<GetType>(element));
    }
    auto execute_sub_batch =
        []<FilterType filter_type = FilterType::sequential>(
            const milvus::ArrayView* data,
            const bool* valid_data,
            const int32_t* offsets,
            const int size,
            TargetBitmapView res,
            TargetBitmapView valid_res,
            const std::unordered_set<GetType>& elements) {
        auto executor = [&](size_t i) {
            const auto& array = data[i];
            for (int j = 0; j < array.length(); ++j) {
                if (elements.count(array.template get_data<GetType>(j)) > 0) {
                    return true;
                }
            }
            return false;
        };
        for (int i = 0; i < size; ++i) {
            auto offset = i;
            if constexpr (filter_type == FilterType::random) {
                offset = (offsets) ? offsets[i] : i;
            }
            if (valid_data != nullptr && !valid_data[offset]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            res[i] = executor(offset);
        }
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
PhyJsonContainsFilterExpr::ExecJsonContains(OffsetVector* input) {
    using GetType =
        std::conditional_t<std::is_same_v<ExprValueType, std::string>,
                           std::string_view,
                           ExprValueType>;

    FieldId field_id = expr_->column_.field_id_;
    if (CanUseJsonKeyIndex(field_id) && !has_offset_input_) {
        return ExecJsonContainsByKeyIndex<ExprValueType>();
    }

    auto real_batch_size =
        has_offset_input_ ? input->size() : GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    auto res_vec = std::make_shared<ColumnVector>(
        TargetBitmap(real_batch_size), TargetBitmap(real_batch_size));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);
    valid_res.set();

    std::unordered_set<GetType> elements;
    auto pointer = milvus::Json::pointer(expr_->column_.nested_path_);
    for (auto const& element : expr_->vals_) {
        elements.insert(GetValueFromProto<GetType>(element));
    }
    auto execute_sub_batch =
        []<FilterType filter_type = FilterType::sequential>(
            const milvus::Json* data,
            const bool* valid_data,
            const int32_t* offsets,
            const int size,
            TargetBitmapView res,
            TargetBitmapView valid_res,
            const std::string& pointer,
            const std::unordered_set<GetType>& elements) {
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
                if (elements.count(val.value()) > 0) {
                    return true;
                }
            }
            return false;
        };
        for (size_t i = 0; i < size; ++i) {
            auto offset = i;
            if constexpr (filter_type == FilterType::random) {
                offset = (offsets) ? offsets[i] : i;
            }
            if (valid_data != nullptr && !valid_data[offset]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            res[i] = executor(offset);
        }
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
PhyJsonContainsFilterExpr::ExecJsonContainsByKeyIndex() {
    using GetType =
        std::conditional_t<std::is_same_v<ExprValueType, std::string>,
                           std::string_view,
                           ExprValueType>;
    auto real_batch_size = current_data_chunk_pos_ + batch_size_ > active_count_
                               ? active_count_ - current_data_chunk_pos_
                               : batch_size_;
    std::unordered_set<GetType> elements;
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
    if (cached_index_chunk_id_ != 0) {
        const segcore::SegmentInternalInterface* segment = nullptr;
        if (segment_->type() == SegmentType::Growing) {
            segment =
                dynamic_cast<const segcore::SegmentGrowingImpl*>(segment_);
        } else if (segment_->type() == SegmentType::Sealed) {
            segment = dynamic_cast<const segcore::SegmentSealed*>(segment_);
        }
        auto field_id = expr_->column_.field_id_;
        auto* index = segment->GetJsonKeyIndex(field_id);
        Assert(index != nullptr);
        auto filter_func = [segment, &elements, &field_id](uint32_t row_id,
                                                           uint16_t offset,
                                                           uint16_t size) {
            auto json_pair = segment->GetJsonData(field_id, row_id);
            if (!json_pair.second) {
                return false;
            }
            auto json =
                milvus::Json(json_pair.first.data(), json_pair.first.size());
            auto array = json.array_at(offset, size);

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
        bool is_growing = segment_->type() == SegmentType::Growing;
        cached_index_chunk_res_ =
            index->FilterByPath(pointer, active_count_, is_growing, filter_func)
                .clone();
        cached_index_chunk_id_ = 0;
    }
    TargetBitmap result;
    result.append(
        cached_index_chunk_res_, current_data_chunk_pos_, real_batch_size);
    current_data_chunk_pos_ += real_batch_size;
    return std::make_shared<ColumnVector>(std::move(result),
                                          TargetBitmap(real_batch_size, true));
}

VectorPtr
PhyJsonContainsFilterExpr::ExecJsonContainsArray(OffsetVector* input) {
    FieldId field_id = expr_->column_.field_id_;
    if (CanUseJsonKeyIndex(field_id) && !has_offset_input_) {
        return ExecJsonContainsArrayByKeyIndex();
    }
    auto real_batch_size =
        has_offset_input_ ? input->size() : GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    auto res_vec = std::make_shared<ColumnVector>(
        TargetBitmap(real_batch_size), TargetBitmap(real_batch_size));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);
    valid_res.set();

    auto pointer = milvus::Json::pointer(expr_->column_.nested_path_);
    std::vector<proto::plan::Array> elements;
    for (auto const& element : expr_->vals_) {
        elements.emplace_back(GetValueFromProto<proto::plan::Array>(element));
    }
    auto execute_sub_batch =
        []<FilterType filter_type = FilterType::sequential>(
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
        for (size_t i = 0; i < size; ++i) {
            auto offset = i;
            if constexpr (filter_type == FilterType::random) {
                offset = (offsets) ? offsets[i] : i;
            }
            if (valid_data != nullptr && !valid_data[offset]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            res[i] = executor(offset);
        }
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
PhyJsonContainsFilterExpr::ExecJsonContainsArrayByKeyIndex() {
    auto real_batch_size = current_data_chunk_pos_ + batch_size_ > active_count_
                               ? active_count_ - current_data_chunk_pos_
                               : batch_size_;
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
    if (cached_index_chunk_id_ != 0) {
        const segcore::SegmentInternalInterface* segment = nullptr;
        if (segment_->type() == SegmentType::Growing) {
            segment =
                dynamic_cast<const segcore::SegmentGrowingImpl*>(segment_);
        } else if (segment_->type() == SegmentType::Sealed) {
            segment = dynamic_cast<const segcore::SegmentSealed*>(segment_);
        }
        auto field_id = expr_->column_.field_id_;
        auto* index = segment->GetJsonKeyIndex(field_id);
        Assert(index != nullptr);
        auto filter_func = [segment, &elements, &field_id](uint32_t row_id,
                                                           uint16_t offset,
                                                           uint16_t size) {
            auto json_pair = segment->GetJsonData(field_id, row_id);
            if (!json_pair.second) {
                return false;
            }
            auto json =
                milvus::Json(json_pair.first.data(), json_pair.first.size());
            auto array = json.array_at(offset, size);
            if (array.error()) {
                return false;
            }
            for (auto&& it : array) {
                auto val = it.get_array();
                if (val.error()) {
                    continue;
                }
                for (auto const& element : elements) {
                    if (CompareTwoJsonArray(val, element)) {
                        return true;
                    }
                }
            }
            return false;
        };
        bool is_growing = segment_->type() == SegmentType::Growing;
        cached_index_chunk_res_ =
            index->FilterByPath(pointer, active_count_, is_growing, filter_func)
                .clone();
        cached_index_chunk_id_ = 0;
    }
    TargetBitmap result;
    result.append(
        cached_index_chunk_res_, current_data_chunk_pos_, real_batch_size);
    current_data_chunk_pos_ += real_batch_size;
    return std::make_shared<ColumnVector>(std::move(result),
                                          TargetBitmap(real_batch_size, true));
}

template <typename ExprValueType>
VectorPtr
PhyJsonContainsFilterExpr::ExecArrayContainsAll(OffsetVector* input) {
    using GetType =
        std::conditional_t<std::is_same_v<ExprValueType, std::string>,
                           std::string_view,
                           ExprValueType>;
    AssertInfo(expr_->column_.nested_path_.size() == 0,
               "[ExecArrayContainsAll]nested path must be null");
    auto real_batch_size =
        has_offset_input_ ? input->size() : GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    auto res_vec = std::make_shared<ColumnVector>(
        TargetBitmap(real_batch_size), TargetBitmap(real_batch_size));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);
    valid_res.set();

    std::unordered_set<GetType> elements;
    for (auto const& element : expr_->vals_) {
        elements.insert(GetValueFromProto<GetType>(element));
    }

    auto execute_sub_batch =
        []<FilterType filter_type = FilterType::sequential>(
            const milvus::ArrayView* data,
            const bool* valid_data,
            const int32_t* offsets,
            const int size,
            TargetBitmapView res,
            TargetBitmapView valid_res,
            const std::unordered_set<GetType>& elements) {
        auto executor = [&](size_t i) {
            std::unordered_set<GetType> tmp_elements(elements);
            // Note: array can only be iterated once
            for (int j = 0; j < data[i].length(); ++j) {
                tmp_elements.erase(data[i].template get_data<GetType>(j));
                if (tmp_elements.size() == 0) {
                    return true;
                }
            }
            return tmp_elements.size() == 0;
        };
        for (int i = 0; i < size; ++i) {
            auto offset = i;
            if constexpr (filter_type == FilterType::random) {
                offset = (offsets) ? offsets[i] : i;
            }
            if (valid_data != nullptr && !valid_data[offset]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            res[i] = executor(offset);
        }
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
PhyJsonContainsFilterExpr::ExecJsonContainsAll(OffsetVector* input) {
    using GetType =
        std::conditional_t<std::is_same_v<ExprValueType, std::string>,
                           std::string_view,
                           ExprValueType>;

    FieldId field_id = expr_->column_.field_id_;
    if (CanUseJsonKeyIndex(field_id) && !has_offset_input_) {
        return ExecJsonContainsAllByKeyIndex<ExprValueType>();
    }
    auto real_batch_size =
        has_offset_input_ ? input->size() : GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    auto res_vec = std::make_shared<ColumnVector>(
        TargetBitmap(real_batch_size), TargetBitmap(real_batch_size));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);
    valid_res.set();

    auto pointer = milvus::Json::pointer(expr_->column_.nested_path_);
    std::unordered_set<GetType> elements;
    for (auto const& element : expr_->vals_) {
        elements.insert(GetValueFromProto<GetType>(element));
    }

    auto execute_sub_batch =
        []<FilterType filter_type = FilterType::sequential>(
            const milvus::Json* data,
            const bool* valid_data,
            const int32_t* offsets,
            const int size,
            TargetBitmapView res,
            TargetBitmapView valid_res,
            const std::string& pointer,
            const std::unordered_set<GetType>& elements) {
        auto executor = [&](const size_t i) -> bool {
            auto doc = data[i].doc();
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
        for (size_t i = 0; i < size; ++i) {
            auto offset = i;
            if constexpr (filter_type == FilterType::random) {
                offset = (offsets) ? offsets[i] : i;
            }
            if (valid_data != nullptr && !valid_data[offset]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            res[i] = executor(offset);
        }
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
PhyJsonContainsFilterExpr::ExecJsonContainsAllByKeyIndex() {
    using GetType =
        std::conditional_t<std::is_same_v<ExprValueType, std::string>,
                           std::string_view,
                           ExprValueType>;
    auto real_batch_size = current_data_chunk_pos_ + batch_size_ > active_count_
                               ? active_count_ - current_data_chunk_pos_
                               : batch_size_;
    std::unordered_set<GetType> elements;
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
    if (cached_index_chunk_id_ != 0) {
        const segcore::SegmentInternalInterface* segment = nullptr;
        if (segment_->type() == SegmentType::Growing) {
            segment =
                dynamic_cast<const segcore::SegmentGrowingImpl*>(segment_);
        } else if (segment_->type() == SegmentType::Sealed) {
            segment = dynamic_cast<const segcore::SegmentSealed*>(segment_);
        }
        auto field_id = expr_->column_.field_id_;
        auto* index = segment->GetJsonKeyIndex(field_id);
        Assert(index != nullptr);
        auto filter_func = [segment, &elements, &field_id](uint32_t row_id,
                                                           uint16_t offset,
                                                           uint16_t size) {
            auto json_pair = segment->GetJsonData(field_id, row_id);
            if (!json_pair.second) {
                return false;
            }
            auto json =
                milvus::Json(json_pair.first.data(), json_pair.first.size());
            auto array = json.array_at(offset, size);
            if (array.error()) {
                return false;
            }
            std::unordered_set<GetType> tmp_elements(elements);
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
            return tmp_elements.empty();
        };
        bool is_growing = segment_->type() == SegmentType::Growing;
        cached_index_chunk_res_ =
            index->FilterByPath(pointer, active_count_, is_growing, filter_func)
                .clone();
        cached_index_chunk_id_ = 0;
    }
    TargetBitmap result;
    result.append(
        cached_index_chunk_res_, current_data_chunk_pos_, real_batch_size);
    current_data_chunk_pos_ += real_batch_size;
    return std::make_shared<ColumnVector>(std::move(result),
                                          TargetBitmap(real_batch_size, true));
}

VectorPtr
PhyJsonContainsFilterExpr::ExecJsonContainsAllWithDiffType(
    OffsetVector* input) {
    FieldId field_id = expr_->column_.field_id_;
    if (CanUseJsonKeyIndex(field_id) && !has_offset_input_) {
        return ExecJsonContainsAllWithDiffTypeByKeyIndex();
    }
    auto real_batch_size =
        has_offset_input_ ? input->size() : GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }
    auto res_vec = std::make_shared<ColumnVector>(
        TargetBitmap(real_batch_size), TargetBitmap(real_batch_size));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);
    valid_res.set();

    auto pointer = milvus::Json::pointer(expr_->column_.nested_path_);

    auto elements = expr_->vals_;
    std::unordered_set<int> elements_index;
    int i = 0;
    for (auto& element : elements) {
        elements_index.insert(i);
        i++;
    }

    auto execute_sub_batch =
        []<FilterType filter_type = FilterType::sequential>(
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
                            PanicInfo(DataTypeInvalid,
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
        for (size_t i = 0; i < size; ++i) {
            auto offset = i;
            if constexpr (filter_type == FilterType::random) {
                offset = (offsets) ? offsets[i] : i;
            }
            if (valid_data != nullptr && !valid_data[offset]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            res[i] = executor(offset);
        }
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
PhyJsonContainsFilterExpr::ExecJsonContainsAllWithDiffTypeByKeyIndex() {
    auto real_batch_size = current_data_chunk_pos_ + batch_size_ > active_count_
                               ? active_count_ - current_data_chunk_pos_
                               : batch_size_;
    auto pointer = milvus::Json::pointer(expr_->column_.nested_path_);
    auto elements = expr_->vals_;
    std::unordered_set<int> elements_index;
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
    if (cached_index_chunk_id_ != 0) {
        const segcore::SegmentInternalInterface* segment = nullptr;
        if (segment_->type() == SegmentType::Growing) {
            segment =
                dynamic_cast<const segcore::SegmentGrowingImpl*>(segment_);
        } else if (segment_->type() == SegmentType::Sealed) {
            segment = dynamic_cast<const segcore::SegmentSealed*>(segment_);
        }
        auto field_id = expr_->column_.field_id_;
        auto* index = segment->GetJsonKeyIndex(field_id);
        Assert(index != nullptr);
        auto filter_func = [segment, &elements, &elements_index, &field_id](
                               uint32_t row_id,
                               uint16_t offset,
                               uint16_t size) {
            auto json_pair = segment->GetJsonData(field_id, row_id);
            if (!json_pair.second) {
                return false;
            }
            auto json =
                milvus::Json(json_pair.first.data(), json_pair.first.size());
            std::unordered_set<int> tmp_elements_index(elements_index);
            auto array = json.array_at(offset, size);
            if (array.error()) {
                return false;
            }
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
        bool is_growing = segment_->type() == SegmentType::Growing;
        cached_index_chunk_res_ =
            index->FilterByPath(pointer, active_count_, is_growing, filter_func)
                .clone();
        cached_index_chunk_id_ = 0;
    }
    TargetBitmap result;
    result.append(
        cached_index_chunk_res_, current_data_chunk_pos_, real_batch_size);
    current_data_chunk_pos_ += real_batch_size;
    return std::make_shared<ColumnVector>(std::move(result),
                                          TargetBitmap(real_batch_size, true));
}

VectorPtr
PhyJsonContainsFilterExpr::ExecJsonContainsAllArray(OffsetVector* input) {
    FieldId field_id = expr_->column_.field_id_;
    if (CanUseJsonKeyIndex(field_id) && !has_offset_input_) {
        return ExecJsonContainsAllArrayByKeyIndex();
    }
    auto real_batch_size =
        has_offset_input_ ? input->size() : GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    auto res_vec = std::make_shared<ColumnVector>(
        TargetBitmap(real_batch_size), TargetBitmap(real_batch_size));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);
    valid_res.set();

    auto pointer = milvus::Json::pointer(expr_->column_.nested_path_);

    std::vector<proto::plan::Array> elements;
    for (auto const& element : expr_->vals_) {
        elements.emplace_back(GetValueFromProto<proto::plan::Array>(element));
    }
    auto execute_sub_batch =
        []<FilterType filter_type = FilterType::sequential>(
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
        for (size_t i = 0; i < size; ++i) {
            auto offset = i;
            if constexpr (filter_type == FilterType::random) {
                offset = (offsets) ? offsets[i] : i;
            }
            if (valid_data != nullptr && !valid_data[offset]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            res[i] = executor(offset);
        }
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
PhyJsonContainsFilterExpr::ExecJsonContainsAllArrayByKeyIndex() {
    auto real_batch_size = current_data_chunk_pos_ + batch_size_ > active_count_
                               ? active_count_ - current_data_chunk_pos_
                               : batch_size_;
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
    if (cached_index_chunk_id_ != 0) {
        const segcore::SegmentInternalInterface* segment = nullptr;
        if (segment_->type() == SegmentType::Growing) {
            segment =
                dynamic_cast<const segcore::SegmentGrowingImpl*>(segment_);
        } else if (segment_->type() == SegmentType::Sealed) {
            segment = dynamic_cast<const segcore::SegmentSealed*>(segment_);
        }
        auto field_id = expr_->column_.field_id_;
        auto* index = segment->GetJsonKeyIndex(field_id);
        Assert(index != nullptr);
        auto filter_func = [segment, &elements, &field_id](uint32_t row_id,
                                                           uint16_t offset,
                                                           uint16_t size) {
            auto json_pair = segment->GetJsonData(field_id, row_id);
            if (!json_pair.second) {
                return false;
            }
            auto json =
                milvus::Json(json_pair.first.data(), json_pair.first.size());
            auto array = json.array_at(offset, size);
            if (array.error()) {
                return false;
            }
            std::unordered_set<int> exist_elements_index;
            for (auto&& it : array) {
                auto json_array = it.get_array();
                if (json_array.error()) {
                    continue;
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
        bool is_growing = segment_->type() == SegmentType::Growing;
        cached_index_chunk_res_ =
            index->FilterByPath(pointer, active_count_, is_growing, filter_func)
                .clone();
        cached_index_chunk_id_ = 0;
    }
    TargetBitmap result;
    result.append(
        cached_index_chunk_res_, current_data_chunk_pos_, real_batch_size);
    current_data_chunk_pos_ += real_batch_size;
    return std::make_shared<ColumnVector>(std::move(result),
                                          TargetBitmap(real_batch_size, true));
}

VectorPtr
PhyJsonContainsFilterExpr::ExecJsonContainsWithDiffType(OffsetVector* input) {
    FieldId field_id = expr_->column_.field_id_;
    if (CanUseJsonKeyIndex(field_id) && !has_offset_input_) {
        return ExecJsonContainsWithDiffTypeByKeyIndex();
    }
    auto real_batch_size =
        has_offset_input_ ? input->size() : GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    auto res_vec = std::make_shared<ColumnVector>(
        TargetBitmap(real_batch_size), TargetBitmap(real_batch_size));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);
    valid_res.set();

    auto pointer = milvus::Json::pointer(expr_->column_.nested_path_);

    auto elements = expr_->vals_;
    std::unordered_set<int> elements_index;
    int i = 0;
    for (auto& element : elements) {
        elements_index.insert(i);
        i++;
    }

    auto execute_sub_batch =
        []<FilterType filter_type = FilterType::sequential>(
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
                            PanicInfo(DataTypeInvalid,
                                      fmt::format("unsupported data type {}",
                                                  element.val_case()));
                    }
                }
            }
            return false;
        };
        for (size_t i = 0; i < size; ++i) {
            auto offset = i;
            if constexpr (filter_type == FilterType::random) {
                offset = (offsets) ? offsets[i] : i;
            }
            if (valid_data != nullptr && !valid_data[offset]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            res[i] = executor(offset);
        }
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
PhyJsonContainsFilterExpr::ExecJsonContainsWithDiffTypeByKeyIndex() {
    auto real_batch_size = current_data_chunk_pos_ + batch_size_ > active_count_
                               ? active_count_ - current_data_chunk_pos_
                               : batch_size_;
    auto pointer = milvus::Json::pointer(expr_->column_.nested_path_);
    auto elements = expr_->vals_;
    if (elements.empty()) {
        MoveCursor();
        return std::make_shared<ColumnVector>(
            TargetBitmap(real_batch_size, false),
            TargetBitmap(real_batch_size, true));
    }
    if (cached_index_chunk_id_ != 0) {
        const segcore::SegmentInternalInterface* segment = nullptr;
        if (segment_->type() == SegmentType::Growing) {
            segment =
                dynamic_cast<const segcore::SegmentGrowingImpl*>(segment_);
        } else if (segment_->type() == SegmentType::Sealed) {
            segment = dynamic_cast<const segcore::SegmentSealed*>(segment_);
        }
        auto field_id = expr_->column_.field_id_;
        auto* index = segment->GetJsonKeyIndex(field_id);
        Assert(index != nullptr);
        auto filter_func = [segment, &elements, &field_id](uint32_t row_id,
                                                           uint16_t offset,
                                                           uint16_t size) {
            auto json_pair = segment->GetJsonData(field_id, row_id);
            if (!json_pair.second) {
                return false;
            }
            auto json =
                milvus::Json(json_pair.first.data(), json_pair.first.size());
            auto array = json.array_at(offset, size);
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
                                      fmt::format("unsupported data type {}",
                                                  element.val_case()));
                    }
                }
            }
            return false;
        };
        bool is_growing = segment_->type() == SegmentType::Growing;
        cached_index_chunk_res_ =
            index->FilterByPath(pointer, active_count_, is_growing, filter_func)
                .clone();
        cached_index_chunk_id_ = 0;
    }
    TargetBitmap result;
    result.append(
        cached_index_chunk_res_, current_data_chunk_pos_, real_batch_size);
    current_data_chunk_pos_ += real_batch_size;
    return std::make_shared<ColumnVector>(std::move(result),
                                          TargetBitmap(real_batch_size, true));
}

VectorPtr
PhyJsonContainsFilterExpr::EvalArrayContainsForIndexSegment() {
    switch (expr_->column_.element_type_) {
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
            PanicInfo(DataTypeInvalid,
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
        elements.insert(GetValueFromProto<GetType>(element));
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
                    PanicInfo(
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
