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

#include "ExistsExpr.h"
#include "common/Json.h"
#include "common/JsonCastType.h"
#include "common/Types.h"
#include "common/Vector.h"
#include "index/JsonInvertedIndex.h"

namespace milvus {
namespace exec {

void
PhyExistsFilterExpr::Eval(EvalCtx& context, VectorPtr& result) {
    context.set_apply_valid_data_after_flip(false);
    auto input = context.get_offset_input();
    SetHasOffsetInput((input != nullptr));
    switch (expr_->column_.data_type_) {
        case DataType::JSON: {
            if (SegmentExpr::CanUseIndex() && !has_offset_input_) {
                result = EvalJsonExistsForIndex();
            } else {
                result = EvalJsonExistsForDataSegment(context);
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
PhyExistsFilterExpr::EvalJsonExistsForIndex() {
    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    if (cached_index_chunk_id_ != 0) {
        cached_index_chunk_id_ = 0;
        auto pointer = milvus::Json::pointer(expr_->column_.nested_path_);
        auto* index = pinned_index_[cached_index_chunk_id_].get();
        AssertInfo(index != nullptr,
                   "Cannot find json index with path: " + pointer);
        switch (index->GetCastType().data_type()) {
            case JsonCastType::DataType::DOUBLE: {
                auto* json_index =
                    const_cast<index::JsonInvertedIndex<double>*>(
                        dynamic_cast<const index::JsonInvertedIndex<double>*>(
                            index));
                cached_index_chunk_res_ = std::make_shared<TargetBitmap>(
                    std::move(json_index->Exists()));
                break;
            }

            case JsonCastType::DataType::VARCHAR: {
                auto* json_index = const_cast<
                    index::JsonInvertedIndex<std::string>*>(
                    dynamic_cast<const index::JsonInvertedIndex<std::string>*>(
                        index));
                cached_index_chunk_res_ = std::make_shared<TargetBitmap>(
                    std::move(json_index->Exists()));
                break;
            }

            case JsonCastType::DataType::BOOL: {
                auto* json_index = const_cast<index::JsonInvertedIndex<bool>*>(
                    dynamic_cast<const index::JsonInvertedIndex<bool>*>(index));
                cached_index_chunk_res_ = std::make_shared<TargetBitmap>(
                    std::move(json_index->Exists()));
                break;
            }

            case JsonCastType::DataType::JSON: {
                auto* json_flat_index = const_cast<index::JsonFlatIndex*>(
                    dynamic_cast<const index::JsonFlatIndex*>(index));
                auto executor =
                    json_flat_index->create_executor<double>(pointer);
                cached_index_chunk_res_ = std::make_shared<TargetBitmap>(
                    std::move(executor->IsNotNull()));
                break;
            }

            default:
                ThrowInfo(DataTypeInvalid,
                          "unsupported data type: {}",
                          index->GetCastType());
        }
    }
    TargetBitmap res;
    res.append(
        *cached_index_chunk_res_, current_index_chunk_pos_, real_batch_size);
    current_index_chunk_pos_ += real_batch_size;
    return std::make_shared<ColumnVector>(std::move(res),
                                          TargetBitmap(real_batch_size, true));
}

VectorPtr
PhyExistsFilterExpr::EvalJsonExistsForDataSegment(EvalCtx& context) {
    auto* input = context.get_offset_input();
    const auto& bitmap_input = context.get_bitmap_input();
    FieldId field_id = expr_->column_.field_id_;
    if (CanUseJsonStats(context, field_id) && !has_offset_input_) {
        return EvalJsonExistsForDataSegmentByStats();
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
    int processed_cursor = 0;
    auto execute_sub_batch =
        [&bitmap_input, &
         processed_cursor ]<FilterType filter_type = FilterType::sequential>(
            const milvus::Json* data,
            const bool* valid_data,
            const int32_t* offsets,
            const int size,
            TargetBitmapView res,
            TargetBitmapView valid_res,
            const std::string& pointer) {
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
            res[i] = data[offset].exist(pointer);
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
                                                    pointer);
    } else {
        processed_size = ProcessDataChunks<Json>(
            execute_sub_batch, std::nullptr_t{}, res, valid_res, pointer);
    }
    AssertInfo(processed_size == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
}

VectorPtr
PhyExistsFilterExpr::EvalJsonExistsForDataSegmentByStats() {
    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    auto pointer = milvus::index::JsonPointer(expr_->column_.nested_path_);
    if (cached_index_chunk_id_ != 0 &&
        segment_->type() == SegmentType::Sealed) {
        cached_index_chunk_id_ = 0;
        auto segment = static_cast<const segcore::SegmentSealed*>(segment_);
        auto field_id = expr_->column_.field_id_;
        pinned_json_stats_ = segment->GetJsonStats(op_ctx_, field_id);
        auto* index = pinned_json_stats_.get();
        Assert(index != nullptr);

        cached_index_chunk_res_ = std::make_shared<TargetBitmap>(active_count_);
        cached_index_chunk_valid_res_ =
            std::make_shared<TargetBitmap>(active_count_, true);
        TargetBitmapView res_view(*cached_index_chunk_res_);
        TargetBitmapView valid_res_view(*cached_index_chunk_valid_res_);

        // process shredding data
        auto shredding_fields = index->GetShreddingFields(pointer);
        for (const auto& field : shredding_fields) {
            index->ExecutorForGettingValid(op_ctx_, field, valid_res_view);
            res_view |= valid_res_view;
        }

        if (!index->CanSkipShared(pointer)) {
            // process shared data
            index->ExecuteExistsPathForSharedData(pointer, res_view);
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

}  //namespace exec
}  // namespace milvus
