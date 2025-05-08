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
            if (is_index_mode_ && !has_offset_input_) {
                result = EvalJsonExistsForIndex();
            } else {
                result = EvalJsonExistsForDataSegment(context);
            }
            break;
        }
        default:
            PanicInfo(DataTypeInvalid,
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
        auto* index = segment_->GetJsonIndex(expr_->column_.field_id_, pointer);
        AssertInfo(index != nullptr,
                   "Cannot find json index with path: " + pointer);
        switch (index->GetCastType().data_type()) {
            case JsonCastType::DataType::DOUBLE: {
                auto* json_index =
                    dynamic_cast<index::JsonInvertedIndex<double>*>(index);
                cached_index_chunk_res_ = json_index->IsNotNull().clone();
                break;
            }

            case JsonCastType::DataType::VARCHAR: {
                auto* json_index =
                    dynamic_cast<index::JsonInvertedIndex<std::string>*>(index);
                cached_index_chunk_res_ = json_index->IsNotNull().clone();
                break;
            }

            case JsonCastType::DataType::BOOL: {
                auto* json_index =
                    dynamic_cast<index::JsonInvertedIndex<bool>*>(index);
                cached_index_chunk_res_ = json_index->IsNotNull().clone();
                break;
            }

            default:
                PanicInfo(DataTypeInvalid,
                          "unsupported data type: {}",
                          index->GetCastType());
        }
    }
    TargetBitmap res;
    res.append(
        cached_index_chunk_res_, current_index_chunk_pos_, real_batch_size);
    current_index_chunk_pos_ += real_batch_size;
    return std::make_shared<ColumnVector>(std::move(res),
                                          TargetBitmap(real_batch_size, true));
}

VectorPtr
PhyExistsFilterExpr::EvalJsonExistsForDataSegment(EvalCtx& context) {
    auto* input = context.get_offset_input();
    const auto& bitmap_input = context.get_bitmap_input();
    FieldId field_id = expr_->column_.field_id_;
    if (CanUseJsonKeyIndex(field_id) && !has_offset_input_) {
        return EvalJsonExistsForDataSegmentForIndex();
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
PhyExistsFilterExpr::EvalJsonExistsForDataSegmentForIndex() {
    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    auto pointer = milvus::Json::pointer(expr_->column_.nested_path_);
    if (cached_index_chunk_id_ != 0) {
        cached_index_chunk_id_ = 0;
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
        auto filter_func = [segment, field_id, pointer](bool valid,
                                                        uint8_t type,
                                                        uint32_t row_id,
                                                        uint16_t offset,
                                                        uint16_t size,
                                                        uint32_t value) {
            return true;
        };
        bool is_growing = segment_->type() == SegmentType::Growing;
        bool is_strong_consistency = consistency_level_ == 0;
        cached_index_chunk_res_ = index
                                      ->FilterByPath(pointer,
                                                     active_count_,
                                                     is_growing,
                                                     is_strong_consistency,
                                                     filter_func)
                                      .clone();
    }
    TargetBitmap result;
    result.append(
        cached_index_chunk_res_, current_data_global_pos_, real_batch_size);
    MoveCursor();
    return std::make_shared<ColumnVector>(std::move(result),
                                          TargetBitmap(real_batch_size, true));
}

}  //namespace exec
}  // namespace milvus
