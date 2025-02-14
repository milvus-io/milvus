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

namespace milvus {
namespace exec {

void
PhyExistsFilterExpr::Eval(EvalCtx& context, VectorPtr& result) {
    auto input = context.get_offset_input();
    SetHasOffsetInput((input != nullptr));
    switch (expr_->column_.data_type_) {
        case DataType::JSON: {
            if (is_index_mode_) {
                PanicInfo(ExprInvalid,
                          "exists expr for json index mode not supported");
            }
            result = EvalJsonExistsForDataSegment(input);
            break;
        }
        default:
            PanicInfo(DataTypeInvalid,
                      "unsupported data type: {}",
                      expr_->column_.data_type_);
    }
}

VectorPtr
PhyExistsFilterExpr::EvalJsonExistsForDataSegment(OffsetVector* input) {
    FieldId field_id = expr_->column_.field_id_;
    if (CanUseJsonKeyIndex(field_id) && !has_offset_input_) {
        return EvalJsonExistsForDataSegmentForIndex();
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
    auto execute_sub_batch =
        []<FilterType filter_type = FilterType::sequential>(
            const milvus::Json* data,
            const bool* valid_data,
            const int32_t* offsets,
            const int size,
            TargetBitmapView res,
            TargetBitmapView valid_res,
            const std::string& pointer) {
        for (int i = 0; i < size; ++i) {
            auto offset = i;
            if constexpr (filter_type == FilterType::random) {
                offset = (offsets) ? offsets[i] : i;
            }
            if (valid_data != nullptr && !valid_data[offset]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            res[i] = data[offset].exist(pointer);
        }
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
    auto real_batch_size = current_data_chunk_pos_ + batch_size_ > active_count_
                               ? active_count_ - current_data_chunk_pos_
                               : batch_size_;
    auto pointer = milvus::Json::pointer(expr_->column_.nested_path_);
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
        auto filter_func = [segment, field_id, pointer](uint32_t row_id,
                                                        uint16_t offset,
                                                        uint16_t size) {
            return true;
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

}  //namespace exec
}  // namespace milvus
