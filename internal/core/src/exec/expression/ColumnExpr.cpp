// Licensed to the LF AI & Data foundation under on0
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

#include "ColumnExpr.h"

namespace milvus {
namespace exec {

int64_t
PhyColumnExpr::GetNextBatchSize() {
    auto current_rows = GetCurrentRows();

    return current_rows + batch_size_ >= segment_chunk_reader_.active_count_
               ? segment_chunk_reader_.active_count_ - current_rows
               : batch_size_;
}

void
PhyColumnExpr::Eval(EvalCtx& context, VectorPtr& result) {
    auto input = context.get_offset_input();
    SetHasOffsetInput(input != nullptr);
    switch (this->expr_->type()) {
        case DataType::BOOL:
            result = DoEval<bool>(input);
            break;
        case DataType::INT8:
            result = DoEval<int8_t>(input);
            break;
        case DataType::INT16:
            result = DoEval<int16_t>(input);
            break;
        case DataType::INT32:
            result = DoEval<int32_t>(input);
            break;
        case DataType::INT64:
            result = DoEval<int64_t>(input);
            break;
        case DataType::FLOAT:
            result = DoEval<float>(input);
            break;
        case DataType::DOUBLE:
            result = DoEval<double>(input);
            break;
        case DataType::VARCHAR: {
            result = DoEval<std::string>(input);
            break;
        }
        default:
            PanicInfo(DataTypeInvalid,
                      "unsupported data type: {}",
                      this->expr_->type());
    }
}

template <typename T>
VectorPtr
PhyColumnExpr::DoEval(OffsetVector* input) {
    // similar to PhyCompareFilterExpr::ExecCompareExprDispatcher(OpType op)
    // take offsets as input
    if (has_offset_input_) {
        auto real_batch_size = input->size();
        if (real_batch_size == 0) {
            return nullptr;
        }

        auto res_vec = std::make_shared<ColumnVector>(
            expr_->GetColumn().data_type_, real_batch_size);
        T* res_value = res_vec->RawAsValues<T>();
        TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);
        valid_res.set();

        auto data_barrier = segment_chunk_reader_.segment_->num_chunk_data(
            expr_->GetColumn().field_id_);

        int64_t processed_rows = 0;
        const auto size_per_chunk = segment_chunk_reader_.SizePerChunk();
        for (auto i = 0; i < real_batch_size; ++i) {
            auto offset = (*input)[i];
            auto [chunk_id,
                  chunk_offset] = [&]() -> std::pair<int64_t, int64_t> {
                if (segment_chunk_reader_.segment_->type() ==
                    SegmentType::Growing) {
                    return {offset / size_per_chunk, offset % size_per_chunk};
                } else if (segment_chunk_reader_.segment_->is_chunked() &&
                           data_barrier > 0) {
                    return segment_chunk_reader_.segment_->get_chunk_by_offset(
                        expr_->GetColumn().field_id_, offset);
                } else {
                    return {0, offset};
                }
            }();
            auto chunk_data = segment_chunk_reader_.GetChunkDataAccessor(
                expr_->GetColumn().data_type_,
                expr_->GetColumn().field_id_,
                chunk_id,
                data_barrier);
            auto chunk_data_by_offset = chunk_data(chunk_offset);
            if (!chunk_data_by_offset.has_value()) {
                valid_res[processed_rows] = false;
            } else {
                res_value[processed_rows] =
                    boost::get<T>(chunk_data_by_offset.value());
            }
            processed_rows++;
        }
        return res_vec;
    }

    // normal path
    if (segment_chunk_reader_.segment_->is_chunked()) {
        auto real_batch_size = GetNextBatchSize();
        if (real_batch_size == 0) {
            return nullptr;
        }

        auto res_vec = std::make_shared<ColumnVector>(
            expr_->GetColumn().data_type_, real_batch_size);
        T* res_value = res_vec->RawAsValues<T>();
        TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);
        valid_res.set();
        auto chunk_data = segment_chunk_reader_.GetChunkDataAccessor(
            expr_->GetColumn().data_type_,
            expr_->GetColumn().field_id_,
            is_indexed_,
            current_chunk_id_,
            current_chunk_pos_);
        for (int i = 0; i < real_batch_size; ++i) {
            auto data = chunk_data();
            if (!data.has_value()) {
                valid_res[i] = false;
                continue;
            }
            res_value[i] = boost::get<T>(data.value());
        }
        return res_vec;
    } else {
        auto real_batch_size = GetNextBatchSize();
        if (real_batch_size == 0) {
            return nullptr;
        }

        auto res_vec = std::make_shared<ColumnVector>(
            expr_->GetColumn().data_type_, real_batch_size);
        T* res_value = res_vec->RawAsValues<T>();
        TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);
        valid_res.set();

        auto data_barrier = segment_chunk_reader_.segment_->num_chunk_data(
            expr_->GetColumn().field_id_);

        int64_t processed_rows = 0;
        for (int64_t chunk_id = current_chunk_id_; chunk_id < num_chunk_;
             ++chunk_id) {
            auto chunk_size =
                chunk_id == num_chunk_ - 1
                    ? segment_chunk_reader_.active_count_ -
                          chunk_id * segment_chunk_reader_.SizePerChunk()
                    : segment_chunk_reader_.SizePerChunk();
            auto chunk_data = segment_chunk_reader_.GetChunkDataAccessor(
                expr_->GetColumn().data_type_,
                expr_->GetColumn().field_id_,
                chunk_id,
                data_barrier);

            for (int i = chunk_id == current_chunk_id_ ? current_chunk_pos_ : 0;
                 i < chunk_size;
                 ++i) {
                if (!chunk_data(i).has_value()) {
                    valid_res[processed_rows] = false;
                } else {
                    res_value[processed_rows] =
                        boost::get<T>(chunk_data(i).value());
                }
                processed_rows++;

                if (processed_rows >= batch_size_) {
                    current_chunk_id_ = chunk_id;
                    current_chunk_pos_ = i + 1;
                    return res_vec;
                }
            }
        }
        return res_vec;
    }
}

}  //namespace exec
}  // namespace milvus
