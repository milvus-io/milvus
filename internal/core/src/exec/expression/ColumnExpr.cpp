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

#include "ColumnExpr.h"

namespace milvus {
namespace exec {

template <typename T>
ChunkDataAccessor
PhyColumnExpr::GetChunkData(FieldId field_id, int chunk_id, int data_barrier) {
    if (is_indexed_ && chunk_id >= data_barrier) {
        auto& indexing = segment_->chunk_scalar_index<T>(field_id, chunk_id);
        if (indexing.HasRawData()) {
            return [&indexing](int i) -> const number {
                return indexing.Reverse_Lookup(i);
            };
        }
    }
    auto chunk_data = segment_->chunk_data<T>(field_id, chunk_id).data();
    return [chunk_data](int i) -> const number { return chunk_data[i]; };
}

template <>
ChunkDataAccessor
PhyColumnExpr::GetChunkData<std::string>(FieldId field_id,
                                         int chunk_id,
                                         int data_barrier) {
    if (is_indexed_ && chunk_id >= data_barrier) {
        auto& indexing =
            segment_->chunk_scalar_index<std::string>(field_id, chunk_id);
        if (indexing.HasRawData()) {
            return [&indexing](int i) -> const std::string {
                return indexing.Reverse_Lookup(i);
            };
        }
    }
    if (segment_->type() == SegmentType::Growing &&
        !storage::MmapManager::GetInstance()
             .GetMmapConfig()
             .growing_enable_mmap) {
        auto chunk_data =
            segment_->chunk_data<std::string>(field_id, chunk_id).data();
        return [chunk_data](int i) -> const number { return chunk_data[i]; };
    } else {
        auto chunk_data =
            segment_->chunk_view<std::string_view>(field_id, chunk_id)
                .first.data();
        return [chunk_data](int i) -> const number {
            return std::string(chunk_data[i]);
        };
    }
}

ChunkDataAccessor
PhyColumnExpr::GetChunkData(DataType data_type,
                            FieldId field_id,
                            int chunk_id,
                            int data_barrier) {
    switch (data_type) {
        case DataType::BOOL:
            return GetChunkData<bool>(field_id, chunk_id, data_barrier);
        case DataType::INT8:
            return GetChunkData<int8_t>(field_id, chunk_id, data_barrier);
        case DataType::INT16:
            return GetChunkData<int16_t>(field_id, chunk_id, data_barrier);
        case DataType::INT32:
            return GetChunkData<int32_t>(field_id, chunk_id, data_barrier);
        case DataType::INT64:
            return GetChunkData<int64_t>(field_id, chunk_id, data_barrier);
        case DataType::FLOAT:
            return GetChunkData<float>(field_id, chunk_id, data_barrier);
        case DataType::DOUBLE:
            return GetChunkData<double>(field_id, chunk_id, data_barrier);
        case DataType::VARCHAR: {
            return GetChunkData<std::string>(field_id, chunk_id, data_barrier);
        }
        default:
            PanicInfo(DataTypeInvalid, "unsupported data type: {}", data_type);
    }
}
void
PhyColumnExpr::Eval(EvalCtx& context, VectorPtr& result) {
    result = DoEval();
}

VectorPtr
PhyColumnExpr::DoEval() {
    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }
    // TODO: simd support
    auto res_vec = std::make_shared<LazySegmentVector>(
        this->expr_->type(), real_batch_size, size_per_chunk_);
    const auto data_barrier =
        segment_->num_chunk_data(expr_->GetColumn().field_id_);
    int64_t processed_rows = 0;
    for (int64_t chunk_id = current_chunk_id_; chunk_id < num_chunk_;
         ++chunk_id) {
        auto chunk_size = chunk_id == num_chunk_ - 1
                              ? active_count_ - chunk_id * size_per_chunk_
                              : size_per_chunk_;
        auto chunk_data = GetChunkData(expr_->GetColumn().data_type_,
                                       expr_->GetColumn().field_id_,
                                       chunk_id,
                                       data_barrier);
        int64_t remaining_chunk_size = chunk_id == current_chunk_id_
                                           ? chunk_size - current_chunk_pos_
                                           : chunk_size;
        res_vec->AddChunkData(chunk_data);
        if (processed_rows + remaining_chunk_size >= batch_size_) {
            int64_t current_chunk_pos = chunk_id == current_chunk_id_ ? current_chunk_pos_ : 0;
            current_chunk_id_ = chunk_id;
            current_chunk_pos_ = current_chunk_pos + (batch_size_ - processed_rows);
            processed_rows = batch_size_;
            return res_vec;
        } else {
            processed_rows += remaining_chunk_size;
        }
    }
    return res_vec;
}

}  //namespace exec
}  // namespace milvus
