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

int64_t
PhyColumnExpr::GetNextBatchSize() {
    auto current_rows = GetCurrentRows();

    return current_rows + batch_size_ >= active_count_
               ? active_count_ - current_rows
               : batch_size_;
}

template <typename T>
MultipleChunkDataAccessor
PhyColumnExpr::GetChunkData(FieldId field_id,
                            bool index,
                            int64_t& current_chunk_id,
                            int64_t& current_chunk_pos) {
    if (index) {
        auto& indexing = const_cast<index::ScalarIndex<T>&>(
            segment_->chunk_scalar_index<T>(field_id, current_chunk_id));
        auto current_chunk_size = segment_->type() == SegmentType::Growing
                                      ? size_per_chunk_
                                      : active_count_;

        if (indexing.HasRawData()) {
            return [&, current_chunk_size]() -> const number {
                if (current_chunk_pos >= current_chunk_size) {
                    current_chunk_id++;
                    current_chunk_pos = 0;
                    indexing = const_cast<index::ScalarIndex<T>&>(
                        segment_->chunk_scalar_index<T>(field_id,
                                                        current_chunk_id));
                }
                return indexing.Reverse_Lookup(current_chunk_pos++);
            };
        }
    }
    auto chunk_data =
        segment_->chunk_data<T>(field_id, current_chunk_id).data();
    auto current_chunk_size = segment_->chunk_size(field_id, current_chunk_id);
    return
        [=, &current_chunk_id, &current_chunk_pos]() mutable -> const number {
            if (current_chunk_pos >= current_chunk_size) {
                current_chunk_id++;
                current_chunk_pos = 0;
                chunk_data =
                    segment_->chunk_data<T>(field_id, current_chunk_id).data();
                current_chunk_size =
                    segment_->chunk_size(field_id, current_chunk_id);
            }

            return chunk_data[current_chunk_pos++];
        };
}

template <>
MultipleChunkDataAccessor
PhyColumnExpr::GetChunkData<std::string>(FieldId field_id,
                                         bool index,
                                         int64_t& current_chunk_id,
                                         int64_t& current_chunk_pos) {
    if (index) {
        auto& indexing = const_cast<index::ScalarIndex<std::string>&>(
            segment_->chunk_scalar_index<std::string>(field_id,
                                                      current_chunk_id));
        auto current_chunk_size = segment_->type() == SegmentType::Growing
                                      ? size_per_chunk_
                                      : active_count_;

        if (indexing.HasRawData()) {
            return [&, current_chunk_size]() mutable -> const number {
                if (current_chunk_pos >= current_chunk_size) {
                    current_chunk_id++;
                    current_chunk_pos = 0;
                    indexing = const_cast<index::ScalarIndex<std::string>&>(
                        segment_->chunk_scalar_index<std::string>(
                            field_id, current_chunk_id));
                }
                return indexing.Reverse_Lookup(current_chunk_pos++);
            };
        }
    }
    if (segment_->type() == SegmentType::Growing &&
        !storage::MmapManager::GetInstance()
             .GetMmapConfig()
             .growing_enable_mmap) {
        auto chunk_data =
            segment_->chunk_data<std::string>(field_id, current_chunk_id)
                .data();
        auto current_chunk_size =
            segment_->chunk_size(field_id, current_chunk_id);
        return [=,
                &current_chunk_id,
                &current_chunk_pos]() mutable -> const number {
            if (current_chunk_pos >= current_chunk_size) {
                current_chunk_id++;
                current_chunk_pos = 0;
                chunk_data =
                    segment_
                        ->chunk_data<std::string>(field_id, current_chunk_id)
                        .data();
                current_chunk_size =
                    segment_->chunk_size(field_id, current_chunk_id);
            }

            return chunk_data[current_chunk_pos++];
        };
    } else {
        auto chunk_data =
            segment_->chunk_view<std::string_view>(field_id, current_chunk_id)
                .first.data();
        auto current_chunk_size =
            segment_->chunk_size(field_id, current_chunk_id);
        return [=,
                &current_chunk_id,
                &current_chunk_pos]() mutable -> const number {
            if (current_chunk_pos >= current_chunk_size) {
                current_chunk_id++;
                current_chunk_pos = 0;
                chunk_data = segment_
                                 ->chunk_view<std::string_view>(
                                     field_id, current_chunk_id)
                                 .first.data();
                current_chunk_size =
                    segment_->chunk_size(field_id, current_chunk_id);
            }

            return std::string(chunk_data[current_chunk_pos++]);
        };
    }
}

MultipleChunkDataAccessor
PhyColumnExpr::GetChunkData(DataType data_type,
                            FieldId field_id,
                            bool index,
                            int64_t& current_chunk_id,
                            int64_t& current_chunk_pos) {
    switch (data_type) {
        case DataType::BOOL:
            return GetChunkData<bool>(
                field_id, index, current_chunk_id, current_chunk_pos);
        case DataType::INT8:
            return GetChunkData<int8_t>(
                field_id, index, current_chunk_id, current_chunk_pos);
        case DataType::INT16:
            return GetChunkData<int16_t>(
                field_id, index, current_chunk_id, current_chunk_pos);
        case DataType::INT32:
            return GetChunkData<int32_t>(
                field_id, index, current_chunk_id, current_chunk_pos);
        case DataType::INT64:
            return GetChunkData<int64_t>(
                field_id, index, current_chunk_id, current_chunk_pos);
        case DataType::FLOAT:
            return GetChunkData<float>(
                field_id, index, current_chunk_id, current_chunk_pos);
        case DataType::DOUBLE:
            return GetChunkData<double>(
                field_id, index, current_chunk_id, current_chunk_pos);
        case DataType::VARCHAR: {
            return GetChunkData<std::string>(
                field_id, index, current_chunk_id, current_chunk_pos);
        }
        default:
            PanicInfo(DataTypeInvalid, "unsupported data type: {}", data_type);
    }
}

template <typename T>
ChunkDataAccessor
PhyColumnExpr::GetChunkData(FieldId field_id, int chunk_id, int data_barrier) {
    if (chunk_id >= data_barrier) {
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
    if (chunk_id >= data_barrier) {
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
    switch (this->expr_->type()) {
        case DataType::BOOL:
            result = DoEval<bool>();
            break;
        case DataType::INT8:
            result = DoEval<int8_t>();
            break;
        case DataType::INT16:
            result = DoEval<int16_t>();
            break;
        case DataType::INT32:
            result = DoEval<int32_t>();
            break;
        case DataType::INT64:
            result = DoEval<int64_t>();
            break;
        case DataType::FLOAT:
            result = DoEval<float>();
            break;
        case DataType::DOUBLE:
            result = DoEval<double>();
            break;
        case DataType::VARCHAR: {
            result = DoEval<std::string>();
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
PhyColumnExpr::DoEval() {
    // same as PhyCompareFilterExpr::ExecCompareExprDispatcher(OpType op)
    if (segment_->is_chunked()) {
        auto real_batch_size = GetNextBatchSize();
        if (real_batch_size == 0) {
            return nullptr;
        }

        auto res_vec = std::make_shared<ColumnValueVector<T>>(
            expr_->GetColumn().data_type_, real_batch_size);

        auto chunk_data = GetChunkData(expr_->GetColumn().data_type_,
                                       expr_->GetColumn().field_id_,
                                       is_indexed_,
                                       current_chunk_id_,
                                       current_chunk_pos_);
        for (int i = 0; i < real_batch_size; ++i) {
            res_vec->Set(i, boost::get<T>(chunk_data()));
        }
        return res_vec;
    } else {
        auto real_batch_size = GetNextBatchSize();
        if (real_batch_size == 0) {
            return nullptr;
        }

        auto res_vec = std::make_shared<ColumnValueVector<T>>(
            expr_->GetColumn().data_type_, real_batch_size);

        auto data_barrier =
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

            for (int i = chunk_id == current_chunk_id_ ? current_chunk_pos_ : 0;
                 i < chunk_size;
                 ++i) {
                res_vec->Set(processed_rows++, boost::get<T>(chunk_data(i)));
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
