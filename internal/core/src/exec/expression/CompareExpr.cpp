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

#include "CompareExpr.h"
#include "common/type_c.h"
#include "query/Relational.h"

namespace milvus {
namespace exec {

bool
PhyCompareFilterExpr::IsStringExpr() {
    return expr_->left_data_type_ == DataType::VARCHAR ||
           expr_->right_data_type_ == DataType::VARCHAR;
}

int64_t
PhyCompareFilterExpr::GetNextBatchSize() {
    auto current_rows = GetCurrentRows();

    return current_rows + batch_size_ >= active_count_
               ? active_count_ - current_rows
               : batch_size_;
}

template <typename T>
MultipleChunkDataAccessor
PhyCompareFilterExpr::GetChunkData(FieldId field_id,
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
PhyCompareFilterExpr::GetChunkData<std::string>(FieldId field_id,
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
PhyCompareFilterExpr::GetChunkData(DataType data_type,
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

template <typename OpType>
VectorPtr
PhyCompareFilterExpr::ExecCompareExprDispatcher(OpType op) {
    if (segment_->is_chunked()) {
        auto real_batch_size = GetNextBatchSize();
        if (real_batch_size == 0) {
            return nullptr;
        }

        auto res_vec =
            std::make_shared<ColumnVector>(TargetBitmap(real_batch_size));
        TargetBitmapView res(res_vec->GetRawData(), real_batch_size);

        auto left = GetChunkData(expr_->left_data_type_,
                                 expr_->left_field_id_,
                                 is_left_indexed_,
                                 left_current_chunk_id_,
                                 left_current_chunk_pos_);
        auto right = GetChunkData(expr_->right_data_type_,
                                  expr_->right_field_id_,
                                  is_right_indexed_,
                                  right_current_chunk_id_,
                                  right_current_chunk_pos_);
        for (int i = 0; i < real_batch_size; ++i) {
            res[i] = boost::apply_visitor(
                milvus::query::Relational<decltype(op)>{}, left(), right());
        }
        return res_vec;
    } else {
        auto real_batch_size = GetNextBatchSize();
        if (real_batch_size == 0) {
            return nullptr;
        }

        auto res_vec =
            std::make_shared<ColumnVector>(TargetBitmap(real_batch_size));
        TargetBitmapView res(res_vec->GetRawData(), real_batch_size);

        auto left_data_barrier =
            segment_->num_chunk_data(expr_->left_field_id_);
        auto right_data_barrier =
            segment_->num_chunk_data(expr_->right_field_id_);

        int64_t processed_rows = 0;
        for (int64_t chunk_id = current_chunk_id_; chunk_id < num_chunk_;
             ++chunk_id) {
            auto chunk_size = chunk_id == num_chunk_ - 1
                                  ? active_count_ - chunk_id * size_per_chunk_
                                  : size_per_chunk_;
            auto left = GetChunkData(expr_->left_data_type_,
                                     expr_->left_field_id_,
                                     chunk_id,
                                     left_data_barrier);
            auto right = GetChunkData(expr_->right_data_type_,
                                      expr_->right_field_id_,
                                      chunk_id,
                                      right_data_barrier);

            for (int i = chunk_id == current_chunk_id_ ? current_chunk_pos_ : 0;
                 i < chunk_size;
                 ++i) {
                res[processed_rows++] = boost::apply_visitor(
                    milvus::query::Relational<decltype(op)>{},
                    left(i),
                    right(i));

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

template <typename T>
ChunkDataAccessor
PhyCompareFilterExpr::GetChunkData(FieldId field_id,
                                   int chunk_id,
                                   int data_barrier) {
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
PhyCompareFilterExpr::GetChunkData<std::string>(FieldId field_id,
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
PhyCompareFilterExpr::GetChunkData(DataType data_type,
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
PhyCompareFilterExpr::Eval(EvalCtx& context, VectorPtr& result) {
    // For segment both fields has no index, can use SIMD to speed up.
    // Avoiding too much call stack that blocks SIMD.
    if (!is_left_indexed_ && !is_right_indexed_ && !IsStringExpr()) {
        result = ExecCompareExprDispatcherForBothDataSegment();
        return;
    }
    result = ExecCompareExprDispatcherForHybridSegment();
}

VectorPtr
PhyCompareFilterExpr::ExecCompareExprDispatcherForHybridSegment() {
    switch (expr_->op_type_) {
        case OpType::Equal: {
            return ExecCompareExprDispatcher(std::equal_to<>{});
        }
        case OpType::NotEqual: {
            return ExecCompareExprDispatcher(std::not_equal_to<>{});
        }
        case OpType::GreaterEqual: {
            return ExecCompareExprDispatcher(std::greater_equal<>{});
        }
        case OpType::GreaterThan: {
            return ExecCompareExprDispatcher(std::greater<>{});
        }
        case OpType::LessEqual: {
            return ExecCompareExprDispatcher(std::less_equal<>{});
        }
        case OpType::LessThan: {
            return ExecCompareExprDispatcher(std::less<>{});
        }
        case OpType::PrefixMatch: {
            return ExecCompareExprDispatcher(
                milvus::query::MatchOp<OpType::PrefixMatch>{});
        }
            // case OpType::PostfixMatch: {
            // }
        default: {
            PanicInfo(OpTypeInvalid, "unsupported optype: {}", expr_->op_type_);
        }
    }
}

VectorPtr
PhyCompareFilterExpr::ExecCompareExprDispatcherForBothDataSegment() {
    switch (expr_->left_data_type_) {
        case DataType::BOOL:
            return ExecCompareLeftType<bool>();
        case DataType::INT8:
            return ExecCompareLeftType<int8_t>();
        case DataType::INT16:
            return ExecCompareLeftType<int16_t>();
        case DataType::INT32:
            return ExecCompareLeftType<int32_t>();
        case DataType::INT64:
            return ExecCompareLeftType<int64_t>();
        case DataType::FLOAT:
            return ExecCompareLeftType<float>();
        case DataType::DOUBLE:
            return ExecCompareLeftType<double>();
        default:
            PanicInfo(
                DataTypeInvalid,
                fmt::format("unsupported left datatype:{} of compare expr",
                            expr_->left_data_type_));
    }
}

template <typename T>
VectorPtr
PhyCompareFilterExpr::ExecCompareLeftType() {
    switch (expr_->right_data_type_) {
        case DataType::BOOL:
            return ExecCompareRightType<T, bool>();
        case DataType::INT8:
            return ExecCompareRightType<T, int8_t>();
        case DataType::INT16:
            return ExecCompareRightType<T, int16_t>();
        case DataType::INT32:
            return ExecCompareRightType<T, int32_t>();
        case DataType::INT64:
            return ExecCompareRightType<T, int64_t>();
        case DataType::FLOAT:
            return ExecCompareRightType<T, float>();
        case DataType::DOUBLE:
            return ExecCompareRightType<T, double>();
        default:
            PanicInfo(
                DataTypeInvalid,
                fmt::format("unsupported right datatype:{} of compare expr",
                            expr_->right_data_type_));
    }
}

template <typename T, typename U>
VectorPtr
PhyCompareFilterExpr::ExecCompareRightType() {
    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    auto res_vec =
        std::make_shared<ColumnVector>(TargetBitmap(real_batch_size));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);

    auto expr_type = expr_->op_type_;
    auto execute_sub_batch = [expr_type](const T* left,
                                         const U* right,
                                         const int size,
                                         TargetBitmapView res) {
        switch (expr_type) {
            case proto::plan::GreaterThan: {
                CompareElementFunc<T, U, proto::plan::GreaterThan> func;
                func(left, right, size, res);
                break;
            }
            case proto::plan::GreaterEqual: {
                CompareElementFunc<T, U, proto::plan::GreaterEqual> func;
                func(left, right, size, res);
                break;
            }
            case proto::plan::LessThan: {
                CompareElementFunc<T, U, proto::plan::LessThan> func;
                func(left, right, size, res);
                break;
            }
            case proto::plan::LessEqual: {
                CompareElementFunc<T, U, proto::plan::LessEqual> func;
                func(left, right, size, res);
                break;
            }
            case proto::plan::Equal: {
                CompareElementFunc<T, U, proto::plan::Equal> func;
                func(left, right, size, res);
                break;
            }
            case proto::plan::NotEqual: {
                CompareElementFunc<T, U, proto::plan::NotEqual> func;
                func(left, right, size, res);
                break;
            }
            default:
                PanicInfo(
                    OpTypeInvalid,
                    fmt::format(
                        "unsupported operator type for compare column expr: {}",
                        expr_type));
        }
    };
    int64_t processed_size =
        ProcessBothDataChunks<T, U>(execute_sub_batch, res);
    AssertInfo(processed_size == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
};

}  //namespace exec
}  // namespace milvus
