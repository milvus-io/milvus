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
#pragma once

#include <arrow/array.h>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "common/EasyAssert.h"
#include "common/FieldMeta.h"
#include "common/Types.h"
#include "mmap/ChunkedColumnInterface.h"
#include "milvus-storage/reader.h"

namespace milvus {

// ExternalFieldChunkedColumn handles external fields that are lazily loaded.
// It only supports take-by-offsets operations and throws errors for scan operations.
// This is used for external collections where fields need to be fetched from
// external storage on demand using milvus-storage Reader::take().
class ExternalFieldChunkedColumn : public ChunkedColumnInterface {
 public:
    ExternalFieldChunkedColumn(int64_t num_rows,
                               DataType data_type,
                               bool nullable,
                               FieldId field_id,
                               milvus_storage::api::Reader* reader)
        : num_rows_(num_rows),
          data_type_(data_type),
          nullable_(nullable),
          field_id_(field_id),
          reader_(reader) {
        num_rows_until_chunk_ = {0, num_rows_};
    }

    ~ExternalFieldChunkedColumn() override = default;

    // Scan operations - NOT SUPPORTED for external fields
    cachinglayer::PinWrapper<const char*>
    DataOfChunk(milvus::OpContext* op_ctx, int chunk_id) const override {
        ThrowInfo(ErrorCode::Unsupported,
                  "DataOfChunk not supported for ExternalFieldChunkedColumn. "
                  "External fields only support take-by-offsets operations.");
    }

    PinWrapper<SpanBase>
    Span(milvus::OpContext* op_ctx, int64_t chunk_id) const override {
        ThrowInfo(ErrorCode::Unsupported,
                  "Span not supported for ExternalFieldChunkedColumn. "
                  "External fields only support take-by-offsets operations.");
    }

    PinWrapper<Chunk*>
    GetChunk(milvus::OpContext* op_ctx, int64_t chunk_id) const override {
        ThrowInfo(ErrorCode::Unsupported,
                  "GetChunk not supported for ExternalFieldChunkedColumn. "
                  "External fields only support take-by-offsets operations.");
    }

    std::vector<PinWrapper<Chunk*>>
    GetAllChunks(milvus::OpContext* op_ctx) const override {
        ThrowInfo(ErrorCode::Unsupported,
                  "GetAllChunks not supported for ExternalFieldChunkedColumn. "
                  "External fields only support take-by-offsets operations.");
    }

    PinWrapper<std::pair<std::vector<std::string_view>, FixedVector<bool>>>
    StringViews(milvus::OpContext* op_ctx,
                int64_t chunk_id,
                std::optional<std::pair<int64_t, int64_t>> offset_len =
                    std::nullopt) const override {
        ThrowInfo(ErrorCode::Unsupported,
                  "StringViews not supported for ExternalFieldChunkedColumn. "
                  "External fields only support take-by-offsets operations.");
    }

    PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>
    ArrayViews(
        milvus::OpContext* op_ctx,
        int64_t chunk_id,
        std::optional<std::pair<int64_t, int64_t>> offset_len) const override {
        ThrowInfo(ErrorCode::Unsupported,
                  "ArrayViews not supported for ExternalFieldChunkedColumn. "
                  "External fields only support take-by-offsets operations.");
    }

    PinWrapper<std::pair<std::vector<VectorArrayView>, FixedVector<bool>>>
    VectorArrayViews(
        milvus::OpContext* op_ctx,
        int64_t chunk_id,
        std::optional<std::pair<int64_t, int64_t>> offset_len) const override {
        ThrowInfo(
            ErrorCode::Unsupported,
            "VectorArrayViews not supported for ExternalFieldChunkedColumn. "
            "External fields only support take-by-offsets operations.");
    }

    PinWrapper<const size_t*>
    VectorArrayOffsets(milvus::OpContext* op_ctx,
                       int64_t chunk_id) const override {
        ThrowInfo(ErrorCode::Unsupported,
                  "VectorArrayOffsets not supported for "
                  "ExternalFieldChunkedColumn. "
                  "External fields only support take-by-offsets operations.");
    }

    // Metadata operations - SUPPORTED
    bool
    IsValid(milvus::OpContext* op_ctx, size_t offset) const override {
        return offset < num_rows_;
    }

    void
    BulkIsValid(milvus::OpContext* ctx,
                std::function<void(bool, size_t)> fn,
                const int64_t* offsets,
                int64_t count) const override {
        if (offsets == nullptr) {
            for (int64_t i = 0; i < num_rows_; i++) {
                fn(true, i);
            }
        } else {
            for (int64_t i = 0; i < count; i++) {
                fn(true, i);
            }
        }
    }

    bool
    IsNullable() const override {
        return nullable_;
    }

    size_t
    NumRows() const override {
        return num_rows_;
    }

    int64_t
    num_chunks() const override {
        return 1;
    }

    size_t
    DataByteSize() const override {
        return 0;
    }

    int64_t
    chunk_row_nums(int64_t chunk_id) const override {
        AssertInfo(chunk_id == 0,
                   "ExternalFieldChunkedColumn has only 1 chunk");
        return num_rows_;
    }

    void
    PrefetchChunks(milvus::OpContext* op_ctx,
                   const std::vector<int64_t>& chunk_ids) const override {
    }

    std::pair<size_t, size_t>
    GetChunkIDByOffset(int64_t offset) const override {
        AssertInfo(offset >= 0 && offset < num_rows_,
                   "offset {} is out of range, num_rows: {}",
                   offset,
                   num_rows_);
        return {0, static_cast<size_t>(offset)};
    }

    std::pair<std::vector<milvus::cachinglayer::cid_t>, std::vector<int64_t>>
    GetChunkIDsByOffsets(const int64_t* offsets, int64_t count) const override {
        std::vector<milvus::cachinglayer::cid_t> cids(count, 0);
        std::vector<int64_t> offsets_in_chunk(offsets, offsets + count);
        return std::make_pair(std::move(cids), std::move(offsets_in_chunk));
    }

    int64_t
    GetNumRowsUntilChunk(int64_t chunk_id) const override {
        return num_rows_until_chunk_[chunk_id];
    }

    const std::vector<int64_t>&
    GetNumRowsUntilChunk() const override {
        return num_rows_until_chunk_;
    }

    PinWrapper<std::pair<std::vector<std::string_view>, FixedVector<bool>>>
    StringViewsByOffsets(milvus::OpContext* op_ctx,
                         int64_t chunk_id,
                         const FixedVector<int32_t>& offsets) const override {
        ThrowInfo(ErrorCode::Unsupported,
                  "StringViewsByOffsets not supported for "
                  "ExternalFieldChunkedColumn. "
                  "Use BulkRawStringAt instead.");
    }

    PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>
    ArrayViewsByOffsets(milvus::OpContext* op_ctx,
                        int64_t chunk_id,
                        const FixedVector<int32_t>& offsets) const override {
        ThrowInfo(ErrorCode::Unsupported,
                  "ArrayViewsByOffsets not supported for "
                  "ExternalFieldChunkedColumn.");
    }

    // Take-by-offsets operations - SUPPORTED via milvus-storage Reader::take()
    void
    BulkValueAt(milvus::OpContext* op_ctx,
                std::function<void(const char*, size_t)> fn,
                const int64_t* offsets,
                int64_t count) override {
        ThrowInfo(ErrorCode::Unsupported,
                  "BulkValueAt not supported for ExternalFieldChunkedColumn. "
                  "Use type-specific Bulk* methods instead.");
    }

    void
    BulkPrimitiveValueAt(milvus::OpContext* op_ctx,
                         void* dst,
                         const int64_t* offsets,
                         int64_t count) override {
        AssertInfo(reader_ != nullptr, "Reader is not set");
        std::vector<int64_t> row_indices(offsets, offsets + count);
        auto result = reader_->take(row_indices);
        AssertInfo(result.ok(),
                   "Failed to take data from external storage: {}",
                   result.status().ToString());
        auto batch = result.ValueOrDie();
        auto column = batch->GetColumnByName(std::to_string(field_id_.get()));
        AssertInfo(column != nullptr,
                   "Column {} not found in result",
                   field_id_.get());

        CopyPrimitiveArrayToBuffer(column, dst, count);
    }

    void
    BulkVectorValueAt(milvus::OpContext* op_ctx,
                      void* dst,
                      const int64_t* offsets,
                      int64_t element_sizeof,
                      int64_t count) override {
        AssertInfo(reader_ != nullptr, "Reader is not set");
        std::vector<int64_t> row_indices(offsets, offsets + count);
        auto result = reader_->take(row_indices);
        AssertInfo(result.ok(),
                   "Failed to take data from external storage: {}",
                   result.status().ToString());
        auto batch = result.ValueOrDie();
        auto column = batch->GetColumnByName(std::to_string(field_id_.get()));
        AssertInfo(column != nullptr,
                   "Column {} not found in result",
                   field_id_.get());

        auto typed_dst = static_cast<char*>(dst);
        auto fixed_size_list =
            std::static_pointer_cast<arrow::FixedSizeBinaryArray>(column);
        for (int64_t i = 0; i < count; i++) {
            auto value = fixed_size_list->GetValue(i);
            memcpy(typed_dst + i * element_sizeof, value, element_sizeof);
        }
    }

    void
    BulkRawStringAt(milvus::OpContext* op_ctx,
                    std::function<void(std::string_view, size_t, bool)> fn,
                    const int64_t* offsets,
                    int64_t count) const override {
        if (offsets == nullptr) {
            ThrowInfo(ErrorCode::Unsupported,
                      "Full scan (offsets=nullptr) not supported for "
                      "ExternalFieldChunkedColumn");
        }
        AssertInfo(reader_ != nullptr, "Reader is not set");
        std::vector<int64_t> row_indices(offsets, offsets + count);
        auto result = reader_->take(row_indices);
        AssertInfo(result.ok(),
                   "Failed to take data from external storage: {}",
                   result.status().ToString());
        auto batch = result.ValueOrDie();
        auto column = batch->GetColumnByName(std::to_string(field_id_.get()));
        AssertInfo(column != nullptr,
                   "Column {} not found in result",
                   field_id_.get());

        auto string_array = std::static_pointer_cast<arrow::StringArray>(column);
        for (int64_t i = 0; i < count; i++) {
            bool is_valid = !string_array->IsNull(i);
            auto value = string_array->GetView(i);
            fn(std::string_view(value.data(), value.size()), i, is_valid);
        }
    }

    DataType
    GetDataType() const {
        return data_type_;
    }

    FieldId
    GetFieldId() const {
        return field_id_;
    }

 private:
    void
    CopyPrimitiveArrayToBuffer(const std::shared_ptr<arrow::Array>& array,
                               void* dst,
                               int64_t count) const {
        switch (data_type_) {
            case DataType::BOOL: {
                auto typed = std::static_pointer_cast<arrow::BooleanArray>(array);
                auto typed_dst = static_cast<bool*>(dst);
                for (int64_t i = 0; i < count; i++) {
                    typed_dst[i] = typed->Value(i);
                }
                break;
            }
            case DataType::INT8: {
                auto typed = std::static_pointer_cast<arrow::Int8Array>(array);
                std::memcpy(dst, typed->raw_values(), count * sizeof(int8_t));
                break;
            }
            case DataType::INT16: {
                auto typed = std::static_pointer_cast<arrow::Int16Array>(array);
                std::memcpy(dst, typed->raw_values(), count * sizeof(int16_t));
                break;
            }
            case DataType::INT32: {
                auto typed = std::static_pointer_cast<arrow::Int32Array>(array);
                std::memcpy(dst, typed->raw_values(), count * sizeof(int32_t));
                break;
            }
            case DataType::INT64: {
                auto typed = std::static_pointer_cast<arrow::Int64Array>(array);
                std::memcpy(dst, typed->raw_values(), count * sizeof(int64_t));
                break;
            }
            case DataType::FLOAT: {
                auto typed = std::static_pointer_cast<arrow::FloatArray>(array);
                std::memcpy(dst, typed->raw_values(), count * sizeof(float));
                break;
            }
            case DataType::DOUBLE: {
                auto typed = std::static_pointer_cast<arrow::DoubleArray>(array);
                std::memcpy(dst, typed->raw_values(), count * sizeof(double));
                break;
            }
            default:
                ThrowInfo(ErrorCode::Unsupported,
                          "CopyPrimitiveArrayToBuffer not supported for data "
                          "type: {}",
                          static_cast<int>(data_type_));
        }
    }

    int64_t num_rows_;
    DataType data_type_;
    bool nullable_;
    FieldId field_id_;
    milvus_storage::api::Reader* reader_;  // Non-owning pointer
    std::vector<int64_t> num_rows_until_chunk_;
};

}  // namespace milvus
