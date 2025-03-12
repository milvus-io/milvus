// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once
#include <cstddef>
#include <cstdint>
#include <memory>
#include <numeric>
#include <vector>
#include "arrow/array/array_primitive.h"
#include "common/ChunkTarget.h"
#include "arrow/record_batch.h"
#include "common/Chunk.h"
#include "common/EasyAssert.h"
#include "common/FieldDataInterface.h"
namespace milvus {
class ChunkWriterBase {
 public:
    explicit ChunkWriterBase(bool nullable) : nullable_(nullable) {
    }

    ChunkWriterBase(File& file, size_t offset, bool nullable)
        : file_(&file), file_offset_(offset), nullable_(nullable) {
    }

    virtual void
    write(std::shared_ptr<arrow::RecordBatchReader> data) = 0;

    virtual std::shared_ptr<Chunk>
    finish() = 0;

    std::pair<char*, size_t>
    get_data() {
        return target_->get();
    }

    void
    write_null_bit_maps(
        const std::vector<std::pair<const uint8_t*, int64_t>>& null_bitmaps) {
        if (nullable_) {
            for (auto [data, size] : null_bitmaps) {
                if (data != nullptr) {
                    target_->write(data, size);
                } else {
                    // have to append always-true bitmap due to arrow optimize this
                    std::vector<uint8_t> null_bitmap(size, 0xff);
                    target_->write(null_bitmap.data(), size);
                }
            }
        }
    }

 protected:
    int row_nums_ = 0;
    File* file_ = nullptr;
    size_t file_offset_ = 0;
    bool nullable_ = false;
    std::shared_ptr<ChunkTarget> target_;
};

template <typename ArrowType, typename T>
class ChunkWriter final : public ChunkWriterBase {
 public:
    ChunkWriter(int dim, bool nullable) : ChunkWriterBase(nullable), dim_(dim) {
    }

    ChunkWriter(int dim, File& file, size_t offset, bool nullable)
        : ChunkWriterBase(file, offset, nullable), dim_(dim){};

    void
    write(std::shared_ptr<arrow::RecordBatchReader> data) override {
        auto size = 0;
        auto row_nums = 0;

        auto batch_vec = data->ToRecordBatches().ValueOrDie();

        for (auto& batch : batch_vec) {
            row_nums += batch->num_rows();
            auto data = batch->column(0);
            auto array = std::static_pointer_cast<ArrowType>(data);
            if (nullable_) {
                auto null_bitmap_n = (data->length() + 7) / 8;
                size += null_bitmap_n;
            }
            size += array->length() * dim_ * sizeof(T);
        }

        row_nums_ = row_nums;
        if (file_) {
            target_ = std::make_shared<MmapChunkTarget>(*file_, file_offset_);
        } else {
            target_ = std::make_shared<MemChunkTarget>(size);
        }
        // Chunk layout:
        // 1. Null bitmap (if nullable_=true): Indicates which values are null
        // 2. Data values: Contiguous storage of data elements in the order:
        //    data1, data2, ..., dataN where each data element has size dim_*sizeof(T)
        if (nullable_) {
            for (auto& batch : batch_vec) {
                auto data = batch->column(0);
                auto null_bitmap = data->null_bitmap_data();
                auto null_bitmap_n = (data->length() + 7) / 8;
                if (null_bitmap) {
                    target_->write(null_bitmap, null_bitmap_n);
                } else {
                    std::vector<uint8_t> null_bitmap(null_bitmap_n, 0xff);
                    target_->write(null_bitmap.data(), null_bitmap_n);
                }
            }
        }

        for (auto& batch : batch_vec) {
            auto data = batch->column(0);
            auto array = std::static_pointer_cast<ArrowType>(data);
            auto data_ptr = array->raw_values();
            target_->write(data_ptr, array->length() * dim_ * sizeof(T));
        }
    }

    std::shared_ptr<Chunk>
    finish() override {
        auto [data, size] = target_->get();
        return std::make_shared<FixedWidthChunk>(
            row_nums_, dim_, data, size, sizeof(T), nullable_);
    }

 private:
    int dim_;
};

template <>
inline void
ChunkWriter<arrow::BooleanArray, bool>::write(
    std::shared_ptr<arrow::RecordBatchReader> data) {
    auto size = 0;
    auto row_nums = 0;
    auto batch_vec = data->ToRecordBatches().ValueOrDie();

    for (auto& batch : batch_vec) {
        row_nums += batch->num_rows();
        auto data = batch->column(0);
        auto array = std::dynamic_pointer_cast<arrow::BooleanArray>(data);
        size += array->length() * dim_;
        size += (data->length() + 7) / 8;
    }
    row_nums_ = row_nums;
    if (file_) {
        target_ = std::make_shared<MmapChunkTarget>(*file_, file_offset_);
    } else {
        target_ = std::make_shared<MemChunkTarget>(size);
    }

    if (nullable_) {
        // chunk layout: nullbitmap, data1, data2, ..., datan
        for (auto& batch : batch_vec) {
            auto data = batch->column(0);
            auto null_bitmap = data->null_bitmap_data();
            auto null_bitmap_n = (data->length() + 7) / 8;
            if (null_bitmap) {
                target_->write(null_bitmap, null_bitmap_n);
            } else {
                std::vector<uint8_t> null_bitmap(null_bitmap_n, 0xff);
                target_->write(null_bitmap.data(), null_bitmap_n);
            }
        }
    }

    for (auto& batch : batch_vec) {
        auto data = batch->column(0);
        auto array = std::dynamic_pointer_cast<arrow::BooleanArray>(data);
        for (int i = 0; i < array->length(); i++) {
            auto value = array->Value(i);
            target_->write(&value, sizeof(bool));
        }
    }
}

class StringChunkWriter : public ChunkWriterBase {
 public:
    using ChunkWriterBase::ChunkWriterBase;

    void
    write(std::shared_ptr<arrow::RecordBatchReader> data) override;

    std::shared_ptr<Chunk>
    finish() override;
};

class JSONChunkWriter : public ChunkWriterBase {
 public:
    using ChunkWriterBase::ChunkWriterBase;

    void
    write(std::shared_ptr<arrow::RecordBatchReader> data) override;

    std::shared_ptr<Chunk>
    finish() override;
};

class ArrayChunkWriter : public ChunkWriterBase {
 public:
    ArrayChunkWriter(const milvus::DataType element_type, bool nullable)
        : ChunkWriterBase(nullable), element_type_(element_type) {
    }
    ArrayChunkWriter(const milvus::DataType element_type,
                     File& file,
                     size_t offset,
                     bool nullable)
        : ChunkWriterBase(file, offset, nullable), element_type_(element_type) {
    }

    void
    write(std::shared_ptr<arrow::RecordBatchReader> data) override;

    std::shared_ptr<Chunk>
    finish() override;

 private:
    const milvus::DataType element_type_;
};

class SparseFloatVectorChunkWriter : public ChunkWriterBase {
 public:
    using ChunkWriterBase::ChunkWriterBase;

    void
    write(std::shared_ptr<arrow::RecordBatchReader> data) override;

    std::shared_ptr<Chunk>
    finish() override;
};

std::shared_ptr<Chunk>
create_chunk(const FieldMeta& field_meta,
             int dim,
             std::shared_ptr<arrow::RecordBatchReader> r);

std::shared_ptr<Chunk>
create_chunk(const FieldMeta& field_meta,
             int dim,
             File& file,
             size_t file_offset,
             std::shared_ptr<arrow::RecordBatchReader> r);
}  // namespace milvus