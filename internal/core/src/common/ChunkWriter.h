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
#include <utility>
#include <vector>
#include "arrow/array/array_primitive.h"
#include "common/ChunkTarget.h"
#include "arrow/record_batch.h"
#include "common/Chunk.h"
#include "common/EasyAssert.h"
#include "common/FieldDataInterface.h"

#include "storage/FileWriter.h"

#include "common/Geometry.h"
namespace milvus {

class ChunkWriterBase {
 public:
    explicit ChunkWriterBase(bool nullable) : nullable_(nullable) {
    }

    ChunkWriterBase(std::string file_path, bool nullable)
        : file_path_(std::move(file_path)), nullable_(nullable) {
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
    std::string file_path_{""};
    bool nullable_ = false;
    std::shared_ptr<ChunkTarget> target_;
};

template <typename ArrowType, typename T>
class ChunkWriter : public ChunkWriterBase {
 public:
    ChunkWriter(int dim, bool nullable) : ChunkWriterBase(nullable), dim_(dim) {
    }

    ChunkWriter(int dim, std::string file_path, bool nullable)
        : ChunkWriterBase(std::move(file_path), nullable), dim_(dim) {};

    void
    write(std::shared_ptr<arrow::RecordBatchReader> data) override {
        auto size = 0;
        auto row_nums = 0;

        auto batch_vec = data->ToRecordBatches().ValueOrDie();

        for (auto& batch : batch_vec) {
            row_nums += batch->num_rows();
            auto data = batch->column(0);
            auto array = std::dynamic_pointer_cast<ArrowType>(data);
            auto null_bitmap_n = (data->length() + 7) / 8;
            size += null_bitmap_n + array->length() * dim_ * sizeof(T);
        }

        row_nums_ = row_nums;
        if (!file_path_.empty()) {
            target_ = std::make_shared<MmapChunkTarget>(file_path_);
        } else {
            target_ = std::make_shared<MemChunkTarget>(size);
        }

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

        for (auto& batch : batch_vec) {
            auto data = batch->column(0);
            auto array = std::dynamic_pointer_cast<ArrowType>(data);
            auto data_ptr = array->raw_values();
            target_->write(data_ptr, array->length() * dim_ * sizeof(T));
        }
    }

    std::shared_ptr<Chunk>
    finish() override {
        auto [data, size] = target_->get();
        auto mmap_file_raii = file_path_.empty()
                                  ? nullptr
                                  : std::make_unique<MmapFileRAII>(file_path_);
        return std::make_unique<FixedWidthChunk>(row_nums_,
                                                 dim_,
                                                 data,
                                                 size,
                                                 sizeof(T),
                                                 nullable_,
                                                 std::move(mmap_file_raii));
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
    if (!file_path_.empty()) {
        target_ = std::make_shared<MmapChunkTarget>(file_path_);
    } else {
        target_ = std::make_shared<MemChunkTarget>(size);
    }
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

class GeometryChunkWriter : public ChunkWriterBase {
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
                     std::string file_path,
                     bool nullable)
        : ChunkWriterBase(std::move(file_path), nullable),
          element_type_(element_type) {
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
             std::shared_ptr<arrow::RecordBatchReader> r);

std::shared_ptr<Chunk>
create_chunk(const FieldMeta& field_meta,
             std::shared_ptr<arrow::RecordBatchReader> r,
             const std::string& file_path);

}  // namespace milvus
