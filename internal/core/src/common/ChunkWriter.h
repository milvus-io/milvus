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
#include "arrow/type_fwd.h"
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
    write(const arrow::ArrayVector& data) = 0;

    virtual std::unique_ptr<Chunk>
    finish() = 0;

    std::pair<char*, size_t>
    get_data() {
        return target_->get();
    }

    void
    write_null_bit_maps(
        const std::vector<std::tuple<const uint8_t*, int64_t, int64_t>>&
            null_bitmaps) {
        if (nullable_) {
            // merge all null bitmaps in case of multiple chunk null bitmap dislocation
            // say [0xFF, 0x00] with size [7, 8] cannot be treated as [0xFF, 0x00] after merged but
            // [0x7F, 0x00], othersize the null index will be dislocated
            std::vector<uint8_t> merged_null_bitmap;
            int64_t size_total_bit = 0;
            for (auto [data, size_bits, offset_bits] : null_bitmaps) {
                // resize in byte
                merged_null_bitmap.resize((size_total_bit + size_bits + 7) / 8,
                                          0xFF);
                if (data != nullptr) {
                    bitset::detail::ElementWiseBitsetPolicy<uint8_t>::op_copy(
                        data,
                        offset_bits,
                        merged_null_bitmap.data(),
                        size_total_bit,
                        size_bits);
                } else {
                    // have to append always-true bitmap due to arrow optimize this
                    std::vector<uint8_t> null_bitmap(size_bits, 0xff);
                    bitset::detail::ElementWiseBitsetPolicy<uint8_t>::op_copy(
                        null_bitmap.data(),
                        0,
                        merged_null_bitmap.data(),
                        size_total_bit,
                        size_bits);
                }
                size_total_bit += size_bits;
            }
            target_->write(merged_null_bitmap.data(), (size_total_bit + 7) / 8);
        }
    }

 protected:
    int row_nums_ = 0;
    std::string file_path_{""};
    bool nullable_ = false;
    std::shared_ptr<ChunkTarget> target_;
};

template <typename ArrowType, typename T>
class ChunkWriter final : public ChunkWriterBase {
 public:
    ChunkWriter(int dim, bool nullable) : ChunkWriterBase(nullable), dim_(dim) {
    }

    ChunkWriter(int dim, std::string file_path, bool nullable)
        : ChunkWriterBase(std::move(file_path), nullable), dim_(dim){};

    void
    write(const arrow::ArrayVector& array_vec) override {
        auto size = 0;
        auto row_nums = 0;

        for (const auto& data : array_vec) {
            row_nums += data->length();
            auto array = std::static_pointer_cast<ArrowType>(data);
            if (nullable_) {
                auto null_bitmap_n = (data->length() + 7) / 8;
                size += null_bitmap_n;
            }
            size += array->length() * dim_ * sizeof(T);
        }

        row_nums_ = row_nums;
        if (!file_path_.empty()) {
            target_ = std::make_shared<MmapChunkTarget>(file_path_);
        } else {
            target_ = std::make_shared<MemChunkTarget>(size);
        }
        // Chunk layout:
        // 1. Null bitmap (if nullable_=true): Indicates which values are null
        // 2. Data values: Contiguous storage of data elements in the order:
        //    data1, data2, ..., dataN where each data element has size dim_*sizeof(T)
        if (nullable_) {
            // tuple <data, size, offset>
            std::vector<std::tuple<const uint8_t*, int64_t, int64_t>>
                null_bitmaps;
            for (const auto& data : array_vec) {
                null_bitmaps.emplace_back(
                    data->null_bitmap_data(), data->length(), data->offset());
            }
            write_null_bit_maps(null_bitmaps);
        }

        for (const auto& data : array_vec) {
            auto array = std::static_pointer_cast<ArrowType>(data);
            auto data_ptr = array->raw_values();
            target_->write(data_ptr, array->length() * dim_ * sizeof(T));
        }
    }

    std::unique_ptr<Chunk>
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
    const arrow::ArrayVector& array_vec) {
    auto size = 0;
    auto row_nums = 0;

    for (const auto& data : array_vec) {
        row_nums += data->length();
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

    if (nullable_) {
        // tuple <data, size, offset>
        std::vector<std::tuple<const uint8_t*, int64_t, int64_t>> null_bitmaps;
        for (const auto& data : array_vec) {
            null_bitmaps.emplace_back(
                data->null_bitmap_data(), data->length(), data->offset());
        }
        write_null_bit_maps(null_bitmaps);
    }

    for (const auto& data : array_vec) {
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
    write(const arrow::ArrayVector& array_vec) override;

    std::unique_ptr<Chunk>
    finish() override;
};

class JSONChunkWriter : public ChunkWriterBase {
 public:
    using ChunkWriterBase::ChunkWriterBase;

    void
    write(const arrow::ArrayVector& array_vec) override;

    std::unique_ptr<Chunk>
    finish() override;
};

class GeometryChunkWriter : public ChunkWriterBase {
 public:
    using ChunkWriterBase::ChunkWriterBase;
    void
    write(const arrow::ArrayVector& array_vec) override;

    std::unique_ptr<Chunk>
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
    write(const arrow::ArrayVector& array_vec) override;

    std::unique_ptr<Chunk>
    finish() override;

 private:
    const milvus::DataType element_type_;
};

class VectorArrayChunkWriter : public ChunkWriterBase {
 public:
    VectorArrayChunkWriter(int64_t dim,
                           const milvus::DataType element_type,
                           std::string file_path = "")
        : ChunkWriterBase(std::move(file_path), false),
          element_type_(element_type),
          dim_(dim) {
    }

    void
    write(const arrow::ArrayVector& array_vec) override;

    std::unique_ptr<Chunk>
    finish() override;

 private:
    void
    writeFloatVectorArray(const arrow::ArrayVector& array_vec);

    size_t
    calculateTotalSize(const arrow::ArrayVector& array_vec);

    const milvus::DataType element_type_;
    int64_t dim_;
};

class SparseFloatVectorChunkWriter : public ChunkWriterBase {
 public:
    using ChunkWriterBase::ChunkWriterBase;

    void
    write(const arrow::ArrayVector& array_vec) override;

    std::unique_ptr<Chunk>
    finish() override;
};

std::unique_ptr<Chunk>
create_chunk(const FieldMeta& field_meta, const arrow::ArrayVector& array_vec);

std::unique_ptr<Chunk>
create_chunk(const FieldMeta& field_meta,
             const arrow::ArrayVector& array_vec,
             const std::string& file_path);

arrow::ArrayVector
read_single_column_batches(std::shared_ptr<arrow::RecordBatchReader> reader);

}  // namespace milvus
