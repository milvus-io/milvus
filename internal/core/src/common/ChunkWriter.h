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
#include <utility>
#include <vector>
#include "arrow/array/array_primitive.h"
#include "arrow/type_fwd.h"
#include "common/ChunkTarget.h"
#include "arrow/record_batch.h"
#include "common/Chunk.h"
#include "pb/common.pb.h"

namespace milvus {
class ChunkWriterBase {
 public:
    explicit ChunkWriterBase(bool nullable) : nullable_(nullable) {
    }

    virtual std::pair<size_t, size_t>
    calculate_size(const arrow::ArrayVector& data) = 0;

    virtual void
    write_to_target(const arrow::ArrayVector& array_vec,
                    const std::shared_ptr<ChunkTarget>& target) = 0;

 protected:
    void
    write_null_bit_maps(
        const std::vector<std::tuple<const uint8_t*, int64_t, int64_t>>&
            null_bitmaps,
        const std::shared_ptr<ChunkTarget>& target) {
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
            target->write(merged_null_bitmap.data(), (size_total_bit + 7) / 8);
        }
    }

 protected:
    size_t row_nums_ = 0;
    bool nullable_ = false;
};

template <typename ArrowType, typename T>
class ChunkWriter final : public ChunkWriterBase {
 public:
    ChunkWriter(int dim, bool nullable) : ChunkWriterBase(nullable), dim_(dim) {
    }

    std::pair<size_t, size_t>
    calculate_size(const arrow::ArrayVector& array_vec) override {
        size_t size = 0;
        size_t row_nums = 0;
        for (const auto& data : array_vec) {
            row_nums += data->length();
            auto array = std::static_pointer_cast<ArrowType>(data);
            size += array->length() * dim_ * sizeof(T);
        }
        if (nullable_) {
            size += (row_nums + 7) / 8;
        }
        row_nums_ = row_nums;
        return {size, row_nums};
    }

    void
    write_to_target(const arrow::ArrayVector& array_vec,
                    const std::shared_ptr<ChunkTarget>& target) override {
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
            write_null_bit_maps(null_bitmaps, target);
        }

        for (const auto& data : array_vec) {
            auto array = std::static_pointer_cast<ArrowType>(data);
            auto data_ptr = array->raw_values();
            target->write(data_ptr, array->length() * dim_ * sizeof(T));
        }
    }

 private:
    const int64_t dim_;
};

template <typename T>
class NullableVectorChunkWriter final : public ChunkWriterBase {
 public:
    NullableVectorChunkWriter(int64_t dim, bool nullable)
        : ChunkWriterBase(nullable), dim_(dim) {
        Assert(nullable && "NullableVectorChunkWriter requires nullable=true");
    }

    std::pair<size_t, size_t>
    calculate_size(const arrow::ArrayVector& array_vec) override {
        size_t size = 0;
        size_t row_nums = 0;

        for (const auto& data : array_vec) {
            row_nums += data->length();
            auto binary_array =
                std::static_pointer_cast<arrow::BinaryArray>(data);
            int64_t valid_count = data->length() - binary_array->null_count();
            size += valid_count * dim_ * sizeof(T);
        }

        // null bitmap size
        size += (row_nums + 7) / 8;
        row_nums_ = row_nums;
        return {size, row_nums};
    }

    void
    write_to_target(const arrow::ArrayVector& array_vec,
                    const std::shared_ptr<ChunkTarget>& target) override {
        std::vector<std::tuple<const uint8_t*, int64_t, int64_t>> null_bitmaps;
        for (const auto& data : array_vec) {
            null_bitmaps.emplace_back(
                data->null_bitmap_data(), data->length(), data->offset());
        }
        write_null_bit_maps(null_bitmaps, target);

        for (const auto& data : array_vec) {
            auto binary_array =
                std::static_pointer_cast<arrow::BinaryArray>(data);
            auto data_offset = binary_array->value_offset(0);
            auto data_ptr = binary_array->value_data()->data() + data_offset;
            int64_t valid_count = data->length() - binary_array->null_count();
            target->write(data_ptr, valid_count * dim_ * sizeof(T));
        }
    }

 private:
    const int64_t dim_;
};

template <>
inline void
ChunkWriter<arrow::BooleanArray, bool>::write_to_target(
    const arrow::ArrayVector& array_vec,
    const std::shared_ptr<ChunkTarget>& target) {
    if (nullable_) {
        // tuple <data, size, offset>
        std::vector<std::tuple<const uint8_t*, int64_t, int64_t>> null_bitmaps;
        for (const auto& data : array_vec) {
            null_bitmaps.emplace_back(
                data->null_bitmap_data(), data->length(), data->offset());
        }
        write_null_bit_maps(null_bitmaps, target);
    }

    for (const auto& data : array_vec) {
        auto array = std::dynamic_pointer_cast<arrow::BooleanArray>(data);
        for (int i = 0; i < array->length(); i++) {
            auto value = array->Value(i);
            target->write(&value, sizeof(bool));
        }
    }
}

class StringChunkWriter : public ChunkWriterBase {
 public:
    using ChunkWriterBase::ChunkWriterBase;

    std::pair<size_t, size_t>
    calculate_size(const arrow::ArrayVector& array_vec) override;

    void
    write_to_target(const arrow::ArrayVector& array_vec,
                    const std::shared_ptr<ChunkTarget>& target) override;

 private:
    std::vector<std::string_view> strs_;
};

class JSONChunkWriter : public ChunkWriterBase {
 public:
    using ChunkWriterBase::ChunkWriterBase;

    std::pair<size_t, size_t>
    calculate_size(const arrow::ArrayVector& array_vec) override;

    void
    write_to_target(const arrow::ArrayVector& array_vec,
                    const std::shared_ptr<ChunkTarget>& target) override;
};

class GeometryChunkWriter : public ChunkWriterBase {
 public:
    using ChunkWriterBase::ChunkWriterBase;

    std::pair<size_t, size_t>
    calculate_size(const arrow::ArrayVector& array_vec) override;

    void
    write_to_target(const arrow::ArrayVector& array_vec,
                    const std::shared_ptr<ChunkTarget>& target) override;
};

class ArrayChunkWriter : public ChunkWriterBase {
 public:
    ArrayChunkWriter(const milvus::DataType element_type, bool nullable)
        : ChunkWriterBase(nullable), element_type_(element_type) {
    }

    std::pair<size_t, size_t>
    calculate_size(const arrow::ArrayVector& array_vec) override;

    void
    write_to_target(const arrow::ArrayVector& array_vec,
                    const std::shared_ptr<ChunkTarget>& target) override;

 private:
    const milvus::DataType element_type_;
};

class VectorArrayChunkWriter : public ChunkWriterBase {
 public:
    VectorArrayChunkWriter(int64_t dim, const milvus::DataType element_type)
        : ChunkWriterBase(false), element_type_(element_type), dim_(dim) {
    }

    std::pair<size_t, size_t>
    calculate_size(const arrow::ArrayVector& array_vec) override;

    void
    write_to_target(const arrow::ArrayVector& array_vec,
                    const std::shared_ptr<ChunkTarget>& target) override;

 private:
    const milvus::DataType element_type_;
    const int64_t dim_;
};

class SparseFloatVectorChunkWriter : public ChunkWriterBase {
 public:
    using ChunkWriterBase::ChunkWriterBase;

    std::pair<size_t, size_t>
    calculate_size(const arrow::ArrayVector& array_vec) override;

    void
    write_to_target(const arrow::ArrayVector& array_vec,
                    const std::shared_ptr<ChunkTarget>& target) override;
};

// A reusable buffer that holds the raw chunk memory and its mmap guard.
// This can be used to create multiple Chunk instances that share the same
// underlying memory.
struct ChunkBuffer {
    char* data{nullptr};
    size_t size{0};
    size_t row_nums{0};
    std::shared_ptr<ChunkMmapGuard> guard;
};

// Build a chunk buffer from Arrow arrays, but do not materialize the Chunk
// object yet. This is useful when multiple Chunk instances need to share
// the same underlying memory.
ChunkBuffer
create_chunk_buffer(const FieldMeta& field_meta,
                    const arrow::ArrayVector& array_vec,
                    bool mmap_populate = true,
                    const std::string& file_path = "",
                    proto::common::LoadPriority load_priority =
                        proto::common::LoadPriority::HIGH);

// Create a Chunk view from an existing ChunkBuffer. Multiple Chunk instances
// created from the same buffer will share the same underlying memory via
// the shared ChunkMmapGuard in the buffer.
std::unique_ptr<Chunk>
make_chunk_from_buffer(const FieldMeta& field_meta,
                       const ChunkBuffer& buffer,
                       size_t row_nums_override = 0);

std::unique_ptr<Chunk>
create_chunk(const FieldMeta& field_meta,
             const arrow::ArrayVector& array_vec,
             bool mmap_populate = true,
             const std::string& file_path = "",
             proto::common::LoadPriority load_priority =
                 proto::common::LoadPriority::HIGH);

std::unordered_map<FieldId, std::shared_ptr<Chunk>>
create_group_chunk(const std::vector<FieldId>& field_ids,
                   const std::vector<FieldMeta>& field_metas,
                   const std::vector<arrow::ArrayVector>& array_vec,
                   bool mmap_populate = true,
                   const std::string& file_path = "",
                   proto::common::LoadPriority load_priority =
                       proto::common::LoadPriority::HIGH);

arrow::ArrayVector
read_single_column_batches(std::shared_ptr<arrow::RecordBatchReader> reader);

}  // namespace milvus
