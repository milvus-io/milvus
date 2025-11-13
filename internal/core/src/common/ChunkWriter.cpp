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

#include "common/ChunkWriter.h"
#include <cstdint>
#include <memory>
#include <tuple>
#include <utility>
#include <vector>
#include "arrow/array/array_binary.h"
#include "arrow/array/array_primitive.h"
#include "arrow/record_batch.h"
#include "arrow/type_fwd.h"
#include "common/Chunk.h"
#include "common/EasyAssert.h"
#include "common/Types.h"
#include "simdjson/padded_string.h"

namespace milvus {

std::pair<size_t, size_t>
StringChunkWriter::calculate_size(const arrow::ArrayVector& array_vec) {
    row_nums_ = 0;
    size_t size = 0;
    // tuple <data, size, offset>
    std::vector<std::tuple<const uint8_t*, int64_t, int64_t>> null_bitmaps;
    for (const auto& data : array_vec) {
        // for bson, we use binary array to store the string
        auto array = std::dynamic_pointer_cast<arrow::BinaryArray>(data);
        for (int i = 0; i < array->length(); i++) {
            auto str = array->GetView(i);
            size += str.size();
        }
        row_nums_ += array->length();
    }
    if (nullable_) {
        size += (row_nums_ + 7) / 8;
    }
    size += sizeof(uint32_t) * (row_nums_ + 1) + MMAP_STRING_PADDING;
    return {size, row_nums_};
}

void
StringChunkWriter::write_to_target(const arrow::ArrayVector& array_vec,
                                   const std::shared_ptr<ChunkTarget>& target) {
    std::vector<std::string_view> strs;
    // tuple <data, size, offset>
    std::vector<std::tuple<const uint8_t*, int64_t, int64_t>> null_bitmaps;
    for (const auto& data : array_vec) {
        // for bson, we use binary array to store the string
        auto array = std::dynamic_pointer_cast<arrow::BinaryArray>(data);
        for (int i = 0; i < array->length(); i++) {
            auto str = array->GetView(i);
            strs.emplace_back(str);
        }
        if (nullable_) {
            null_bitmaps.emplace_back(
                data->null_bitmap_data(), data->length(), data->offset());
        }
    }

    // chunk layout: null bitmap, offset1, offset2, ..., offsetn, str1, str2, ..., strn, padding
    // write null bitmaps
    write_null_bit_maps(null_bitmaps, target);

    // write data
    const int offset_num = row_nums_ + 1;
    const uint32_t null_bitmap_bytes =
        nullable_ ? static_cast<uint32_t>((row_nums_ + 7) / 8) : 0;
    uint32_t offset_start_pos =
        null_bitmap_bytes + sizeof(uint32_t) * offset_num;
    std::vector<uint32_t> offsets;
    offsets.reserve(offset_num);
    for (const auto& str : strs) {
        offsets.push_back(offset_start_pos);
        offset_start_pos += str.size();
    }
    offsets.push_back(offset_start_pos);

    target->write(offsets.data(), offsets.size() * sizeof(uint32_t));
    for (auto str : strs) {
        target->write(str.data(), str.size());
    }

    // write padding, maybe not needed anymore
    // FIXME
    char padding[MMAP_STRING_PADDING];
    target->write(padding, MMAP_STRING_PADDING);
}

std::pair<size_t, size_t>
JSONChunkWriter::calculate_size(const arrow::ArrayVector& array_vec) {
    row_nums_ = 0;
    size_t size = 0;
    for (const auto& data : array_vec) {
        auto array = std::dynamic_pointer_cast<arrow::BinaryArray>(data);
        for (int i = 0; i < array->length(); i++) {
            auto str = array->GetView(i);
            auto json = Json(simdjson::padded_string(str));
            size += json.data().size();
        }
        row_nums_ += array->length();
    }
    if (nullable_) {
        size += (row_nums_ + 7) / 8;
    }
    size += sizeof(uint32_t) * (row_nums_ + 1) + simdjson::SIMDJSON_PADDING;

    return {size, row_nums_};
}

void
JSONChunkWriter::write_to_target(const arrow::ArrayVector& array_vec,
                                 const std::shared_ptr<ChunkTarget>& target) {
    std::vector<Json> jsons;
    // tuple <data, size, offset>
    std::vector<std::tuple<const uint8_t*, int64_t, int64_t>> null_bitmaps;
    for (const auto& data : array_vec) {
        auto array = std::dynamic_pointer_cast<arrow::BinaryArray>(data);
        for (int i = 0; i < array->length(); i++) {
            auto str = array->GetView(i);
            auto json = Json(simdjson::padded_string(str));
            jsons.push_back(std::move(json));
        }
        if (nullable_) {
            null_bitmaps.emplace_back(
                data->null_bitmap_data(), data->length(), data->offset());
        }
    }

    // chunk layout: null bitmaps, offset1, offset2, ... ,json1, json2, ..., jsonn
    // write null bitmaps
    write_null_bit_maps(null_bitmaps, target);

    const int offset_num = row_nums_ + 1;
    const uint32_t null_bitmap_bytes =
        nullable_ ? static_cast<uint32_t>((row_nums_ + 7) / 8) : 0;
    uint32_t offset_start_pos =
        null_bitmap_bytes + sizeof(uint32_t) * offset_num;
    std::vector<uint32_t> offsets;
    offsets.reserve(offset_num);
    for (const auto& json : jsons) {
        offsets.push_back(offset_start_pos);
        offset_start_pos += json.data().size();
    }
    offsets.push_back(offset_start_pos);

    target->write(offsets.data(), offset_num * sizeof(uint32_t));

    // write data
    for (const auto& json : jsons) {
        target->write(json.data().data(), json.data().size());
    }

    char padding[simdjson::SIMDJSON_PADDING];
    target->write(padding, simdjson::SIMDJSON_PADDING);
}

std::pair<size_t, size_t>
GeometryChunkWriter::calculate_size(const arrow::ArrayVector& array_vec) {
    row_nums_ = 0;
    size_t size = 0;
    for (const auto& data : array_vec) {
        auto array = std::dynamic_pointer_cast<arrow::BinaryArray>(data);
        for (int64_t i = 0; i < array->length(); ++i) {
            auto str = array->GetView(i);
            size += str.size();
        }
        row_nums_ += array->length();
    }
    if (nullable_) {
        size += (row_nums_ + 7) / 8;
    }
    size += sizeof(uint32_t) * (row_nums_ + 1) + MMAP_GEOMETRY_PADDING;
    return {size, row_nums_};
}

void
GeometryChunkWriter::write_to_target(
    const arrow::ArrayVector& array_vec,
    const std::shared_ptr<ChunkTarget>& target) {
    std::vector<std::string_view> wkb_strs;
    std::vector<std::tuple<const uint8_t*, int64_t, int64_t>> null_bitmaps;
    wkb_strs.reserve(row_nums_);

    for (const auto& data : array_vec) {
        auto array = std::dynamic_pointer_cast<arrow::BinaryArray>(data);
        for (int64_t i = 0; i < array->length(); ++i) {
            auto str = array->GetView(i);
            wkb_strs.emplace_back(str);
        }
        if (nullable_) {
            null_bitmaps.emplace_back(
                data->null_bitmap_data(), data->length(), data->offset());
        }
    }

    // chunk layout: null bitmap, offsets, wkb strings, padding
    write_null_bit_maps(null_bitmaps, target);

    const int offset_num = row_nums_ + 1;
    const uint32_t null_bitmap_bytes =
        nullable_ ? static_cast<uint32_t>((row_nums_ + 7) / 8) : 0;
    uint32_t offset_start_pos =
        null_bitmap_bytes +
        static_cast<uint32_t>(sizeof(uint32_t) * offset_num);
    std::vector<uint32_t> offsets;
    offsets.reserve(offset_num);
    for (const auto& str : wkb_strs) {
        offsets.push_back(offset_start_pos);
        offset_start_pos += str.size();
    }
    offsets.push_back(offset_start_pos);

    target->write(offsets.data(), offsets.size() * sizeof(uint32_t));

    for (const auto& str : wkb_strs) {
        target->write(str.data(), str.size());
    }

    char padding[MMAP_GEOMETRY_PADDING];
    target->write(padding, MMAP_GEOMETRY_PADDING);
}

std::pair<size_t, size_t>
ArrayChunkWriter::calculate_size(const arrow::ArrayVector& array_vec) {
    row_nums_ = 0;
    size_t size = 0;
    const bool is_string = IsStringDataType(element_type_);

    for (const auto& data : array_vec) {
        auto array = std::dynamic_pointer_cast<arrow::BinaryArray>(data);
        for (int64_t i = 0; i < array->length(); ++i) {
            auto str = array->GetView(i);
            ScalarFieldProto scalar_array;
            scalar_array.ParseFromArray(str.data(), str.size());
            Array arr(scalar_array);
            size += arr.byte_size();
            if (is_string) {
                size += sizeof(uint32_t) * arr.length();
            }
        }

        row_nums_ += array->length();
    }
    if (nullable_) {
        size += (row_nums_ + 7) / 8;
    }
    size += sizeof(uint32_t) * (row_nums_ * 2 + 1) + MMAP_ARRAY_PADDING;
    return {size, row_nums_};
}

void
ArrayChunkWriter::write_to_target(const arrow::ArrayVector& array_vec,
                                  const std::shared_ptr<ChunkTarget>& target) {
    const bool is_string = IsStringDataType(element_type_);
    std::vector<Array> arrays;
    arrays.reserve(row_nums_);
    std::vector<std::tuple<const uint8_t*, int64_t, int64_t>> null_bitmaps;

    for (const auto& data : array_vec) {
        auto array = std::dynamic_pointer_cast<arrow::BinaryArray>(data);
        for (int64_t i = 0; i < array->length(); ++i) {
            auto str = array->GetView(i);
            ScalarFieldProto scalar_array;
            scalar_array.ParseFromArray(str.data(), str.size());
            arrays.emplace_back(Array(scalar_array));
        }
        if (nullable_) {
            null_bitmaps.emplace_back(
                data->null_bitmap_data(), data->length(), data->offset());
        }
    }

    write_null_bit_maps(null_bitmaps, target);

    const int offsets_num = row_nums_ + 1;
    const int len_num = row_nums_;
    const uint32_t null_bitmap_bytes =
        nullable_ ? static_cast<uint32_t>((row_nums_ + 7) / 8) : 0;
    uint32_t offset_start_pos =
        null_bitmap_bytes + sizeof(uint32_t) * (offsets_num + len_num);

    std::vector<uint32_t> offsets(offsets_num);
    std::vector<uint32_t> lens(len_num);

    for (size_t i = 0; i < arrays.size(); ++i) {
        auto& arr = arrays[i];
        offsets[i] = offset_start_pos;
        lens[i] = arr.length();
        if (is_string) {
            offset_start_pos += sizeof(uint32_t) * lens[i];
        }
        offset_start_pos += arr.byte_size();
    }

    if (!offsets.empty()) {
        offsets.back() = offset_start_pos;
    }

    for (int i = 0; i < row_nums_; ++i) {
        target->write(&offsets[i], sizeof(uint32_t));
        target->write(&lens[i], sizeof(uint32_t));
    }
    target->write(&offsets.back(), sizeof(uint32_t));

    for (auto& arr : arrays) {
        if (is_string) {
            target->write(arr.get_offsets_data(),
                          arr.length() * sizeof(uint32_t));
        }
        target->write(arr.data(), arr.byte_size());
    }

    char padding[MMAP_ARRAY_PADDING];
    target->write(padding, MMAP_ARRAY_PADDING);
}

std::pair<size_t, size_t>
VectorArrayChunkWriter::calculate_size(const arrow::ArrayVector& array_vec) {
    size_t total_rows = 0;
    size_t total_size = 0;

    for (const auto& array_data : array_vec) {
        total_rows += array_data->length();
        auto list_array =
            std::static_pointer_cast<arrow::ListArray>(array_data);

        switch (element_type_) {
            case milvus::DataType::VECTOR_FLOAT:
            case milvus::DataType::VECTOR_BINARY:
            case milvus::DataType::VECTOR_FLOAT16:
            case milvus::DataType::VECTOR_BFLOAT16:
            case milvus::DataType::VECTOR_INT8: {
                auto binary_values =
                    std::static_pointer_cast<arrow::FixedSizeBinaryArray>(
                        list_array->values());
                total_size +=
                    binary_values->length() * binary_values->byte_width();
                break;
            }
            default:
                ThrowInfo(DataTypeInvalid,
                          "Invalid element type {} for VectorArray",
                          static_cast<int>(element_type_));
        }
    }

    row_nums_ = total_rows;

    // Add space for offset and length arrays
    total_size += sizeof(uint32_t) * (total_rows * 2 + 1) + MMAP_ARRAY_PADDING;
    return {total_size, total_rows};
}

void
VectorArrayChunkWriter::write_to_target(
    const arrow::ArrayVector& array_vec,
    const std::shared_ptr<ChunkTarget>& target) {
    std::vector<uint32_t> offsets_lens;
    offsets_lens.reserve(row_nums_ * 2 + 1);
    std::vector<const uint8_t*> vector_data_ptrs;
    std::vector<size_t> data_sizes;

    uint32_t current_offset = sizeof(uint32_t) * (row_nums_ * 2 + 1);

    for (const auto& array_data : array_vec) {
        auto list_array =
            std::static_pointer_cast<arrow::ListArray>(array_data);
        auto binary_values =
            std::static_pointer_cast<arrow::FixedSizeBinaryArray>(
                list_array->values());
        const int32_t* list_offsets = list_array->raw_value_offsets();
        int byte_width = binary_values->byte_width();

        // Generate offsets and lengths for each row
        // Each list contains multiple vectors, each stored as a fixed-size binary chunk
        for (int64_t i = 0; i < list_array->length(); i++) {
            auto start_idx = list_offsets[i];
            auto end_idx = list_offsets[i + 1];
            auto vector_count = end_idx - start_idx;
            auto byte_size = static_cast<uint32_t>(vector_count * byte_width);

            offsets_lens.push_back(current_offset);
            offsets_lens.push_back(static_cast<uint32_t>(vector_count));

            for (int32_t j = start_idx; j < end_idx; ++j) {
                vector_data_ptrs.push_back(binary_values->GetValue(j));
                data_sizes.push_back(byte_width);
            }

            current_offset += byte_size;
        }
    }

    offsets_lens.push_back(current_offset);

    // Write offset and length arrays
    for (size_t i = 0; i < offsets_lens.size() - 1; i += 2) {
        target->write(&offsets_lens[i], sizeof(uint32_t));      // offset
        target->write(&offsets_lens[i + 1], sizeof(uint32_t));  // length
    }
    target->write(&offsets_lens.back(), sizeof(uint32_t));  // final offset

    for (size_t i = 0; i < vector_data_ptrs.size(); i++) {
        target->write(vector_data_ptrs[i], data_sizes[i]);
    }

    char padding[MMAP_ARRAY_PADDING];
    target->write(padding, MMAP_ARRAY_PADDING);
}

std::pair<size_t, size_t>
SparseFloatVectorChunkWriter::calculate_size(
    const arrow::ArrayVector& array_vec) {
    row_nums_ = 0;
    size_t size = 0;

    for (const auto& data : array_vec) {
        auto array = std::dynamic_pointer_cast<arrow::BinaryArray>(data);
        for (int64_t i = 0; i < array->length(); ++i) {
            auto str = array->GetView(i);
            size += str.size();
        }
        row_nums_ += array->length();
    }

    if (nullable_) {
        size += (row_nums_ + 7) / 8;
    }
    size += sizeof(uint64_t) * (row_nums_ + 1);
    return {size, row_nums_};
}

void
SparseFloatVectorChunkWriter::write_to_target(
    const arrow::ArrayVector& array_vec,
    const std::shared_ptr<ChunkTarget>& target) {
    std::vector<std::string> strs;
    strs.reserve(row_nums_);
    std::vector<std::tuple<const uint8_t*, int64_t, int64_t>> null_bitmaps;

    for (const auto& data : array_vec) {
        auto array = std::dynamic_pointer_cast<arrow::BinaryArray>(data);
        for (int64_t i = 0; i < array->length(); ++i) {
            auto str = array->GetView(i);
            strs.emplace_back(str);
        }
        if (nullable_) {
            null_bitmaps.emplace_back(
                data->null_bitmap_data(), data->length(), data->offset());
        }
    }

    write_null_bit_maps(null_bitmaps, target);

    const int offset_num = row_nums_ + 1;
    const uint64_t null_bitmap_bytes =
        nullable_ ? static_cast<uint64_t>((row_nums_ + 7) / 8) : 0;
    uint64_t offset_start_pos =
        null_bitmap_bytes + sizeof(uint64_t) * offset_num;
    std::vector<uint64_t> offsets;
    offsets.reserve(offset_num);

    for (const auto& str : strs) {
        offsets.push_back(offset_start_pos);
        offset_start_pos += str.size();
    }
    offsets.push_back(offset_start_pos);

    target->write(offsets.data(), offsets.size() * sizeof(uint64_t));

    for (const auto& str : strs) {
        target->write(str.data(), str.size());
    }
}

static inline std::shared_ptr<ChunkWriterBase>
create_chunk_writer(const FieldMeta& field_meta) {
    int dim = IsVectorDataType(field_meta.get_data_type()) &&
                      !IsSparseFloatVectorDataType(field_meta.get_data_type())
                  ? field_meta.get_dim()
                  : 1;
    bool nullable = field_meta.is_nullable();
    switch (field_meta.get_data_type()) {
        case milvus::DataType::BOOL:
            return std::make_shared<ChunkWriter<arrow::BooleanArray, bool>>(
                dim, nullable);
        case milvus::DataType::INT8:
            return std::make_shared<ChunkWriter<arrow::Int8Array, int8_t>>(
                dim, nullable);
        case milvus::DataType::INT16:
            return std::make_shared<ChunkWriter<arrow::Int16Array, int16_t>>(
                dim, nullable);
        case milvus::DataType::INT32:
            return std::make_shared<ChunkWriter<arrow::Int32Array, int32_t>>(
                dim, nullable);
        case milvus::DataType::INT64:
            return std::make_shared<ChunkWriter<arrow::Int64Array, int64_t>>(
                dim, nullable);
        case milvus::DataType::FLOAT:
            return std::make_shared<ChunkWriter<arrow::FloatArray, float>>(
                dim, nullable);
        case milvus::DataType::DOUBLE:
            return std::make_shared<ChunkWriter<arrow::DoubleArray, double>>(
                dim, nullable);
        case milvus::DataType::TIMESTAMPTZ:
            return std::make_shared<ChunkWriter<arrow::Int64Array, int64_t>>(
                dim, nullable);
        case milvus::DataType::VECTOR_FLOAT:
            return std::make_shared<
                ChunkWriter<arrow::FixedSizeBinaryArray, knowhere::fp32>>(
                dim, nullable);
        case milvus::DataType::VECTOR_BINARY:
            return std::make_shared<
                ChunkWriter<arrow::FixedSizeBinaryArray, knowhere::bin1>>(
                dim / 8, nullable);
        case milvus::DataType::VECTOR_FLOAT16:
            return std::make_shared<
                ChunkWriter<arrow::FixedSizeBinaryArray, knowhere::fp16>>(
                dim, nullable);
        case milvus::DataType::VECTOR_BFLOAT16:
            return std::make_shared<
                ChunkWriter<arrow::FixedSizeBinaryArray, knowhere::bf16>>(
                dim, nullable);
        case milvus::DataType::VECTOR_INT8:
            return std::make_shared<
                ChunkWriter<arrow::FixedSizeBinaryArray, knowhere::int8>>(
                dim, nullable);
        case milvus::DataType::VARCHAR:
        case milvus::DataType::STRING:
        case milvus::DataType::TEXT:
            return std::make_shared<StringChunkWriter>(nullable);
        case milvus::DataType::JSON:
            return std::make_shared<JSONChunkWriter>(nullable);
        case milvus::DataType::GEOMETRY: {
            return std::make_shared<GeometryChunkWriter>(nullable);
        }
        case milvus::DataType::ARRAY:
            return std::make_shared<ArrayChunkWriter>(
                field_meta.get_element_type(), nullable);
        case milvus::DataType::VECTOR_SPARSE_U32_F32:
            return std::make_shared<SparseFloatVectorChunkWriter>(nullable);
        case milvus::DataType::VECTOR_ARRAY:
            return std::make_shared<VectorArrayChunkWriter>(
                dim, field_meta.get_element_type());
        default:
            ThrowInfo(Unsupported, "Unsupported data type");
    }
}

static inline std::unique_ptr<Chunk>
make_chunk(const FieldMeta& field_meta,
           size_t row_nums,
           char* data,
           size_t size,
           const std::string& file_path,
           std::shared_ptr<ChunkMmapGuard> chunk_mmap_guard) {
    int dim = IsVectorDataType(field_meta.get_data_type()) &&
                      !IsSparseFloatVectorDataType(field_meta.get_data_type())
                  ? field_meta.get_dim()
                  : 1;
    bool nullable = field_meta.is_nullable();
    switch (field_meta.get_data_type()) {
        case milvus::DataType::BOOL:
            return std::make_unique<FixedWidthChunk>(row_nums,
                                                     dim,
                                                     data,
                                                     size,
                                                     sizeof(bool),
                                                     nullable,
                                                     chunk_mmap_guard);
        case milvus::DataType::INT8:
            return std::make_unique<FixedWidthChunk>(row_nums,
                                                     dim,
                                                     data,
                                                     size,
                                                     sizeof(int8_t),
                                                     nullable,
                                                     chunk_mmap_guard);
        case milvus::DataType::INT16:
            return std::make_unique<FixedWidthChunk>(row_nums,
                                                     dim,
                                                     data,
                                                     size,
                                                     sizeof(int16_t),
                                                     nullable,
                                                     chunk_mmap_guard);
        case milvus::DataType::INT32:
            return std::make_unique<FixedWidthChunk>(row_nums,
                                                     dim,
                                                     data,
                                                     size,
                                                     sizeof(int32_t),
                                                     nullable,
                                                     chunk_mmap_guard);
        case milvus::DataType::INT64:
            return std::make_unique<FixedWidthChunk>(row_nums,
                                                     dim,
                                                     data,
                                                     size,
                                                     sizeof(int64_t),
                                                     nullable,
                                                     chunk_mmap_guard);
        case milvus::DataType::FLOAT:
            return std::make_unique<FixedWidthChunk>(row_nums,
                                                     dim,
                                                     data,
                                                     size,
                                                     sizeof(float),
                                                     nullable,
                                                     chunk_mmap_guard);
        case milvus::DataType::DOUBLE:
            return std::make_unique<FixedWidthChunk>(row_nums,
                                                     dim,
                                                     data,
                                                     size,
                                                     sizeof(double),
                                                     nullable,
                                                     chunk_mmap_guard);
        case milvus::DataType::TIMESTAMPTZ:
            return std::make_unique<FixedWidthChunk>(row_nums,
                                                     dim,
                                                     data,
                                                     size,
                                                     sizeof(int64_t),
                                                     nullable,
                                                     chunk_mmap_guard);
        case milvus::DataType::VECTOR_FLOAT:
            return std::make_unique<FixedWidthChunk>(row_nums,
                                                     dim,
                                                     data,
                                                     size,
                                                     sizeof(knowhere::fp32),
                                                     nullable,
                                                     chunk_mmap_guard);
        case milvus::DataType::VECTOR_BINARY:
            return std::make_unique<FixedWidthChunk>(row_nums,
                                                     dim,
                                                     data,
                                                     size,
                                                     sizeof(knowhere::bin1),
                                                     nullable,
                                                     chunk_mmap_guard);
        case milvus::DataType::VECTOR_FLOAT16:
            return std::make_unique<FixedWidthChunk>(row_nums,
                                                     dim,
                                                     data,
                                                     size,
                                                     sizeof(knowhere::fp16),
                                                     nullable,
                                                     chunk_mmap_guard);
        case milvus::DataType::VECTOR_BFLOAT16:
            return std::make_unique<FixedWidthChunk>(row_nums,
                                                     dim,
                                                     data,
                                                     size,
                                                     sizeof(knowhere::bf16),
                                                     nullable,
                                                     chunk_mmap_guard);
        case milvus::DataType::VECTOR_INT8:
            return std::make_unique<FixedWidthChunk>(row_nums,
                                                     dim,
                                                     data,
                                                     size,
                                                     sizeof(knowhere::int8),
                                                     nullable,
                                                     chunk_mmap_guard);
        case milvus::DataType::VARCHAR:
        case milvus::DataType::STRING:
        case milvus::DataType::TEXT:
            return std::make_unique<StringChunk>(
                row_nums, data, size, nullable, chunk_mmap_guard);
        case milvus::DataType::JSON:
            return std::make_unique<JSONChunk>(
                row_nums, data, size, nullable, chunk_mmap_guard);
        case milvus::DataType::GEOMETRY: {
            return std::make_unique<GeometryChunk>(
                row_nums, data, size, nullable, chunk_mmap_guard);
        }
        case milvus::DataType::ARRAY:
            return std::make_unique<ArrayChunk>(row_nums,
                                                data,
                                                size,
                                                field_meta.get_element_type(),
                                                nullable,
                                                chunk_mmap_guard);
        case milvus::DataType::VECTOR_SPARSE_U32_F32:
            return std::make_unique<SparseFloatVectorChunk>(
                row_nums, data, size, nullable, chunk_mmap_guard);
        case milvus::DataType::VECTOR_ARRAY:
            return std::make_unique<VectorArrayChunk>(
                dim,
                row_nums,
                data,
                size,
                field_meta.get_element_type(),
                chunk_mmap_guard);
        default:
            ThrowInfo(DataTypeInvalid, "Unsupported data type");
    }
}

std::unique_ptr<Chunk>
create_chunk(const FieldMeta& field_meta,
             const arrow::ArrayVector& array_vec,
             const std::string& file_path) {
    auto cw = create_chunk_writer(field_meta);
    auto [size, row_nums] = cw->calculate_size(array_vec);
    size_t aligned_size = (size + ChunkTarget::ALIGNED_SIZE - 1) &
                          ~(ChunkTarget::ALIGNED_SIZE - 1);
    std::shared_ptr<ChunkTarget> target;
    if (file_path.empty()) {
        target = std::make_shared<MemChunkTarget>(aligned_size);
    } else {
        target = std::make_shared<MmapChunkTarget>(file_path, aligned_size);
    }
    cw->write_to_target(array_vec, target);
    auto data = target->release();
    std::shared_ptr<ChunkMmapGuard> chunk_mmap_guard = nullptr;
    if (!file_path.empty()) {
        chunk_mmap_guard =
            std::make_shared<ChunkMmapGuard>(data, size, file_path);
    } else {
        chunk_mmap_guard = std::make_shared<ChunkMmapGuard>(data, size, "");
    }
    return make_chunk(
        field_meta, row_nums, data, size, file_path, chunk_mmap_guard);
}

std::unordered_map<FieldId, std::shared_ptr<Chunk>>
create_group_chunk(const std::vector<FieldId>& field_ids,
                   const std::vector<FieldMeta>& field_metas,
                   const std::vector<arrow::ArrayVector>& array_vec,
                   const std::string& file_path) {
    std::vector<std::shared_ptr<ChunkWriterBase>> cws;
    cws.reserve(field_ids.size());
    size_t total_aligned_size = 0, final_row_nums = 0;
    for (size_t i = 0; i < field_ids.size(); i++) {
        auto field_meta = field_metas[i];
        cws.push_back(create_chunk_writer(field_meta));
    }
    std::vector<size_t> chunk_sizes;
    chunk_sizes.reserve(field_ids.size());
    std::vector<size_t> chunk_offsets;
    chunk_offsets.reserve(field_ids.size());
    for (size_t i = 0; i < field_ids.size(); i++) {
        auto [size, row_nums] = cws[i]->calculate_size(array_vec[i]);
        // Allocate and place each sub-chunk at an aligned boundary,
        // but keep the raw (unaligned) size for chunk construction.
        auto aligned_size = (size + ChunkTarget::ALIGNED_SIZE - 1) &
                            ~(ChunkTarget::ALIGNED_SIZE - 1);
        // store raw size for make_chunk()
        chunk_sizes.push_back(size);
        chunk_offsets.push_back(total_aligned_size);
        total_aligned_size += aligned_size;
        // each column should have the same number of rows
        if (i == 0) {
            final_row_nums = row_nums;
        } else {
            if (row_nums != final_row_nums) {
                ThrowInfo(DataTypeInvalid,
                          "All columns should have the same number of rows");
            }
        }
    }
    std::shared_ptr<ChunkTarget> target;
    if (file_path.empty()) {
        target = std::make_shared<MemChunkTarget>(total_aligned_size);
    } else {
        target =
            std::make_shared<MmapChunkTarget>(file_path, total_aligned_size);
    }
    for (size_t i = 0; i < field_ids.size(); i++) {
        auto start_off = target->tell();
        cws[i]->write_to_target(array_vec[i], target);
        auto end_off = target->tell();
        auto written = static_cast<size_t>(end_off - start_off);
        if (written != chunk_sizes[i]) {
            ThrowInfo(DataTypeInvalid,
                      "The written size {} of field {} does not match the "
                      "expected size {}",
                      written,
                      field_ids[i].get(),
                      chunk_sizes[i]);
        }
        auto aligned_size = (written + ChunkTarget::ALIGNED_SIZE - 1) &
                            ~(ChunkTarget::ALIGNED_SIZE - 1);
        auto padding_size = aligned_size - written;
        if (padding_size > 0) {
            std::string padding(padding_size, 0);
            target->write(padding.data(), padding_size);
        }
    }

    auto data = target->release();

    // For mmap mode, create a shared mmap region manager
    std::shared_ptr<ChunkMmapGuard> chunk_mmap_guard = nullptr;
    if (!file_path.empty()) {
        chunk_mmap_guard = std::make_shared<ChunkMmapGuard>(
            data, total_aligned_size, file_path);
    } else {
        chunk_mmap_guard =
            std::make_shared<ChunkMmapGuard>(data, total_aligned_size, "");
    }

    std::unordered_map<FieldId, std::shared_ptr<Chunk>> chunks;
    for (size_t i = 0; i < field_ids.size(); i++) {
        chunks[field_ids[i]] = std::move(make_chunk(field_metas[i],
                                                    final_row_nums,
                                                    data + chunk_offsets[i],
                                                    chunk_sizes[i],
                                                    file_path,
                                                    chunk_mmap_guard));
        LOG_INFO(
            "created chunk for field {} with chunk offset: {}, chunk "
            "size: {}, file path: {}",
            field_ids[i].get(),
            chunk_offsets[i],
            chunk_sizes[i],
            file_path);
    }

    return chunks;
}

arrow::ArrayVector
read_single_column_batches(std::shared_ptr<arrow::RecordBatchReader> reader) {
    arrow::ArrayVector array_vec;
    for (auto batch : *reader) {
        auto batch_data = batch.ValueOrDie();
        array_vec.push_back(std::move(batch_data->column(0)));
    }
    return array_vec;
}

}  // namespace milvus