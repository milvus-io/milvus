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
#include <limits>
#include <memory>
#include <tuple>
#include <utility>
#include <vector>

#include "NamedType/underlying_functionalities.hpp"
#include "arrow/array/array_binary.h"
#include "arrow/array/array_nested.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "common/Array.h"
#include "common/Chunk.h"
#include "common/EasyAssert.h"
#include "common/FieldMeta.h"
#include "common/Types.h"
#include "glog/logging.h"
#include "knowhere/operands.h"
#include "log/Log.h"
#include "simdjson/base.h"
#include "simdjson/padded_string.h"
#include "storage/FileWriter.h"

namespace milvus {

std::pair<size_t, size_t>
StringChunkWriter::calculate_size(const arrow::ArrayVector& array_vec) {
    // Single pass over Arrow: compute row count, absolute offsets, total
    // size. write_to_target reuses offsets_ and walks Arrow just once more
    // to emit the string bytes (2 arrow passes total instead of 4).
    row_nums_ = 0;
    for (const auto& data : array_vec) {
        row_nums_ += data->length();
    }

    const int offset_num = row_nums_ + 1;
    const size_t null_bitmap_bytes = nullable_ ? (row_nums_ + 7) / 8 : 0;
    size_t cursor = null_bitmap_bytes + sizeof(uint32_t) * offset_num;

    offsets_.clear();
    offsets_.reserve(offset_num);
    for (const auto& data : array_vec) {
        auto array = std::dynamic_pointer_cast<arrow::BinaryArray>(data);
        for (int i = 0; i < array->length(); i++) {
            offsets_.push_back(static_cast<uint32_t>(cursor));
            cursor += array->GetView(i).size();
        }
    }
    // String chunk uses uint32 offsets on disk; reject oversize chunks loudly
    // rather than silently wrapping.
    AssertInfo(cursor <= std::numeric_limits<uint32_t>::max(),
               "string chunk size {} exceeds uint32 offset limit",
               cursor);
    offsets_.push_back(static_cast<uint32_t>(cursor));

    size_t size = cursor + MMAP_STRING_PADDING;
    return {size, row_nums_};
}

void
StringChunkWriter::write_to_target(const arrow::ArrayVector& array_vec,
                                   const std::shared_ptr<ChunkTarget>& target) {
    // chunk layout: null bitmap, offsets[row_nums_+1], str1..strN, padding
    if (nullable_) {
        std::vector<std::tuple<const uint8_t*, int64_t, int64_t>> null_bitmaps;
        null_bitmaps.reserve(array_vec.size());
        for (const auto& data : array_vec) {
            null_bitmaps.emplace_back(
                data->null_bitmap_data(), data->length(), data->offset());
        }
        write_null_bit_maps(null_bitmaps, target);
    }

    target->write(offsets_.data(), offsets_.size() * sizeof(uint32_t));

    for (const auto& data : array_vec) {
        auto array = std::dynamic_pointer_cast<arrow::BinaryArray>(data);
        for (int i = 0; i < array->length(); i++) {
            auto str = array->GetView(i);
            target->write(str.data(), str.size());
        }
    }

    char padding[MMAP_STRING_PADDING] = {};
    target->write(padding, MMAP_STRING_PADDING);

    offsets_.clear();
    offsets_.shrink_to_fit();
}

std::pair<size_t, size_t>
JSONChunkWriter::calculate_size(const arrow::ArrayVector& array_vec) {
    // Single pass over Arrow: compute row count, absolute offsets, total
    // size. No per-row simdjson::padded_string allocation — write_to_target
    // copies bytes directly from the Arrow buffer and emits a single
    // SIMDJSON_PADDING region at the tail.
    row_nums_ = 0;
    for (const auto& data : array_vec) {
        row_nums_ += data->length();
    }

    const int offset_num = row_nums_ + 1;
    const size_t null_bitmap_bytes = nullable_ ? (row_nums_ + 7) / 8 : 0;
    size_t cursor = null_bitmap_bytes + sizeof(uint32_t) * offset_num;

    offsets_.clear();
    offsets_.reserve(offset_num);
    for (const auto& data : array_vec) {
        auto array = std::dynamic_pointer_cast<arrow::BinaryArray>(data);
        for (int i = 0; i < array->length(); i++) {
            offsets_.push_back(static_cast<uint32_t>(cursor));
            cursor += array->GetView(i).size();
        }
    }
    AssertInfo(cursor <= std::numeric_limits<uint32_t>::max(),
               "json chunk size {} exceeds uint32 offset limit",
               cursor);
    offsets_.push_back(static_cast<uint32_t>(cursor));

    size_t size = cursor + simdjson::SIMDJSON_PADDING;
    return {size, row_nums_};
}

void
JSONChunkWriter::write_to_target(const arrow::ArrayVector& array_vec,
                                 const std::shared_ptr<ChunkTarget>& target) {
    // chunk layout: null bitmap, offsets[row_nums_+1], json1..jsonN, padding
    if (nullable_) {
        std::vector<std::tuple<const uint8_t*, int64_t, int64_t>> null_bitmaps;
        null_bitmaps.reserve(array_vec.size());
        for (const auto& data : array_vec) {
            null_bitmaps.emplace_back(
                data->null_bitmap_data(), data->length(), data->offset());
        }
        write_null_bit_maps(null_bitmaps, target);
    }

    target->write(offsets_.data(), offsets_.size() * sizeof(uint32_t));

    for (const auto& data : array_vec) {
        auto array = std::dynamic_pointer_cast<arrow::BinaryArray>(data);
        for (int i = 0; i < array->length(); i++) {
            auto str = array->GetView(i);
            target->write(str.data(), str.size());
        }
    }

    char padding[simdjson::SIMDJSON_PADDING] = {};
    target->write(padding, simdjson::SIMDJSON_PADDING);

    offsets_.clear();
    offsets_.shrink_to_fit();
}

std::pair<size_t, size_t>
GeometryChunkWriter::calculate_size(const arrow::ArrayVector& array_vec) {
    // Same pattern as String/JSON: single Arrow pass produces offsets_ and
    // total size; write_to_target reuses offsets_ + walks Arrow once for
    // the WKB bytes.
    row_nums_ = 0;
    for (const auto& data : array_vec) {
        row_nums_ += data->length();
    }

    const int offset_num = row_nums_ + 1;
    const size_t null_bitmap_bytes = nullable_ ? (row_nums_ + 7) / 8 : 0;
    size_t cursor = null_bitmap_bytes + sizeof(uint32_t) * offset_num;

    offsets_.clear();
    offsets_.reserve(offset_num);
    for (const auto& data : array_vec) {
        auto array = std::dynamic_pointer_cast<arrow::BinaryArray>(data);
        for (int64_t i = 0; i < array->length(); ++i) {
            offsets_.push_back(static_cast<uint32_t>(cursor));
            cursor += array->GetView(i).size();
        }
    }
    AssertInfo(cursor <= std::numeric_limits<uint32_t>::max(),
               "geometry chunk size {} exceeds uint32 offset limit",
               cursor);
    offsets_.push_back(static_cast<uint32_t>(cursor));

    size_t size = cursor + MMAP_GEOMETRY_PADDING;
    return {size, row_nums_};
}

void
GeometryChunkWriter::write_to_target(
    const arrow::ArrayVector& array_vec,
    const std::shared_ptr<ChunkTarget>& target) {
    // chunk layout: null bitmap, offsets, wkb strings, padding
    if (nullable_) {
        std::vector<std::tuple<const uint8_t*, int64_t, int64_t>> null_bitmaps;
        null_bitmaps.reserve(array_vec.size());
        for (const auto& data : array_vec) {
            null_bitmaps.emplace_back(
                data->null_bitmap_data(), data->length(), data->offset());
        }
        write_null_bit_maps(null_bitmaps, target);
    }

    target->write(offsets_.data(), offsets_.size() * sizeof(uint32_t));

    for (const auto& data : array_vec) {
        auto array = std::dynamic_pointer_cast<arrow::BinaryArray>(data);
        for (int64_t i = 0; i < array->length(); ++i) {
            auto str = array->GetView(i);
            target->write(str.data(), str.size());
        }
    }

    char padding[MMAP_GEOMETRY_PADDING] = {};
    target->write(padding, MMAP_GEOMETRY_PADDING);

    offsets_.clear();
    offsets_.shrink_to_fit();
}

std::pair<size_t, size_t>
ArrayChunkWriter::calculate_size(const arrow::ArrayVector& array_vec) {
    // Parse ScalarFieldProto once per row here and cache the resulting
    // Array objects so write_to_target does not re-parse. Also produce the
    // interleaved [off0, len0, off1, len1, ..., offN-1, lenN-1, offN] header
    // so write_to_target can emit it in a single target->write call.
    const bool is_string = IsStringDataType(element_type_);

    row_nums_ = 0;
    for (const auto& data : array_vec) {
        row_nums_ += data->length();
    }

    cached_arrays_.clear();
    cached_arrays_.reserve(row_nums_);
    header_.clear();
    header_.reserve(row_nums_ * 2 + 1);

    const int header_entries = row_nums_ * 2 + 1;
    const size_t null_bitmap_bytes = nullable_ ? (row_nums_ + 7) / 8 : 0;
    size_t cursor = null_bitmap_bytes + sizeof(uint32_t) * header_entries;

    for (const auto& data : array_vec) {
        auto array = std::dynamic_pointer_cast<arrow::BinaryArray>(data);
        for (int64_t i = 0; i < array->length(); ++i) {
            auto str = array->GetView(i);
            ScalarFieldProto scalar_array;
            scalar_array.ParseFromArray(str.data(), str.size());
            cached_arrays_.emplace_back(scalar_array);
            const auto& arr = cached_arrays_.back();
            header_.push_back(static_cast<uint32_t>(cursor));        // off_i
            header_.push_back(static_cast<uint32_t>(arr.length()));  // len_i
            if (is_string) {
                cursor += sizeof(uint32_t) * arr.length();
            }
            cursor += arr.byte_size();
        }
    }
    header_.push_back(static_cast<uint32_t>(cursor));  // off_N (sentinel)

    size_t size = cursor + MMAP_ARRAY_PADDING;
    return {size, row_nums_};
}

void
ArrayChunkWriter::write_to_target(const arrow::ArrayVector& array_vec,
                                  const std::shared_ptr<ChunkTarget>& target) {
    const bool is_string = IsStringDataType(element_type_);

    if (nullable_) {
        std::vector<std::tuple<const uint8_t*, int64_t, int64_t>> null_bitmaps;
        null_bitmaps.reserve(array_vec.size());
        for (const auto& data : array_vec) {
            null_bitmaps.emplace_back(
                data->null_bitmap_data(), data->length(), data->offset());
        }
        write_null_bit_maps(null_bitmaps, target);
    }

    // Header: interleaved [off0, len0, off1, len1, ..., offN-1, lenN-1, offN]
    target->write(header_.data(), header_.size() * sizeof(uint32_t));

    for (auto& arr : cached_arrays_) {
        if (is_string) {
            target->write(arr.get_offsets_data(),
                          arr.length() * sizeof(uint32_t));
        }
        target->write(arr.data(), arr.byte_size());
    }

    char padding[MMAP_ARRAY_PADDING] = {};
    target->write(padding, MMAP_ARRAY_PADDING);

    cached_arrays_.clear();
    cached_arrays_.shrink_to_fit();
    header_.clear();
    header_.shrink_to_fit();
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
                int byte_width = binary_values->byte_width();
                // Calculate actual values count using list offsets
                // This handles sliced ListArrays correctly, as values() returns
                // the entire underlying array, but we only need the values
                // referenced by this slice
                const int32_t* list_offsets = list_array->raw_value_offsets();
                int64_t actual_values_count =
                    list_offsets[list_array->length()] - list_offsets[0];
                total_size += actual_values_count * byte_width;
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
            if (!nullable_ || !array->IsNull(i)) {
                auto str = array->GetView(i);
                size += str.size();
            }
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
    std::vector<std::string_view> strs;
    strs.reserve(row_nums_);
    std::vector<std::tuple<const uint8_t*, int64_t, int64_t>> null_bitmaps;

    for (const auto& data : array_vec) {
        auto array = std::dynamic_pointer_cast<arrow::BinaryArray>(data);
        for (int64_t i = 0; i < array->length(); ++i) {
            if (!nullable_ || !array->IsNull(i)) {
                auto str = array->GetView(i);
                strs.emplace_back(str);
            }
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

    if (nullable_) {
        size_t str_idx = 0;
        for (const auto& data : array_vec) {
            auto array = std::dynamic_pointer_cast<arrow::BinaryArray>(data);
            for (int i = 0; i < array->length(); i++) {
                offsets.push_back(offset_start_pos);
                if (!array->IsNull(i)) {
                    offset_start_pos += strs[str_idx].size();
                    str_idx++;
                }
            }
        }
    } else {
        for (const auto& str : strs) {
            offsets.push_back(offset_start_pos);
            offset_start_pos += str.size();
        }
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
            if (nullable) {
                return std::make_shared<
                    NullableVectorChunkWriter<knowhere::fp32>>(dim, nullable);
            }
            return std::make_shared<
                ChunkWriter<arrow::FixedSizeBinaryArray, knowhere::fp32>>(
                dim, nullable);
        case milvus::DataType::VECTOR_BINARY:
            if (nullable) {
                return std::make_shared<
                    NullableVectorChunkWriter<knowhere::bin1>>(dim / 8,
                                                               nullable);
            }
            return std::make_shared<
                ChunkWriter<arrow::FixedSizeBinaryArray, knowhere::bin1>>(
                dim / 8, nullable);
        case milvus::DataType::VECTOR_FLOAT16:
            if (nullable) {
                return std::make_shared<
                    NullableVectorChunkWriter<knowhere::fp16>>(dim, nullable);
            }
            return std::make_shared<
                ChunkWriter<arrow::FixedSizeBinaryArray, knowhere::fp16>>(
                dim, nullable);
        case milvus::DataType::VECTOR_BFLOAT16:
            if (nullable) {
                return std::make_shared<
                    NullableVectorChunkWriter<knowhere::bf16>>(dim, nullable);
            }
            return std::make_shared<
                ChunkWriter<arrow::FixedSizeBinaryArray, knowhere::bf16>>(
                dim, nullable);
        case milvus::DataType::VECTOR_INT8:
            if (nullable) {
                return std::make_shared<
                    NullableVectorChunkWriter<knowhere::int8>>(dim, nullable);
            }
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
                                                     dim / 8,
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

ChunkBuffer
create_chunk_buffer(const FieldMeta& field_meta,
                    const arrow::ArrayVector& array_vec,
                    bool mmap_populate,
                    const std::string& file_path,
                    proto::common::LoadPriority load_priority) {
    auto cw = create_chunk_writer(field_meta);
    auto [size, row_nums] = cw->calculate_size(array_vec);
    size_t aligned_size = (size + ChunkTarget::ALIGNED_SIZE - 1) &
                          ~(ChunkTarget::ALIGNED_SIZE - 1);
    std::shared_ptr<ChunkTarget> target;
    if (file_path.empty()) {
        target = std::make_shared<MemChunkTarget>(aligned_size, mmap_populate);
    } else {
        auto io_prio = storage::io::GetPriorityFromLoadPriority(load_priority);
        target = std::make_shared<MmapChunkTarget>(
            file_path, mmap_populate, aligned_size, io_prio);
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
    ChunkBuffer buffer;
    buffer.data = data;
    buffer.size = size;
    buffer.row_nums = row_nums;
    buffer.guard = std::move(chunk_mmap_guard);
    return buffer;
}

std::unique_ptr<Chunk>
make_chunk_from_buffer(const FieldMeta& field_meta,
                       const ChunkBuffer& buffer,
                       size_t row_nums_override) {
    auto row_nums =
        row_nums_override == 0 ? buffer.row_nums : row_nums_override;
    return make_chunk(
        field_meta, row_nums, buffer.data, buffer.size, buffer.guard);
}

std::unique_ptr<Chunk>
create_chunk(const FieldMeta& field_meta,
             const arrow::ArrayVector& array_vec,
             bool mmap_populate,
             const std::string& file_path,
             proto::common::LoadPriority load_priority) {
    auto buffer = create_chunk_buffer(
        field_meta, array_vec, mmap_populate, file_path, load_priority);
    return make_chunk_from_buffer(field_meta, buffer, 0);
}

std::unordered_map<FieldId, std::shared_ptr<Chunk>>
create_group_chunk(const std::vector<FieldId>& field_ids,
                   const std::vector<FieldMeta>& field_metas,
                   const std::vector<arrow::ArrayVector>& array_vec,
                   bool mmap_populate,
                   const std::string& file_path,
                   proto::common::LoadPriority load_priority) {
    std::vector<std::shared_ptr<ChunkWriterBase>> cws;
    cws.reserve(field_ids.size());
    size_t total_aligned_size = 0, final_row_nums = 0;
    for (size_t i = 0; i < field_ids.size(); i++) {
        const auto& field_meta = field_metas[i];
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
        target =
            std::make_shared<MemChunkTarget>(total_aligned_size, mmap_populate);
    } else {
        target = std::make_shared<MmapChunkTarget>(
            file_path,
            mmap_populate,
            total_aligned_size,
            storage::io::GetPriorityFromLoadPriority(load_priority));
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
            // Use a stack buffer to avoid heap allocation for padding zeros.
            // ChunkTarget::ALIGNED_SIZE is typically small (e.g. 64/512),
            // so padding_size is always < ALIGNED_SIZE.
            char padding[ChunkTarget::ALIGNED_SIZE] = {};
            target->write(padding, padding_size);
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
        chunks[field_ids[i]] = make_chunk(field_metas[i],
                                          final_row_nums,
                                          data + chunk_offsets[i],
                                          chunk_sizes[i],
                                          chunk_mmap_guard);
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
    for (const auto& batch : *reader) {
        auto batch_data = batch.ValueOrDie();
        array_vec.push_back(batch_data->column(0));
    }
    return array_vec;
}

}  // namespace milvus
