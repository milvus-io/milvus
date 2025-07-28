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
#include <unordered_map>
#include <utility>
#include <vector>
#include "arrow/array/array_binary.h"
#include "arrow/array/array_primitive.h"
#include "arrow/record_batch.h"
#include "arrow/type_fwd.h"
#include "common/Chunk.h"
#include "common/EasyAssert.h"
#include "common/FieldDataInterface.h"
#include "common/Types.h"
#include "common/VectorTrait.h"
#include "simdjson/common_defs.h"
#include "simdjson/padded_string.h"
#include "storage/FileWriter.h"

namespace milvus {

void
StringChunkWriter::write(const arrow::ArrayVector& array_vec) {
    auto size = 0;
    std::vector<std::string_view> strs;
    // tuple <data, size, offset>
    std::vector<std::tuple<const uint8_t*, int64_t, int64_t>> null_bitmaps;
    for (const auto& data : array_vec) {
        auto array = std::dynamic_pointer_cast<arrow::StringArray>(data);
        for (int i = 0; i < array->length(); i++) {
            auto str = array->GetView(i);
            strs.emplace_back(str);
            size += str.size();
        }
        if (nullable_) {
            auto null_bitmap_n = (data->length() + 7) / 8;
            // size, offset all in bits
            null_bitmaps.emplace_back(
                data->null_bitmap_data(), data->length(), data->offset());
            size += null_bitmap_n;
        }
        row_nums_ += array->length();
    }

    size += sizeof(uint32_t) * (row_nums_ + 1) + MMAP_STRING_PADDING;
    if (!file_path_.empty()) {
        target_ = std::make_shared<MmapChunkTarget>(file_path_);
    } else {
        target_ = std::make_shared<MemChunkTarget>(size);
    }

    // chunk layout: null bitmap, offset1, offset2, ..., offsetn, str1, str2, ..., strn, padding
    // write null bitmaps
    write_null_bit_maps(null_bitmaps);

    // write data
    int offset_num = row_nums_ + 1;
    uint32_t offset_start_pos = target_->tell() + sizeof(uint32_t) * offset_num;
    std::vector<uint32_t> offsets;
    offsets.reserve(offset_num);
    for (const auto& str : strs) {
        offsets.push_back(offset_start_pos);
        offset_start_pos += str.size();
    }
    offsets.push_back(offset_start_pos);

    target_->write(offsets.data(), offsets.size() * sizeof(uint32_t));
    for (auto str : strs) {
        target_->write(str.data(), str.size());
    }
}

std::unique_ptr<Chunk>
StringChunkWriter::finish() {
    // write padding, maybe not needed anymore
    // FIXME
    char padding[MMAP_STRING_PADDING];
    target_->write(padding, MMAP_STRING_PADDING);
    auto [data, size] = target_->get();
    return std::make_unique<StringChunk>(row_nums_, data, size, nullable_);
}

void
JSONChunkWriter::write(const arrow::ArrayVector& array_vec) {
    auto size = 0;
    std::vector<Json> jsons;
    // tuple <data, size, offset>
    std::vector<std::tuple<const uint8_t*, int64_t, int64_t>> null_bitmaps;
    for (const auto& data : array_vec) {
        auto array = std::dynamic_pointer_cast<arrow::BinaryArray>(data);
        for (int i = 0; i < array->length(); i++) {
            auto str = array->GetView(i);
            auto json = Json(simdjson::padded_string(str));
            size += json.data().size();
            jsons.push_back(std::move(json));
        }
        if (nullable_) {
            auto null_bitmap_n = (data->length() + 7) / 8;
            // size, offset all in bits
            null_bitmaps.emplace_back(
                data->null_bitmap_data(), data->length(), data->offset());
            size += null_bitmap_n;
        }
        row_nums_ += array->length();
    }
    size += sizeof(uint32_t) * (row_nums_ + 1) + simdjson::SIMDJSON_PADDING;
    if (!file_path_.empty()) {
        target_ = std::make_shared<MmapChunkTarget>(file_path_);
    } else {
        target_ = std::make_shared<MemChunkTarget>(size);
    }

    // chunk layout: null bitmaps, offset1, offset2, ... ,json1, json2, ..., jsonn
    // write null bitmaps
    write_null_bit_maps(null_bitmaps);

    int offset_num = row_nums_ + 1;
    uint32_t offset_start_pos = target_->tell() + sizeof(uint32_t) * offset_num;
    std::vector<uint32_t> offsets;
    offsets.reserve(offset_num);
    for (const auto& json : jsons) {
        offsets.push_back(offset_start_pos);
        offset_start_pos += json.data().size();
    }
    offsets.push_back(offset_start_pos);

    target_->write(offsets.data(), offset_num * sizeof(uint32_t));

    // write data
    for (const auto& json : jsons) {
        target_->write(json.data().data(), json.data().size());
    }
}

std::unique_ptr<Chunk>
JSONChunkWriter::finish() {
    char padding[simdjson::SIMDJSON_PADDING];
    target_->write(padding, simdjson::SIMDJSON_PADDING);

    auto [data, size] = target_->get();
    return std::make_unique<JSONChunk>(row_nums_, data, size, nullable_);
}

void
ArrayChunkWriter::write(const arrow::ArrayVector& array_vec) {
    auto size = 0;
    auto is_string = IsStringDataType(element_type_);
    std::vector<Array> arrays;
    // tuple <data, size, offset>
    std::vector<std::tuple<const uint8_t*, int64_t, int64_t>> null_bitmaps;

    for (const auto& data : array_vec) {
        auto array = std::dynamic_pointer_cast<arrow::BinaryArray>(data);
        for (int i = 0; i < array->length(); i++) {
            auto str = array->GetView(i);
            ScalarFieldProto scalar_array;
            scalar_array.ParseFromArray(str.data(), str.size());
            auto arr = Array(scalar_array);
            size += arr.byte_size();
            if (is_string) {
                // element offsets size
                size += sizeof(uint32_t) * arr.length();
            }
            arrays.push_back(std::move(arr));
        }
        row_nums_ += array->length();
        if (nullable_) {
            auto null_bitmap_n = (data->length() + 7) / 8;
            // size, offset all in bits
            null_bitmaps.emplace_back(
                data->null_bitmap_data(), data->length(), data->offset());
            size += null_bitmap_n;
        }
    }

    // offsets + lens
    size += sizeof(uint32_t) * (row_nums_ * 2 + 1) + MMAP_ARRAY_PADDING;
    if (!file_path_.empty()) {
        target_ = std::make_shared<MmapChunkTarget>(file_path_);
    } else {
        target_ = std::make_shared<MemChunkTarget>(size);
    }

    // chunk layout: nullbitmaps, offsets, elem_off1, elem_off2, .. data1, data2, ..., datan, padding
    write_null_bit_maps(null_bitmaps);

    int offsets_num = row_nums_ + 1;
    int len_num = row_nums_;
    uint32_t offset_start_pos =
        target_->tell() + sizeof(uint32_t) * (offsets_num + len_num);
    std::vector<uint32_t> offsets(offsets_num);
    std::vector<uint32_t> lens(len_num);
    for (auto i = 0; i < arrays.size(); i++) {
        auto& arr = arrays[i];
        offsets[i] = offset_start_pos;
        lens[i] = arr.length();
        offset_start_pos += is_string ? sizeof(uint32_t) * lens[i] : 0;
        offset_start_pos += arr.byte_size();
    }
    if (offsets_num > 0) {
        offsets[offsets_num - 1] = offset_start_pos;
    }

    for (int i = 0; i < offsets.size(); i++) {
        if (i == offsets.size() - 1) {
            target_->write(&offsets[i], sizeof(uint32_t));
            break;
        }
        target_->write(&offsets[i], sizeof(uint32_t));
        target_->write(&lens[i], sizeof(uint32_t));
    }

    for (auto& arr : arrays) {
        if (is_string) {
            target_->write(arr.get_offsets_data(),
                           arr.length() * sizeof(uint32_t));
        }
        target_->write(arr.data(), arr.byte_size());
    }
}

std::unique_ptr<Chunk>
ArrayChunkWriter::finish() {
    char padding[MMAP_ARRAY_PADDING];
    target_->write(padding, MMAP_ARRAY_PADDING);

    auto [data, size] = target_->get();
    return std::make_unique<ArrayChunk>(
        row_nums_, data, size, element_type_, nullable_);
}

// 1. Deserialize VectorFieldProto (proto::schema::VectorField) from arrow::ArrayVector
// where VectorFieldProto is vector array and each element it self is a VectorFieldProto.
// 2. Transform this vector of VectorFieldProto to vector of our local representation of VectorArray.
// 3. the contents of these VectorArray are concatenated in some format in target_.
// See more details for the format in the comments of VectorArrayChunk.
void
VectorArrayChunkWriter::write(const arrow::ArrayVector& arrow_array_vec) {
    auto size = 0;
    std::vector<VectorArray> vector_arrays;
    vector_arrays.reserve(arrow_array_vec.size());

    for (const auto& data : arrow_array_vec) {
        auto array = std::dynamic_pointer_cast<arrow::BinaryArray>(data);
        for (size_t i = 0; i < array->length(); i++) {
            auto str = array->GetView(i);
            VectorFieldProto vector_field;
            vector_field.ParseFromArray(str.data(), str.size());
            auto arr = VectorArray(vector_field);
            size += arr.byte_size();
            vector_arrays.push_back(std::move(arr));
        }
        row_nums_ += array->length();
    }

    // offsets + lens
    size += sizeof(uint32_t) * (row_nums_ * 2 + 1) + MMAP_ARRAY_PADDING;
    if (!file_path_.empty()) {
        target_ = std::make_shared<MmapChunkTarget>(file_path_);
    } else {
        target_ = std::make_shared<MemChunkTarget>(size);
    }

    int offsets_num = row_nums_ + 1;
    int len_num = row_nums_;
    uint32_t offset_start_pos =
        target_->tell() + sizeof(uint32_t) * (offsets_num + len_num);
    std::vector<uint32_t> offsets(offsets_num);
    std::vector<uint32_t> lens(len_num);
    for (size_t i = 0; i < vector_arrays.size(); i++) {
        auto& arr = vector_arrays[i];
        offsets[i] = offset_start_pos;
        lens[i] = arr.length();
        offset_start_pos += arr.byte_size();
    }
    if (offsets_num > 0) {
        offsets[offsets_num - 1] = offset_start_pos;
    }

    for (int i = 0; i < offsets.size(); i++) {
        if (i == offsets.size() - 1) {
            target_->write(&offsets[i], sizeof(uint32_t));
            break;
        }
        target_->write(&offsets[i], sizeof(uint32_t));
        target_->write(&lens[i], sizeof(uint32_t));
    }

    for (auto& arr : vector_arrays) {
        target_->write(arr.data(), arr.byte_size());
    }
}

std::unique_ptr<Chunk>
VectorArrayChunkWriter::finish() {
    char padding[MMAP_ARRAY_PADDING];
    target_->write(padding, MMAP_ARRAY_PADDING);

    auto [data, size] = target_->get();
    return std::make_unique<VectorArrayChunk>(
        dim_, row_nums_, data, size, element_type_);
}

void
SparseFloatVectorChunkWriter::write(const arrow::ArrayVector& array_vec) {
    auto size = 0;
    std::vector<std::string> strs;
    std::vector<std::pair<const uint8_t*, int64_t>> null_bitmaps;
    for (const auto& data : array_vec) {
        auto array = std::dynamic_pointer_cast<arrow::BinaryArray>(data);
        for (int i = 0; i < array->length(); i++) {
            auto str = array->GetView(i);
            strs.emplace_back(str);
            size += str.size();
        }
        auto null_bitmap_n = (data->length() + 7) / 8;
        null_bitmaps.emplace_back(data->null_bitmap_data(), null_bitmap_n);
        size += null_bitmap_n;
        row_nums_ += array->length();
    }
    size += sizeof(uint64_t) * (row_nums_ + 1);
    if (!file_path_.empty()) {
        target_ = std::make_shared<MmapChunkTarget>(file_path_);
    } else {
        target_ = std::make_shared<MemChunkTarget>(size);
    }

    // chunk layout: null bitmap, offset1, offset2, ..., offsetn, str1, str2, ..., strn
    // write null bitmaps
    for (auto [data, size] : null_bitmaps) {
        if (data == nullptr) {
            std::vector<uint8_t> null_bitmap(size, 0xff);
            target_->write(null_bitmap.data(), size);
        } else {
            target_->write(data, size);
        }
    }

    // write data

    int offset_num = row_nums_ + 1;
    int offset_start_pos = target_->tell() + sizeof(uint64_t) * offset_num;
    std::vector<uint64_t> offsets;

    for (const auto& str : strs) {
        offsets.push_back(offset_start_pos);
        offset_start_pos += str.size();
    }
    offsets.push_back(offset_start_pos);

    target_->write(offsets.data(), offsets.size() * sizeof(uint64_t));

    for (auto str : strs) {
        target_->write(str.data(), str.size());
    }
}

std::unique_ptr<Chunk>
SparseFloatVectorChunkWriter::finish() {
    auto [data, size] = target_->get();
    return std::make_unique<SparseFloatVectorChunk>(
        row_nums_, data, size, nullable_);
}

template <typename... Args>
std::shared_ptr<ChunkWriterBase>
create_chunk_writer(const FieldMeta& field_meta, Args&&... args) {
    int dim = IsVectorDataType(field_meta.get_data_type()) &&
                      !IsSparseFloatVectorDataType(field_meta.get_data_type())
                  ? field_meta.get_dim()
                  : 1;
    bool nullable = field_meta.is_nullable();
    switch (field_meta.get_data_type()) {
        case milvus::DataType::BOOL:
            return std::make_shared<ChunkWriter<arrow::BooleanArray, bool>>(
                dim, std::forward<Args>(args)..., nullable);
        case milvus::DataType::INT8:
            return std::make_shared<ChunkWriter<arrow::Int8Array, int8_t>>(
                dim, std::forward<Args>(args)..., nullable);
        case milvus::DataType::INT16:
            return std::make_shared<ChunkWriter<arrow::Int16Array, int16_t>>(
                dim, std::forward<Args>(args)..., nullable);
        case milvus::DataType::INT32:
            return std::make_shared<ChunkWriter<arrow::Int32Array, int32_t>>(
                dim, std::forward<Args>(args)..., nullable);
        case milvus::DataType::INT64:
            return std::make_shared<ChunkWriter<arrow::Int64Array, int64_t>>(
                dim, std::forward<Args>(args)..., nullable);
        case milvus::DataType::FLOAT:
            return std::make_shared<ChunkWriter<arrow::FloatArray, float>>(
                dim, std::forward<Args>(args)..., nullable);
        case milvus::DataType::DOUBLE:
            return std::make_shared<ChunkWriter<arrow::DoubleArray, double>>(
                dim, std::forward<Args>(args)..., nullable);
        case milvus::DataType::VECTOR_FLOAT:
            return std::make_shared<
                ChunkWriter<arrow::FixedSizeBinaryArray, knowhere::fp32>>(
                dim, std::forward<Args>(args)..., nullable);
        case milvus::DataType::VECTOR_BINARY:
            return std::make_shared<
                ChunkWriter<arrow::FixedSizeBinaryArray, knowhere::bin1>>(
                dim / 8, std::forward<Args>(args)..., nullable);
        case milvus::DataType::VECTOR_FLOAT16:
            return std::make_shared<
                ChunkWriter<arrow::FixedSizeBinaryArray, knowhere::fp16>>(
                dim, std::forward<Args>(args)..., nullable);
        case milvus::DataType::VECTOR_BFLOAT16:
            return std::make_shared<
                ChunkWriter<arrow::FixedSizeBinaryArray, knowhere::bf16>>(
                dim, std::forward<Args>(args)..., nullable);
        case milvus::DataType::VECTOR_INT8:
            return std::make_shared<
                ChunkWriter<arrow::FixedSizeBinaryArray, knowhere::int8>>(
                dim, std::forward<Args>(args)..., nullable);
        case milvus::DataType::VARCHAR:
        case milvus::DataType::STRING:
        case milvus::DataType::TEXT:
            return std::make_shared<StringChunkWriter>(
                std::forward<Args>(args)..., nullable);
        case milvus::DataType::JSON:
            return std::make_shared<JSONChunkWriter>(
                std::forward<Args>(args)..., nullable);
        case milvus::DataType::ARRAY:
            return std::make_shared<ArrayChunkWriter>(
                field_meta.get_element_type(),
                std::forward<Args>(args)...,
                nullable);
        case milvus::DataType::VECTOR_SPARSE_FLOAT:
            return std::make_shared<SparseFloatVectorChunkWriter>(
                std::forward<Args>(args)..., nullable);
        case milvus::DataType::VECTOR_ARRAY:
            return std::make_shared<VectorArrayChunkWriter>(
                dim,
                field_meta.get_element_type(),
                std::forward<Args>(args)...);
        default:
            ThrowInfo(Unsupported, "Unsupported data type");
    }
}

std::unique_ptr<Chunk>
create_chunk(const FieldMeta& field_meta, const arrow::ArrayVector& array_vec) {
    auto cw = create_chunk_writer(field_meta);
    cw->write(array_vec);
    return cw->finish();
}

std::unique_ptr<Chunk>
create_chunk(const FieldMeta& field_meta,
             const arrow::ArrayVector& array_vec,
             const std::string& file_path) {
    auto cw = create_chunk_writer(field_meta, file_path);
    cw->write(array_vec);
    return cw->finish();
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