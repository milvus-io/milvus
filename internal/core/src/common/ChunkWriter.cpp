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
#include <utility>
#include <vector>
#include "arrow/array/array_binary.h"
#include "arrow/array/array_primitive.h"
#include "arrow/record_batch.h"
#include "common/Chunk.h"
#include "common/EasyAssert.h"
#include "common/FieldDataInterface.h"
#include "common/Types.h"
#include "common/VectorTrait.h"
#include "simdjson/common_defs.h"
#include "simdjson/padded_string.h"
namespace milvus {

void
StringChunkWriter::write(std::shared_ptr<arrow::RecordBatchReader> data) {
    auto size = 0;
    std::vector<std::string> strs;
    std::vector<std::pair<const uint8_t*, int64_t>> null_bitmaps;
    for (auto batch : *data) {
        auto data = batch.ValueOrDie()->column(0);
        auto array = std::dynamic_pointer_cast<arrow::StringArray>(data);
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
    size += sizeof(uint64_t) * (row_nums_ + 1) + MMAP_STRING_PADDING;
    if (file_) {
        target_ = std::make_shared<MmapChunkTarget>(*file_, file_offset_);
    } else {
        target_ = std::make_shared<MemChunkTarget>(size);
    }

    // chunk layout: null bitmap, offset1, offset2, ..., offsetn, str1, str2, ..., strn, padding
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

std::shared_ptr<Chunk>
StringChunkWriter::finish() {
    // write padding, maybe not needed anymore
    // FIXME
    char padding[MMAP_STRING_PADDING];
    target_->write(padding, MMAP_STRING_PADDING);
    auto [data, size] = target_->get();
    return std::make_shared<StringChunk>(row_nums_, data, size, nullable_);
}

void
JSONChunkWriter::write(std::shared_ptr<arrow::RecordBatchReader> data) {
    auto size = 0;

    std::vector<Json> jsons;
    std::vector<std::pair<const uint8_t*, int64_t>> null_bitmaps;
    for (auto batch : *data) {
        auto data = batch.ValueOrDie()->column(0);
        auto array = std::dynamic_pointer_cast<arrow::BinaryArray>(data);
        for (int i = 0; i < array->length(); i++) {
            auto str = array->GetView(i);
            auto json = Json(simdjson::padded_string(str));
            size += json.data().size();
            jsons.push_back(std::move(json));
        }
        // AssertInfo(data->length() % 8 == 0,
        //            "String length should be multiple of 8");
        auto null_bitmap_n = (data->length() + 7) / 8;
        null_bitmaps.emplace_back(data->null_bitmap_data(), null_bitmap_n);
        size += null_bitmap_n;
        row_nums_ += array->length();
    }
    size += sizeof(uint64_t) * (row_nums_ + 1) + simdjson::SIMDJSON_PADDING;
    if (file_) {
        target_ = std::make_shared<MmapChunkTarget>(*file_, file_offset_);
    } else {
        target_ = std::make_shared<MemChunkTarget>(size);
    }

    // chunk layout: null bitmaps, offset1, offset2, ... ,json1, json2, ..., jsonn
    // write null bitmaps
    for (auto [data, size] : null_bitmaps) {
        if (data == nullptr) {
            std::vector<uint8_t> null_bitmap(size, 0xff);
            target_->write(null_bitmap.data(), size);
        } else {
            target_->write(data, size);
        }
    }

    int offset_num = row_nums_ + 1;
    int offset_start_pos = target_->tell() + sizeof(uint64_t) * offset_num;
    std::vector<uint64_t> offsets;

    for (const auto& json : jsons) {
        offsets.push_back(offset_start_pos);
        offset_start_pos += json.data().size();
    }
    offsets.push_back(offset_start_pos);

    target_->write(offsets.data(), offsets.size() * sizeof(uint64_t));

    // write data
    for (const auto& json : jsons) {
        target_->write(json.data().data(), json.data().size());
    }
}

std::shared_ptr<Chunk>
JSONChunkWriter::finish() {
    char padding[simdjson::SIMDJSON_PADDING];
    target_->write(padding, simdjson::SIMDJSON_PADDING);

    auto [data, size] = target_->get();
    return std::make_shared<JSONChunk>(row_nums_, data, size, nullable_);
}

void
ArrayChunkWriter::write(std::shared_ptr<arrow::RecordBatchReader> data) {
    auto size = 0;

    auto is_string = IsStringDataType(element_type_);
    std::vector<Array> arrays;
    std::vector<std::pair<const uint8_t*, int64_t>> null_bitmaps;
    for (auto batch : *data) {
        auto data = batch.ValueOrDie()->column(0);
        auto array = std::dynamic_pointer_cast<arrow::BinaryArray>(data);
        for (int i = 0; i < array->length(); i++) {
            auto str = array->GetView(i);
            ScalarArray scalar_array;
            scalar_array.ParseFromArray(str.data(), str.size());
            auto arr = Array(scalar_array);
            size += arr.byte_size();
            arrays.push_back(std::move(arr));
            if (is_string) {
                // element offsets size
                size += sizeof(uint64_t) * arr.length();
            }
        }
        row_nums_ += array->length();
        auto null_bitmap_n = (data->length() + 7) / 8;
        null_bitmaps.emplace_back(data->null_bitmap_data(), null_bitmap_n);
        size += null_bitmap_n;
    }

    // offsets + lens
    size += sizeof(uint64_t) * (row_nums_ * 2 + 1) + MMAP_ARRAY_PADDING;
    if (file_) {
        target_ = std::make_shared<MmapChunkTarget>(*file_, file_offset_);
    } else {
        target_ = std::make_shared<MemChunkTarget>(size);
    }

    // chunk layout: nullbitmaps, offsets, elem_off1, elem_off2, .. data1, data2, ..., datan, padding
    for (auto [data, size] : null_bitmaps) {
        if (data == nullptr) {
            std::vector<uint8_t> null_bitmap(size, 0xff);
            target_->write(null_bitmap.data(), size);
        } else {
            target_->write(data, size);
        }
    }

    int offsets_num = row_nums_ + 1;
    int len_num = row_nums_;
    int offset_start_pos =
        target_->tell() + sizeof(uint64_t) * (offsets_num + len_num);
    std::vector<uint64_t> offsets;
    std::vector<uint64_t> lens;
    for (auto& arr : arrays) {
        offsets.push_back(offset_start_pos);
        lens.push_back(arr.length());
        offset_start_pos +=
            is_string ? sizeof(uint64_t) * arr.get_offsets().size() : 0;
        offset_start_pos += arr.byte_size();
    }
    offsets.push_back(offset_start_pos);

    for (int i = 0; i < offsets.size(); i++) {
        if (i == offsets.size() - 1) {
            target_->write(&offsets[i], sizeof(uint64_t));
            break;
        }
        target_->write(&offsets[i], sizeof(uint64_t));
        target_->write(&lens[i], sizeof(uint64_t));
    }

    for (auto& arr : arrays) {
        if (is_string) {
            target_->write(arr.get_offsets().data(),
                           arr.get_offsets().size() * sizeof(uint64_t));
        }
        target_->write(arr.data(), arr.byte_size());
    }
}

std::shared_ptr<Chunk>
ArrayChunkWriter::finish() {
    char padding[MMAP_ARRAY_PADDING];
    target_->write(padding, MMAP_ARRAY_PADDING);

    auto [data, size] = target_->get();
    return std::make_shared<ArrayChunk>(
        row_nums_, data, size, element_type_, nullable_);
}

void
SparseFloatVectorChunkWriter::write(
    std::shared_ptr<arrow::RecordBatchReader> data) {
    auto size = 0;
    std::vector<std::string> strs;
    std::vector<std::pair<const uint8_t*, int64_t>> null_bitmaps;
    for (auto batch : *data) {
        auto data = batch.ValueOrDie()->column(0);
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
    if (file_) {
        target_ = std::make_shared<MmapChunkTarget>(*file_, file_offset_);
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

std::shared_ptr<Chunk>
SparseFloatVectorChunkWriter::finish() {
    auto [data, size] = target_->get();
    return std::make_shared<SparseFloatVectorChunk>(
        row_nums_, data, size, nullable_);
}

std::shared_ptr<Chunk>
create_chunk(const FieldMeta& field_meta,
             int dim,
             std::shared_ptr<arrow::RecordBatchReader> r) {
    std::shared_ptr<ChunkWriterBase> w;
    bool nullable = field_meta.is_nullable();

    switch (field_meta.get_data_type()) {
        case milvus::DataType::BOOL: {
            w = std::make_shared<ChunkWriter<arrow::BooleanArray, bool>>(
                dim, nullable);
            break;
        }
        case milvus::DataType::INT8: {
            w = std::make_shared<ChunkWriter<arrow::Int8Array, int8_t>>(
                dim, nullable);
            break;
        }
        case milvus::DataType::INT16: {
            w = std::make_shared<ChunkWriter<arrow::Int16Array, int16_t>>(
                dim, nullable);
            break;
        }
        case milvus::DataType::INT32: {
            w = std::make_shared<ChunkWriter<arrow::Int32Array, int32_t>>(
                dim, nullable);
            break;
        }
        case milvus::DataType::INT64: {
            w = std::make_shared<ChunkWriter<arrow::Int64Array, int64_t>>(
                dim, nullable);
            break;
        }
        case milvus::DataType::FLOAT: {
            w = std::make_shared<ChunkWriter<arrow::FloatArray, float>>(
                dim, nullable);
            break;
        }
        case milvus::DataType::DOUBLE: {
            w = std::make_shared<ChunkWriter<arrow::DoubleArray, double>>(
                dim, nullable);
            break;
        }
        case milvus::DataType::VECTOR_FLOAT: {
            w = std::make_shared<
                ChunkWriter<arrow::FixedSizeBinaryArray, float>>(dim, nullable);
            break;
        }
        case milvus::DataType::VECTOR_BINARY: {
            w = std::make_shared<
                ChunkWriter<arrow::FixedSizeBinaryArray, uint8_t>>(dim / 8,
                                                                   nullable);
            break;
        }
        case milvus::DataType::VECTOR_FLOAT16: {
            w = std::make_shared<
                ChunkWriter<arrow::FixedSizeBinaryArray, knowhere::fp16>>(
                dim, nullable);
            break;
        }
        case milvus::DataType::VECTOR_BFLOAT16: {
            w = std::make_shared<
                ChunkWriter<arrow::FixedSizeBinaryArray, knowhere::bf16>>(
                dim, nullable);
            break;
        }
        case milvus::DataType::VARCHAR:
        case milvus::DataType::STRING: {
            w = std::make_shared<StringChunkWriter>(nullable);
            break;
        }
        case milvus::DataType::JSON: {
            w = std::make_shared<JSONChunkWriter>(nullable);
            break;
        }
        case milvus::DataType::ARRAY: {
            w = std::make_shared<ArrayChunkWriter>(
                field_meta.get_element_type(), nullable);
            break;
        }
        case milvus::DataType::VECTOR_SPARSE_FLOAT: {
            w = std::make_shared<SparseFloatVectorChunkWriter>(nullable);
            break;
        }
        default:
            PanicInfo(Unsupported, "Unsupported data type");
    }

    w->write(std::move(r));
    return w->finish();
}

std::shared_ptr<Chunk>
create_chunk(const FieldMeta& field_meta,
             int dim,
             File& file,
             size_t file_offset,
             std::shared_ptr<arrow::RecordBatchReader> r) {
    std::shared_ptr<ChunkWriterBase> w;
    bool nullable = field_meta.is_nullable();

    switch (field_meta.get_data_type()) {
        case milvus::DataType::BOOL: {
            w = std::make_shared<ChunkWriter<arrow::BooleanArray, bool>>(
                dim, file, file_offset, nullable);
            break;
        }
        case milvus::DataType::INT8: {
            w = std::make_shared<ChunkWriter<arrow::Int8Array, int8_t>>(
                dim, file, file_offset, nullable);
            break;
        }
        case milvus::DataType::INT16: {
            w = std::make_shared<ChunkWriter<arrow::Int16Array, int16_t>>(
                dim, file, file_offset, nullable);
            break;
        }
        case milvus::DataType::INT32: {
            w = std::make_shared<ChunkWriter<arrow::Int32Array, int32_t>>(
                dim, file, file_offset, nullable);
            break;
        }
        case milvus::DataType::INT64: {
            w = std::make_shared<ChunkWriter<arrow::Int64Array, int64_t>>(
                dim, file, file_offset, nullable);
            break;
        }
        case milvus::DataType::FLOAT: {
            w = std::make_shared<ChunkWriter<arrow::FloatArray, float>>(
                dim, file, file_offset, nullable);
            break;
        }
        case milvus::DataType::DOUBLE: {
            w = std::make_shared<ChunkWriter<arrow::DoubleArray, double>>(
                dim, file, file_offset, nullable);
            break;
        }
        case milvus::DataType::VECTOR_FLOAT: {
            w = std::make_shared<
                ChunkWriter<arrow::FixedSizeBinaryArray, float>>(
                dim, file, file_offset, nullable);
            break;
        }
        case milvus::DataType::VECTOR_BINARY: {
            w = std::make_shared<
                ChunkWriter<arrow::FixedSizeBinaryArray, uint8_t>>(
                dim / 8, file, file_offset, nullable);
            break;
        }
        case milvus::DataType::VECTOR_FLOAT16: {
            w = std::make_shared<
                ChunkWriter<arrow::FixedSizeBinaryArray, knowhere::fp16>>(
                dim, file, file_offset, nullable);
            break;
        }
        case milvus::DataType::VECTOR_BFLOAT16: {
            w = std::make_shared<
                ChunkWriter<arrow::FixedSizeBinaryArray, knowhere::bf16>>(
                dim, file, file_offset, nullable);
            break;
        }
        case milvus::DataType::VARCHAR:
        case milvus::DataType::STRING: {
            w = std::make_shared<StringChunkWriter>(
                file, file_offset, nullable);
            break;
        }
        case milvus::DataType::JSON: {
            w = std::make_shared<JSONChunkWriter>(file, file_offset, nullable);
            break;
        }
        case milvus::DataType::ARRAY: {
            w = std::make_shared<ArrayChunkWriter>(
                field_meta.get_element_type(), file, file_offset, nullable);
            break;
        }
        case milvus::DataType::VECTOR_SPARSE_FLOAT: {
            w = std::make_shared<SparseFloatVectorChunkWriter>(
                file, file_offset, nullable);
            break;
        }
        default:
            PanicInfo(Unsupported, "Unsupported data type");
    }

    w->write(std::move(r));
    return w->finish();
}

}  // namespace milvus