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
#include <string_view>
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
    std::vector<std::string_view> strs;
    std::vector<std::pair<const uint8_t*, int64_t>> null_bitmaps;
    for (auto batch : *data) {
        auto data = batch.ValueOrDie()->column(0);
        auto array = std::dynamic_pointer_cast<arrow::StringArray>(data);
        for (int i = 0; i < array->length(); i++) {
            auto str = array->GetView(i);
            strs.push_back(str);
            size += str.size();
        }
        auto null_bitmap_n = (data->length() + 7) / 8;
        null_bitmaps.emplace_back(data->null_bitmap_data(), null_bitmap_n);
        size += null_bitmap_n;
        row_nums_ += array->length();
    }
    size += sizeof(uint64_t) * row_nums_ + MMAP_STRING_PADDING;
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
    offsets_pos_ = target_->tell();
    target_->skip(sizeof(uint64_t) * row_nums_);

    for (auto str : strs) {
        offsets_.push_back(target_->tell());
        target_->write(str.data(), str.size());
    }
}

std::shared_ptr<Chunk>
StringChunkWriter::finish() {
    // write padding, maybe not needed anymore
    // FIXME
    char padding[MMAP_STRING_PADDING];
    target_->write(padding, MMAP_STRING_PADDING);

    // seek back to write offsets
    target_->seek(offsets_pos_);
    target_->write(offsets_.data(), offsets_.size() * sizeof(uint64_t));
    auto [data, size] = target_->get();
    return std::make_shared<StringChunk>(row_nums_, data, size);
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
        AssertInfo(data->length() % 8 == 0,
                   "String length should be multiple of 8");
        auto null_bitmap_n = (data->length() + 7) / 8;
        null_bitmaps.emplace_back(data->null_bitmap_data(), null_bitmap_n);
        size += null_bitmap_n;
        row_nums_ += array->length();
    }
    size += sizeof(uint64_t) * row_nums_ + simdjson::SIMDJSON_PADDING;
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

    offsets_pos_ = target_->tell();
    target_->skip(sizeof(uint64_t) * row_nums_);

    // write data
    for (auto json : jsons) {
        offsets_.push_back(target_->tell());
        target_->write(json.data().data(), json.data().size());
    }
}

std::shared_ptr<Chunk>
JSONChunkWriter::finish() {
    char padding[simdjson::SIMDJSON_PADDING];
    target_->write(padding, simdjson::SIMDJSON_PADDING);

    // write offsets and padding
    target_->seek(offsets_pos_);
    target_->write(offsets_.data(), offsets_.size() * sizeof(uint64_t));
    auto [data, size] = target_->get();
    return std::make_shared<JSONChunk>(row_nums_, data, size);
}

void
ArrayChunkWriter::write(std::shared_ptr<arrow::RecordBatchReader> data) {
    auto size = 0;

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
            // element offsets size
            size += sizeof(uint64_t) * arr.length();
        }
        row_nums_ += array->length();
        auto null_bitmap_n = (data->length() + 7) / 8;
        null_bitmaps.emplace_back(data->null_bitmap_data(), null_bitmap_n);
        size += null_bitmap_n;
    }

    auto is_string = IsStringDataType(element_type_);
    // offsets + lens
    size += is_string ? sizeof(uint64_t) * row_nums_ * 2 + MMAP_ARRAY_PADDING
                      : sizeof(uint64_t) * row_nums_ + MMAP_ARRAY_PADDING;
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

    offsets_pos_ = target_->tell();
    target_->skip(sizeof(uint64_t) * row_nums_ * 2);
    for (auto& arr : arrays) {
        // write elements offsets
        offsets_.push_back(target_->tell());
        if (is_string) {
            target_->write(arr.get_offsets().data(),
                           arr.get_offsets().size() * sizeof(uint64_t));
        }
        lens_.push_back(arr.length());
        target_->write(arr.data(), arr.byte_size());
    }
}

std::shared_ptr<Chunk>
ArrayChunkWriter::finish() {
    char padding[MMAP_ARRAY_PADDING];
    target_->write(padding, MMAP_ARRAY_PADDING);

    // write offsets and lens
    target_->seek(offsets_pos_);
    for (size_t i = 0; i < offsets_.size(); i++) {
        target_->write(&offsets_[i], sizeof(uint64_t));
        target_->write(&lens_[i], sizeof(uint64_t));
    }
    auto [data, size] = target_->get();
    return std::make_shared<ArrayChunk>(row_nums_, data, size, element_type_);
}

void
SparseFloatVectorChunkWriter::write(
    std::shared_ptr<arrow::RecordBatchReader> data) {
    auto size = 0;
    std::vector<std::string_view> strs;
    std::vector<std::pair<const uint8_t*, int64_t>> null_bitmaps;
    for (auto batch : *data) {
        auto data = batch.ValueOrDie()->column(0);
        auto array = std::dynamic_pointer_cast<arrow::BinaryArray>(data);
        for (int i = 0; i < array->length(); i++) {
            auto str = array->GetView(i);
            strs.push_back(str);
            size += str.size();
        }
        auto null_bitmap_n = (data->length() + 7) / 8;
        null_bitmaps.emplace_back(data->null_bitmap_data(), null_bitmap_n);
        size += null_bitmap_n;
        row_nums_ += array->length();
    }
    size += sizeof(uint64_t) * row_nums_;
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
    offsets_pos_ = target_->tell();
    target_->skip(sizeof(uint64_t) * row_nums_);

    for (auto str : strs) {
        offsets_.push_back(target_->tell());
        target_->write(str.data(), str.size());
    }
}

std::shared_ptr<Chunk>
SparseFloatVectorChunkWriter::finish() {
    // seek back to write offsets
    target_->seek(offsets_pos_);
    target_->write(offsets_.data(), offsets_.size() * sizeof(uint64_t));
    auto [data, size] = target_->get();
    return std::make_shared<SparseFloatVectorChunk>(row_nums_, data, size);
}

std::shared_ptr<Chunk>
create_chunk(const FieldMeta& field_meta,
             int dim,
             std::shared_ptr<arrow::RecordBatchReader> r) {
    std::shared_ptr<ChunkWriterBase> w;

    switch (field_meta.get_data_type()) {
        case milvus::DataType::BOOL: {
            w = std::make_shared<ChunkWriter<arrow::BooleanArray, bool>>(dim);
            break;
        }
        case milvus::DataType::INT8: {
            w = std::make_shared<ChunkWriter<arrow::Int8Array, int8_t>>(dim);
            break;
        }
        case milvus::DataType::INT16: {
            w = std::make_shared<ChunkWriter<arrow::Int16Array, int16_t>>(dim);
            break;
        }
        case milvus::DataType::INT32: {
            w = std::make_shared<ChunkWriter<arrow::Int32Array, int32_t>>(dim);
            break;
        }
        case milvus::DataType::INT64: {
            w = std::make_shared<ChunkWriter<arrow::Int64Array, int64_t>>(dim);
            break;
        }
        case milvus::DataType::FLOAT: {
            w = std::make_shared<ChunkWriter<arrow::FloatArray, float>>(dim);
            break;
        }
        case milvus::DataType::DOUBLE: {
            w = std::make_shared<ChunkWriter<arrow::DoubleArray, double>>(dim);
            break;
        }
        case milvus::DataType::VECTOR_FLOAT: {
            w = std::make_shared<
                ChunkWriter<arrow::FixedSizeBinaryArray, float>>(dim);
            break;
        }
        case milvus::DataType::VECTOR_BINARY: {
            w = std::make_shared<
                ChunkWriter<arrow::FixedSizeBinaryArray, uint8_t>>(dim / 8);
            break;
        }
        case milvus::DataType::VECTOR_FLOAT16: {
            w = std::make_shared<
                ChunkWriter<arrow::FixedSizeBinaryArray, knowhere::fp16>>(dim);
            break;
        }
        case milvus::DataType::VECTOR_BFLOAT16: {
            w = std::make_shared<
                ChunkWriter<arrow::FixedSizeBinaryArray, knowhere::bf16>>(dim);
            break;
        }
        case milvus::DataType::VARCHAR:
        case milvus::DataType::STRING: {
            w = std::make_shared<StringChunkWriter>();
            break;
        }
        case milvus::DataType::JSON: {
            w = std::make_shared<JSONChunkWriter>();
            break;
        }
        case milvus::DataType::ARRAY: {
            w = std::make_shared<ArrayChunkWriter>(
                field_meta.get_element_type());
            break;
        }
        case milvus::DataType::VECTOR_SPARSE_FLOAT: {
            w = std::make_shared<SparseFloatVectorChunkWriter>();
            break;
        }
        default:
            PanicInfo(Unsupported, "Unsupported data type");
    }

    w->write(r);
    return w->finish();
}

}  // namespace milvus