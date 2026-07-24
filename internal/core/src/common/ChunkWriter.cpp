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

#include <array>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "NamedType/underlying_functionalities.hpp"
#include "arrow/array/array_binary.h"
#include "arrow/array/array_nested.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "common/Array.h"
#include "common/ArrayChunkBuilder.h"
#include "common/ArrayValue.h"
#include "common/ColumnarArrayChunk.h"
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
#include "storage/MmapManager.h"

namespace milvus {
namespace {

struct ColumnarArrayBuildNode {
    ArrayOffsets offsets;
    // Serialized with the existing Chunk/Arrow convention: one bit per row,
    // where 1 means valid and 0 means null.
    std::vector<uint8_t> validity_bitmap;
    std::unique_ptr<ColumnarArrayBuildNode> array_child;
    DataType leaf_type{DataType::NONE};
    std::vector<char> fixed_data;
    std::vector<uint32_t> string_offsets;
    std::string string_data;
};

class BorrowedArrayChunkTarget final : public ChunkTarget {
 public:
    BorrowedArrayChunkTarget(char* data, size_t capacity)
        : data_(data), capacity_(capacity) {
    }

    void
    write(const void* data, size_t size) override {
        AssertInfo(size <= capacity_ - position_,
                   "borrowed array chunk target capacity exceeded");
        if (size != 0) {
            std::memcpy(data_ + position_, data, size);
        }
        position_ += size;
    }

    char*
    release() override {
        return data_;
    }

    size_t
    tell() override {
        return position_;
    }

 private:
    char* data_;
    size_t capacity_;
    size_t position_{0};
};

size_t
CheckedAdd(size_t left, size_t right, std::string_view description) {
    AssertInfo(left <= std::numeric_limits<size_t>::max() - right,
               "{} size overflow: {} + {}",
               description,
               left,
               right);
    return left + right;
}

size_t
CheckedMultiply(size_t left, size_t right, std::string_view description) {
    AssertInfo(right == 0 || left <= std::numeric_limits<size_t>::max() / right,
               "{} size overflow: {} * {}",
               description,
               left,
               right);
    return left * right;
}

DataType
GetColumnarArrayElementType(const proto::schema::TypeSchema& type) {
    return type.has_element_schema() ? DataType::ARRAY
                                     : DataType(type.element_type());
}

bool
IsSupportedColumnarArrayLeaf(DataType data_type) {
    switch (data_type) {
        case DataType::BOOL:
        case DataType::INT8:
        case DataType::INT16:
        case DataType::INT32:
        case DataType::INT64:
        case DataType::FLOAT:
        case DataType::DOUBLE:
        case DataType::STRING:
        case DataType::VARCHAR:
            return true;
        default:
            return false;
    }
}

void
ValidateColumnarArrayType(const proto::schema::TypeSchema& type) {
    AssertInfo(DataType(type.data_type()) == DataType::ARRAY,
               "columnar array type must be ARRAY, got {}",
               type.data_type());
    if (type.has_element_schema()) {
        AssertInfo(type.element_type() == proto::schema::DataType::None,
                   "nested columnar array type must not set element_type");
        AssertInfo(
            DataType(type.element_schema().data_type()) == DataType::ARRAY,
            "nested columnar array element_schema must be ARRAY, got {}",
            type.element_schema().data_type());
        ValidateColumnarArrayType(type.element_schema());
        return;
    }

    const auto element_type = DataType(type.element_type());
    AssertInfo(element_type != DataType::NONE,
               "leaf columnar array type must set element_type");
    AssertInfo(element_type != DataType::ARRAY,
               "nested columnar array type must use element_schema");
    AssertInfo(IsSupportedColumnarArrayLeaf(element_type),
               "unsupported columnar array leaf type {}",
               element_type);
}

template <typename T>
void
AppendFixedValue(std::vector<char>& data, T value) {
    const auto old_size = data.size();
    data.resize(CheckedAdd(old_size, sizeof(T), "fixed array leaf"));
    std::memcpy(data.data() + old_size, &value, sizeof(T));
}

size_t
AppendLeafRow(ColumnarArrayBuildNode& node,
              const ScalarFieldProto& row,
              DataType data_type) {
    if (row.data_case() == ScalarFieldProto::DATA_NOT_SET) {
        return 0;
    }

    switch (data_type) {
        case DataType::BOOL: {
            AssertInfo(row.data_case() == ScalarFieldProto::kBoolData,
                       "expected bool array row, got proto case {}",
                       static_cast<int>(row.data_case()));
            for (auto value : row.bool_data().data()) {
                AppendFixedValue<uint8_t>(node.fixed_data, value ? 1 : 0);
            }
            return row.bool_data().data_size();
        }
        case DataType::INT8:
        case DataType::INT16:
        case DataType::INT32: {
            AssertInfo(row.data_case() == ScalarFieldProto::kIntData,
                       "expected int array row, got proto case {}",
                       static_cast<int>(row.data_case()));
            for (auto value : row.int_data().data()) {
                AppendFixedValue<int32_t>(node.fixed_data, value);
            }
            return row.int_data().data_size();
        }
        case DataType::INT64: {
            AssertInfo(row.data_case() == ScalarFieldProto::kLongData,
                       "expected long array row, got proto case {}",
                       static_cast<int>(row.data_case()));
            for (auto value : row.long_data().data()) {
                AppendFixedValue<int64_t>(node.fixed_data, value);
            }
            return row.long_data().data_size();
        }
        case DataType::FLOAT: {
            AssertInfo(row.data_case() == ScalarFieldProto::kFloatData,
                       "expected float array row, got proto case {}",
                       static_cast<int>(row.data_case()));
            for (auto value : row.float_data().data()) {
                AppendFixedValue<float>(node.fixed_data, value);
            }
            return row.float_data().data_size();
        }
        case DataType::DOUBLE: {
            AssertInfo(row.data_case() == ScalarFieldProto::kDoubleData,
                       "expected double array row, got proto case {}",
                       static_cast<int>(row.data_case()));
            for (auto value : row.double_data().data()) {
                AppendFixedValue<double>(node.fixed_data, value);
            }
            return row.double_data().data_size();
        }
        case DataType::STRING:
        case DataType::VARCHAR: {
            AssertInfo(row.data_case() == ScalarFieldProto::kStringData,
                       "expected string array row, got proto case {}",
                       static_cast<int>(row.data_case()));
            for (const auto& value : row.string_data().data()) {
                node.string_data.append(value);
                AssertInfo(
                    node.string_data.size() <=
                        static_cast<size_t>(
                            std::numeric_limits<uint32_t>::max()),
                    "columnar array string leaf exceeds uint32 offset range");
                node.string_offsets.push_back(
                    static_cast<uint32_t>(node.string_data.size()));
            }
            return row.string_data().data_size();
        }
        default:
            ThrowInfo(Unsupported,
                      "unsupported columnar array leaf type {}",
                      data_type);
    }
}

std::unique_ptr<ColumnarArrayBuildNode>
BuildColumnarArrayNodeImpl(const std::vector<const ScalarFieldProto*>& rows,
                           const proto::schema::TypeSchema& type) {
    auto node = std::make_unique<ColumnarArrayBuildNode>();
    node->validity_bitmap.resize(rows.size() / 8 + (rows.size() % 8 != 0), 0);
    node->offsets.reserve(rows.size() + 1);
    node->offsets.push_back(0);
    for (size_t i = 0; i < rows.size(); ++i) {
        if (rows[i]->data_case() != ScalarFieldProto::DATA_NOT_SET) {
            node->validity_bitmap[i >> 3] |=
                static_cast<uint8_t>(1U << (i & 0x07));
        }
    }

    if (type.has_element_schema()) {
        std::vector<const ScalarFieldProto*> child_rows;
        for (const auto* row : rows) {
            if (row->data_case() == ScalarFieldProto::DATA_NOT_SET) {
                node->offsets.push_back(child_rows.size());
                continue;
            }

            AssertInfo(row->data_case() == ScalarFieldProto::kArrayData,
                       "expected nested array proto row, got case {}",
                       static_cast<int>(row->data_case()));
            const auto& array_data = row->array_data();
            const auto expected_element_type =
                static_cast<proto::schema::DataType>(
                    GetColumnarArrayElementType(type.element_schema()));
            if (array_data.element_type() != proto::schema::DataType::None) {
                AssertInfo(
                    array_data.element_type() == expected_element_type,
                    "nested array proto element type must be {}, got {}",
                    expected_element_type,
                    array_data.element_type());
            }
            for (const auto& child_row : array_data.data()) {
                child_rows.push_back(&child_row);
            }
            node->offsets.push_back(child_rows.size());
        }
        node->array_child =
            BuildColumnarArrayNodeImpl(child_rows, type.element_schema());
        return node;
    }

    node->leaf_type = GetColumnarArrayElementType(type);
    if (IsStringDataType(node->leaf_type)) {
        node->string_offsets.push_back(0);
    }

    size_t child_count = 0;
    for (const auto* row : rows) {
        child_count = CheckedAdd(child_count,
                                 AppendLeafRow(*node, *row, node->leaf_type),
                                 "columnar array child count");
        node->offsets.push_back(child_count);
    }

    if (IsStringDataType(node->leaf_type)) {
        const auto offsets_bytes = CheckedMultiply(
            node->string_offsets.size(), sizeof(uint32_t), "string offsets");
        AssertInfo(offsets_bytes <= static_cast<size_t>(
                                        std::numeric_limits<uint32_t>::max()),
                   "columnar array string offsets exceed uint32 range");
        for (auto& offset : node->string_offsets) {
            AssertInfo(
                offset <= std::numeric_limits<uint32_t>::max() - offsets_bytes,
                "columnar array string offset exceeds uint32 range");
            offset += static_cast<uint32_t>(offsets_bytes);
        }
    }

    return node;
}

std::unique_ptr<ColumnarArrayBuildNode>
BuildColumnarArrayNode(const std::vector<const ScalarFieldProto*>& rows,
                       const proto::schema::TypeSchema& type) {
    ValidateColumnarArrayType(type);
    return BuildColumnarArrayNodeImpl(rows, type);
}

size_t
ColumnarArrayNodeSize(const ColumnarArrayBuildNode& node);

void
WriteColumnarArrayNode(const ColumnarArrayBuildNode& node,
                       const std::shared_ptr<ChunkTarget>& target);

void
WriteColumnarArrayAlignment(int64_t row_count,
                            bool nullable,
                            const std::shared_ptr<ChunkTarget>& target);

size_t
ColumnarArrayChildSize(const ColumnarArrayBuildNode& node) {
    if (node.array_child != nullptr) {
        return ColumnarArrayNodeSize(*node.array_child);
    }
    if (IsStringDataType(node.leaf_type)) {
        const auto offsets_size = CheckedMultiply(
            node.string_offsets.size(), sizeof(uint32_t), "string offsets");
        return CheckedAdd(offsets_size, node.string_data.size(), "array child");
    }
    return node.fixed_data.size();
}

size_t
ColumnarArrayNodeSize(const ColumnarArrayBuildNode& node) {
    const auto row_count = node.offsets.size() - 1;
    AssertInfo(
        row_count <= static_cast<size_t>(std::numeric_limits<int64_t>::max()),
        "nested array row count {} exceeds int64 range",
        row_count);
    const auto prefix_size = ColumnarArrayChunk::RootDataOffset(
        static_cast<int64_t>(row_count), true);
    const auto offsets_size = CheckedMultiply(
        node.offsets.size(), sizeof(ArrayOffset), "array offsets");
    auto size = CheckedAdd(prefix_size, offsets_size, "array node");
    return CheckedAdd(size, ColumnarArrayChildSize(node), "array node");
}

void
WriteColumnarArrayChild(const ColumnarArrayBuildNode& node,
                        const std::shared_ptr<ChunkTarget>& target) {
    if (node.array_child != nullptr) {
        WriteColumnarArrayNode(*node.array_child, target);
        return;
    }
    if (IsStringDataType(node.leaf_type)) {
        target->write(node.string_offsets.data(),
                      node.string_offsets.size() * sizeof(uint32_t));
        if (!node.string_data.empty()) {
            target->write(node.string_data.data(), node.string_data.size());
        }
        return;
    }
    if (!node.fixed_data.empty()) {
        target->write(node.fixed_data.data(), node.fixed_data.size());
    }
}

void
WriteColumnarArrayNode(const ColumnarArrayBuildNode& node,
                       const std::shared_ptr<ChunkTarget>& target) {
    const auto row_count = node.offsets.size() - 1;
    target->write(node.validity_bitmap.data(), node.validity_bitmap.size());
    WriteColumnarArrayAlignment(static_cast<int64_t>(row_count), true, target);
    target->write(node.offsets.data(),
                  node.offsets.size() * sizeof(ArrayOffset));
    WriteColumnarArrayChild(node, target);
}

void
WriteColumnarArrayRootNode(const ColumnarArrayBuildNode& node,
                           const std::shared_ptr<ChunkTarget>& target) {
    target->write(node.offsets.data(),
                  node.offsets.size() * sizeof(ArrayOffset));
    WriteColumnarArrayChild(node, target);
}

size_t
ColumnarArraySerializedSize(int64_t row_count,
                            bool nullable,
                            const ColumnarArrayBuildNode& root) {
    auto size = ColumnarArrayChunk::RootDataOffset(row_count, nullable);
    const auto offsets_size = CheckedMultiply(
        root.offsets.size(), sizeof(ArrayOffset), "root array offsets");
    size = CheckedAdd(size, offsets_size, "columnar array");
    size = CheckedAdd(size, ColumnarArrayChildSize(root), "columnar array");
    return CheckedAdd(size, MMAP_ARRAY_PADDING, "columnar array");
}

void
WriteColumnarArrayAlignment(int64_t row_count,
                            bool nullable,
                            const std::shared_ptr<ChunkTarget>& target) {
    const auto null_bitmap_bytes =
        nullable ? (static_cast<size_t>(row_count) + 7) / 8 : 0;
    const auto root_data_offset =
        ColumnarArrayChunk::RootDataOffset(row_count, nullable);
    const auto alignment_bytes = root_data_offset - null_bitmap_bytes;
    if (alignment_bytes != 0) {
        std::array<char, alignof(ArrayOffset)> zeros{};
        target->write(zeros.data(), alignment_bytes);
    }
}

}  // namespace

std::shared_ptr<const ColumnarArrayChunk>
CreateMmapColumnarArrayChunkFromProtoRows(
    std::span<const ScalarFieldProto* const> rows,
    const proto::schema::TypeSchema& type,
    bool nullable,
    const storage::MmapChunkDescriptorPtr& mmap_descriptor) {
    AssertInfo(
        rows.size() <= static_cast<size_t>(std::numeric_limits<int64_t>::max()),
        "nested ARRAY row count {} exceeds int64 range",
        rows.size());

    std::vector<const ScalarFieldProto*> row_ptrs(rows.begin(), rows.end());
    if (!nullable) {
        for (const auto* row : row_ptrs) {
            AssertInfo(row->data_case() != ScalarFieldProto::DATA_NOT_SET,
                       "non-nullable nested ARRAY row has no payload");
        }
    }

    auto root = BuildColumnarArrayNode(row_ptrs, type);
    const auto row_count = static_cast<int64_t>(rows.size());
    const auto serialized_size =
        ColumnarArraySerializedSize(row_count, nullable, *root);

    auto mmap_manager =
        storage::MmapManager::GetInstance().GetMmapChunkManager();
    auto* data = static_cast<char*>(
        mmap_manager->Allocate(mmap_descriptor, serialized_size));
    AssertInfo(data != nullptr,
               "failed to allocate {} bytes for nested ARRAY mmap block",
               serialized_size);

    auto target =
        std::make_shared<BorrowedArrayChunkTarget>(data, serialized_size);
    if (nullable) {
        target->write(root->validity_bitmap.data(),
                      root->validity_bitmap.size());
    }
    WriteColumnarArrayAlignment(row_count, nullable, target);
    WriteColumnarArrayRootNode(*root, target);
    char padding[MMAP_ARRAY_PADDING] = {};
    target->write(padding, MMAP_ARRAY_PADDING);

    return std::make_shared<const ColumnarArrayChunk>(
        row_count, data, serialized_size, type, nullable, nullptr);
}

struct ColumnarArrayChunkWriter::Impl {
    std::vector<ScalarFieldProto> rows;
    std::unique_ptr<ColumnarArrayBuildNode> root;
    size_t serialized_size{0};
};

ColumnarArrayChunkWriter::ColumnarArrayChunkWriter(
    proto::schema::TypeSchema type, bool nullable)
    : ChunkWriterBase(nullable),
      type_(std::move(type)),
      impl_(std::make_unique<Impl>()) {
    ValidateColumnarArrayType(type_);
}

ColumnarArrayChunkWriter::~ColumnarArrayChunkWriter() = default;

std::pair<size_t, size_t>
ColumnarArrayChunkWriter::calculate_size(const arrow::ArrayVector& array_vec) {
    row_nums_ = 0;
    for (const auto& data : array_vec) {
        row_nums_ =
            CheckedAdd(row_nums_, data->length(), "columnar array row count");
    }
    AssertInfo(
        row_nums_ <= static_cast<size_t>(std::numeric_limits<int64_t>::max()),
        "columnar array row count {} exceeds int64 range",
        row_nums_);

    impl_->rows.clear();
    impl_->rows.reserve(row_nums_);
    for (const auto& data : array_vec) {
        auto array = std::dynamic_pointer_cast<arrow::BinaryArray>(data);
        AssertInfo(array != nullptr,
                   "ColumnarArrayChunkWriter expects arrow::BinaryArray, got "
                   "type id {}; upstream normalizer must coerce to BINARY",
                   data ? static_cast<int>(data->type_id()) : -1);
        AssertInfo(nullable_ || array->null_count() == 0,
                   "non-nullable nested ARRAY column contains {} null rows",
                   array->null_count());
        for (int64_t i = 0; i < array->length(); ++i) {
            ScalarFieldProto row;
            if (!array->IsNull(i)) {
                const auto value = array->GetView(i);
                AssertInfo(row.ParseFromArray(value.data(), value.size()),
                           "failed to parse columnar array row {}",
                           i);
                AssertInfo(
                    row.data_case() != ScalarFieldProto::DATA_NOT_SET,
                    "valid columnar array row {} has no ScalarField payload",
                    i);
            }
            impl_->rows.emplace_back(std::move(row));
        }
    }

    std::vector<const ScalarFieldProto*> rows;
    rows.reserve(impl_->rows.size());
    for (const auto& row : impl_->rows) {
        rows.push_back(&row);
    }
    impl_->root = BuildColumnarArrayNode(rows, type_);
    impl_->serialized_size = ColumnarArraySerializedSize(
        static_cast<int64_t>(row_nums_), nullable_, *impl_->root);
    return {impl_->serialized_size, row_nums_};
}

void
ColumnarArrayChunkWriter::write_to_target(
    const arrow::ArrayVector& array_vec,
    const std::shared_ptr<ChunkTarget>& target) {
    if (nullable_) {
        std::vector<std::tuple<const uint8_t*, int64_t, int64_t>> null_bitmaps;
        null_bitmaps.reserve(array_vec.size());
        for (const auto& data : array_vec) {
            null_bitmaps.emplace_back(
                data->null_bitmap_data(), data->length(), data->offset());
        }
        write_null_bit_maps(null_bitmaps, target);
    }

    WriteColumnarArrayAlignment(
        static_cast<int64_t>(row_nums_), nullable_, target);
    WriteColumnarArrayRootNode(*impl_->root, target);
    char padding[MMAP_ARRAY_PADDING] = {};
    target->write(padding, MMAP_ARRAY_PADDING);

    impl_->root.reset();
    impl_->rows.clear();
    impl_->rows.shrink_to_fit();
}

std::shared_ptr<const ArrayValueStorage>
CreateArrayValueStorageFromProto(const ScalarFieldProto& row,
                                 std::shared_ptr<const proto::schema::TypeSchema>
                                     type) {
    AssertInfo(type != nullptr, "ArrayValue type must not be null");
    auto root = BuildColumnarArrayNode({&row}, *type);

    auto storage = std::make_shared<ArrayValueStorage>();
    storage->type = std::move(type);
    storage->length = root->offsets.back();
    storage->is_null = row.data_case() == ScalarFieldProto::DATA_NOT_SET;

    const auto child_size = ColumnarArrayChildSize(*root);
    const auto storage_size =
        CheckedAdd(child_size, MMAP_ARRAY_PADDING, "ArrayValue storage");
    storage->buffer.resize(storage_size);
    auto target = std::make_shared<BorrowedArrayChunkTarget>(
        storage->buffer.data(), storage->buffer.size());
    WriteColumnarArrayChild(*root, target);
    char padding[MMAP_ARRAY_PADDING] = {};
    target->write(padding, MMAP_ARRAY_PADDING);

    auto* data = target->release();
    storage->child = array_detail::CreateColumnarArrayChild(
        storage->type, storage->length, data, child_size, nullptr);
    return storage;
}

namespace {

size_t
CalculateBinaryLikeChunkSize(
    const arrow::ArrayVector& array_vec,
    bool nullable,
    size_t& row_nums,
    std::vector<uint32_t>& offsets,
    std::vector<std::pair<const uint8_t*, size_t>>& payload_segments,
    size_t padding_size,
    const char* writer_name,
    const char* chunk_name) {
    row_nums = 0;
    for (const auto& data : array_vec) {
        row_nums += data->length();
    }

    const size_t offset_num = row_nums + 1;
    const size_t null_bitmap_bytes = nullable ? (row_nums + 7) / 8 : 0;
    size_t cursor = null_bitmap_bytes + sizeof(uint32_t) * offset_num;

    offsets.clear();
    offsets.reserve(offset_num);
    payload_segments.clear();
    payload_segments.reserve(array_vec.size());

    for (const auto& data : array_vec) {
        auto array = std::dynamic_pointer_cast<arrow::BinaryArray>(data);
        AssertInfo(array != nullptr,
                   "{} expects arrow::BinaryArray, got type id {}; upstream "
                   "normalizer must coerce to BINARY",
                   writer_name,
                   data ? static_cast<int>(data->type_id()) : -1);

        const auto length = array->length();
        const auto payload_begin = array->value_offset(0);
        const auto payload_end = array->value_offset(length);
        AssertInfo(payload_end >= payload_begin,
                   "{} got invalid Arrow binary offsets: begin {}, end {}",
                   writer_name,
                   payload_begin,
                   payload_end);

        for (int64_t i = 0; i < length; ++i) {
            const auto relative_offset =
                static_cast<size_t>(array->value_offset(i) - payload_begin);
            const auto absolute_offset = cursor + relative_offset;
            offsets.push_back(static_cast<uint32_t>(absolute_offset));
        }

        const auto payload_size =
            static_cast<size_t>(payload_end - payload_begin);
        if (payload_size > 0) {
            payload_segments.emplace_back(
                array->value_data()->data() + payload_begin, payload_size);
        }
        cursor += payload_size;
    }

    AssertInfo(cursor <= std::numeric_limits<uint32_t>::max(),
               "{} chunk size {} exceeds uint32 offset limit",
               chunk_name,
               cursor);
    offsets.push_back(static_cast<uint32_t>(cursor));

    return cursor + padding_size;
}

void
WriteBinaryLikePayload(
    const std::vector<uint32_t>& offsets,
    const std::vector<std::pair<const uint8_t*, size_t>>& payload_segments,
    const std::shared_ptr<ChunkTarget>& target,
    size_t padding_size) {
    target->write(offsets.data(), offsets.size() * sizeof(uint32_t));

    for (const auto& segment : payload_segments) {
        target->write(segment.first, segment.second);
    }

    char padding[simdjson::SIMDJSON_PADDING] = {};
    target->write(padding, padding_size);
}

}  // namespace

std::pair<size_t, size_t>
StringChunkWriter::calculate_size(const arrow::ArrayVector& array_vec) {
    // Single pass over Arrow: compute row count, absolute Milvus offsets, and
    // contiguous Arrow payload segments. write_to_target emits each payload
    // segment with one write instead of one write per row.
    auto size = CalculateBinaryLikeChunkSize(array_vec,
                                             nullable_,
                                             row_nums_,
                                             offsets_,
                                             payload_segments_,
                                             MMAP_STRING_PADDING,
                                             "StringChunkWriter",
                                             "string");
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

    WriteBinaryLikePayload(
        offsets_, payload_segments_, target, MMAP_STRING_PADDING);
    offsets_.clear();
    payload_segments_.clear();
}

std::pair<size_t, size_t>
JSONChunkWriter::calculate_size(const arrow::ArrayVector& array_vec) {
    // Single pass over Arrow: compute row count, absolute Milvus offsets, and
    // contiguous Arrow payload segments. No per-row simdjson::padded_string
    // allocation — write_to_target emits a single SIMDJSON padding region.
    auto size = CalculateBinaryLikeChunkSize(array_vec,
                                             nullable_,
                                             row_nums_,
                                             offsets_,
                                             payload_segments_,
                                             simdjson::SIMDJSON_PADDING,
                                             "JSONChunkWriter",
                                             "json");
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

    WriteBinaryLikePayload(
        offsets_, payload_segments_, target, simdjson::SIMDJSON_PADDING);
    offsets_.clear();
    payload_segments_.clear();
}

std::pair<size_t, size_t>
GeometryChunkWriter::calculate_size(const arrow::ArrayVector& array_vec) {
    // Same layout as String/JSON; only the tail padding size differs.
    auto size = CalculateBinaryLikeChunkSize(array_vec,
                                             nullable_,
                                             row_nums_,
                                             offsets_,
                                             payload_segments_,
                                             MMAP_GEOMETRY_PADDING,
                                             "GeometryChunkWriter",
                                             "geometry");
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

    WriteBinaryLikePayload(
        offsets_, payload_segments_, target, MMAP_GEOMETRY_PADDING);
    offsets_.clear();
    payload_segments_.clear();
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
        AssertInfo(array != nullptr,
                   "ArrayChunkWriter expects arrow::BinaryArray, got "
                   "type id {}; upstream normalizer must coerce to BINARY",
                   data ? static_cast<int>(data->type_id()) : -1);
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
        AssertInfo(nullable_ || list_array->null_count() == 0,
                   "VECTOR_ARRAY does not support null rows");

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
                const int32_t* list_offsets = list_array->raw_value_offsets();
                int64_t actual_values_count = 0;
                for (int64_t i = 0; i < list_array->length(); ++i) {
                    if (nullable_ && list_array->IsNull(i)) {
                        continue;
                    }
                    actual_values_count +=
                        list_offsets[i + 1] - list_offsets[i];
                }
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

    if (nullable_) {
        total_size += (total_rows + 7) / 8;
    }
    // Add space for logical-row offset and length arrays.
    total_size += sizeof(uint32_t) * (row_nums_ * 2 + 1) + MMAP_ARRAY_PADDING;
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

    if (nullable_) {
        std::vector<std::tuple<const uint8_t*, int64_t, int64_t>> null_bitmaps;
        null_bitmaps.reserve(array_vec.size());
        for (const auto& data : array_vec) {
            null_bitmaps.emplace_back(
                data->null_bitmap_data(), data->length(), data->offset());
        }
        write_null_bit_maps(null_bitmaps, target);
    }

    uint32_t current_offset =
        (nullable_ ? static_cast<uint32_t>((row_nums_ + 7) / 8) : 0) +
        sizeof(uint32_t) * (row_nums_ * 2 + 1);

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
            if (nullable_ && list_array->IsNull(i)) {
                offsets_lens.push_back(current_offset);
                offsets_lens.push_back(0);
                continue;
            }
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
            if (field_meta.has_element_schema()) {
                return std::make_shared<ColumnarArrayChunkWriter>(
                    field_meta.get_array_type_schema(), nullable);
            }
            return std::make_shared<ArrayChunkWriter>(
                field_meta.get_element_type(), nullable);
        case milvus::DataType::VECTOR_SPARSE_U32_F32:
            return std::make_shared<SparseFloatVectorChunkWriter>(nullable);
        case milvus::DataType::VECTOR_ARRAY:
            return std::make_shared<VectorArrayChunkWriter>(
                dim, field_meta.get_element_type(), nullable);
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
            if (field_meta.has_element_schema()) {
                return std::make_unique<ColumnarArrayChunk>(
                    row_nums,
                    data,
                    size,
                    field_meta.get_array_type_schema(),
                    nullable,
                    chunk_mmap_guard);
            }
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
                chunk_mmap_guard,
                nullable);
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
    // The writer is one-shot. Release its scratch buffers before a
    // file-backed target populates the resulting mmap in release().
    cw.reset();
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
        // Release each one-shot writer's scratch buffers before the combined
        // file-backed mapping is populated in target->release().
        cws[i].reset();
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
