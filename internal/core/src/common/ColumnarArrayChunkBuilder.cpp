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

#include "common/ColumnarArrayChunkBuilder.h"

#include <array>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "arrow/array/array_binary.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/concatenate.h"
#include "common/ArrayValue.h"
#include "common/ChunkWriter.h"
#include "common/ColumnarArrayChunk.h"
#include "common/EasyAssert.h"
#include "storage/MmapManager.h"
#include "storage/Util.h"

namespace milvus {
namespace {

size_t
LeafPrefixSize(size_t count, bool nullable) {
    if (!nullable) {
        return 0;
    }
    const auto bitmap_bytes = (count + 7) / 8;
    return (bitmap_bytes + alignof(uint64_t) - 1) & ~(alignof(uint64_t) - 1);
}

void
ValidateArrowBufferElements(const std::shared_ptr<arrow::Buffer>& buffer,
                            uint64_t first,
                            uint64_t count,
                            size_t width,
                            const char* description) {
    if (count == 0) {
        return;
    }
    if (buffer == nullptr || buffer->size() < 0 ||
        first > static_cast<uint64_t>(buffer->size()) / width ||
        count > static_cast<uint64_t>(buffer->size()) / width - first) {
        ThrowInfo(ErrorCode::DataFormatBroken,
                  "array {} buffer is shorter than the declared range",
                  description);
    }
}

void
ValidateArrowValidityBitmap(const arrow::Array& array) {
    const auto& data = array.data();
    if (array.offset() < 0 || array.length() < 0 ||
        array.offset() >
            std::numeric_limits<int64_t>::max() - array.length()) {
        ThrowInfo(ErrorCode::DataFormatBroken,
                  "array validity range is invalid");
    }
    if (data->buffers.empty()) {
        ThrowInfo(ErrorCode::DataFormatBroken,
                  "array has no validity buffer slot");
    }
    const auto& bitmap = data->buffers[0];
    if (bitmap == nullptr) {
        if (data->null_count > 0) {
            ThrowInfo(ErrorCode::DataFormatBroken,
                      "array has nulls without a validity bitmap");
        }
        return;
    }
    const auto bits = static_cast<uint64_t>(array.offset()) +
                      static_cast<uint64_t>(array.length());
    ValidateArrowBufferElements(
        bitmap, 0, bits / 8 + (bits % 8 != 0), 1, "validity");
}

struct ColumnarArrayBuildNode {
    ArrayOffsets offsets;
    // Serialized with the existing Chunk/Arrow convention: one bit per row,
    // where 1 means valid and 0 means null.
    std::vector<uint8_t> validity_bitmap;
    std::vector<uint8_t> leaf_validity_bitmap;
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

DataType
GetColumnarArrayElementType(const proto::schema::TypeSchema& type) {
    const auto& element = type.array_element();
    return element.has_array_element() ? DataType::ARRAY
                                       : DataType(element.leaf_type());
}

size_t
GetLeafElementCount(const ScalarFieldProto& row, DataType data_type) {
    if (row.data_case() == ScalarFieldProto::DATA_NOT_SET) {
        return 0;
    }

    switch (data_type) {
        case DataType::BOOL: {
            AssertInfo(row.data_case() == ScalarFieldProto::kBoolData,
                       "expected bool array row, got proto case {}",
                       static_cast<int>(row.data_case()));
            return row.bool_data().data_size();
        }
        case DataType::INT8:
        case DataType::INT16:
        case DataType::INT32: {
            AssertInfo(row.data_case() == ScalarFieldProto::kIntData,
                       "expected int array row, got proto case {}",
                       static_cast<int>(row.data_case()));
            return row.int_data().data_size();
        }
        case DataType::INT64: {
            AssertInfo(row.data_case() == ScalarFieldProto::kLongData,
                       "expected long array row, got proto case {}",
                       static_cast<int>(row.data_case()));
            return row.long_data().data_size();
        }
        case DataType::FLOAT: {
            AssertInfo(row.data_case() == ScalarFieldProto::kFloatData,
                       "expected float array row, got proto case {}",
                       static_cast<int>(row.data_case()));
            return row.float_data().data_size();
        }
        case DataType::DOUBLE: {
            AssertInfo(row.data_case() == ScalarFieldProto::kDoubleData,
                       "expected double array row, got proto case {}",
                       static_cast<int>(row.data_case()));
            return row.double_data().data_size();
        }
        case DataType::STRING:
        case DataType::VARCHAR: {
            AssertInfo(row.data_case() == ScalarFieldProto::kStringData,
                       "expected string array row, got proto case {}",
                       static_cast<int>(row.data_case()));
            return row.string_data().data_size();
        }
        default:
            ThrowInfo(Unsupported,
                      "unsupported columnar array leaf type {}",
                      data_type);
    }
}

size_t
GetLeafElementCount(const ArrayValueView& row, DataType data_type) {
    const auto element_type = row.element_type();
    AssertInfo(element_type == data_type,
               "array view element type must be {}, got {}",
               data_type,
               element_type);
    return row.size();
}

template <typename Values>
size_t
CopyFixedValues(std::vector<char>& target,
                size_t element_offset,
                const Values& values) {
    using ValueType = typename Values::value_type;
    const auto bytes = static_cast<size_t>(values.size()) * sizeof(ValueType);
    if (bytes != 0) {
        std::memcpy(target.data() + element_offset * sizeof(ValueType),
                    values.data(),
                    bytes);
    }
    return element_offset + values.size();
}

size_t
CopyLeafRow(ColumnarArrayBuildNode& node,
            const ScalarFieldProto& row,
            DataType data_type,
            size_t element_offset) {
    if (row.data_case() == ScalarFieldProto::DATA_NOT_SET) {
        return element_offset;
    }

    switch (data_type) {
        case DataType::BOOL:
            for (auto value : row.bool_data().data()) {
                node.fixed_data[element_offset++] = value ? 1 : 0;
            }
            return element_offset;
        case DataType::INT8:
            for (auto value : row.int_data().data()) {
                node.fixed_data[element_offset++] = static_cast<int8_t>(value);
            }
            return element_offset;
        case DataType::INT16:
            for (auto value : row.int_data().data()) {
                const auto narrow = static_cast<int16_t>(value);
                std::memcpy(
                    node.fixed_data.data() + element_offset++ * sizeof(narrow),
                    &narrow,
                    sizeof(narrow));
            }
            return element_offset;
        case DataType::INT32:
            return CopyFixedValues(
                node.fixed_data, element_offset, row.int_data().data());
        case DataType::INT64:
            return CopyFixedValues(
                node.fixed_data, element_offset, row.long_data().data());
        case DataType::FLOAT:
            return CopyFixedValues(
                node.fixed_data, element_offset, row.float_data().data());
        case DataType::DOUBLE:
            return CopyFixedValues(
                node.fixed_data, element_offset, row.double_data().data());
        case DataType::STRING:
        case DataType::VARCHAR:
            for (const auto& value : row.string_data().data()) {
                node.string_data.append(value);
                node.string_offsets.push_back(
                    static_cast<uint32_t>(node.string_data.size()));
            }
            return element_offset + row.string_data().data_size();
        default:
            ThrowInfo(Unsupported,
                      "unsupported columnar array leaf type {}",
                      data_type);
    }
}

size_t
GetLeafFixedWidth(DataType data_type) {
    switch (data_type) {
        case DataType::BOOL:
            return sizeof(uint8_t);
        case DataType::INT8:
            return sizeof(int8_t);
        case DataType::INT16:
            return sizeof(int16_t);
        case DataType::INT32:
            return sizeof(int32_t);
        case DataType::INT64:
            return sizeof(int64_t);
        case DataType::FLOAT:
            return sizeof(float);
        case DataType::DOUBLE:
            return sizeof(double);
        default:
            ThrowInfo(Unsupported,
                      "columnar array leaf type {} is not fixed width",
                      data_type);
    }
}

size_t
CopyLeafRow(ColumnarArrayBuildNode& node,
            const ArrayValueView& row,
            DataType data_type,
            size_t element_offset) {
    const auto row_size = row.size();
    if (IsStringDataType(data_type)) {
        for (size_t i = 0; i < row_size; ++i) {
            const auto value = row.get_data_unchecked<std::string_view>(i);
            node.string_data.append(value);
            node.string_offsets.push_back(
                static_cast<uint32_t>(node.string_data.size()));
        }
        return element_offset + row_size;
    }

    const auto width = GetLeafFixedWidth(data_type);
    const auto bytes = row_size * width;
    if (bytes != 0) {
        const auto& chunk = static_cast<const FixedWidthChunk&>(row.child());
        std::memcpy(node.fixed_data.data() + element_offset * width,
                    chunk.ValueAt(static_cast<int64_t>(row.begin())),
                    bytes);
    }
    return element_offset + row_size;
}

void
FinalizeStringLeaf(ColumnarArrayBuildNode& node, bool nullable) {
    const auto offsets_bytes =
        node.string_offsets.size() * sizeof(uint32_t) +
        LeafPrefixSize(node.string_offsets.size() - 1, nullable);
    AssertInfo(offsets_bytes <=
                   static_cast<size_t>(std::numeric_limits<uint32_t>::max()),
               "columnar array string offsets exceed uint32 range");
    for (auto& offset : node.string_offsets) {
        AssertInfo(
            offset <= std::numeric_limits<uint32_t>::max() - offsets_bytes,
            "columnar array string offset exceeds uint32 range");
        offset += static_cast<uint32_t>(offsets_bytes);
    }
}

std::unique_ptr<ColumnarArrayBuildNode>
BuildNodeFromProtoRows(const std::vector<const ScalarFieldProto*>& rows,
                       const proto::schema::TypeSchema& type,
                       std::span<const uint8_t> row_validity = {}) {
    AssertInfo(row_validity.empty() || row_validity.size() == rows.size(),
               "array row validity length {} does not match {} rows",
               row_validity.size(),
               rows.size());
    auto node = std::make_unique<ColumnarArrayBuildNode>();
    if (type.nullable()) {
        node->validity_bitmap.resize((rows.size() + 7) / 8, 0);
    }
    node->offsets.reserve(rows.size() + 1);
    node->offsets.push_back(0);
    for (size_t i = 0; i < rows.size(); ++i) {
        AssertInfo(
            rows[i] != nullptr, "nested ARRAY row {} must not be null", i);
        const auto valid =
            row_validity.empty()
                ? rows[i]->data_case() != ScalarFieldProto::DATA_NOT_SET
                : row_validity[i] != 0;
        AssertInfo(type.nullable() || valid,
                   "non-nullable nested ARRAY node contains null row {}",
                   i);
        AssertInfo(
            !valid || rows[i]->data_case() != ScalarFieldProto::DATA_NOT_SET,
            "valid array row {} has no typed payload",
            i);
        if (!valid && !row_validity.empty()) {
            const auto element_type = GetColumnarArrayElementType(type);
            if (element_type == DataType::ARRAY) {
                AssertInfo(
                    rows[i]->data_case() == ScalarFieldProto::kArrayData &&
                        rows[i]->array_data().data_size() == 0 &&
                        rows[i]->array_data().element_type() ==
                            static_cast<proto::schema::DataType>(
                                GetColumnarArrayElementType(
                                    type.array_element())) &&
                        rows[i]->valid_data_size() == 0,
                    "null nested ARRAY row {} must have a typed empty "
                    "placeholder",
                    i);
            } else {
                AssertInfo(
                    rows[i]->data_case() != ScalarFieldProto::DATA_NOT_SET &&
                        GetLeafElementCount(*rows[i], element_type) == 0 &&
                        rows[i]->valid_data_size() == 0,
                    "null nested ARRAY row {} must have a typed empty "
                    "placeholder",
                    i);
            }
        }
        if (type.nullable() && valid) {
            node->validity_bitmap[i >> 3] |=
                static_cast<uint8_t>(1U << (i & 0x07));
        }
    }

    if (GetColumnarArrayElementType(type) == DataType::ARRAY) {
        const auto& child_type = type.array_element();
        const auto expected_element_type = static_cast<proto::schema::DataType>(
            GetColumnarArrayElementType(child_type));
        size_t child_count = 0;
        for (size_t row_index = 0; row_index < rows.size(); ++row_index) {
            const auto* row = rows[row_index];
            if ((row_validity.empty() &&
                 row->data_case() == ScalarFieldProto::DATA_NOT_SET) ||
                (!row_validity.empty() && !row_validity[row_index])) {
                node->offsets.push_back(child_count);
                continue;
            }

            AssertInfo(row->data_case() == ScalarFieldProto::kArrayData,
                       "expected nested array proto row, got case {}",
                       static_cast<int>(row->data_case()));
            const auto& array_data = row->array_data();
            if (array_data.element_type() != proto::schema::DataType::None) {
                AssertInfo(array_data.element_type() == expected_element_type,
                           "nested array proto element type must be {}, got {}",
                           expected_element_type,
                           array_data.element_type());
            }
            child_count += array_data.data_size();
            node->offsets.push_back(child_count);
        }

        std::vector<const ScalarFieldProto*> child_rows;
        std::vector<uint8_t> child_validity;
        child_rows.reserve(child_count);
        child_validity.reserve(child_count);
        for (size_t row_index = 0; row_index < rows.size(); ++row_index) {
            const auto* row = rows[row_index];
            if ((row_validity.empty() &&
                 row->data_case() == ScalarFieldProto::DATA_NOT_SET) ||
                (!row_validity.empty() && !row_validity[row_index])) {
                continue;
            }
            AssertInfo(
                child_type.nullable()
                    ? row->valid_data_size() == row->array_data().data_size()
                    : row->valid_data_size() == 0,
                "nested array row {} has invalid element validity length",
                row_index);
            int child_index = 0;
            for (const auto& child_row : row->array_data().data()) {
                AssertInfo(
                    child_type.nullable() ||
                        child_row.data_case() != ScalarFieldProto::DATA_NOT_SET,
                    "non-nullable nested ARRAY node contains null row");
                child_rows.push_back(&child_row);
                child_validity.push_back(
                    child_type.nullable() ? row->valid_data(child_index++) : 1);
            }
        }
        node->array_child =
            BuildNodeFromProtoRows(child_rows, child_type, child_validity);
        return node;
    }

    node->leaf_type = GetColumnarArrayElementType(type);
    const auto is_string_leaf = IsStringDataType(node->leaf_type);
    size_t child_count = 0;
    size_t string_data_size = 0;
    for (const auto* row : rows) {
        child_count += GetLeafElementCount(*row, node->leaf_type);
        node->offsets.push_back(child_count);
        if (is_string_leaf &&
            row->data_case() != ScalarFieldProto::DATA_NOT_SET) {
            for (const auto& value : row->string_data().data()) {
                AssertInfo(
                    value.size() <=
                        std::numeric_limits<uint32_t>::max() - string_data_size,
                    "columnar array string leaf exceeds uint32 offset range");
                string_data_size += value.size();
            }
        }
    }

    const bool leaf_nullable = type.array_element().nullable();
    if (leaf_nullable) {
        node->leaf_validity_bitmap.resize((child_count + 7) / 8, 0);
    }

    if (is_string_leaf) {
        node->string_offsets.reserve(child_count + 1);
        node->string_offsets.push_back(0);
        node->string_data.reserve(string_data_size);
    } else {
        const auto width = GetLeafFixedWidth(node->leaf_type);
        AssertInfo(child_count <= std::numeric_limits<size_t>::max() / width,
                   "columnar array fixed leaf size overflow: {} * {}",
                   child_count,
                   width);
        node->fixed_data.resize(child_count * width);
    }

    size_t element_offset = 0;
    for (size_t row_index = 0; row_index < rows.size(); ++row_index) {
        const auto* row = rows[row_index];
        const auto count = GetLeafElementCount(*row, node->leaf_type);
        AssertInfo(leaf_nullable ? row->valid_data_size() == count
                                 : row->valid_data_size() == 0,
                   "array row {} has invalid leaf validity length",
                   row_index);
        if (leaf_nullable) {
            for (size_t i = 0; i < count; ++i) {
                if (row->valid_data(static_cast<int>(i))) {
                    const auto index = element_offset + i;
                    node->leaf_validity_bitmap[index >> 3] |=
                        static_cast<uint8_t>(1U << (index & 7));
                }
            }
        }
        element_offset =
            CopyLeafRow(*node, *row, node->leaf_type, element_offset);
    }
    AssertInfo(element_offset == child_count,
               "columnar array leaf element count mismatch: copied {}, "
               "expected {}",
               element_offset,
               child_count);

    if (is_string_leaf) {
        FinalizeStringLeaf(*node, leaf_nullable);
    }

    return node;
}

std::unique_ptr<ColumnarArrayBuildNode>
BuildNodeFromViews(const std::vector<ArrayValueView>& rows,
                   std::span<const uint8_t> valid_data,
                   const proto::schema::TypeSchema& type) {
    auto node = std::make_unique<ColumnarArrayBuildNode>();
    if (type.nullable()) {
        node->validity_bitmap.resize((rows.size() + 7) / 8, 0);
    }
    node->offsets.reserve(rows.size() + 1);
    node->offsets.push_back(0);
    for (size_t i = 0; i < rows.size(); ++i) {
        const auto valid = valid_data.empty() || valid_data[i] != 0;
        AssertInfo(type.nullable() || valid,
                   "non-nullable nested ARRAY node contains null row {}",
                   i);
        if (type.nullable() && valid) {
            node->validity_bitmap[i >> 3] |=
                static_cast<uint8_t>(1U << (i & 0x07));
        }
    }

    if (GetColumnarArrayElementType(type) == DataType::ARRAY) {
        const auto& child_type = type.array_element();
        size_t child_count = 0;
        for (size_t row_index = 0; row_index < rows.size(); ++row_index) {
            if (valid_data.empty() || valid_data[row_index] != 0) {
                child_count += rows[row_index].size();
            }
            node->offsets.push_back(child_count);
        }

        std::vector<ArrayValueView> child_rows;
        std::vector<uint8_t> child_valid_data;
        child_rows.reserve(child_count);
        child_valid_data.reserve(child_count);
        for (size_t row_index = 0; row_index < rows.size(); ++row_index) {
            if (!valid_data.empty() && valid_data[row_index] == 0) {
                continue;
            }

            const auto& row = rows[row_index];
            const auto row_size = row.size();
            for (size_t i = 0; i < row_size; ++i) {
                auto child = row.array_at(i);
                child_valid_data.push_back(!child.is_null());
                child_rows.push_back(std::move(child));
            }
        }
        node->array_child = BuildNodeFromViews(
            child_rows,
            std::span<const uint8_t>(child_valid_data.data(),
                                     child_valid_data.size()),
            child_type);
        return node;
    }

    node->leaf_type = GetColumnarArrayElementType(type);
    const auto is_string_leaf = IsStringDataType(node->leaf_type);
    size_t child_count = 0;
    size_t string_data_size = 0;
    for (size_t row_index = 0; row_index < rows.size(); ++row_index) {
        if (valid_data.empty() || valid_data[row_index] != 0) {
            const auto& row = rows[row_index];
            child_count += GetLeafElementCount(row, node->leaf_type);
            if (is_string_leaf) {
                for (size_t i = 0; i < row.size(); ++i) {
                    const auto value =
                        row.get_data_unchecked<std::string_view>(i);
                    AssertInfo(
                        value.size() <= std::numeric_limits<uint32_t>::max() -
                                            string_data_size,
                        "columnar array string leaf exceeds uint32 offset "
                        "range");
                    string_data_size += value.size();
                }
            }
        }
        node->offsets.push_back(child_count);
    }

    if (is_string_leaf) {
        node->string_offsets.reserve(child_count + 1);
        node->string_offsets.push_back(0);
        node->string_data.reserve(string_data_size);
    } else {
        const auto width = GetLeafFixedWidth(node->leaf_type);
        AssertInfo(child_count <= std::numeric_limits<size_t>::max() / width,
                   "columnar array fixed leaf size overflow: {} * {}",
                   child_count,
                   width);
        node->fixed_data.resize(child_count * width);
    }

    if (type.array_element().nullable()) {
        node->leaf_validity_bitmap.resize((child_count + 7) / 8, 0);
    }

    size_t element_offset = 0;
    for (size_t row_index = 0; row_index < rows.size(); ++row_index) {
        if (valid_data.empty() || valid_data[row_index] != 0) {
            if (type.array_element().nullable()) {
                for (size_t i = 0; i < rows[row_index].size(); ++i) {
                    if (rows[row_index].is_valid(i)) {
                        const auto index = element_offset + i;
                        node->leaf_validity_bitmap[index >> 3] |=
                            static_cast<uint8_t>(1U << (index & 7));
                    }
                }
            }
            element_offset = CopyLeafRow(
                *node, rows[row_index], node->leaf_type, element_offset);
        }
    }
    AssertInfo(element_offset == child_count,
               "columnar array leaf element count mismatch: copied {}, "
               "expected {}",
               element_offset,
               child_count);

    if (is_string_leaf) {
        FinalizeStringLeaf(*node, type.array_element().nullable());
    }

    return node;
}

std::unique_ptr<ColumnarArrayBuildNode>
BuildNodeFromArrow(const arrow::ListArray& rows,
                   const proto::schema::TypeSchema& type) {
    auto node = std::make_unique<ColumnarArrayBuildNode>();
    ValidateArrowValidityBitmap(rows);
    const auto row_count = static_cast<size_t>(rows.length());
    if (rows.data()->buffers.size() < 2) {
        ThrowInfo(ErrorCode::DataFormatBroken,
                  "array list has no offsets buffer");
    }
    ValidateArrowBufferElements(rows.data()->buffers[1],
                                static_cast<uint64_t>(rows.offset()),
                                static_cast<uint64_t>(row_count) + 1,
                                sizeof(int32_t),
                                "list offsets");
    const auto& child = rows.values();
    if (child == nullptr || child->length() < 0) {
        ThrowInfo(ErrorCode::DataFormatBroken,
                  "array list has no valid child values");
    }
    if (type.nullable()) {
        node->validity_bitmap.resize((row_count + 7) / 8, 0);
    }
    const auto base = rows.value_offset(0);
    if (base < 0 || base > child->length()) {
        ThrowInfo(ErrorCode::DataFormatBroken,
                  "array list starting offset exceeds child values");
    }
    int64_t previous = base;
    node->offsets.reserve(row_count + 1);
    for (size_t i = 0; i <= row_count; ++i) {
        const int64_t current = rows.value_offset(static_cast<int64_t>(i));
        if (current < previous || current > child->length()) {
            ThrowInfo(ErrorCode::DataFormatBroken,
                      "array list offset {} is outside child values",
                      i);
        }
        const auto offset = current - base;
        node->offsets.push_back(static_cast<ArrayOffset>(offset));
        previous = current;
        if (i == row_count) {
            continue;
        }
        const bool valid = !rows.IsNull(static_cast<int64_t>(i));
        if (!type.nullable() && !valid) {
            ThrowInfo(ErrorCode::DataFormatBroken,
                      "non-nullable array contains null row {}",
                      i);
        }
        const int64_t next = rows.value_offset(static_cast<int64_t>(i + 1));
        if (!valid && next != current) {
            ThrowInfo(ErrorCode::DataFormatBroken,
                      "null array row {} has a non-empty child range",
                      i);
        }
        if (type.nullable() && valid) {
            node->validity_bitmap[i >> 3] |=
                static_cast<uint8_t>(1U << (i & 7));
        }
    }

    auto values = child->Slice(base, previous - base);
    const auto element_type = GetColumnarArrayElementType(type);
    if (element_type == DataType::ARRAY) {
        auto nested = std::dynamic_pointer_cast<arrow::ListArray>(values);
        if (nested == nullptr) {
            ThrowInfo(ErrorCode::DataFormatBroken,
                      "nested ARRAY requires Arrow list children, got {}",
                      values->type()->ToString());
        }
        node->array_child = BuildNodeFromArrow(*nested, type.array_element());
        return node;
    }

    node->leaf_type = element_type;
    if (!values->type()->Equals(GetArrowDataType(element_type))) {
        ThrowInfo(ErrorCode::DataFormatBroken,
                  "array leaf type {} does not match schema {}",
                  values->type()->ToString(),
                  element_type);
    }
    ValidateArrowValidityBitmap(*values);
    const auto child_count = static_cast<size_t>(values->length());
    const bool leaf_nullable = type.array_element().nullable();
    if (leaf_nullable) {
        node->leaf_validity_bitmap.resize((child_count + 7) / 8, 0);
    }
    for (size_t i = 0; i < child_count; ++i) {
        const bool valid = !values->IsNull(static_cast<int64_t>(i));
        if (!leaf_nullable && !valid) {
            ThrowInfo(ErrorCode::DataFormatBroken,
                      "non-nullable array leaf contains null element {}",
                      i);
        }
        if (leaf_nullable && valid) {
            node->leaf_validity_bitmap[i >> 3] |=
                static_cast<uint8_t>(1U << (i & 7));
        }
    }
    if (IsStringDataType(element_type)) {
        const auto& strings = static_cast<const arrow::StringArray&>(*values);
        if (strings.data()->buffers.size() < 3) {
            ThrowInfo(ErrorCode::DataFormatBroken,
                      "array string leaf has incomplete buffers");
        }
        ValidateArrowBufferElements(strings.data()->buffers[1],
                                    static_cast<uint64_t>(strings.offset()),
                                    static_cast<uint64_t>(child_count) + 1,
                                    sizeof(int32_t),
                                    "string offsets");
        const auto& string_buffer = strings.data()->buffers[2];
        int64_t previous_string_offset = 0;
        for (size_t i = 0; i <= child_count; ++i) {
            const int64_t string_offset =
                strings.value_offset(static_cast<int64_t>(i));
            if (string_offset < previous_string_offset ||
                (string_buffer == nullptr ? string_offset != 0
                                          : string_offset >
                                                string_buffer->size())) {
                ThrowInfo(ErrorCode::DataFormatBroken,
                          "array string offset {} exceeds data buffer",
                          i);
            }
            previous_string_offset = string_offset;
        }
        node->string_offsets.reserve(child_count + 1);
        node->string_offsets.push_back(0);
        for (size_t i = 0; i < child_count; ++i) {
            if (!strings.IsNull(static_cast<int64_t>(i)) &&
                strings.value_offset(static_cast<int64_t>(i + 1)) >
                    strings.value_offset(static_cast<int64_t>(i))) {
                node->string_data.append(
                    strings.GetView(static_cast<int64_t>(i)));
            }
            if (node->string_data.size() >
                std::numeric_limits<uint32_t>::max()) {
                ThrowInfo(ErrorCode::DataFormatBroken,
                          "array string leaf exceeds uint32 offset range");
            }
            node->string_offsets.push_back(
                static_cast<uint32_t>(node->string_data.size()));
        }
        FinalizeStringLeaf(*node, leaf_nullable);
    } else if (element_type == DataType::BOOL) {
        const auto& booleans = static_cast<const arrow::BooleanArray&>(*values);
        if (booleans.data()->buffers.size() < 2) {
            ThrowInfo(ErrorCode::DataFormatBroken,
                      "array bool leaf has no values buffer");
        }
        const auto bits = static_cast<uint64_t>(booleans.offset()) +
                          static_cast<uint64_t>(child_count);
        ValidateArrowBufferElements(booleans.data()->buffers[1],
                                    0,
                                    bits / 8 + (bits % 8 != 0),
                                    1,
                                    "bool values");
        node->fixed_data.resize(child_count);
        for (size_t i = 0; i < child_count; ++i) {
            node->fixed_data[i] = !booleans.IsNull(static_cast<int64_t>(i)) &&
                                  booleans.Value(static_cast<int64_t>(i));
        }
    } else {
        const auto width = GetLeafFixedWidth(element_type);
        if (child_count > std::numeric_limits<size_t>::max() / width) {
            ThrowInfo(ErrorCode::DataFormatBroken,
                      "array fixed leaf size overflow");
        }
        const auto bytes = child_count * width;
        if (bytes != 0) {
            if (values->data()->buffers.size() < 2) {
                ThrowInfo(ErrorCode::DataFormatBroken,
                          "array fixed leaf has no values buffer");
            }
            const auto& buffer = values->data()->buffers[1];
            ValidateArrowBufferElements(
                buffer,
                static_cast<uint64_t>(values->offset()),
                static_cast<uint64_t>(child_count),
                width,
                "fixed leaf values");
            node->fixed_data.resize(bytes);
            std::memcpy(node->fixed_data.data(),
                        buffer->data() + values->offset() * width,
                        bytes);
        }
    }
    return node;
}

std::unique_ptr<ColumnarArrayBuildNode>
BuildColumnarArrayNode(const std::vector<const ScalarFieldProto*>& rows,
                       const proto::schema::TypeSchema& type) {
    ColumnarArrayChunk::ValidateArrayType(type);
    return BuildNodeFromProtoRows(rows, type);
}

std::unique_ptr<ColumnarArrayBuildNode>
BuildColumnarArrayNode(std::span<const ArrayValue> values,
                       std::span<const uint8_t> valid_data,
                       const proto::schema::TypeSchema& type) {
    ColumnarArrayChunk::ValidateArrayType(type);
    AssertInfo(valid_data.empty() || valid_data.size() == values.size(),
               "nested ARRAY valid data size {} must match row count {}",
               valid_data.size(),
               values.size());

    std::vector<ArrayValueView> rows(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        const auto valid = valid_data.empty() || valid_data[i] != 0;
        if (valid) {
            AssertInfo(!values[i].is_null(),
                       "valid nested ARRAY row {} has no payload",
                       i);
            rows[i] = values[i].View();
        }
    }
    return BuildNodeFromViews(rows, valid_data, type);
}

size_t
ColumnarArrayNodeByteSize(const ColumnarArrayBuildNode& node,
                          const proto::schema::TypeSchema& type);

void
WriteColumnarArrayNode(const ColumnarArrayBuildNode& node,
                       const proto::schema::TypeSchema& type,
                       const std::shared_ptr<ChunkTarget>& target);

void
WriteAlignmentPadding(int64_t row_count,
                      bool nullable,
                      const std::shared_ptr<ChunkTarget>& target);

size_t
ColumnarArrayChildByteSize(const ColumnarArrayBuildNode& node,
                           const proto::schema::TypeSchema& type) {
    if (node.array_child != nullptr) {
        return ColumnarArrayNodeByteSize(*node.array_child,
                                         type.array_element());
    }
    const auto leaf_rows = node.offsets.back();
    const auto prefix =
        LeafPrefixSize(leaf_rows, type.array_element().nullable());
    return prefix + (IsStringDataType(node.leaf_type)
                         ? node.string_offsets.size() * sizeof(uint32_t) +
                               node.string_data.size()
                         : node.fixed_data.size());
}

size_t
ColumnarArrayNodeByteSize(const ColumnarArrayBuildNode& node,
                          const proto::schema::TypeSchema& type) {
    const auto row_count = node.offsets.size() - 1;
    const auto prefix_size = ColumnarArrayChunk::NodeDataOffset(
        static_cast<int64_t>(row_count), type.nullable());
    return prefix_size + node.offsets.size() * sizeof(ArrayOffset) +
           ColumnarArrayChildByteSize(node, type);
}

void
WriteColumnarArrayChild(const ColumnarArrayBuildNode& node,
                        const proto::schema::TypeSchema& type,
                        const std::shared_ptr<ChunkTarget>& target) {
    if (node.array_child != nullptr) {
        WriteColumnarArrayNode(*node.array_child, type.array_element(), target);
        return;
    }
    if (type.array_element().nullable()) {
        if (!node.leaf_validity_bitmap.empty()) {
            target->write(node.leaf_validity_bitmap.data(),
                          node.leaf_validity_bitmap.size());
        }
        const auto padding = LeafPrefixSize(node.offsets.back(), true) -
                             node.leaf_validity_bitmap.size();
        std::array<char, alignof(uint64_t)> zeros{};
        target->write(zeros.data(), padding);
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
                       const proto::schema::TypeSchema& type,
                       const std::shared_ptr<ChunkTarget>& target) {
    // Serialize one Array node as:
    //   [validity bitmap, when nullable][alignment padding, when needed]
    //   [offsets: row_count + 1][recursively serialized child]
    const auto row_count = node.offsets.size() - 1;
    if (type.nullable() && !node.validity_bitmap.empty()) {
        target->write(node.validity_bitmap.data(), node.validity_bitmap.size());
    }
    WriteAlignmentPadding(
        static_cast<int64_t>(row_count), type.nullable(), target);
    target->write(node.offsets.data(),
                  node.offsets.size() * sizeof(ArrayOffset));
    WriteColumnarArrayChild(node, type, target);
}

size_t
ColumnarArraySerializedByteSize(const ColumnarArrayBuildNode& root,
                                const proto::schema::TypeSchema& type) {
    return ColumnarArrayNodeByteSize(root, type) + MMAP_ARRAY_PADDING;
}

void
WriteAlignmentPadding(int64_t row_count,
                      bool nullable,
                      const std::shared_ptr<ChunkTarget>& target) {
    const auto null_bitmap_bytes =
        nullable ? (static_cast<size_t>(row_count) + 7) / 8 : 0;
    const auto root_data_offset =
        ColumnarArrayChunk::NodeDataOffset(row_count, nullable);
    const auto alignment_bytes = root_data_offset - null_bitmap_bytes;
    if (alignment_bytes != 0) {
        std::array<char, alignof(ArrayOffset)> zeros{};
        target->write(zeros.data(), alignment_bytes);
    }
}

std::shared_ptr<const ColumnarArrayChunk>
MaterializeMmapColumnarArrayChunk(
    const ColumnarArrayBuildNode& root,
    int64_t row_count,
    const proto::schema::TypeSchema& type,
    const storage::MmapChunkDescriptorPtr& mmap_descriptor) {
    const auto serialized_size = ColumnarArraySerializedByteSize(root, type);

    auto mmap_manager =
        storage::MmapManager::GetInstance().GetMmapChunkManager();
    auto* data = static_cast<char*>(
        mmap_manager->Allocate(mmap_descriptor, serialized_size));
    AssertInfo(data != nullptr,
               "failed to allocate {} bytes for nested ARRAY mmap block",
               serialized_size);

    auto target =
        std::make_shared<BorrowedArrayChunkTarget>(data, serialized_size);
    WriteColumnarArrayNode(root, type, target);
    char padding[MMAP_ARRAY_PADDING] = {};
    target->write(padding, MMAP_ARRAY_PADDING);

    return std::make_shared<const ColumnarArrayChunk>(
        row_count,
        data,
        serialized_size,
        std::make_shared<const proto::schema::TypeSchema>(type),
        nullptr);
}

}  // namespace

std::shared_ptr<const ColumnarArrayChunk>
CreateMmapColumnarArrayChunkFromProtoRows(
    std::span<const ScalarFieldProto* const> rows,
    const proto::schema::TypeSchema& type,
    const storage::MmapChunkDescriptorPtr& mmap_descriptor) {
    AssertInfo(
        rows.size() <= static_cast<size_t>(std::numeric_limits<int64_t>::max()),
        "nested ARRAY row count {} exceeds int64 range",
        rows.size());

    std::vector<const ScalarFieldProto*> row_ptrs(rows.begin(), rows.end());
    auto root = BuildColumnarArrayNode(row_ptrs, type);
    const auto row_count = static_cast<int64_t>(rows.size());
    return MaterializeMmapColumnarArrayChunk(
        *root, row_count, type, mmap_descriptor);
}

std::shared_ptr<const ColumnarArrayChunk>
CreateMmapColumnarArrayChunkFromValues(
    std::span<const ArrayValue> values,
    std::span<const uint8_t> valid_data,
    const storage::MmapChunkDescriptorPtr& mmap_descriptor) {
    AssertInfo(!values.empty(),
               "cannot build nested ARRAY mmap block from no rows");
    AssertInfo(values.size() <=
                   static_cast<size_t>(std::numeric_limits<int64_t>::max()),
               "nested ARRAY row count {} exceeds int64 range",
               values.size());

    const auto& type = values.front().type();
    auto root = BuildColumnarArrayNode(values, valid_data, type);
    const auto row_count = static_cast<int64_t>(values.size());
    return MaterializeMmapColumnarArrayChunk(
        *root, row_count, type, mmap_descriptor);
}

struct ColumnarArrayChunkWriter::Impl {
    std::unique_ptr<ColumnarArrayBuildNode> root;
    size_t serialized_size{0};
};

ColumnarArrayChunkWriter::ColumnarArrayChunkWriter(
    proto::schema::TypeSchema type)
    : ChunkWriterBase(type.nullable()),
      type_(std::move(type)),
      impl_(std::make_unique<Impl>()) {
    ColumnarArrayChunk::ValidateArrayType(type_);
}

ColumnarArrayChunkWriter::~ColumnarArrayChunkWriter() = default;

std::pair<size_t, size_t>
ColumnarArrayChunkWriter::calculate_size(const arrow::ArrayVector& array_vec) {
    row_nums_ = 0;
    for (const auto& data : array_vec) {
        row_nums_ += data->length();
    }
    AssertInfo(
        row_nums_ <= static_cast<size_t>(std::numeric_limits<int64_t>::max()),
        "columnar array row count {} exceeds int64 range",
        row_nums_);

    arrow::ArrayVector lists;
    lists.reserve(array_vec.size());
    for (const auto& data : array_vec) {
        auto canonical = storage::CanonicalizeArrowVariants(data);
        auto array = std::dynamic_pointer_cast<arrow::ListArray>(canonical);
        AssertInfo(array != nullptr,
                   "ColumnarArrayChunkWriter expects Arrow ListArray, got "
                   "type id {}",
                   data ? static_cast<int>(data->type_id()) : -1);
        lists.push_back(std::move(array));
    }
    AssertInfo(!lists.empty(), "ColumnarArrayChunkWriter requires an array");
    std::shared_ptr<arrow::Array> combined = lists.front();
    if (lists.size() > 1) {
        auto result = arrow::Concatenate(lists);
        if (!result.ok()) {
            ThrowInfo(storage::ArrowStatusToErrorCode(result),
                      "failed to concatenate array chunks: {}",
                      result.status().ToString());
        }
        combined = *result;
    }
    impl_->root = BuildNodeFromArrow(
        static_cast<const arrow::ListArray&>(*combined), type_);
    impl_->serialized_size =
        ColumnarArraySerializedByteSize(*impl_->root, type_);
    return {impl_->serialized_size, row_nums_};
}

void
ColumnarArrayChunkWriter::write_to_target(
    const arrow::ArrayVector& array_vec,
    const std::shared_ptr<ChunkTarget>& target) {
    WriteColumnarArrayNode(*impl_->root, type_, target);
    char padding[MMAP_ARRAY_PADDING] = {};
    target->write(padding, MMAP_ARRAY_PADDING);

    impl_->root.reset();
}

std::shared_ptr<const ArrayValueStorage>
CreateArrayValueStorageFromProto(
    const ScalarFieldProto& row,
    std::shared_ptr<const proto::schema::TypeSchema> type) {
    AssertInfo(type != nullptr, "ArrayValue type must not be null");
    auto root = BuildColumnarArrayNode({&row}, *type);

    auto storage = std::make_shared<ArrayValueStorage>();
    storage->type = std::move(type);
    storage->length = root->offsets.back();
    storage->is_null = row.data_case() == ScalarFieldProto::DATA_NOT_SET;

    const auto child_size = ColumnarArrayChildByteSize(*root, *storage->type);
    // Reserve the same block-level trailing padding as a complete column
    // buffer, but exclude it from the child Chunk's logical size below.
    const auto storage_size = child_size + MMAP_ARRAY_PADDING;
    storage->buffer.resize(storage_size);
    auto target = std::make_shared<BorrowedArrayChunkTarget>(
        storage->buffer.data(), storage->buffer.size());
    WriteColumnarArrayChild(*root, *storage->type, target);
    char padding[MMAP_ARRAY_PADDING] = {};
    target->write(padding, MMAP_ARRAY_PADDING);

    auto* data = target->release();
    storage->child = array_detail::CreateColumnarArrayChildChunk(
        storage->type, storage->length, data, child_size, nullptr);
    return storage;
}

std::vector<ScalarFieldProto>
ArrowListToScalarFieldProto(const arrow::ListArray& rows,
                            const proto::schema::TypeSchema& type) {
    ColumnarArrayChunk::ValidateArrayType(type);
    auto root = BuildNodeFromArrow(rows, type);
    const auto size = ColumnarArraySerializedByteSize(*root, type);
    std::vector<char> buffer(size);
    auto target = std::make_shared<BorrowedArrayChunkTarget>(
        buffer.data(), buffer.size());
    WriteColumnarArrayNode(*root, type, target);
    char padding[MMAP_ARRAY_PADDING] = {};
    target->write(padding, MMAP_ARRAY_PADDING);
    auto array_type =
        std::make_shared<const proto::schema::TypeSchema>(type);
    ColumnarArrayChunk chunk(rows.length(), buffer.data(), size, array_type);
    std::vector<ScalarFieldProto> result;
    result.reserve(rows.length());
    for (int64_t row = 0; row < rows.length(); ++row) {
        result.push_back(chunk.View(row).output_data());
    }
    return result;
}

}  // namespace milvus
