// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/Chunk.h"
#include "common/EasyAssert.h"
#include "common/Types.h"
#include "pb/schema.pb.h"

namespace milvus {

using ArrayOffset = uint64_t;
using ArrayOffsets = std::vector<ArrayOffset>;

class ArrayValue;
class ArrayValueView;

namespace array_detail {

std::shared_ptr<const Chunk>
CreateColumnarArrayChild(
    const std::shared_ptr<const proto::schema::TypeSchema>& array_type,
    ArrayOffset row_count,
    char* data,
    uint64_t size,
    const std::shared_ptr<ChunkMmapGuard>& chunk_mmap_guard);

}  // namespace array_detail

// A read-only view over one recursively serialized Array column chunk.
//
// Root layout:
//   [validity bitmap, when nullable][alignment padding]
//   [offsets: row_count + 1][child node][MMAP_ARRAY_PADDING]
//
// Nested Array node layout:
//   [validity bitmap][alignment padding][offsets: row_count + 1][child node]
//
// Bitmap bits follow the Chunk convention: 1 means valid and 0 means null.
// Null and empty Arrays have the same repeated offset; the bitmap distinguishes
// them. The terminal array offset is the child row count. The child node
// describes its own physical representation: another Array node, a
// fixed-width payload, or a StringChunk-compatible [byte offsets][chars]
// payload.
class ColumnarArrayChunk final : public Chunk {
    friend class ArrayValue;
    friend class ArrayValueView;
    friend std::shared_ptr<const Chunk>
    array_detail::CreateColumnarArrayChild(
        const std::shared_ptr<const proto::schema::TypeSchema>& array_type,
        ArrayOffset row_count,
        char* data,
        uint64_t size,
        const std::shared_ptr<ChunkMmapGuard>& chunk_mmap_guard);

 public:
    using Ptr = std::shared_ptr<const ColumnarArrayChunk>;

    ColumnarArrayChunk(
        int64_t row_nums,
        char* data,
        uint64_t size,
        proto::schema::TypeSchema type,
        bool nullable,
        std::shared_ptr<ChunkMmapGuard> chunk_mmap_guard = nullptr)
        : ColumnarArrayChunk(
              row_nums,
              data,
              size,
              std::make_shared<const proto::schema::TypeSchema>(
                  std::move(type)),
              nullable,
              std::move(chunk_mmap_guard)) {
    }

    ColumnarArrayChunk(
        int64_t row_nums,
        char* data,
        uint64_t size,
        std::shared_ptr<const proto::schema::TypeSchema> type,
        bool nullable,
        std::shared_ptr<ChunkMmapGuard> chunk_mmap_guard = nullptr)
        : Chunk(row_nums, data, size, nullable, std::move(chunk_mmap_guard)),
          type_(std::move(type)) {
        AssertInfo(type_ != nullptr,
                   "ColumnarArrayChunk type must not be null");
        ValidateArrayType(*type_);
        const auto root_data_offset = RootDataOffset(row_nums, nullable);
        AssertInfo(size >= root_data_offset + MMAP_ARRAY_PADDING,
                   "columnar array chunk size {} is too small for root "
                   "offset {} and padding {}",
                   size,
                   root_data_offset,
                   MMAP_ARRAY_PADDING);
        InitializeNode(data_ + root_data_offset,
                       size - root_data_offset - MMAP_ARRAY_PADDING);
    }

    size_t
    row_count() const {
        return static_cast<size_t>(RowNums());
    }

    size_t
    byte_size() const {
        return static_cast<size_t>(Size());
    }

    bool
    is_valid(size_t row) const {
        AssertInfo(row < row_count(),
                   "ColumnarArrayChunk row {} out of range {}",
                   row,
                   row_count());
        return isValid(static_cast<int>(row));
    }

    // ViewType is intentionally dependent because ArrayValueView is only
    // completed after this header has been parsed.
    template <typename ViewType = ArrayValueView>
    ViewType
    View(size_t row) const {
        AssertInfo(row < row_count(),
                   "ColumnarArrayChunk row {} out of range {}",
                   row,
                   row_count());
        return ViewType::FromValidated(type_.get(),
                                       child_.get(),
                                       offsets_[row],
                                       offsets_[row + 1],
                                       !isValid(static_cast<int>(row)));
    }

    void
    output_data(size_t row, ScalarFieldProto& output) const {
        AssertInfo(row < row_count(),
                   "ColumnarArrayChunk row {} out of range {}",
                   row,
                   row_count());
        if (!isValid(static_cast<int>(row))) {
            output.Clear();
            return;
        }
        OutputRange(
            *type_, *child_, offsets_[row], offsets_[row + 1], output);
    }

    ScalarFieldProto
    output_data(size_t row) const {
        ScalarFieldProto output;
        output_data(row, output);
        return output;
    }

    const char*
    ValueAt(int64_t idx) const override {
        ThrowInfo(ErrorCode::Unsupported,
                  "ColumnarArrayChunk::ValueAt is not supported, index {}",
                  idx);
    }

    const proto::schema::TypeSchema&
    type() const {
        return *type_;
    }

    std::span<const ArrayOffset>
    offsets() const {
        return offsets_;
    }

    const Chunk&
    child() const {
        return *child_;
    }

    static size_t
    RootDataOffset(int64_t row_nums, bool nullable) {
        AssertInfo(row_nums >= 0, "array row count must not be negative");
        if (!nullable) {
            return 0;
        }
        const auto rows = static_cast<size_t>(row_nums);
        const auto null_bitmap_bytes = rows / 8 + (rows % 8 != 0);
        return AlignUp(null_bitmap_bytes, alignof(ArrayOffset));
    }

 private:
    struct NestedNodeTag {};

    ColumnarArrayChunk(NestedNodeTag,
                       int64_t row_nums,
                       char* data,
                       uint64_t size,
                       std::shared_ptr<const proto::schema::TypeSchema> type,
                       std::shared_ptr<ChunkMmapGuard> chunk_mmap_guard)
        : Chunk(row_nums, data, size, true, std::move(chunk_mmap_guard)),
          type_(std::move(type)) {
        AssertInfo(type_ != nullptr,
                   "nested ColumnarArrayChunk type must not be null");
        const auto node_data_offset = RootDataOffset(row_nums, true);
        AssertInfo(size >= node_data_offset,
                   "nested columnar array chunk size {} is too small for "
                   "node offset {}",
                   size,
                   node_data_offset);
        InitializeNode(data_ + node_data_offset, size_ - node_data_offset);
    }

    template <typename>
    inline static constexpr bool AlwaysFalse = false;

    static size_t
    AlignUp(size_t value, size_t alignment) {
        return (value + alignment - 1) & ~(alignment - 1);
    }

    static bool
    IsSupportedLeafType(DataType data_type) {
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

    static size_t
    ExpectedFixedWidth(DataType data_type) {
        switch (data_type) {
            case DataType::BOOL:
                return sizeof(uint8_t);
            case DataType::INT8:
            case DataType::INT16:
            case DataType::INT32:
                // ScalarField stores small integer Array values as int32.
                return sizeof(int32_t);
            case DataType::INT64:
                return sizeof(int64_t);
            case DataType::FLOAT:
                return sizeof(float);
            case DataType::DOUBLE:
                return sizeof(double);
            default:
                ThrowInfo(Unsupported,
                          "ArrayValue leaf type {} is not fixed width",
                          data_type);
        }
    }

    static DataType
    GetElementType(const proto::schema::TypeSchema& type) {
        return type.has_element_schema() ? DataType::ARRAY
                                         : DataType(type.element_type());
    }

 public:
    static void
    ValidateArrayType(const proto::schema::TypeSchema& type) {
        AssertInfo(DataType(type.data_type()) == DataType::ARRAY,
                   "ArrayValue type must be ARRAY, got {}",
                   type.data_type());

        if (type.has_element_schema()) {
            AssertInfo(type.element_type() == proto::schema::DataType::None,
                       "nested ArrayValue type must not set element_type");
            AssertInfo(
                DataType(type.element_schema().data_type()) == DataType::ARRAY,
                "nested ArrayValue element_schema must be ARRAY, got {}",
                type.element_schema().data_type());
            ValidateArrayType(type.element_schema());
            return;
        }

        const auto element_type = DataType(type.element_type());
        AssertInfo(element_type != DataType::NONE,
                   "leaf ArrayValue type must set element_type");
        AssertInfo(element_type != DataType::ARRAY,
                   "nested ArrayValue must use element_schema");
        AssertInfo(IsSupportedLeafType(element_type),
                   "unsupported ArrayValue leaf type {}",
                   element_type);
    }

 private:
    static int32_t
    CheckedLeafRowCount(ArrayOffset row_count) {
        AssertInfo(row_count <= static_cast<ArrayOffset>(
                                    std::numeric_limits<int32_t>::max()),
                   "array leaf row count {} exceeds int32 range",
                   row_count);
        return static_cast<int32_t>(row_count);
    }

    void
    ValidateOffsets() const {
        AssertInfo(offsets_.front() == 0,
                   "ColumnarArrayChunk offsets must start at zero, got {}",
                   offsets_.front());
        AssertInfo(std::is_sorted(offsets_.begin(), offsets_.end()),
                   "ColumnarArrayChunk offsets must be monotonic");
        AssertInfo(offsets_.back() <= static_cast<ArrayOffset>(
                                          std::numeric_limits<int64_t>::max()),
                   "ColumnarArrayChunk child row count {} exceeds int64 range",
                   offsets_.back());
        for (size_t row = 0; row < row_count(); ++row) {
            AssertInfo(isValid(static_cast<int>(row)) ||
                           offsets_[row] == offsets_[row + 1],
                       "null array row {} must have an empty offset range",
                       row);
        }
    }

    static void
    ValidateStringPayload(const char* data,
                          uint64_t size,
                          ArrayOffset row_count) {
        const auto rows = CheckedLeafRowCount(row_count);
        const auto offset_count = static_cast<size_t>(rows) + 1;
        const auto offsets_bytes = offset_count * sizeof(uint32_t);
        AssertInfo(size >= offsets_bytes,
                   "string leaf size {} is smaller than offsets size {}",
                   size,
                   offsets_bytes);
        AssertInfo(size <= std::numeric_limits<uint32_t>::max(),
                   "string leaf size {} exceeds uint32 offset range",
                   size);

        const auto* offsets = reinterpret_cast<const uint32_t*>(data);
        AssertInfo(offsets[0] == offsets_bytes,
                   "string leaf first offset {} does not match header size {}",
                   offsets[0],
                   offsets_bytes);
        AssertInfo(std::is_sorted(offsets, offsets + offset_count),
                   "string leaf offsets must be monotonic");
        AssertInfo(offsets[offset_count - 1] == size,
                   "string leaf terminal offset {} does not match size {}",
                   offsets[offset_count - 1],
                   size);
    }

    void
    InitializeNode(char* node_data, uint64_t node_size) {
        AssertInfo(node_data != nullptr,
                   "ColumnarArrayChunk data must not be null");
        AssertInfo(
            reinterpret_cast<uintptr_t>(node_data) % alignof(ArrayOffset) == 0,
            "ColumnarArrayChunk offsets are not {}-byte aligned",
            alignof(ArrayOffset));

        const auto rows = static_cast<size_t>(row_nums_);
        const auto offset_count = rows + 1;
        AssertInfo(offset_count <=
                       std::numeric_limits<size_t>::max() / sizeof(ArrayOffset),
                   "ColumnarArrayChunk offset count {} overflows byte size",
                   offset_count);
        const auto offsets_bytes = offset_count * sizeof(ArrayOffset);
        AssertInfo(node_size >= offsets_bytes,
                   "ColumnarArrayChunk node size {} is smaller than offsets "
                   "size {}",
                   node_size,
                   offsets_bytes);

        offsets_ = std::span<const ArrayOffset>(
            reinterpret_cast<const ArrayOffset*>(node_data), offset_count);
        ValidateOffsets();

        const auto child_rows = offsets_.back();
        auto* child_data = node_data + offsets_bytes;
        const auto child_size = node_size - offsets_bytes;
        child_ = array_detail::CreateColumnarArrayChild(
            type_, child_rows, child_data, child_size, chunk_mmap_guard_);
    }

    static int
    ProtoReserveSize(size_t size) {
        AssertInfo(size <= static_cast<size_t>(std::numeric_limits<int>::max()),
                   "protobuf array size {} exceeds int range",
                   size);
        return static_cast<int>(size);
    }

    template <typename T>
    static T
    GetData(const proto::schema::TypeSchema& type,
            const Chunk& child,
            size_t index) {
        AssertInfo(!type.has_element_schema(),
                   "get_data<T> requires a scalar child chunk");

        const auto element_type = GetElementType(type);
        using ValueType = std::decay_t<T>;

        if constexpr (std::is_same_v<ValueType, bool>) {
            AssertInfo(element_type == DataType::BOOL,
                       "requested bool from array element type {}",
                       element_type);
            const auto& chunk = static_cast<const FixedWidthChunk&>(child);
            return *reinterpret_cast<const uint8_t*>(
                       chunk.ValueAt(static_cast<int64_t>(index))) != 0;
        } else if constexpr (std::is_same_v<ValueType, int> ||
                             std::is_same_v<ValueType, int8_t> ||
                             std::is_same_v<ValueType, int16_t> ||
                             std::is_same_v<ValueType, int32_t>) {
            AssertInfo(element_type == DataType::INT8 ||
                           element_type == DataType::INT16 ||
                           element_type == DataType::INT32,
                       "requested int from array element type {}",
                       element_type);
            const auto& chunk = static_cast<const FixedWidthChunk&>(child);
            return static_cast<T>(*reinterpret_cast<const int32_t*>(
                chunk.ValueAt(static_cast<int64_t>(index))));
        } else if constexpr (std::is_same_v<ValueType, int64_t>) {
            AssertInfo(element_type == DataType::INT64,
                       "requested int64 from array element type {}",
                       element_type);
            const auto& chunk = static_cast<const FixedWidthChunk&>(child);
            return *reinterpret_cast<const int64_t*>(
                chunk.ValueAt(static_cast<int64_t>(index)));
        } else if constexpr (std::is_same_v<ValueType, float>) {
            AssertInfo(element_type == DataType::FLOAT,
                       "requested float from array element type {}",
                       element_type);
            const auto& chunk = static_cast<const FixedWidthChunk&>(child);
            return *reinterpret_cast<const float*>(
                chunk.ValueAt(static_cast<int64_t>(index)));
        } else if constexpr (std::is_same_v<ValueType, double>) {
            AssertInfo(element_type == DataType::DOUBLE,
                       "requested double from array element type {}",
                       element_type);
            const auto& chunk = static_cast<const FixedWidthChunk&>(child);
            return *reinterpret_cast<const double*>(
                chunk.ValueAt(static_cast<int64_t>(index)));
        } else if constexpr (std::is_same_v<ValueType, std::string_view> ||
                             std::is_same_v<ValueType, std::string>) {
            AssertInfo(IsStringDataType(element_type),
                       "requested string from array element type {}",
                       element_type);
            const auto& chunk = static_cast<const StringChunk&>(child);
            const auto value = chunk[static_cast<int>(index)];
            return T(value.data(), value.size());
        } else {
            static_assert(AlwaysFalse<T>,
                          "unsupported ArrayValueView value type");
        }
    }

    static void
    OutputRange(const proto::schema::TypeSchema& type,
                const Chunk& child,
                ArrayOffset begin,
                ArrayOffset end,
                ScalarFieldProto& output) {
        output.Clear();

        if (type.has_element_schema()) {
            const auto& nested = static_cast<const ColumnarArrayChunk&>(child);
            auto* data = output.mutable_array_data();
            data->set_element_type(static_cast<proto::schema::DataType>(
                GetElementType(type.element_schema())));
            data->mutable_data()->Reserve(
                ProtoReserveSize(static_cast<size_t>(end - begin)));
            for (auto row = begin; row < end; ++row) {
                auto* child_output = data->add_data();
                if (!nested.isValid(static_cast<int>(row))) {
                    child_output->Clear();
                    continue;
                }
                OutputRange(nested.type(),
                            nested.child(),
                            nested.offsets()[row],
                            nested.offsets()[row + 1],
                            *child_output);
            }
            return;
        }

        switch (GetElementType(type)) {
            case DataType::BOOL: {
                auto* data = output.mutable_bool_data()->mutable_data();
                data->Reserve(ProtoReserveSize(end - begin));
                for (auto i = begin; i < end; ++i) {
                    data->Add(GetData<bool>(type, child, i));
                }
                return;
            }
            case DataType::INT8:
            case DataType::INT16:
            case DataType::INT32: {
                auto* data = output.mutable_int_data()->mutable_data();
                data->Reserve(ProtoReserveSize(end - begin));
                for (auto i = begin; i < end; ++i) {
                    data->Add(GetData<int32_t>(type, child, i));
                }
                return;
            }
            case DataType::INT64: {
                auto* data = output.mutable_long_data()->mutable_data();
                data->Reserve(ProtoReserveSize(end - begin));
                for (auto i = begin; i < end; ++i) {
                    data->Add(GetData<int64_t>(type, child, i));
                }
                return;
            }
            case DataType::FLOAT: {
                auto* data = output.mutable_float_data()->mutable_data();
                data->Reserve(ProtoReserveSize(end - begin));
                for (auto i = begin; i < end; ++i) {
                    data->Add(GetData<float>(type, child, i));
                }
                return;
            }
            case DataType::DOUBLE: {
                auto* data = output.mutable_double_data()->mutable_data();
                data->Reserve(ProtoReserveSize(end - begin));
                for (auto i = begin; i < end; ++i) {
                    data->Add(GetData<double>(type, child, i));
                }
                return;
            }
            case DataType::STRING:
            case DataType::VARCHAR: {
                auto* data = output.mutable_string_data()->mutable_data();
                data->Reserve(ProtoReserveSize(end - begin));
                for (auto i = begin; i < end; ++i) {
                    const auto value =
                        GetData<std::string_view>(type, child, i);
                    data->Add(std::string(value));
                }
                return;
            }
            default:
                ThrowInfo(Unsupported,
                          "unsupported ArrayValue leaf type {}",
                          GetElementType(type));
        }
    }

 private:
    std::shared_ptr<const proto::schema::TypeSchema> type_;
    std::span<const ArrayOffset> offsets_;
    std::shared_ptr<const Chunk> child_;
};

namespace array_detail {

inline std::shared_ptr<const Chunk>
CreateColumnarArrayChild(
    const std::shared_ptr<const proto::schema::TypeSchema>& array_type,
    ArrayOffset row_count,
    char* data,
    uint64_t size,
    const std::shared_ptr<ChunkMmapGuard>& chunk_mmap_guard) {
    AssertInfo(array_type != nullptr,
               "columnar array child type must not be null");
    if (array_type->has_element_schema()) {
        AssertInfo(row_count <= static_cast<ArrayOffset>(
                                    std::numeric_limits<int64_t>::max()),
                   "nested array row count {} exceeds int64 range",
                   row_count);
        auto child_type = std::shared_ptr<const proto::schema::TypeSchema>(
            array_type, &array_type->element_schema());
        return std::shared_ptr<const ColumnarArrayChunk>(
            new ColumnarArrayChunk(ColumnarArrayChunk::NestedNodeTag{},
                                   static_cast<int64_t>(row_count),
                                   data,
                                   size,
                                   std::move(child_type),
                                   chunk_mmap_guard));
    }

    const auto element_type = ColumnarArrayChunk::GetElementType(*array_type);
    const auto leaf_rows = ColumnarArrayChunk::CheckedLeafRowCount(row_count);
    if (IsStringDataType(element_type)) {
        ColumnarArrayChunk::ValidateStringPayload(data, size, row_count);
        return std::make_shared<const StringChunk>(
            leaf_rows, data, size, false, chunk_mmap_guard);
    }

    const auto width = ColumnarArrayChunk::ExpectedFixedWidth(element_type);
    AssertInfo(row_count <= std::numeric_limits<uint64_t>::max() / width,
               "fixed-width array leaf byte size overflows");
    const auto expected_size = row_count * width;
    AssertInfo(size == expected_size,
               "fixed-width array leaf size {} does not match expected {}",
               size,
               expected_size);
    return std::make_shared<const FixedWidthChunk>(
        leaf_rows, 1, data, size, width, false, chunk_mmap_guard);
}

}  // namespace array_detail

}  // namespace milvus
