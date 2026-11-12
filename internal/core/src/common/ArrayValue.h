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

#include <cassert>
#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/ColumnarArrayChunk.h"
#include "common/EasyAssert.h"

namespace arrow {
class ListArray;
}

namespace milvus {

// Heap-backed storage for one logical Array value. The root value is described
// by [0, length); child owns the actual leaf column or the first nested Array
// column. buffer must outlive child because every child Chunk is a non-owning
// view over this memory. Declaring child last ensures it is destroyed first.
struct ArrayValueStorage {
    std::shared_ptr<const proto::schema::TypeSchema> type;
    ArrayOffset length{0};
    bool is_null{false};

    std::vector<char> buffer;
    std::shared_ptr<const Chunk> child;
};

std::shared_ptr<const ArrayValueStorage>
CreateArrayValueStorageFromProto(
    const ScalarFieldProto& row,
    std::shared_ptr<const proto::schema::TypeSchema> type);

// An owning, immutable representation of one logical Array value. Copies share
// the immutable heap buffer and its child Chunk tree. Unlike a sealed
// ColumnarArrayChunk, the single root value stores length directly and does not
// materialize the redundant root offsets [0, length].
//
// Segments do not keep ArrayValues: growing chunks hold one ArrayValueView per
// row into per-batch ColumnarArrayChunks and sealed segments hold
// ColumnarArrayChunks, so stored rows are read as ArrayValueView. An ArrayValue
// exists only as an element of FieldData<ArrayValue> while binlogs are read or
// written or a growing segment is loaded, and as an owned sealed Take result
// (RawTakeResult::GetOwn); growing inserts build blocks from the protobuf rows
// directly. ConcurrentVector<ArrayValue> and
// VariableLengthChunk<ArrayValue> use the type only as a tag.
class ArrayValue {
    friend class ArrayValueView;
    friend std::vector<ArrayValue>
    ArrowListToArrayValues(
        const arrow::ListArray& rows,
        std::shared_ptr<const proto::schema::TypeSchema> type);

 public:
    ArrayValue() = default;

    explicit ArrayValue(const ScalarFieldProto& row,
                        const proto::schema::TypeSchema& type)
        : ArrayValue(row,
                     std::make_shared<const proto::schema::TypeSchema>(type)) {
    }

    explicit ArrayValue(const ScalarFieldProto& row,
                        std::shared_ptr<const proto::schema::TypeSchema> type)
        : ArrayValue(CreateArrayValueStorageFromProto(row, std::move(type))) {
    }

    static ArrayValue
    FromProto(const ScalarFieldProto& row,
              const proto::schema::TypeSchema& type) {
        return ArrayValue(row, type);
    }

    static ArrayValue
    FromProto(const ScalarFieldProto& row,
              std::shared_ptr<const proto::schema::TypeSchema> type) {
        return ArrayValue(row, std::move(type));
    }

    size_t
    size() const {
        assert(storage_ != nullptr);
        return static_cast<size_t>(storage_->length);
    }

    size_t
    byte_size() const {
        assert(storage_ != nullptr);
        return storage_->buffer.size();
    }

    bool
    is_null() const {
        assert(storage_ != nullptr);
        return storage_->is_null;
    }

    const char*
    data() const {
        assert(storage_ != nullptr);
        return storage_->buffer.data();
    }

    void
    output_data(ScalarFieldProto& output) const {
        assert(storage_ != nullptr && storage_->type != nullptr &&
               storage_->child != nullptr);
        if (storage_->is_null) {
            output.Clear();
            return;
        }
        ColumnarArrayChunk::OutputRange(
            *storage_->type, *storage_->child, 0, storage_->length, output);
    }

    ScalarFieldProto
    output_data() const {
        ScalarFieldProto output;
        output_data(output);
        return output;
    }

    const proto::schema::TypeSchema&
    type() const {
        assert(storage_ != nullptr && storage_->type != nullptr);
        return *storage_->type;
    }

    // The type above as the shared, immutable object this value holds, for
    // callers that keep it beyond the value's lifetime.
    const std::shared_ptr<const proto::schema::TypeSchema>&
    shared_type() const {
        assert(storage_ != nullptr && storage_->type != nullptr);
        return storage_->type;
    }

    const Chunk&
    child() const {
        assert(storage_ != nullptr && storage_->child != nullptr);
        return *storage_->child;
    }

    ArrayValueView
    View() const;

 private:
    explicit ArrayValue(std::shared_ptr<const ArrayValueStorage> storage)
        : storage_(std::move(storage)) {
        assert(storage_ != nullptr && storage_->type != nullptr &&
               storage_->child != nullptr);
    }

    std::shared_ptr<const ArrayValueStorage> storage_;
};

// A non-owning view of one logical Array value. The owning ArrayValue,
// ColumnarArrayChunk, or an outer cache pin must outlive the view. A
// default-constructed view must be assigned before use.
class ArrayValueView {
    friend class ColumnarArrayChunk;

 private:
    struct ValidatedTag {};

 public:
    ArrayValueView() = default;

    explicit ArrayValueView(const ArrayValue& array)
        : ArrayValueView(ValidatedTag{},
                         &array.type(),
                         &array.child(),
                         0,
                         static_cast<ArrayOffset>(array.size()),
                         array.is_null()) {
    }

    size_t
    size() const {
        assert(type_ != nullptr && child_ != nullptr);
        return static_cast<size_t>(end_ - begin_);
    }

    bool
    empty() const {
        return size() == 0;
    }

    bool
    is_null() const {
        assert(type_ != nullptr && child_ != nullptr);
        return is_null_;
    }

    DataType
    element_type() const {
        assert(type_ != nullptr && child_ != nullptr);
        return ColumnarArrayChunk::GetElementType(*type_);
    }

    bool
    is_nested_array() const {
        return element_type() == DataType::ARRAY;
    }

    bool
    is_valid(size_t index) const {
        AssertInfo(
            index < size(), "array element {} out of range {}", index, size());
        return child_->isValid(static_cast<int>(begin_ + index));
    }

    ArrayValueView
    array_at(size_t index) const {
        assert(type_ != nullptr && child_ != nullptr);
        const auto length = static_cast<size_t>(end_ - begin_);
        AssertInfo(index < length,
                   "array view index {} out of range {}",
                   index,
                   length);
        AssertInfo(type_->array_element().has_array_element(),
                   "array_at requires a nested Array element");

        const auto& nested = static_cast<const ColumnarArrayChunk&>(*child_);
        const auto row = static_cast<size_t>(begin_ + index);
        return FromValidated(&nested.type(),
                             &nested.child(),
                             nested.offsets()[row],
                             nested.offsets()[row + 1],
                             !nested.isValid(static_cast<int>(row)));
    }

    template <typename T>
    T
    get_data_unchecked(size_t index) const {
        assert(type_ != nullptr && child_ != nullptr);
        const auto length = static_cast<size_t>(end_ - begin_);
        AssertInfo(index < length,
                   "array view index {} out of range {}",
                   index,
                   length);
        return ColumnarArrayChunk::ReadScalarElement<T>(
            *type_, *child_, static_cast<size_t>(begin_ + index));
    }

    // Whether this scalar Array equals the literal. Null is an ordinary value
    // and a literal cannot spell it, so a value holding a null element never
    // equals one. Mirrors ArrayView::is_same_array.
    bool
    is_same_array(const proto::plan::Array& literal) const {
        const auto length = size();
        if (static_cast<size_t>(literal.array_size()) != length) {
            return false;
        }
        if (length == 0) {
            return true;
        }
        if (!literal.same_type()) {
            return false;
        }
        const auto type = element_type();
        const auto expected_val_case =
            array_detail::ExpectedLiteralValCase(type);
        if (expected_val_case ==
            proto::plan::GenericValue::ValCase::VAL_NOT_SET) {
            return false;
        }
        for (size_t i = 0; i < length; ++i) {
            const auto& expected = literal.array(static_cast<int>(i));
            if (!is_valid(i) || expected.val_case() != expected_val_case) {
                return false;
            }
            bool same = false;
            switch (type) {
                case DataType::BOOL:
                    same = get_data_unchecked<bool>(i) == expected.bool_val();
                    break;
                case DataType::INT8:
                case DataType::INT16:
                case DataType::INT32:
                case DataType::INT64:
                    same =
                        get_data_unchecked<int64_t>(i) == expected.int64_val();
                    break;
                case DataType::FLOAT:
                    same = get_data_unchecked<float>(i) ==
                           static_cast<float>(expected.float_val());
                    break;
                case DataType::DOUBLE:
                    same =
                        get_data_unchecked<double>(i) == expected.float_val();
                    break;
                default:
                    same = get_data_unchecked<std::string_view>(i) ==
                           expected.string_val();
                    break;
            }
            if (!same) {
                return false;
            }
        }
        return true;
    }

    void
    output_data(ScalarFieldProto& output) const {
        assert(type_ != nullptr && child_ != nullptr);
        if (is_null_) {
            output.Clear();
            return;
        }
        ColumnarArrayChunk::OutputRange(*type_, *child_, begin_, end_, output);
    }

    ScalarFieldProto
    output_data() const {
        ScalarFieldProto output;
        output_data(output);
        return output;
    }

    const proto::schema::TypeSchema&
    type() const {
        assert(type_ != nullptr && child_ != nullptr);
        return *type_;
    }

    ArrayOffset
    begin() const {
        assert(type_ != nullptr && child_ != nullptr);
        return begin_;
    }

    ArrayOffset
    end() const {
        assert(type_ != nullptr && child_ != nullptr);
        return end_;
    }

    const Chunk&
    child() const {
        assert(type_ != nullptr && child_ != nullptr);
        return *child_;
    }

 private:
    ArrayValueView(ValidatedTag,
                   const proto::schema::TypeSchema* type,
                   const Chunk* child,
                   ArrayOffset begin,
                   ArrayOffset end,
                   bool is_null)
        : type_(type),
          child_(child),
          begin_(begin),
          end_(end),
          is_null_(is_null) {
        AssertInfo(type_ != nullptr && child_ != nullptr,
                   "ArrayValueView type and child must not be null");
    }

    static ArrayValueView
    FromValidated(const proto::schema::TypeSchema* type,
                  const Chunk* child,
                  ArrayOffset begin,
                  ArrayOffset end,
                  bool is_null) {
        return ArrayValueView(ValidatedTag{}, type, child, begin, end, is_null);
    }

    const proto::schema::TypeSchema* type_{nullptr};
    const Chunk* child_{nullptr};
    ArrayOffset begin_{0};
    ArrayOffset end_{0};
    bool is_null_{false};
};

inline ArrayValueView
ArrayValue::View() const {
    return ArrayValueView(*this);
}

}  // namespace milvus
