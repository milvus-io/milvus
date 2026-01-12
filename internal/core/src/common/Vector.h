// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <memory>

#include "common/EasyAssert.h"
#include "Types.h"
#include "bitset/bitset.h"
#include "common/FieldData.h"
#include "common/FieldDataInterface.h"
#include "common/Types.h"

namespace milvus {
class BaseVector;
using VectorPtr = std::shared_ptr<BaseVector>;

/**
 * @brief base class for different type vector  
 * @todo implement full null value support
 */
class BaseVector {
 public:
    BaseVector(DataType data_type,
               size_t length,
               std::optional<size_t> null_count = std::nullopt)
        : type_kind_(data_type), length_(length), null_count_(null_count) {
    }
    virtual ~BaseVector() = default;

    int64_t
    size() const {
        return length_;
    }

    DataType
    type() const {
        return type_kind_;
    }

    int32_t
    elementSize() const {
        return GetDataTypeSize(type_kind_);
    };

    size_t
    nullCount() const {
        return null_count_.has_value() ? null_count_.value() : 0;
    }

    virtual void
    resize(vector_size_t newSize, bool setNotNull = true) {
        length_ = newSize;
    }

    static void
    prepareForReuse(VectorPtr& vector, vector_size_t size);

    /// Resets non-reusable buffers and updates child vectors by calling
    /// BaseVector::prepareForReuse.
    /// Base implementation checks and resets nulls buffer if needed. Keeps the
    /// nulls buffer if singly-referenced, mutable and has at least one null bit set.
    virtual void
    prepareForReuse();

    /// Creates a new empty instance of the same dynamic type with the given size.
    /// Used when cloning non-unique vectors to preserve the dynamic type.
    virtual VectorPtr
    cloneEmpty(vector_size_t size) const = 0;

 protected:
    DataType type_kind_;
    size_t length_;
    // todo: use null_count to skip some bitset operate
    std::optional<size_t> null_count_;
};

/**
 * SimpleVector abstracts over various Columnar Storage Formats,
 * it is used in custom functions.
 */
class SimpleVector : public BaseVector {
 public:
    SimpleVector(DataType data_type,
                 size_t length,
                 std::optional<size_t> null_count = std::nullopt)
        : BaseVector(data_type, length, null_count) {
    }

    virtual void*
    RawValueAt(size_t index, size_t size_of_element) = 0;

    virtual bool
    ValidAt(size_t index) = 0;
};

/**
 * @brief Single vector for scalar types
 * @todo using memory pool && buffer replace FieldData
 */
class ColumnVector final : public SimpleVector {
 public:
    ColumnVector(DataType data_type,
                 size_t length,
                 std::optional<size_t> null_count = std::nullopt)
        : SimpleVector(data_type, length, null_count),
          is_bitmap_(false),
          valid_values_(length,
                        !null_count.has_value() || null_count.value() == 0) {
        values_ = InitScalarFieldData(data_type, false, length);
    }

    //    ColumnVector(FixedVector<bool>&& data)
    //        : BaseVector(DataType::BOOL, data.size()) {
    //        values_ =
    //            std::make_shared<FieldData<bool>>(DataType::BOOL, std::move(data));
    //    }

    // the size is the number of bits
    // TODO: separate the usage of bitmap from scalar field data
    ColumnVector(TargetBitmap&& bitmap, TargetBitmap&& valid_bitmap)
        : SimpleVector(DataType::INT8, bitmap.size()),
          is_bitmap_(true),
          valid_values_(std::move(valid_bitmap)) {
        values_ = std::make_shared<FieldBitsetImpl<uint8_t>>(DataType::INT8,
                                                             std::move(bitmap));
    }

    ColumnVector(FieldDataPtr&& value, TargetBitmap&& valid_bitmap)
        : SimpleVector(value ? value->get_data_type() : DataType::NONE,
                       value ? value->Length() : 0),
          is_bitmap_(false),
          valid_values_(std::move(valid_bitmap)) {
        AssertInfo(value, "ColumnVector value cannot be null");
        values_ = std::move(value);
        // Compute null_count_ from valid_bitmap: count false bits (nulls)
        size_t nulls = 0;
        for (size_t i = 0; i < valid_values_.size(); ++i) {
            if (!valid_values_[i]) {
                ++nulls;
            }
        }
        null_count_ = (nulls > 0) ? std::optional<size_t>(nulls) : std::nullopt;
    }

    virtual ~ColumnVector() override {
        values_.reset();
        valid_values_.reset();
    }

    void*
    RawValueAt(size_t index, size_t size_of_element) override {
        return reinterpret_cast<char*>(GetRawData()) + index * size_of_element;
    }

    template <typename T>
    T
    ValueAt(size_t index) const {
        AssertInfo(index < length_, "ValueAt index out of range");
        return *(reinterpret_cast<T*>(GetRawData()) + index);
    }

    template <typename T>
    void
    SetValueAt(size_t index, const T& value) {
        AssertInfo(index < length_, "SetValueAt index out of range");
        *(reinterpret_cast<T*>(values_->Data()) + index) = value;
    }

    void
    nullAt(size_t index) {
        // Update null_count_ when setting a value to null
        if (valid_values_[index]) {
            // Was valid, now null: increment count
            null_count_ = null_count_.value_or(0) + 1;
        }
        valid_values_.set(index, false);
    }

    void
    clearNullAt(size_t index) {
        // Update null_count_ when clearing a null value
        if (!valid_values_[index]) {
            // Was null, now valid: decrement count
            if (null_count_.has_value()) {
                null_count_ =
                    (null_count_.value() > 0) ? null_count_.value() - 1 : 0;
            }
        }
        valid_values_.set(index, true);
    }

    bool
    ValidAt(size_t index) override {
        return valid_values_[index];
    }

    void*
    GetRawData() const {
        return values_->Data();
    }

    void*
    GetValidRawData() {
        return valid_values_.data();
    }

    template <typename As>
    As*
    RawAsValues() const {
        return reinterpret_cast<As*>(values_->Data());
    }

    bool
    IsBitmap() const {
        return is_bitmap_;
    }

    void
    resize(vector_size_t new_size, bool setNotNull = true) override {
        AssertInfo(!is_bitmap_, "Cannot resize bitmap column vector");
        auto old_size = length_;
        BaseVector::resize(new_size, setNotNull);
        ResizeScalarFieldData(type(), new_size, values_);

        if (new_size > old_size) {
            // Growing: new bits are added
            valid_values_.resize(new_size);
            if (setNotNull) {
                // All new bits are set to valid (true), no nulls added
                for (auto i = old_size; i < new_size; ++i) {
                    valid_values_.set(i, true);
                }
                // null_count_ unchanged (no new nulls)
            } else {
                // New bits default to null (false)
                for (auto i = old_size; i < new_size; ++i) {
                    valid_values_.set(i, false);
                }
                // Update null_count_: add (new_size - old_size) nulls
                null_count_ = null_count_.value_or(0) + (new_size - old_size);
            }
        } else if (new_size < old_size) {
            // Shrinking: need to count nulls in removed range and subtract
            size_t removed_nulls = 0;
            for (auto i = new_size; i < old_size; ++i) {
                if (!valid_values_[i]) {
                    ++removed_nulls;
                }
            }
            valid_values_.resize(new_size);
            // Update null_count_: subtract removed nulls
            if (removed_nulls > 0 && null_count_.has_value()) {
                null_count_ = (null_count_.value() >= removed_nulls)
                                  ? null_count_.value() - removed_nulls
                                  : 0;
            }
        }
        // If new_size == old_size, no change needed
    }

    void
    append(const ColumnVector& other) {
        // Validate that both vectors have the same type
        AssertInfo(type() == other.type(),
                   "Cannot append ColumnVector with different type: {} != {}",
                   static_cast<int>(type()),
                   static_cast<int>(other.type()));

        auto old_size = length_;
        values_->FillFieldData(other.GetRawData(), other.size());
        length_ += other.size();
        valid_values_.resize(length_);

        // Copy validity from other
        for (size_t i = 0; i < other.size(); ++i) {
            valid_values_.set(old_size + i, other.valid_values_[i]);
        }

        // Update null_count_ by accumulating the original null_count_ and other's null count
        size_t other_nulls = 0;
        if (other.null_count_.has_value()) {
            // Use cached null_count_ if available
            other_nulls = other.null_count_.value();
        } else {
            // Compute null count from other.valid_values_ if not cached
            for (size_t i = 0; i < other.size(); ++i) {
                if (!other.valid_values_[i]) {
                    ++other_nulls;
                }
            }
        }

        // Accumulate null counts
        size_t total_nulls = null_count_.value_or(0) + other_nulls;
        null_count_ = (total_nulls > 0) ? std::optional<size_t>{total_nulls}
                                        : std::nullopt;
    }

    VectorPtr
    cloneEmpty(vector_size_t size) const override {
        return std::make_shared<ColumnVector>(type(), size, std::nullopt);
    }

 private:
    bool is_bitmap_;  // TODO: remove the field after implementing BitmapVector
    FieldDataPtr values_;
    TargetBitmap valid_values_;  // false means the value is null
};

using ColumnVectorPtr = std::shared_ptr<ColumnVector>;

template <typename T>
class ConstantVector : public SimpleVector {
 public:
    ConstantVector(DataType data_type,
                   size_t length,
                   const T& val,
                   std::optional<size_t> null_count = std::nullopt)
        : SimpleVector(data_type, length),
          val_(val),
          is_null_(null_count.has_value() && null_count.value() > 0) {
    }

    void*
    RawValueAt(size_t _index, size_t _size_of_element) override {
        return &val_;
    }

    bool
    ValidAt(size_t _index) override {
        return !is_null_;
    }

    const T&
    GetValue() const {
        return val_;
    }

    bool
    IsNull() const {
        return is_null_;
    }

    VectorPtr
    cloneEmpty(vector_size_t size) const override {
        return std::make_shared<ConstantVector<T>>(
            type(), size, val_, std::nullopt);
    }

 private:
    T val_;
    bool is_null_;
};

/**
 * @brief Multi vectors for scalar types
 * mainly using it to pass internal result in segcore scalar engine system
 */
class RowVector : public BaseVector {
 public:
    RowVector(std::vector<DataType>& data_types,
              size_t length,
              std::optional<size_t> null_count = std::nullopt)
        : BaseVector(DataType::ROW, length, null_count) {
        for (auto& type : data_types) {
            children_values_.emplace_back(
                std::make_shared<ColumnVector>(type, length));
        }
    }

    RowVector(const std::vector<VectorPtr>& children)
        : BaseVector(DataType::ROW, 0) {
        for (auto& child : children) {
            children_values_.push_back(child);
            if (child->size() > length_) {
                length_ = child->size();
            }
        }
    }

    RowVector(std::vector<VectorPtr>&& children)
        : BaseVector(DataType::ROW, 0), children_values_(std::move(children)) {
        for (auto& child : children_values_) {
            if (child->size() > length_) {
                length_ = child->size();
            }
        }
    }

    RowVector(const RowTypePtr& rowType,
              size_t size,
              std::optional<size_t> nullCount = std::nullopt)
        : BaseVector(DataType::ROW, size, nullCount) {
        auto column_count = rowType->column_count();
        for (auto i = 0; i < column_count; i++) {
            auto column_type = rowType->column_type(i);
            children_values_.emplace_back(
                std::make_shared<ColumnVector>(column_type, size));
        }
    }

    const std::vector<VectorPtr>&
    childrens() const {
        return children_values_;
    }

    VectorPtr
    child(int index) const {
        assert(index < children_values_.size());
        return children_values_[index];
    }

    void
    resize(vector_size_t new_size, bool setNotNull = true) override;

    VectorPtr
    cloneEmpty(vector_size_t size) const override {
        // Extract data types from existing children to preserve structure
        std::vector<DataType> data_types;
        for (const auto& child : children_values_) {
            data_types.push_back(child->type());
        }
        return std::make_shared<RowVector>(data_types, size, std::nullopt);
    }

 private:
    std::vector<VectorPtr> children_values_;
};

using RowVectorPtr = std::shared_ptr<RowVector>;
}  // namespace milvus
