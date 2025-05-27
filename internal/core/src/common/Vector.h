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
    size() {
        return length_;
    }

    DataType
    type() {
        return type_kind_;
    }

 protected:
    DataType type_kind_;
    size_t length_;
    // todo: use null_count to skip some bitset operate
    std::optional<size_t> null_count_;
};

using VectorPtr = std::shared_ptr<BaseVector>;

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

    virtual ~ColumnVector() override {
        values_.reset();
        valid_values_.reset();
    }

    void*
    RawValueAt(size_t index, size_t size_of_element) override {
        return reinterpret_cast<char*>(GetRawData()) + index * size_of_element;
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

    const std::vector<VectorPtr>&
    childrens() const {
        return children_values_;
    }

    VectorPtr
    child(int index) const {
        assert(index < children_values_.size());
        return children_values_[index];
    }

 private:
    std::vector<VectorPtr> children_values_;
};

using RowVectorPtr = std::shared_ptr<RowVector>;
}  // namespace milvus
