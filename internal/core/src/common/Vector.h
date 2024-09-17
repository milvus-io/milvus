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

#include <boost/variant.hpp>
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
    std::optional<size_t> null_count_;
};

using VectorPtr = std::shared_ptr<BaseVector>;

/**
 * @brief Single vector for scalar types
 * @todo using memory pool && buffer replace FieldData
 */
class ColumnVector final : public BaseVector {
 public:
    ColumnVector(DataType data_type,
                 size_t length,
                 std::optional<size_t> null_count = std::nullopt)
        : BaseVector(data_type, length, null_count) {
        //todo: support null expr
        values_ = InitScalarFieldData(data_type, false, length);
    }

    //    ColumnVector(FixedVector<bool>&& data)
    //        : BaseVector(DataType::BOOL, data.size()) {
    //        values_ =
    //            std::make_shared<FieldData<bool>>(DataType::BOOL, std::move(data));
    //    }

    // the size is the number of bits
    ColumnVector(TargetBitmap&& bitmap)
        : BaseVector(DataType::INT8, bitmap.size()) {
        values_ = std::make_shared<FieldBitsetImpl<uint8_t>>(DataType::INT8,
                                                             std::move(bitmap));
    }

    virtual ~ColumnVector() override {
        values_.reset();
    }

    void*
    GetRawData() const {
        return values_->Data();
    }

    template <typename As>
    const As*
    RawAsValues() const {
        return reinterpret_cast<const As*>(values_->Data());
    }

 private:
    FieldDataPtr values_;
};

using ColumnVectorPtr = std::shared_ptr<ColumnVector>;

template <typename T>
class ValueVector : public BaseVector {
 public:
    ValueVector(DataType data_type,
                size_t length,
                std::optional<size_t> null_count = std::nullopt)
        : BaseVector(data_type, length) {
    }

    virtual const T&
    GetValueAt(size_t index) const = 0;

    virtual TargetBitmap
    Apply(std::function<bool(const T&)> func) = 0;
};

template <typename T>
class ConstantVector final : public ValueVector<T> {
 public:
    ConstantVector(DataType data_type,
                   size_t length,
                   const T& val,
                   std::optional<size_t> null_count = std::nullopt)
        : ValueVector<T>(data_type, length, null_count), val_(val) {
    }

    const T&
    GetValueAt(size_t _) const override {
        return val_;
    }

    TargetBitmap
    Apply(std::function<bool(const T&)> func) override {
        return TargetBitmap(this->size(), func(val_));
    }

 private:
    const T val_;
};

// TODO: simd support
template <typename T>
class ColumnValueVector final : public ValueVector<T> {
 public:
    ColumnValueVector(DataType data_type,
                      size_t length,
                      std::optional<size_t> null_count = std::nullopt)
        : ValueVector<T>(data_type, length, null_count), vec_(length) {
    }

    void
    Set(size_t index, const T& val) {
        vec_[index] = val;
    };

    const T&
    GetValueAt(size_t i) const override {
        return vec_[i];
    }

    TargetBitmap
    Apply(std::function<bool(const T&)> func) override {
        TargetBitmap result_vec(this->size());
        for (int i = 0; i < this->size(); ++i) {
            result_vec.set(i, func(GetValueAt(i)));
        }
        return result_vec;
    }

 private:
    FixedVector<T> vec_;
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
