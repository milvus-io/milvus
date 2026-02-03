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

#pragma once
#include <memory>
#include <string_view>
#include <vector>
#include "common/Array.h"
#include "common/Types.h"
#include "common/TypeTraits.h"
#include "common/VectorTrait.h"
#include "common/VectorArray.h"
#include "common/EasyAssert.h"

namespace milvus {

class BaseDataView {
 public:
    BaseDataView() = default;
    virtual ~BaseDataView() = default;

    virtual const bool*
    ValidData() const = 0;

    // for growing segment
    // set valid data after get data from vector
    virtual void
    SetValidData(const bool* valid) = 0;

    virtual int64_t
    RowCount() const = 0;
};

// Primary template for ChunkDataView
// T is the actual data type that Data() returns (e.g., std::string_view, ArrayView, int64_t)
template <typename T, typename Enable = void>
class ChunkDataView : public BaseDataView {
 public:
    using ValueType = T;

    ChunkDataView() = default;
    virtual ~ChunkDataView() = default;

    virtual const T&
    operator[](int64_t idx) const = 0;

    virtual const T*
    Data() const = 0;
};

// Specialization for vector types (FloatVector, Float16Vector, etc.)
template <typename VectorType>
class ChunkDataView<
    VectorType,
    typename std::enable_if_t<std::is_base_of_v<VectorTrait, VectorType> &&
                              !std::is_same_v<VectorType, VectorArray>>>
    : public BaseDataView {
 public:
    using ValueType = typename VectorType::embedded_type;

    ChunkDataView() = default;
    virtual ~ChunkDataView() = default;

    virtual const ValueType&
    operator[](int64_t idx) const = 0;

    virtual const ValueType*
    Data() const = 0;
};

// Primary template for ContiguousDataView
template <typename T, typename Enable = void>
class ContiguousDataView : public ChunkDataView<T> {
 public:
    ContiguousDataView(const T* data,
                       const bool* valid,
                       int64_t row_nums,
                       int64_t size)
        : data_(data), valid_(valid), row_nums_(row_nums), size_(size) {
    }

    ContiguousDataView(const T* data, int64_t row_nums, int64_t size)
        : data_(data), row_nums_(row_nums), size_(size) {
    }

    virtual ~ContiguousDataView() = default;

    const T&
    operator[](int64_t idx) const override {
        return data_[idx];
    }

    void
    SetValidData(const bool* valid) override {
        valid_ = valid;
    }

    int64_t
    RowCount() const override {
        return row_nums_;
    }

    const T*
    Data() const override {
        return data_;
    }

    const bool*
    ValidData() const override {
        return valid_;
    }

 private:
    const T* data_;
    int64_t row_nums_;
    uint64_t size_;
    const bool* valid_{nullptr};
};

// Specialization for VectorArrayView
template <>
class ContiguousDataView<VectorArrayView>
    : public ChunkDataView<VectorArrayView> {
 public:
    // Construction from VectorArrayView vector (sealed segment)
    ContiguousDataView(std::vector<VectorArrayView>&& data,
                       const bool* valid,
                       int64_t row_nums,
                       int64_t dim)
        : data_(std::move(data)),
          valid_(valid),
          row_nums_(row_nums),
          dim_(dim) {
    }

    // Construction from raw VectorArrayView pointer
    ContiguousDataView(const VectorArrayView* data,
                       int64_t row_nums,
                       size_t element_size)
        : row_nums_(row_nums), dim_(0) {
        data_ = std::vector<VectorArrayView>(data, data + row_nums);
    }

    // Construction from raw VectorArray pointer (growing segment - needs conversion)
    ContiguousDataView(const VectorArray* data,
                       int64_t row_nums,
                       size_t element_size)
        : row_nums_(row_nums), dim_(0) {
        arrays_.reserve(row_nums);
        data_.reserve(row_nums);
        for (int64_t i = 0; i < row_nums; ++i) {
            arrays_.push_back(data[i]);
        }
        for (int64_t i = 0; i < row_nums; ++i) {
            data_.emplace_back(const_cast<char*>(arrays_[i].data()),
                               arrays_[i].dim(),
                               arrays_[i].length(),
                               arrays_[i].byte_size(),
                               arrays_[i].get_element_type());
            if (i == 0) {
                dim_ = arrays_[i].dim();
            }
        }
    }

    const VectorArrayView&
    operator[](int64_t idx) const override {
        return data_[idx];
    }

    int64_t
    RowCount() const override {
        return row_nums_;
    }

    const VectorArrayView*
    Data() const override {
        return data_.data();
    }

    const bool*
    ValidData() const override {
        return valid_;
    }

    void
    SetValidData(const bool* valid) override {
        valid_ = valid;
    }

 private:
    std::vector<VectorArray> arrays_;  // Owned data (for growing segment)
    std::vector<VectorArrayView> data_;
    int64_t row_nums_;
    int64_t dim_;
    const bool* valid_{nullptr};
};

// Specialization for vector types (excluding VectorArray which has its own specialization)
template <typename VectorType>
class ContiguousDataView<
    VectorType,
    std::enable_if_t<std::is_base_of_v<VectorTrait, VectorType> &&
                     !std::is_same_v<VectorType, VectorArray>>>
    : public ChunkDataView<VectorType> {
 public:
    using ValueType = typename VectorType::embedded_type;

    ContiguousDataView(const ValueType* data,
                       const bool* valid,
                       int64_t row_nums,
                       int64_t dim,
                       int64_t element_size)
        : data_(data),
          valid_(valid),
          row_nums_(row_nums),
          dim_(dim),
          element_size_(element_size) {
    }

    ContiguousDataView(const ValueType* data, int64_t row_nums, int64_t dim)
        : data_(data), row_nums_(row_nums), dim_(dim) {
    }

    const ValueType&
    operator[](int64_t idx) const override {
        return data_[idx * dim_];
    }

    void
    SetValidData(const bool* valid) override {
        valid_ = valid;
    }

    const ValueType*
    Data() const override {
        return data_;
    }

    int64_t
    RowCount() const override {
        return row_nums_;
    }

    const bool*
    ValidData() const override {
        return valid_;
    }

 private:
    const ValueType* data_;
    const bool* valid_{nullptr};
    int64_t row_nums_;
    int64_t dim_;
    uint64_t element_size_;
};

// Specialization for std::string_view
// Handles both mmap (string_view*) and growing (string*) cases
template <>
class ContiguousDataView<std::string_view>
    : public ChunkDataView<std::string_view> {
 public:
    // Construction from string_view vector (sealed segment)
    ContiguousDataView(std::vector<std::string_view>&& views,
                       FixedVector<bool>&& valid,
                       int64_t row_nums)
        : data_(std::move(views)),
          valid_data_(std::move(valid)),
          row_nums_(row_nums) {
    }

    // Construction from raw string_view pointer (mmap case)
    ContiguousDataView(const std::string_view* data,
                       int64_t row_nums,
                       size_t element_size)
        : row_nums_(row_nums) {
        data_ = std::vector<std::string_view>(data, data + row_nums);
    }

    // Construction from raw string pointer (growing segment - needs conversion)
    ContiguousDataView(const std::string* data,
                       int64_t row_nums,
                       size_t element_size)
        : row_nums_(row_nums) {
        data_.reserve(row_nums);
        for (int64_t i = 0; i < row_nums; ++i) {
            data_.emplace_back(data[i]);
        }
    }

    const std::string_view&
    operator[](int64_t idx) const override {
        return data_[idx];
    }

    const std::string_view*
    Data() const override {
        return data_.data();
    }

    int64_t
    RowCount() const override {
        return row_nums_;
    }

    const bool*
    ValidData() const override {
        return valid_data_.empty() ? nullptr : valid_data_.data();
    }

    void
    SetValidData(const bool* valid) override {
        valid_data_.resize(row_nums_);
        std::copy_n(valid, row_nums_, valid_data_.begin());
    }

 private:
    std::vector<std::string_view> data_;
    FixedVector<bool> valid_data_;
    int64_t row_nums_;
};

// Specialization for ArrayView
// Handles both mmap (ArrayView*) and growing (Array*) cases
template <>
class ContiguousDataView<ArrayView> : public ChunkDataView<ArrayView> {
 public:
    // Construction from ArrayView vector (sealed segment)
    ContiguousDataView(std::vector<ArrayView>&& views,
                       FixedVector<bool>&& valid,
                       int64_t row_nums)
        : data_(std::move(views)),
          valid_data_(std::move(valid)),
          row_nums_(row_nums) {
    }

    // With offset/length filter
    ContiguousDataView(std::vector<ArrayView>&& views,
                       FixedVector<bool>&& valid,
                       int64_t row_nums,
                       int64_t start_offset,
                       int64_t length)
        : data_(std::move(views)),
          valid_data_(std::move(valid)),
          row_nums_(length) {
    }

    // Construction from raw ArrayView pointer (mmap case)
    ContiguousDataView(const ArrayView* data,
                       int64_t row_nums,
                       size_t element_size)
        : row_nums_(row_nums) {
        data_ = std::vector<ArrayView>(data, data + row_nums);
    }

    // Construction from raw Array pointer (growing segment - needs conversion)
    ContiguousDataView(const Array* data, int64_t row_nums, size_t element_size)
        : row_nums_(row_nums) {
        data_.reserve(row_nums);
        for (int64_t i = 0; i < row_nums; ++i) {
            data_.push_back(ArrayView(const_cast<char*>(data[i].data()),
                                      data[i].length(),
                                      data[i].byte_size(),
                                      data[i].get_element_type(),
                                      data[i].get_offsets_data()));
        }
    }

    const ArrayView&
    operator[](int64_t idx) const override {
        return data_[idx];
    }

    const ArrayView*
    Data() const override {
        return data_.data();
    }

    int64_t
    RowCount() const override {
        return row_nums_;
    }

    const bool*
    ValidData() const override {
        return valid_data_.empty() ? nullptr : valid_data_.data();
    }

    void
    SetValidData(const bool* valid) override {
        valid_data_.resize(row_nums_);
        std::copy_n(valid, row_nums_, valid_data_.begin());
    }

 private:
    std::vector<ArrayView> data_;  // Views for access (always used)
    FixedVector<bool> valid_data_;
    int64_t row_nums_;
};

// Specialization for Json
template <>
class ContiguousDataView<Json> : public ChunkDataView<Json> {
 public:
    // Construction from raw Json pointer (growing segment case)
    ContiguousDataView(const Json* data, int64_t row_nums, size_t element_size)
        : row_nums_(row_nums) {
        data_.reserve(row_nums);
        for (int64_t i = 0; i < row_nums; ++i) {
            data_.push_back(data[i]);
        }
    }

    // Construction from string_view vector (sealed segment case - JSON stored as string)
    ContiguousDataView(std::vector<std::string_view>&& views,
                       FixedVector<bool>&& valid,
                       int64_t row_nums)
        : valid_data_(std::move(valid)), row_nums_(row_nums) {
        data_.reserve(row_nums);
        for (int64_t i = 0; i < row_nums; ++i) {
            data_.emplace_back(views[i].data(), views[i].size());
        }
    }

    const Json&
    operator[](int64_t idx) const override {
        return data_[idx];
    }

    const Json*
    Data() const override {
        return data_.data();
    }

    int64_t
    RowCount() const override {
        return row_nums_;
    }

    const bool*
    ValidData() const override {
        return valid_data_.empty() ? nullptr : valid_data_.data();
    }

    void
    SetValidData(const bool* valid) override {
        valid_data_.resize(row_nums_);
        std::copy_n(valid, row_nums_, valid_data_.begin());
    }

 private:
    std::vector<Json> data_;
    FixedVector<bool> valid_data_;
    int64_t row_nums_;
};

class AnyDataView {
 public:
    template <typename T>
    AnyDataView(ChunkDataView<T>* view) : view_(view), type_(&typeid(T)) {
    }

    template <
        typename Derived,
        typename = std::enable_if_t<std::is_base_of_v<BaseDataView, Derived>>>
    AnyDataView(std::shared_ptr<Derived> view) : view_(std::move(view)) {
    }

    template <typename T>
    std::shared_ptr<ChunkDataView<T>>
    as() {
        return std::dynamic_pointer_cast<ChunkDataView<T>>(view_);
    }

    const bool*
    ValidData() const {
        return view_->ValidData();
    }

    void
    SetValidData(const bool* valid) {
        view_->SetValidData(valid);
    }

    int64_t
    RowCount() const {
        return view_->RowCount();
    }

 private:
    std::shared_ptr<BaseDataView> view_;
    const std::type_info* type_;
};

}  // namespace milvus
