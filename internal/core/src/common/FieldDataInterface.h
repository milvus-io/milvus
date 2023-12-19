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

#include <cstddef>
#include <iostream>
#include <memory>
#include <type_traits>
#include <vector>
#include <string>
#include <mutex>
#include <shared_mutex>

#include "arrow/api.h"
#include "arrow/array/array_binary.h"
#include "common/FieldMeta.h"
#include "common/Utils.h"
#include "common/VectorTrait.h"
#include "common/EasyAssert.h"
#include "common/Array.h"

namespace milvus {

using DataType = milvus::DataType;

class FieldDataBase {
 public:
    explicit FieldDataBase(DataType data_type) : data_type_(data_type) {
    }
    virtual ~FieldDataBase() = default;

    virtual void
    FillFieldData(const void* source, ssize_t element_count) = 0;

    virtual void
    FillFieldData(const std::shared_ptr<arrow::Array> array) = 0;

    virtual void*
    Data() = 0;

    virtual const void*
    RawValue(ssize_t offset) const = 0;

    virtual int64_t
    Size() const = 0;

    virtual int64_t
    Size(ssize_t index) const = 0;

    virtual size_t
    Length() const = 0;

    virtual bool
    IsFull() const = 0;

    virtual void
    Reserve(size_t cap) = 0;

 public:
    virtual int64_t
    get_num_rows() const = 0;

    virtual int64_t
    get_dim() const = 0;

    DataType
    get_data_type() const {
        return data_type_;
    }

 protected:
    const DataType data_type_;
};

template <typename Type, bool is_scalar = false>
class FieldDataImpl : public FieldDataBase {
 public:
    // constants
    using Chunk = FixedVector<Type>;
    FieldDataImpl(FieldDataImpl&&) = delete;
    FieldDataImpl(const FieldDataImpl&) = delete;

    FieldDataImpl&
    operator=(FieldDataImpl&&) = delete;
    FieldDataImpl&
    operator=(const FieldDataImpl&) = delete;

 public:
    explicit FieldDataImpl(ssize_t dim,
                           DataType data_type,
                           int64_t buffered_num_rows = 0)
        : FieldDataBase(data_type),
          num_rows_(buffered_num_rows),
          dim_(is_scalar ? 1 : dim) {
        field_data_.resize(num_rows_ * dim_);
    }

    explicit FieldDataImpl(size_t dim, DataType type, Chunk&& field_data)
        : FieldDataBase(type), dim_(is_scalar ? 1 : dim) {
        field_data_ = std::move(field_data);
        num_rows_ = field_data.size() / dim;
    }

    void
    FillFieldData(const void* source, ssize_t element_count) override;

    void
    FillFieldData(const std::shared_ptr<arrow::Array> array) override;

    virtual void
    FillFieldData(const std::shared_ptr<arrow::StringArray>& array){};

    virtual void
    FillFieldData(const std::shared_ptr<arrow::BinaryArray>& array){};

    std::string
    GetName() const {
        return "FieldDataImpl";
    }

    void*
    Data() override {
        return field_data_.data();
    }

    const void*
    RawValue(ssize_t offset) const override {
        AssertInfo(offset < get_num_rows(),
                   "field data subscript out of range");
        AssertInfo(offset < length(),
                   "subscript position don't has valid value");
        return &field_data_[offset];
    }

    int64_t
    Size() const override {
        return sizeof(Type) * length() * dim_;
    }

    int64_t
    Size(ssize_t offset) const override {
        AssertInfo(offset < get_num_rows(),
                   "field data subscript out of range");
        AssertInfo(offset < length(),
                   "subscript position don't has valid value");
        return sizeof(Type) * dim_;
    }

    size_t
    Length() const override {
        return length_;
    }

    bool
    IsFull() const override {
        auto buffered_num_rows = get_num_rows();
        auto filled_num_rows = length();
        return buffered_num_rows == filled_num_rows;
    }

    void
    Reserve(size_t cap) override {
        std::lock_guard lck(num_rows_mutex_);
        if (cap > num_rows_) {
            num_rows_ = cap;
            field_data_.resize(num_rows_ * dim_);
        }
    }

 public:
    int64_t
    get_num_rows() const override {
        std::shared_lock lck(num_rows_mutex_);
        return num_rows_;
    }

    void
    resize_field_data(int64_t num_rows) {
        std::lock_guard lck(num_rows_mutex_);
        if (num_rows > num_rows_) {
            num_rows_ = num_rows;
            field_data_.resize(num_rows_ * dim_);
        }
    }

    size_t
    length() const {
        std::shared_lock lck(tell_mutex_);
        return length_;
    }

    int64_t
    get_dim() const override {
        return dim_;
    }

 protected:
    Chunk field_data_;
    int64_t num_rows_;
    mutable std::shared_mutex num_rows_mutex_;
    size_t length_{};
    mutable std::shared_mutex tell_mutex_;

 private:
    const ssize_t dim_;
};

class FieldDataStringImpl : public FieldDataImpl<std::string, true> {
 public:
    explicit FieldDataStringImpl(DataType data_type, int64_t total_num_rows = 0)
        : FieldDataImpl<std::string, true>(1, data_type, total_num_rows) {
    }

    int64_t
    Size() const override {
        int64_t data_size = 0;
        for (size_t offset = 0; offset < length(); ++offset) {
            data_size += field_data_[offset].size();
        }

        return data_size;
    }

    int64_t
    Size(ssize_t offset) const override {
        AssertInfo(offset < get_num_rows(),
                   "field data subscript out of range");
        AssertInfo(offset < length(),
                   "subscript position don't has valid value");
        return field_data_[offset].size();
    }

    void
    FillFieldData(const std::shared_ptr<arrow::StringArray>& array) override {
        auto n = array->length();
        if (n == 0) {
            return;
        }

        std::lock_guard lck(tell_mutex_);
        if (length_ + n > get_num_rows()) {
            resize_field_data(length_ + n);
        }

        auto i = 0;
        for (const auto& str : *array) {
            field_data_[length_ + i] = str.value();
            i++;
        }
        length_ += n;
    }
};

class FieldDataJsonImpl : public FieldDataImpl<Json, true> {
 public:
    explicit FieldDataJsonImpl(DataType data_type, int64_t total_num_rows = 0)
        : FieldDataImpl<Json, true>(1, data_type, total_num_rows) {
    }

    int64_t
    Size() const override {
        int64_t data_size = 0;
        for (size_t offset = 0; offset < length(); ++offset) {
            data_size += field_data_[offset].data().size();
        }

        return data_size;
    }

    int64_t
    Size(ssize_t offset) const override {
        AssertInfo(offset < get_num_rows(),
                   "field data subscript out of range");
        AssertInfo(offset < length(),
                   "subscript position don't has valid value");
        return field_data_[offset].data().size();
    }

    void
    FillFieldData(const std::shared_ptr<arrow::BinaryArray>& array) override {
        auto n = array->length();
        if (n == 0) {
            return;
        }

        std::lock_guard lck(tell_mutex_);
        if (length_ + n > get_num_rows()) {
            resize_field_data(length_ + n);
        }

        auto i = 0;
        for (const auto& json : *array) {
            field_data_[length_ + i] =
                Json(simdjson::padded_string(json.value()));
            i++;
        }
        length_ += n;
    }
};

class FieldDataArrayImpl : public FieldDataImpl<Array, true> {
 public:
    explicit FieldDataArrayImpl(DataType data_type, int64_t total_num_rows = 0)
        : FieldDataImpl<Array, true>(1, data_type, total_num_rows) {
    }

    int64_t
    Size() const {
        int64_t data_size = 0;
        for (size_t offset = 0; offset < length(); ++offset) {
            data_size += field_data_[offset].byte_size();
        }

        return data_size;
    }

    int64_t
    Size(ssize_t offset) const {
        AssertInfo(offset < get_num_rows(),
                   "field data subscript out of range");
        AssertInfo(offset < length(),
                   "subscript position don't has valid value");
        return field_data_[offset].byte_size();
    }
};

}  // namespace milvus
