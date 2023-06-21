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

#include <iostream>
#include <memory>
#include <vector>
#include <string>
#include <mutex>
#include <shared_mutex>

#include "arrow/api.h"
#include "common/FieldMeta.h"
#include "common/Utils.h"
#include "common/VectorTrait.h"
#include "exceptions/EasyAssert.h"
#include "storage/Exception.h"

namespace milvus::storage {

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

    virtual const void*
    Data() const = 0;

    virtual const void*
    RawValue(ssize_t offset) const = 0;

    virtual int64_t
    Size() const = 0;

    virtual int64_t
    Size(ssize_t index) const = 0;

    virtual bool
    IsFull() const = 0;

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
          dim_(is_scalar ? 1 : dim),
          num_rows_(buffered_num_rows),
          tell_(0) {
        field_data_.resize(num_rows_ * dim_);
    }

    void
    FillFieldData(const void* source, ssize_t element_count) override;

    void
    FillFieldData(const std::shared_ptr<arrow::Array> array) override;

    std::string
    GetName() const {
        return "FieldDataImpl";
    }

    const void*
    Data() const override {
        return field_data_.data();
    }

    const void*
    RawValue(ssize_t offset) const override {
        AssertInfo(offset < get_num_rows(),
                   "field data subscript out of range");
        AssertInfo(offset < get_tell(),
                   "subscript position don't has valid value");
        return &field_data_[offset];
    }

    int64_t
    Size() const override {
        return sizeof(Type) * get_tell() * dim_;
    }

    int64_t
    Size(ssize_t offset) const override {
        AssertInfo(offset < get_num_rows(),
                   "field data subscript out of range");
        AssertInfo(offset < get_tell(),
                   "subscript position don't has valid value");
        return sizeof(Type) * dim_;
    }

    bool
    IsFull() const override {
        auto buffered_num_rows = get_num_rows();
        auto filled_num_rows = get_tell();
        return buffered_num_rows == filled_num_rows;
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

    int64_t
    get_tell() const {
        std::shared_lock lck(tell_mutex_);
        return tell_;
    }

    int64_t
    get_dim() const override {
        return dim_;
    }

 protected:
    Chunk field_data_;
    int64_t num_rows_;
    mutable std::shared_mutex num_rows_mutex_;
    int64_t tell_;
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
    Size() const {
        int64_t data_size = 0;
        for (size_t offset = 0; offset < get_tell(); ++offset) {
            data_size += field_data_[offset].size();
        }

        return data_size;
    }

    int64_t
    Size(ssize_t offset) const {
        AssertInfo(offset < get_num_rows(),
                   "field data subscript out of range");
        AssertInfo(offset < get_tell(),
                   "subscript position don't has valid value");
        return field_data_[offset].size();
    }
};

class FieldDataJsonImpl : public FieldDataImpl<Json, true> {
 public:
    explicit FieldDataJsonImpl(DataType data_type, int64_t total_num_rows = 0)
        : FieldDataImpl<Json, true>(1, data_type, total_num_rows) {
    }

    int64_t
    Size() const {
        int64_t data_size = 0;
        for (size_t offset = 0; offset < get_tell(); ++offset) {
            data_size += field_data_[offset].data().size();
        }

        return data_size;
    }

    int64_t
    Size(ssize_t offset) const {
        AssertInfo(offset < get_num_rows(),
                   "field data subscript out of range");
        AssertInfo(offset < get_tell(),
                   "subscript position don't has valid value");
        return field_data_[offset].data().size();
    }
};

}  // namespace milvus::storage
