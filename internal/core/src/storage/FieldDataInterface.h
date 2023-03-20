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

 public:
    virtual int
    get_num_rows() const = 0;

    virtual int64_t
    get_dim() const = 0;

    virtual int64_t
    get_element_size(ssize_t offset) const = 0;

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
    explicit FieldDataImpl(ssize_t dim, DataType data_type)
        : FieldDataBase(data_type), dim_(is_scalar ? 1 : dim) {
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
        return &field_data_[offset];
    }

    int64_t
    Size() const override {
        return sizeof(Type) * field_data_.size();
    }

 public:
    int
    get_num_rows() const override {
        auto len = field_data_.size();
        AssertInfo(len % dim_ == 0, "field data size not aligned");
        return len / dim_;
    }

    int64_t
    get_dim() const override {
        return dim_;
    }

    int64_t
    get_element_size(ssize_t offset) const override {
        return sizeof(Type) * dim_;
    }

 protected:
    Chunk field_data_;

 private:
    const ssize_t dim_;
};

class FieldDataStringImpl : public FieldDataImpl<std::string, true> {
 public:
    explicit FieldDataStringImpl(DataType data_type)
        : FieldDataImpl<std::string, true>(1, data_type) {
    }

    const void*
    RawValue(ssize_t offset) const {
        return field_data_[offset].c_str();
    }

    int64_t
    Size() const {
        int64_t data_size = 0;
        for (size_t offset = 0; offset < field_data_.size(); ++offset) {
            data_size += get_element_size(offset);
        }

        return data_size;
    }

 public:
    int64_t
    get_element_size(ssize_t offset) const {
        return field_data_[offset].size();
    }
};

}  // namespace milvus::storage
