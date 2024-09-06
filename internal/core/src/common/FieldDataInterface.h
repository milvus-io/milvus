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

#include "Types.h"
#include "arrow/api.h"
#include "arrow/array/array_binary.h"
#include "common/FieldMeta.h"
#include "common/Utils.h"
#include "common/VectorTrait.h"
#include "common/EasyAssert.h"
#include "common/Array.h"
#include "knowhere/dataset.h"

namespace milvus {

using DataType = milvus::DataType;

class FieldDataBase {
 public:
    explicit FieldDataBase(DataType data_type, bool nullable)
        : data_type_(data_type), nullable_(nullable) {
    }
    virtual ~FieldDataBase() = default;

    // For all FieldDataImpl subclasses, source is a pointer to element_count of
    // Type
    virtual void
    FillFieldData(const void* source, ssize_t element_count) = 0;

    virtual void
    FillFieldData(const void* field_data,
                  const uint8_t* valid_data,
                  ssize_t element_count) = 0;

    virtual void
    FillFieldData(const std::shared_ptr<arrow::Array> array) = 0;

    // For all FieldDataImpl subclasses, this method returns Type* that points
    // at all rows in this field data.
    virtual void*
    Data() = 0;

    virtual uint8_t*
    ValidData() = 0;

    // For all FieldDataImpl subclasses, this method returns a Type* that points
    // at the offset-th row of this field data.
    virtual const void*
    RawValue(ssize_t offset) const = 0;

    // Returns the serialized bytes size of all rows.
    virtual int64_t
    Size() const = 0;

    virtual int64_t
    DataSize() const = 0;

    virtual int64_t
    ValidDataSize() const = 0;

    // Returns the serialized bytes size of the index-th row.
    virtual int64_t
    DataSize(ssize_t index) const = 0;

    // Number of filled rows
    virtual size_t
    Length() const = 0;

    virtual bool
    IsFull() const = 0;

    virtual bool
    IsNullable() const = 0;

    virtual void
    Reserve(size_t cap) = 0;

 public:
    // row capacity
    virtual int64_t
    get_num_rows() const = 0;

    // each row is represented as how many Type elements
    virtual int64_t
    get_dim() const = 0;

    DataType
    get_data_type() const {
        return data_type_;
    }

    virtual int64_t
    get_null_count() const = 0;

    virtual bool
    is_valid(ssize_t offset) const = 0;

 protected:
    const DataType data_type_;
    const bool nullable_;
};

template <typename Type>
class FieldBitsetImpl : public FieldDataBase {
 public:
    FieldBitsetImpl() = delete;
    FieldBitsetImpl(FieldBitsetImpl&&) = delete;
    FieldBitsetImpl(const FieldBitsetImpl&) = delete;

    FieldBitsetImpl&
    operator=(FieldBitsetImpl&&) = delete;
    FieldBitsetImpl&
    operator=(const FieldBitsetImpl&) = delete;

    explicit FieldBitsetImpl(DataType data_type, TargetBitmap&& bitmap)
        : FieldDataBase(data_type, false), length_(bitmap.size()) {
        data_ = std::move(bitmap).into();
        cap_ = data_.size() * sizeof(Type) * 8;
        Assert(cap_ >= length_);
    }

    // FillFieldData used for read and write with storage,
    // no need to implement for bitset which used in runtime process.
    void
    FillFieldData(const void* source, ssize_t element_count) override {
        PanicInfo(NotImplemented,
                  "FillFieldData(const void* source, ssize_t element_count)"
                  "not implemented for bitset");
    }

    void
    FillFieldData(const void* field_data,
                  const uint8_t* valid_data,
                  ssize_t element_count) override {
        PanicInfo(NotImplemented,
                  "FillFieldData(const void* field_data, "
                  "const uint8_t* valid_data, ssize_t element_count)"
                  "not implemented for bitset");
    }

    void
    FillFieldData(const std::shared_ptr<arrow::Array> array) override {
        PanicInfo(NotImplemented,
                  "FillFieldData(const std::shared_ptr<arrow::Array>& array) "
                  "not implemented for bitset");
    }

    virtual void
    FillFieldData(const std::shared_ptr<arrow::StringArray>& array) {
        PanicInfo(NotImplemented,
                  "FillFieldData(const std::shared_ptr<arrow::StringArray>& "
                  "array) not implemented for bitset");
    }

    virtual void
    FillFieldData(const std::shared_ptr<arrow::BinaryArray>& array) {
        PanicInfo(NotImplemented,
                  "FillFieldData(const std::shared_ptr<arrow::BinaryArray>& "
                  "array) not implemented for bitset");
    }

    std::string
    GetName() const {
        return "FieldBitsetImpl";
    }

    void*
    Data() override {
        return data_.data();
    }

    uint8_t*
    ValidData() override {
        PanicInfo(NotImplemented, "ValidData() not implemented for bitset");
    }

    const void*
    RawValue(ssize_t offset) const override {
        PanicInfo(NotImplemented,
                  "RawValue(ssize_t offset) not implemented for bitset");
    }

    int64_t
    Size() const override {
        return DataSize() + ValidDataSize();
    }

    int64_t
    DataSize() const override {
        return sizeof(Type) * get_num_rows();
    }

    int64_t
    DataSize(ssize_t offset) const override {
        return sizeof(Type);
    }

    int64_t
    ValidDataSize() const override {
        return 0;
    }

    size_t
    Length() const override {
        return get_length();
    }

    bool
    IsFull() const override {
        auto cap_num_rows = get_num_rows();
        auto filled_num_rows = get_length();
        return cap_num_rows == filled_num_rows;
    }

    bool
    IsNullable() const override {
        return false;
    }

    void
    Reserve(size_t cap) override {
        std::lock_guard lck(cap_mutex_);
        AssertInfo(cap % (8 * sizeof(Type)) == 0,
                   "Reverse bitset size must be a multiple of {}",
                   8 * sizeof(Type));
        if (cap > cap_) {
            data_.resize(cap / (8 * sizeof(Type)));
            cap_ = cap;
        }
    }

 public:
    int64_t
    get_num_rows() const override {
        std::shared_lock lck(cap_mutex_);
        return cap_;
    }

    size_t
    get_length() const {
        std::shared_lock lck(length_mutex_);
        return length_;
    }

    int64_t
    get_dim() const override {
        return 1;
    }

    int64_t
    get_null_count() const override {
        PanicInfo(NotImplemented,
                  "get_null_count() not implemented for bitset");
    }

    bool
    is_valid(ssize_t offset) const override {
        PanicInfo(NotImplemented,
                  "is_valid(ssize_t offset) not implemented for bitset");
    }

 private:
    FixedVector<Type> data_{};
    // capacity that data_ can store
    int64_t cap_;
    mutable std::shared_mutex cap_mutex_;
    // number of actual elements in data_
    size_t length_{};
    mutable std::shared_mutex length_mutex_;
};

template <typename Type, bool is_type_entire_row = false>
class FieldDataImpl : public FieldDataBase {
 public:
    FieldDataImpl(FieldDataImpl&&) = delete;
    FieldDataImpl(const FieldDataImpl&) = delete;

    FieldDataImpl&
    operator=(FieldDataImpl&&) = delete;
    FieldDataImpl&
    operator=(const FieldDataImpl&) = delete;

 public:
    explicit FieldDataImpl(ssize_t dim,
                           DataType data_type,
                           bool nullable,
                           int64_t buffered_num_rows = 0)
        : FieldDataBase(data_type, nullable),
          num_rows_(buffered_num_rows),
          dim_(is_type_entire_row ? 1 : dim) {
        data_.resize(num_rows_ * dim_);
        if (nullable) {
            if (IsVectorDataType(data_type)) {
                PanicInfo(NotImplemented, "vector type not support null");
            }
            valid_data_.resize((num_rows_ + 7) / 8);
        }
    }

    explicit FieldDataImpl(size_t dim,
                           DataType type,
                           bool nullable,
                           FixedVector<Type>&& data)
        : FieldDataBase(type, nullable), dim_(is_type_entire_row ? 1 : dim) {
        AssertInfo(!nullable, "need to fill valid_data when nullable is true");
        data_ = std::move(data);
        Assert(data_.size() % dim == 0);
        num_rows_ = data_.size() / dim;
    }

    explicit FieldDataImpl(size_t dim,
                           DataType type,
                           bool nullable,
                           FixedVector<Type>&& data,
                           FixedVector<uint8_t>&& valid_data)
        : FieldDataBase(type, nullable), dim_(is_type_entire_row ? 1 : dim) {
        AssertInfo(nullable,
                   "no need to fill valid_data when nullable is false");
        data_ = std::move(data);
        valid_data_ = std::move(valid_data);
        Assert(data_.size() % dim == 0);
        num_rows_ = data_.size() / dim;
    }

    void
    FillFieldData(const void* source, ssize_t element_count) override;

    void
    FillFieldData(const void* field_data,
                  const uint8_t* valid_data,
                  ssize_t element_count) override;

    void
    FillFieldData(const std::shared_ptr<arrow::Array> array) override;

    virtual void
    FillFieldData(const std::shared_ptr<arrow::StringArray>& array) {
        PanicInfo(NotImplemented,
                  "FillFieldData(const std::shared_ptr<arrow::StringArray>& "
                  "array) not implemented by default");
    }

    virtual void
    FillFieldData(const std::shared_ptr<arrow::BinaryArray>& array) {
        PanicInfo(NotImplemented,
                  "FillFieldData(const std::shared_ptr<arrow::BinaryArray>& "
                  "array) not implemented by default");
    }

    std::string
    GetName() const {
        return "FieldDataImpl";
    }

    void*
    Data() override {
        return data_.data();
    }

    uint8_t*
    ValidData() override {
        return valid_data_.data();
    }

    const void*
    RawValue(ssize_t offset) const override {
        AssertInfo(offset < get_num_rows(),
                   "field data subscript out of range");
        AssertInfo(offset < length(),
                   "subscript position don't has valid value");
        return &data_[offset];
    }

    // std::optional<const void*>
    // Value(ssize_t offset) {
    //     if (!is_type_entire_row) {
    //         return RawValue(offset);
    //     }
    //     AssertInfo(offset < get_num_rows(),
    //                "field data subscript out of range");
    //     AssertInfo(offset < length(),
    //                "subscript position don't has valid value");
    //     if (nullable_ && !valid_data_[offset]) {
    //         return std::nullopt;
    //     }
    //     return &field_data_[offset];
    // }

    int64_t
    Size() const override {
        return DataSize() + ValidDataSize();
    }

    int64_t
    DataSize() const override {
        return sizeof(Type) * length() * dim_;
    }

    int64_t
    DataSize(ssize_t offset) const override {
        AssertInfo(offset < get_num_rows(),
                   "field data subscript out of range");
        AssertInfo(offset < length(),
                   "subscript position don't has valid value");
        return sizeof(Type) * dim_;
    }

    int64_t
    ValidDataSize() const override {
        if (nullable_) {
            return sizeof(uint8_t) * (length() + 7) / 8;
        }
        return 0;
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

    bool
    IsNullable() const override {
        return nullable_;
    }

    void
    Reserve(size_t cap) override {
        std::lock_guard lck(num_rows_mutex_);
        if (cap > num_rows_) {
            num_rows_ = cap;
            data_.resize(num_rows_ * dim_);
        }
        if (nullable_) {
            valid_data_.resize((num_rows_ + 7) / 8);
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
            data_.resize(num_rows_ * dim_);
            if (nullable_) {
                valid_data_.resize((num_rows + 7) / 8);
            }
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

    int64_t
    get_null_count() const override {
        std::shared_lock lck(tell_mutex_);
        return null_count;
    }

    bool
    is_valid(ssize_t offset) const override {
        std::shared_lock lck(tell_mutex_);
        AssertInfo(offset < get_num_rows(),
                   "field data subscript out of range");
        AssertInfo(offset < length(),
                   "subscript position don't has valid value");
        if (!nullable_) {
            return true;
        }
        auto bit = (valid_data_[offset >> 3] >> ((offset & 0x07))) & 1;
        return bit;
    }

 protected:
    FixedVector<Type> data_{};
    FixedVector<uint8_t> valid_data_{};
    // number of elements data_ can hold
    int64_t num_rows_;
    mutable std::shared_mutex num_rows_mutex_;
    int64_t null_count{0};
    // number of actual elements in data_
    size_t length_{};
    mutable std::shared_mutex tell_mutex_;

 private:
    const ssize_t dim_;
};

class FieldDataStringImpl : public FieldDataImpl<std::string, true> {
 public:
    explicit FieldDataStringImpl(DataType data_type,
                                 bool nullable,
                                 int64_t total_num_rows = 0)
        : FieldDataImpl<std::string, true>(
              1, data_type, nullable, total_num_rows) {
    }

    int64_t
    DataSize() const override {
        int64_t data_size = 0;
        for (size_t offset = 0; offset < length(); ++offset) {
            data_size += data_[offset].size();
        }

        return data_size;
    }

    int64_t
    DataSize(ssize_t offset) const override {
        AssertInfo(offset < get_num_rows(),
                   "field data subscript out of range");
        AssertInfo(offset < length(),
                   "subscript position don't has valid value");
        return data_[offset].size();
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
            data_[length_ + i] = str.value();
            i++;
        }
        if (IsNullable()) {
            auto valid_data = array->null_bitmap_data();
            if (valid_data == nullptr) {
                valid_data_.resize((n + 7) / 8, 0xFF);
            } else {
                std::copy_n(valid_data, (n + 7) / 8, valid_data_.data());
            }
        }
        length_ += n;
    }
};

class FieldDataJsonImpl : public FieldDataImpl<Json, true> {
 public:
    explicit FieldDataJsonImpl(DataType data_type,
                               bool nullable,
                               int64_t total_num_rows = 0)
        : FieldDataImpl<Json, true>(1, data_type, nullable, total_num_rows) {
    }

    int64_t
    DataSize() const override {
        int64_t data_size = 0;
        for (size_t offset = 0; offset < length(); ++offset) {
            data_size += data_[offset].data().size();
        }

        return data_size;
    }

    int64_t
    DataSize(ssize_t offset) const override {
        AssertInfo(offset < get_num_rows(),
                   "field data subscript out of range");
        AssertInfo(offset < length(),
                   "subscript position don't has valid value");
        return data_[offset].data().size();
    }

    void
    FillFieldData(const std::shared_ptr<arrow::Array> array) override {
        AssertInfo(array->type()->id() == arrow::Type::type::BINARY,
                   "inconsistent data type, expected: {}, got: {}",
                   "BINARY",
                   array->type()->ToString());
        auto json_array = std::dynamic_pointer_cast<arrow::BinaryArray>(array);
        FillFieldData(json_array);
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
            data_[length_ + i] = Json(simdjson::padded_string(json.value()));
            i++;
        }
        if (IsNullable()) {
            auto valid_data = array->null_bitmap_data();
            if (valid_data == nullptr) {
                valid_data_.assign((n + 7) / 8, 0xFF);
            } else {
                std::copy_n(valid_data, (n + 7) / 8, valid_data_.data());
            }
        }
        length_ += n;
    }
};

class FieldDataSparseVectorImpl
    : public FieldDataImpl<knowhere::sparse::SparseRow<float>, true> {
 public:
    explicit FieldDataSparseVectorImpl(DataType data_type,
                                       int64_t total_num_rows = 0)
        : FieldDataImpl<knowhere::sparse::SparseRow<float>, true>(
              /*dim=*/1, data_type, false, total_num_rows),
          vec_dim_(0) {
        AssertInfo(data_type == DataType::VECTOR_SPARSE_FLOAT,
                   "invalid data type for sparse vector");
    }

    int64_t
    DataSize() const override {
        int64_t data_size = 0;
        for (size_t i = 0; i < length(); ++i) {
            data_size += data_[i].data_byte_size();
        }
        return data_size;
    }

    int64_t
    DataSize(ssize_t offset) const override {
        AssertInfo(offset < get_num_rows(),
                   "field data subscript out of range");
        AssertInfo(offset < length(),
                   "subscript position don't has valid value");
        return data_[offset].data_byte_size();
    }

    // source is a pointer to element_count of
    // knowhere::sparse::SparseRow<float>
    void
    FillFieldData(const void* source, ssize_t element_count) override {
        if (element_count == 0) {
            return;
        }

        std::lock_guard lck(tell_mutex_);
        if (length_ + element_count > get_num_rows()) {
            resize_field_data(length_ + element_count);
        }
        auto ptr =
            static_cast<const knowhere::sparse::SparseRow<float>*>(source);
        for (int64_t i = 0; i < element_count; ++i) {
            auto& row = ptr[i];
            vec_dim_ = std::max(vec_dim_, row.dim());
        }
        std::copy_n(ptr, element_count, data_.data() + length_);
        length_ += element_count;
    }

    // each binary in array is a knowhere::sparse::SparseRow<float>
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

        for (int64_t i = 0; i < array->length(); ++i) {
            auto view = array->GetView(i);
            auto& row = data_[length_ + i];
            row = CopyAndWrapSparseRow(view.data(), view.size());
            vec_dim_ = std::max(vec_dim_, row.dim());
        }
        length_ += n;
    }

    int64_t
    Dim() const {
        return vec_dim_;
    }

 private:
    int64_t vec_dim_ = 0;
};

class FieldDataArrayImpl : public FieldDataImpl<Array, true> {
 public:
    explicit FieldDataArrayImpl(DataType data_type,
                                bool nullable,
                                int64_t total_num_rows = 0)
        : FieldDataImpl<Array, true>(1, data_type, nullable, total_num_rows) {
    }

    int64_t
    DataSize() const override {
        int64_t data_size = 0;
        for (size_t offset = 0; offset < length(); ++offset) {
            data_size += data_[offset].byte_size();
        }
        return data_size;
    }

    int64_t
    DataSize(ssize_t offset) const override {
        AssertInfo(offset < get_num_rows(),
                   "field data subscript out of range");
        AssertInfo(offset < length(),
                   "subscript position don't has valid value");
        return data_[offset].byte_size();
    }
};

}  // namespace milvus
