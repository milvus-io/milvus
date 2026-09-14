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

#include <type_traits>
#include <utility>
#include <vector>
#include <memory>

#include <arrow/array.h>
#include <arrow/array/builder_primitive.h>
#include <fmt/core.h>

#include "FieldMeta.h"
#include "Types.h"
#include "common/FastMem.h"

namespace milvus {

namespace array_detail {

inline proto::plan::GenericValue::ValCase
ExpectedLiteralValCase(DataType element_type) {
    switch (element_type) {
        case DataType::BOOL:
            return proto::plan::GenericValue::ValCase::kBoolVal;
        case DataType::INT8:
        case DataType::INT16:
        case DataType::INT32:
        case DataType::INT64:
            return proto::plan::GenericValue::ValCase::kInt64Val;
        case DataType::FLOAT:
        case DataType::DOUBLE:
            return proto::plan::GenericValue::ValCase::kFloatVal;
        case DataType::STRING:
        case DataType::VARCHAR:
        case DataType::GEOMETRY:
            return proto::plan::GenericValue::ValCase::kStringVal;
        default:
            return proto::plan::GenericValue::ValCase::VAL_NOT_SET;
    }
}

}  // namespace array_detail

class Array {
 public:
    Array() = default;

    ~Array() = default;

    Array(char* data,
          int len,
          size_t size,
          DataType element_type,
          const uint32_t* offsets_ptr)
        : Array(data,
                len,
                size,
                element_type,
                offsets_ptr,
                TargetBitmapView(),
                false) {
    }

    Array(char* data,
          int len,
          size_t size,
          DataType element_type,
          const uint32_t* offsets_ptr,
          const TargetBitmapView& element_valid_data,
          bool element_nullable)
        : size_(size),
          length_(len),
          element_type_(element_type),
          element_nullable_(element_nullable) {
        data_ = std::make_unique<char[]>(size);
        milvus::fastmem::FastMemcpy(data_.get(), data, size);
        if (IsVariableDataType(element_type)) {
            AssertInfo(offsets_ptr != nullptr,
                       "For variable type elements in array, offsets_ptr must "
                       "be non-null");
            offsets_ptr_ = std::make_unique<uint32_t[]>(len);
            milvus::fastmem::FastMemcpy(
                offsets_ptr_.get(), offsets_ptr, len * sizeof(uint32_t));
        }
        init_element_valid_data(element_valid_data);
    }

    explicit Array(const ScalarFieldProto& field_data)
        : Array(field_data, false) {
    }

    Array(const ScalarFieldProto& field_data, bool element_nullable)
        : element_nullable_(element_nullable) {
        switch (field_data.data_case()) {
            case ScalarFieldProto::kBoolData: {
                element_type_ = DataType::BOOL;
                length_ = field_data.bool_data().data().size();
                size_ = length_;
                data_ = std::make_unique<char[]>(size_);
                for (int i = 0; i < length_; ++i) {
                    reinterpret_cast<bool*>(data_.get())[i] =
                        field_data.bool_data().data(i);
                }
                break;
            }
            case ScalarFieldProto::kIntData: {
                element_type_ = DataType::INT32;
                length_ = field_data.int_data().data().size();
                size_ = length_ * sizeof(int32_t);
                data_ = std::make_unique<char[]>(size_);
                for (int i = 0; i < length_; ++i) {
                    reinterpret_cast<int*>(data_.get())[i] =
                        field_data.int_data().data(i);
                }
                break;
            }
            case ScalarFieldProto::kLongData: {
                element_type_ = DataType::INT64;
                length_ = field_data.long_data().data().size();
                size_ = length_ * sizeof(int64_t);
                data_ = std::make_unique<char[]>(size_);
                for (int i = 0; i < length_; ++i) {
                    reinterpret_cast<int64_t*>(data_.get())[i] =
                        field_data.long_data().data(i);
                }
                break;
            }
            case ScalarFieldProto::kFloatData: {
                element_type_ = DataType::FLOAT;
                length_ = field_data.float_data().data().size();
                size_ = length_ * sizeof(float);
                data_ = std::make_unique<char[]>(size_);
                for (int i = 0; i < length_; ++i) {
                    reinterpret_cast<float*>(data_.get())[i] =
                        field_data.float_data().data(i);
                }
                break;
            }
            case ScalarFieldProto::kDoubleData: {
                element_type_ = DataType::DOUBLE;
                length_ = field_data.double_data().data().size();
                size_ = length_ * sizeof(double);
                data_ = std::make_unique<char[]>(size_);
                for (int i = 0; i < length_; ++i) {
                    reinterpret_cast<double*>(data_.get())[i] =
                        field_data.double_data().data(i);
                }
                break;
            }
            case ScalarFieldProto::kStringData: {
                element_type_ = DataType::STRING;
                length_ = field_data.string_data().data().size();
                offsets_ptr_ = std::make_unique<uint32_t[]>(length_);
                for (int i = 0; i < length_; ++i) {
                    offsets_ptr_[i] = size_;
                    size_ +=
                        field_data.string_data()
                            .data(i)
                            .size();  //type risk here between uint32_t vs size_t
                }
                data_ = std::make_unique<char[]>(size_);
                for (int i = 0; i < length_; ++i) {
                    const auto& value = field_data.string_data().data(i);
                    milvus::fastmem::FastMemcpy(data_.get() + offsets_ptr_[i],
                                                value.data(),
                                                value.size());
                }
                break;
            }
            default: {
                // empty array
            }
        }
        if (element_nullable_) {
            init_element_valid_data(field_data.valid_data());
        } else {
            AssertInfo(field_data.valid_data_size() == 0,
                       "non-element-nullable array cannot carry element "
                       "valid_data");
        }
    }

    Array(const Array& array)
        : length_{array.length_},
          size_{array.size_},
          element_type_{array.element_type_},
          element_nullable_{array.element_nullable_},
          has_invalid_element_{array.has_invalid_element_} {
        data_ = std::make_unique<char[]>(array.size_);
        milvus::fastmem::FastMemcpy(
            data_.get(), array.data_.get(), array.size_);
        if (IsVariableDataType(array.element_type_)) {
            AssertInfo(array.get_offsets_data() != nullptr,
                       "for array with variable length elements, offsets_ptr"
                       "must not be nullptr");
            offsets_ptr_ = std::make_unique<uint32_t[]>(length_);
            milvus::fastmem::FastMemcpy(offsets_ptr_.get(),
                                        array.get_offsets_data(),
                                        array.length() * sizeof(uint32_t));
        }
        if (element_nullable_) {
            element_valid_data_ = array.element_valid_data_.clone();
        }
    }

    friend void
    swap(Array& array1, Array& array2) noexcept {
        using std::swap;
        swap(array1.data_, array2.data_);
        swap(array1.length_, array2.length_);
        swap(array1.size_, array2.size_);
        swap(array1.element_type_, array2.element_type_);
        swap(array1.offsets_ptr_, array2.offsets_ptr_);
        swap(array1.element_nullable_, array2.element_nullable_);
        swap(array1.element_valid_data_, array2.element_valid_data_);
        swap(array1.has_invalid_element_, array2.has_invalid_element_);
    }

    Array&
    operator=(const Array& array) {
        Array temp(array);
        swap(*this, temp);
        return *this;
    }

    Array(Array&& other) noexcept : Array() {
        swap(*this, other);
    }

    Array&
    operator=(Array&& other) noexcept {
        swap(*this, other);
        return *this;
    }

    template <typename T>
    T
    get_data_unchecked(const int index) const {
        // Reads raw payload; caller must handle element validity separately.
        return get_raw_data<T>(index);
    }

    bool
    is_element_valid(int index) const {
        AssertInfo(index >= 0 && index < length_,
                   "index out of range, index={}, length={}",
                   index,
                   length_);
        if (!element_nullable_) {
            return true;
        }
        return element_valid_data_[index];
    }

    uint32_t*
    get_offsets_data() const {
        return offsets_ptr_.get();
    }

    void
    output_data(ScalarFieldProto& data_array) const {
        switch (element_type_) {
            case DataType::BOOL: {
                data_array.mutable_bool_data()->mutable_data()->Reserve(
                    length_);
                for (int j = 0; j < length_; ++j) {
                    auto element = get_raw_data<bool>(j);
                    data_array.mutable_bool_data()->add_data(element);
                }
                break;
            }
            case DataType::INT8:
            case DataType::INT16:
            case DataType::INT32: {
                data_array.mutable_int_data()->mutable_data()->Reserve(length_);
                for (int j = 0; j < length_; ++j) {
                    auto element = get_raw_data<int>(j);
                    data_array.mutable_int_data()->add_data(element);
                }
                break;
            }
            case DataType::INT64: {
                data_array.mutable_long_data()->mutable_data()->Reserve(
                    length_);
                for (int j = 0; j < length_; ++j) {
                    auto element = get_raw_data<int64_t>(j);
                    data_array.mutable_long_data()->add_data(element);
                }
                break;
            }
            case DataType::STRING:
            case DataType::VARCHAR: {
                data_array.mutable_string_data()->mutable_data()->Reserve(
                    length_);
                for (int j = 0; j < length_; ++j) {
                    auto element = get_raw_data<std::string_view>(j);
                    data_array.mutable_string_data()->add_data(element.data(),
                                                               element.size());
                }
                break;
            }
            case DataType::FLOAT: {
                data_array.mutable_float_data()->mutable_data()->Reserve(
                    length_);
                for (int j = 0; j < length_; ++j) {
                    auto element = get_raw_data<float>(j);
                    data_array.mutable_float_data()->add_data(element);
                }
                break;
            }
            case DataType::DOUBLE: {
                data_array.mutable_double_data()->mutable_data()->Reserve(
                    length_);
                for (int j = 0; j < length_; ++j) {
                    auto element = get_raw_data<double>(j);
                    data_array.mutable_double_data()->add_data(element);
                }
                break;
            }
            case DataType::GEOMETRY: {
                data_array.mutable_geometry_data()->mutable_data()->Reserve(
                    length_);
                for (int j = 0; j < length_; ++j) {
                    auto element = get_raw_data<std::string_view>(j);
                    data_array.mutable_geometry_data()->add_data(
                        element.data(), element.size());
                }
                break;
            }
            default: {
                // empty array
            }
        }
        if (element_nullable_) {
            data_array.mutable_valid_data()->Reserve(length_);
            for (int i = 0; i < length_; ++i) {
                data_array.add_valid_data(is_element_valid(i));
            }
        }
    }

    ScalarFieldProto
    output_data() const {
        ScalarFieldProto data_array;
        output_data(data_array);
        return data_array;
    }

    int
    length() const {
        return length_;
    }

    size_t
    byte_size() const {
        return size_;
    }

    DataType
    get_element_type() const {
        return element_type_;
    }

    const char*
    data() const {
        return data_.get();
    }

    bool
    is_same_array(const proto::plan::Array& arr2) const {
        if (arr2.array_size() != length_) {
            return false;
        }
        if (length_ == 0) {
            return true;
        }
        if (!arr2.same_type()) {
            return false;
        }
        if (has_invalid_element_) {
            // TODO(SpadeA): support nullable proto::plan::Array constants.
            return false;
        }
        const auto expected_val_case =
            array_detail::ExpectedLiteralValCase(element_type_);
        switch (element_type_) {
            case DataType::BOOL: {
                for (int i = 0; i < length_; i++) {
                    if (arr2.array(i).val_case() != expected_val_case) {
                        return false;
                    }
                    auto val = get_raw_data<bool>(i);
                    if (val != arr2.array(i).bool_val()) {
                        return false;
                    }
                }
                return true;
            }
            case DataType::INT8:
            case DataType::INT16:
            case DataType::INT32: {
                for (int i = 0; i < length_; i++) {
                    if (arr2.array(i).val_case() != expected_val_case) {
                        return false;
                    }
                    auto val = get_raw_data<int>(i);
                    if (val != arr2.array(i).int64_val()) {
                        return false;
                    }
                }
                return true;
            }
            case DataType::INT64: {
                for (int i = 0; i < length_; i++) {
                    if (arr2.array(i).val_case() != expected_val_case) {
                        return false;
                    }
                    auto val = get_raw_data<int64_t>(i);
                    if (val != arr2.array(i).int64_val()) {
                        return false;
                    }
                }
                return true;
            }
            case DataType::FLOAT: {
                for (int i = 0; i < length_; i++) {
                    if (arr2.array(i).val_case() != expected_val_case) {
                        return false;
                    }
                    auto val = get_raw_data<float>(i);
                    if (val != static_cast<float>(arr2.array(i).float_val())) {
                        return false;
                    }
                }
                return true;
            }
            case DataType::DOUBLE: {
                for (int i = 0; i < length_; i++) {
                    if (arr2.array(i).val_case() != expected_val_case) {
                        return false;
                    }
                    auto val = get_raw_data<double>(i);
                    if (val != arr2.array(i).float_val()) {
                        return false;
                    }
                }
                return true;
            }
            case DataType::VARCHAR:
            case DataType::STRING:
            case DataType::GEOMETRY: {
                for (int i = 0; i < length_; i++) {
                    if (arr2.array(i).val_case() != expected_val_case) {
                        return false;
                    }
                    auto val = get_raw_data<std::string>(i);
                    if (val != arr2.array(i).string_val()) {
                        return false;
                    }
                }
                return true;
            }
            default:
                return false;
        }
    }

 private:
    template <typename T>
    T
    get_raw_data(const int index) const {
        AssertInfo(index >= 0 && index < length_,
                   "index out of range, index={}, length={}",
                   index,
                   length_);
        if constexpr (std::is_same_v<T, std::string> ||
                      std::is_same_v<T, std::string_view>) {
            size_t element_length =
                (index == length_ - 1)
                    ? size_ - offsets_ptr_[length_ - 1]
                    : offsets_ptr_[index + 1] - offsets_ptr_[index];
            return T(data_.get() + offsets_ptr_[index], element_length);
        }
        if constexpr (std::is_same_v<T, int> || std::is_same_v<T, int64_t> ||
                      std::is_same_v<T, int8_t> || std::is_same_v<T, int16_t> ||
                      std::is_same_v<T, float> || std::is_same_v<T, double>) {
            switch (element_type_) {
                case DataType::INT8:
                case DataType::INT16:
                case DataType::INT32:
                    return static_cast<T>(
                        reinterpret_cast<int32_t*>(data_.get())[index]);
                case DataType::INT64:
                    return static_cast<T>(
                        reinterpret_cast<int64_t*>(data_.get())[index]);
                case DataType::FLOAT:
                    return static_cast<T>(
                        reinterpret_cast<float*>(data_.get())[index]);
                case DataType::DOUBLE:
                    return static_cast<T>(
                        reinterpret_cast<double*>(data_.get())[index]);
                default:
                    ThrowInfo(Unsupported,
                              "unsupported element type for array");
            }
        }
        return reinterpret_cast<T*>(data_.get())[index];
    }

    void
    init_element_valid_data(const TargetBitmapView& element_valid_data) {
        if (!element_nullable_) {
            AssertInfo(element_valid_data.size() == 0,
                       "non-element-nullable array cannot carry element valid "
                       "data, bitmap_length={}",
                       element_valid_data.size());
            return;
        }

        AssertInfo(element_valid_data.size() == length_,
                   "element valid data bitmap length must equal array logical "
                   "length, bitmap_length={}, array_length={}",
                   element_valid_data.size(),
                   length_);
        element_valid_data_ = TargetBitmap(element_valid_data);
        has_invalid_element_ = !element_valid_data_.all();
    }

    void
    init_element_valid_data(
        const google::protobuf::RepeatedField<bool>& element_valid_data) {
        if (!element_nullable_) {
            return;
        }

        AssertInfo(element_valid_data.size() == length_,
                   "element valid data length must equal array logical "
                   "length, valid_data_size={}, array_length={}",
                   element_valid_data.size(),
                   length_);

        element_valid_data_ = TargetBitmap(length_, false);

        for (int i = 0; i < length_; ++i) {
            if (element_valid_data.Get(i)) {
                element_valid_data_.set(i);
            }
        }
        has_invalid_element_ = !element_valid_data_.all();
    }

    std::unique_ptr<char[]> data_{nullptr};
    int length_ = 0;
    int size_ = 0;
    DataType element_type_ = DataType::NONE;
    std::unique_ptr<uint32_t[]> offsets_ptr_{nullptr};
    bool element_nullable_ = false;
    // TODO: TargetBitmap currently adds 32 bytes per row on 64-bit builds,
    // including for non-element-nullable arrays. If this becomes an issue, use
    // std::unique_ptr<uint64_t[]> for the validity bitmap or move nullable
    // storage into a dedicated NullableArray class.
    TargetBitmap element_valid_data_{};
    bool has_invalid_element_ = false;
};

class ArrayView {
 public:
    ArrayView() = default;

    ArrayView(const ArrayView& other)
        : data_(other.data_),
          length_(other.length_),
          size_(other.size_),
          element_type_(other.element_type_),
          offsets_ptr_(other.offsets_ptr_),
          element_nullable_(other.element_nullable_),
          element_valid_data_(other.element_valid_data_),
          has_invalid_element_(other.has_invalid_element_) {
        AssertInfo(data_ != nullptr || (length_ == 0 && size_ == 0),
                   "data pointer for non-empty ArrayView cannot be nullptr");
        if (length_ > 0 && IsVariableDataType(element_type_)) {
            AssertInfo(offsets_ptr_ != nullptr,
                       "for array with variable length elements, offsets_ptr "
                       "must not be nullptr");
        }
    }

    ArrayView(char* data,
              int len,
              size_t size,
              DataType element_type,
              uint32_t* offsets_ptr)
        : ArrayView(data,
                    len,
                    size,
                    element_type,
                    offsets_ptr,
                    TargetBitmapView(),
                    false) {
    }

    ArrayView(char* data,
              int len,
              size_t size,
              DataType element_type,
              uint32_t* offsets_ptr,
              const TargetBitmapView& element_valid_data,
              bool element_nullable)
        : data_(data),
          length_(len),
          size_(size),
          element_type_(element_type),
          offsets_ptr_(offsets_ptr),
          element_nullable_(element_nullable),
          element_valid_data_(element_valid_data) {
        if (element_nullable_) {
            AssertInfo(
                element_valid_data_.size() == length_,
                "element valid data bitmap length must equal array logical "
                "length, bitmap_length={}, array_length={}",
                element_valid_data_.size(),
                length_);
            has_invalid_element_ = !element_valid_data_.all();
        } else {
            AssertInfo(element_valid_data_.size() == 0,
                       "non-element-nullable array cannot carry element valid "
                       "data, bitmap_length={}",
                       element_valid_data_.size());
        }
        AssertInfo(data != nullptr || (length_ == 0 && size_ == 0),
                   "data pointer for non-empty ArrayView cannot be nullptr");
        if (length_ > 0 && IsVariableDataType(element_type_)) {
            AssertInfo(offsets_ptr != nullptr,
                       "for array with variable length elements, offsets_ptr "
                       "must not be nullptr");
        }
    }

    template <typename T>
    T
    get_data_unchecked(const int index) const {
        // Reads raw payload; caller must handle element validity separately.
        return get_raw_data<T>(index);
    }

    bool
    is_element_valid(int index) const {
        AssertInfo(index >= 0 && index < length_,
                   "index out of range, index={}, length={}",
                   index,
                   length_);
        if (!element_nullable_) {
            return true;
        }
        return element_valid_data_[index];
    }

    void
    output_data(ScalarFieldProto& data_array) const {
        switch (element_type_) {
            case DataType::BOOL: {
                data_array.mutable_bool_data()->mutable_data()->Reserve(
                    length_);
                for (int j = 0; j < length_; ++j) {
                    auto element = get_raw_data<bool>(j);
                    data_array.mutable_bool_data()->add_data(element);
                }
                break;
            }
            case DataType::INT8:
            case DataType::INT16:
            case DataType::INT32: {
                data_array.mutable_int_data()->mutable_data()->Reserve(length_);
                for (int j = 0; j < length_; ++j) {
                    auto element = get_raw_data<int>(j);
                    data_array.mutable_int_data()->add_data(element);
                }
                break;
            }
            case DataType::INT64: {
                data_array.mutable_long_data()->mutable_data()->Reserve(
                    length_);
                for (int j = 0; j < length_; ++j) {
                    auto element = get_raw_data<int64_t>(j);
                    data_array.mutable_long_data()->add_data(element);
                }
                break;
            }
            case DataType::STRING:
            case DataType::VARCHAR: {
                data_array.mutable_string_data()->mutable_data()->Reserve(
                    length_);
                for (int j = 0; j < length_; ++j) {
                    auto element = get_raw_data<std::string_view>(j);
                    data_array.mutable_string_data()->add_data(element.data(),
                                                               element.size());
                }
                break;
            }
            case DataType::FLOAT: {
                data_array.mutable_float_data()->mutable_data()->Reserve(
                    length_);
                for (int j = 0; j < length_; ++j) {
                    auto element = get_raw_data<float>(j);
                    data_array.mutable_float_data()->add_data(element);
                }
                break;
            }
            case DataType::DOUBLE: {
                data_array.mutable_double_data()->mutable_data()->Reserve(
                    length_);
                for (int j = 0; j < length_; ++j) {
                    auto element = get_raw_data<double>(j);
                    data_array.mutable_double_data()->add_data(element);
                }
                break;
            }
            case DataType::GEOMETRY: {
                data_array.mutable_geometry_data()->mutable_data()->Reserve(
                    length_);
                for (int j = 0; j < length_; ++j) {
                    auto element = get_raw_data<std::string_view>(j);
                    data_array.mutable_geometry_data()->add_data(
                        element.data(), element.size());
                }
                break;
            }
            default: {
                // empty array
            }
        }
        if (element_nullable_) {
            data_array.mutable_valid_data()->Reserve(length_);
            for (int i = 0; i < length_; ++i) {
                data_array.add_valid_data(is_element_valid(i));
            }
        }
    }

    void
    output_data(Array& array) const {
        array = Array(data_,
                      length_,
                      static_cast<size_t>(size_),
                      element_type_,
                      offsets_ptr_,
                      element_valid_data_,
                      element_nullable_);
    }

    ScalarFieldProto
    output_data() const {
        ScalarFieldProto data_array;
        output_data(data_array);
        return data_array;
    }

    int
    length() const {
        return length_;
    }

    const void*
    data() const {
        return data_;
    }

    bool
    is_same_array(const proto::plan::Array& arr2) const {
        if (arr2.array_size() != length_) {
            return false;
        }
        if (!arr2.same_type()) {
            return false;
        }
        if (has_invalid_element_) {
            // TODO(SpadeA): support nullable proto::plan::Array constants.
            return false;
        }
        const auto expected_val_case =
            array_detail::ExpectedLiteralValCase(element_type_);
        switch (element_type_) {
            case DataType::BOOL: {
                for (int i = 0; i < length_; i++) {
                    if (arr2.array(i).val_case() != expected_val_case) {
                        return false;
                    }
                    auto val = get_raw_data<bool>(i);
                    if (val != arr2.array(i).bool_val()) {
                        return false;
                    }
                }
                return true;
            }
            case DataType::INT8:
            case DataType::INT16:
            case DataType::INT32: {
                for (int i = 0; i < length_; i++) {
                    if (arr2.array(i).val_case() != expected_val_case) {
                        return false;
                    }
                    auto val = get_raw_data<int>(i);
                    if (val != arr2.array(i).int64_val()) {
                        return false;
                    }
                }
                return true;
            }
            case DataType::INT64: {
                for (int i = 0; i < length_; i++) {
                    if (arr2.array(i).val_case() != expected_val_case) {
                        return false;
                    }
                    auto val = get_raw_data<int64_t>(i);
                    if (val != arr2.array(i).int64_val()) {
                        return false;
                    }
                }
                return true;
            }
            case DataType::FLOAT: {
                for (int i = 0; i < length_; i++) {
                    if (arr2.array(i).val_case() != expected_val_case) {
                        return false;
                    }
                    auto val = get_raw_data<float>(i);
                    if (val != static_cast<float>(arr2.array(i).float_val())) {
                        return false;
                    }
                }
                return true;
            }
            case DataType::DOUBLE: {
                for (int i = 0; i < length_; i++) {
                    if (arr2.array(i).val_case() != expected_val_case) {
                        return false;
                    }
                    auto val = get_raw_data<double>(i);
                    if (val != arr2.array(i).float_val()) {
                        return false;
                    }
                }
                return true;
            }
            case DataType::VARCHAR:
            case DataType::STRING:
            case DataType::GEOMETRY: {
                for (int i = 0; i < length_; i++) {
                    if (arr2.array(i).val_case() != expected_val_case) {
                        return false;
                    }
                    auto val = get_raw_data<std::string>(i);
                    if (val != arr2.array(i).string_val()) {
                        return false;
                    }
                }
                return true;
            }
            default:
                return length_ == 0;
        }
    }

 private:
    template <typename T>
    T
    get_raw_data(const int index) const {
        AssertInfo(index >= 0 && index < length_,
                   "index out of range, index={}, length={}",
                   index,
                   length_);

        if constexpr (std::is_same_v<T, std::string> ||
                      std::is_same_v<T, std::string_view>) {
            size_t element_length =
                (index == length_ - 1)
                    ? size_ - offsets_ptr_[length_ - 1]
                    : offsets_ptr_[index + 1] - offsets_ptr_[index];
            return T(data_ + offsets_ptr_[index], element_length);
        }
        if constexpr (std::is_same_v<T, int> || std::is_same_v<T, int64_t> ||
                      std::is_same_v<T, int8_t> || std::is_same_v<T, int16_t> ||
                      std::is_same_v<T, float> || std::is_same_v<T, double>) {
            switch (element_type_) {
                case DataType::INT8:
                case DataType::INT16:
                case DataType::INT32:
                    return static_cast<T>(
                        reinterpret_cast<int32_t*>(data_)[index]);
                case DataType::INT64:
                    return static_cast<T>(
                        reinterpret_cast<int64_t*>(data_)[index]);
                case DataType::FLOAT:
                    return static_cast<T>(
                        reinterpret_cast<float*>(data_)[index]);
                case DataType::DOUBLE:
                    return static_cast<T>(
                        reinterpret_cast<double*>(data_)[index]);
                default:
                    ThrowInfo(Unsupported,
                              "unsupported element type for array");
            }
        }
        return reinterpret_cast<T*>(data_)[index];
    }

    char* data_{nullptr};
    int length_ = 0;
    int size_ = 0;
    DataType element_type_ = DataType::NONE;

    //offsets ptr
    uint32_t* offsets_ptr_{nullptr};
    bool element_nullable_ = false;
    TargetBitmapView element_valid_data_{};
    bool has_invalid_element_ = false;
};

}  // namespace milvus
