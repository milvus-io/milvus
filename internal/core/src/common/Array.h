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

#include <arrow/array.h>
#include <arrow/array/builder_primitive.h>
#include <fmt/core.h>

#include "FieldMeta.h"
#include "Types.h"

namespace milvus {

class Array {
 public:
    Array() = default;

    ~Array() {
        delete[] data_;
        if (offsets_ptr_) {
            // only deallocate offsets for string type array
            delete[] offsets_ptr_;
        }
    }

    Array(char* data,
          int len,
          size_t size,
          DataType element_type,
          const uint32_t* offsets_ptr)
        : size_(size), length_(len), element_type_(element_type) {
        data_ = new char[size];
        std::copy(data, data + size, data_);
        if (IsVariableDataType(element_type)) {
            AssertInfo(offsets_ptr != nullptr,
                       "For variable type elements in array, offsets_ptr must "
                       "be non-null");
            offsets_ptr_ = new uint32_t[len];
            std::copy(offsets_ptr, offsets_ptr + len, offsets_ptr_);
        }
    }

    explicit Array(const ScalarArray& field_data) {
        switch (field_data.data_case()) {
            case ScalarArray::kBoolData: {
                element_type_ = DataType::BOOL;
                length_ = field_data.bool_data().data().size();
                auto data = new bool[length_];
                size_ = length_;
                for (int i = 0; i < length_; ++i) {
                    data[i] = field_data.bool_data().data(i);
                }
                data_ = reinterpret_cast<char*>(data);
                break;
            }
            case ScalarArray::kIntData: {
                element_type_ = DataType::INT32;
                length_ = field_data.int_data().data().size();
                size_ = length_ * sizeof(int32_t);
                data_ = new char[size_];
                for (int i = 0; i < length_; ++i) {
                    reinterpret_cast<int*>(data_)[i] =
                        field_data.int_data().data(i);
                }
                break;
            }
            case ScalarArray::kLongData: {
                element_type_ = DataType::INT64;
                length_ = field_data.long_data().data().size();
                size_ = length_ * sizeof(int64_t);
                data_ = new char[size_];
                for (int i = 0; i < length_; ++i) {
                    reinterpret_cast<int64_t*>(data_)[i] =
                        field_data.long_data().data(i);
                }
                break;
            }
            case ScalarArray::kFloatData: {
                element_type_ = DataType::FLOAT;
                length_ = field_data.float_data().data().size();
                size_ = length_ * sizeof(float);
                data_ = new char[size_];
                for (int i = 0; i < length_; ++i) {
                    reinterpret_cast<float*>(data_)[i] =
                        field_data.float_data().data(i);
                }
                break;
            }
            case ScalarArray::kDoubleData: {
                element_type_ = DataType::DOUBLE;
                length_ = field_data.double_data().data().size();
                size_ = length_ * sizeof(double);
                data_ = new char[size_];
                for (int i = 0; i < length_; ++i) {
                    reinterpret_cast<double*>(data_)[i] =
                        field_data.double_data().data(i);
                }
                break;
            }
            case ScalarArray::kStringData: {
                element_type_ = DataType::STRING;
                length_ = field_data.string_data().data().size();
                offsets_ptr_ = new uint32_t[length_];
                for (int i = 0; i < length_; ++i) {
                    offsets_ptr_[i] = size_;
                    size_ +=
                        field_data.string_data()
                            .data(i)
                            .size();  //type risk here between uint32_t vs size_t
                }
                data_ = new char[size_];
                for (int i = 0; i < length_; ++i) {
                    std::copy_n(field_data.string_data().data(i).data(),
                                field_data.string_data().data(i).size(),
                                data_ + offsets_ptr_[i]);
                }
                break;
            }
            default: {
                // empty array
            }
        }
    }

    Array(const Array& array) noexcept
        : length_{array.length_},
          size_{array.size_},
          element_type_{array.element_type_} {
        data_ = new char[array.size_];
        std::copy(array.data_, array.data_ + array.size_, data_);
        if (IsVariableDataType(array.element_type_)) {
            AssertInfo(array.get_offsets_data() != nullptr,
                       "for array with variable length elements, offsets_ptr"
                       "must not be nullptr");
            offsets_ptr_ = new uint32_t[length_];
            std::copy_n(array.get_offsets_data(), array.length(), offsets_ptr_);
        }
    }

    Array&
    operator=(const Array& array) {
        delete[] data_;
        if (offsets_ptr_) {
            delete[] offsets_ptr_;
        }
        length_ = array.length_;
        size_ = array.size_;
        element_type_ = array.element_type_;
        data_ = new char[size_];
        std::copy(array.data_, array.data_ + size_, data_);
        if (IsVariableDataType(element_type_)) {
            AssertInfo(array.get_offsets_data() != nullptr,
                       "for array with variable length elements, offsets_ptr"
                       "must not be nullptr");
            offsets_ptr_ = new uint32_t[length_];
            std::copy_n(array.get_offsets_data(), array.length(), offsets_ptr_);
        }
        return *this;
    }

    bool
    operator==(const Array& arr) const {
        if (element_type_ != arr.element_type_) {
            return false;
        }
        if (length_ != arr.length_) {
            return false;
        }
        if (length_ == 0) {
            return true;
        }
        switch (element_type_) {
            case DataType::INT64: {
                for (int i = 0; i < length_; ++i) {
                    if (get_data<int64_t>(i) != arr.get_data<int64_t>(i)) {
                        return false;
                    }
                }
                return true;
            }
            case DataType::BOOL: {
                for (int i = 0; i < length_; ++i) {
                    if (get_data<bool>(i) != arr.get_data<bool>(i)) {
                        return false;
                    }
                }
                return true;
            }
            case DataType::DOUBLE: {
                for (int i = 0; i < length_; ++i) {
                    if (get_data<double>(i) != arr.get_data<double>(i)) {
                        return false;
                    }
                }
                return true;
            }
            case DataType::FLOAT: {
                for (int i = 0; i < length_; ++i) {
                    if (get_data<float>(i) != arr.get_data<float>(i)) {
                        return false;
                    }
                }
                return true;
            }
            case DataType::INT32:
            case DataType::INT16:
            case DataType::INT8: {
                for (int i = 0; i < length_; ++i) {
                    if (get_data<int>(i) != arr.get_data<int>(i)) {
                        return false;
                    }
                }
                return true;
            }
            case DataType::STRING:
            case DataType::VARCHAR: {
                for (int i = 0; i < length_; ++i) {
                    if (get_data<std::string_view>(i) !=
                        arr.get_data<std::string_view>(i)) {
                        return false;
                    }
                }
                return true;
            }
            default:
                PanicInfo(Unsupported, "unsupported element type for array");
        }
    }

    template <typename T>
    T
    get_data(const int index) const {
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
                    PanicInfo(Unsupported,
                              "unsupported element type for array");
            }
        }
        return reinterpret_cast<T*>(data_)[index];
    }

    uint32_t*
    get_offsets_data() const {
        return offsets_ptr_;
    }

    ScalarArray
    output_data() const {
        ScalarArray data_array;
        switch (element_type_) {
            case DataType::BOOL: {
                for (int j = 0; j < length_; ++j) {
                    auto element = get_data<bool>(j);
                    data_array.mutable_bool_data()->add_data(element);
                }
                break;
            }
            case DataType::INT8:
            case DataType::INT16:
            case DataType::INT32: {
                for (int j = 0; j < length_; ++j) {
                    auto element = get_data<int>(j);
                    data_array.mutable_int_data()->add_data(element);
                }
                break;
            }
            case DataType::INT64: {
                for (int j = 0; j < length_; ++j) {
                    auto element = get_data<int64_t>(j);
                    data_array.mutable_long_data()->add_data(element);
                }
                break;
            }
            case DataType::STRING:
            case DataType::VARCHAR: {
                for (int j = 0; j < length_; ++j) {
                    auto element = get_data<std::string>(j);
                    data_array.mutable_string_data()->add_data(element);
                }
                break;
            }
            case DataType::FLOAT: {
                for (int j = 0; j < length_; ++j) {
                    auto element = get_data<float>(j);
                    data_array.mutable_float_data()->add_data(element);
                }
                break;
            }
            case DataType::DOUBLE: {
                for (int j = 0; j < length_; ++j) {
                    auto element = get_data<double>(j);
                    data_array.mutable_double_data()->add_data(element);
                }
                break;
            }
            default: {
                // empty array
            }
        }
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
        return data_;
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
        switch (element_type_) {
            case DataType::BOOL: {
                for (int i = 0; i < length_; i++) {
                    auto val = get_data<bool>(i);
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
                    auto val = get_data<int>(i);
                    if (val != arr2.array(i).int64_val()) {
                        return false;
                    }
                }
                return true;
            }
            case DataType::INT64: {
                for (int i = 0; i < length_; i++) {
                    auto val = get_data<int64_t>(i);
                    if (val != arr2.array(i).int64_val()) {
                        return false;
                    }
                }
                return true;
            }
            case DataType::FLOAT: {
                for (int i = 0; i < length_; i++) {
                    auto val = get_data<float>(i);
                    if (val != arr2.array(i).float_val()) {
                        return false;
                    }
                }
                return true;
            }
            case DataType::DOUBLE: {
                for (int i = 0; i < length_; i++) {
                    auto val = get_data<double>(i);
                    if (val != arr2.array(i).float_val()) {
                        return false;
                    }
                }
                return true;
            }
            case DataType::VARCHAR:
            case DataType::STRING: {
                for (int i = 0; i < length_; i++) {
                    auto val = get_data<std::string>(i);
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
    char* data_{nullptr};
    int length_ = 0;
    int size_ = 0;
    DataType element_type_ = DataType::NONE;
    uint32_t* offsets_ptr_{nullptr};
};

class ArrayView {
 public:
    ArrayView() = default;

    ArrayView(const ArrayView& other)
        : data_(other.data_),
          length_(other.length_),
          size_(other.size_),
          element_type_(other.element_type_),
          offsets_ptr_(other.offsets_ptr_) {
        AssertInfo(data_ != nullptr,
                   "data pointer for ArrayView cannot be nullptr");
        if (IsVariableDataType(element_type_)) {
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
        : data_(data),
          length_(len),
          size_(size),
          element_type_(element_type),
          offsets_ptr_(offsets_ptr) {
        AssertInfo(data != nullptr,
                   "data pointer for ArrayView cannot be nullptr");
        if (IsVariableDataType(element_type_)) {
            AssertInfo(offsets_ptr != nullptr,
                       "for array with variable length elements, offsets_ptr "
                       "must not be nullptr");
        }
    }

    template <typename T>
    T
    get_data(const int index) const {
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
                    PanicInfo(Unsupported,
                              "unsupported element type for array");
            }
        }
        return reinterpret_cast<T*>(data_)[index];
    }

    ScalarArray
    output_data() const {
        ScalarArray data_array;
        switch (element_type_) {
            case DataType::BOOL: {
                for (int j = 0; j < length_; ++j) {
                    auto element = get_data<bool>(j);
                    data_array.mutable_bool_data()->add_data(element);
                }
                break;
            }
            case DataType::INT8:
            case DataType::INT16:
            case DataType::INT32: {
                for (int j = 0; j < length_; ++j) {
                    auto element = get_data<int>(j);
                    data_array.mutable_int_data()->add_data(element);
                }
                break;
            }
            case DataType::INT64: {
                for (int j = 0; j < length_; ++j) {
                    auto element = get_data<int64_t>(j);
                    data_array.mutable_long_data()->add_data(element);
                }
                break;
            }
            case DataType::STRING:
            case DataType::VARCHAR: {
                for (int j = 0; j < length_; ++j) {
                    auto element = get_data<std::string>(j);
                    data_array.mutable_string_data()->add_data(element);
                }
                break;
            }
            case DataType::FLOAT: {
                for (int j = 0; j < length_; ++j) {
                    auto element = get_data<float>(j);
                    data_array.mutable_float_data()->add_data(element);
                }
                break;
            }
            case DataType::DOUBLE: {
                for (int j = 0; j < length_; ++j) {
                    auto element = get_data<double>(j);
                    data_array.mutable_double_data()->add_data(element);
                }
                break;
            }
            default: {
                // empty array
            }
        }
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
        switch (element_type_) {
            case DataType::BOOL: {
                for (int i = 0; i < length_; i++) {
                    auto val = get_data<bool>(i);
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
                    auto val = get_data<int>(i);
                    if (val != arr2.array(i).int64_val()) {
                        return false;
                    }
                }
                return true;
            }
            case DataType::INT64: {
                for (int i = 0; i < length_; i++) {
                    auto val = get_data<int64_t>(i);
                    if (val != arr2.array(i).int64_val()) {
                        return false;
                    }
                }
                return true;
            }
            case DataType::FLOAT: {
                for (int i = 0; i < length_; i++) {
                    auto val = get_data<float>(i);
                    if (val != arr2.array(i).float_val()) {
                        return false;
                    }
                }
                return true;
            }
            case DataType::DOUBLE: {
                for (int i = 0; i < length_; i++) {
                    auto val = get_data<double>(i);
                    if (val != arr2.array(i).float_val()) {
                        return false;
                    }
                }
                return true;
            }
            case DataType::VARCHAR:
            case DataType::STRING: {
                for (int i = 0; i < length_; i++) {
                    auto val = get_data<std::string>(i);
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
    char* data_{nullptr};
    int length_ = 0;
    int size_ = 0;
    DataType element_type_ = DataType::NONE;

    //offsets ptr
    uint32_t* offsets_ptr_{nullptr};
};

}  // namespace milvus
