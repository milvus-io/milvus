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
                offsets_.reserve(length_);
                for (int i = 0; i < length_; ++i) {
                    offsets_.push_back(size_);
                    size_ += field_data.string_data().data(i).size();
                }

                data_ = new char[size_];
                for (int i = 0; i < length_; ++i) {
                    std::copy_n(field_data.string_data().data(i).data(),
                                field_data.string_data().data(i).size(),
                                data_ + offsets_[i]);
                }
                break;
            }
            default: {
                // empty array
            }
        }
    }

    Array(char* data,
          size_t size,
          DataType element_type,
          std::vector<uint64_t>&& element_offsets)
        : size_(size),
          offsets_(std::move(element_offsets)),
          element_type_(element_type) {
        delete[] data_;
        data_ = new char[size];
        std::copy(data, data + size, data_);
        if (datatype_is_variable(element_type_)) {
            length_ = offsets_.size();
        } else {
            // int8, int16, int32 are all promoted to int32
            if (element_type_ == DataType::INT8 ||
                element_type_ == DataType::INT16) {
                length_ = size / sizeof(int32_t);
            } else {
                length_ = size / datatype_sizeof(element_type_);
            }
        }
    }

    Array(const Array& array) noexcept
        : length_{array.length_},
          size_{array.size_},
          element_type_{array.element_type_} {
        delete[] data_;
        data_ = new char[array.size_];
        std::copy(array.data_, array.data_ + array.size_, data_);
        offsets_ = array.offsets_;
    }

    Array&
    operator=(const Array& array) {
        delete[] data_;

        data_ = new char[array.size_];
        std::copy(array.data_, array.data_ + array.size_, data_);
        length_ = array.length_;
        size_ = array.size_;
        offsets_ = array.offsets_;
        element_type_ = array.element_type_;
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
        AssertInfo(
            index >= 0 && index < length_,
            fmt::format(
                "index out of range, index={}, length={}", index, length_));
        if constexpr (std::is_same_v<T, std::string> ||
                      std::is_same_v<T, std::string_view>) {
            size_t element_length = (index == length_ - 1)
                                        ? size_ - offsets_.back()
                                        : offsets_[index + 1] - offsets_[index];
            return T(data_ + offsets_[index], element_length);
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

    const std::vector<uint64_t>&
    get_offsets() const {
        return offsets_;
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
    std::vector<uint64_t> offsets_{};
    DataType element_type_ = DataType::NONE;
};

class ArrayView {
 public:
    ArrayView() = default;

    ArrayView(char* data,
              size_t size,
              DataType element_type,
              std::vector<uint64_t>&& element_offsets)
        : size_(size),
          element_type_(element_type),
          offsets_(std::move(element_offsets)) {
        data_ = data;
        if (datatype_is_variable(element_type_)) {
            length_ = offsets_.size();
        } else {
            // int8, int16, int32 are all promoted to int32
            if (element_type_ == DataType::INT8 ||
                element_type_ == DataType::INT16) {
                length_ = size / sizeof(int32_t);
            } else {
                length_ = size / datatype_sizeof(element_type_);
            }
        }
    }

    template <typename T>
    T
    get_data(const int index) const {
        AssertInfo(
            index >= 0 && index < length_,
            fmt::format(
                "index out of range, index={}, length={}", index, length_));

        if constexpr (std::is_same_v<T, std::string> ||
                      std::is_same_v<T, std::string_view>) {
            size_t element_length = (index == length_ - 1)
                                        ? size_ - offsets_.back()
                                        : offsets_[index + 1] - offsets_[index];
            return T(data_ + offsets_[index], element_length);
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
    std::vector<uint64_t> offsets_{};
    DataType element_type_ = DataType::NONE;
};

}  // namespace milvus
