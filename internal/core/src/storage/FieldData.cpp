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

#include "storage/FieldData.h"

namespace milvus::storage {

template <typename Type, bool is_scalar>
void
FieldDataImpl<Type, is_scalar>::FillFieldData(const void* source,
                                              ssize_t element_count) {
    AssertInfo(element_count % dim_ == 0, "invalid element count");
    if (element_count == 0) {
        return;
    }
    AssertInfo(field_data_.size() == 0, "no empty field vector");
    field_data_.resize(element_count);
    std::copy_n(
        static_cast<const Type*>(source), element_count, field_data_.data());
}

template <typename Type, bool is_scalar>
void
FieldDataImpl<Type, is_scalar>::FillFieldData(
    const std::shared_ptr<arrow::Array> array) {
    AssertInfo(array != nullptr, "null arrow array");
    auto element_count = array->length() * dim_;
    if (element_count == 0) {
        return;
    }
    switch (data_type_) {
        case DataType::BOOL: {
            AssertInfo(array->type()->id() == arrow::Type::type::BOOL,
                       "inconsistent data type");
            auto bool_array =
                std::dynamic_pointer_cast<arrow::BooleanArray>(array);
            FixedVector<bool> values(element_count);
            for (size_t index = 0; index < element_count; ++index) {
                values[index] = bool_array->Value(index);
            }
            return FillFieldData(values.data(), element_count);
        }
        case DataType::INT8: {
            AssertInfo(array->type()->id() == arrow::Type::type::INT8,
                       "inconsistent data type");
            auto int8_array =
                std::dynamic_pointer_cast<arrow::Int8Array>(array);
            return FillFieldData(int8_array->raw_values(), element_count);
        }
        case DataType::INT16: {
            AssertInfo(array->type()->id() == arrow::Type::type::INT16,
                       "inconsistent data type");
            auto int16_array =
                std::dynamic_pointer_cast<arrow::Int16Array>(array);
            return FillFieldData(int16_array->raw_values(), element_count);
        }
        case DataType::INT32: {
            AssertInfo(array->type()->id() == arrow::Type::type::INT32,
                       "inconsistent data type");
            auto int32_array =
                std::dynamic_pointer_cast<arrow::Int32Array>(array);
            return FillFieldData(int32_array->raw_values(), element_count);
        }
        case DataType::INT64: {
            AssertInfo(array->type()->id() == arrow::Type::type::INT64,
                       "inconsistent data type");
            auto int64_array =
                std::dynamic_pointer_cast<arrow::Int64Array>(array);
            return FillFieldData(int64_array->raw_values(), element_count);
        }
        case DataType::FLOAT: {
            AssertInfo(array->type()->id() == arrow::Type::type::FLOAT,
                       "inconsistent data type");
            auto float_array =
                std::dynamic_pointer_cast<arrow::FloatArray>(array);
            return FillFieldData(float_array->raw_values(), element_count);
        }
        case DataType::DOUBLE: {
            AssertInfo(array->type()->id() == arrow::Type::type::DOUBLE,
                       "inconsistent data type");
            auto double_array =
                std::dynamic_pointer_cast<arrow::DoubleArray>(array);
            return FillFieldData(double_array->raw_values(), element_count);
        }
        case DataType::STRING:
        case DataType::VARCHAR: {
            AssertInfo(array->type()->id() == arrow::Type::type::STRING,
                       "inconsistent data type");
            auto string_array =
                std::dynamic_pointer_cast<arrow::StringArray>(array);
            std::vector<std::string> values(element_count);
            for (size_t index = 0; index < element_count; ++index) {
                values[index] = string_array->GetString(index);
            }
            return FillFieldData(values.data(), element_count);
        }
        case DataType::VECTOR_FLOAT: {
            AssertInfo(
                array->type()->id() == arrow::Type::type::FIXED_SIZE_BINARY,
                "inconsistent data type");
            auto vector_array =
                std::dynamic_pointer_cast<arrow::FixedSizeBinaryArray>(array);
            return FillFieldData(vector_array->raw_values(), element_count);
        }
        case DataType::VECTOR_BINARY: {
            AssertInfo(
                array->type()->id() == arrow::Type::type::FIXED_SIZE_BINARY,
                "inconsistent data type");
            auto vector_array =
                std::dynamic_pointer_cast<arrow::FixedSizeBinaryArray>(array);
            return FillFieldData(vector_array->raw_values(), element_count);
        }
        default: {
            throw NotSupportedDataTypeException(GetName() + "::FillFieldData" +
                                                " not support data type " +
                                                datatype_name(data_type_));
        }
    }
}

// scalar data
template class FieldDataImpl<bool, true>;
template class FieldDataImpl<unsigned char, false>;
template class FieldDataImpl<int8_t, true>;
template class FieldDataImpl<int16_t, true>;
template class FieldDataImpl<int32_t, true>;
template class FieldDataImpl<int64_t, true>;
template class FieldDataImpl<float, true>;
template class FieldDataImpl<double, true>;
template class FieldDataImpl<std::string, true>;

// vector data
template class FieldDataImpl<int8_t, false>;
template class FieldDataImpl<float, false>;

}  // namespace milvus::storage
