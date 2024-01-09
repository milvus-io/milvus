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

#include "common/FieldData.h"

#include "arrow/array/array_binary.h"
#include "common/Array.h"
#include "common/EasyAssert.h"
#include "common/Exception.h"
#include "common/FieldDataInterface.h"
#include "common/Json.h"
#include "simdjson/padded_string.h"

namespace milvus {

template <typename Type, bool is_scalar>
void
FieldDataImpl<Type, is_scalar>::FillFieldData(const void* source,
                                              ssize_t element_count) {
    if (element_count == 0) {
        return;
    }

    std::lock_guard lck(tell_mutex_);
    if (length_ + element_count > get_num_rows()) {
        resize_field_data(length_ + element_count);
    }
    std::copy_n(static_cast<const Type*>(source),
                element_count * dim_,
                field_data_.data() + length_ * dim_);
    length_ += element_count;
}

template <typename ArrayType, arrow::Type::type ArrayDataType>
std::pair<const void*, int64_t>
GetDataInfoFromArray(const std::shared_ptr<arrow::Array> array) {
    AssertInfo(array->type()->id() == ArrayDataType,
               "inconsistent data type, expected {}, actual {}",
               ArrayDataType,
               array->type()->id());
    auto typed_array = std::dynamic_pointer_cast<ArrayType>(array);
    auto element_count = array->length();

    return std::make_pair(typed_array->raw_values(), element_count);
}

template <typename Type, bool is_scalar>
void
FieldDataImpl<Type, is_scalar>::FillFieldData(
    const std::shared_ptr<arrow::Array> array) {
    AssertInfo(array != nullptr, "null arrow array");
    auto element_count = array->length();
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
            auto array_info =
                GetDataInfoFromArray<arrow::Int8Array, arrow::Type::type::INT8>(
                    array);
            return FillFieldData(array_info.first, array_info.second);
        }
        case DataType::INT16: {
            auto array_info =
                GetDataInfoFromArray<arrow::Int16Array,
                                     arrow::Type::type::INT16>(array);
            return FillFieldData(array_info.first, array_info.second);
        }
        case DataType::INT32: {
            auto array_info =
                GetDataInfoFromArray<arrow::Int32Array,
                                     arrow::Type::type::INT32>(array);
            return FillFieldData(array_info.first, array_info.second);
        }
        case DataType::INT64: {
            auto array_info =
                GetDataInfoFromArray<arrow::Int64Array,
                                     arrow::Type::type::INT64>(array);
            return FillFieldData(array_info.first, array_info.second);
        }
        case DataType::FLOAT: {
            auto array_info =
                GetDataInfoFromArray<arrow::FloatArray,
                                     arrow::Type::type::FLOAT>(array);
            return FillFieldData(array_info.first, array_info.second);
        }
        case DataType::DOUBLE: {
            auto array_info =
                GetDataInfoFromArray<arrow::DoubleArray,
                                     arrow::Type::type::DOUBLE>(array);
            return FillFieldData(array_info.first, array_info.second);
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
        case DataType::JSON: {
            AssertInfo(array->type()->id() == arrow::Type::type::BINARY,
                       "inconsistent data type");
            auto json_array =
                std::dynamic_pointer_cast<arrow::BinaryArray>(array);
            std::vector<Json> values(element_count);
            for (size_t index = 0; index < element_count; ++index) {
                values[index] =
                    Json(simdjson::padded_string(json_array->GetString(index)));
            }
            return FillFieldData(values.data(), element_count);
        }
        case DataType::ARRAY: {
            auto array_array =
                std::dynamic_pointer_cast<arrow::BinaryArray>(array);
            std::vector<Array> values(element_count);
            for (size_t index = 0; index < element_count; ++index) {
                ScalarArray field_data;
                field_data.ParseFromString(array_array->GetString(index));
                values[index] = Array(field_data);
            }
            return FillFieldData(values.data(), element_count);
        }
        case DataType::VECTOR_FLOAT:
        case DataType::VECTOR_FLOAT16:
        case DataType::VECTOR_BFLOAT16:
        case DataType::VECTOR_BINARY: {
            auto array_info =
                GetDataInfoFromArray<arrow::FixedSizeBinaryArray,
                                     arrow::Type::type::FIXED_SIZE_BINARY>(
                    array);
            return FillFieldData(array_info.first, array_info.second);
        }
        default: {
            throw SegcoreError(DataTypeInvalid,
                               GetName() + "::FillFieldData" +
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
template class FieldDataImpl<Json, true>;
template class FieldDataImpl<Array, true>;

// vector data
template class FieldDataImpl<int8_t, false>;
template class FieldDataImpl<float, false>;
template class FieldDataImpl<float16, false>;
template class FieldDataImpl<bfloat16, false>;

FieldDataPtr
InitScalarFieldData(const DataType& type, int64_t cap_rows) {
    switch (type) {
        case DataType::BOOL:
            return std::make_shared<FieldData<bool>>(type, cap_rows);
        case DataType::INT8:
            return std::make_shared<FieldData<int8_t>>(type, cap_rows);
        case DataType::INT16:
            return std::make_shared<FieldData<int16_t>>(type, cap_rows);
        case DataType::INT32:
            return std::make_shared<FieldData<int32_t>>(type, cap_rows);
        case DataType::INT64:
            return std::make_shared<FieldData<int64_t>>(type, cap_rows);
        case DataType::FLOAT:
            return std::make_shared<FieldData<float>>(type, cap_rows);
        case DataType::DOUBLE:
            return std::make_shared<FieldData<double>>(type, cap_rows);
        case DataType::STRING:
        case DataType::VARCHAR:
            return std::make_shared<FieldData<std::string>>(type, cap_rows);
        case DataType::JSON:
            return std::make_shared<FieldData<Json>>(type, cap_rows);
        default:
            throw NotSupportedDataTypeException(
                "InitScalarFieldData not support data type " +
                datatype_name(type));
    }
}

}  // namespace milvus
