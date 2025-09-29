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
#include <cstdint>

#include "arrow/array/array_binary.h"
#include "arrow/chunked_array.h"
#include "bitset/detail/element_wise.h"
#include "common/Array.h"
#include "common/EasyAssert.h"
#include "common/Exception.h"
#include "common/FieldDataInterface.h"
#include "common/Json.h"
#include "simdjson/padded_string.h"

namespace milvus {

template <typename Type, bool is_type_entire_row>
void
FieldDataImpl<Type, is_type_entire_row>::FillFieldData(const void* source,
                                                       ssize_t element_count) {
    AssertInfo(!nullable_,
               "need to fill valid_data, use the 3-argument version instead");

    if (element_count == 0) {
        return;
    }

    std::lock_guard lck(tell_mutex_);
    if (length_ + element_count > get_num_rows()) {
        resize_field_data(length_ + element_count);
    }
    std::copy_n(static_cast<const Type*>(source),
                element_count * dim_,
                data_.data() + length_ * dim_);
    length_ += element_count;
}

template <typename Type, bool is_type_entire_row>
void
FieldDataImpl<Type, is_type_entire_row>::FillFieldData(
    const void* field_data,
    const uint8_t* valid_data,
    ssize_t element_count,
    ssize_t offset) {
    AssertInfo(
        nullable_,
        "no need to fill valid_data, use the 2-argument version instead");
    if (element_count == 0) {
        return;
    }

    std::lock_guard lck(tell_mutex_);
    if (length_ + element_count > get_num_rows()) {
        resize_field_data(length_ + element_count);
    }
    std::copy_n(static_cast<const Type*>(field_data),
                element_count * dim_,
                data_.data() + length_ * dim_);

    // Note: if 'nullable == true` and valid_data is nullptr
    // means null_count == 0, will fill it with 0xFF
    if (valid_data != nullptr) {
        bitset::detail::ElementWiseBitsetPolicy<uint8_t>::op_copy(
            valid_data, offset, valid_data_.data(), length_, element_count);
    }

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

template <typename Type, bool is_type_entire_row>
void
FieldDataImpl<Type, is_type_entire_row>::FillFieldData(
    const std::shared_ptr<arrow::ChunkedArray> arrays) {
    AssertInfo(arrays != nullptr, "null arrow chunked array");
    auto element_count = arrays->length();
    if (element_count == 0) {
        return;
    }
    for (const auto& array : arrays->chunks()) {
        FillFieldData(array);
    }
}

template <typename Type, bool is_type_entire_row>
void
FieldDataImpl<Type, is_type_entire_row>::FillFieldData(
    const std::shared_ptr<arrow::Array> array) {
    AssertInfo(array != nullptr, "null arrow array");
    auto element_count = array->length();
    if (element_count == 0) {
        return;
    }
    null_count_ = array->null_count();
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
            if (nullable_) {
                return FillFieldData(values.data(),
                                     bool_array->null_bitmap_data(),
                                     element_count,
                                     bool_array->offset());
            }
            return FillFieldData(values.data(), element_count);
        }
        case DataType::INT8: {
            auto array_info =
                GetDataInfoFromArray<arrow::Int8Array, arrow::Type::type::INT8>(
                    array);
            if (nullable_) {
                return FillFieldData(array_info.first,
                                     array->null_bitmap_data(),
                                     element_count,
                                     array->offset());
            }
            return FillFieldData(array_info.first, array_info.second);
        }
        case DataType::INT16: {
            auto array_info =
                GetDataInfoFromArray<arrow::Int16Array,
                                     arrow::Type::type::INT16>(array);
            if (nullable_) {
                return FillFieldData(array_info.first,
                                     array->null_bitmap_data(),
                                     element_count,
                                     array->offset());
            }
            return FillFieldData(array_info.first, array_info.second);
        }
        case DataType::INT32: {
            auto array_info =
                GetDataInfoFromArray<arrow::Int32Array,
                                     arrow::Type::type::INT32>(array);
            if (nullable_) {
                return FillFieldData(array_info.first,
                                     array->null_bitmap_data(),
                                     element_count,
                                     array->offset());
            }
            return FillFieldData(array_info.first, array_info.second);
        }
        case DataType::INT64: {
            auto array_info =
                GetDataInfoFromArray<arrow::Int64Array,
                                     arrow::Type::type::INT64>(array);
            if (nullable_) {
                return FillFieldData(array_info.first,
                                     array->null_bitmap_data(),
                                     element_count,
                                     array->offset());
            }
            return FillFieldData(array_info.first, array_info.second);
        }
        case DataType::FLOAT: {
            auto array_info =
                GetDataInfoFromArray<arrow::FloatArray,
                                     arrow::Type::type::FLOAT>(array);
            if (nullable_) {
                return FillFieldData(array_info.first,
                                     array->null_bitmap_data(),
                                     element_count,
                                     array->offset());
            }
            return FillFieldData(array_info.first, array_info.second);
        }
        case DataType::DOUBLE: {
            auto array_info =
                GetDataInfoFromArray<arrow::DoubleArray,
                                     arrow::Type::type::DOUBLE>(array);
            if (nullable_) {
                return FillFieldData(array_info.first,
                                     array->null_bitmap_data(),
                                     element_count,
                                     array->offset());
            }
            return FillFieldData(array_info.first, array_info.second);
        }
        case DataType::TIMESTAMPTZ: {
            auto array_info =
                GetDataInfoFromArray<arrow::Int64Array,
                                     arrow::Type::type::INT64>(array);
            if (nullable_) {
                return FillFieldData(array_info.first,
                                     array->null_bitmap_data(),
                                     element_count,
                                     array->offset());
            }
            return FillFieldData(array_info.first, array_info.second);
        }
        case DataType::STRING:
        case DataType::VARCHAR:
        case DataType::TEXT: {
            AssertInfo(array->type()->id() == arrow::Type::type::STRING,
                       "inconsistent data type");
            auto string_array =
                std::dynamic_pointer_cast<arrow::StringArray>(array);
            std::vector<std::string> values(element_count);
            for (size_t index = 0; index < element_count; ++index) {
                values[index] = string_array->GetString(index);
            }
            if (nullable_) {
                return FillFieldData(values.data(),
                                     array->null_bitmap_data(),
                                     element_count,
                                     array->offset());
            }
            return FillFieldData(values.data(), element_count);
        }
        case DataType::JSON: {
            // The code here is not referenced.
            // A subclass named FieldDataJsonImpl is implemented, which overloads this function.
            AssertInfo(array->type()->id() == arrow::Type::type::BINARY,
                       "inconsistent data type");
            auto json_array =
                std::dynamic_pointer_cast<arrow::BinaryArray>(array);
            std::vector<Json> values(element_count);
            for (size_t index = 0; index < element_count; ++index) {
                values[index] =
                    Json(simdjson::padded_string(json_array->GetString(index)));
            }
            if (nullable_) {
                return FillFieldData(values.data(),
                                     array->null_bitmap_data(),
                                     element_count,
                                     array->offset());
            }
            return FillFieldData(values.data(), element_count);
        }
        case DataType::GEOMETRY: {
            AssertInfo(array->type()->id() == arrow::Type::type::BINARY,
                       "inconsistent data type");
            auto geometry_array =
                std::dynamic_pointer_cast<arrow::BinaryArray>(array);
            AssertInfo(geometry_array != nullptr,
                       "null geometry arrow binary array");
            std::vector<uint8_t> values(element_count);
            for (size_t index = 0; index < element_count; ++index) {
                values[index] = *geometry_array->GetValue(index, 0);
            }
            if (nullable_) {
                return FillFieldData(values.data(),
                                     array->null_bitmap_data(),
                                     element_count,
                                     array->offset());
            }
            return FillFieldData(values.data(), element_count);
        }
        case DataType::ARRAY: {
            auto array_array =
                std::dynamic_pointer_cast<arrow::BinaryArray>(array);
            std::vector<Array> values(element_count);
            int null_number = 0;
            for (size_t index = 0; index < element_count; ++index) {
                ScalarFieldProto field_data;
                if (array_array->GetString(index) == "") {
                    null_number++;
                    continue;
                }
                auto success =
                    field_data.ParseFromString(array_array->GetString(index));
                AssertInfo(success, "parse from string failed");
                values[index] = Array(field_data);
            }
            if (nullable_) {
                return FillFieldData(values.data(),
                                     array->null_bitmap_data(),
                                     element_count,
                                     array->offset());
            }
            AssertInfo(null_number == 0, "get empty string when not nullable");
            return FillFieldData(values.data(), element_count);
        }
        case DataType::VECTOR_FLOAT:
        case DataType::VECTOR_FLOAT16:
        case DataType::VECTOR_BFLOAT16:
        case DataType::VECTOR_INT8:
        case DataType::VECTOR_BINARY: {
            auto array_info =
                GetDataInfoFromArray<arrow::FixedSizeBinaryArray,
                                     arrow::Type::type::FIXED_SIZE_BINARY>(
                    array);
            return FillFieldData(array_info.first, array_info.second);
        }
        case DataType::VECTOR_SPARSE_U32_F32: {
            AssertInfo(array->type()->id() == arrow::Type::type::BINARY,
                       "inconsistent data type");
            auto arr = std::dynamic_pointer_cast<arrow::BinaryArray>(array);
            std::vector<knowhere::sparse::SparseRow<SparseValueType>> values;
            for (size_t index = 0; index < element_count; ++index) {
                auto view = arr->GetString(index);
                values.push_back(
                    CopyAndWrapSparseRow(view.data(), view.size()));
            }
            return FillFieldData(values.data(), element_count);
        }
        case DataType::VECTOR_ARRAY: {
            auto list_array =
                std::dynamic_pointer_cast<arrow::ListArray>(array);
            AssertInfo(list_array != nullptr,
                       "Failed to cast to ListArray for VECTOR_ARRAY");

            auto vector_array_field =
                dynamic_cast<FieldData<VectorArray>*>(this);
            AssertInfo(vector_array_field != nullptr,
                       "Failed to cast to FieldData<VectorArray>");
            int64_t dim = vector_array_field->get_dim();
            DataType element_type = vector_array_field->get_element_type();

            AssertInfo(dim > 0, "Invalid dimension {} in VECTOR_ARRAY", dim);
            AssertInfo(element_type != DataType::NONE,
                       "Element type not set for VECTOR_ARRAY");

            auto values_array = list_array->values();
            std::vector<VectorArray> values(element_count);

            switch (element_type) {
                case DataType::VECTOR_FLOAT: {
                    auto float_array =
                        std::dynamic_pointer_cast<arrow::FloatArray>(
                            values_array);
                    AssertInfo(
                        float_array != nullptr,
                        "Expected FloatArray for VECTOR_FLOAT element type");

                    for (size_t index = 0; index < element_count; ++index) {
                        int64_t start_offset = list_array->value_offset(index);
                        int64_t end_offset =
                            list_array->value_offset(index + 1);
                        int64_t num_floats = end_offset - start_offset;
                        AssertInfo(num_floats % dim == 0,
                                   "Invalid data: number of floats ({}) not "
                                   "divisible by "
                                   "dimension ({})",
                                   num_floats,
                                   dim);

                        int num_vectors = num_floats / dim;
                        const float* data_ptr =
                            float_array->raw_values() + start_offset;
                        values[index] =
                            VectorArray(static_cast<const void*>(data_ptr),
                                        num_vectors,
                                        dim,
                                        element_type);
                    }
                    break;
                }
                case DataType::VECTOR_BINARY:
                case DataType::VECTOR_FLOAT16:
                case DataType::VECTOR_BFLOAT16:
                case DataType::VECTOR_INT8:
                    ThrowInfo(
                        NotImplemented,
                        "Element type {} in VectorArray not implemented yet",
                        GetDataTypeName(element_type));
                    break;
                default:
                    ThrowInfo(DataTypeInvalid,
                              "Unsupported element type {} in VectorArray",
                              GetDataTypeName(element_type));
            }
            return FillFieldData(values.data(), element_count);
        }
        default: {
            ThrowInfo(DataTypeInvalid,
                      GetName() + "::FillFieldData" +
                          " not support data type " +
                          GetDataTypeName(data_type_));
        }
    }
}

// used for generate added field which has no related binlogs
template <typename Type, bool is_type_entire_row>
void
FieldDataImpl<Type, is_type_entire_row>::FillFieldData(
    std::optional<DefaultValueType> default_value, ssize_t element_count) {
    AssertInfo(nullable_, "added field must be nullable");
    if (element_count == 0) {
        return;
    }
    null_count_ = default_value.has_value() ? 0 : element_count;

    auto valid_data_ptr = [&] {
        ssize_t byte_count = (element_count + 7) / 8;
        std::shared_ptr<uint8_t[]> valid_data(new uint8_t[byte_count]);
        std::fill(valid_data.get(), valid_data.get() + byte_count, 0x00);
        return valid_data;
    }();
    switch (data_type_) {
        case DataType::BOOL: {
            FixedVector<bool> values(element_count);
            if (default_value.has_value()) {
                std::fill(
                    values.begin(), values.end(), default_value->bool_data());
                return FillFieldData(values.data(), nullptr, element_count, 0);
            }
            return FillFieldData(
                values.data(), valid_data_ptr.get(), element_count, 0);
        }
        case DataType::INT8: {
            FixedVector<int8_t> values(element_count);
            if (default_value.has_value()) {
                std::fill(
                    values.begin(), values.end(), default_value->int_data());
                return FillFieldData(values.data(), nullptr, element_count, 0);
            }
            return FillFieldData(
                values.data(), valid_data_ptr.get(), element_count, 0);
        }
        case DataType::INT16: {
            FixedVector<int16_t> values(element_count);
            if (default_value.has_value()) {
                std::fill(
                    values.begin(), values.end(), default_value->int_data());
                return FillFieldData(values.data(), nullptr, element_count, 0);
            }
            return FillFieldData(
                values.data(), valid_data_ptr.get(), element_count, 0);
        }
        case DataType::INT32: {
            FixedVector<int32_t> values(element_count);
            if (default_value.has_value()) {
                std::fill(
                    values.begin(), values.end(), default_value->int_data());
                return FillFieldData(values.data(), nullptr, element_count, 0);
            }
            return FillFieldData(
                values.data(), valid_data_ptr.get(), element_count, 0);
        }
        case DataType::INT64: {
            FixedVector<int64_t> values(element_count);
            if (default_value.has_value()) {
                std::fill(
                    values.begin(), values.end(), default_value->long_data());
                return FillFieldData(values.data(), nullptr, element_count, 0);
            }
            return FillFieldData(
                values.data(), valid_data_ptr.get(), element_count, 0);
        }
        case DataType::FLOAT: {
            FixedVector<float> values(element_count);
            if (default_value.has_value()) {
                std::fill(
                    values.begin(), values.end(), default_value->float_data());
                return FillFieldData(values.data(), nullptr, element_count, 0);
            }
            return FillFieldData(
                values.data(), valid_data_ptr.get(), element_count, 0);
        }
        case DataType::DOUBLE: {
            FixedVector<double> values(element_count);
            if (default_value.has_value()) {
                std::fill(
                    values.begin(), values.end(), default_value->double_data());
                return FillFieldData(values.data(), nullptr, element_count, 0);
            }
            return FillFieldData(
                values.data(), valid_data_ptr.get(), element_count, 0);
        }
        case DataType::TIMESTAMPTZ: {
            FixedVector<int64_t> values(element_count);
            if (default_value.has_value()) {
                std::fill(values.begin(),
                          values.end(),
                          default_value->timestamptz_data());
                return FillFieldData(values.data(), nullptr, element_count, 0);
            }
            return FillFieldData(
                values.data(), valid_data_ptr.get(), element_count, 0);
        }
        case DataType::STRING:
        case DataType::VARCHAR: {
            FixedVector<std::string> values(element_count);
            if (default_value.has_value()) {
                std::fill(
                    values.begin(), values.end(), default_value->string_data());
                return FillFieldData(values.data(), nullptr, element_count, 0);
            }
            return FillFieldData(
                values.data(), valid_data_ptr.get(), element_count, 0);
        }
        case DataType::JSON: {
            // The code here is not referenced.
            // A subclass named FieldDataJsonImpl is implemented, which overloads this function.
            FixedVector<Json> values(element_count);
            return FillFieldData(
                values.data(), valid_data_ptr.get(), element_count, 0);
        }

        case DataType::GEOMETRY: {
            FixedVector<std::string> values(element_count);
            if (default_value.has_value()) {
                std::fill(
                    values.begin(), values.end(), default_value->string_data());
                return FillFieldData(values.data(), nullptr, element_count, 0);
            }
            return FillFieldData(
                values.data(), valid_data_ptr.get(), element_count, 0);
        }
        case DataType::ARRAY: {
            // todo: add array default_value
            FixedVector<Array> values(element_count);
            return FillFieldData(
                values.data(), valid_data_ptr.get(), element_count, 0);
        }
        default: {
            ThrowInfo(DataTypeInvalid,
                      GetName() + "::FillFieldData" +
                          " not support data type " +
                          GetDataTypeName(data_type_));
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
template class FieldDataImpl<Geometry, true>;
template class FieldDataImpl<Array, true>;

// vector data
template class FieldDataImpl<int8_t, false>;
template class FieldDataImpl<float, false>;
template class FieldDataImpl<float16, false>;
template class FieldDataImpl<bfloat16, false>;
template class FieldDataImpl<knowhere::sparse::SparseRow<SparseValueType>,
                             true>;
template class FieldDataImpl<VectorArray, true>;

FieldDataPtr
InitScalarFieldData(const DataType& type, bool nullable, int64_t cap_rows) {
    switch (type) {
        case DataType::BOOL:
            return std::make_shared<FieldData<bool>>(type, nullable, cap_rows);
        case DataType::INT8:
            return std::make_shared<FieldData<int8_t>>(
                type, nullable, cap_rows);
        case DataType::INT16:
            return std::make_shared<FieldData<int16_t>>(
                type, nullable, cap_rows);
        case DataType::INT32:
            return std::make_shared<FieldData<int32_t>>(
                type, nullable, cap_rows);
        case DataType::INT64:
            return std::make_shared<FieldData<int64_t>>(
                type, nullable, cap_rows);
        case DataType::FLOAT:
            return std::make_shared<FieldData<float>>(type, nullable, cap_rows);
        case DataType::DOUBLE:
            return std::make_shared<FieldData<double>>(
                type, nullable, cap_rows);
        case DataType::TIMESTAMPTZ:
            return std::make_shared<FieldData<int64_t>>(
                type, nullable, cap_rows);
        case DataType::STRING:
        case DataType::VARCHAR:
        case DataType::TEXT:
            return std::make_shared<FieldData<std::string>>(
                type, nullable, cap_rows);
        case DataType::JSON:
            return std::make_shared<FieldData<Json>>(type, nullable, cap_rows);
        case DataType::GEOMETRY:
            return std::make_shared<FieldData<std::string>>(
                type, nullable, cap_rows);
        default:
            ThrowInfo(DataTypeInvalid,
                      "InitScalarFieldData not support data type " +
                          GetDataTypeName(type));
    }
}

}  // namespace milvus
