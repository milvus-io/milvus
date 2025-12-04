// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include "segcore/Utils.h"
#include <arrow/record_batch.h>

#include <future>
#include <memory>
#include <string>
#include <vector>

#include "cachinglayer/Manager.h"
#include "common/type_c.h"
#include "common/Common.h"
#include "common/FieldData.h"
#include "common/FieldDataInterface.h"
#include "common/Types.h"
#include "common/Utils.h"
#include "index/ScalarIndex.h"
#include "log/Log.h"
#include "segcore/storagev1translator/SealedIndexTranslator.h"
#include "segcore/storagev1translator/V1SealedIndexTranslator.h"
#include "segcore/Types.h"
#include "storage/DataCodec.h"
#include "storage/RemoteChunkManagerSingleton.h"
#include "storage/ThreadPools.h"
#include "storage/Util.h"

namespace milvus::segcore {

void
ParsePksFromFieldData(std::vector<PkType>& pks, const DataArray& data) {
    auto data_type = static_cast<DataType>(data.type());
    switch (data_type) {
        case DataType::INT64: {
            auto source_data = reinterpret_cast<const int64_t*>(
                data.scalars().long_data().data().data());
            std::copy_n(source_data, pks.size(), pks.data());
            break;
        }
        case DataType::VARCHAR: {
            auto& src_data = data.scalars().string_data().data();
            std::copy(src_data.begin(), src_data.end(), pks.begin());
            break;
        }
        default: {
            ThrowInfo(DataTypeInvalid,
                      fmt::format("unsupported PK {}", data_type));
        }
    }
}

void
ParsePksFromFieldData(DataType data_type,
                      std::vector<PkType>& pks,
                      const std::vector<FieldDataPtr>& datas) {
    int64_t offset = 0;

    for (auto& field_data : datas) {
        AssertInfo(data_type == field_data->get_data_type(),
                   "inconsistent data type when parse pk from field data");
        int64_t row_count = field_data->get_num_rows();
        switch (data_type) {
            case DataType::INT64: {
                std::copy_n(static_cast<const int64_t*>(field_data->Data()),
                            row_count,
                            pks.data() + offset);
                break;
            }
            case DataType::VARCHAR: {
                std::copy_n(static_cast<const std::string*>(field_data->Data()),
                            row_count,
                            pks.data() + offset);
                break;
            }
            default: {
                ThrowInfo(DataTypeInvalid,
                          fmt::format("unsupported PK {}", data_type));
            }
        }
        offset += row_count;
    }
}

void
ParsePksFromIDs(std::vector<PkType>& pks,
                DataType data_type,
                const IdArray& data) {
    switch (data_type) {
        case DataType::INT64: {
            auto source_data =
                reinterpret_cast<const int64_t*>(data.int_id().data().data());
            std::copy_n(source_data, pks.size(), pks.data());
            break;
        }
        case DataType::VARCHAR: {
            auto& source_data = data.str_id().data();
            std::copy(source_data.begin(), source_data.end(), pks.begin());
            break;
        }
        default: {
            ThrowInfo(DataTypeInvalid,
                      fmt::format("unsupported PK {}", data_type));
        }
    }
}

int64_t
GetSizeOfIdArray(const IdArray& data) {
    if (data.has_int_id()) {
        return data.int_id().data_size();
    }

    if (data.has_str_id()) {
        return data.str_id().data_size();
    }

    ThrowInfo(DataTypeInvalid,
              fmt::format("unsupported id {}", data.descriptor()->name()));
}

int64_t
GetRawDataSizeOfDataArray(const DataArray* data,
                          const FieldMeta& field_meta,
                          int64_t num_rows) {
    int64_t result = 0;
    auto data_type = field_meta.get_data_type();
    if (!IsVariableDataType(data_type)) {
        result = field_meta.get_sizeof() * num_rows;
    } else {
        switch (data_type) {
            case DataType::STRING:
            case DataType::VARCHAR:
            case DataType::TEXT: {
                auto& string_data = FIELD_DATA(data, string);
                for (auto& str : string_data) {
                    result += str.size();
                }
                break;
            }
            case DataType::JSON: {
                auto& json_data = FIELD_DATA(data, json);
                for (auto& json_bytes : json_data) {
                    result += json_bytes.size();
                }
                break;
            }
            case DataType::GEOMETRY: {
                auto& geometry_data = FIELD_DATA(data, geometry);
                for (auto& geometry_bytes : geometry_data) {
                    result += geometry_bytes.size();
                }
                break;
            }
            case DataType::ARRAY: {
                auto& array_data = FIELD_DATA(data, array);
                switch (field_meta.get_element_type()) {
                    case DataType::BOOL: {
                        for (auto& array_bytes : array_data) {
                            result += array_bytes.bool_data().data_size() *
                                      sizeof(bool);
                        }
                        break;
                    }
                    case DataType::INT8:
                    case DataType::INT16:
                    case DataType::INT32: {
                        for (auto& array_bytes : array_data) {
                            result += array_bytes.int_data().data_size() *
                                      sizeof(int);
                        }
                        break;
                    }
                    case DataType::INT64: {
                        for (auto& array_bytes : array_data) {
                            result += array_bytes.long_data().data_size() *
                                      sizeof(int64_t);
                        }
                        break;
                    }
                    case DataType::FLOAT: {
                        for (auto& array_bytes : array_data) {
                            result += array_bytes.float_data().data_size() *
                                      sizeof(float);
                        }
                        break;
                    }
                    case DataType::DOUBLE: {
                        for (auto& array_bytes : array_data) {
                            result += array_bytes.double_data().data_size() *
                                      sizeof(double);
                        }
                        break;
                    }
                    case DataType::TIMESTAMPTZ: {
                        for (auto& array_bytes : array_data) {
                            result +=
                                array_bytes.timestamptz_data().data_size() *
                                sizeof(int64_t);
                        }
                        break;
                    }
                    case DataType::VARCHAR:
                    case DataType::STRING:
                    case DataType::TEXT: {
                        for (auto& array_bytes : array_data) {
                            auto element_num =
                                array_bytes.string_data().data_size();
                            for (int i = 0; i < element_num; ++i) {
                                result +=
                                    array_bytes.string_data().data(i).size();
                            }
                        }
                        break;
                    }
                    default:
                        ThrowInfo(
                            DataTypeInvalid,
                            fmt::format("unsupported element type for array",
                                        field_meta.get_element_type()));
                }

                break;
            }
            case DataType::VECTOR_SPARSE_U32_F32: {
                // TODO(SPARSE, size)
                result += data->vectors().sparse_float_vector().ByteSizeLong();
                break;
            }
            case DataType::VECTOR_ARRAY: {
                auto& obj = data->vectors().vector_array().data();
                switch (field_meta.get_element_type()) {
                    case DataType::VECTOR_FLOAT: {
                        for (auto& e : obj) {
                            result += e.float_vector().ByteSizeLong();
                        }
                        break;
                    }
                    default: {
                        ThrowInfo(NotImplemented,
                                  fmt::format("not implemented vector type {}",
                                              field_meta.get_element_type()));
                    }
                }
                break;
            }
            default: {
                ThrowInfo(
                    DataTypeInvalid,
                    fmt::format("unsupported variable datatype {}", data_type));
            }
        }
    }

    return result;
}

// Note: this is temporary solution.
// modify bulk script implement to make process more clear

std::unique_ptr<DataArray>
CreateEmptyScalarDataArray(int64_t count, const FieldMeta& field_meta) {
    auto data_type = field_meta.get_data_type();
    auto data_array = std::make_unique<DataArray>();
    data_array->set_field_id(field_meta.get_id().get());
    data_array->set_type(static_cast<milvus::proto::schema::DataType>(
        field_meta.get_data_type()));

    if (field_meta.is_nullable()) {
        data_array->mutable_valid_data()->Resize(count, false);
    }

    auto scalar_array = data_array->mutable_scalars();
    switch (data_type) {
        case DataType::BOOL: {
            auto obj = scalar_array->mutable_bool_data();
            obj->mutable_data()->Resize(count, false);
            break;
        }
        case DataType::INT8: {
            auto obj = scalar_array->mutable_int_data();
            obj->mutable_data()->Resize(count, 0);
            break;
        }
        case DataType::INT16: {
            auto obj = scalar_array->mutable_int_data();
            obj->mutable_data()->Resize(count, 0);
            break;
        }
        case DataType::INT32: {
            auto obj = scalar_array->mutable_int_data();
            obj->mutable_data()->Resize(count, 0);
            break;
        }
        case DataType::INT64: {
            auto obj = scalar_array->mutable_long_data();
            obj->mutable_data()->Resize(count, 0);
            break;
        }
        case DataType::FLOAT: {
            auto obj = scalar_array->mutable_float_data();
            obj->mutable_data()->Resize(count, 0);
            break;
        }
        case DataType::DOUBLE: {
            auto obj = scalar_array->mutable_double_data();
            obj->mutable_data()->Resize(count, 0);
            break;
        }
        case DataType::TIMESTAMPTZ: {
            auto obj = scalar_array->mutable_timestamptz_data();
            obj->mutable_data()->Resize(count, 0);
            break;
        }
        case DataType::VARCHAR:
        case DataType::STRING:
        case DataType::TEXT: {
            auto obj = scalar_array->mutable_string_data();
            obj->mutable_data()->Reserve(count);
            for (auto i = 0; i < count; i++) {
                *(obj->mutable_data()->Add()) = std::string();
            }
            break;
        }
        case DataType::JSON: {
            auto obj = scalar_array->mutable_json_data();
            obj->mutable_data()->Reserve(count);
            for (int i = 0; i < count; i++) {
                *(obj->mutable_data()->Add()) = std::string();
            }
            break;
        }
        case DataType::GEOMETRY: {
            auto obj = scalar_array->mutable_geometry_data();
            obj->mutable_data()->Reserve(count);
            for (int i = 0; i < count; i++) {
                *(obj->mutable_data()->Add()) = std::string();
            }
            break;
        }
        case DataType::ARRAY: {
            auto obj = scalar_array->mutable_array_data();
            obj->mutable_data()->Reserve(count);
            obj->set_element_type(static_cast<milvus::proto::schema::DataType>(
                field_meta.get_element_type()));
            for (int i = 0; i < count; i++) {
                *(obj->mutable_data()->Add()) = proto::schema::ScalarField();
            }
            break;
        }
        default: {
            ThrowInfo(DataTypeInvalid,
                      fmt::format("unsupported datatype {}", data_type));
        }
    }

    return data_array;
}

std::unique_ptr<DataArray>
CreateEmptyVectorDataArray(int64_t count, const FieldMeta& field_meta) {
    auto data_type = field_meta.get_data_type();
    auto data_array = std::make_unique<DataArray>();
    data_array->set_field_id(field_meta.get_id().get());
    data_array->set_type(static_cast<milvus::proto::schema::DataType>(
        field_meta.get_data_type()));

    auto vector_array = data_array->mutable_vectors();
    auto dim = 0;
    if (data_type != DataType::VECTOR_SPARSE_U32_F32) {
        dim = field_meta.get_dim();
        vector_array->set_dim(dim);
    }
    switch (data_type) {
        case DataType::VECTOR_FLOAT: {
            auto length = count * dim;
            auto obj = vector_array->mutable_float_vector();
            obj->mutable_data()->Resize(length, 0);
            break;
        }
        case DataType::VECTOR_BINARY: {
            AssertInfo(dim % 8 == 0,
                       "Binary vector field dimension is not a multiple of 8");
            auto num_bytes = count * dim / 8;
            auto obj = vector_array->mutable_binary_vector();
            obj->resize(num_bytes);
            break;
        }
        case DataType::VECTOR_FLOAT16: {
            auto length = count * dim;
            auto obj = vector_array->mutable_float16_vector();
            obj->resize(length * sizeof(float16));
            break;
        }
        case DataType::VECTOR_BFLOAT16: {
            auto length = count * dim;
            auto obj = vector_array->mutable_bfloat16_vector();
            obj->resize(length * sizeof(bfloat16));
            break;
        }
        case DataType::VECTOR_SPARSE_U32_F32: {
            // does nothing here
            break;
        }
        case DataType::VECTOR_INT8: {
            auto length = count * dim;
            auto obj = vector_array->mutable_int8_vector();
            obj->resize(length);
            break;
        }
        case DataType::VECTOR_ARRAY: {
            auto obj = vector_array->mutable_vector_array();
            obj->set_element_type(static_cast<milvus::proto::schema::DataType>(
                field_meta.get_element_type()));
            obj->mutable_data()->Reserve(count);
            for (int i = 0; i < count; i++) {
                *(obj->mutable_data()->Add()) = proto::schema::VectorField();
            }
            break;
        }
        default: {
            ThrowInfo(DataTypeInvalid,
                      fmt::format("unsupported datatype {}", data_type));
        }
    }
    return data_array;
}

std::unique_ptr<DataArray>
CreateScalarDataArrayFrom(const void* data_raw,
                          const void* valid_data,
                          int64_t count,
                          const FieldMeta& field_meta) {
    auto data_type = field_meta.get_data_type();
    auto data_array = std::make_unique<DataArray>();
    data_array->set_field_id(field_meta.get_id().get());
    data_array->set_type(static_cast<milvus::proto::schema::DataType>(
        field_meta.get_data_type()));
    if (field_meta.is_nullable()) {
        auto valid_data_ = reinterpret_cast<const bool*>(valid_data);
        auto obj = data_array->mutable_valid_data();
        obj->Add(valid_data_, valid_data_ + count);
    }

    auto scalar_array = data_array->mutable_scalars();
    switch (data_type) {
        case DataType::BOOL: {
            auto data = reinterpret_cast<const bool*>(data_raw);
            auto obj = scalar_array->mutable_bool_data();
            obj->mutable_data()->Add(data, data + count);
            break;
        }
        case DataType::INT8: {
            auto data = reinterpret_cast<const int8_t*>(data_raw);
            auto obj = scalar_array->mutable_int_data();
            obj->mutable_data()->Add(data, data + count);
            break;
        }
        case DataType::INT16: {
            auto data = reinterpret_cast<const int16_t*>(data_raw);
            auto obj = scalar_array->mutable_int_data();
            obj->mutable_data()->Add(data, data + count);
            break;
        }
        case DataType::INT32: {
            auto data = reinterpret_cast<const int32_t*>(data_raw);
            auto obj = scalar_array->mutable_int_data();
            obj->mutable_data()->Add(data, data + count);
            break;
        }
        case DataType::INT64: {
            auto data = reinterpret_cast<const int64_t*>(data_raw);
            auto obj = scalar_array->mutable_long_data();
            obj->mutable_data()->Add(data, data + count);
            break;
        }
        case DataType::FLOAT: {
            auto data = reinterpret_cast<const float*>(data_raw);
            auto obj = scalar_array->mutable_float_data();
            obj->mutable_data()->Add(data, data + count);
            break;
        }
        case DataType::DOUBLE: {
            auto data = reinterpret_cast<const double*>(data_raw);
            auto obj = scalar_array->mutable_double_data();
            obj->mutable_data()->Add(data, data + count);
            break;
        }
        case DataType::TIMESTAMPTZ: {
            auto data = reinterpret_cast<const int64_t*>(data_raw);
            auto obj = scalar_array->mutable_timestamptz_data();
            obj->mutable_data()->Add(data, data + count);
            break;
        }
        case DataType::VARCHAR:
        case DataType::TEXT: {
            auto data = reinterpret_cast<const std::string*>(data_raw);
            auto obj = scalar_array->mutable_string_data();
            for (auto i = 0; i < count; i++) {
                *(obj->mutable_data()->Add()) = data[i];
            }
            break;
        }
        case DataType::JSON: {
            auto data = reinterpret_cast<const std::string*>(data_raw);
            auto obj = scalar_array->mutable_json_data();
            for (auto i = 0; i < count; i++) {
                *(obj->mutable_data()->Add()) = data[i];
            }
            break;
        }
        case DataType::GEOMETRY: {
            auto data = reinterpret_cast<const std::string*>(data_raw);
            auto obj = scalar_array->mutable_geometry_data();
            for (auto i = 0; i < count; i++) {
                *(obj->mutable_data()->Add()) =
                    std::string(data[i].data(), data[i].size());
            }
            break;
        }
        case DataType::ARRAY: {
            auto data = reinterpret_cast<const ScalarFieldProto*>(data_raw);
            auto obj = scalar_array->mutable_array_data();
            obj->set_element_type(static_cast<milvus::proto::schema::DataType>(
                field_meta.get_element_type()));
            for (auto i = 0; i < count; i++) {
                *(obj->mutable_data()->Add()) = data[i];
            }
            break;
        }
        default: {
            ThrowInfo(DataTypeInvalid,
                      fmt::format("unsupported datatype {}", data_type));
        }
    }

    return data_array;
}

std::unique_ptr<DataArray>
CreateVectorDataArrayFrom(const void* data_raw,
                          int64_t count,
                          const FieldMeta& field_meta) {
    auto data_type = field_meta.get_data_type();
    auto data_array = std::make_unique<DataArray>();
    data_array->set_field_id(field_meta.get_id().get());
    data_array->set_type(static_cast<milvus::proto::schema::DataType>(
        field_meta.get_data_type()));

    auto vector_array = data_array->mutable_vectors();
    auto dim = 0;
    if (!IsSparseFloatVectorDataType(data_type)) {
        dim = field_meta.get_dim();
        vector_array->set_dim(dim);
    }
    switch (data_type) {
        case DataType::VECTOR_FLOAT: {
            auto length = count * dim;
            auto data = reinterpret_cast<const float*>(data_raw);
            auto obj = vector_array->mutable_float_vector();
            obj->mutable_data()->Add(data, data + length);
            break;
        }
        case DataType::VECTOR_BINARY: {
            AssertInfo(dim % 8 == 0,
                       "Binary vector field dimension is not a multiple of 8");
            auto num_bytes = count * dim / 8;
            auto data = reinterpret_cast<const char*>(data_raw);
            auto obj = vector_array->mutable_binary_vector();
            obj->assign(data, num_bytes);
            break;
        }
        case DataType::VECTOR_FLOAT16: {
            auto length = count * dim;
            auto data = reinterpret_cast<const char*>(data_raw);
            auto obj = vector_array->mutable_float16_vector();
            obj->assign(data, length * sizeof(float16));
            break;
        }
        case DataType::VECTOR_BFLOAT16: {
            auto length = count * dim;
            auto data = reinterpret_cast<const char*>(data_raw);
            auto obj = vector_array->mutable_bfloat16_vector();
            obj->assign(data, length * sizeof(bfloat16));
            break;
        }
        case DataType::VECTOR_SPARSE_U32_F32: {
            SparseRowsToProto(
                [&](size_t i) {
                    return reinterpret_cast<const knowhere::sparse::SparseRow<
                               SparseValueType>*>(data_raw) +
                           i;
                },
                count,
                vector_array->mutable_sparse_float_vector());
            vector_array->set_dim(vector_array->sparse_float_vector().dim());
            break;
        }
        case DataType::VECTOR_INT8: {
            auto length = count * dim;
            auto data = reinterpret_cast<const char*>(data_raw);
            auto obj = vector_array->mutable_int8_vector();
            obj->assign(data, length * sizeof(int8));
            break;
        }
        case DataType::VECTOR_ARRAY: {
            auto data = reinterpret_cast<const VectorFieldProto*>(data_raw);
            auto vector_type = field_meta.get_element_type();
            auto obj = vector_array->mutable_vector_array();
            obj->set_dim(dim);

            // Set element type based on vector type
            switch (vector_type) {
                case DataType::VECTOR_FLOAT:
                    obj->set_element_type(
                        milvus::proto::schema::DataType::FloatVector);
                    break;
                case DataType::VECTOR_FLOAT16:
                    obj->set_element_type(
                        milvus::proto::schema::DataType::Float16Vector);
                    break;
                case DataType::VECTOR_BFLOAT16:
                    obj->set_element_type(
                        milvus::proto::schema::DataType::BFloat16Vector);
                    break;
                case DataType::VECTOR_BINARY:
                    obj->set_element_type(
                        milvus::proto::schema::DataType::BinaryVector);
                    break;
                case DataType::VECTOR_INT8:
                    obj->set_element_type(
                        milvus::proto::schema::DataType::Int8Vector);
                    break;
                default:
                    ThrowInfo(NotImplemented,
                              fmt::format("not implemented vector type {}",
                                          vector_type));
            }

            // Add all vector data
            for (auto i = 0; i < count; i++) {
                *(obj->mutable_data()->Add()) = data[i];
            }
            break;
        }
        default: {
            ThrowInfo(DataTypeInvalid,
                      fmt::format("unsupported datatype {}", data_type));
        }
    }
    return data_array;
}

std::unique_ptr<DataArray>
CreateDataArrayFrom(const void* data_raw,
                    const void* valid_data,
                    int64_t count,
                    const FieldMeta& field_meta) {
    auto data_type = field_meta.get_data_type();

    if (!IsVectorDataType(data_type) && data_type != DataType::VECTOR_ARRAY) {
        return CreateScalarDataArrayFrom(
            data_raw, valid_data, count, field_meta);
    }

    return CreateVectorDataArrayFrom(data_raw, count, field_meta);
}

// TODO remove merge dataArray, instead fill target entity when get data slice
std::unique_ptr<DataArray>
MergeDataArray(std::vector<MergeBase>& merge_bases,
               const FieldMeta& field_meta) {
    auto data_type = field_meta.get_data_type();
    auto data_array = std::make_unique<DataArray>();
    data_array->set_field_id(field_meta.get_id().get());
    auto nullable = field_meta.is_nullable();
    data_array->set_type(static_cast<milvus::proto::schema::DataType>(
        field_meta.get_data_type()));

    for (auto& merge_base : merge_bases) {
        auto src_field_data = merge_base.get_field_data(field_meta.get_id());
        auto src_offset = merge_base.getOffset();
        AssertInfo(data_type == DataType(src_field_data->type()),
                   "merge field data type not consistent");
        if (field_meta.is_vector()) {
            auto vector_array = data_array->mutable_vectors();
            auto dim = 0;
            if (!IsSparseFloatVectorDataType(data_type)) {
                dim = field_meta.get_dim();
                vector_array->set_dim(dim);
            }
            if (field_meta.get_data_type() == DataType::VECTOR_FLOAT) {
                auto data = VEC_FIELD_DATA(src_field_data, float).data();
                auto obj = vector_array->mutable_float_vector();
                obj->mutable_data()->Add(data + src_offset * dim,
                                         data + (src_offset + 1) * dim);
            } else if (field_meta.get_data_type() == DataType::VECTOR_FLOAT16) {
                auto data = VEC_FIELD_DATA(src_field_data, float16);
                auto obj = vector_array->mutable_float16_vector();
                obj->assign(data, dim * sizeof(float16));
            } else if (field_meta.get_data_type() ==
                       DataType::VECTOR_BFLOAT16) {
                auto data = VEC_FIELD_DATA(src_field_data, bfloat16);
                auto obj = vector_array->mutable_bfloat16_vector();
                obj->assign(data, dim * sizeof(bfloat16));
            } else if (field_meta.get_data_type() == DataType::VECTOR_BINARY) {
                AssertInfo(
                    dim % 8 == 0,
                    "Binary vector field dimension is not a multiple of 8");
                auto num_bytes = dim / 8;
                auto data = VEC_FIELD_DATA(src_field_data, binary);
                auto obj = vector_array->mutable_binary_vector();
                obj->assign(data + src_offset * num_bytes, num_bytes);
            } else if (field_meta.get_data_type() ==
                       DataType::VECTOR_SPARSE_U32_F32) {
                auto src = src_field_data->vectors().sparse_float_vector();
                auto dst = vector_array->mutable_sparse_float_vector();
                if (src.dim() > dst->dim()) {
                    dst->set_dim(src.dim());
                }
                vector_array->set_dim(dst->dim());
                *dst->mutable_contents() = src.contents();
            } else if (field_meta.get_data_type() == DataType::VECTOR_INT8) {
                auto data = VEC_FIELD_DATA(src_field_data, int8);
                auto obj = vector_array->mutable_int8_vector();
                obj->assign(data, dim * sizeof(int8));
            } else if (field_meta.get_data_type() == DataType::VECTOR_ARRAY) {
                auto data = src_field_data->vectors().vector_array();
                auto obj = vector_array->mutable_vector_array();
                obj->set_element_type(
                    proto::schema::DataType(field_meta.get_element_type()));
                obj->CopyFrom(data);
            } else {
                ThrowInfo(DataTypeInvalid,
                          fmt::format("unsupported datatype {}", data_type));
            }
            continue;
        }

        if (nullable) {
            auto data = src_field_data->valid_data().data();
            auto obj = data_array->mutable_valid_data();
            *(obj->Add()) = data[src_offset];
        }

        auto scalar_array = data_array->mutable_scalars();
        switch (data_type) {
            case DataType::BOOL: {
                auto data = FIELD_DATA(src_field_data, bool).data();
                auto obj = scalar_array->mutable_bool_data();
                *(obj->mutable_data()->Add()) = data[src_offset];
                break;
            }
            case DataType::INT8:
            case DataType::INT16:
            case DataType::INT32: {
                auto data = FIELD_DATA(src_field_data, int).data();
                auto obj = scalar_array->mutable_int_data();
                *(obj->mutable_data()->Add()) = data[src_offset];
                break;
            }
            case DataType::INT64: {
                auto data = FIELD_DATA(src_field_data, long).data();
                auto obj = scalar_array->mutable_long_data();
                *(obj->mutable_data()->Add()) = data[src_offset];
                break;
            }
            case DataType::FLOAT: {
                auto data = FIELD_DATA(src_field_data, float).data();
                auto obj = scalar_array->mutable_float_data();
                *(obj->mutable_data()->Add()) = data[src_offset];
                break;
            }
            case DataType::DOUBLE: {
                auto data = FIELD_DATA(src_field_data, double).data();
                auto obj = scalar_array->mutable_double_data();
                *(obj->mutable_data()->Add()) = data[src_offset];
                break;
            }
            case DataType::TIMESTAMPTZ: {
                auto data = FIELD_DATA(src_field_data, timestamptz)
                                .data();  //Here is a marco
                auto obj = scalar_array->mutable_timestamptz_data();
                *(obj->mutable_data()->Add()) = data[src_offset];
                break;
            }
            case DataType::VARCHAR:
            case DataType::TEXT: {
                auto& data = FIELD_DATA(src_field_data, string);
                auto obj = scalar_array->mutable_string_data();
                *(obj->mutable_data()->Add()) = data[src_offset];
                break;
            }
            case DataType::JSON: {
                auto& data = FIELD_DATA(src_field_data, json);
                auto obj = scalar_array->mutable_json_data();
                *(obj->mutable_data()->Add()) = data[src_offset];
                break;
            }
            case DataType::GEOMETRY: {
                auto& data = FIELD_DATA(src_field_data, geometry);
                auto obj = scalar_array->mutable_geometry_data();
                *(obj->mutable_data()->Add()) = std::string(
                    data[src_offset].data(), data[src_offset].size());
                break;
            }
            case DataType::ARRAY: {
                auto& data = FIELD_DATA(src_field_data, array);
                auto obj = scalar_array->mutable_array_data();
                obj->set_element_type(
                    proto::schema::DataType(field_meta.get_element_type()));
                *(obj->mutable_data()->Add()) = data[src_offset];
                break;
            }
            default: {
                ThrowInfo(DataTypeInvalid,
                          fmt::format("unsupported datatype {}", data_type));
            }
        }
    }
    return data_array;
}

// TODO: split scalar IndexBase with knowhere::Index
std::unique_ptr<DataArray>
ReverseDataFromIndex(const index::IndexBase* index,
                     const int64_t* seg_offsets,
                     int64_t count,
                     const FieldMeta& field_meta) {
    auto data_type = field_meta.get_data_type();
    auto data_array = std::make_unique<DataArray>();
    data_array->set_field_id(field_meta.get_id().get());
    data_array->set_type(static_cast<milvus::proto::schema::DataType>(
        field_meta.get_data_type()));
    auto nullable = field_meta.is_nullable();
    std::vector<bool> valid_data;
    if (nullable) {
        valid_data.resize(count);
    }

    auto scalar_array = data_array->mutable_scalars();
    switch (data_type) {
        case DataType::BOOL: {
            using IndexType = index::ScalarIndex<bool>;
            auto ptr = dynamic_cast<const IndexType*>(index);
            std::vector<bool> raw_data(count);
            for (int64_t i = 0; i < count; ++i) {
                auto raw = ptr->Reverse_Lookup(seg_offsets[i]);
                // if has no value, means nullable must be true, no need to check nullable again here
                if (!raw.has_value()) {
                    valid_data[i] = false;
                    continue;
                }
                if (nullable) {
                    valid_data[i] = true;
                }
                raw_data[i] = raw.value();
            }
            auto obj = scalar_array->mutable_bool_data();
            *(obj->mutable_data()) = {raw_data.begin(), raw_data.end()};
            break;
        }
        case DataType::INT8: {
            using IndexType = index::ScalarIndex<int8_t>;
            auto ptr = dynamic_cast<const IndexType*>(index);
            std::vector<int8_t> raw_data(count);
            for (int64_t i = 0; i < count; ++i) {
                auto raw = ptr->Reverse_Lookup(seg_offsets[i]);
                // if has no value, means nullable must be true, no need to check nullable again here
                if (!raw.has_value()) {
                    valid_data[i] = false;
                    continue;
                }
                if (nullable) {
                    valid_data[i] = true;
                }
                raw_data[i] = raw.value();
            }
            auto obj = scalar_array->mutable_int_data();
            *(obj->mutable_data()) = {raw_data.begin(), raw_data.end()};
            break;
        }
        case DataType::INT16: {
            using IndexType = index::ScalarIndex<int16_t>;
            auto ptr = dynamic_cast<const IndexType*>(index);
            std::vector<int16_t> raw_data(count);
            for (int64_t i = 0; i < count; ++i) {
                auto raw = ptr->Reverse_Lookup(seg_offsets[i]);
                // if has no value, means nullable must be true, no need to check nullable again here
                if (!raw.has_value()) {
                    valid_data[i] = false;
                    continue;
                }
                if (nullable) {
                    valid_data[i] = true;
                }
                raw_data[i] = raw.value();
            }
            auto obj = scalar_array->mutable_int_data();
            *(obj->mutable_data()) = {raw_data.begin(), raw_data.end()};
            break;
        }
        case DataType::INT32: {
            using IndexType = index::ScalarIndex<int32_t>;
            auto ptr = dynamic_cast<const IndexType*>(index);
            std::vector<int32_t> raw_data(count);
            for (int64_t i = 0; i < count; ++i) {
                auto raw = ptr->Reverse_Lookup(seg_offsets[i]);
                // if has no value, means nullable must be true, no need to check nullable again here
                if (!raw.has_value()) {
                    valid_data[i] = false;
                    continue;
                }
                if (nullable) {
                    valid_data[i] = true;
                }
                raw_data[i] = raw.value();
            }
            auto obj = scalar_array->mutable_int_data();
            *(obj->mutable_data()) = {raw_data.begin(), raw_data.end()};
            break;
        }
        case DataType::INT64: {
            using IndexType = index::ScalarIndex<int64_t>;
            auto ptr = dynamic_cast<const IndexType*>(index);
            std::vector<int64_t> raw_data(count);
            for (int64_t i = 0; i < count; ++i) {
                auto raw = ptr->Reverse_Lookup(seg_offsets[i]);
                // if has no value, means nullable must be true, no need to check nullable again here
                if (!raw.has_value()) {
                    valid_data[i] = false;
                    continue;
                }
                if (nullable) {
                    valid_data[i] = true;
                }
                raw_data[i] = raw.value();
            }
            auto obj = scalar_array->mutable_long_data();
            *(obj->mutable_data()) = {raw_data.begin(), raw_data.end()};
            break;
        }
        case DataType::FLOAT: {
            using IndexType = index::ScalarIndex<float>;
            auto ptr = dynamic_cast<const IndexType*>(index);
            std::vector<float> raw_data(count);
            for (int64_t i = 0; i < count; ++i) {
                auto raw = ptr->Reverse_Lookup(seg_offsets[i]);
                // if has no value, means nullable must be true, no need to check nullable again here
                if (!raw.has_value()) {
                    valid_data[i] = false;
                    continue;
                }
                if (nullable) {
                    valid_data[i] = true;
                }
                raw_data[i] = raw.value();
            }
            auto obj = scalar_array->mutable_float_data();
            *(obj->mutable_data()) = {raw_data.begin(), raw_data.end()};
            break;
        }
        case DataType::DOUBLE: {
            using IndexType = index::ScalarIndex<double>;
            auto ptr = dynamic_cast<const IndexType*>(index);
            std::vector<double> raw_data(count);
            for (int64_t i = 0; i < count; ++i) {
                auto raw = ptr->Reverse_Lookup(seg_offsets[i]);
                // if has no value, means nullable must be true, no need to check nullable again here
                if (!raw.has_value()) {
                    valid_data[i] = false;
                    continue;
                }
                if (nullable) {
                    valid_data[i] = true;
                }
                raw_data[i] = raw.value();
            }
            auto obj = scalar_array->mutable_double_data();
            *(obj->mutable_data()) = {raw_data.begin(), raw_data.end()};
            break;
        }
        case DataType::TIMESTAMPTZ: {
            using IndexType = index::ScalarIndex<int64_t>;
            auto ptr = dynamic_cast<const IndexType*>(index);
            std::vector<int64_t> raw_data(count);
            for (int64_t i = 0; i < count; ++i) {
                auto raw = ptr->Reverse_Lookup(seg_offsets[i]);
                // if has no value, means nullable must be true, no need to check nullable again
                if (!raw.has_value()) {
                    valid_data[i] = false;
                    continue;
                }
                if (nullable) {
                    valid_data[i] = true;
                }
                raw_data[i] = raw.value();
                auto obj = scalar_array->mutable_timestamptz_data();
                *(obj->mutable_data()) = {raw_data.begin(), raw_data.end()};
                break;
            }
        }
        case DataType::VARCHAR: {
            using IndexType = index::ScalarIndex<std::string>;
            auto ptr = dynamic_cast<const IndexType*>(index);
            std::vector<std::string> raw_data(count);
            for (int64_t i = 0; i < count; ++i) {
                auto raw = ptr->Reverse_Lookup(seg_offsets[i]);
                // if has no value, means nullable must be true, no need to check nullable again here
                if (!raw.has_value()) {
                    valid_data[i] = false;
                    continue;
                }
                if (nullable) {
                    valid_data[i] = true;
                }
                raw_data[i] = raw.value();
            }
            auto obj = scalar_array->mutable_string_data();
            *(obj->mutable_data()) = {raw_data.begin(), raw_data.end()};
            break;
        }
        case DataType::GEOMETRY: {
            using IndexType = index::ScalarIndex<std::string>;
            auto ptr = dynamic_cast<const IndexType*>(index);
            std::vector<std::string> raw_data(count);
            for (int64_t i = 0; i < count; ++i) {
                auto raw = ptr->Reverse_Lookup(seg_offsets[i]);
                // if has no value, means nullable must be true, no need to check nullable again here
                if (!raw.has_value()) {
                    valid_data[i] = false;
                    continue;
                }
                if (nullable) {
                    valid_data[i] = true;
                }
                raw_data[i] = raw.value();
            }
            auto obj = scalar_array->mutable_geometry_data();
            *(obj->mutable_data()) = {raw_data.begin(), raw_data.end()};
            break;
        }
        default: {
            ThrowInfo(DataTypeInvalid,
                      fmt::format("unsupported datatype {}", data_type));
        }
    }

    if (nullable) {
        *(data_array->mutable_valid_data()) = {valid_data.begin(),
                                               valid_data.end()};
    }

    return data_array;
}

void
LoadArrowReaderForJsonStatsFromRemote(
    const std::vector<std::string>& remote_files,
    std::shared_ptr<ArrowReaderChannel> channel) {
    try {
        auto rcm = storage::RemoteChunkManagerSingleton::GetInstance()
                       .GetRemoteChunkManager();
        auto& pool = ThreadPools::GetThreadPool(ThreadPoolPriority::HIGH);

        std::vector<std::future<std::shared_ptr<milvus::ArrowDataWrapper>>>
            futures;
        futures.reserve(remote_files.size());
        for (const auto& file : remote_files) {
            auto future = pool.Submit([rcm, file]() {
                auto fileSize = rcm->Size(file);
                auto buf = std::shared_ptr<uint8_t[]>(new uint8_t[fileSize]);
                rcm->Read(file, buf.get(), fileSize);

                auto arrow_buf =
                    std::make_shared<arrow::Buffer>(buf.get(), fileSize);
                auto buffer_reader =
                    std::make_shared<arrow::io::BufferReader>(arrow_buf);

                std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
                auto status = parquet::arrow::OpenFile(
                    buffer_reader, arrow::default_memory_pool(), &arrow_reader);
                AssertInfo(status.ok(),
                           "failed to open parquet file: {}",
                           status.message());

                std::shared_ptr<arrow::RecordBatchReader> batch_reader;
                status = arrow_reader->GetRecordBatchReader(&batch_reader);
                AssertInfo(status.ok(),
                           "failed to get record batch reader: {}",
                           status.message());

                return std::make_shared<ArrowDataWrapper>(
                    std::move(batch_reader), std::move(arrow_reader), buf);
            });
            futures.emplace_back(std::move(future));
        }

        for (auto& future : futures) {
            auto field_data = future.get();
            channel->push(field_data);
        }

        channel->close();
    } catch (std::exception& e) {
        LOG_INFO("failed to load data from remote: {}", e.what());
        channel->close(std::current_exception());
    }
}

// init segcore storage config first, and create default remote chunk manager
// segcore use default remote chunk manager to load data from minio/s3
void
LoadArrowReaderFromRemote(const std::vector<std::string>& remote_files,
                          std::shared_ptr<ArrowReaderChannel> channel,
                          milvus::proto::common::LoadPriority priority) {
    try {
        auto rcm = storage::RemoteChunkManagerSingleton::GetInstance()
                       .GetRemoteChunkManager();

        auto codec_futures = storage::GetObjectData(
            rcm.get(), remote_files, milvus::PriorityForLoad(priority), false);
        for (auto& codec_future : codec_futures) {
            auto reader = codec_future.get()->GetReader();
            channel->push(reader);
        }
        channel->close();
    } catch (std::exception& e) {
        LOG_INFO("failed to load data from remote: {}", e.what());
        channel->close(std::current_exception());
    }
}

void
LoadFieldDatasFromRemote(const std::vector<std::string>& remote_files,
                         FieldDataChannelPtr channel,
                         milvus::proto::common::LoadPriority priority) {
    try {
        auto rcm = storage::RemoteChunkManagerSingleton::GetInstance()
                       .GetRemoteChunkManager();
        auto codec_futures = storage::GetObjectData(
            rcm.get(), remote_files, milvus::PriorityForLoad(priority));
        for (auto& codec_future : codec_futures) {
            auto field_data = codec_future.get()->GetFieldData();
            channel->push(field_data);
        }
        channel->close();
    } catch (std::exception& e) {
        LOG_INFO("failed to load data from remote: {}", e.what());
        channel->close(std::current_exception());
    }
}
int64_t
upper_bound(const ConcurrentVector<Timestamp>& timestamps,
            int64_t first,
            int64_t last,
            Timestamp value) {
    int64_t mid;
    while (first < last) {
        mid = first + (last - first) / 2;
        if (value >= timestamps[mid]) {
            first = mid + 1;
        } else {
            last = mid;
        }
    }
    return first;
}

// Get the globally configured cache warmup policy for the given content type.
CacheWarmupPolicy
getCacheWarmupPolicy(bool is_vector, bool is_index, bool in_load_list) {
    auto& manager = milvus::cachinglayer::Manager::GetInstance();
    // if field not in load list(hint), disable warmup
    if (!in_load_list) {
        return CacheWarmupPolicy::CacheWarmupPolicy_Disable;
    }
    if (is_index) {
        return is_vector ? manager.getVectorIndexCacheWarmupPolicy()
                         : manager.getScalarIndexCacheWarmupPolicy();
    } else {
        return is_vector ? manager.getVectorFieldCacheWarmupPolicy()
                         : manager.getScalarFieldCacheWarmupPolicy();
    }
}

milvus::cachinglayer::CellDataType
getCellDataType(bool is_vector, bool is_index) {
    if (is_index) {
        return is_vector ? milvus::cachinglayer::CellDataType::VECTOR_INDEX
                         : milvus::cachinglayer::CellDataType::SCALAR_INDEX;
    } else {
        return is_vector ? milvus::cachinglayer::CellDataType::VECTOR_FIELD
                         : milvus::cachinglayer::CellDataType::SCALAR_FIELD;
    }
}

void
LoadIndexData(milvus::tracer::TraceContext& ctx,
              milvus::segcore::LoadIndexInfo* load_index_info) {
    auto& index_params = load_index_info->index_params;
    auto field_type = load_index_info->field_type;
    auto engine_version = load_index_info->index_engine_version;

    milvus::index::CreateIndexInfo index_info;
    index_info.field_type = load_index_info->field_type;
    index_info.index_engine_version = engine_version;

    auto config = milvus::index::ParseConfigFromIndexParams(
        load_index_info->index_params);
    auto load_priority_str = config[milvus::LOAD_PRIORITY].get<std::string>();
    auto priority_for_load = milvus::PriorityForLoad(load_priority_str);
    config[milvus::LOAD_PRIORITY] = priority_for_load;

    // Config should have value for milvus::index::SCALAR_INDEX_ENGINE_VERSION for production calling chain.
    // Use value_or(1) for unit test without setting this value
    index_info.scalar_index_engine_version =
        milvus::index::GetValueFromConfig<int32_t>(
            config, milvus::index::SCALAR_INDEX_ENGINE_VERSION)
            .value_or(1);

    index_info.tantivy_index_version =
        milvus::index::GetValueFromConfig<int32_t>(
            config, milvus::index::TANTIVY_INDEX_VERSION)
            .value_or(milvus::index::TANTIVY_INDEX_LATEST_VERSION);

    LOG_INFO(
        "[collection={}][segment={}][field={}][enable_mmap={}][load_"
        "priority={}] load index {}, "
        "mmap_dir_path={}",
        load_index_info->collection_id,
        load_index_info->segment_id,
        load_index_info->field_id,
        load_index_info->enable_mmap,
        load_priority_str,
        load_index_info->index_id,
        load_index_info->mmap_dir_path);
    // get index type
    AssertInfo(index_params.find("index_type") != index_params.end(),
               "index type is empty");
    index_info.index_type = index_params.at("index_type");

    // get metric type
    if (milvus::IsVectorDataType(field_type)) {
        AssertInfo(index_params.find("metric_type") != index_params.end(),
                   "metric type is empty for vector index");
        index_info.metric_type = index_params.at("metric_type");
    }

    if (index_info.index_type == milvus::index::NGRAM_INDEX_TYPE) {
        AssertInfo(
            index_params.find(milvus::index::MIN_GRAM) != index_params.end(),
            "min_gram is empty for ngram index");
        AssertInfo(
            index_params.find(milvus::index::MAX_GRAM) != index_params.end(),
            "max_gram is empty for ngram index");

        // get min_gram and max_gram and convert to uintptr_t
        milvus::index::NgramParams ngram_params{};
        ngram_params.loading_index = true;
        ngram_params.min_gram =
            std::stoul(milvus::index::GetValueFromConfig<std::string>(
                           config, milvus::index::MIN_GRAM)
                           .value());
        ngram_params.max_gram =
            std::stoul(milvus::index::GetValueFromConfig<std::string>(
                           config, milvus::index::MAX_GRAM)
                           .value());
        index_info.ngram_params = std::make_optional(ngram_params);
    }

    // init file manager
    milvus::storage::FieldDataMeta field_meta{load_index_info->collection_id,
                                              load_index_info->partition_id,
                                              load_index_info->segment_id,
                                              load_index_info->field_id,
                                              load_index_info->schema};
    milvus::storage::IndexMeta index_meta{load_index_info->segment_id,
                                          load_index_info->field_id,
                                          load_index_info->index_build_id,
                                          load_index_info->index_version};
    config[milvus::index::INDEX_FILES] = load_index_info->index_files;

    if (load_index_info->field_type == milvus::DataType::JSON) {
        index_info.json_cast_type = milvus::JsonCastType::FromString(
            config.at(JSON_CAST_TYPE).get<std::string>());
        index_info.json_path = config.at(JSON_PATH).get<std::string>();
    }
    auto remote_chunk_manager =
        milvus::storage::RemoteChunkManagerSingleton::GetInstance()
            .GetRemoteChunkManager();
    auto fs = milvus_storage::ArrowFileSystemSingleton::GetInstance()
                  .GetArrowFileSystem();
    AssertInfo(fs != nullptr, "arrow file system is nullptr");
    milvus::storage::FileManagerContext file_manager_context(
        field_meta, index_meta, remote_chunk_manager, fs);
    file_manager_context.set_for_loading_index(true);

    // use cache layer to load vector/scalar index
    std::unique_ptr<milvus::cachinglayer::Translator<milvus::index::IndexBase>>
        translator = std::make_unique<
            milvus::segcore::storagev1translator::SealedIndexTranslator>(
            index_info, load_index_info, ctx, file_manager_context, config);

    load_index_info->cache_index =
        milvus::cachinglayer::Manager::GetInstance().CreateCacheSlot(
            std::move(translator));
}

}  // namespace milvus::segcore
