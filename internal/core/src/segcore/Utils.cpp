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

#include "common/Common.h"
#include "common/FieldData.h"
#include "common/Types.h"
#include "index/ScalarIndex.h"
#include "mmap/Utils.h"
#include "log/Log.h"
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
            PanicInfo(DataTypeInvalid,
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
                PanicInfo(DataTypeInvalid,
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
            PanicInfo(DataTypeInvalid,
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

    PanicInfo(DataTypeInvalid,
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
            case DataType::VARCHAR: {
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
                    case DataType::VARCHAR:
                    case DataType::STRING: {
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
                        PanicInfo(
                            DataTypeInvalid,
                            fmt::format("unsupported element type for array",
                                        field_meta.get_element_type()));
                }

                break;
            }
            case DataType::VECTOR_SPARSE_FLOAT: {
                // TODO(SPARSE, size)
                result += data->vectors().sparse_float_vector().ByteSizeLong();
                break;
            }
            default: {
                PanicInfo(
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
CreateScalarDataArray(int64_t count, const FieldMeta& field_meta) {
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
        case DataType::VARCHAR:
        case DataType::STRING: {
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
            PanicInfo(DataTypeInvalid,
                      fmt::format("unsupported datatype {}", data_type));
        }
    }

    return data_array;
}

std::unique_ptr<DataArray>
CreateVectorDataArray(int64_t count, const FieldMeta& field_meta) {
    auto data_type = field_meta.get_data_type();
    auto data_array = std::make_unique<DataArray>();
    data_array->set_field_id(field_meta.get_id().get());
    data_array->set_type(static_cast<milvus::proto::schema::DataType>(
        field_meta.get_data_type()));

    auto vector_array = data_array->mutable_vectors();
    auto dim = 0;
    if (data_type != DataType::VECTOR_SPARSE_FLOAT) {
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
        case DataType::VECTOR_SPARSE_FLOAT: {
            // does nothing here
            break;
        }
        default: {
            PanicInfo(DataTypeInvalid,
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
        case DataType::VARCHAR: {
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
        case DataType::ARRAY: {
            auto data = reinterpret_cast<const ScalarArray*>(data_raw);
            auto obj = scalar_array->mutable_array_data();
            obj->set_element_type(static_cast<milvus::proto::schema::DataType>(
                field_meta.get_element_type()));
            for (auto i = 0; i < count; i++) {
                *(obj->mutable_data()->Add()) = data[i];
            }
            break;
        }
        default: {
            PanicInfo(DataTypeInvalid,
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
        case DataType::VECTOR_SPARSE_FLOAT: {
            SparseRowsToProto(
                [&](size_t i) {
                    return reinterpret_cast<
                               const knowhere::sparse::SparseRow<float>*>(
                               data_raw) +
                           i;
                },
                count,
                vector_array->mutable_sparse_float_vector());
            vector_array->set_dim(vector_array->sparse_float_vector().dim());
            break;
        }
        default: {
            PanicInfo(DataTypeInvalid,
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

    if (!IsVectorDataType(data_type)) {
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
                       DataType::VECTOR_SPARSE_FLOAT) {
                auto src = src_field_data->vectors().sparse_float_vector();
                auto dst = vector_array->mutable_sparse_float_vector();
                if (src.dim() > dst->dim()) {
                    dst->set_dim(src.dim());
                }
                vector_array->set_dim(dst->dim());
                *dst->mutable_contents() = src.contents();
            } else {
                PanicInfo(DataTypeInvalid,
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
            case DataType::VARCHAR: {
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
            case DataType::ARRAY: {
                auto& data = FIELD_DATA(src_field_data, array);
                auto obj = scalar_array->mutable_array_data();
                obj->set_element_type(
                    proto::schema::DataType(field_meta.get_element_type()));
                *(obj->mutable_data()->Add()) = data[src_offset];
                break;
            }
            default: {
                PanicInfo(DataTypeInvalid,
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
        default: {
            PanicInfo(DataTypeInvalid,
                      fmt::format("unsupported datatype {}", data_type));
        }
    }

    if (nullable) {
        *(data_array->mutable_valid_data()) = {valid_data.begin(),
                                               valid_data.end()};
    }

    return data_array;
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
        auto& pool =
            ThreadPools::GetThreadPool(milvus::PriorityForLoad(priority));
        std::vector<std::future<std::shared_ptr<milvus::ArrowDataWrapper>>>
            futures;
        futures.reserve(remote_files.size());
        for (const auto& file : remote_files) {
            auto future = pool.Submit([rcm, file]() {
                auto fileSize = rcm->Size(file);
                auto buf = std::shared_ptr<uint8_t[]>(new uint8_t[fileSize]);
                rcm->Read(file, buf.get(), fileSize);
                auto result =
                    storage::DeserializeFileData(buf, fileSize, false);
                result->SetData(buf);
                return result->GetReader();
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

void
LoadFieldDatasFromRemote(const std::vector<std::string>& remote_files,
                         FieldDataChannelPtr channel,
                         milvus::proto::common::LoadPriority priority) {
    try {
        auto rcm = storage::RemoteChunkManagerSingleton::GetInstance()
                       .GetRemoteChunkManager();
        auto& pool =
            ThreadPools::GetThreadPool(milvus::PriorityForLoad(priority));
        std::vector<std::future<FieldDataPtr>> futures;
        futures.reserve(remote_files.size());
        for (const auto& file : remote_files) {
            auto future = pool.Submit([rcm, file]() {
                auto fileSize = rcm->Size(file);
                auto buf = std::shared_ptr<uint8_t[]>(new uint8_t[fileSize]);
                rcm->Read(file, buf.get(), fileSize);
                auto result = storage::DeserializeFileData(buf, fileSize);
                return result->GetFieldData();
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
}  // namespace milvus::segcore
