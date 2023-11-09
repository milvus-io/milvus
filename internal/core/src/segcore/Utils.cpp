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

#include <memory>
#include <string>

#include "index/ScalarIndex.h"
#include "log/Log.h"
#include "storage/FieldData.h"
#include "storage/RemoteChunkManagerSingleton.h"
#include "common/Common.h"
#include "storage/ThreadPool.h"
#include "storage/Util.h"
#include "mmap/Utils.h"

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
                      const std::vector<storage::FieldDataPtr>& datas) {
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
    if (!datatype_is_variable(data_type)) {
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
        case DataType::VARCHAR: {
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
    auto dim = field_meta.get_dim();
    vector_array->set_dim(dim);
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
        default: {
            PanicInfo(DataTypeInvalid,
                      fmt::format("unsupported datatype {}", data_type));
        }
    }
    return data_array;
}

std::unique_ptr<DataArray>
CreateScalarDataArrayFrom(const void* data_raw,
                          int64_t count,
                          const FieldMeta& field_meta) {
    auto data_type = field_meta.get_data_type();
    auto data_array = std::make_unique<DataArray>();
    data_array->set_field_id(field_meta.get_id().get());
    data_array->set_type(static_cast<milvus::proto::schema::DataType>(
        field_meta.get_data_type()));

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
    auto dim = field_meta.get_dim();
    vector_array->set_dim(dim);
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
        default: {
            PanicInfo(DataTypeInvalid,
                      fmt::format("unsupported datatype {}", data_type));
        }
    }
    return data_array;
}

std::unique_ptr<DataArray>
CreateDataArrayFrom(const void* data_raw,
                    int64_t count,
                    const FieldMeta& field_meta) {
    auto data_type = field_meta.get_data_type();

    if (!datatype_is_vector(data_type)) {
        return CreateScalarDataArrayFrom(data_raw, count, field_meta);
    }

    return CreateVectorDataArrayFrom(data_raw, count, field_meta);
}

// TODO remove merge dataArray, instead fill target entity when get data slice
std::unique_ptr<DataArray>
MergeDataArray(
    std::vector<std::pair<milvus::SearchResult*, int64_t>>& result_offsets,
    const FieldMeta& field_meta) {
    auto data_type = field_meta.get_data_type();
    auto data_array = std::make_unique<DataArray>();
    data_array->set_field_id(field_meta.get_id().get());
    data_array->set_type(static_cast<milvus::proto::schema::DataType>(
        field_meta.get_data_type()));

    for (auto& result_pair : result_offsets) {
        auto src_field_data =
            result_pair.first->output_fields_data_[field_meta.get_id()].get();
        auto src_offset = result_pair.second;
        AssertInfo(data_type == DataType(src_field_data->type()),
                   "merge field data type not consistent");
        if (field_meta.is_vector()) {
            auto vector_array = data_array->mutable_vectors();
            auto dim = field_meta.get_dim();
            vector_array->set_dim(dim);
            if (field_meta.get_data_type() == DataType::VECTOR_FLOAT) {
                auto data = VEC_FIELD_DATA(src_field_data, float).data();
                auto obj = vector_array->mutable_float_vector();
                obj->mutable_data()->Add(data + src_offset * dim,
                                         data + (src_offset + 1) * dim);
            } else if (field_meta.get_data_type() == DataType::VECTOR_BINARY) {
                AssertInfo(
                    dim % 8 == 0,
                    "Binary vector field dimension is not a multiple of 8");
                auto num_bytes = dim / 8;
                auto data = VEC_FIELD_DATA(src_field_data, binary);
                auto obj = vector_array->mutable_binary_vector();
                obj->assign(data + src_offset * num_bytes, num_bytes);
            } else {
                PanicInfo(DataTypeInvalid,
                          fmt::format("unsupported datatype {}", data_type));
            }
            continue;
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

    auto scalar_array = data_array->mutable_scalars();
    switch (data_type) {
        case DataType::BOOL: {
            using IndexType = index::ScalarIndex<bool>;
            auto ptr = dynamic_cast<const IndexType*>(index);
            std::vector<bool> raw_data(count);
            for (int64_t i = 0; i < count; ++i) {
                raw_data[i] = ptr->Reverse_Lookup(seg_offsets[i]);
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
                raw_data[i] = ptr->Reverse_Lookup(seg_offsets[i]);
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
                raw_data[i] = ptr->Reverse_Lookup(seg_offsets[i]);
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
                raw_data[i] = ptr->Reverse_Lookup(seg_offsets[i]);
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
                raw_data[i] = ptr->Reverse_Lookup(seg_offsets[i]);
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
                raw_data[i] = ptr->Reverse_Lookup(seg_offsets[i]);
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
                raw_data[i] = ptr->Reverse_Lookup(seg_offsets[i]);
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
                raw_data[i] = ptr->Reverse_Lookup(seg_offsets[i]);
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

    return data_array;
}
// init segcore storage config first, and create default remote chunk manager
// segcore use default remote chunk manager to load data from minio/s3
void
LoadFieldDatasFromRemote(std::vector<std::string>& remote_files,
                         storage::FieldDataChannelPtr channel) {
    try {
        auto parallel_degree = static_cast<uint64_t>(
            DEFAULT_FIELD_MAX_MEMORY_LIMIT / FILE_SLICE_SIZE);

        auto rcm = storage::RemoteChunkManagerSingleton::GetInstance()
                       .GetRemoteChunkManager();
        std::sort(remote_files.begin(),
                  remote_files.end(),
                  [](const std::string& a, const std::string& b) {
                      return std::stol(a.substr(a.find_last_of('/') + 1)) <
                             std::stol(b.substr(b.find_last_of('/') + 1));
                  });

        std::vector<std::string> batch_files;

        auto FetchRawData = [&]() {
            auto result = storage::GetObjectData(rcm.get(), batch_files);
            for (auto& data : result) {
                channel->push(data);
            }
        };

        for (auto& file : remote_files) {
            if (batch_files.size() >= parallel_degree) {
                FetchRawData();
                batch_files.clear();
            }

            batch_files.emplace_back(file);
        }

        if (batch_files.size() > 0) {
            FetchRawData();
        }

        channel->close();
    } catch (std::exception e) {
        LOG_SEGCORE_INFO_ << "failed to load data from remote: " << e.what();
        channel->close(std::move(e));
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
