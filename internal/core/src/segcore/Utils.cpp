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
#include "index/ScalarIndex.h"

namespace milvus::segcore {

void
ParsePksFromFieldData(std::vector<PkType>& pks, const DataArray& data) {
    switch (DataType(data.type())) {
        case DataType::INT64: {
            auto source_data = reinterpret_cast<const int64_t*>(data.scalars().long_data().data().data());
            std::copy_n(source_data, pks.size(), pks.data());
            break;
        }
        case DataType::VARCHAR: {
            auto src_data = data.scalars().string_data().data();
            std::copy(src_data.begin(), src_data.end(), pks.begin());
            break;
        }
        default: {
            PanicInfo("unsupported");
        }
    }
}

void
ParsePksFromIDs(std::vector<PkType>& pks, DataType data_type, const IdArray& data) {
    switch (data_type) {
        case DataType::INT64: {
            auto source_data = reinterpret_cast<const int64_t*>(data.int_id().data().data());
            std::copy_n(source_data, pks.size(), pks.data());
            break;
        }
        case DataType::VARCHAR: {
            auto source_data = data.str_id().data();
            std::copy(source_data.begin(), source_data.end(), pks.begin());
            break;
        }
        default: {
            PanicInfo("unsupported");
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

    PanicInfo("unsupported id type");
}

// Note: this is temporary solution.
// modify bulk script implement to make process more clear
std::unique_ptr<DataArray>
CreateScalarDataArrayFrom(const void* data_raw, int64_t count, const FieldMeta& field_meta) {
    auto data_type = field_meta.get_data_type();
    auto data_array = std::make_unique<DataArray>();
    data_array->set_field_id(field_meta.get_id().get());
    data_array->set_type(milvus::proto::schema::DataType(field_meta.get_data_type()));

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
            for (auto i = 0; i < count; i++) *(obj->mutable_data()->Add()) = data[i];
            break;
        }
        default: {
            PanicInfo("unsupported datatype");
        }
    }

    return data_array;
}

std::unique_ptr<DataArray>
CreateVectorDataArrayFrom(const void* data_raw, int64_t count, const FieldMeta& field_meta) {
    auto data_type = field_meta.get_data_type();
    auto data_array = std::make_unique<DataArray>();
    data_array->set_field_id(field_meta.get_id().get());
    data_array->set_type(milvus::proto::schema::DataType(field_meta.get_data_type()));

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
            AssertInfo(dim % 8 == 0, "Binary vector field dimension is not a multiple of 8");
            auto num_bytes = count * dim / 8;
            auto data = reinterpret_cast<const char*>(data_raw);
            auto obj = vector_array->mutable_binary_vector();
            obj->assign(data, num_bytes);
            break;
        }
        default: {
            PanicInfo("unsupported datatype");
        }
    }
    return data_array;
}

std::unique_ptr<DataArray>
CreateDataArrayFrom(const void* data_raw, int64_t count, const FieldMeta& field_meta) {
    auto data_type = field_meta.get_data_type();

    if (!datatype_is_vector(data_type)) {
        return CreateScalarDataArrayFrom(data_raw, count, field_meta);
    }

    return CreateVectorDataArrayFrom(data_raw, count, field_meta);
}

// TODO remove merge dataArray, instead fill target entity when get data slice
std::unique_ptr<DataArray>
MergeDataArray(std::vector<std::pair<milvus::SearchResult*, int64_t>>& result_offsets, const FieldMeta& field_meta) {
    auto data_type = field_meta.get_data_type();
    auto data_array = std::make_unique<DataArray>();
    data_array->set_field_id(field_meta.get_id().get());
    data_array->set_type(milvus::proto::schema::DataType(field_meta.get_data_type()));

    for (auto& result_pair : result_offsets) {
        auto src_field_data = result_pair.first->output_fields_data_[field_meta.get_id()].get();
        auto src_offset = result_pair.second;
        AssertInfo(data_type == DataType(src_field_data->type()), "merge field data type not consistent");
        if (field_meta.is_vector()) {
            auto vector_array = data_array->mutable_vectors();
            auto dim = field_meta.get_dim();
            vector_array->set_dim(dim);
            if (field_meta.get_data_type() == DataType::VECTOR_FLOAT) {
                auto data = src_field_data->vectors().float_vector().data().data();
                auto obj = vector_array->mutable_float_vector();
                obj->mutable_data()->Add(data + src_offset * dim, data + (src_offset + 1) * dim);
            } else if (field_meta.get_data_type() == DataType::VECTOR_BINARY) {
                AssertInfo(dim % 8 == 0, "Binary vector field dimension is not a multiple of 8");
                auto num_bytes = dim / 8;
                auto data = src_field_data->vectors().binary_vector().data();
                auto obj = vector_array->mutable_binary_vector();
                obj->assign(data + src_offset * num_bytes, num_bytes);
            } else {
                PanicInfo("logical error");
            }
            continue;
        }

        auto scalar_array = data_array->mutable_scalars();
        switch (data_type) {
            case DataType::BOOL: {
                auto data = src_field_data->scalars().bool_data().data().data();
                auto obj = scalar_array->mutable_bool_data();
                *(obj->mutable_data()->Add()) = data[src_offset];
                continue;
            }
            case DataType::INT8:
            case DataType::INT16:
            case DataType::INT32: {
                auto data = src_field_data->scalars().int_data().data().data();
                auto obj = scalar_array->mutable_int_data();
                *(obj->mutable_data()->Add()) = data[src_offset];
                continue;
            }
            case DataType::INT64: {
                auto data = src_field_data->scalars().long_data().data().data();
                auto obj = scalar_array->mutable_long_data();
                *(obj->mutable_data()->Add()) = data[src_offset];
                continue;
            }
            case DataType::FLOAT: {
                auto data = src_field_data->scalars().float_data().data().data();
                auto obj = scalar_array->mutable_float_data();
                *(obj->mutable_data()->Add()) = data[src_offset];
                continue;
            }
            case DataType::DOUBLE: {
                auto data = src_field_data->scalars().double_data().data().data();
                auto obj = scalar_array->mutable_double_data();
                *(obj->mutable_data()->Add()) = data[src_offset];
                continue;
            }
            case DataType::VARCHAR: {
                auto data = src_field_data->scalars().string_data();
                auto obj = scalar_array->mutable_string_data();
                *(obj->mutable_data()->Add()) = data.data(src_offset);
                continue;
            }
            default: {
                PanicInfo("unsupported datatype");
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
    data_array->set_type(milvus::proto::schema::DataType(field_meta.get_data_type()));

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
            PanicInfo("unsupported datatype");
        }
    }

    return data_array;
}

}  // namespace milvus::segcore
