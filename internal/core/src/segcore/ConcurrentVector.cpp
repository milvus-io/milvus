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

#include "segcore/ConcurrentVector.h"

namespace milvus::segcore {

void
VectorBase::set_data_raw(ssize_t element_offset,
                         ssize_t element_count,
                         const DataArray* data,
                         const FieldMeta& field_meta) {
    if (field_meta.is_vector()) {
        if (field_meta.get_data_type() == DataType::VECTOR_FLOAT) {
            return set_data_raw(element_offset, data->vectors().float_vector().data().data(), element_count);
        } else if (field_meta.get_data_type() == DataType::VECTOR_BINARY) {
            return set_data_raw(element_offset, data->vectors().binary_vector().data(), element_count);
        } else {
            PanicInfo("unsupported");
        }
    }

    switch (field_meta.get_data_type()) {
        case DataType::BOOL: {
            return set_data_raw(element_offset, data->scalars().bool_data().data().data(), element_count);
        }
        case DataType::INT8: {
            auto src_data = data->scalars().int_data().data();
            std::vector<int8_t> data_raw(src_data.size());
            std::copy_n(src_data.data(), src_data.size(), data_raw.data());
            return set_data_raw(element_offset, data_raw.data(), element_count);
        }
        case DataType::INT16: {
            auto src_data = data->scalars().int_data().data();
            std::vector<int16_t> data_raw(src_data.size());
            std::copy_n(src_data.data(), src_data.size(), data_raw.data());
            return set_data_raw(element_offset, data_raw.data(), element_count);
        }
        case DataType::INT32: {
            return set_data_raw(element_offset, data->scalars().int_data().data().data(), element_count);
        }
        case DataType::INT64: {
            return set_data_raw(element_offset, data->scalars().long_data().data().data(), element_count);
        }
        case DataType::UINT8: {
            auto src_data = data->scalars().uint_data().data();
            std::vector<uint8_t> data_raw(src_data.size());
            std::copy_n(src_data.data(), src_data.size(), data_raw.data());
            return set_data_raw(element_offset, data_raw.data(), element_count);
        }
        case DataType::UINT16: {
            auto src_data = data->scalars().uint_data().data();
            std::vector<uint16_t> data_raw(src_data.size());
            std::copy_n(src_data.data(), src_data.size(), data_raw.data());
            return set_data_raw(element_offset, data_raw.data(), element_count);
        }
        case DataType::UINT32: {
            return set_data_raw(element_offset, data->scalars().uint_data().data().data(), element_count);
        }
        case DataType::UINT64: {
            return set_data_raw(element_offset, data->scalars().ulong_data().data().data(), element_count);
        }
        case DataType::FLOAT: {
            return set_data_raw(element_offset, data->scalars().float_data().data().data(), element_count);
        }
        case DataType::DOUBLE: {
            return set_data_raw(element_offset, data->scalars().double_data().data().data(), element_count);
        }
        case DataType::VARCHAR: {
            auto begin = data->scalars().string_data().data().begin();
            auto end = data->scalars().string_data().data().end();
            std::vector<std::string> data_raw(begin, end);
            return set_data_raw(element_offset, data_raw.data(), element_count);
        }
        default: {
            PanicInfo("unsupported");
        }
    }
}

void
VectorBase::fill_chunk_data(ssize_t element_count, const DataArray* data, const FieldMeta& field_meta) {
    if (field_meta.is_vector()) {
        if (field_meta.get_data_type() == DataType::VECTOR_FLOAT) {
            return fill_chunk_data(data->vectors().float_vector().data().data(), element_count);
        } else if (field_meta.get_data_type() == DataType::VECTOR_BINARY) {
            return fill_chunk_data(data->vectors().binary_vector().data(), element_count);
        } else {
            PanicInfo("unsupported");
        }
    }

    switch (field_meta.get_data_type()) {
        case DataType::BOOL: {
            return fill_chunk_data(data->scalars().bool_data().data().data(), element_count);
        }
        case DataType::INT8: {
            auto src_data = data->scalars().int_data().data();
            std::vector<int8_t> data_raw(src_data.size());
            std::copy_n(src_data.data(), src_data.size(), data_raw.data());
            return fill_chunk_data(data_raw.data(), element_count);
        }
        case DataType::INT16: {
            auto src_data = data->scalars().int_data().data();
            std::vector<int16_t> data_raw(src_data.size());
            std::copy_n(src_data.data(), src_data.size(), data_raw.data());
            return fill_chunk_data(data_raw.data(), element_count);
        }
        case DataType::INT32: {
            return fill_chunk_data(data->scalars().int_data().data().data(), element_count);
        }
        case DataType::INT64: {
            return fill_chunk_data(data->scalars().long_data().data().data(), element_count);
        }
        case DataType::UINT8: {
            auto src_data = data->scalars().uint_data().data();
            std::vector<uint8_t> data_raw(src_data.size());
            std::copy_n(src_data.data(), src_data.size(), data_raw.data());
            return fill_chunk_data(data_raw.data(), element_count);
        }
        case DataType::UINT16: {
            auto src_data = data->scalars().uint_data().data();
            std::vector<uint16_t> data_raw(src_data.size());
            std::copy_n(src_data.data(), src_data.size(), data_raw.data());
            return fill_chunk_data(data_raw.data(), element_count);
        }
        case DataType::UINT32: {
            return fill_chunk_data(data->scalars().uint_data().data().data(), element_count);
        }
        case DataType::UINT64: {
            return fill_chunk_data(data->scalars().ulong_data().data().data(), element_count);
        }
        case DataType::FLOAT: {
            return fill_chunk_data(data->scalars().float_data().data().data(), element_count);
        }
        case DataType::DOUBLE: {
            return fill_chunk_data(data->scalars().double_data().data().data(), element_count);
        }
        case DataType::VARCHAR: {
            auto begin = data->scalars().string_data().data().begin();
            auto end = data->scalars().string_data().data().end();
            std::vector<std::string> data_raw(begin, end);
            return fill_chunk_data(data_raw.data(), element_count);
        }
        default: {
            PanicInfo("unsupported");
        }
    }
}
}  // namespace milvus::segcore
