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
    auto raw_array_data = data->data->data();
    if (field_meta.is_vector()) {
        if (field_meta.get_data_type() == DataType::VECTOR_FLOAT) {
            return set_data_raw(
                element_offset,
                reinterpret_cast<const float*>(arrow::FixedSizeBinaryArray(raw_array_data).raw_values()),
                element_count);
        } else if (field_meta.get_data_type() == DataType::VECTOR_BINARY) {
            return set_data_raw(element_offset, arrow::FixedSizeBinaryArray(raw_array_data).raw_values(),
                                element_count);
        } else {
            PanicInfo("unsupported");
        }
    }

    switch (field_meta.get_data_type()) {
        case DataType::BOOL: {
            auto src_data = arrow::BooleanArray(raw_array_data);
            bool data_raw[src_data.length()];
            for (auto iter = src_data.begin(); iter != src_data.end(); iter++) {
                data_raw[iter.index()] = src_data.Value(iter.index());
            }

            return set_data_raw(element_offset, data_raw, element_count);
        }
        case DataType::INT8: {
            return set_data_raw(element_offset, arrow::Int8Array(raw_array_data).raw_values(), element_count);
        }
        case DataType::INT16: {
            return set_data_raw(element_offset, arrow::Int16Array(raw_array_data).raw_values(), element_count);
        }
        case DataType::INT32: {
            return set_data_raw(element_offset, arrow::Int32Array(raw_array_data).raw_values(), element_count);
        }
        case DataType::INT64: {
            return set_data_raw(element_offset, arrow::Int64Array(raw_array_data).raw_values(), element_count);
        }
        case DataType::FLOAT: {
            return set_data_raw(element_offset, arrow::FloatArray(raw_array_data).raw_values(), element_count);
        }
        case DataType::DOUBLE: {
            return set_data_raw(element_offset, arrow::DoubleArray(raw_array_data).raw_values(), element_count);
        }
        case DataType::VARCHAR: {
            auto src_data = arrow::StringArray(raw_array_data);
            std::vector<std::string> data_raw(src_data.length());
            std::transform(src_data.begin(), src_data.end(), data_raw.begin(),
                           [](auto item) { return std::string(*item); });
            return set_data_raw(element_offset, data_raw.data(), element_count);
        }
        default: {
            PanicInfo("unsupported");
        }
    }
}

void
VectorBase::fill_chunk_data(ssize_t element_count, const DataArray* data, const FieldMeta& field_meta) {
    auto raw_array_data = data->data->data();
    if (field_meta.is_vector()) {
        if (field_meta.get_data_type() == DataType::VECTOR_FLOAT) {
            return fill_chunk_data(
                reinterpret_cast<const float*>(arrow::FixedSizeBinaryArray(raw_array_data).raw_values()),
                element_count);
        } else if (field_meta.get_data_type() == DataType::VECTOR_BINARY) {
            return fill_chunk_data(arrow::FixedSizeBinaryArray(raw_array_data).raw_values(), element_count);
        } else {
            PanicInfo("unsupported");
        }
    }

    switch (field_meta.get_data_type()) {
        case DataType::BOOL: {
            auto src_data = arrow::BooleanArray(raw_array_data);
            bool data_raw[src_data.length()];
            for (auto iter = src_data.begin(); iter != src_data.end(); iter++) {
                data_raw[iter.index()] = src_data.Value(iter.index());
            }

            return fill_chunk_data(data_raw, element_count);
        }
        case DataType::INT8: {
            return fill_chunk_data(arrow::Int8Array(raw_array_data).raw_values(), element_count);
        }
        case DataType::INT16: {
            return fill_chunk_data(arrow::Int16Array(raw_array_data).raw_values(), element_count);
        }
        case DataType::INT32: {
            return fill_chunk_data(arrow::Int32Array(raw_array_data).raw_values(), element_count);
        }
        case DataType::INT64: {
            return fill_chunk_data(arrow::Int64Array(raw_array_data).raw_values(), element_count);
        }
        case DataType::FLOAT: {
            return fill_chunk_data(arrow::FloatArray(raw_array_data).raw_values(), element_count);
        }
        case DataType::DOUBLE: {
            return fill_chunk_data(arrow::DoubleArray(raw_array_data).raw_values(), element_count);
        }
        case DataType::VARCHAR: {
            auto src_data = arrow::StringArray(raw_array_data);
            std::vector<std::string> data_raw(src_data.length());
            std::transform(src_data.begin(), src_data.end(), data_raw.begin(),
                           [](auto item) { return std::string(*item); });
            return fill_chunk_data(data_raw.data(), element_count);
        }
        default: {
            PanicInfo("unsupported");
        }
    }
}
}  // namespace milvus::segcore
