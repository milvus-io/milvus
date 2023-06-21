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
#include "common/Types.h"
#include "common/Utils.h"
#include "nlohmann/json.hpp"
#include "simdjson.h"

namespace milvus::segcore {

void
VectorBase::set_data_raw(ssize_t element_offset,
                         ssize_t element_count,
                         const DataArray* data,
                         const FieldMeta& field_meta) {
    if (field_meta.is_vector()) {
        if (field_meta.get_data_type() == DataType::VECTOR_FLOAT) {
            return set_data_raw(element_offset,
                                VEC_FIELD_DATA(data, float).data(),
                                element_count);
        } else if (field_meta.get_data_type() == DataType::VECTOR_BINARY) {
            return set_data_raw(
                element_offset, VEC_FIELD_DATA(data, binary), element_count);
        } else {
            PanicInfo("unsupported");
        }
    }

    switch (field_meta.get_data_type()) {
        case DataType::BOOL: {
            return set_data_raw(
                element_offset, FIELD_DATA(data, bool).data(), element_count);
        }
        case DataType::INT8: {
            auto& src_data = FIELD_DATA(data, int);
            std::vector<int8_t> data_raw(src_data.size());
            std::copy_n(src_data.data(), src_data.size(), data_raw.data());
            return set_data_raw(element_offset, data_raw.data(), element_count);
        }
        case DataType::INT16: {
            auto& src_data = FIELD_DATA(data, int);
            std::vector<int16_t> data_raw(src_data.size());
            std::copy_n(src_data.data(), src_data.size(), data_raw.data());
            return set_data_raw(element_offset, data_raw.data(), element_count);
        }
        case DataType::INT32: {
            return set_data_raw(
                element_offset, FIELD_DATA(data, int).data(), element_count);
        }
        case DataType::INT64: {
            return set_data_raw(
                element_offset, FIELD_DATA(data, long).data(), element_count);
        }
        case DataType::FLOAT: {
            return set_data_raw(
                element_offset, FIELD_DATA(data, float).data(), element_count);
        }
        case DataType::DOUBLE: {
            return set_data_raw(
                element_offset, FIELD_DATA(data, double).data(), element_count);
        }
        case DataType::VARCHAR: {
            auto& field_data = FIELD_DATA(data, string);
            std::vector<std::string> data_raw(field_data.begin(),
                                              field_data.end());
            return set_data_raw(element_offset, data_raw.data(), element_count);
        }
        case DataType::JSON: {
            auto& json_data = FIELD_DATA(data, json);
            std::vector<Json> data_raw{};
            data_raw.reserve(json_data.size());
            for (auto& json_bytes : json_data) {
                data_raw.emplace_back(simdjson::padded_string(json_bytes));
            }

            return set_data_raw(element_offset, data_raw.data(), element_count);
        }
        default: {
            PanicInfo(fmt::format("unsupported datatype {}",
                                  field_meta.get_data_type()));
        }
    }
}

}  // namespace milvus::segcore
