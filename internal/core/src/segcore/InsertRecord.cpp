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

#include <sys/timeb.h>
#include "InsertRecord.h"

namespace milvus::segcore {

InsertRecord::InsertRecord(const Schema& schema, int64_t size_per_chunk)
    : row_ids_(size_per_chunk), timestamps_(size_per_chunk) {
    std::optional<FieldId> pk_field_id = schema.get_primary_field_id();

    for (auto& field : schema) {
        auto field_id = field.first;
        auto& field_meta = field.second;
        if (pk2offset_ == nullptr && pk_field_id.has_value() && pk_field_id.value() == field_id) {
            switch (field_meta.get_data_type()) {
                case DataType::INT64: {
                    pk2offset_ = std::make_unique<OffsetHashMap<int64_t>>();
                    break;
                }
                case DataType::VARCHAR: {
                    pk2offset_ = std::make_unique<OffsetHashMap<std::string>>();
                    break;
                }
            }
        }
        if (field_meta.is_vector()) {
            if (field_meta.get_data_type() == DataType::VECTOR_FLOAT) {
                this->append_field_data<FloatVector>(field_id, field_meta.get_dim(), size_per_chunk);
                continue;
            } else if (field_meta.get_data_type() == DataType::VECTOR_BINARY) {
                this->append_field_data<BinaryVector>(field_id, field_meta.get_dim(), size_per_chunk);
                continue;
            } else {
                PanicInfo("unsupported");
            }
        }
        switch (field_meta.get_data_type()) {
            case DataType::BOOL: {
                this->append_field_data<bool>(field_id, size_per_chunk);
                break;
            }
            case DataType::INT8: {
                this->append_field_data<int8_t>(field_id, size_per_chunk);
                break;
            }
            case DataType::INT16: {
                this->append_field_data<int16_t>(field_id, size_per_chunk);
                break;
            }
            case DataType::INT32: {
                this->append_field_data<int32_t>(field_id, size_per_chunk);
                break;
            }
            case DataType::INT64: {
                this->append_field_data<int64_t>(field_id, size_per_chunk);
                break;
            }
            case DataType::FLOAT: {
                this->append_field_data<float>(field_id, size_per_chunk);
                break;
            }
            case DataType::DOUBLE: {
                this->append_field_data<double>(field_id, size_per_chunk);
                break;
            }
            case DataType::VARCHAR: {
                this->append_field_data<std::string>(field_id, size_per_chunk);
                break;
            }
            default: {
                PanicInfo("unsupported");
            }
        }
    }
}

}  // namespace milvus::segcore
