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

#include "InsertRecord.h"

namespace milvus::segcore {

InsertRecord::InsertRecord(const Schema& schema, int64_t size_per_chunk)
    : uids_(size_per_chunk), timestamps_(size_per_chunk) {
    for (auto& field : schema) {
        if (field.is_vector()) {
            if (field.get_data_type() == DataType::VECTOR_FLOAT) {
                this->append_field_data<FloatVector>(field.get_dim(), size_per_chunk);
                continue;
            } else if (field.get_data_type() == DataType::VECTOR_BINARY) {
                this->append_field_data<BinaryVector>(field.get_dim(), size_per_chunk);
                continue;
            } else {
                PanicInfo("unsupported");
            }
        }
        switch (field.get_data_type()) {
            case DataType::BOOL: {
                this->append_field_data<bool>(size_per_chunk);
                break;
            }
            case DataType::INT8: {
                this->append_field_data<int8_t>(size_per_chunk);
                break;
            }
            case DataType::INT16: {
                this->append_field_data<int16_t>(size_per_chunk);
                break;
            }
            case DataType::INT32: {
                this->append_field_data<int32_t>(size_per_chunk);
                break;
            }

            case DataType::INT64: {
                this->append_field_data<int64_t>(size_per_chunk);
                break;
            }

            case DataType::FLOAT: {
                this->append_field_data<float>(size_per_chunk);
                break;
            }

            case DataType::DOUBLE: {
                this->append_field_data<double>(size_per_chunk);
                break;
            }
            default: {
                PanicInfo("unsupported");
            }
        }
    }
}
}  // namespace milvus::segcore
