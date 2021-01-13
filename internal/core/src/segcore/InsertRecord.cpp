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

InsertRecord::InsertRecord(const Schema& schema, int64_t chunk_size) : uids_(1), timestamps_(1) {
    for (auto& field : schema) {
        if (field.is_vector()) {
            if (field.get_data_type() == DataType::VECTOR_FLOAT) {
                this->insert_entity<FloatVector>(field.get_dim(), chunk_size);
                continue;
            } else if (field.get_data_type() == DataType::VECTOR_BINARY) {
                this->insert_entity<BinaryVector>(field.get_dim(), chunk_size);
                continue;
            } else {
                PanicInfo("unsupported");
            }
        }
        switch (field.get_data_type()) {
            case DataType::BOOL: {
                this->insert_entity<bool>(chunk_size);
                break;
            }
            case DataType::INT8: {
                this->insert_entity<int8_t>(chunk_size);
                break;
            }
            case DataType::INT16: {
                this->insert_entity<int16_t>(chunk_size);
                break;
            }
            case DataType::INT32: {
                this->insert_entity<int32_t>(chunk_size);
                break;
            }

            case DataType::INT64: {
                this->insert_entity<int64_t>(chunk_size);
                break;
            }

            case DataType::FLOAT: {
                this->insert_entity<float>(chunk_size);
                break;
            }

            case DataType::DOUBLE: {
                this->insert_entity<double>(chunk_size);
                break;
            }
            default: {
                PanicInfo("unsupported");
            }
        }
    }
}
}  // namespace milvus::segcore
