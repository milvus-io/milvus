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
                entity_vec_.emplace_back(std::make_shared<ConcurrentVector<FloatVector>>(field.get_dim(), chunk_size));
                continue;
            } else if (field.get_data_type() == DataType::VECTOR_BINARY) {
                entity_vec_.emplace_back(std::make_shared<ConcurrentVector<BinaryVector>>(field.get_dim(), chunk_size));
                continue;
            } else {
                PanicInfo("unsupported");
            }
        }
        switch (field.get_data_type()) {
            case DataType::BOOL: {
                entity_vec_.emplace_back(std::make_shared<ConcurrentVector<bool>>(chunk_size));
                break;
            }
            case DataType::INT8: {
                entity_vec_.emplace_back(std::make_shared<ConcurrentVector<int8_t>>(chunk_size));
                break;
            }
            case DataType::INT16: {
                entity_vec_.emplace_back(std::make_shared<ConcurrentVector<int16_t>>(chunk_size));
                break;
            }
            case DataType::INT32: {
                entity_vec_.emplace_back(std::make_shared<ConcurrentVector<int32_t>>(chunk_size));
                break;
            }

            case DataType::INT64: {
                entity_vec_.emplace_back(std::make_shared<ConcurrentVector<int64_t>>(chunk_size));
                break;
            }

            case DataType::FLOAT: {
                entity_vec_.emplace_back(std::make_shared<ConcurrentVector<float>>(chunk_size));
                break;
            }

            case DataType::DOUBLE: {
                entity_vec_.emplace_back(std::make_shared<ConcurrentVector<double>>(chunk_size));
                break;
            }
            default: {
                PanicInfo("unsupported");
            }
        }
    }
}
}  // namespace milvus::segcore
