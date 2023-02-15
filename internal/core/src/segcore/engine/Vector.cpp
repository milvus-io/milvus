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

#include "segcore/engine/Vector.h"

namespace milvus::engine {

facebook::velox::TypePtr
ToVeloxType(const milvus::DataType type) {
    switch (type) {
        case milvus::DataType::BOOL:
            return facebook::velox::BOOLEAN();
        case milvus::DataType::INT8:
            return facebook::velox::TINYINT();
        case milvus::DataType::INT16:
            return facebook::velox::SMALLINT();
        case milvus::DataType::INT32:
            return facebook::velox::INTEGER();
        case milvus::DataType::INT64:
            return facebook::velox::BIGINT();
        case milvus::DataType::FLOAT:
            return facebook::velox::REAL();
        case milvus::DataType::DOUBLE:
            return facebook::velox::DOUBLE();
        case milvus::DataType::STRING:
        case milvus::DataType::VARCHAR:
            return facebook::velox::VARCHAR();
        default:
            PanicInfo("unsupported data type");
    }
}
}  // namespace milvus::engine
