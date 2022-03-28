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

#include "index/IndexFactory.h"
#include "index/ScalarIndexSort.h"

namespace milvus::scalar {

IndexBasePtr
IndexFactory::CreateIndex(CDataType dtype, std::string index_type) {
    switch (dtype) {
        case Bool:
            return CreateIndex<bool>(index_type);
        case Int8:
            return CreateIndex<int8_t>(index_type);
        case Int16:
            return CreateIndex<int16_t>(index_type);
        case Int32:
            return CreateIndex<int32_t>(index_type);
        case Int64:
            return CreateIndex<int64_t>(index_type);
        case Float:
            return CreateIndex<float>(index_type);
        case Double:
            return CreateIndex<double>(index_type);
        case String:
        case VarChar:
            return CreateIndex<std::string>(index_type);
        case None:
        case BinaryVector:
        case FloatVector:
        default:
            throw std::invalid_argument(std::string("invalid data type: ") + std::to_string(dtype));
    }
}

}  // namespace milvus::scalar
