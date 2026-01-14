// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "Types.h"
#include "common/Exception.h"
#include "common/EasyAssert.h"

const RowTypePtr RowType::None = std::make_shared<const RowType>(
    std::vector<std::string>{}, std::vector<milvus::DataType>{});
namespace milvus {
bool
IsFixedSizeType(DataType type) {
    switch (type) {
        case DataType::NONE:
            return false;
        case DataType::BOOL:
            return TypeTraits<DataType::BOOL>::IsFixedWidth;
        case DataType::INT8:
            return TypeTraits<DataType::INT8>::IsFixedWidth;
        case DataType::INT16:
            return TypeTraits<DataType::INT16>::IsFixedWidth;
        case DataType::INT32:
            return TypeTraits<DataType::INT32>::IsFixedWidth;
        case DataType::INT64:
            return TypeTraits<DataType::INT64>::IsFixedWidth;
        case DataType::TIMESTAMPTZ:
            return TypeTraits<DataType::TIMESTAMPTZ>::IsFixedWidth;
        case DataType::FLOAT:
            return TypeTraits<DataType::FLOAT>::IsFixedWidth;
        case DataType::DOUBLE:
            return TypeTraits<DataType::DOUBLE>::IsFixedWidth;
        case DataType::STRING:
            return TypeTraits<DataType::STRING>::IsFixedWidth;
        case DataType::VARCHAR:
            return TypeTraits<DataType::VARCHAR>::IsFixedWidth;
        case DataType::ARRAY:
            return TypeTraits<DataType::ARRAY>::IsFixedWidth;
        case DataType::JSON:
            return TypeTraits<DataType::JSON>::IsFixedWidth;
        case DataType::ROW:
            return TypeTraits<DataType::ROW>::IsFixedWidth;
        case DataType::VECTOR_BINARY:
            return TypeTraits<DataType::VECTOR_BINARY>::IsFixedWidth;
        case DataType::VECTOR_FLOAT:
            return TypeTraits<DataType::VECTOR_FLOAT>::IsFixedWidth;
        case DataType::GEOMETRY:
            return TypeTraits<DataType::GEOMETRY>::IsFixedWidth;
        case DataType::TEXT:
            return TypeTraits<DataType::TEXT>::IsFixedWidth;
        case DataType::VECTOR_FLOAT16:
            return TypeTraits<DataType::VECTOR_FLOAT16>::IsFixedWidth;
        case DataType::VECTOR_BFLOAT16:
            return TypeTraits<DataType::VECTOR_BFLOAT16>::IsFixedWidth;
        case DataType::VECTOR_SPARSE_U32_F32:
            return TypeTraits<DataType::VECTOR_SPARSE_U32_F32>::IsFixedWidth;
        case DataType::VECTOR_INT8:
            return TypeTraits<DataType::VECTOR_INT8>::IsFixedWidth;
        case DataType::VECTOR_ARRAY:
            return TypeTraits<DataType::VECTOR_ARRAY>::IsFixedWidth;
        default:
            ThrowInfo(DataTypeInvalid, "unknown data type: {}", type);
    }
}
}  // namespace milvus
