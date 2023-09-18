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

#include "index/IndexFactory.h"
#include "index/VectorMemIndex.h"
#include "index/Utils.h"
#include "index/Meta.h"
#include "knowhere/utils.h"

#include "index/VectorDiskIndex.h"

namespace milvus::index {

IndexBasePtr
IndexFactory::CreateIndex(
    const CreateIndexInfo& create_index_info,
    const storage::FileManagerContext& file_manager_context) {
    if (datatype_is_vector(create_index_info.field_type)) {
        return CreateVectorIndex(create_index_info, file_manager_context);
    }

    return CreateScalarIndex(create_index_info, file_manager_context);
}

IndexBasePtr
IndexFactory::CreateScalarIndex(
    const CreateIndexInfo& create_index_info,
    const storage::FileManagerContext& file_manager_context) {
    auto data_type = create_index_info.field_type;
    auto index_type = create_index_info.index_type;

    switch (data_type) {
        // create scalar index
        case DataType::BOOL:
            return CreateScalarIndex<bool>(index_type, file_manager_context);
        case DataType::INT8:
            return CreateScalarIndex<int8_t>(index_type, file_manager_context);
        case DataType::INT16:
            return CreateScalarIndex<int16_t>(index_type, file_manager_context);
        case DataType::INT32:
            return CreateScalarIndex<int32_t>(index_type, file_manager_context);
        case DataType::INT64:
            return CreateScalarIndex<int64_t>(index_type, file_manager_context);
        case DataType::FLOAT:
            return CreateScalarIndex<float>(index_type, file_manager_context);
        case DataType::DOUBLE:
            return CreateScalarIndex<double>(index_type, file_manager_context);

            // create string index
        case DataType::STRING:
        case DataType::VARCHAR:
            return CreateScalarIndex<std::string>(index_type,
                                                  file_manager_context);
        default:
            throw SegcoreError(
                DataTypeInvalid,
                fmt::format("invalid data type to build index: {}", data_type));
    }
}

IndexBasePtr
IndexFactory::CreateVectorIndex(
    const CreateIndexInfo& create_index_info,
    const storage::FileManagerContext& file_manager_context) {
    auto index_type = create_index_info.index_type;
    auto metric_type = create_index_info.metric_type;
    auto version = create_index_info.index_engine_version;
    // create disk index
    auto data_type = create_index_info.field_type;
    if (knowhere::UseDiskLoad(index_type, version)) {
        switch (data_type) {
            case DataType::VECTOR_FLOAT: {
                return std::make_unique<VectorDiskAnnIndex<float>>(
                    index_type, metric_type, version, file_manager_context);
            }
            default:
                throw SegcoreError(
                    DataTypeInvalid,
                    fmt::format("invalid data type to build disk index: {}",
                                data_type));
        }
    }

    // create mem index
    return std::make_unique<VectorMemIndex>(
        index_type, metric_type, version, file_manager_context);
}

}  // namespace milvus::index
