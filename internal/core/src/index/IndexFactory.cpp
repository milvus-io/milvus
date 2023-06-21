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
#include "index/VectorMemNMIndex.h"
#include "index/Utils.h"
#include "index/Meta.h"

#ifdef BUILD_DISK_ANN
#include "index/VectorDiskIndex.h"
#endif

namespace milvus::index {

IndexBasePtr
IndexFactory::CreateIndex(const CreateIndexInfo& create_index_info,
                          storage::FileManagerImplPtr file_manager) {
    if (datatype_is_vector(create_index_info.field_type)) {
        return CreateVectorIndex(create_index_info, file_manager);
    }

    return CreateScalarIndex(create_index_info, file_manager);
}

IndexBasePtr
IndexFactory::CreateScalarIndex(const CreateIndexInfo& create_index_info,
                                storage::FileManagerImplPtr file_manager) {
    auto data_type = create_index_info.field_type;
    auto index_type = create_index_info.index_type;

    switch (data_type) {
        // create scalar index
        case DataType::BOOL:
            return CreateScalarIndex<bool>(index_type, file_manager);
        case DataType::INT8:
            return CreateScalarIndex<int8_t>(index_type, file_manager);
        case DataType::INT16:
            return CreateScalarIndex<int16_t>(index_type, file_manager);
        case DataType::INT32:
            return CreateScalarIndex<int32_t>(index_type, file_manager);
        case DataType::INT64:
            return CreateScalarIndex<int64_t>(index_type, file_manager);
        case DataType::FLOAT:
            return CreateScalarIndex<float>(index_type, file_manager);
        case DataType::DOUBLE:
            return CreateScalarIndex<double>(index_type, file_manager);

            // create string index
        case DataType::STRING:
        case DataType::VARCHAR:
            return CreateScalarIndex<std::string>(index_type, file_manager);
        default:
            throw std::invalid_argument(
                std::string("invalid data type to build index: ") +
                std::to_string(int(data_type)));
    }
}

IndexBasePtr
IndexFactory::CreateVectorIndex(const CreateIndexInfo& create_index_info,
                                storage::FileManagerImplPtr file_manager) {
    auto data_type = create_index_info.field_type;
    auto index_type = create_index_info.index_type;
    auto metric_type = create_index_info.metric_type;

#ifdef BUILD_DISK_ANN
    // create disk index
    if (is_in_disk_list(index_type)) {
        switch (data_type) {
            case DataType::VECTOR_FLOAT: {
                return std::make_unique<VectorDiskAnnIndex<float>>(
                    index_type, metric_type, file_manager);
            }
            default:
                throw std::invalid_argument(
                    std::string("invalid data type to build disk index: ") +
                    std::to_string(int(data_type)));
        }
    }
#endif

    if (is_in_nm_list(index_type)) {
        return std::make_unique<VectorMemNMIndex>(
            index_type, metric_type, file_manager);
    }
    // create mem index
    return std::make_unique<VectorMemIndex>(
        index_type, metric_type, file_manager);
}

}  // namespace milvus::index
