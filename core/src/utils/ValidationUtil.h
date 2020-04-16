// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once

#include "db/Types.h"
#include "db/meta/MetaTypes.h"
#include "utils/Json.h"
#include "utils/Status.h"

#include <string>
#include <vector>

namespace milvus {
namespace server {

constexpr int64_t QUERY_MAX_TOPK = 2048;

class ValidationUtil {
 private:
    ValidationUtil() = default;

 public:
    static Status
    ValidateCollectionName(const std::string& collection_name);

    static Status
    ValidateTableDimension(int64_t dimension, int64_t metric_type);

    static Status
    ValidateCollectionIndexType(int32_t index_type);

    static Status
    ValidateIndexParams(const milvus::json& index_params, const engine::meta::CollectionSchema& table_schema,
                        int32_t index_type);

    static Status
    ValidateSearchParams(const milvus::json& search_params, const engine::meta::CollectionSchema& table_schema,
                         int64_t topk);

    static Status
    ValidateVectorData(const engine::VectorsData& vectors, const engine::meta::CollectionSchema& table_schema);

    static Status
    ValidateVectorDataSize(const engine::VectorsData& vectors, const engine::meta::CollectionSchema& table_schema);

    static Status
    ValidateCollectionIndexFileSize(int64_t index_file_size);

    static Status
    ValidateCollectionIndexMetricType(int32_t metric_type);

    static Status
    ValidateSearchTopk(int64_t top_k);

    static Status
    ValidatePartitionName(const std::string& partition_name);

    static Status
    ValidatePartitionTags(const std::vector<std::string>& partition_tags);

    static Status
    ValidateGpuIndex(int32_t gpu_index);

#ifdef MILVUS_GPU_VERSION
    static Status
    GetGpuMemory(int32_t gpu_index, size_t& memory);
#endif

    static Status
    ValidateIpAddress(const std::string& ip_address);

    static Status
    ValidateStringIsNumber(const std::string& str);

    static Status
    ValidateStringIsBool(const std::string& str);

    static Status
    ValidateStringIsFloat(const std::string& str);

    static Status
    ValidateDbURI(const std::string& uri);

    static Status
    ValidateStoragePath(const std::string& path);
};

}  // namespace server
}  // namespace milvus
