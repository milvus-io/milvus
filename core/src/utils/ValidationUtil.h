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

#include "db/meta/MetaTypes.h"
#include "utils/Status.h"

#include <string>
#include <vector>

namespace milvus {
namespace server {

class ValidationUtil {
 private:
    ValidationUtil() = default;

 public:
    static Status
    ValidateTableName(const std::string& table_name);

    static Status
    ValidateTableDimension(int64_t dimension);

    static Status
    ValidateTableIndexType(int32_t index_type);

    static bool
    IsBinaryIndexType(int32_t index_type);

    static Status
    ValidateTableIndexNlist(int32_t nlist);

    static Status
    ValidateTableIndexFileSize(int64_t index_file_size);

    static Status
    ValidateTableIndexMetricType(int32_t metric_type);

    static bool
    IsBinaryMetricType(int32_t metric_type);

    static Status
    ValidateSearchTopk(int64_t top_k, const engine::meta::TableSchema& table_schema);

    static Status
    ValidateSearchNprobe(int64_t nprobe, const engine::meta::TableSchema& table_schema);

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
};

}  // namespace server
}  // namespace milvus
