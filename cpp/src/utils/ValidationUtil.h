// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


#pragma once

#include "db/meta/MetaTypes.h"
#include "Error.h"

namespace zilliz {
namespace milvus {
namespace server {

class ValidationUtil {
public:
    static ErrorCode
    ValidateTableName(const std::string &table_name);

    static ErrorCode
    ValidateTableDimension(int64_t dimension);

    static ErrorCode
    ValidateTableIndexType(int32_t index_type);

    static ErrorCode
    ValidateTableIndexNlist(int32_t nlist);

    static ErrorCode
    ValidateTableIndexFileSize(int64_t index_file_size);

    static ErrorCode
    ValidateTableIndexMetricType(int32_t metric_type);

    static ErrorCode
    ValidateSearchTopk(int64_t top_k, const engine::meta::TableSchema& table_schema);

    static ErrorCode
    ValidateSearchNprobe(int64_t nprobe, const engine::meta::TableSchema& table_schema);

    static ErrorCode
    ValidateGpuIndex(uint32_t gpu_index);

    static ErrorCode
    GetGpuMemory(uint32_t gpu_index, size_t &memory);

    static ErrorCode
    ValidateIpAddress(const std::string &ip_address);

    static ErrorCode
    ValidateStringIsNumber(const std::string &str);

    static ErrorCode
    ValidateStringIsBool(std::string &str);

    static ErrorCode
    ValidateStringIsDouble(const std::string &str, double &val);

    static ErrorCode
    ValidateDbURI(const std::string &uri);
};

}
}
}