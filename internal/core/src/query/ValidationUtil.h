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
#include "server/delivery/request/Types.h"
#include "utils/Json.h"
#include "utils/Status.h"

#include <string>
#include <vector>

namespace milvus {
namespace server {

constexpr int64_t QUERY_MAX_TOPK = 16384;
constexpr int64_t GPU_QUERY_MAX_TOPK = 2048;
constexpr int64_t GPU_QUERY_MAX_NPROBE = 2048;

extern Status
ValidateCollectionName(const std::string& collection_name);

extern Status
ValidateFieldName(const std::string& field_name);

extern Status
ValidateDimension(int64_t dimension, bool is_binary);

extern Status
ValidateVectorIndexType(std::string& index_type, bool is_binary);

extern Status
ValidateStructuredIndexType(std::string& index_type);

extern Status
ValidateIndexParams(const milvus::json& index_params, int64_t dimension, const std::string& index_type);

extern Status
ValidateSegmentRowCount(int64_t segment_row_count);

extern Status
ValidateIndexMetricType(const std::string& metric_type, const std::string& index_type);

extern Status
ValidateSearchMetricType(const std::string& metric_type, bool is_binary);

extern Status
ValidateSearchTopk(int64_t top_k);

extern Status
ValidatePartitionTags(const std::vector<std::string>& partition_tags);

extern Status
ValidateInsertDataSize(const InsertParam& insert_param);

extern Status
ValidateCompactThreshold(double threshold);
}  // namespace server
}  // namespace milvus
