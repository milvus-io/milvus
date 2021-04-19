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

#include "server/ValidationUtil.h"
// #include "db/Constants.h"
// #include "db/Utils.h"
// #include "knowhere/index/vector_index/ConfAdapter.h"
// #include "knowhere/index/vector_index/helpers/IndexParameter.h"
#include "utils/Log.h"
#include "utils/StringHelpFunctions.h"

#include <algorithm>
#include <limits>
#include <set>
#include <string>

namespace milvus {
namespace server {

namespace {

Status
CheckParameterRange(const milvus::json& json_params, const std::string& param_name, int64_t min, int64_t max,
                    bool min_close = true, bool max_closed = true) {

    return Status::OK();
}

Status
CheckParameterExistence(const milvus::json& json_params, const std::string& param_name) {


    return Status::OK();
}

}  // namespace

Status
ValidateCollectionName(const std::string& collection_name) {


    return Status::OK();
}

Status
ValidateFieldName(const std::string& field_name) {


    return Status::OK();
}

Status
ValidateIndexType(std::string& index_type) {

    return Status::OK();
}

Status
ValidateDimension(int64_t dim, bool is_binary) {


    return Status::OK();
}

Status
ValidateIndexParams(const milvus::json& index_params, int64_t dimension, const std::string& index_type) {

    return Status::OK();
}

Status
ValidateSegmentRowCount(int64_t segment_row_count) {

    return Status::OK();
}

Status
ValidateIndexMetricType(const std::string& metric_type, const std::string& index_type) {


    return Status::OK();
}

Status
ValidateSearchTopk(int64_t top_k) {


    return Status::OK();
}

Status
ValidatePartitionTags(const std::vector<std::string>& partition_tags) {


    return Status::OK();
}

Status
ValidateInsertDataSize(const engine::DataChunkPtr& data) {

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
