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

#include <map>
#include <stdint.h>
#include <string>
#include <vector>
#include "common/Types.h"
#include "index/Index.h"
#include "storage/Types.h"

struct BuildIndexInfo {
    int64_t collection_id;
    int64_t partition_id;
    int64_t segment_id;
    int64_t field_id;
    milvus::DataType field_type;
    int64_t index_id;
    int64_t index_build_id;
    int64_t index_version;
    std::vector<std::string> insert_files;
    milvus::storage::StorageConfig storage_config;
    milvus::Config config;
    std::string field_name;
    std::string data_store_path;
    int64_t data_store_version;
    std::string index_store_path;
    int64_t dim;
    int32_t index_engine_version;
    milvus::OptFieldT opt_fields;
};

struct AnalyzeInfo {
    int64_t collection_id;
    int64_t partition_id;
    // int64_t segment_id;  // no use
    int64_t field_id;
    milvus::DataType field_type;
    int64_t task_id;
    int64_t version;
    std::map<int64_t, std::vector<std::string>> insert_files; // segment_id->files
    //    std::vector<std::string> insert_files;
    std::map<int64_t, int64_t> num_rows;
    milvus::storage::StorageConfig storage_config;
    milvus::Config config;
    std::string field_name;
    std::string centroids_store_path;
    std::vector<std::string> segments_offset_mapping;
    int64_t data_store_version;
    std::string index_store_path;
    int64_t dim;
    int32_t index_engine_version;
    int64_t segment_size;
    int64_t train_size;
};
