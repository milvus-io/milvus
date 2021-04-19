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

// #include <src/db/snapshot/Context.h>
// #include <src/segment/Segment.h>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

// #include "query/BooleanQuery.h"
#include "server/delivery/request/BaseReq.h"
#include "utils/Status.h"

namespace milvus {
namespace server {

class ReqHandler {
 public:
    ReqHandler() = default;

    Status
    CreateCollection(const ContextPtr& context, const std::string& collection_name, FieldsType& fields,
                     milvus::json& json_params);

    Status
    DropCollection(const ContextPtr& context, const std::string& collection_name);

    Status
    HasCollection(const ContextPtr& context, const std::string& collection_name, bool& has_collection);

    Status
    ListCollections(const ContextPtr& context, std::vector<std::string>& collections);

    Status
    GetCollectionInfo(const ContextPtr& context, const std::string& collection_name,
                      CollectionSchema& collection_schema);

    Status
    GetCollectionStats(const ContextPtr& context, const std::string& collection_name, std::string& collection_stats);

    Status
    CountEntities(const ContextPtr& context, const std::string& collection_name, int64_t& count);

    Status
    CreatePartition(const ContextPtr& context, const std::string& collection_name, const std::string& tag);

    Status
    DropPartition(const ContextPtr& context, const std::string& collection_name, const std::string& tag);

    Status
    HasPartition(const ContextPtr& context, const std::string& collection_name, const std::string& tag,
                 bool& has_partition);

    Status
    ListPartitions(const ContextPtr& context, const std::string& collection_name, std::vector<std::string>& partitions);

    Status
    CreateIndex(const ContextPtr& context, const std::string& collection_name, const std::string& field_name,
                const std::string& index_name, const milvus::json& json_params);

    Status
    DescribeIndex(const ContextPtr& context, const std::string& collection_name, const std::string& field_name,
                  std::string& index_name, milvus::json& json_params);

    Status
    DropIndex(const ContextPtr& context, const std::string& collection_name, const std::string& field_name,
              const std::string& index_name);

    Status
    Insert(const ContextPtr& context, const std::string& collection_name, const std::string& partition_name,
           const int64_t& row_count, std::unordered_map<std::string, std::vector<uint8_t>>& chunk_data);

    Status
    GetEntityByID(const ContextPtr& context, const std::string& collection_name, const engine::IDNumbers& ids,
                  std::vector<std::string>& field_names, std::vector<bool>& valid_row,
                   engine::DataChunkPtr& data_chunk);

    Status
    DeleteEntityByID(const ContextPtr& context, const std::string& collection_name, const engine::IDNumbers& ids);

    Status
    Search(const ContextPtr& context, const query::QueryPtr& query_ptr, const milvus::json& json_params,
            engine::QueryResultPtr& result);

    Status
    ListIDInSegment(const ContextPtr& context, const std::string& collection_name, int64_t segment_id,
                    engine::IDNumbers& ids);

    Status
    LoadCollection(const ContextPtr& context, const std::string& collection_name);

    Status
    Flush(const ContextPtr& context, const std::vector<std::string>& collection_names);

    Status
    Compact(const ContextPtr& context, const std::string& collection_name, double compact_threshold);

    Status
    Cmd(const ContextPtr& context, const std::string& cmd, std::string& reply);
};

}  // namespace server
}  // namespace milvus
