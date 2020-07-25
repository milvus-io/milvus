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

#include <src/db/snapshot/Context.h>
#include <src/segment/Segment.h>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "context/HybridSearchContext.h"
#include "query/BooleanQuery.h"
#include "server/delivery/request/BaseRequest.h"
#include "utils/Status.h"

namespace milvus {
namespace server {

class RequestHandler {
 public:
    RequestHandler() = default;

    Status
    HasCollection(const std::shared_ptr<Context>& context, const std::string& collection_name, bool& has_collection);

    Status
    DropCollection(const std::shared_ptr<Context>& context, const std::string& collection_name);

    Status
    CreateIndex(const std::shared_ptr<Context>& context, const std::string& collection_name,
                const std::string& field_name, const std::string& index_name, const milvus::json& json_params);

    Status
    Insert(const std::shared_ptr<Context>& context, const std::string& collection_name, engine::VectorsData& vectors,
           const std::string& partition_tag);

    Status
    GetVectorIDs(const std::shared_ptr<Context>& context, const std::string& collection_name,
                 const std::string& segment_name, std::vector<int64_t>& vector_ids);

    Status
    ShowCollections(const std::shared_ptr<Context>& context, std::vector<std::string>& collections);

    Status
    ShowCollectionInfo(const std::shared_ptr<Context>& context, const std::string& collection_name,
                       std::string& collection_info);

    Status
    Search(const std::shared_ptr<Context>& context, const std::string& collection_name, engine::VectorsData& vectors,
           int64_t topk, const milvus::json& extra_params, const std::vector<std::string>& partition_list,
           const std::vector<std::string>& file_id_list, TopKQueryResult& result);

    Status
    SearchByID(const std::shared_ptr<Context>& context, const std::string& collection_name,
               const std::vector<int64_t>& id_array, int64_t topk, const milvus::json& extra_params,
               const std::vector<std::string>& partition_list, TopKQueryResult& result);

    Status
    CountCollection(const std::shared_ptr<Context>& context, const std::string& collection_name, int64_t& count);

    Status
    Cmd(const std::shared_ptr<Context>& context, const std::string& cmd, std::string& reply);

    Status
    DeleteByID(const std::shared_ptr<Context>& context, const std::string& collection_name,
               const std::vector<int64_t>& vector_ids);

    Status
    PreloadCollection(const std::shared_ptr<Context>& context, const std::string& collection_name);

    Status
    ReLoadSegments(const std::shared_ptr<Context>& context, const std::string& collection_name,
                   const std::vector<std::string>& segment_ids);

    Status
    DescribeIndex(const std::shared_ptr<Context>& context, const std::string& collection_name, IndexParam& param);

    Status
    DropIndex(const std::shared_ptr<Context>& context, const std::string& collection_name,
              const std::string& field_name, const std::string& index_name);

    Status
    CreatePartition(const std::shared_ptr<Context>& context, const std::string& collection_name,
                    const std::string& tag);

    Status
    HasPartition(const std::shared_ptr<Context>& context, const std::string& collection_name, const std::string& tag,
                 bool& has_partition);

    Status
    ShowPartitions(const std::shared_ptr<Context>& context, const std::string& collection_name,
                   std::vector<std::string>& partitions);

    Status
    DropPartition(const std::shared_ptr<Context>& context, const std::string& collection_name, const std::string& tag);

    Status
    Flush(const std::shared_ptr<Context>& context, const std::vector<std::string>& collection_names);

    Status
    Compact(const std::shared_ptr<Context>& context, const std::string& collection_name, double compact_threshold);

    /*******************************************New Interface*********************************************/

    Status
    CreateHybridCollection(const std::shared_ptr<Context>& context, const std::string& collection_name,
                           std::unordered_map<std::string, engine::meta::hybrid::DataType>& field_types,
                           std::unordered_map<std::string, milvus::json>& field_index_params,
                           std::unordered_map<std::string, std::string>& field_params, milvus::json& json_params);

    Status
    DescribeHybridCollection(const std::shared_ptr<Context>& context, const std::string& collection_name,
                             HybridCollectionSchema& collection_schema);

    Status
    HasHybridCollection(const std::shared_ptr<Context>& context, std::string& collection_name, bool& has_collection);

    Status
    DropHybridCollection(const std::shared_ptr<Context>& context, std::string& collection_name);

    Status
    InsertEntity(const std::shared_ptr<Context>& context, const std::string& collection_name,
                 const std::string& partition_name, const int32_t& row_count,
                 std::unordered_map<std::string, std::vector<uint8_t>>& chunk_data);

    Status
    GetEntityByID(const std::shared_ptr<Context>& context, const std::string& collection_name,
                  const engine::IDNumbers& ids, std::vector<std::string>& field_names,
                  engine::snapshot::CollectionMappings& field_mappings, engine::DataChunkPtr& data_chunk);

    Status
    HybridSearch(const std::shared_ptr<milvus::server::Context>& context, const query::QueryPtr& query_ptr,
                 const milvus::json& json_params, engine::QueryResultPtr& result);
};

}  // namespace server
}  // namespace milvus
