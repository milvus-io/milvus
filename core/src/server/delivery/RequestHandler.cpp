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

#include "server/delivery/RequestHandler.h"

#include <set>

#include "server/delivery/RequestScheduler.h"
#include "server/delivery/request/BaseReq.h"
#include "server/delivery/request/CmdReq.h"
#include "server/delivery/request/CompactReq.h"
#include "server/delivery/request/CountEntitiesReq.h"
#include "server/delivery/request/CreateCollectionReq.h"
#include "server/delivery/request/CreateIndexReq.h"
#include "server/delivery/request/CreatePartitionReq.h"
#include "server/delivery/request/DeleteEntityByIDReq.h"
#include "server/delivery/request/DropCollectionReq.h"
#include "server/delivery/request/DropIndexReq.h"
#include "server/delivery/request/DropPartitionReq.h"
#include "server/delivery/request/FlushReq.h"
#include "server/delivery/request/GetCollectionInfoReq.h"
#include "server/delivery/request/GetCollectionStatsRequest.h"
#include "server/delivery/request/GetEntityByIDReq.h"
#include "server/delivery/request/HasCollectionReq.h"
#include "server/delivery/request/HasPartitionReq.h"
#include "server/delivery/request/InsertReq.h"
#include "server/delivery/request/ListCollectionsReq.h"
#include "server/delivery/request/ListIDInSegmentReq.h"
#include "server/delivery/request/ListPartitionsReq.h"
#include "server/delivery/request/LoadCollectionReq.h"
#include "server/delivery/request/SearchReq.h"

namespace milvus {
namespace server {

Status
RequestHandler::CreateCollection(const std::shared_ptr<Context>& context, const std::string& collection_name,
                                 std::unordered_map<std::string, engine::meta::hybrid::DataType>& field_types,
                                 std::unordered_map<std::string, milvus::json>& field_index_params,
                                 std::unordered_map<std::string, std::string>& field_params, milvus::json& json_param) {
    BaseRequestPtr request_ptr = CreateCollectionRequest::Create(context, collection_name, field_types,
                                                                 field_index_params, field_params, json_param);
    RequestScheduler::ExecRequest(request_ptr);
    return request_ptr->status();
}

Status
RequestHandler::DropCollection(const std::shared_ptr<Context>& context, const std::string& collection_name) {
    BaseRequestPtr request_ptr = DropCollectionRequest::Create(context, collection_name);
    RequestScheduler::ExecRequest(request_ptr);
    return request_ptr->status();
}

Status
RequestHandler::HasCollection(const std::shared_ptr<Context>& context, const std::string& collection_name,
                              bool& has_collection) {
    BaseRequestPtr request_ptr = HasCollectionRequest::Create(context, collection_name, has_collection);
    RequestScheduler::ExecRequest(request_ptr);
    return request_ptr->status();
}

Status
RequestHandler::ListCollections(const std::shared_ptr<Context>& context, std::vector<std::string>& collections) {
    BaseRequestPtr request_ptr = ListCollectionsRequest::Create(context, collections);
    RequestScheduler::ExecRequest(request_ptr);
    return request_ptr->status();
}

Status
RequestHandler::GetCollectionInfo(const std::shared_ptr<Context>& context, const std::string& collection_name,
                                  CollectionSchema& collection_schema) {
    BaseRequestPtr request_ptr = GetCollectionInfoRequest::Create(context, collection_name, collection_schema);
    RequestScheduler::ExecRequest(request_ptr);
    return request_ptr->status();
}

Status
RequestHandler::GetCollectionStats(const std::shared_ptr<Context>& context, const std::string& collection_name,
                                   std::string& collection_stats) {
    BaseRequestPtr request_ptr = GetCollectionStatsRequest::Create(context, collection_name, collection_stats);
    RequestScheduler::ExecRequest(request_ptr);
    return request_ptr->status();
}

Status
RequestHandler::CountEntities(const std::shared_ptr<Context>& context, const std::string& collection_name,
                              int64_t& count) {
    BaseRequestPtr request_ptr = CountEntitiesRequest::Create(context, collection_name, count);
    RequestScheduler::ExecRequest(request_ptr);
    return request_ptr->status();
}

Status
RequestHandler::CreatePartition(const std::shared_ptr<Context>& context, const std::string& collection_name,
                                const std::string& tag) {
    BaseRequestPtr request_ptr = CreatePartitionRequest::Create(context, collection_name, tag);
    RequestScheduler::ExecRequest(request_ptr);
    return request_ptr->status();
}

Status
RequestHandler::DropPartition(const std::shared_ptr<Context>& context, const std::string& collection_name,
                              const std::string& tag) {
    BaseRequestPtr request_ptr = DropPartitionRequest::Create(context, collection_name, tag);
    RequestScheduler::ExecRequest(request_ptr);
    return request_ptr->status();
}

Status
RequestHandler::HasPartition(const std::shared_ptr<Context>& context, const std::string& collection_name,
                             const std::string& tag, bool& has_partition) {
    BaseRequestPtr request_ptr = HasPartitionRequest::Create(context, collection_name, tag, has_partition);
    RequestScheduler::ExecRequest(request_ptr);
    return request_ptr->status();
}

Status
RequestHandler::ListPartitions(const std::shared_ptr<Context>& context, const std::string& collection_name,
                               std::vector<std::string>& partitions) {
    BaseRequestPtr request_ptr = ListPartitionsRequest::Create(context, collection_name, partitions);
    RequestScheduler::ExecRequest(request_ptr);
    return request_ptr->status();
}

Status
RequestHandler::CreateIndex(const std::shared_ptr<Context>& context, const std::string& collection_name,
                            const std::string& field_name, const std::string& index_name,
                            const milvus::json& json_params) {
    BaseRequestPtr request_ptr =
        CreateIndexRequest::Create(context, collection_name, field_name, index_name, json_params);
    RequestScheduler::ExecRequest(request_ptr);
    return request_ptr->status();
}

Status
RequestHandler::DropIndex(const std::shared_ptr<Context>& context, const std::string& collection_name,
                          const std::string& field_name, const std::string& index_name) {
    BaseRequestPtr request_ptr = DropIndexRequest::Create(context, collection_name, index_name, field_name);
    RequestScheduler::ExecRequest(request_ptr);
    return request_ptr->status();
}

Status
RequestHandler::Insert(const std::shared_ptr<Context>& context, const std::string& collection_name,
                       const std::string& partition_name, const int64_t& row_count,
                       std::unordered_map<std::string, std::vector<uint8_t>>& chunk_data) {
    BaseRequestPtr request_ptr = InsertRequest::Create(context, collection_name, partition_name, row_count, chunk_data);
    RequestScheduler::ExecRequest(request_ptr);
    return request_ptr->status();
}

Status
RequestHandler::GetEntityByID(const std::shared_ptr<Context>& context, const std::string& collection_name,
                              const engine::IDNumbers& ids, std::vector<std::string>& field_names,
                              engine::snapshot::CollectionMappings& field_mappings, engine::DataChunkPtr& data_chunk) {
    BaseRequestPtr request_ptr =
        GetEntityByIDRequest::Create(context, collection_name, ids, field_names, field_mappings, data_chunk);
    RequestScheduler::ExecRequest(request_ptr);
    return request_ptr->status();
}

Status
RequestHandler::DeleteEntityByID(const std::shared_ptr<Context>& context, const std::string& collection_name,
                                 const engine::IDNumbers& ids) {
    BaseRequestPtr request_ptr = DeleteEntityByIDRequest::Create(context, collection_name, ids);
    RequestScheduler::ExecRequest(request_ptr);
    return request_ptr->status();
}

Status
RequestHandler::Search(const std::shared_ptr<milvus::server::Context>& context, const query::QueryPtr& query_ptr,
                       const milvus::json& json_params, engine::QueryResultPtr& result) {
    BaseRequestPtr request_ptr = SearchRequest::Create(context, query_ptr, json_params, result);
    RequestScheduler::ExecRequest(request_ptr);
    return request_ptr->status();
}

Status
RequestHandler::ListIDInSegment(const std::shared_ptr<Context>& context, const std::string& collection_name,
                                int64_t segment_id, engine::IDNumbers& ids) {
    BaseRequestPtr request_ptr = ListIDInSegmentRequest::Create(context, collection_name, segment_id, ids);
    RequestScheduler::ExecRequest(request_ptr);
    return request_ptr->status();
}

Status
RequestHandler::LoadCollection(const std::shared_ptr<Context>& context, const std::string& collection_name) {
    BaseRequestPtr request_ptr = LoadCollectionRequest::Create(context, collection_name);
    RequestScheduler::ExecRequest(request_ptr);
    return request_ptr->status();
}

Status
RequestHandler::Flush(const std::shared_ptr<Context>& context, const std::vector<std::string>& collection_names) {
    BaseRequestPtr request_ptr = FlushRequest::Create(context, collection_names);
    RequestScheduler::ExecRequest(request_ptr);
    return request_ptr->status();
}

Status
RequestHandler::Compact(const std::shared_ptr<Context>& context, const std::string& collection_name,
                        double compact_threshold) {
    BaseRequestPtr request_ptr = CompactRequest::Create(context, collection_name, compact_threshold);
    RequestScheduler::ExecRequest(request_ptr);
    return request_ptr->status();
}

Status
RequestHandler::Cmd(const std::shared_ptr<Context>& context, const std::string& cmd, std::string& reply) {
    BaseRequestPtr request_ptr = CmdRequest::Create(context, cmd, reply);
    RequestScheduler::ExecRequest(request_ptr);
    return request_ptr->status();
}
}  // namespace server
}  // namespace milvus
