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

#include "server/delivery/ReqHandler.h"

#include <set>

#include "server/delivery/ReqScheduler.h"
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
#include "server/delivery/request/GetCollectionStatsReq.h"
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
ReqHandler::CreateCollection(const std::shared_ptr<Context>& context, const std::string& collection_name,
                             std::unordered_map<std::string, engine::meta::hybrid::DataType>& field_types,
                             std::unordered_map<std::string, milvus::json>& field_index_params,
                             std::unordered_map<std::string, std::string>& field_params, milvus::json& json_param) {
    BaseReqPtr req_ptr = CreateCollectionReq::Create(context, collection_name, field_types, field_index_params,
                                                     field_params, json_param);
    ReqScheduler::ExecReq(req_ptr);
    return req_ptr->status();
}

Status
ReqHandler::DropCollection(const std::shared_ptr<Context>& context, const std::string& collection_name) {
    BaseReqPtr req_ptr = DropCollectionReq::Create(context, collection_name);
    ReqScheduler::ExecReq(req_ptr);
    return req_ptr->status();
}

Status
ReqHandler::HasCollection(const std::shared_ptr<Context>& context, const std::string& collection_name,
                          bool& has_collection) {
    BaseReqPtr req_ptr = HasCollectionReq::Create(context, collection_name, has_collection);
    ReqScheduler::ExecReq(req_ptr);
    return req_ptr->status();
}

Status
ReqHandler::ListCollections(const std::shared_ptr<Context>& context, std::vector<std::string>& collections) {
    BaseReqPtr req_ptr = ListCollectionsReq::Create(context, collections);
    ReqScheduler::ExecReq(req_ptr);
    return req_ptr->status();
}

Status
ReqHandler::GetCollectionInfo(const std::shared_ptr<Context>& context, const std::string& collection_name,
                              CollectionSchema& collection_schema) {
    BaseReqPtr req_ptr = GetCollectionInfoReq::Create(context, collection_name, collection_schema);
    ReqScheduler::ExecReq(req_ptr);
    return req_ptr->status();
}

Status
ReqHandler::GetCollectionStats(const std::shared_ptr<Context>& context, const std::string& collection_name,
                               std::string& collection_stats) {
    BaseReqPtr req_ptr = GetCollectionStatsReq::Create(context, collection_name, collection_stats);
    ReqScheduler::ExecReq(req_ptr);
    return req_ptr->status();
}

Status
ReqHandler::CountEntities(const std::shared_ptr<Context>& context, const std::string& collection_name, int64_t& count) {
    BaseReqPtr req_ptr = CountEntitiesReq::Create(context, collection_name, count);
    ReqScheduler::ExecReq(req_ptr);
    return req_ptr->status();
}

Status
ReqHandler::CreatePartition(const std::shared_ptr<Context>& context, const std::string& collection_name,
                            const std::string& tag) {
    BaseReqPtr req_ptr = CreatePartitionReq::Create(context, collection_name, tag);
    ReqScheduler::ExecReq(req_ptr);
    return req_ptr->status();
}

Status
ReqHandler::DropPartition(const std::shared_ptr<Context>& context, const std::string& collection_name,
                          const std::string& tag) {
    BaseReqPtr req_ptr = DropPartitionReq::Create(context, collection_name, tag);
    ReqScheduler::ExecReq(req_ptr);
    return req_ptr->status();
}

Status
ReqHandler::HasPartition(const std::shared_ptr<Context>& context, const std::string& collection_name,
                         const std::string& tag, bool& has_partition) {
    BaseReqPtr req_ptr = HasPartitionReq::Create(context, collection_name, tag, has_partition);
    ReqScheduler::ExecReq(req_ptr);
    return req_ptr->status();
}

Status
ReqHandler::ListPartitions(const std::shared_ptr<Context>& context, const std::string& collection_name,
                           std::vector<std::string>& partitions) {
    BaseReqPtr req_ptr = ListPartitionsReq::Create(context, collection_name, partitions);
    ReqScheduler::ExecReq(req_ptr);
    return req_ptr->status();
}

Status
ReqHandler::CreateIndex(const std::shared_ptr<Context>& context, const std::string& collection_name,
                        const std::string& field_name, const std::string& index_name, const milvus::json& json_params) {
    BaseReqPtr req_ptr = CreateIndexReq::Create(context, collection_name, field_name, index_name, json_params);
    ReqScheduler::ExecReq(req_ptr);
    return req_ptr->status();
}

Status
ReqHandler::DropIndex(const std::shared_ptr<Context>& context, const std::string& collection_name,
                      const std::string& field_name, const std::string& index_name) {
    BaseReqPtr req_ptr = DropIndexReq::Create(context, collection_name, index_name, field_name);
    ReqScheduler::ExecReq(req_ptr);
    return req_ptr->status();
}

Status
ReqHandler::Insert(const std::shared_ptr<Context>& context, const std::string& collection_name,
                   const std::string& partition_name, const int64_t& row_count,
                   std::unordered_map<std::string, std::vector<uint8_t>>& chunk_data) {
    BaseReqPtr req_ptr = InsertReq::Create(context, collection_name, partition_name, row_count, chunk_data);
    ReqScheduler::ExecReq(req_ptr);
    return req_ptr->status();
}

Status
ReqHandler::GetEntityByID(const std::shared_ptr<Context>& context, const std::string& collection_name,
                          const engine::IDNumbers& ids, std::vector<std::string>& field_names,
                          std::vector<bool>& valid_row, engine::snapshot::CollectionMappings& field_mappings,
                          engine::DataChunkPtr& data_chunk) {
    BaseReqPtr req_ptr =
        GetEntityByIDReq::Create(context, collection_name, ids, field_names, valid_row, field_mappings, data_chunk);
    ReqScheduler::ExecReq(req_ptr);
    return req_ptr->status();
}

Status
ReqHandler::DeleteEntityByID(const std::shared_ptr<Context>& context, const std::string& collection_name,
                             const engine::IDNumbers& ids) {
    BaseReqPtr req_ptr = DeleteEntityByIDReq::Create(context, collection_name, ids);
    ReqScheduler::ExecReq(req_ptr);
    return req_ptr->status();
}

Status
ReqHandler::Search(const std::shared_ptr<milvus::server::Context>& context, const query::QueryPtr& query_ptr,
                   const milvus::json& json_params, engine::QueryResultPtr& result) {
    BaseReqPtr req_ptr = SearchReq::Create(context, query_ptr, json_params, result);
    ReqScheduler::ExecReq(req_ptr);
    return req_ptr->status();
}

Status
ReqHandler::ListIDInSegment(const std::shared_ptr<Context>& context, const std::string& collection_name,
                            int64_t segment_id, engine::IDNumbers& ids) {
    BaseReqPtr req_ptr = ListIDInSegmentReq::Create(context, collection_name, segment_id, ids);
    ReqScheduler::ExecReq(req_ptr);
    return req_ptr->status();
}

Status
ReqHandler::LoadCollection(const std::shared_ptr<Context>& context, const std::string& collection_name) {
    BaseReqPtr req_ptr = LoadCollectionReq::Create(context, collection_name);
    ReqScheduler::ExecReq(req_ptr);
    return req_ptr->status();
}

Status
ReqHandler::Flush(const std::shared_ptr<Context>& context, const std::vector<std::string>& collection_names) {
    BaseReqPtr req_ptr = FlushReq::Create(context, collection_names);
    ReqScheduler::ExecReq(req_ptr);
    return req_ptr->status();
}

Status
ReqHandler::Compact(const std::shared_ptr<Context>& context, const std::string& collection_name,
                    double compact_threshold) {
    BaseReqPtr req_ptr = CompactReq::Create(context, collection_name, compact_threshold);
    ReqScheduler::ExecReq(req_ptr);
    return req_ptr->status();
}

Status
ReqHandler::Cmd(const std::shared_ptr<Context>& context, const std::string& cmd, std::string& reply) {
    BaseReqPtr req_ptr = CmdReq::Create(context, cmd, reply);
    ReqScheduler::ExecReq(req_ptr);
    return req_ptr->status();
}
}  // namespace server
}  // namespace milvus
