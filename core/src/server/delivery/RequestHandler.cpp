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
#include "server/delivery/request/BaseRequest.h"
#include "server/delivery/request/CmdRequest.h"
#include "server/delivery/request/CompactRequest.h"
#include "server/delivery/request/CountCollectionRequest.h"
#include "server/delivery/request/CreateCollectionRequest.h"
#include "server/delivery/request/CreateIndexRequest.h"
#include "server/delivery/request/CreatePartitionRequest.h"
#include "server/delivery/request/DeleteByIDRequest.h"
#include "server/delivery/request/DescribeCollectionRequest.h"
#include "server/delivery/request/DescribeIndexRequest.h"
#include "server/delivery/request/DropCollectionRequest.h"
#include "server/delivery/request/DropIndexRequest.h"
#include "server/delivery/request/DropPartitionRequest.h"
#include "server/delivery/request/FlushRequest.h"
#include "server/delivery/request/GetVectorIDsRequest.h"
#include "server/delivery/request/GetVectorsByIDRequest.h"
#include "server/delivery/request/HasCollectionRequest.h"
#include "server/delivery/request/HasPartitionRequest.h"
#include "server/delivery/request/InsertRequest.h"
#include "server/delivery/request/PreloadCollectionRequest.h"
#include "server/delivery/request/SearchByIDRequest.h"
#include "server/delivery/request/SearchRequest.h"
#include "server/delivery/request/ShowCollectionInfoRequest.h"
#include "server/delivery/request/ShowCollectionsRequest.h"
#include "server/delivery/request/ShowPartitionsRequest.h"

#include "server/delivery/hybrid_request/CreateHybridCollectionRequest.h"
#include "server/delivery/hybrid_request/DescribeHybridCollectionRequest.h"
#include "server/delivery/hybrid_request/HybridSearchRequest.h"
#include "server/delivery/hybrid_request/InsertEntityRequest.h"

namespace milvus {
namespace server {

Status
RequestHandler::CreateCollection(const std::shared_ptr<Context>& context, const std::string& collection_name,
                                 int64_t dimension, int64_t index_file_size, int64_t metric_type) {
    BaseRequestPtr request_ptr =
        CreateCollectionRequest::Create(context, collection_name, dimension, index_file_size, metric_type);
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
RequestHandler::DropCollection(const std::shared_ptr<Context>& context, const std::string& collection_name) {
    BaseRequestPtr request_ptr = DropCollectionRequest::Create(context, collection_name);
    RequestScheduler::ExecRequest(request_ptr);

    return request_ptr->status();
}

Status
RequestHandler::CreateIndex(const std::shared_ptr<Context>& context, const std::string& collection_name,
                            int64_t index_type, const milvus::json& json_params) {
    BaseRequestPtr request_ptr = CreateIndexRequest::Create(context, collection_name, index_type, json_params);
    RequestScheduler::ExecRequest(request_ptr);

    return request_ptr->status();
}

Status
RequestHandler::Insert(const std::shared_ptr<Context>& context, const std::string& collection_name,
                       engine::VectorsData& vectors, const std::string& partition_tag) {
    BaseRequestPtr request_ptr = InsertRequest::Create(context, collection_name, vectors, partition_tag);
    RequestScheduler::ExecRequest(request_ptr);

    return request_ptr->status();
}

Status
RequestHandler::GetVectorsByID(const std::shared_ptr<Context>& context, const std::string& collection_name,
                               const std::vector<int64_t>& ids, std::vector<engine::VectorsData>& vectors) {
    BaseRequestPtr request_ptr = GetVectorsByIDRequest::Create(context, collection_name, ids, vectors);
    RequestScheduler::ExecRequest(request_ptr);

    return request_ptr->status();
}

Status
RequestHandler::GetVectorIDs(const std::shared_ptr<Context>& context, const std::string& collection_name,
                             const std::string& segment_name, std::vector<int64_t>& vector_ids) {
    BaseRequestPtr request_ptr = GetVectorIDsRequest::Create(context, collection_name, segment_name, vector_ids);
    RequestScheduler::ExecRequest(request_ptr);

    return request_ptr->status();
}

Status
RequestHandler::ShowCollections(const std::shared_ptr<Context>& context, std::vector<std::string>& collections) {
    BaseRequestPtr request_ptr = ShowCollectionsRequest::Create(context, collections);
    RequestScheduler::ExecRequest(request_ptr);

    return request_ptr->status();
}

Status
RequestHandler::ShowCollectionInfo(const std::shared_ptr<Context>& context, const std::string& collection_name,
                                   std::string& collection_info) {
    BaseRequestPtr request_ptr = ShowCollectionInfoRequest::Create(context, collection_name, collection_info);
    RequestScheduler::ExecRequest(request_ptr);

    return request_ptr->status();
}

Status
RequestHandler::Search(const std::shared_ptr<Context>& context, const std::string& collection_name,
                       const engine::VectorsData& vectors, int64_t topk, const milvus::json& extra_params,
                       const std::vector<std::string>& partition_list, const std::vector<std::string>& file_id_list,
                       TopKQueryResult& result) {
    BaseRequestPtr request_ptr = SearchRequest::Create(context, collection_name, vectors, topk, extra_params,
                                                       partition_list, file_id_list, result);
    RequestScheduler::ExecRequest(request_ptr);

    return request_ptr->status();
}

Status
RequestHandler::SearchByID(const std::shared_ptr<Context>& context, const std::string& collection_name,
                           const std::vector<int64_t>& id_array, int64_t topk, const milvus::json& extra_params,
                           const std::vector<std::string>& partition_list, TopKQueryResult& result) {
    BaseRequestPtr request_ptr =
        SearchByIDRequest::Create(context, collection_name, id_array, topk, extra_params, partition_list, result);
    RequestScheduler::ExecRequest(request_ptr);

    return request_ptr->status();
}

Status
RequestHandler::DescribeCollection(const std::shared_ptr<Context>& context, const std::string& collection_name,
                                   CollectionSchema& collection_schema) {
    BaseRequestPtr request_ptr = DescribeCollectionRequest::Create(context, collection_name, collection_schema);
    RequestScheduler::ExecRequest(request_ptr);

    return request_ptr->status();
}

Status
RequestHandler::CountCollection(const std::shared_ptr<Context>& context, const std::string& collection_name,
                                int64_t& count) {
    BaseRequestPtr request_ptr = CountCollectionRequest::Create(context, collection_name, count);
    RequestScheduler::ExecRequest(request_ptr);

    return request_ptr->status();
}

Status
RequestHandler::Cmd(const std::shared_ptr<Context>& context, const std::string& cmd, std::string& reply) {
    BaseRequestPtr request_ptr = CmdRequest::Create(context, cmd, reply);
    RequestScheduler::ExecRequest(request_ptr);

    return request_ptr->status();
}

Status
RequestHandler::DeleteByID(const std::shared_ptr<Context>& context, const std::string& collection_name,
                           const std::vector<int64_t>& vector_ids) {
    BaseRequestPtr request_ptr = DeleteByIDRequest::Create(context, collection_name, vector_ids);
    RequestScheduler::ExecRequest(request_ptr);

    return request_ptr->status();
}

Status
RequestHandler::PreloadCollection(const std::shared_ptr<Context>& context, const std::string& collection_name) {
    BaseRequestPtr request_ptr = PreloadCollectionRequest::Create(context, collection_name);
    RequestScheduler::ExecRequest(request_ptr);

    return request_ptr->status();
}

Status
RequestHandler::DescribeIndex(const std::shared_ptr<Context>& context, const std::string& collection_name,
                              IndexParam& param) {
    BaseRequestPtr request_ptr = DescribeIndexRequest::Create(context, collection_name, param);
    RequestScheduler::ExecRequest(request_ptr);

    return request_ptr->status();
}

Status
RequestHandler::DropIndex(const std::shared_ptr<Context>& context, const std::string& collection_name) {
    BaseRequestPtr request_ptr = DropIndexRequest::Create(context, collection_name);
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
RequestHandler::HasPartition(const std::shared_ptr<Context>& context, const std::string& collection_name,
                             const std::string& tag, bool& has_partition) {
    BaseRequestPtr request_ptr = HasPartitionRequest::Create(context, collection_name, tag, has_partition);
    RequestScheduler::ExecRequest(request_ptr);

    return request_ptr->status();
}

Status
RequestHandler::ShowPartitions(const std::shared_ptr<Context>& context, const std::string& collection_name,
                               std::vector<PartitionParam>& partitions) {
    BaseRequestPtr request_ptr = ShowPartitionsRequest::Create(context, collection_name, partitions);
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
RequestHandler::Flush(const std::shared_ptr<Context>& context, const std::vector<std::string>& collection_names) {
    BaseRequestPtr request_ptr = FlushRequest::Create(context, collection_names);
    RequestScheduler::ExecRequest(request_ptr);

    return request_ptr->status();
}

Status
RequestHandler::Compact(const std::shared_ptr<Context>& context, const std::string& collection_name) {
    BaseRequestPtr request_ptr = CompactRequest::Create(context, collection_name);
    RequestScheduler::ExecRequest(request_ptr);

    return request_ptr->status();
}

/*******************************************New Interface*********************************************/

Status
RequestHandler::CreateHybridCollection(const std::shared_ptr<Context>& context, const std::string& collection_name,
                                       std::vector<std::pair<std::string, engine::meta::hybrid::DataType>>& field_types,
                                       std::vector<std::pair<std::string, uint64_t>>& vector_dimensions,
                                       std::vector<std::pair<std::string, std::string>>& field_extra_params) {
    BaseRequestPtr request_ptr = CreateHybridCollectionRequest::Create(context, collection_name, field_types,
                                                                       vector_dimensions, field_extra_params);

    RequestScheduler::ExecRequest(request_ptr);
    return request_ptr->status();
}

Status
RequestHandler::DescribeHybridCollection(const std::shared_ptr<Context>& context, const std::string& collection_name,
                                         std::unordered_map<std::string, engine::meta::hybrid::DataType>& field_types) {
    BaseRequestPtr request_ptr = DescribeHybridCollectionRequest::Create(context, collection_name, field_types);

    RequestScheduler::ExecRequest(request_ptr);
    return request_ptr->status();
}

Status
RequestHandler::HasHybridCollection(const std::shared_ptr<Context>& context, std::string& collection_name,
                                    bool& has_collection) {
}

Status
RequestHandler::InsertEntity(const std::shared_ptr<Context>& context, const std::string& collection_name,
                             const std::string& partition_tag, uint64_t& row_num, std::vector<std::string>& field_names,
                             std::vector<uint8_t>& attr_values,
                             std::unordered_map<std::string, engine::VectorsData>& vector_datas) {
    BaseRequestPtr request_ptr = InsertEntityRequest::Create(context, collection_name, partition_tag, row_num,
                                                             field_names, attr_values, vector_datas);

    RequestScheduler::ExecRequest(request_ptr);
    return request_ptr->status();
}

Status
RequestHandler::HybridSearch(const std::shared_ptr<Context>& context,
                             context::HybridSearchContextPtr hybrid_search_context, const std::string& collection_name,
                             std::vector<std::string>& partition_list, milvus::query::GeneralQueryPtr& general_query,
                             TopKQueryResult& result) {
    BaseRequestPtr request_ptr = HybridSearchRequest::Create(context, hybrid_search_context, collection_name,
                                                             partition_list, general_query, result);
    RequestScheduler::ExecRequest(request_ptr);

    return request_ptr->status();
}

}  // namespace server
}  // namespace milvus
