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
    CreateCollection(const std::shared_ptr<Context>& context, const std::string& collection_name, int64_t dimension,
                     int64_t index_file_size, int64_t metric_type);

    Status
    HasCollection(const std::shared_ptr<Context>& context, const std::string& collection_name, bool& has_collection);

    Status
    DropCollection(const std::shared_ptr<Context>& context, const std::string& collection_name);

    Status
    CreateIndex(const std::shared_ptr<Context>& context, const std::string& collection_name, int64_t index_type,
                const milvus::json& json_params);

    Status
    Insert(const std::shared_ptr<Context>& context, const std::string& collection_name, engine::VectorsData& vectors,
           const std::string& partition_tag);

    Status
    GetVectorByID(const std::shared_ptr<Context>& context, const std::string& collection_name,
                  const std::vector<int64_t>& ids, engine::VectorsData& vectors);

    Status
    GetVectorIDs(const std::shared_ptr<Context>& context, const std::string& collection_name,
                 const std::string& segment_name, std::vector<int64_t>& vector_ids);

    Status
    ShowCollections(const std::shared_ptr<Context>& context, std::vector<std::string>& collections);

    Status
    ShowCollectionInfo(const std::shared_ptr<Context>& context, const std::string& collection_name,
                       CollectionInfo& collection_info);

    Status
    Search(const std::shared_ptr<Context>& context, const std::string& collection_name,
           const engine::VectorsData& vectors, int64_t topk, const milvus::json& extra_params,
           const std::vector<std::string>& partition_list, const std::vector<std::string>& file_id_list,
           TopKQueryResult& result);

    Status
    SearchByID(const std::shared_ptr<Context>& context, const std::string& collection_name, int64_t vector_id,
               int64_t topk, const milvus::json& extra_params, const std::vector<std::string>& partition_list,
               TopKQueryResult& result);

    Status
    DescribeCollection(const std::shared_ptr<Context>& context, const std::string& collection_name,
                       CollectionSchema& collection_schema);

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
    DescribeIndex(const std::shared_ptr<Context>& context, const std::string& collection_name, IndexParam& param);

    Status
    DropIndex(const std::shared_ptr<Context>& context, const std::string& collection_name);

    Status
    CreatePartition(const std::shared_ptr<Context>& context, const std::string& collection_name,
                    const std::string& tag);

    Status
    ShowPartitions(const std::shared_ptr<Context>& context, const std::string& collection_name,
                   std::vector<PartitionParam>& partitions);

    Status
    DropPartition(const std::shared_ptr<Context>& context, const std::string& collection_name, const std::string& tag);

    Status
    Flush(const std::shared_ptr<Context>& context, const std::vector<std::string>& collection_names);

    Status
    Compact(const std::shared_ptr<Context>& context, const std::string& collection_name);

    /*******************************************New Interface*********************************************/

    Status
    CreateHybridCollection(const std::shared_ptr<Context>& context, const std::string& collection_name,
                           std::vector<std::pair<std::string, engine::meta::hybrid::DataType>>& field_types,
                           std::vector<std::pair<std::string, uint64_t>>& vector_dimensions,
                           std::vector<std::pair<std::string, std::string>>& field_extra_params);

    Status
    HasHybridCollection(const std::shared_ptr<Context>& context, std::string& collection_name, bool& has_collection);

    Status
    DropHybridCollection(const std::shared_ptr<Context>& context, std::string& collection_name);

    Status
    InsertEntity(const std::shared_ptr<Context>& context, const std::string& collection_name,
                 const std::string& partition_tag, uint64_t& row_num, std::vector<std::string>& field_names,
                 std::vector<uint8_t>& attr_values, std::unordered_map<std::string, engine::VectorsData>& vector_datas);

    Status
    HybridSearch(const std::shared_ptr<Context>& context, context::HybridSearchContextPtr hybrid_search_context,
                 const std::string& collection_name, std::vector<std::string>& partition_list,
                 query::GeneralQueryPtr& boolean_query, TopKQueryResult& result);
};

}  // namespace server
}  // namespace milvus
