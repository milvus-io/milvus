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

#include "GrpcClient.h"
#include "MilvusApi.h"

#include <memory>
#include <string>
#include <vector>

namespace milvus {

class ClientProxy : public Connection {
 public:
    // Implementations of the Connection interface
    Status
    Connect(const ConnectParam& connect_param) override;

    Status
    Connect(const std::string& uri) override;

    Status
    Connected() const override;

    Status
    Disconnect() override;

    std::string
    ClientVersion() const override;

    std::string
    ServerVersion() const override;

    std::string
    ServerStatus() const override;

    Status
    GetConfig(const std::string& node_name, std::string& value) const override;

    Status
    SetConfig(const std::string& node_name, const std::string& value) const override;

    Status
    CreateCollection(const CollectionParam& param) override;

    bool
    HasCollection(const std::string& collection_name) override;

    Status
    DropCollection(const std::string& collection_name) override;

    Status
    CreateIndex(const IndexParam& index_param) override;

    Status
    Insert(const std::string& collection_name,
           const std::string& partition_tag,
           const std::vector<Entity>& entity_array,
           std::vector<int64_t>& id_array) override;

    Status
    GetEntityByID(const std::string& collection_name, int64_t entity_id, Entity& entity_data) override;

    Status
    GetIDsInSegment(const std::string& collection_name, const std::string& segment_name,
                    std::vector<int64_t>& id_array) override;

    Status
    Search(const std::string& collection_name, const std::vector<std::string>& partition_tag_array,
           const std::vector<Entity>& entity_array, int64_t topk,
           const std::string& extra_params, TopKQueryResult& topk_query_result) override;

    Status
    DescribeCollection(const std::string& collection_name, CollectionParam& collection_schema) override;

    Status
    CountCollection(const std::string& collection_name, int64_t& entity_count) override;

    Status
    ShowCollections(std::vector<std::string>& collection_array) override;

    Status
    ShowCollectionInfo(const std::string& collection_name, CollectionInfo& collection_info) override;

    Status
    DeleteByID(const std::string& collection_name, const std::vector<int64_t>& id_array) override;

    Status
    PreloadCollection(const std::string& collection_name) const override;

    Status
    DescribeIndex(const std::string& collection_name, IndexParam& index_param) const override;

    Status
    DropIndex(const std::string& collection_name) const override;

    Status
    CreatePartition(const PartitionParam& partition_param) override;

    Status
    ShowPartitions(const std::string& collection_name, PartitionTagList& partition_tag_array) const override;

    Status
    DropPartition(const PartitionParam& partition_param) override;

    Status
    FlushCollection(const std::string& collection_name) override;

    Status
    Flush() override;

    Status
    CompactCollection(const std::string& collection_name) override;

    /*******************************New Interface**********************************/

    Status
    CreateHybridCollection(const HMapping& mapping) override;

    Status
    InsertEntity(const std::string& collection_name,
                 const std::string& partition_tag,
                 HEntity& entities,
                 std::vector<uint64_t>& id_array) override;

    Status
    HybridSearch(const std::string& collection_name,
                 const std::vector<std::string>& partition_list,
                 BooleanQueryPtr& boolean_query,
                 const std::string& extra_params,
                 TopKQueryResult& topk_query_result) override;

 private:
    std::shared_ptr<::grpc::Channel> channel_;
    std::shared_ptr<GrpcClient> client_ptr_;
    bool connected_ = false;
};

}  // namespace milvus
