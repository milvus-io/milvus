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

    Status
    CreateCollection(const Mapping& mapping, const std::string& extra_params) override;

    Status
    DropCollection(const std::string& collection_name) override;

    bool
    HasCollection(const std::string& collection_name) override;

    Status
    ListCollections(std::vector<std::string>& collection_array) override;

    Status
    GetCollectionInfo(const std::string& collection_name, Mapping& mapping) override;

    Status
    GetCollectionStats(const std::string& collection_name, std::string& collection_stats) override;

    Status
    CountEntities(const std::string& collection_name, int64_t& entity_count) override;

    Status
    CreatePartition(const PartitionParam& partition_param) override;

    Status
    DropPartition(const PartitionParam& partition_param) override;

    bool
    HasPartition(const std::string& collection_name, const std::string& partition_tag) const override;

    Status
    ListPartitions(const std::string& collection_name, PartitionTagList& partition_tag_array) const override;

    Status
    CreateIndex(const IndexParam& index_param) override;

    Status
    DropIndex(const std::string& collection_name, const std::string& field_name,
              const std::string& index_name) const override;

    Status
    Insert(const std::string& collection_name, const std::string& partition_tag, const FieldValue& entity_array,
           std::vector<int64_t>& id_array) override;

    Status
    GetEntityByID(const std::string& collection_name, const std::vector<int64_t>& id_array,
                  std::string& entities) override;

    Status
    DeleteEntityByID(const std::string& collection_name, const std::vector<int64_t>& id_array) override;

    Status
    Search(const std::string& collection_name, const std::vector<std::string>& partition_list, const std::string& dsl,
           const VectorParam& vector_param, TopKQueryResult& query_result) override;

    Status
    SearchPB(const std::string& collection_name, const std::vector<std::string>& partition_list,
             BooleanQueryPtr& boolean_query, const std::string& extra_params, TopKQueryResult& query_result) override;

    Status
    ListIDInSegment(const std::string& collection_name, const int64_t& segment_id,
                    std::vector<int64_t>& id_array) override;

    Status
    LoadCollection(const std::string& collection_name) const override;

    Status
    Flush(const std::vector<std::string>& collection_name_array) override;

    Status
    Compact(const std::string& collection_name, const double& threshold) override;

 private:
    std::shared_ptr<::grpc::Channel> channel_;
    std::shared_ptr<GrpcClient> client_ptr_;
    bool connected_ = false;
};

}  // namespace milvus
