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

#include "include/MilvusApi.h"
#include "grpc-gen/gen-milvus/milvus.grpc.pb.h"
//#include "grpc/gen-status/status.grpc.pb.h"

#include <chrono>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <thread>

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

namespace milvus {
class GrpcClient {
 public:
    explicit GrpcClient(std::shared_ptr<::grpc::Channel>& channel);

    virtual ~GrpcClient();

    Status
    CreateCollection(const grpc::Mapping& collection_schema);

    bool
    HasCollection(const grpc::CollectionName& collection_name, Status& status);

    Status
    DropCollection(const grpc::CollectionName& collection_name);

    Status
    CreateIndex(const grpc::IndexParam& index_param);

    Status
    Insert(const grpc::InsertParam& insert_param, grpc::EntityIds& entity_ids);

    Status
    GetEntityByID(const grpc::EntityIdentity& enrtity_identity, ::milvus::grpc::Entities& entities);

    Status
    ListIDInSegment(const grpc::GetEntityIDsParam& param, grpc::EntityIds& entity_ids);

    Status
    Search(const grpc::SearchParam& search_param, ::milvus::grpc::QueryResult& topk_query_result);

    Status
    GetCollectionInfo(const std::string& collection_name, grpc::Mapping& grpc_schema);

    int64_t
    CountEntities(grpc::CollectionName& collection_name, Status& status);

    Status
    ListCollections(milvus::grpc::CollectionNameList& collection_name_list);

    Status
    GetCollectionStats(grpc::CollectionName& collection_name, grpc::CollectionInfo& collection_stats);

    Status
    Cmd(const std::string& cmd, std::string& result);

    Status
    DeleteEntityByID(grpc::DeleteByIDParam& delete_by_id_param);

    Status
    LoadCollection(grpc::CollectionName& collection_name);

    Status
    DropIndex(grpc::IndexParam& index_param);

    Status
    CreatePartition(const grpc::PartitionParam& partition_param);

    bool
    HasPartition(const grpc::PartitionParam& partition_param, Status& status) const;

    Status
    ListPartitions(const grpc::CollectionName& collection_name, grpc::PartitionList& partition_array) const;

    Status
    DropPartition(const ::milvus::grpc::PartitionParam& partition_param);

    Status
    Flush(const std::string& collection_name);

    Status
    Compact(milvus::grpc::CompactParam& compact_param);

    Status
    Disconnect();

    /*******************************New Interface**********************************/

    Status
    SearchPB(milvus::grpc::SearchParamPB& search_param, milvus::grpc::QueryResult& result);

 private:
    std::unique_ptr<grpc::MilvusService::Stub> stub_;
};

}  // namespace milvus
