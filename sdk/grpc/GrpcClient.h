// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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
    CreateTable(const grpc::TableSchema& table_schema);

    bool
    HasTable(const grpc::TableName& table_name, Status& status);

    Status
    DropTable(const grpc::TableName& table_name);

    Status
    CreateIndex(const grpc::IndexParam& index_param);

    void
    Insert(grpc::VectorIds& vector_ids, const grpc::InsertParam& insert_param, Status& status);

    Status
    Search(::milvus::grpc::TopKQueryResult& topk_query_result, const grpc::SearchParam& search_param);

    Status
    DescribeTable(grpc::TableSchema& grpc_schema, const std::string& table_name);

    int64_t
    CountTable(const std::string& table_name, Status& status);

    Status
    ShowTables(milvus::grpc::TableNameList& table_name_list);

    Status
    Cmd(std::string& result, const std::string& cmd);

    Status
    DeleteByDate(grpc::DeleteByDateParam& delete_by_range_param);

    Status
    PreloadTable(grpc::TableName& table_name);

    Status
    DescribeIndex(grpc::TableName& table_name, grpc::IndexParam& index_param);

    Status
    DropIndex(grpc::TableName& table_name);

    Status
    CreatePartition(const grpc::PartitionParam& partition_param);

    Status
    ShowPartitions(const grpc::TableName& table_name, grpc::PartitionList& partition_array) const;

    Status
    DropPartition(const ::milvus::grpc::PartitionParam& partition_param);

    Status
    Disconnect();

 private:
    std::unique_ptr<grpc::MilvusService::Stub> stub_;
};

}  // namespace milvus
