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
    Connect(const ConnectParam& param) override;

    Status
    Connect(const std::string& uri) override;

    Status
    Connected() const override;

    Status
    Disconnect() override;

    Status
    CreateTable(const TableSchema& param) override;

    bool
    HasTable(const std::string& table_name) override;

    Status
    DropTable(const std::string& table_name) override;

    Status
    CreateIndex(const IndexParam& index_param) override;

    Status
    Insert(const std::string& table_name, const std::string& partition_tag, const std::vector<RowRecord>& record_array,
           std::vector<int64_t>& id_array) override;

    Status
    Search(const std::string& table_name, const std::vector<std::string>& partition_tags,
           const std::vector<RowRecord>& query_record_array, const std::vector<Range>& query_range_array, int64_t topk,
           int64_t nprobe, TopKQueryResult& topk_query_result) override;

    Status
    DescribeTable(const std::string& table_name, TableSchema& table_schema) override;

    Status
    CountTable(const std::string& table_name, int64_t& row_count) override;

    Status
    ShowTables(std::vector<std::string>& table_array) override;

    std::string
    ClientVersion() const override;

    std::string
    ServerVersion() const override;

    std::string
    ServerStatus() const override;

    std::string
    DumpTaskTables() const override;

    Status
    DeleteByDate(const std::string& table_name, const Range& range) override;

    Status
    PreloadTable(const std::string& table_name) const override;

    Status
    DescribeIndex(const std::string& table_name, IndexParam& index_param) const override;

    Status
    DropIndex(const std::string& table_name) const override;

    Status
    CreatePartition(const PartitionParam& partition_param) override;

    Status
    ShowPartitions(const std::string& table_name, PartitionList& partition_array) const override;

    Status
    DropPartition(const PartitionParam& partition_param) override;

    Status
    GetConfig(const std::string& node_name, std::string& value) const override;

    Status
    SetConfig(const std::string& node_name, const std::string& value) const override;

 private:
    std::shared_ptr<::grpc::Channel> channel_;
    std::shared_ptr<GrpcClient> client_ptr_;
    bool connected_ = false;
};

}  // namespace milvus
