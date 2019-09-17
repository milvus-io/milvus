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

#include "MilvusApi.h"
#include "GrpcClient.h"

namespace milvus {

class ClientProxy : public Connection {
public:
    // Implementations of the Connection interface
    virtual Status
    Connect(const ConnectParam &param) override;

    virtual Status
    Connect(const std::string &uri) override;

    virtual Status
    Connected() const override;

    virtual Status
    Disconnect() override;

    virtual Status
    CreateTable(const TableSchema &param) override;

    virtual bool
    HasTable(const std::string &table_name) override;

    virtual Status
    DropTable(const std::string &table_name) override;

    virtual Status
    CreateIndex(const IndexParam &index_param) override;

    virtual Status
    Insert(const std::string &table_name,
                    const std::vector<RowRecord> &record_array,
                    std::vector<int64_t> &id_array) override;

    virtual Status
    Search(const std::string &table_name,
                    const std::vector<RowRecord> &query_record_array,
                    const std::vector<Range> &query_range_array,
                    int64_t topk,
                    int64_t nprobe,
                    std::vector<TopKQueryResult> &topk_query_result_array) override;

    virtual Status
    DescribeTable(const std::string &table_name, TableSchema &table_schema) override;

    virtual Status
    CountTable(const std::string &table_name, int64_t &row_count) override;

    virtual Status
    ShowTables(std::vector<std::string> &table_array) override;

    virtual std::string
    ClientVersion() const override;

    virtual std::string
    ServerVersion() const override;

    virtual std::string
    ServerStatus() const override;

    virtual std::string
    DumpTaskTables() const override;

    virtual Status
    DeleteByRange(Range &range,
                  const std::string &table_name) override;

    virtual Status
    PreloadTable(const std::string &table_name) const override;

    virtual Status
    DescribeIndex(const std::string &table_name, IndexParam &index_param) const override;

    virtual Status
    DropIndex(const std::string &table_name) const override;

private:
    std::shared_ptr<::grpc::Channel> channel_;

private:
    std::shared_ptr<GrpcClient> client_ptr_;
    bool connected_ = false;
};

}
