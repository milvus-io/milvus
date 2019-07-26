/*******************************************************************************
* Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
* Unauthorized copying of this file, via any medium is strictly prohibited.
* Proprietary and confidential.
******************************************************************************/
#pragma once
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

#include "milvus.grpc.pb.h"
//#include "status.grpc.pb.h"

#include <memory>

namespace milvus {
class GrpcClient {
public:
    explicit GrpcClient(std::shared_ptr<::grpc::Channel>& channel);

    virtual ~GrpcClient();

    void CreateTable(const grpc::TableSchema& table_schema);

    bool HasTable(const grpc::TableName& table_name);

    void DropTable(const grpc::TableName& table_name);

    void BuildIndex(const grpc::TableName& table_name);

    void InsertVector(grpc::VectorIds& vector_ids, const grpc::InsertInfos& insert_infos);

    void SearchVector(std::vector<grpc::TopKQueryResult>& result_array,
                      const grpc::SearchVectorInfos& search_vector_infos);

    void DescribeTable(grpc::TableSchema& grpc_schema, const std::string& table_name);

    int64_t GetTableRowCount(const std::string& table_name);

    void ShowTables(std::vector<std::string> &table_array);

    void Ping(std::string &result, const std::string& cmd);

    void Disconnect();

private:
    std::unique_ptr<grpc::MilvusService::Stub> stub_;
};

}
