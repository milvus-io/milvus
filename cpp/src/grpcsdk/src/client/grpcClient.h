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

#include "Status.h"
#include "milvus.grpc.pb.h"
#include "status.grpc.pb.h"

#include <memory>

namespace zilliz {
namespace milvus {
class grpcClient {
public:
    grpcClient(std::shared_ptr<::grpc::Channel> channel);

    virtual ~grpcClient();

    void CreateTable(const ::milvus::grpc::TableSchema table_schema);

    bool HasTable(const ::milvus::grpc::TableName table_name);

    void DropTable(const ::milvus::grpc::TableName table_name);

    void BuildIndex(const ::milvus::grpc::TableName table_name);

    void InsertVector(::milvus::grpc::VectorIds& vector_ids, const ::milvus::grpc::InsertInfos insert_infos);

    void SearchVector(std::vector<::milvus::grpc::TopKQueryResult>& result_array,
                      const ::milvus::grpc::SearchVectorInfos search_vector_infos);

    void DescribeTable(::milvus::grpc::TableSchema& grpc_schema, const std::string table_name);

    int64_t GetTableRowCount(const std::string table_name);

    void ShowTables(std::vector<std::string> &table_array);

    void Ping(std::string &result, const std::string cmd);

    void Disconnect();

private:
    std::unique_ptr<::milvus::grpc::MilvusService::Stub> stub_;
};

}
}