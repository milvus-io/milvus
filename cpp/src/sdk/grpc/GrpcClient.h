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
#include "MilvusApi.h"

#include "milvus.grpc.pb.h"
//#include "status.grpc.pb.h"

#include <memory>

namespace milvus {
class GrpcClient {
public:
    explicit
    GrpcClient(std::shared_ptr<::grpc::Channel>& channel);

    virtual
    ~GrpcClient();

    Status
    CreateTable(const grpc::TableSchema& table_schema);

    bool
    HasTable(const grpc::TableName& table_name, Status& status);

    Status
    DropTable(const grpc::TableName& table_name);

    Status
    CreateIndex(const grpc::IndexParam& index_param);

    void
    Insert(grpc::VectorIds& vector_ids,
                      const grpc::InsertParam& insert_param,
                      Status& status);

    Status
    Search(std::vector<grpc::TopKQueryResult>& result_array,
                      const grpc::SearchParam& search_param);

    Status
    DescribeTable(grpc::TableSchema& grpc_schema,
                        const std::string& table_name);

    int64_t
    CountTable(const std::string& table_name, Status& status);

    Status
    ShowTables(std::vector<std::string> &table_array);

    Status
    Cmd(std::string &result, const std::string& cmd);

    Status
    DeleteByRange(grpc::DeleteByRangeParam &delete_by_range_param);

    Status
    PreloadTable(grpc::TableName &table_name);

    Status
    DescribeIndex(grpc::TableName &table_name, grpc::IndexParam &index_param);

    Status
    DropIndex(grpc::TableName &table_name);

    Status
    Disconnect();

private:
    std::unique_ptr<grpc::MilvusService::Stub> stub_;
};

}
