/*******************************************************************************
* Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
* Unauthorized copying of this file, via any medium is strictly prohibited.
* Proprietary and confidential.
******************************************************************************/
#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include "grpcClient.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Status;

namespace zilliz {
namespace milvus {
grpcClient::grpcClient(std::shared_ptr<::grpc::Channel> channel)
        : stub_(::milvus::grpc::MilvusService::NewStub(channel)) {

}

grpcClient::~grpcClient() {

}

void
grpcClient::CreateTable(const ::milvus::grpc::TableSchema table_schema) {
    ClientContext context;
    ::milvus::Status response;
    ::grpc::Status status = stub_->CreateTable(&context, table_schema, &response);

    if (!status.ok()) {
        std::cout << "CreateTable rpc failed!\n";
    }
}

bool
grpcClient::HasTable(const ::milvus::grpc::TableName table_name) {
    ClientContext context;
    ::milvus::grpc::BoolReply response;
    ::grpc::Status status = stub_->HasTable(&context, table_name, &response);

    if (!status.ok()) {
        std::cout << "HasTable rpc failed!\n";
    }
    return response.bool_reply();
}

void
grpcClient::DropTable(const ::milvus::grpc::TableName table_name) {
    ClientContext context;
    ::milvus::Status response;
    ::grpc::Status status = stub_->DropTable(&context, table_name, &response);

    if (!status.ok()) {
        std::cout << "DropTable rpc failed!\n";
    }
}

void
grpcClient::BuildIndex(const ::milvus::grpc::TableName table_name) {
    ClientContext context;
    ::milvus::Status response;
    ::grpc::Status status = stub_->BuildIndex(&context, table_name, &response);

    if (!status.ok()) {
        std::cout << "BuildIndex rpc failed!\n";
    }
}

void
grpcClient::InsertVector(::milvus::grpc::VectorIds& vector_ids, const ::milvus::grpc::InsertInfos insert_infos) {
    ClientContext context;
    ::grpc::Status status = stub_->InsertVector(&context, insert_infos, &vector_ids);
//    std::cout << vector_ids.vector_id_array_size();
    if (!status.ok()) {
        std::cout << "InsertVector rpc failed!\n";
    }
}

void
grpcClient::SearchVector(std::vector<::milvus::grpc::TopKQueryResult>& result_array,
                         const ::milvus::grpc::SearchVectorInfos search_vector_infos) {
    ::milvus::grpc::TopKQueryResult query_result;
    ClientContext context;
    std::unique_ptr<ClientReader<::milvus::grpc::TopKQueryResult> > reader(
            stub_->SearchVector(&context, search_vector_infos));

    while (reader->Read(&query_result)) {
        result_array.emplace_back(query_result);
    }

    ::grpc::Status status = reader->Finish();

    if (!status.ok()) {
        std::cout << "SearchVector rpc failed!\n";
    }
}

void
grpcClient::DescribeTable(::milvus::grpc::TableSchema &grpc_schema, const std::string table_name) {
    ClientContext context;
    ::milvus::grpc::TableName grpc_tablename;
    grpc_tablename.set_table_name(table_name);
    ::grpc::Status status = stub_->DescribeTable(&context, grpc_tablename, &grpc_schema);

    if (!status.ok()) {
        std::cout << "DescribeTable rpc failed!\n";
    }
}

int64_t
grpcClient::GetTableRowCount(const std::string table_name) {
    ClientContext context;
    ::milvus::grpc::TableRowCount response;
    ::milvus::grpc::TableName grpc_tablename;
    ::grpc::Status status = stub_->GetTableRowCount(&context, grpc_tablename, &response);

    if (!status.ok()) {
        std::cout << "DescribeTable rpc failed!\n";
    }
    return response.table_row_count();
}

void
grpcClient::ShowTables(std::vector<std::string> &table_array) {
    ClientContext context;
    ::milvus::grpc::Command command;
    std::unique_ptr<ClientReader<::milvus::grpc::TableName> > reader(
            stub_->ShowTables(&context, command));

    ::milvus::grpc::TableName table_name;
    while (reader->Read(&table_name)) {
        table_array.emplace_back(table_name.table_name());
    }
    ::grpc::Status status = reader->Finish();

    if (!status.ok()) {
        std::cout << "ShowTables rpc failed!\n";
    }
}

void
grpcClient::Ping(std::string &result, const std::string cmd) {
    ClientContext context;
    ::milvus::grpc::ServerStatus response;
    ::milvus::grpc::Command command;
    command.set_cmd(cmd);
    ::grpc::Status status = stub_->Ping(&context, command, &response);

    result = response.info();
    if (!status.ok()) {
        std::cout << "Ping rpc failed!\n";
    }
}

void
grpcClient::Disconnect() {
}

}
}