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

#include "GrpcClient.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Status;

namespace milvus {
GrpcClient::GrpcClient(std::shared_ptr<::grpc::Channel>& channel)
        : stub_(::milvus::grpc::MilvusService::NewStub(channel)) {

}

GrpcClient::~GrpcClient() = default;

void
GrpcClient::CreateTable(const ::milvus::grpc::TableSchema& table_schema) {
    ClientContext context;
    grpc::Status response;
    ::grpc::Status status = stub_->CreateTable(&context, table_schema, &response);

    if (!status.ok()) {
        std::cerr << "CreateTable gRPC failed!" << std::endl;
    }

    if (response.error_code() != grpc::SUCCESS) {
        std::cerr << response.reason() << std::endl;
    }
}

bool
GrpcClient::HasTable(const ::milvus::grpc::TableName& table_name) {
    ClientContext context;
    ::milvus::grpc::BoolReply response;
    ::grpc::Status status = stub_->HasTable(&context, table_name, &response);

    if (!status.ok()) {
        std::cerr << "HasTable gRPC failed!" << std::endl;
    }
    if (response.status().error_code() != grpc::SUCCESS) {
        std::cerr << response.status().reason() << std::endl;
    }
    return response.bool_reply();
}

void
GrpcClient::DropTable(const ::milvus::grpc::TableName& table_name) {
    ClientContext context;
    grpc::Status response;
    ::grpc::Status status = stub_->DropTable(&context, table_name, &response);

    if (!status.ok()) {
        std::cerr << "DropTable gRPC failed!\n";
    }

    if (response.error_code() != grpc::SUCCESS) {
        std::cerr << response.reason() << std::endl;
    }
}

void
GrpcClient::BuildIndex(const ::milvus::grpc::TableName& table_name) {
    ClientContext context;
    grpc::Status response;
    ::grpc::Status status = stub_->BuildIndex(&context, table_name, &response);

    if (!status.ok()) {
        std::cerr << "BuildIndex rpc failed!\n";
    }

    if (response.error_code() != grpc::SUCCESS) {
        std::cerr << response.reason() << std::endl;
    }
}

void
GrpcClient::InsertVector(::milvus::grpc::VectorIds& vector_ids, const ::milvus::grpc::InsertInfos& insert_infos) {
    ClientContext context;
    ::grpc::Status status = stub_->InsertVector(&context, insert_infos, &vector_ids);

    if (!status.ok()) {
        std::cerr << "InsertVector rpc failed!\n";
    }

    if (vector_ids.status().error_code() != grpc::SUCCESS) {
        std::cerr << vector_ids.status().reason() << std::endl;
    }
}

void
GrpcClient::SearchVector(std::vector<::milvus::grpc::TopKQueryResult>& result_array,
                         const ::milvus::grpc::SearchVectorInfos& search_vector_infos) {
    ::milvus::grpc::TopKQueryResult query_result;
    ClientContext context;
    std::unique_ptr<ClientReader<::milvus::grpc::TopKQueryResult> > reader(
            stub_->SearchVector(&context, search_vector_infos));

    while (reader->Read(&query_result)) {
        result_array.emplace_back(query_result);
    }

    ::grpc::Status status = reader->Finish();

    if (!status.ok()) {
        std::cerr << "SearchVector rpc failed!" << std::endl;
        std::cerr << status.error_message() << std::endl;
    }

    if (query_result.status().error_code() != grpc::SUCCESS) {
        std::cerr << query_result.status().reason() << std::endl;
    }
}

void
GrpcClient::DescribeTable(::milvus::grpc::TableSchema& grpc_schema, const std::string& table_name) {
    ClientContext context;
    ::milvus::grpc::TableName grpc_tablename;
    grpc_tablename.set_table_name(table_name);
    ::grpc::Status status = stub_->DescribeTable(&context, grpc_tablename, &grpc_schema);

    if (!status.ok()) {
        std::cerr << "DescribeTable rpc failed!" << std::endl;
        std::cerr << status.error_message() << std::endl;
    }

    if (grpc_schema.table_name().status().error_code() != grpc::SUCCESS) {
        std::cerr << grpc_schema.table_name().status().reason() << std::endl;
    }
}

int64_t
GrpcClient::GetTableRowCount(const std::string& table_name) {
    ClientContext context;
    ::milvus::grpc::TableRowCount response;
    ::milvus::grpc::TableName grpc_tablename;
    grpc_tablename.set_table_name(table_name);
    ::grpc::Status status = stub_->GetTableRowCount(&context, grpc_tablename, &response);

    if (!status.ok()) {
        std::cerr << "DescribeTable rpc failed!\n";
        return -1;
    }

    if (response.status().error_code() != grpc::SUCCESS) {
        std::cerr << response.status().reason() << std::endl;
        return -1;
    }
    return response.table_row_count();
}

void
GrpcClient::ShowTables(std::vector<std::string> &table_array) {
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
        std::cerr << "ShowTables gRPC failed!" << std::endl;
        std::cerr << status.error_message() << std::endl;
    }

    if (table_name.status().error_code() != grpc::SUCCESS) {
        std::cerr << table_name.status().reason() << std::endl;
    }
}

void
GrpcClient::Ping(std::string &result, const std::string& cmd) {
    ClientContext context;
    ::milvus::grpc::ServerStatus response;
    ::milvus::grpc::Command command;
    command.set_cmd(cmd);
    std::cout << "in ping \n";
    ::grpc::Status status = stub_->Ping(&context, command, &response);
    std::cout << "finish ping stub \n";

    result = response.info();
    if (!status.ok()) {
        std::cerr << "Ping gRPC failed!" << std::endl;
    }

    if (response.status().error_code() != grpc::SUCCESS) {
        std::cerr << response.status().reason() << std::endl;
    }
}

void
GrpcClient::Disconnect() {
    stub_.release();
}

}