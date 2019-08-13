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

Status
GrpcClient::CreateTable(const ::milvus::grpc::TableSchema& table_schema) {
    ClientContext context;
    grpc::Status response;
    ::grpc::Status grpc_status = stub_->CreateTable(&context, table_schema, &response);

    if (!grpc_status.ok()) {
        std::cerr << "CreateTable gRPC failed!" << std::endl;
        return Status(StatusCode::RPCFailed, grpc_status.error_message());
    }

    if (response.error_code() != grpc::SUCCESS) {
        std::cerr << response.reason() << std::endl;
        return Status(StatusCode::ServerFailed, response.reason());
    }
    return Status::OK();
}

bool
GrpcClient::HasTable(const ::milvus::grpc::TableName& table_name,
                     Status& status) {
    ClientContext context;
    ::milvus::grpc::BoolReply response;
    ::grpc::Status grpc_status = stub_->HasTable(&context, table_name, &response);

    if (!grpc_status.ok()) {
        std::cerr << "HasTable gRPC failed!" << std::endl;
        status = Status(StatusCode::RPCFailed, grpc_status.error_message());
    }
    if (response.status().error_code() != grpc::SUCCESS) {
        std::cerr << response.status().reason() << std::endl;
        status = Status(StatusCode::ServerFailed, response.status().reason());
    }
    status = Status::OK();
    return response.bool_reply();
}

Status
GrpcClient::DropTable(const ::milvus::grpc::TableName& table_name) {
    ClientContext context;
    grpc::Status response;
    ::grpc::Status grpc_status = stub_->DropTable(&context, table_name, &response);

    if (!grpc_status.ok()) {
        std::cerr << "DropTable gRPC failed!" << std::endl;
        return Status(StatusCode::RPCFailed, grpc_status.error_message());
    }
    if (response.error_code() != grpc::SUCCESS) {
        std::cerr << response.reason() << std::endl;
        return Status(StatusCode::ServerFailed, response.reason());
    }

    return Status::OK();
}

Status
GrpcClient::CreateIndex(const ::milvus::grpc::IndexParam& index_param) {
    ClientContext context;
    grpc::Status response;
    ::grpc::Status grpc_status = stub_->CreateIndex(&context, index_param, &response);

    if (!grpc_status.ok()) {
        std::cerr << "BuildIndex rpc failed!" << std::endl;
        return Status(StatusCode::RPCFailed, grpc_status.error_message());
    }
    if (response.error_code() != grpc::SUCCESS) {
        std::cerr << response.reason() << std::endl;
        return Status(StatusCode::ServerFailed, response.reason());
    }

    return Status::OK();
}

void
GrpcClient::Insert(::milvus::grpc::VectorIds& vector_ids,
                         const ::milvus::grpc::InsertParam& insert_param,
                         Status& status) {
    ClientContext context;
    ::grpc::Status grpc_status = stub_->Insert(&context, insert_param, &vector_ids);

    if (!grpc_status.ok()) {
        std::cerr << "InsertVector rpc failed!" << std::endl;
        status = Status(StatusCode::RPCFailed, grpc_status.error_message());
        return;
    }
    if (vector_ids.status().error_code() != grpc::SUCCESS) {
        std::cerr << vector_ids.status().reason() << std::endl;
        status = Status(StatusCode::ServerFailed, vector_ids.status().reason());
        return;
    }

    status = Status::OK();
}

Status
GrpcClient::Search(std::vector<::milvus::grpc::TopKQueryResult>& result_array,
                         const ::milvus::grpc::SearchParam& search_param) {
    ::milvus::grpc::TopKQueryResult query_result;
    ClientContext context;
    std::unique_ptr<ClientReader<::milvus::grpc::TopKQueryResult> > reader(
            stub_->Search(&context, search_param));

    while (reader->Read(&query_result)) {
        result_array.emplace_back(query_result);
    }

    ::grpc::Status grpc_status = reader->Finish();

    if (!grpc_status.ok()) {
        std::cerr << "SearchVector rpc failed!" << std::endl;
        std::cerr << grpc_status.error_message() << std::endl;
        return Status(StatusCode::RPCFailed, grpc_status.error_message());
    }
    if (query_result.status().error_code() != grpc::SUCCESS) {
        std::cerr << query_result.status().reason() << std::endl;
        return Status(StatusCode::ServerFailed,
            query_result.status().reason());
    }

    return Status::OK();
}

Status
GrpcClient::DescribeTable(::milvus::grpc::TableSchema& grpc_schema,
                          const std::string& table_name) {
    ClientContext context;
    ::milvus::grpc::TableName grpc_tablename;
    grpc_tablename.set_table_name(table_name);
    ::grpc::Status grpc_status = stub_->DescribeTable(&context, grpc_tablename, &grpc_schema);

    if (!grpc_status.ok()) {
        std::cerr << "DescribeTable rpc failed!" << std::endl;
        std::cerr << grpc_status.error_message() << std::endl;
        return Status(StatusCode::RPCFailed, grpc_status.error_message());
    }

    if (grpc_schema.table_name().status().error_code() != grpc::SUCCESS) {
        std::cerr << grpc_schema.table_name().status().reason() << std::endl;
        return Status(StatusCode::ServerFailed,
            grpc_schema.table_name().status().reason());
    }

    return Status::OK();
}

int64_t
GrpcClient::CountTable(const std::string& table_name, Status& status) {
    ClientContext context;
    ::milvus::grpc::TableRowCount response;
    ::milvus::grpc::TableName grpc_tablename;
    grpc_tablename.set_table_name(table_name);
    ::grpc::Status grpc_status = stub_->CountTable(&context, grpc_tablename, &response);

    if (!grpc_status.ok()) {
        std::cerr << "DescribeTable rpc failed!" << std::endl;
        status = Status(StatusCode::RPCFailed,  grpc_status.error_message());
        return -1;
    }

    if (response.status().error_code() != grpc::SUCCESS) {
        std::cerr << response.status().reason() << std::endl;
        status = Status(StatusCode::ServerFailed, response.status().reason());
        return -1;
    }

    status = Status::OK();
    return response.table_row_count();
}

Status
GrpcClient::ShowTables(std::vector<std::string> &table_array) {
    ClientContext context;
    ::milvus::grpc::Command command;
    std::unique_ptr<ClientReader<::milvus::grpc::TableName> > reader(
            stub_->ShowTables(&context, command));

    ::milvus::grpc::TableName table_name;
    while (reader->Read(&table_name)) {
        table_array.emplace_back(table_name.table_name());
    }
    ::grpc::Status grpc_status = reader->Finish();

    if (!grpc_status.ok()) {
        std::cerr << "ShowTables gRPC failed!" << std::endl;
        std::cerr << grpc_status.error_message() << std::endl;
        return Status(StatusCode::RPCFailed, grpc_status.error_message());
    }

    if (table_name.status().error_code() != grpc::SUCCESS) {
        std::cerr << table_name.status().reason() << std::endl;
        return Status(StatusCode::ServerFailed,
            table_name.status().reason());
    }

    return Status::OK();
}

Status
GrpcClient::Cmd(std::string &result,
                 const std::string& cmd) {
    ClientContext context;
    ::milvus::grpc::StringReply response;
    ::milvus::grpc::Command command;
    command.set_cmd(cmd);
    ::grpc::Status grpc_status = stub_->Cmd(&context, command, &response);

    result = response.string_reply();
    if (!grpc_status.ok()) {
        std::cerr << "Ping gRPC failed!" << std::endl;
        return Status(StatusCode::RPCFailed, grpc_status.error_message());
    }

    if (response.status().error_code() != grpc::SUCCESS) {
        std::cerr << response.status().reason() << std::endl;
        return Status(StatusCode::ServerFailed, response.status().reason());
    }

    return Status::OK();
}

Status
GrpcClient::Disconnect() {
    stub_.release();
    return Status::OK();
}

}