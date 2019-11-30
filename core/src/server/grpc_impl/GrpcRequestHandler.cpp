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

#include "server/grpc_impl/GrpcRequestHandler.h"
#include "server/grpc_impl/GrpcRequestScheduler.h"
#include "server/grpc_impl/request/CmdRequest.h"
#include "server/grpc_impl/request/CountTableRequest.h"
#include "server/grpc_impl/request/CreateIndexRequest.h"
#include "server/grpc_impl/request/CreatePartitionRequest.h"
#include "server/grpc_impl/request/CreateTableRequest.h"
#include "server/grpc_impl/request/DeleteByDateRequest.h"
#include "server/grpc_impl/request/DescribeIndexRequest.h"
#include "server/grpc_impl/request/DescribeTableRequest.h"
#include "server/grpc_impl/request/DropIndexRequest.h"
#include "server/grpc_impl/request/DropPartitionRequest.h"
#include "server/grpc_impl/request/DropTableRequest.h"
#include "server/grpc_impl/request/HasTableRequest.h"
#include "server/grpc_impl/request/InsertRequest.h"
#include "server/grpc_impl/request/PreloadTableRequest.h"
#include "server/grpc_impl/request/SearchRequest.h"
#include "server/grpc_impl/request/ShowPartitionsRequest.h"
#include "server/grpc_impl/request/ShowTablesRequest.h"
#include "utils/TimeRecorder.h"

#include <vector>

namespace milvus {
namespace server {
namespace grpc {

::grpc::Status
GrpcRequestHandler::CreateTable(::grpc::ServerContext* context, const ::milvus::grpc::TableSchema* request,
                                ::milvus::grpc::Status* response) {
    BaseRequestPtr request_ptr = CreateTableRequest::Create(request);
    GrpcRequestScheduler::ExecRequest(request_ptr, response);
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::HasTable(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                             ::milvus::grpc::BoolReply* response) {
    bool has_table = false;
    BaseRequestPtr request_ptr = HasTableRequest::Create(request->table_name(), has_table);
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecRequest(request_ptr, &grpc_status);
    response->set_bool_reply(has_table);
    response->mutable_status()->set_reason(grpc_status.reason());
    response->mutable_status()->set_error_code(grpc_status.error_code());
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DropTable(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                              ::milvus::grpc::Status* response) {
    BaseRequestPtr request_ptr = DropTableRequest::Create(request->table_name());
    GrpcRequestScheduler::ExecRequest(request_ptr, response);
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::CreateIndex(::grpc::ServerContext* context, const ::milvus::grpc::IndexParam* request,
                                ::milvus::grpc::Status* response) {
    BaseRequestPtr request_ptr = CreateIndexRequest::Create(request);
    GrpcRequestScheduler::ExecRequest(request_ptr, response);
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::Insert(::grpc::ServerContext* context, const ::milvus::grpc::InsertParam* request,
                           ::milvus::grpc::VectorIds* response) {
    BaseRequestPtr request_ptr = InsertRequest::Create(request, response);
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecRequest(request_ptr, &grpc_status);
    response->mutable_status()->set_reason(grpc_status.reason());
    response->mutable_status()->set_error_code(grpc_status.error_code());
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::Search(::grpc::ServerContext* context, const ::milvus::grpc::SearchParam* request,
                           ::milvus::grpc::TopKQueryResult* response) {
    std::vector<std::string> file_id_array;
    BaseRequestPtr request_ptr = SearchRequest::Create(request, file_id_array, response);
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecRequest(request_ptr, &grpc_status);
    response->mutable_status()->set_error_code(grpc_status.error_code());
    response->mutable_status()->set_reason(grpc_status.reason());
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::SearchInFiles(::grpc::ServerContext* context, const ::milvus::grpc::SearchInFilesParam* request,
                                  ::milvus::grpc::TopKQueryResult* response) {
    std::vector<std::string> file_id_array;
    for (int i = 0; i < request->file_id_array_size(); i++) {
        file_id_array.push_back(request->file_id_array(i));
    }
    ::milvus::grpc::SearchInFilesParam* request_mutable = const_cast<::milvus::grpc::SearchInFilesParam*>(request);
    BaseRequestPtr request_ptr =
        SearchRequest::Create(request_mutable->mutable_search_param(), file_id_array, response);
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecRequest(request_ptr, &grpc_status);
    response->mutable_status()->set_error_code(grpc_status.error_code());
    response->mutable_status()->set_reason(grpc_status.reason());
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DescribeTable(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                                  ::milvus::grpc::TableSchema* response) {
    BaseRequestPtr request_ptr = DescribeTableRequest::Create(request->table_name(), response);
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecRequest(request_ptr, &grpc_status);
    response->mutable_status()->set_error_code(grpc_status.error_code());
    response->mutable_status()->set_reason(grpc_status.reason());
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::CountTable(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                               ::milvus::grpc::TableRowCount* response) {
    int64_t row_count = 0;
    BaseRequestPtr request_ptr = CountTableRequest::Create(request->table_name(), row_count);
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecRequest(request_ptr, &grpc_status);
    response->set_table_row_count(row_count);
    response->mutable_status()->set_reason(grpc_status.reason());
    response->mutable_status()->set_error_code(grpc_status.error_code());
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::ShowTables(::grpc::ServerContext* context, const ::milvus::grpc::Command* request,
                               ::milvus::grpc::TableNameList* response) {
    BaseRequestPtr request_ptr = ShowTablesRequest::Create(response);
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecRequest(request_ptr, &grpc_status);
    response->mutable_status()->set_error_code(grpc_status.error_code());
    response->mutable_status()->set_reason(grpc_status.reason());
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::Cmd(::grpc::ServerContext* context, const ::milvus::grpc::Command* request,
                        ::milvus::grpc::StringReply* response) {
    std::string result;
    BaseRequestPtr request_ptr = CmdRequest::Create(request->cmd(), result);
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecRequest(request_ptr, &grpc_status);
    response->set_string_reply(result);
    response->mutable_status()->set_reason(grpc_status.reason());
    response->mutable_status()->set_error_code(grpc_status.error_code());
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DeleteByDate(::grpc::ServerContext* context, const ::milvus::grpc::DeleteByDateParam* request,
                                 ::milvus::grpc::Status* response) {
    BaseRequestPtr request_ptr = DeleteByDateRequest::Create(request);
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecRequest(request_ptr, &grpc_status);
    response->set_error_code(grpc_status.error_code());
    response->set_reason(grpc_status.reason());
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::PreloadTable(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                                 ::milvus::grpc::Status* response) {
    BaseRequestPtr request_ptr = PreloadTableRequest::Create(request->table_name());
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecRequest(request_ptr, &grpc_status);
    response->set_reason(grpc_status.reason());
    response->set_error_code(grpc_status.error_code());
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DescribeIndex(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                                  ::milvus::grpc::IndexParam* response) {
    BaseRequestPtr request_ptr = DescribeIndexRequest::Create(request->table_name(), response);
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecRequest(request_ptr, &grpc_status);
    response->mutable_status()->set_reason(grpc_status.reason());
    response->mutable_status()->set_error_code(grpc_status.error_code());
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DropIndex(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                              ::milvus::grpc::Status* response) {
    BaseRequestPtr request_ptr = DropIndexRequest::Create(request->table_name());
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecRequest(request_ptr, &grpc_status);
    response->set_reason(grpc_status.reason());
    response->set_error_code(grpc_status.error_code());
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::CreatePartition(::grpc::ServerContext* context, const ::milvus::grpc::PartitionParam* request,
                                    ::milvus::grpc::Status* response) {
    BaseRequestPtr request_ptr = CreatePartitionRequest::Create(request);
    GrpcRequestScheduler::ExecRequest(request_ptr, response);
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::ShowPartitions(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                                   ::milvus::grpc::PartitionList* response) {
    BaseRequestPtr request_ptr = ShowPartitionsRequest::Create(request->table_name(), response);
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecRequest(request_ptr, &grpc_status);
    response->mutable_status()->set_reason(grpc_status.reason());
    response->mutable_status()->set_error_code(grpc_status.error_code());
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DropPartition(::grpc::ServerContext* context, const ::milvus::grpc::PartitionParam* request,
                                  ::milvus::grpc::Status* response) {
    BaseRequestPtr request_ptr = DropPartitionRequest::Create(request);
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecRequest(request_ptr, &grpc_status);
    response->set_reason(grpc_status.reason());
    response->set_error_code(grpc_status.error_code());
    return ::grpc::Status::OK;
}

}  // namespace grpc
}  // namespace server
}  // namespace milvus
