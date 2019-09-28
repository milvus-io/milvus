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
#include "server/grpc_impl/GrpcRequestTask.h"
#include "utils/TimeRecorder.h"

#include <vector>

namespace zilliz {
namespace milvus {
namespace server {
namespace grpc {

::grpc::Status
GrpcRequestHandler::CreateTable(::grpc::ServerContext* context, const ::milvus::grpc::TableSchema* request,
                                ::milvus::grpc::Status* response) {
    BaseTaskPtr task_ptr = CreateTableTask::Create(request);
    GrpcRequestScheduler::ExecTask(task_ptr, response);
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::HasTable(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                             ::milvus::grpc::BoolReply* response) {
    bool has_table = false;
    BaseTaskPtr task_ptr = HasTableTask::Create(request->table_name(), has_table);
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecTask(task_ptr, &grpc_status);
    response->set_bool_reply(has_table);
    response->mutable_status()->set_reason(grpc_status.reason());
    response->mutable_status()->set_error_code(grpc_status.error_code());
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DropTable(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                              ::milvus::grpc::Status* response) {
    BaseTaskPtr task_ptr = DropTableTask::Create(request->table_name());
    GrpcRequestScheduler::ExecTask(task_ptr, response);
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::CreateIndex(::grpc::ServerContext* context, const ::milvus::grpc::IndexParam* request,
                                ::milvus::grpc::Status* response) {
    BaseTaskPtr task_ptr = CreateIndexTask::Create(request);
    GrpcRequestScheduler::ExecTask(task_ptr, response);
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::Insert(::grpc::ServerContext* context, const ::milvus::grpc::InsertParam* request,
                           ::milvus::grpc::VectorIds* response) {
    BaseTaskPtr task_ptr = InsertTask::Create(request, response);
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecTask(task_ptr, &grpc_status);
    response->mutable_status()->set_reason(grpc_status.reason());
    response->mutable_status()->set_error_code(grpc_status.error_code());
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::Search(::grpc::ServerContext* context, const ::milvus::grpc::SearchParam* request,
                           ::milvus::grpc::TopKQueryResultList* response) {
    std::vector<std::string> file_id_array;
    BaseTaskPtr task_ptr = SearchTask::Create(request, file_id_array, response);
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecTask(task_ptr, &grpc_status);
    response->mutable_status()->set_error_code(grpc_status.error_code());
    response->mutable_status()->set_reason(grpc_status.reason());
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::SearchInFiles(::grpc::ServerContext* context, const ::milvus::grpc::SearchInFilesParam* request,
                                  ::milvus::grpc::TopKQueryResultList* response) {
    std::vector<std::string> file_id_array;
    for (int i = 0; i < request->file_id_array_size(); i++) {
        file_id_array.push_back(request->file_id_array(i));
    }
    ::milvus::grpc::SearchInFilesParam* request_mutable = const_cast<::milvus::grpc::SearchInFilesParam*>(request);
    BaseTaskPtr task_ptr = SearchTask::Create(request_mutable->mutable_search_param(), file_id_array, response);
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecTask(task_ptr, &grpc_status);
    response->mutable_status()->set_error_code(grpc_status.error_code());
    response->mutable_status()->set_reason(grpc_status.reason());
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DescribeTable(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                                  ::milvus::grpc::TableSchema* response) {
    BaseTaskPtr task_ptr = DescribeTableTask::Create(request->table_name(), response);
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecTask(task_ptr, &grpc_status);
    response->mutable_status()->set_error_code(grpc_status.error_code());
    response->mutable_status()->set_reason(grpc_status.reason());
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::CountTable(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                               ::milvus::grpc::TableRowCount* response) {
    int64_t row_count = 0;
    BaseTaskPtr task_ptr = CountTableTask::Create(request->table_name(), row_count);
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecTask(task_ptr, &grpc_status);
    response->set_table_row_count(row_count);
    response->mutable_status()->set_reason(grpc_status.reason());
    response->mutable_status()->set_error_code(grpc_status.error_code());
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::ShowTables(::grpc::ServerContext* context, const ::milvus::grpc::Command* request,
                               ::milvus::grpc::TableNameList* response) {
    BaseTaskPtr task_ptr = ShowTablesTask::Create(response);
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecTask(task_ptr, &grpc_status);
    response->mutable_status()->set_error_code(grpc_status.error_code());
    response->mutable_status()->set_reason(grpc_status.reason());
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::Cmd(::grpc::ServerContext* context, const ::milvus::grpc::Command* request,
                        ::milvus::grpc::StringReply* response) {
    std::string result;
    BaseTaskPtr task_ptr = CmdTask::Create(request->cmd(), result);
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecTask(task_ptr, &grpc_status);
    response->set_string_reply(result);
    response->mutable_status()->set_reason(grpc_status.reason());
    response->mutable_status()->set_error_code(grpc_status.error_code());
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DeleteByRange(::grpc::ServerContext* context, const ::milvus::grpc::DeleteByRangeParam* request,
                                  ::milvus::grpc::Status* response) {
    BaseTaskPtr task_ptr = DeleteByRangeTask::Create(request);
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecTask(task_ptr, &grpc_status);
    response->set_error_code(grpc_status.error_code());
    response->set_reason(grpc_status.reason());
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::PreloadTable(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                                 ::milvus::grpc::Status* response) {
    BaseTaskPtr task_ptr = PreloadTableTask::Create(request->table_name());
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecTask(task_ptr, &grpc_status);
    response->set_reason(grpc_status.reason());
    response->set_error_code(grpc_status.error_code());
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DescribeIndex(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                                  ::milvus::grpc::IndexParam* response) {
    BaseTaskPtr task_ptr = DescribeIndexTask::Create(request->table_name(), response);
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecTask(task_ptr, &grpc_status);
    response->mutable_status()->set_reason(grpc_status.reason());
    response->mutable_status()->set_error_code(grpc_status.error_code());
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DropIndex(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                              ::milvus::grpc::Status* response) {
    BaseTaskPtr task_ptr = DropIndexTask::Create(request->table_name());
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecTask(task_ptr, &grpc_status);
    response->set_reason(grpc_status.reason());
    response->set_error_code(grpc_status.error_code());
    return ::grpc::Status::OK;
}

}  // namespace grpc
}  // namespace server
}  // namespace milvus
}  // namespace zilliz
