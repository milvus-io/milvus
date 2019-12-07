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
#include "src/server/delivery/RequestScheduler.h"
#include "server/delivery/request/CmdRequest.h"
#include "server/delivery/request/CountTableRequest.h"
#include "server/delivery/request/CreateIndexRequest.h"
#include "server/delivery/request/CreatePartitionRequest.h"
#include "server/delivery/request/CreateTableRequest.h"
#include "server/delivery/request/DeleteByDateRequest.h"
#include "server/delivery/request/DescribeIndexRequest.h"
#include "server/delivery/request/DescribeTableRequest.h"
#include "server/delivery/request/DropIndexRequest.h"
#include "server/delivery/request/DropPartitionRequest.h"
#include "server/delivery/request/DropTableRequest.h"
#include "server/delivery/request/HasTableRequest.h"
#include "server/delivery/request/InsertRequest.h"
#include "server/delivery/request/PreloadTableRequest.h"
#include "server/delivery/request/SearchRequest.h"
#include "server/delivery/request/ShowPartitionsRequest.h"
#include "server/delivery/request/ShowTablesRequest.h"
#include "src/server/delivery/RequestHandler.h"
#include "utils/TimeRecorder.h"

#include <vector>

namespace milvus {
namespace server {
namespace grpc {

namespace {
::milvus::grpc::ErrorCode
ErrorMap(ErrorCode code) {
    static const std::map<ErrorCode, ::milvus::grpc::ErrorCode> code_map = {
        {SERVER_UNEXPECTED_ERROR, ::milvus::grpc::ErrorCode::UNEXPECTED_ERROR},
        {SERVER_UNSUPPORTED_ERROR, ::milvus::grpc::ErrorCode::UNEXPECTED_ERROR},
        {SERVER_NULL_POINTER, ::milvus::grpc::ErrorCode::UNEXPECTED_ERROR},
        {SERVER_INVALID_ARGUMENT, ::milvus::grpc::ErrorCode::ILLEGAL_ARGUMENT},
        {SERVER_FILE_NOT_FOUND, ::milvus::grpc::ErrorCode::FILE_NOT_FOUND},
        {SERVER_NOT_IMPLEMENT, ::milvus::grpc::ErrorCode::UNEXPECTED_ERROR},
        {SERVER_CANNOT_CREATE_FOLDER, ::milvus::grpc::ErrorCode::CANNOT_CREATE_FOLDER},
        {SERVER_CANNOT_CREATE_FILE, ::milvus::grpc::ErrorCode::CANNOT_CREATE_FILE},
        {SERVER_CANNOT_DELETE_FOLDER, ::milvus::grpc::ErrorCode::CANNOT_DELETE_FOLDER},
        {SERVER_CANNOT_DELETE_FILE, ::milvus::grpc::ErrorCode::CANNOT_DELETE_FILE},
        {SERVER_TABLE_NOT_EXIST, ::milvus::grpc::ErrorCode::TABLE_NOT_EXISTS},
        {SERVER_INVALID_TABLE_NAME, ::milvus::grpc::ErrorCode::ILLEGAL_TABLE_NAME},
        {SERVER_INVALID_TABLE_DIMENSION, ::milvus::grpc::ErrorCode::ILLEGAL_DIMENSION},
        {SERVER_INVALID_TIME_RANGE, ::milvus::grpc::ErrorCode::ILLEGAL_RANGE},
        {SERVER_INVALID_VECTOR_DIMENSION, ::milvus::grpc::ErrorCode::ILLEGAL_DIMENSION},

        {SERVER_INVALID_INDEX_TYPE, ::milvus::grpc::ErrorCode::ILLEGAL_INDEX_TYPE},
        {SERVER_INVALID_ROWRECORD, ::milvus::grpc::ErrorCode::ILLEGAL_ROWRECORD},
        {SERVER_INVALID_ROWRECORD_ARRAY, ::milvus::grpc::ErrorCode::ILLEGAL_ROWRECORD},
        {SERVER_INVALID_TOPK, ::milvus::grpc::ErrorCode::ILLEGAL_TOPK},
        {SERVER_INVALID_NPROBE, ::milvus::grpc::ErrorCode::ILLEGAL_ARGUMENT},
        {SERVER_INVALID_INDEX_NLIST, ::milvus::grpc::ErrorCode::ILLEGAL_NLIST},
        {SERVER_INVALID_INDEX_METRIC_TYPE, ::milvus::grpc::ErrorCode::ILLEGAL_METRIC_TYPE},
        {SERVER_INVALID_INDEX_FILE_SIZE, ::milvus::grpc::ErrorCode::ILLEGAL_ARGUMENT},
        {SERVER_ILLEGAL_VECTOR_ID, ::milvus::grpc::ErrorCode::ILLEGAL_VECTOR_ID},
        {SERVER_ILLEGAL_SEARCH_RESULT, ::milvus::grpc::ErrorCode::ILLEGAL_SEARCH_RESULT},
        {SERVER_CACHE_FULL, ::milvus::grpc::ErrorCode::CACHE_FAILED},
        {DB_META_TRANSACTION_FAILED, ::milvus::grpc::ErrorCode::META_FAILED},
        {SERVER_BUILD_INDEX_ERROR, ::milvus::grpc::ErrorCode::BUILD_INDEX_ERROR},
        {SERVER_OUT_OF_MEMORY, ::milvus::grpc::ErrorCode::OUT_OF_MEMORY},
    };

    if (code_map.find(code) != code_map.end()) {
        return code_map.at(code);
    } else {
        return ::milvus::grpc::ErrorCode::UNEXPECTED_ERROR;
    }
}
}  // namespace


::grpc::Status
GrpcRequestHandler::CreateTable(::grpc::ServerContext* context, const ::milvus::grpc::TableSchema* request,
                                ::milvus::grpc::Status* response) {
    Status status = RequestHandler::CreateTable(request->table_name(),
                                                request->dimension(),
                                                request->index_file_size(),
                                                request->metric_type());
    ConvertToProtoStatus(status, *response);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::HasTable(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                             ::milvus::grpc::BoolReply* response) {
    bool has_table = false;

    Status status = RequestHandler::HasTable(request->table_name(), has_table);
    ::milvus::grpc::Status grpc_status;
    ConvertToProtoStatus(status, grpc_status);
    response->set_bool_reply(has_table);
    response->set_allocated_status(&grpc_status);
//    response->mutable_status()->set_reason(grpc_status.reason());
//    response->mutable_status()->set_error_code(grpc_status.error_code());

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DropTable(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                              ::milvus::grpc::Status* response) {
    Status status = RequestHandler::DropTable(request->table_name());
    ConvertToProtoStatus(status, *response);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::CreateIndex(::grpc::ServerContext* context, const ::milvus::grpc::IndexParam* request,
                                ::milvus::grpc::Status* response) {
    Status status = RequestHandler::CreateIndex(request->table_name(),
                                                request->index().index_type(),
                                                request->index().nlist());
    ConvertToProtoStatus(status, *response);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::Insert(::grpc::ServerContext* context, const ::milvus::grpc::InsertParam* request,
                           ::milvus::grpc::VectorIds* response) {

    std::vector<std::vector<float>> record_array;
    for (size_t i = 0; i < request->row_record_array_size(); i++) {
        std::vector<float> record(request->row_record_array(i).vector_data().data(),
                                  request->row_record_array(i).vector_data().data()
                                  + request->row_record_array(i).vector_data_size());
        record_array.push_back(record);
    }

    std::vector<int64_t> id_array(request->row_id_array().data(),
                                  request->row_id_array().data() + request->row_record_array_size());
    std::vector<int64_t> id_out_array;
    Status status = RequestHandler::Insert(request->table_name(),
                                           record_array,
                                           id_array,
                                           request->partition_tag(),
                                           id_out_array);

    ConvertToProtoStatus(status, *response->mutable_status());
    for (auto& id : id_out_array) {
        response->add_vector_id_array(id);
    }

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::Search(::grpc::ServerContext* context, const ::milvus::grpc::SearchParam* request,
                           ::milvus::grpc::TopKQueryResult* response) {
    std::vector<std::string> file_id_array;
    BaseRequestPtr request_ptr = SearchRequest::Create(request, file_id_array, response);
    ::milvus::grpc::Status grpc_status;
    RequestScheduler::ExecRequest(request_ptr, &grpc_status);
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
    RequestScheduler::ExecRequest(request_ptr, &grpc_status);
    response->mutable_status()->set_error_code(grpc_status.error_code());
    response->mutable_status()->set_reason(grpc_status.reason());
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DescribeTable(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                                  ::milvus::grpc::TableSchema* response) {
    BaseRequestPtr request_ptr = DescribeTableRequest::Create(request->table_name(), response);
    ::milvus::grpc::Status grpc_status;
    RequestScheduler::ExecRequest(request_ptr, &grpc_status);
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
    RequestScheduler::ExecRequest(request_ptr, &grpc_status);
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
    RequestScheduler::ExecRequest(request_ptr, &grpc_status);
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
    RequestScheduler::ExecRequest(request_ptr, &grpc_status);
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
    RequestScheduler::ExecRequest(request_ptr, &grpc_status);
    response->set_error_code(grpc_status.error_code());
    response->set_reason(grpc_status.reason());
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::PreloadTable(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                                 ::milvus::grpc::Status* response) {
    BaseRequestPtr request_ptr = PreloadTableRequest::Create(request->table_name());
    ::milvus::grpc::Status grpc_status;
    RequestScheduler::ExecRequest(request_ptr, &grpc_status);
    response->set_reason(grpc_status.reason());
    response->set_error_code(grpc_status.error_code());
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DescribeIndex(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                                  ::milvus::grpc::IndexParam* response) {
    BaseRequestPtr request_ptr = DescribeIndexRequest::Create(request->table_name(), response);
    ::milvus::grpc::Status grpc_status;
    RequestScheduler::ExecRequest(request_ptr, &grpc_status);
    response->mutable_status()->set_reason(grpc_status.reason());
    response->mutable_status()->set_error_code(grpc_status.error_code());
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DropIndex(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                              ::milvus::grpc::Status* response) {
    BaseRequestPtr request_ptr = DropIndexRequest::Create(request->table_name());
    ::milvus::grpc::Status grpc_status;
    RequestScheduler::ExecRequest(request_ptr, &grpc_status);
    response->set_reason(grpc_status.reason());
    response->set_error_code(grpc_status.error_code());
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::CreatePartition(::grpc::ServerContext* context, const ::milvus::grpc::PartitionParam* request,
                                    ::milvus::grpc::Status* response) {
    BaseRequestPtr request_ptr = CreatePartitionRequest::Create(request);
    RequestScheduler::ExecRequest(request_ptr, response);
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::ShowPartitions(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                                   ::milvus::grpc::PartitionList* response) {
    BaseRequestPtr request_ptr = ShowPartitionsRequest::Create(request->table_name(), response);
    ::milvus::grpc::Status grpc_status;
    RequestScheduler::ExecRequest(request_ptr, &grpc_status);
    response->mutable_status()->set_reason(grpc_status.reason());
    response->mutable_status()->set_error_code(grpc_status.error_code());
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DropPartition(::grpc::ServerContext* context, const ::milvus::grpc::PartitionParam* request,
                                  ::milvus::grpc::Status* response) {
    BaseRequestPtr request_ptr = DropPartitionRequest::Create(request);
    ::milvus::grpc::Status grpc_status;
    RequestScheduler::ExecRequest(request_ptr, &grpc_status);
    response->set_reason(grpc_status.reason());
    response->set_error_code(grpc_status.error_code());
    return ::grpc::Status::OK;
}

void
GrpcRequestHandler::ConvertToProtoStatus(const Status& status, ::milvus::grpc::Status& grpc_status) {
    grpc_status.set_error_code(ErrorMap(status.code()));
    grpc_status.set_reason(status.message());
}

}  // namespace grpc
}  // namespace server
}  // namespace milvus
