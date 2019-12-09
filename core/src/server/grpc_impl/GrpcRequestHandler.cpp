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

#include <src/server/Config.h>

#include <memory>
#include <unordered_map>
#include <vector>

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
#include "tracing/TextMapCarrier.h"
#include "tracing/TracerUtil.h"
#include "utils/TimeRecorder.h"

namespace milvus {
namespace server {
namespace grpc {

GrpcRequestHandler::GrpcRequestHandler(const std::shared_ptr<opentracing::Tracer>& tracer)
    : tracer_(tracer), random_num_generator_() {
    std::random_device random_device;
    random_num_generator_.seed(random_device());
}

void
GrpcRequestHandler::OnPostRecvInitialMetaData(
    ::grpc::experimental::ServerRpcInfo* server_rpc_info,
    ::grpc::experimental::InterceptorBatchMethods* interceptor_batch_methods) {
    std::unordered_map<std::string, std::string> text_map;
    auto* metadata_map = interceptor_batch_methods->GetRecvInitialMetadata();
    auto context_kv = metadata_map->find(tracing::TracerUtil::GetTraceContextHeaderName());
    if (context_kv != metadata_map->end()) {
        text_map[std::string(context_kv->first.data(), context_kv->first.length())] =
            std::string(context_kv->second.data(), context_kv->second.length());
    }
    // test debug mode
    //    if (std::string(server_rpc_info->method()).find("Search") != std::string::npos) {
    //        text_map["demo-debug-id"] = "debug-id";
    //    }

    tracing::TextMapCarrier carrier{text_map};
    auto span_context_maybe = tracer_->Extract(carrier);
    if (!span_context_maybe) {
        std::cerr << span_context_maybe.error().message() << std::endl;
        return;
    }
    auto span = tracer_->StartSpan(server_rpc_info->method(), {opentracing::ChildOf(span_context_maybe->get())});
    auto server_context = server_rpc_info->server_context();
    auto client_metadata = server_context->client_metadata();
    // TODO: request id
    std::string request_id;
    auto request_id_kv = client_metadata.find("request_id");
    if (request_id_kv != client_metadata.end()) {
        request_id = request_id_kv->second.data();
    } else {
        request_id = std::to_string(random_id()) + std::to_string(random_id());
    }
    auto trace_context = std::make_shared<tracing::TraceContext>(span);
    auto context = std::make_shared<Context>(request_id);
    context->SetTraceContext(trace_context);
    context_map_[server_rpc_info->server_context()] = context;
}

void
GrpcRequestHandler::OnPreSendMessage(::grpc::experimental::ServerRpcInfo* server_rpc_info,
                                     ::grpc::experimental::InterceptorBatchMethods* interceptor_batch_methods) {
    context_map_[server_rpc_info->server_context()]->GetTraceContext()->GetSpan()->Finish();
    auto search = context_map_.find(server_rpc_info->server_context());
    if (search != context_map_.end()) {
        std::lock_guard<std::mutex> lock(context_map_mutex_);
        context_map_.erase(search);
    }
}

const std::shared_ptr<Context>&
GrpcRequestHandler::GetContext(::grpc::ServerContext* server_context) {
    return context_map_[server_context];
}

void
GrpcRequestHandler::SetContext(::grpc::ServerContext* server_context, const std::shared_ptr<Context>& context) {
    context_map_[server_context] = context;
}

uint64_t
GrpcRequestHandler::random_id() const {
    std::lock_guard<std::mutex> lock(random_mutex_);
    auto value = random_num_generator_();
    while (value == 0) {
        value = random_num_generator_();
    }
    return value;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

::grpc::Status
GrpcRequestHandler::CreateTable(::grpc::ServerContext* context, const ::milvus::grpc::TableSchema* request,
                                ::milvus::grpc::Status* response) {
    BaseRequestPtr request_ptr = CreateTableRequest::Create(context_map_[context], request);
    GrpcRequestScheduler::ExecRequest(request_ptr, response);
    SET_TRACING_TAG(*response, context);
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::HasTable(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                             ::milvus::grpc::BoolReply* response) {
    bool has_table = false;
    BaseRequestPtr request_ptr = HasTableRequest::Create(context_map_[context], request->table_name(), has_table);
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecRequest(request_ptr, &grpc_status);
    response->set_bool_reply(has_table);
    SET_RESPONSE(response, grpc_status, context);
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DropTable(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                              ::milvus::grpc::Status* response) {
    BaseRequestPtr request_ptr = DropTableRequest::Create(context_map_[context], request->table_name());
    GrpcRequestScheduler::ExecRequest(request_ptr, response);
    SET_TRACING_TAG(*response, context);
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::CreateIndex(::grpc::ServerContext* context, const ::milvus::grpc::IndexParam* request,
                                ::milvus::grpc::Status* response) {
    BaseRequestPtr request_ptr = CreateIndexRequest::Create(context_map_[context], request);
    GrpcRequestScheduler::ExecRequest(request_ptr, response);
    SET_TRACING_TAG(*response, context);
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::Insert(::grpc::ServerContext* context, const ::milvus::grpc::InsertParam* request,
                           ::milvus::grpc::VectorIds* response) {
    BaseRequestPtr request_ptr = InsertRequest::Create(context_map_[context], request, response);
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecRequest(request_ptr, &grpc_status);
    SET_RESPONSE(response, grpc_status, context);
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::Search(::grpc::ServerContext* context, const ::milvus::grpc::SearchParam* request,
                           ::milvus::grpc::TopKQueryResult* response) {
    std::vector<std::string> file_id_array;
    BaseRequestPtr request_ptr = SearchRequest::Create(context_map_[context], request, file_id_array, response);
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecRequest(request_ptr, &grpc_status);
    SET_RESPONSE(response, grpc_status, context);
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
        SearchRequest::Create(context_map_[context], request_mutable->mutable_search_param(), file_id_array, response);
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecRequest(request_ptr, &grpc_status);
    SET_RESPONSE(response, grpc_status, context);
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DescribeTable(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                                  ::milvus::grpc::TableSchema* response) {
    BaseRequestPtr request_ptr = DescribeTableRequest::Create(context_map_[context], request->table_name(), response);
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecRequest(request_ptr, &grpc_status);
    SET_RESPONSE(response, grpc_status, context);
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::CountTable(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                               ::milvus::grpc::TableRowCount* response) {
    int64_t row_count = 0;
    BaseRequestPtr request_ptr = CountTableRequest::Create(context_map_[context], request->table_name(), row_count);
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecRequest(request_ptr, &grpc_status);
    response->set_table_row_count(row_count);
    SET_RESPONSE(response, grpc_status, context);
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::ShowTables(::grpc::ServerContext* context, const ::milvus::grpc::Command* request,
                               ::milvus::grpc::TableNameList* response) {
    BaseRequestPtr request_ptr = ShowTablesRequest::Create(context_map_[context], response);
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecRequest(request_ptr, &grpc_status);
    SET_RESPONSE(response, grpc_status, context);
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::Cmd(::grpc::ServerContext* context, const ::milvus::grpc::Command* request,
                        ::milvus::grpc::StringReply* response) {
    std::string result;
    BaseRequestPtr request_ptr = CmdRequest::Create(context_map_[context], request->cmd(), result);
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecRequest(request_ptr, &grpc_status);
    response->set_string_reply(result);
    SET_RESPONSE(response, grpc_status, context);
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DeleteByDate(::grpc::ServerContext* context, const ::milvus::grpc::DeleteByDateParam* request,
                                 ::milvus::grpc::Status* response) {
    BaseRequestPtr request_ptr = DeleteByDateRequest::Create(context_map_[context], request);
    GrpcRequestScheduler::ExecRequest(request_ptr, response);
    SET_TRACING_TAG(*response, context);
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::PreloadTable(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                                 ::milvus::grpc::Status* response) {
    BaseRequestPtr request_ptr = PreloadTableRequest::Create(context_map_[context], request->table_name());
    GrpcRequestScheduler::ExecRequest(request_ptr, response);
    SET_TRACING_TAG(*response, context);
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DescribeIndex(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                                  ::milvus::grpc::IndexParam* response) {
    BaseRequestPtr request_ptr = DescribeIndexRequest::Create(context_map_[context], request->table_name(), response);
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecRequest(request_ptr, &grpc_status);
    SET_RESPONSE(response, grpc_status, context);
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DropIndex(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                              ::milvus::grpc::Status* response) {
    BaseRequestPtr request_ptr = DropIndexRequest::Create(context_map_[context], request->table_name());
    GrpcRequestScheduler::ExecRequest(request_ptr, response);
    SET_TRACING_TAG(*response, context);
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::CreatePartition(::grpc::ServerContext* context, const ::milvus::grpc::PartitionParam* request,
                                    ::milvus::grpc::Status* response) {
    BaseRequestPtr request_ptr = CreatePartitionRequest::Create(context_map_[context], request);
    GrpcRequestScheduler::ExecRequest(request_ptr, response);
    SET_TRACING_TAG(*response, context);
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::ShowPartitions(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                                   ::milvus::grpc::PartitionList* response) {
    BaseRequestPtr request_ptr = ShowPartitionsRequest::Create(context_map_[context], request->table_name(), response);
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecRequest(request_ptr, &grpc_status);
    SET_RESPONSE(response, grpc_status, context);
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DropPartition(::grpc::ServerContext* context, const ::milvus::grpc::PartitionParam* request,
                                  ::milvus::grpc::Status* response) {
    BaseRequestPtr request_ptr = DropPartitionRequest::Create(context_map_[context], request);
    GrpcRequestScheduler::ExecRequest(request_ptr, response);
    SET_TRACING_TAG(*response, context);
    return ::grpc::Status::OK;
}

}  // namespace grpc
}  // namespace server
}  // namespace milvus
