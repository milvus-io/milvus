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

#include <memory>
#include <unordered_map>
#include <vector>

#include "server/Config.h"
#include "server/grpc_impl/GrpcRequestHandler.h"
#include "tracing/TextMapCarrier.h"
#include "tracing/TracerUtil.h"
#include "utils/TimeRecorder.h"

namespace milvus {
namespace server {
namespace grpc {

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
    CHECK_NULLPTR_RETURN(request);

    Status status = request_handler_.CreateTable(context_map_[context], request->table_name(), request->dimension(),
                                                 request->index_file_size(), request->metric_type());
    SET_RESPONSE(response, status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::HasTable(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                             ::milvus::grpc::BoolReply* response) {
    CHECK_NULLPTR_RETURN(request);

    bool has_table = false;

    Status status = request_handler_.HasTable(context_map_[context], request->table_name(), has_table);
    response->set_bool_reply(has_table);
    SET_RESPONSE(response->mutable_status(), status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DropTable(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                              ::milvus::grpc::Status* response) {
    CHECK_NULLPTR_RETURN(request);

    Status status = request_handler_.DropTable(context_map_[context], request->table_name());

    SET_RESPONSE(response, status, context);
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::CreateIndex(::grpc::ServerContext* context, const ::milvus::grpc::IndexParam* request,
                                ::milvus::grpc::Status* response) {
    CHECK_NULLPTR_RETURN(request);

    Status status = request_handler_.CreateIndex(context_map_[context], request->table_name(),
                                                 request->index().index_type(), request->index().nlist());

    SET_RESPONSE(response, status, context);
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::Insert(::grpc::ServerContext* context, const ::milvus::grpc::InsertParam* request,
                           ::milvus::grpc::VectorIds* response) {
    CHECK_NULLPTR_RETURN(request);

    int64_t record_data_size = 0;
    for (auto& record : request->row_record_array()) {
        record_data_size += record.vector_data_size();
    }

    std::vector<float> record_array(record_data_size, 0.0f);
    int64_t offset = 0;
    for (auto& record : request->row_record_array()) {
        memcpy(&record_array[offset], record.vector_data().data(), record.vector_data_size() * sizeof(float));
        offset += record.vector_data_size();
    }

    std::vector<int64_t> id_array;
    if (request->row_id_array_size() > 0) {
        id_array.resize(request->row_id_array_size());
        memcpy(id_array.data(), request->row_id_array().data(), request->row_id_array_size() * sizeof(int64_t));
    }

    Status status =
        request_handler_.Insert(context_map_[context], request->table_name(), request->row_record_array_size(),
                                record_array, request->partition_tag(), id_array);

    response->mutable_vector_id_array()->Resize(static_cast<int>(id_array.size()), 0);
    memcpy(response->mutable_vector_id_array()->mutable_data(), id_array.data(), id_array.size() * sizeof(int64_t));

    SET_RESPONSE(response->mutable_status(), status, context);
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::Search(::grpc::ServerContext* context, const ::milvus::grpc::SearchParam* request,
                           ::milvus::grpc::TopKQueryResult* response) {
    CHECK_NULLPTR_RETURN(request);

    int64_t record_data_size = 0;
    for (auto& record : request->query_record_array()) {
        record_data_size += record.vector_data_size();
    }

    std::vector<float> record_array(record_data_size);
    int64_t offset = 0;
    for (auto& record : request->query_record_array()) {
        memcpy(&record_array[offset], record.vector_data().data(), record.vector_data_size() * sizeof(float));
        offset += record.vector_data_size();
    }

    std::vector<Range> ranges;
    for (auto& range : request->query_range_array()) {
        ranges.emplace_back(range.start_value(), range.end_value());
    }

    std::vector<std::string> partitions;
    for (auto& partition : request->partition_tag_array()) {
        partitions.emplace_back(partition);
    }

    std::vector<std::string> file_ids;
    TopKQueryResult result;

    Status status =
        request_handler_.Search(context_map_[context], request->table_name(), request->query_record_array_size(),
                                record_array, ranges, request->topk(), request->nprobe(), partitions, file_ids, result);

    // construct result
    response->set_row_num(result.row_num_);

    response->mutable_ids()->Resize(static_cast<int>(result.id_list_.size()), 0);
    memcpy(response->mutable_ids()->mutable_data(), result.id_list_.data(), result.id_list_.size() * sizeof(int64_t));

    response->mutable_distances()->Resize(static_cast<int>(result.distance_list_.size()), 0.0);
    memcpy(response->mutable_distances()->mutable_data(), result.distance_list_.data(),
           result.distance_list_.size() * sizeof(float));

    SET_RESPONSE(response->mutable_status(), status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::SearchInFiles(::grpc::ServerContext* context, const ::milvus::grpc::SearchInFilesParam* request,
                                  ::milvus::grpc::TopKQueryResult* response) {
    CHECK_NULLPTR_RETURN(request);

    std::vector<std::string> file_ids;
    for (auto& file_id : request->file_id_array()) {
        file_ids.emplace_back(file_id);
    }

    auto* search_request = &request->search_param();

    int64_t record_data_size = 0;
    for (auto& record : search_request->query_record_array()) {
        record_data_size += record.vector_data_size();
    }

    std::vector<float> record_array(record_data_size);
    int64_t offset = 0;
    for (auto& record : search_request->query_record_array()) {
        memcpy(&record_array[offset], record.vector_data().data(), record.vector_data_size() * sizeof(float));
        offset += record.vector_data_size();
    }

    std::vector<Range> ranges;
    for (auto& range : search_request->query_range_array()) {
        ranges.emplace_back(range.start_value(), range.end_value());
    }

    std::vector<std::string> partitions;
    for (auto& partition : search_request->partition_tag_array()) {
        partitions.emplace_back(partition);
    }

    TopKQueryResult result;

    Status status = request_handler_.Search(
        context_map_[context], search_request->table_name(), search_request->query_record_array_size(), record_array,
        ranges, search_request->topk(), search_request->nprobe(), partitions, file_ids, result);

    // construct result
    response->set_row_num(result.row_num_);

    response->mutable_ids()->Resize(static_cast<int>(result.id_list_.size()), 0);
    memcpy(response->mutable_ids()->mutable_data(), result.id_list_.data(), result.id_list_.size() * sizeof(int64_t));

    response->mutable_distances()->Resize(static_cast<int>(result.distance_list_.size()), 0.0);
    memcpy(response->mutable_distances()->mutable_data(), result.distance_list_.data(),
           result.distance_list_.size() * sizeof(float));
    SET_RESPONSE(response->mutable_status(), status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DescribeTable(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                                  ::milvus::grpc::TableSchema* response) {
    CHECK_NULLPTR_RETURN(request);

    TableSchema table_schema;
    Status status = request_handler_.DescribeTable(context_map_[context], request->table_name(), table_schema);
    response->set_table_name(table_schema.table_name_);
    response->set_dimension(table_schema.dimension_);
    response->set_index_file_size(table_schema.index_file_size_);
    response->set_metric_type(table_schema.metric_type_);

    SET_RESPONSE(response->mutable_status(), status, context);
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::CountTable(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                               ::milvus::grpc::TableRowCount* response) {
    CHECK_NULLPTR_RETURN(request);

    int64_t row_count = 0;
    Status status = request_handler_.CountTable(context_map_[context], request->table_name(), row_count);
    response->set_table_row_count(row_count);
    SET_RESPONSE(response->mutable_status(), status, context);
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::ShowTables(::grpc::ServerContext* context, const ::milvus::grpc::Command* request,
                               ::milvus::grpc::TableNameList* response) {
    CHECK_NULLPTR_RETURN(request);

    std::vector<std::string> tables;
    Status status = request_handler_.ShowTables(context_map_[context], tables);
    for (auto& table : tables) {
        response->add_table_names(table);
    }
    SET_RESPONSE(response->mutable_status(), status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::Cmd(::grpc::ServerContext* context, const ::milvus::grpc::Command* request,
                        ::milvus::grpc::StringReply* response) {
    CHECK_NULLPTR_RETURN(request);

    std::string reply;
    Status status = request_handler_.Cmd(context_map_[context], request->cmd(), reply);
    response->set_string_reply(reply);
    SET_RESPONSE(response->mutable_status(), status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DeleteByDate(::grpc::ServerContext* context, const ::milvus::grpc::DeleteByDateParam* request,
                                 ::milvus::grpc::Status* response) {
    CHECK_NULLPTR_RETURN(request);

    Range range(request->range().start_value(), request->range().end_value());
    Status status = request_handler_.DeleteByRange(context_map_[context], request->table_name(), range);
    SET_RESPONSE(response, status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::PreloadTable(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                                 ::milvus::grpc::Status* response) {
    CHECK_NULLPTR_RETURN(request);

    Status status = request_handler_.PreloadTable(context_map_[context], request->table_name());
    SET_RESPONSE(response, status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DescribeIndex(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                                  ::milvus::grpc::IndexParam* response) {
    CHECK_NULLPTR_RETURN(request);

    IndexParam param;
    Status status = request_handler_.DescribeIndex(context_map_[context], request->table_name(), param);
    response->set_table_name(param.table_name_);
    response->mutable_index()->set_index_type(param.index_type_);
    response->mutable_index()->set_nlist(param.nlist_);
    SET_RESPONSE(response->mutable_status(), status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DropIndex(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                              ::milvus::grpc::Status* response) {
    CHECK_NULLPTR_RETURN(request);

    Status status = request_handler_.DropIndex(context_map_[context], request->table_name());
    SET_RESPONSE(response, status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::CreatePartition(::grpc::ServerContext* context, const ::milvus::grpc::PartitionParam* request,
                                    ::milvus::grpc::Status* response) {
    CHECK_NULLPTR_RETURN(request);

    Status status = request_handler_.CreatePartition(context_map_[context], request->table_name(),
                                                     request->partition_name(), request->tag());
    SET_RESPONSE(response, status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::ShowPartitions(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                                   ::milvus::grpc::PartitionList* response) {
    CHECK_NULLPTR_RETURN(request);

    std::vector<PartitionParam> partitions;
    Status status = request_handler_.ShowPartitions(context_map_[context], request->table_name(), partitions);
    for (auto& partition : partitions) {
        milvus::grpc::PartitionParam* param = response->add_partition_array();
        param->set_table_name(partition.table_name_);
        param->set_partition_name(partition.partition_name_);
        param->set_tag(partition.tag_);
    }

    SET_RESPONSE(response->mutable_status(), status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DropPartition(::grpc::ServerContext* context, const ::milvus::grpc::PartitionParam* request,
                                  ::milvus::grpc::Status* response) {
    CHECK_NULLPTR_RETURN(request);

    Status status = request_handler_.DropPartition(context_map_[context], request->table_name(),
                                                   request->partition_name(), request->tag());
    SET_RESPONSE(response, status, context);

    return ::grpc::Status::OK;
}

}  // namespace grpc
}  // namespace server
}  // namespace milvus
