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
    Status status = request_handler_.CreateTable(request->table_name(),
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

    Status status = request_handler_.HasTable(request->table_name(), has_table);
    ConvertToProtoStatus(status, *response->mutable_status());
    response->set_bool_reply(has_table);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DropTable(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                              ::milvus::grpc::Status* response) {
    Status status = request_handler_.DropTable(request->table_name());
    ConvertToProtoStatus(status, *response);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::CreateIndex(::grpc::ServerContext* context, const ::milvus::grpc::IndexParam* request,
                                ::milvus::grpc::Status* response) {
    Status status = request_handler_.CreateIndex(request->table_name(),
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
                                  request->row_id_array().data() + request->row_id_array_size());
    std::vector<int64_t> id_out_array;
    Status status = request_handler_.Insert(request->table_name(),
                                           record_array,
                                           id_array,
                                           request->partition_tag(),
                                           id_out_array);

    ConvertToProtoStatus(status, *response->mutable_status());

    response->mutable_vector_id_array()->Resize(static_cast<int>(id_out_array.size()), 0);
    memcpy(response->mutable_vector_id_array()->mutable_data(),
           id_out_array.data(),
           id_out_array.size() * sizeof(int64_t));

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::Search(::grpc::ServerContext* context, const ::milvus::grpc::SearchParam* request,
                           ::milvus::grpc::TopKQueryResult* response) {
    std::vector<std::vector<float>> record_array;
    for (size_t i = 0; i < request->query_record_array_size(); i++) {
        record_array.emplace_back(request->query_record_array(i).vector_data().begin(),
                                  request->query_record_array(i).vector_data().end());
        // TODO: May bug here, record is a local variable in for scope.
//        std::vector<float> record(request->query_record_array(i).vector_data().data(),
//                                  request->query_record_array(i).vector_data().data()
//                                  + request->query_record_array(i).vector_data_size());
//        record_array.push_back(record);
    }
    std::vector<std::pair<std::string, std::string>> ranges;
    for (auto& range : request->query_range_array()) {
        ranges.emplace_back(range.start_value(), range.end_value());
    }

     std::vector<std::string> partitions;
    for (auto& partition: request->partition_tag_array()) {
        partitions.emplace_back(partition);
    }

    std::vector<std::string> file_ids;
    TopKQueryResult result;

    Status status = request_handler_.Search(request->table_name(),
                                           record_array,
                                           ranges,
                                           request->topk(),
                                           request->nprobe(),
                                           partitions,
                                           file_ids,
                                           result);

    // construct result
    ConvertToProtoStatus(status, *response->mutable_status());
    response->set_row_num(result.row_num_);

    response->mutable_ids()->Resize(static_cast<int>(result.id_list_.size()), 0);
    memcpy(response->mutable_ids()->mutable_data(),
           result.id_list_.data(),
           result.id_list_.size() * sizeof(int64_t));

    response->mutable_distances()->Resize(static_cast<int>(result.distance_list_.size()), 0.0);
    memcpy(response->mutable_distances()->mutable_data(),
           result.distance_list_.data(),
           result.distance_list_.size() * sizeof(float));

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::SearchInFiles(::grpc::ServerContext* context, const ::milvus::grpc::SearchInFilesParam* request,
                                  ::milvus::grpc::TopKQueryResult* response) {

    std::vector<std::string> file_ids;
    for (auto& file_id : request->file_id_array()) {
        file_ids.emplace_back(file_id);
    }

    auto* search_request = &request->search_param();
    std::vector<std::vector<float>> record_array;
    for (size_t i = 0; i < search_request->query_record_array_size(); i++) {
        record_array.emplace_back(search_request->query_record_array(i).vector_data().begin(),
                                  search_request->query_record_array(i).vector_data().end());
    }
    std::vector<std::pair<std::string, std::string>> ranges;
    for (auto& range : search_request->query_range_array()) {
        ranges.emplace_back(range.start_value(), range.end_value());
    }

    std::vector<std::string> partitions;
    for (auto& partition: search_request->partition_tag_array()) {
        partitions.emplace_back(partition);
    }

    TopKQueryResult result;

    Status status = request_handler_.Search(search_request->table_name(),
                                           record_array,
                                           ranges,
                                           search_request->topk(),
                                           search_request->nprobe(),
                                           partitions,
                                           file_ids,
                                           result);

    // construct result
    ConvertToProtoStatus(status, *response->mutable_status());
    response->set_row_num(result.row_num_);

    response->mutable_ids()->Resize(static_cast<int>(result.id_list_.size()), 0);
    memcpy(response->mutable_ids()->mutable_data(),
           result.id_list_.data(),
           result.id_list_.size() * sizeof(int64_t));

    response->mutable_distances()->Resize(static_cast<int>(result.distance_list_.size()), 0.0);
    memcpy(response->mutable_distances()->mutable_data(),
           result.distance_list_.data(),
           result.distance_list_.size() * sizeof(float));

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DescribeTable(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                                  ::milvus::grpc::TableSchema* response) {
    TableSchema table_schema;
    Status status = request_handler_.DescribeTable(request->table_name(), table_schema);
    ConvertToProtoStatus(status, *response->mutable_status());
    response->set_table_name(table_schema.table_name_);
    response->set_dimension(table_schema.dimension_);
    response->set_index_file_size(table_schema.index_file_size_);
    response->set_metric_type(table_schema.metric_type_);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::CountTable(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                               ::milvus::grpc::TableRowCount* response) {
    int64_t row_count = 0;
    Status status = request_handler_.CountTable(request->table_name(), row_count);
    ConvertToProtoStatus(status, *response->mutable_status());
    response->set_table_row_count(row_count);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::ShowTables(::grpc::ServerContext* context, const ::milvus::grpc::Command* request,
                               ::milvus::grpc::TableNameList* response) {
    std::vector<std::string> tables;
    Status status = request_handler_.ShowTables(tables);
    ConvertToProtoStatus(status, *response->mutable_status());
    // TODO: Do not invoke set_allocated_*() here, may cause SIGSEGV
    // response->set_allocated_status(&grpc_status);
    for (auto& table: tables) {
        response->add_table_names(table);
    }

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::Cmd(::grpc::ServerContext* context, const ::milvus::grpc::Command* request,
                        ::milvus::grpc::StringReply* response) {
    std::string reply;
    Status status = request_handler_.Cmd(request->cmd(), reply);
    ConvertToProtoStatus(status, *response->mutable_status());
    response->set_string_reply(reply);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DeleteByDate(::grpc::ServerContext* context, const ::milvus::grpc::DeleteByDateParam* request,
                                 ::milvus::grpc::Status* response) {
    Range range(request->range().start_value(), request->range().end_value());
    Status status = request_handler_.DeleteByRange(request->table_name(), range);
    ConvertToProtoStatus(status, *response);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::PreloadTable(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                                 ::milvus::grpc::Status* response) {
    Status status = request_handler_.PreloadTable(request->table_name());
    ConvertToProtoStatus(status, *response);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DescribeIndex(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                                  ::milvus::grpc::IndexParam* response) {
    IndexParam param;
    Status status = request_handler_.DescribeIndex(request->table_name(), param);
    ConvertToProtoStatus(status, *response->mutable_status());
    response->set_table_name(param.table_name_);
    response->mutable_index()->set_index_type(param.index_type_);
    response->mutable_index()->set_nlist(param.nlist_);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DropIndex(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                              ::milvus::grpc::Status* response) {
    Status status = request_handler_.DropIndex(request->table_name());
    ConvertToProtoStatus(status, *response);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::CreatePartition(::grpc::ServerContext* context, const ::milvus::grpc::PartitionParam* request,
                                    ::milvus::grpc::Status* response) {
    Status status = request_handler_.CreatePartition(request->table_name(), request->partition_name(), request->tag());
    ConvertToProtoStatus(status, *response);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::ShowPartitions(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                                   ::milvus::grpc::PartitionList* response) {
    std::vector<PartitionParam> partitions;
    Status status = request_handler_.ShowPartitions(request->table_name(), partitions);
    ConvertToProtoStatus(status, *response->mutable_status());
    for (auto& partition : partitions) {
        milvus::grpc::PartitionParam* param = response->add_partition_array();
        param->set_table_name(partition.table_name_);
        param->set_partition_name(partition.partition_name_);
        param->set_tag(partition.tag_);
    }

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DropPartition(::grpc::ServerContext* context, const ::milvus::grpc::PartitionParam* request,
                                  ::milvus::grpc::Status* response) {
    Status status = request_handler_.DropPartition(request->table_name(), request->partition_name(), request->tag());
    ConvertToProtoStatus(status, *response);

    return ::grpc::Status::OK;
}

void
GrpcRequestHandler::ConvertToProtoStatus(const Status& status, ::milvus::grpc::Status& grpc_status) {
    if (!status.ok()) {
        grpc_status.set_error_code(ErrorMap(status.code()));
        grpc_status.set_reason(status.message());
    }
}

}  // namespace grpc
}  // namespace server
}  // namespace milvus
