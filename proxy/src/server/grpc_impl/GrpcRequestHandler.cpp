// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include "server/grpc_impl/GrpcRequestHandler.h"

#include <algorithm>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "query/BinaryQuery.h"
#include "server/ValidationUtil.h"
#include "server/context/ConnectionContext.h"
#include "tracing/TextMapCarrier.h"
#include "tracing/TracerUtil.h"
#include "utils/Log.h"

namespace milvus {
namespace server {
namespace grpc {

const char* EXTRA_PARAM_KEY = "params";
const size_t MAXIMUM_FIELD_NUM = 64;

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
        {SERVER_COLLECTION_NOT_EXIST, ::milvus::grpc::ErrorCode::COLLECTION_NOT_EXISTS},
        {SERVER_INVALID_COLLECTION_NAME, ::milvus::grpc::ErrorCode::ILLEGAL_COLLECTION_NAME},
        {SERVER_INVALID_COLLECTION_DIMENSION, ::milvus::grpc::ErrorCode::ILLEGAL_DIMENSION},
        {SERVER_INVALID_VECTOR_DIMENSION, ::milvus::grpc::ErrorCode::ILLEGAL_DIMENSION},
        {SERVER_INVALID_FIELD_NAME, ::milvus::grpc::ErrorCode::ILLEGAL_ARGUMENT},
        {SERVER_INVALID_FIELD_NUM, ::milvus::grpc::ErrorCode::ILLEGAL_ARGUMENT},

        {SERVER_INVALID_INDEX_TYPE, ::milvus::grpc::ErrorCode::ILLEGAL_INDEX_TYPE},
        {SERVER_INVALID_ROWRECORD, ::milvus::grpc::ErrorCode::ILLEGAL_ROWRECORD},
        {SERVER_INVALID_ROWRECORD_ARRAY, ::milvus::grpc::ErrorCode::ILLEGAL_ROWRECORD},
        {SERVER_INVALID_TOPK, ::milvus::grpc::ErrorCode::ILLEGAL_TOPK},
        {SERVER_INVALID_NPROBE, ::milvus::grpc::ErrorCode::ILLEGAL_ARGUMENT},
        {SERVER_INVALID_INDEX_NLIST, ::milvus::grpc::ErrorCode::ILLEGAL_NLIST},
        {SERVER_INVALID_INDEX_METRIC_TYPE, ::milvus::grpc::ErrorCode::ILLEGAL_METRIC_TYPE},
        {SERVER_INVALID_SEGMENT_ROW_COUNT, ::milvus::grpc::ErrorCode::ILLEGAL_ARGUMENT},
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

std::string
RequestMap(ReqType req_type) {
    static const std::unordered_map<ReqType, std::string> req_map = {
        {ReqType::kInsert, "Insert"}, {ReqType::kCreateIndex, "CreateIndex"},     {ReqType::kSearch, "Search"},
        {ReqType::kFlush, "Flush"},   {ReqType::kGetEntityByID, "GetEntityByID"}, {ReqType::kCompact, "Compact"},
    };

    if (req_map.find(req_type) != req_map.end()) {
        return req_map.at(req_type);
    } else {
        return "OtherReq";
    }
}

namespace {
void
CopyVectorData(const google::protobuf::RepeatedPtrField<::milvus::grpc::VectorRowRecord>& grpc_records,
               std::vector<uint8_t>& vectors_data) {
    // calculate buffer size
    int64_t float_data_size = 0, binary_data_size = 0;
    for (auto& record : grpc_records) {
        float_data_size += record.float_data_size();
        binary_data_size += record.binary_data().size();
    }

    int64_t data_size = binary_data_size;
    if (float_data_size > 0) {
        data_size = float_data_size * sizeof(float);
    }

    // copy vector data
    vectors_data.resize(data_size);
    int64_t offset = 0;
    if (float_data_size > 0) {
        for (auto& record : grpc_records) {
            int64_t single_size = record.float_data_size() * sizeof(float);
            memcpy(&vectors_data[offset], record.float_data().data(), single_size);
            offset += single_size;
        }
    } else if (binary_data_size > 0) {
        for (auto& record : grpc_records) {
            int64_t single_size = record.binary_data().size();
            memcpy(&vectors_data[offset], record.binary_data().data(), single_size);
            offset += single_size;
        }
    }
}

void
DeSerialization(const ::milvus::grpc::GeneralQuery& general_query, query::BooleanQueryPtr& boolean_clause,
                query::QueryPtr& query_ptr) {
}

void
ConstructResults(const TopKQueryResult& result, ::milvus::grpc::QueryResult* response) {
    if (!response) {
        return;
    }

    response->set_row_num(result.row_num_);

    response->mutable_entities()->mutable_ids()->Resize(static_cast<int>(result.id_list_.size()), 0);
    memcpy(response->mutable_entities()->mutable_ids()->mutable_data(), result.id_list_.data(),
           result.id_list_.size() * sizeof(int64_t));

    response->mutable_distances()->Resize(static_cast<int>(result.distance_list_.size()), 0.0);
    memcpy(response->mutable_distances()->mutable_data(), result.distance_list_.data(),
           result.distance_list_.size() * sizeof(float));
}

class GrpcConnectionContext : public milvus::server::ConnectionContext {
 public:
    explicit GrpcConnectionContext(::grpc::ServerContext* context) : context_(context) {
    }

    bool
    IsConnectionBroken() const override {
        if (context_ == nullptr) {
            return true;
        }

        return context_->IsCancelled();
    }

 private:
    ::grpc::ServerContext* context_ = nullptr;
};

}  // namespace

namespace {

#define REQ_ID ("request_id")

std::atomic<int64_t> _sequential_id;

int64_t
get_sequential_id() {
    return _sequential_id++;
}

void
set_request_id(::grpc::ServerContext* context, const std::string& request_id) {
    if (not context) {
        // error
        LOG_SERVER_ERROR_ << "set_request_id: grpc::ServerContext is nullptr" << std::endl;
        return;
    }

    context->AddInitialMetadata(REQ_ID, request_id);
}

std::string
get_request_id(::grpc::ServerContext* context) {
    if (not context) {
        // error
        LOG_SERVER_ERROR_ << "get_request_id: grpc::ServerContext is nullptr" << std::endl;
        return "INVALID_ID";
    }

    auto server_metadata = context->server_metadata();

    auto request_id_kv = server_metadata.find(REQ_ID);
    if (request_id_kv == server_metadata.end()) {
        // error
        LOG_SERVER_ERROR_ << std::string(REQ_ID) << " not found in grpc.server_metadata" << std::endl;
        return "INVALID_ID";
    }

    return request_id_kv->second.data();
}

}  // namespace

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

    // if client provide request_id in metadata, milvus just use it,
    // else milvus generate a sequential id.
    std::string request_id;
    auto request_id_kv = client_metadata.find("request_id");
    if (request_id_kv != client_metadata.end()) {
        request_id = request_id_kv->second.data();
        LOG_SERVER_DEBUG_ << "client provide request_id: " << request_id;

        // if request_id is being used by another request,
        // convert it to request_id_n.
        std::lock_guard<std::mutex> lock(context_map_mutex_);
        if (context_map_.find(request_id) == context_map_.end()) {
            // if not found exist, mark
            context_map_[request_id] = nullptr;
        } else {
            // Finding a unused suffix
            int64_t suffix = 1;
            std::string try_request_id;
            bool exist = true;
            do {
                try_request_id = request_id + "_" + std::to_string(suffix);
                exist = context_map_.find(try_request_id) != context_map_.end();
                suffix++;
            } while (exist);
            context_map_[try_request_id] = nullptr;
        }
    } else {
        request_id = std::to_string(get_sequential_id());
        set_request_id(server_context, request_id);
        LOG_SERVER_DEBUG_ << "milvus generate request_id: " << request_id;
    }

    auto trace_context = std::make_shared<tracing::TraceContext>(span);
    auto context = std::make_shared<Context>(request_id);
    context->SetTraceContext(trace_context);
    SetContext(server_rpc_info->server_context(), context);
}

void
GrpcRequestHandler::OnPreSendMessage(::grpc::experimental::ServerRpcInfo* server_rpc_info,
                                     ::grpc::experimental::InterceptorBatchMethods* interceptor_batch_methods) {
    std::lock_guard<std::mutex> lock(context_map_mutex_);
    auto request_id = get_request_id(server_rpc_info->server_context());

    if (context_map_.find(request_id) == context_map_.end()) {
        // error
        LOG_SERVER_ERROR_ << "request_id " << request_id << " not found in context_map_";
        return;
    }
    context_map_[request_id]->GetTraceContext()->GetSpan()->Finish();
    context_map_.erase(request_id);
}

std::shared_ptr<Context>
GrpcRequestHandler::GetContext(::grpc::ServerContext* server_context) {
    std::lock_guard<std::mutex> lock(context_map_mutex_);
    auto request_id = get_request_id(server_context);

    auto iter = context_map_.find(request_id);
    if (iter == context_map_.end()) {
        LOG_SERVER_ERROR_ << "GetContext: request_id " << request_id << " not found in context_map_";
        return nullptr;
    }

    if (iter->second != nullptr) {
        ConnectionContextPtr connection_context = std::make_shared<GrpcConnectionContext>(server_context);
        iter->second->SetConnectionContext(connection_context);
    }
    return iter->second;
}

void
GrpcRequestHandler::SetContext(::grpc::ServerContext* server_context, const std::shared_ptr<Context>& context) {
    std::lock_guard<std::mutex> lock(context_map_mutex_);
    auto request_id = get_request_id(server_context);
    context_map_[request_id] = context;
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
GrpcRequestHandler::CreateCollection(::grpc::ServerContext* context, const ::milvus::grpc::Mapping* request,
                                     ::milvus::grpc::Status* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->ReqID().c_str(), __func__);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::HasCollection(::grpc::ServerContext* context, const ::milvus::grpc::CollectionName* request,
                                  ::milvus::grpc::BoolReply* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->ReqID().c_str(), __func__);

    bool has_collection = false;

    Status status = req_handler_.HasCollection(GetContext(context), request->collection_name(), has_collection);
    response->set_bool_reply(has_collection);

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->ReqID().c_str(), __func__);
    SET_RESPONSE(response->mutable_status(), status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DropCollection(::grpc::ServerContext* context, const ::milvus::grpc::CollectionName* request,
                                   ::milvus::grpc::Status* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->ReqID().c_str(), __func__);

    Status status = req_handler_.DropCollection(GetContext(context), request->collection_name());

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->ReqID().c_str(), __func__);
    SET_RESPONSE(response, status, context);
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::CreateIndex(::grpc::ServerContext* context, const ::milvus::grpc::IndexParam* request,
                                ::milvus::grpc::Status* response) {
    CHECK_NULLPTR_RETURN(request)
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->ReqID().c_str(), __func__);

    milvus::json json_params;
    for (int i = 0; i < request->extra_params_size(); i++) {
        const ::milvus::grpc::KeyValuePair& extra = request->extra_params(i);
        if (extra.key() == EXTRA_PARAM_KEY) {
            json_params[EXTRA_PARAM_KEY] = json::parse(extra.value());
        } else {
            json_params[extra.key()] = extra.value();
        }
    }

    Status status = req_handler_.CreateIndex(GetContext(context), request->collection_name(), request->field_name(),
                                             request->index_name(), json_params);

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->ReqID().c_str(), __func__);
    SET_RESPONSE(response, status, context);
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DescribeIndex(::grpc::ServerContext* context, const ::milvus::grpc::IndexParam* request,
                                  ::milvus::grpc::IndexParam* response) {
    CHECK_NULLPTR_RETURN(request)
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->ReqID().c_str(), __func__);

    std::string index_name;
    milvus::json index_params;
    Status status = req_handler_.DescribeIndex(GetContext(context), request->collection_name(), request->field_name(),
                                               index_name, index_params);

    response->set_collection_name(request->collection_name());
    response->set_field_name(request->field_name());
    ::milvus::grpc::KeyValuePair* kv = response->add_extra_params();
    kv->set_key(EXTRA_PARAM_KEY);
    kv->set_value(index_params.dump());

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->ReqID().c_str(), __func__);
    SET_RESPONSE(response->mutable_status(), status, context);
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DropIndex(::grpc::ServerContext* context, const ::milvus::grpc::IndexParam* request,
                              ::milvus::grpc::Status* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->ReqID().c_str(), __func__);

    Status status = req_handler_.DropIndex(GetContext(context), request->collection_name(), request->field_name(),
                                           request->index_name());

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->ReqID().c_str(), __func__);
    SET_RESPONSE(response, status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::GetEntityByID(::grpc::ServerContext* context, const ::milvus::grpc::EntityIdentity* request,
                                  ::milvus::grpc::Entities* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->ReqID().c_str(), __func__);
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::GetEntityIDs(::grpc::ServerContext* context, const ::milvus::grpc::GetEntityIDsParam* request,
                                 ::milvus::grpc::EntityIds* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->ReqID().c_str(), __func__);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::SearchInSegment(::grpc::ServerContext* context, const ::milvus::grpc::SearchInSegmentParam* request,
                                    ::milvus::grpc::QueryResult* response) {

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DescribeCollection(::grpc::ServerContext* context, const ::milvus::grpc::CollectionName* request,
                                       ::milvus::grpc::Mapping* response) {
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->ReqID().c_str(), __func__);
    CHECK_NULLPTR_RETURN(request);
    try {
        milvus::server::CollectionSchema collection_schema;
        Status status =
            req_handler_.GetCollectionInfo(GetContext(context), request->collection_name(), collection_schema);
        if (!status.ok()) {
            SET_RESPONSE(response->mutable_status(), status, context);
            return ::grpc::Status::OK;
        }

        response->set_collection_name(request->collection_name());
        for (auto& field_kv : collection_schema.fields_) {
            auto field = response->add_fields();
            auto& field_name = field_kv.first;
            auto& field_schema = field_kv.second;

            field->set_name(field_name);
            field->set_type((milvus::grpc::DataType)field_schema.field_type_);

            auto grpc_field_param = field->add_extra_params();
            grpc_field_param->set_key(EXTRA_PARAM_KEY);
            grpc_field_param->set_value(field_schema.field_params_.dump());

            for (auto& item : field_schema.index_params_.items()) {
                auto grpc_index_param = field->add_index_params();
                grpc_index_param->set_key(item.key());
                if (item.value().is_object()) {
                    grpc_index_param->set_value(item.value().dump());
                } else {
                    grpc_index_param->set_value(item.value());
                }
            }
        }

        auto grpc_extra_param = response->add_extra_params();
        grpc_extra_param->set_key(EXTRA_PARAM_KEY);
        grpc_extra_param->set_value(collection_schema.extra_params_.dump());
        LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->ReqID().c_str(), __func__);
        SET_RESPONSE(response->mutable_status(), status, context);
    } catch (std::exception& ex) {
        Status status = Status{SERVER_UNEXPECTED_ERROR, "Parsing json string wrong"};
        SET_RESPONSE(response->mutable_status(), status, context);
    }
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::CountCollection(::grpc::ServerContext* context, const ::milvus::grpc::CollectionName* request,
                                    ::milvus::grpc::CollectionRowCount* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->ReqID().c_str(), __func__);

    int64_t row_count = 0;
    Status status = req_handler_.CountEntities(GetContext(context), request->collection_name(), row_count);
    response->set_collection_row_count(row_count);

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->ReqID().c_str(), __func__);
    SET_RESPONSE(response->mutable_status(), status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::ShowCollections(::grpc::ServerContext* context, const ::milvus::grpc::Command* request,
                                    ::milvus::grpc::CollectionNameList* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->ReqID().c_str(), __func__);

    std::vector<std::string> collections;
    Status status = req_handler_.ListCollections(GetContext(context), collections);
    for (auto& collection : collections) {
        response->add_collection_names(collection);
    }

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->ReqID().c_str(), __func__);
    SET_RESPONSE(response->mutable_status(), status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::ShowCollectionInfo(::grpc::ServerContext* context, const ::milvus::grpc::CollectionName* request,
                                       ::milvus::grpc::CollectionInfo* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->ReqID().c_str(), __func__);

    std::string collection_stats;
    Status status = req_handler_.GetCollectionStats(GetContext(context), request->collection_name(), collection_stats);
    response->set_json_info(collection_stats);

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->ReqID().c_str(), __func__);
    SET_RESPONSE(response->mutable_status(), status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::Cmd(::grpc::ServerContext* context, const ::milvus::grpc::Command* request,
                        ::milvus::grpc::StringReply* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->ReqID().c_str(), __func__);

    std::string reply;
    Status status;

    std::string cmd = request->cmd();
    std::vector<std::string> requests;
    if (cmd == "requests") {
        std::lock_guard<std::mutex> lock(context_map_mutex_);
        for (auto& iter : context_map_) {
            if (nullptr == iter.second) {
                continue;
            }
            if (iter.second->ReqID() == get_request_id(context)) {
                continue;
            }
            auto request_str = RequestMap(iter.second->GetReqType()) + "-" + iter.second->ReqID();
            requests.emplace_back(request_str);
        }
        nlohmann::json reply_json;
        reply_json["requests"] = requests;
        reply = reply_json.dump();
        response->set_string_reply(reply);
    } else {
        status = req_handler_.Cmd(GetContext(context), cmd, reply);
        response->set_string_reply(reply);
    }

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->ReqID().c_str(), __func__);
    SET_RESPONSE(response->mutable_status(), status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DeleteByID(::grpc::ServerContext* context, const ::milvus::grpc::DeleteByIDParam* request,
                               ::milvus::grpc::Status* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->ReqID().c_str(), __func__);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::PreloadCollection(::grpc::ServerContext* context, const ::milvus::grpc::CollectionName* request,
                                      ::milvus::grpc::Status* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->ReqID().c_str(), __func__);

    Status status = req_handler_.LoadCollection(GetContext(context), request->collection_name());

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->ReqID().c_str(), __func__);
    SET_RESPONSE(response, status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::CreatePartition(::grpc::ServerContext* context, const ::milvus::grpc::PartitionParam* request,
                                    ::milvus::grpc::Status* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->ReqID().c_str(), __func__);

    Status status = req_handler_.CreatePartition(GetContext(context), request->collection_name(), request->tag());

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->ReqID().c_str(), __func__);
    SET_RESPONSE(response, status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::HasPartition(::grpc::ServerContext* context, const ::milvus::grpc::PartitionParam* request,
                                 ::milvus::grpc::BoolReply* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->ReqID().c_str(), __func__);

    bool has_collection = false;

    Status status =
        req_handler_.HasPartition(GetContext(context), request->collection_name(), request->tag(), has_collection);
    response->set_bool_reply(has_collection);

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->ReqID().c_str(), __func__);
    SET_RESPONSE(response->mutable_status(), status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::ShowPartitions(::grpc::ServerContext* context, const ::milvus::grpc::CollectionName* request,
                                   ::milvus::grpc::PartitionList* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->ReqID().c_str(), __func__);

    std::vector<std::string> partition_names;
    Status status = req_handler_.ListPartitions(GetContext(context), request->collection_name(), partition_names);
    for (auto& pn : partition_names) {
        response->add_partition_tag_array(pn);
    }

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->ReqID().c_str(), __func__);
    SET_RESPONSE(response->mutable_status(), status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DropPartition(::grpc::ServerContext* context, const ::milvus::grpc::PartitionParam* request,
                                  ::milvus::grpc::Status* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->ReqID().c_str(), __func__);

    Status status = req_handler_.DropPartition(GetContext(context), request->collection_name(), request->tag());

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->ReqID().c_str(), __func__);
    SET_RESPONSE(response, status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::Flush(::grpc::ServerContext* context, const ::milvus::grpc::FlushParam* request,
                          ::milvus::grpc::Status* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->ReqID().c_str(), __func__);

    std::vector<std::string> collection_names;
    for (int32_t i = 0; i < request->collection_name_array().size(); i++) {
        collection_names.push_back(request->collection_name_array(i));
    }
    Status status = req_handler_.Flush(GetContext(context), collection_names);

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->ReqID().c_str(), __func__);
    SET_RESPONSE(response, status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::Compact(::grpc::ServerContext* context, const ::milvus::grpc::CompactParam* request,
                            ::milvus::grpc::Status* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->ReqID().c_str(), __func__);

    Status status = req_handler_.Compact(GetContext(context), request->collection_name(), request->threshold());

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->ReqID().c_str(), __func__);
    SET_RESPONSE(response, status, context);

    return ::grpc::Status::OK;
}

/*******************************************New Interface*********************************************/

::grpc::Status
GrpcRequestHandler::Insert(::grpc::ServerContext* context, const ::milvus::grpc::InsertParam* request,
                           ::milvus::grpc::EntityIds* response) {


    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->ReqID().c_str(), __func__);
    return ::grpc::Status::OK;

}

::grpc::Status
GrpcRequestHandler::SearchPB(::grpc::ServerContext* context, const ::milvus::grpc::SearchParamPB* request,
                             ::milvus::grpc::QueryResult* response) {
    CHECK_NULLPTR_RETURN(request);

    return ::grpc::Status::OK;
}

Status
GrpcRequestHandler::ProcessLeafQueryJson(const nlohmann::json& json, query::BooleanQueryPtr& query,
                                         std::string& field_name) {
    auto status = Status::OK();
    if (json.contains("term")) {
        auto leaf_query = std::make_shared<query::LeafQuery>();
        auto term_query = std::make_shared<query::TermQuery>();
        nlohmann::json json_obj = json["term"];
        JSON_NULL_CHECK(json_obj);
        JSON_OBJECT_CHECK(json_obj);
        term_query->json_obj = json_obj;
        nlohmann::json::iterator json_it = json_obj.begin();
        field_name = json_it.key();

        leaf_query->term_query = term_query;
        query->AddLeafQuery(leaf_query);
    } else if (json.contains("range")) {
        auto leaf_query = std::make_shared<query::LeafQuery>();
        auto range_query = std::make_shared<query::RangeQuery>();
        nlohmann::json json_obj = json["range"];
        JSON_NULL_CHECK(json_obj);
        JSON_OBJECT_CHECK(json_obj);
        range_query->json_obj = json_obj;
        nlohmann::json::iterator json_it = json_obj.begin();
        field_name = json_it.key();

        leaf_query->range_query = range_query;
        query->AddLeafQuery(leaf_query);
    } else if (json.contains("vector")) {
        auto leaf_query = std::make_shared<query::LeafQuery>();
        auto vector_json = json["vector"];
        JSON_NULL_CHECK(vector_json);

        leaf_query->vector_placeholder = vector_json.get<std::string>();
        query->AddLeafQuery(leaf_query);
    } else {
        return Status{SERVER_INVALID_ARGUMENT, "Leaf query get wrong key"};
    }
    return status;
}

Status
GrpcRequestHandler::ProcessBooleanQueryJson(const nlohmann::json& query_json, query::BooleanQueryPtr& boolean_query,
                                            query::QueryPtr& query_ptr) {
    auto status = Status::OK();
    if (query_json.empty()) {
        return Status{SERVER_INVALID_ARGUMENT, "BoolQuery is null"};
    }
    for (auto& el : query_json.items()) {
        if (el.key() == "must") {
            boolean_query->SetOccur(query::Occur::MUST);
            auto must_json = el.value();
            if (!must_json.is_array()) {
                std::string msg = "Must json string is not an array";
                return Status{SERVER_INVALID_DSL_PARAMETER, msg};
            }

            for (auto& json : must_json) {
                auto must_query = std::make_shared<query::BooleanQuery>();
                if (json.contains("must") || json.contains("should") || json.contains("must_not")) {
                    STATUS_CHECK(ProcessBooleanQueryJson(json, must_query, query_ptr));
                    boolean_query->AddBooleanQuery(must_query);
                } else {
                    std::string field_name;
                    STATUS_CHECK(ProcessLeafQueryJson(json, boolean_query, field_name));
                    if (!field_name.empty()) {
                        query_ptr->index_fields.insert(field_name);
                    }
                }
            }
        } else if (el.key() == "should") {
            boolean_query->SetOccur(query::Occur::SHOULD);
            auto should_json = el.value();
            if (!should_json.is_array()) {
                std::string msg = "Should json string is not an array";
                return Status{SERVER_INVALID_DSL_PARAMETER, msg};
            }

            for (auto& json : should_json) {
                auto should_query = std::make_shared<query::BooleanQuery>();
                if (json.contains("must") || json.contains("should") || json.contains("must_not")) {
                    STATUS_CHECK(ProcessBooleanQueryJson(json, should_query, query_ptr));
                    boolean_query->AddBooleanQuery(should_query);
                } else {
                    std::string field_name;
                    STATUS_CHECK(ProcessLeafQueryJson(json, boolean_query, field_name));
                    if (!field_name.empty()) {
                        query_ptr->index_fields.insert(field_name);
                    }
                }
            }
        } else if (el.key() == "must_not") {
            boolean_query->SetOccur(query::Occur::MUST_NOT);
            auto should_json = el.value();
            if (!should_json.is_array()) {
                std::string msg = "Must_not json string is not an array";
                return Status{SERVER_INVALID_DSL_PARAMETER, msg};
            }

            for (auto& json : should_json) {
                if (json.contains("must") || json.contains("should") || json.contains("must_not")) {
                    auto must_not_query = std::make_shared<query::BooleanQuery>();
                    STATUS_CHECK(ProcessBooleanQueryJson(json, must_not_query, query_ptr));
                    boolean_query->AddBooleanQuery(must_not_query);
                } else {
                    std::string field_name;
                    STATUS_CHECK(ProcessLeafQueryJson(json, boolean_query, field_name));
                    if (!field_name.empty()) {
                        query_ptr->index_fields.insert(field_name);
                    }
                }
            }
        } else {
            std::string msg = "BoolQuery json string does not include bool query";
            return Status{SERVER_INVALID_DSL_PARAMETER, msg};
        }
    }

    return status;
}

Status
GrpcRequestHandler::DeserializeJsonToBoolQuery(
    const google::protobuf::RepeatedPtrField<::milvus::grpc::VectorParam>& vector_params, const std::string& dsl_string,
    query::BooleanQueryPtr& boolean_query, query::QueryPtr& query_ptr) {
    return Status::OK();
}

::grpc::Status
GrpcRequestHandler::Search(::grpc::ServerContext* context, const ::milvus::grpc::SearchParam* request,
                           ::milvus::grpc::QueryResult* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->ReqID().c_str(), __func__);

    return ::grpc::Status::OK;
}

}  // namespace grpc
}  // namespace server
}  // namespace milvus
