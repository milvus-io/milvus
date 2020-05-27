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

#include <fiu-local.h>
#include <algorithm>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "context/HybridSearchContext.h"
#include "query/BinaryQuery.h"
#include "server/context/ConnectionContext.h"
#include "tracing/TextMapCarrier.h"
#include "tracing/TracerUtil.h"
#include "utils/Log.h"
#include "utils/LogUtil.h"
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
        {SERVER_COLLECTION_NOT_EXIST, ::milvus::grpc::ErrorCode::COLLECTION_NOT_EXISTS},
        {SERVER_INVALID_COLLECTION_NAME, ::milvus::grpc::ErrorCode::ILLEGAL_COLLECTION_NAME},
        {SERVER_INVALID_COLLECTION_DIMENSION, ::milvus::grpc::ErrorCode::ILLEGAL_DIMENSION},
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

std::string
RequestMap(BaseRequest::RequestType request_type) {
    static const std::unordered_map<BaseRequest::RequestType, std::string> request_map = {
        {BaseRequest::kInsert, "Insert"},
        {BaseRequest::kCreateIndex, "CreateIndex"},
        {BaseRequest::kSearch, "Search"},
        {BaseRequest::kSearchByID, "SearchByID"},
        {BaseRequest::kHybridSearch, "HybridSearch"},
        {BaseRequest::kFlush, "Flush"},
        {BaseRequest::kCompact, "Compact"},
    };

    if (request_map.find(request_type) != request_map.end()) {
        return request_map.at(request_type);
    } else {
        return "OtherRequest";
    }
}

namespace {
void
CopyRowRecords(const google::protobuf::RepeatedPtrField<::milvus::grpc::RowRecord>& grpc_records,
               const google::protobuf::RepeatedField<google::protobuf::int64>& grpc_id_array,
               engine::VectorsData& vectors) {
    // step 1: copy vector data
    int64_t float_data_size = 0, binary_data_size = 0;
    for (auto& record : grpc_records) {
        float_data_size += record.float_data_size();
        binary_data_size += record.binary_data().size();
    }

    std::vector<float> float_array(float_data_size, 0.0f);
    std::vector<uint8_t> binary_array(binary_data_size, 0);
    int64_t float_offset = 0, binary_offset = 0;
    if (float_data_size > 0) {
        for (auto& record : grpc_records) {
            memcpy(&float_array[float_offset], record.float_data().data(), record.float_data_size() * sizeof(float));
            float_offset += record.float_data_size();
        }
    } else if (binary_data_size > 0) {
        for (auto& record : grpc_records) {
            memcpy(&binary_array[binary_offset], record.binary_data().data(), record.binary_data().size());
            binary_offset += record.binary_data().size();
        }
    }

    // step 2: copy id array
    std::vector<int64_t> id_array;
    if (grpc_id_array.size() > 0) {
        id_array.resize(grpc_id_array.size());
        memcpy(id_array.data(), grpc_id_array.data(), grpc_id_array.size() * sizeof(int64_t));
    }

    // step 3: contruct vectors
    vectors.vector_count_ = grpc_records.size();
    vectors.float_data_.swap(float_array);
    vectors.binary_data_.swap(binary_array);
    vectors.id_array_.swap(id_array);
}

void
DeSerialization(const ::milvus::grpc::GeneralQuery& general_query, query::BooleanQueryPtr& boolean_clause,
                query::QueryPtr& query_ptr) {
    if (general_query.has_boolean_query()) {
        boolean_clause->SetOccur((query::Occur)general_query.boolean_query().occur());
        for (uint64_t i = 0; i < general_query.boolean_query().general_query_size(); ++i) {
            if (general_query.boolean_query().general_query(i).has_boolean_query()) {
                query::BooleanQueryPtr query = std::make_shared<query::BooleanQuery>();
                DeSerialization(general_query.boolean_query().general_query(i), query, query_ptr);
                boolean_clause->AddBooleanQuery(query);
            } else {
                auto leaf_query = std::make_shared<query::LeafQuery>();
                auto query = general_query.boolean_query().general_query(i);
                if (query.has_term_query()) {
                    query::TermQueryPtr term_query = std::make_shared<query::TermQuery>();
                    term_query->field_name = query.term_query().field_name();
                    term_query->boost = query.term_query().boost();
                    size_t int_size = query.term_query().int_value_size();
                    size_t double_size = query.term_query().double_value_size();
                    if (int_size > 0) {
                        term_query->field_value.resize(int_size * sizeof(int64_t));
                        memcpy(term_query->field_value.data(), query.term_query().int_value().data(),
                               int_size * sizeof(int64_t));
                    } else if (double_size > 0) {
                        term_query->field_value.resize(double_size * sizeof(double));
                        memcpy(term_query->field_value.data(), query.term_query().double_value().data(),
                               double_size * sizeof(double));
                    }
                    leaf_query->term_query = term_query;
                    boolean_clause->AddLeafQuery(leaf_query);
                }
                if (query.has_range_query()) {
                    query::RangeQueryPtr range_query = std::make_shared<query::RangeQuery>();
                    range_query->field_name = query.range_query().field_name();
                    range_query->boost = query.range_query().boost();
                    range_query->compare_expr.resize(query.range_query().operand_size());
                    for (uint64_t j = 0; j < query.range_query().operand_size(); ++j) {
                        range_query->compare_expr[j].compare_operator =
                            query::CompareOperator(query.range_query().operand(j).operator_());
                        range_query->compare_expr[j].operand = query.range_query().operand(j).operand();
                    }
                    leaf_query->range_query = range_query;
                    boolean_clause->AddLeafQuery(leaf_query);
                }
                if (query.has_vector_query()) {
                    query::VectorQueryPtr vector_query = std::make_shared<query::VectorQuery>();

                    engine::VectorsData vectors;
                    CopyRowRecords(query.vector_query().records(),
                                   google::protobuf::RepeatedField<google::protobuf::int64>(), vectors);

                    vector_query->query_vector.float_data = vectors.float_data_;
                    vector_query->query_vector.binary_data = vectors.binary_data_;

                    vector_query->boost = query.vector_query().query_boost();
                    vector_query->field_name = query.vector_query().field_name();
                    vector_query->topk = query.vector_query().topk();

                    milvus::json json_params;
                    for (int j = 0; j < query.vector_query().extra_params_size(); j++) {
                        const ::milvus::grpc::KeyValuePair& extra = query.vector_query().extra_params(j);
                        if (extra.key() == EXTRA_PARAM_KEY) {
                            json_params = json::parse(extra.value());
                        }
                    }
                    vector_query->extra_params = json_params;

                    // TODO(yukun): remove hardcode here
                    std::string vector_placeholder = "placeholder_1";
                    query_ptr->vectors.insert(std::make_pair(vector_placeholder, vector_query));

                    leaf_query->vector_placeholder = vector_placeholder;
                    boolean_clause->AddLeafQuery(leaf_query);
                }
            }
        }
    }
}

void
ConstructResults(const TopKQueryResult& result, ::milvus::grpc::TopKQueryResult* response) {
    if (!response) {
        return;
    }

    response->set_row_num(result.row_num_);

    response->mutable_ids()->Resize(static_cast<int>(result.id_list_.size()), 0);
    memcpy(response->mutable_ids()->mutable_data(), result.id_list_.data(), result.id_list_.size() * sizeof(int64_t));

    response->mutable_distances()->Resize(static_cast<int>(result.distance_list_.size()), 0.0);
    memcpy(response->mutable_distances()->mutable_data(), result.distance_list_.data(),
           result.distance_list_.size() * sizeof(float));
}

void
ConstructHEntityResults(const std::vector<engine::AttrsData>& attrs, const std::vector<engine::VectorsData>& vectors,
                        std::vector<std::string>& field_names, ::milvus::grpc::HEntity* response) {
    if (!response) {
        return;
    }

    response->set_row_num(attrs.size());
    if (field_names.size() > 0) {
        for (auto& name : field_names) {
            response->add_field_names(name);
        }
    } else {
        if (attrs.size() > 0) {
            auto attr_it = attrs[0].attr_type_.begin();
            for (; attr_it != attrs[0].attr_type_.end(); attr_it++) {
                field_names.emplace_back(attr_it->first);
                response->add_field_names(attr_it->first);
            }
        }
    }

    for (uint64_t i = 0; i < field_names.size() - 1; i++) {
        auto field_name = field_names[i];
        if (attrs.size() > 0) {
            if (attrs[0].attr_type_.find(field_name) != attrs[0].attr_type_.end()) {
                response->add_data_types((::milvus::grpc::DataType)attrs[0].attr_type_.at(field_name));
            }
        }
        auto grpc_attr_data = response->add_attr_data();
        std::vector<int64_t> int_data;
        std::vector<double> double_data;
        for (auto& attr : attrs) {
            auto attr_data = attr.attr_data_.at(field_name);
            int64_t grpc_int_data;
            double grpc_double_data;
            switch (attr.attr_type_.at(field_name)) {
                case engine::meta::hybrid::DataType::INT8: {
                    if (attr_data.size() == sizeof(int8_t)) {
                        grpc_int_data = attr_data[0];
                        int_data.emplace_back(grpc_int_data);
                    } else {
                        response->mutable_status()->set_error_code(::milvus::grpc::ErrorCode::UNEXPECTED_ERROR);
                        return;
                    }
                    break;
                }
                case engine::meta::hybrid::DataType::INT16: {
                    if (attr_data.size() == sizeof(int16_t)) {
                        memcpy(&grpc_int_data, attr_data.data(), sizeof(int16_t));
                        int_data.emplace_back(grpc_int_data);
                    } else {
                        response->mutable_status()->set_error_code(::milvus::grpc::ErrorCode::UNEXPECTED_ERROR);
                        return;
                    }
                    break;
                }
                case engine::meta::hybrid::DataType::INT32: {
                    if (attr_data.size() == sizeof(int32_t)) {
                        memcpy(&grpc_int_data, attr_data.data(), sizeof(int32_t));
                        int_data.emplace_back(grpc_int_data);
                    } else {
                        response->mutable_status()->set_error_code(::milvus::grpc::ErrorCode::UNEXPECTED_ERROR);
                        return;
                    }
                    break;
                }
                case engine::meta::hybrid::DataType::INT64: {
                    if (attr_data.size() == sizeof(int64_t)) {
                        memcpy(&grpc_int_data, attr_data.data(), sizeof(int64_t));
                        int_data.emplace_back(grpc_int_data);
                    } else {
                        response->mutable_status()->set_error_code(::milvus::grpc::ErrorCode::UNEXPECTED_ERROR);
                        return;
                    }
                    break;
                }
                case engine::meta::hybrid::DataType::FLOAT: {
                    if (attr_data.size() == sizeof(float)) {
                        float float_data;
                        memcpy(&float_data, attr_data.data(), sizeof(float));
                        grpc_double_data = float_data;
                        double_data.emplace_back(grpc_double_data);
                    } else {
                        response->mutable_status()->set_error_code(::milvus::grpc::ErrorCode::UNEXPECTED_ERROR);
                        return;
                    }
                    break;
                }
                case engine::meta::hybrid::DataType::DOUBLE: {
                    if (attr_data.size() == sizeof(double)) {
                        memcpy(&grpc_double_data, attr_data.data(), sizeof(double));
                        double_data.emplace_back(grpc_double_data);
                    } else {
                        response->mutable_status()->set_error_code(::milvus::grpc::ErrorCode::UNEXPECTED_ERROR);
                        return;
                    }
                    break;
                }
                default: { break; }
            }
        }
        if (int_data.size() > 0) {
            grpc_attr_data->mutable_int_value()->Resize(static_cast<int>(int_data.size()), 0);
            memcpy(grpc_attr_data->mutable_int_value()->mutable_data(), int_data.data(),
                   int_data.size() * sizeof(int64_t));
        } else if (double_data.size() > 0) {
            grpc_attr_data->mutable_double_value()->Resize(static_cast<int>(double_data.size()), 0.0);
            memcpy(grpc_attr_data->mutable_double_value()->mutable_data(), double_data.data(),
                   double_data.size() * sizeof(double));
        }
    }

    ::milvus::grpc::VectorFieldRecord* grpc_vector_data = response->add_vector_data();
    for (auto& vector : vectors) {
        auto grpc_data = grpc_vector_data->add_value();
        if (!vector.float_data_.empty()) {
            grpc_data->mutable_float_data()->Resize(vector.float_data_.size(), 0);
            memcpy(grpc_data->mutable_float_data()->mutable_data(), vector.float_data_.data(),
                   vector.float_data_.size() * sizeof(float));
        } else if (!vector.binary_data_.empty()) {
            grpc_data->mutable_binary_data()->resize(vector.binary_data_.size());
            memcpy(grpc_data->mutable_binary_data()->data(), vector.binary_data_.data(),
                   vector.binary_data_.size() * sizeof(uint8_t));
        }
    }
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
GrpcRequestHandler::CreateCollection(::grpc::ServerContext* context, const ::milvus::grpc::CollectionSchema* request,
                                     ::milvus::grpc::Status* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->RequestID().c_str(), __func__);

    Status status =
        request_handler_.CreateCollection(GetContext(context), request->collection_name(), request->dimension(),
                                          request->index_file_size(), request->metric_type());

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->RequestID().c_str(), __func__);
    SET_RESPONSE(response, status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::HasCollection(::grpc::ServerContext* context, const ::milvus::grpc::CollectionName* request,
                                  ::milvus::grpc::BoolReply* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->RequestID().c_str(), __func__);

    bool has_collection = false;

    Status status = request_handler_.HasCollection(GetContext(context), request->collection_name(), has_collection);
    response->set_bool_reply(has_collection);

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->RequestID().c_str(), __func__);
    SET_RESPONSE(response->mutable_status(), status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DropCollection(::grpc::ServerContext* context, const ::milvus::grpc::CollectionName* request,
                                   ::milvus::grpc::Status* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->RequestID().c_str(), __func__);

    Status status = request_handler_.DropCollection(GetContext(context), request->collection_name());

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->RequestID().c_str(), __func__);
    SET_RESPONSE(response, status, context);
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::CreateIndex(::grpc::ServerContext* context, const ::milvus::grpc::IndexParam* request,
                                ::milvus::grpc::Status* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->RequestID().c_str(), __func__);

    milvus::json json_params;
    for (int i = 0; i < request->extra_params_size(); i++) {
        const ::milvus::grpc::KeyValuePair& extra = request->extra_params(i);
        if (extra.key() == EXTRA_PARAM_KEY) {
            json_params = json::parse(extra.value());
        }
    }

    Status status = request_handler_.CreateIndex(GetContext(context), request->collection_name(), request->index_type(),
                                                 json_params);

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->RequestID().c_str(), __func__);
    SET_RESPONSE(response, status, context);
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::Insert(::grpc::ServerContext* context, const ::milvus::grpc::InsertParam* request,
                           ::milvus::grpc::VectorIds* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->RequestID().c_str(), __func__);

    // step 1: copy vector data
    engine::VectorsData vectors;
    CopyRowRecords(request->row_record_array(), request->row_id_array(), vectors);

    // step 2: insert vectors
    Status status =
        request_handler_.Insert(GetContext(context), request->collection_name(), vectors, request->partition_tag());

    // step 3: return id array
    response->mutable_vector_id_array()->Resize(static_cast<int>(vectors.id_array_.size()), 0);
    memcpy(response->mutable_vector_id_array()->mutable_data(), vectors.id_array_.data(),
           vectors.id_array_.size() * sizeof(int64_t));

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->RequestID().c_str(), __func__);
    SET_RESPONSE(response->mutable_status(), status, context);
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::GetVectorsByID(::grpc::ServerContext* context, const ::milvus::grpc::VectorsIdentity* request,
                                   ::milvus::grpc::VectorsData* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->RequestID().c_str(), __func__);

    std::vector<int64_t> vector_ids;
    vector_ids.reserve(request->id_array_size());
    for (int i = 0; i < request->id_array_size(); i++) {
        vector_ids.push_back(request->id_array(i));
    }

    std::vector<engine::VectorsData> vectors;
    Status status =
        request_handler_.GetVectorsByID(GetContext(context), request->collection_name(), vector_ids, vectors);

    for (auto& vector : vectors) {
        auto grpc_data = response->add_vectors_data();
        if (!vector.float_data_.empty()) {
            grpc_data->mutable_float_data()->Resize(vector.float_data_.size(), 0);
            memcpy(grpc_data->mutable_float_data()->mutable_data(), vector.float_data_.data(),
                   vector.float_data_.size() * sizeof(float));
        } else if (!vector.binary_data_.empty()) {
            grpc_data->mutable_binary_data()->resize(vector.binary_data_.size());
            memcpy(grpc_data->mutable_binary_data()->data(), vector.binary_data_.data(),
                   vector.binary_data_.size() * sizeof(uint8_t));
        }
    }

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->RequestID().c_str(), __func__);
    SET_RESPONSE(response->mutable_status(), status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::GetVectorIDs(::grpc::ServerContext* context, const ::milvus::grpc::GetVectorIDsParam* request,
                                 ::milvus::grpc::VectorIds* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->RequestID().c_str(), __func__);

    std::vector<int64_t> vector_ids;
    Status status = request_handler_.GetVectorIDs(GetContext(context), request->collection_name(),
                                                  request->segment_name(), vector_ids);

    if (!vector_ids.empty()) {
        response->mutable_vector_id_array()->Resize(vector_ids.size(), -1);
        memcpy(response->mutable_vector_id_array()->mutable_data(), vector_ids.data(),
               vector_ids.size() * sizeof(int64_t));
    }

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->RequestID().c_str(), __func__);
    SET_RESPONSE(response->mutable_status(), status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::Search(::grpc::ServerContext* context, const ::milvus::grpc::SearchParam* request,
                           ::milvus::grpc::TopKQueryResult* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->RequestID().c_str(), __func__);

    // step 1: copy vector data
    engine::VectorsData vectors;
    CopyRowRecords(request->query_record_array(), google::protobuf::RepeatedField<google::protobuf::int64>(), vectors);

    // step 2: partition tags
    std::vector<std::string> partitions;
    std::copy(request->partition_tag_array().begin(), request->partition_tag_array().end(),
              std::back_inserter(partitions));

    // step 3: parse extra parameters
    milvus::json json_params;
    for (int i = 0; i < request->extra_params_size(); i++) {
        const ::milvus::grpc::KeyValuePair& extra = request->extra_params(i);
        if (extra.key() == EXTRA_PARAM_KEY) {
            json_params = json::parse(extra.value());
        }
    }

    // step 4: search vectors
    std::vector<std::string> file_ids;
    TopKQueryResult result;
    fiu_do_on("GrpcRequestHandler.Search.not_empty_file_ids", file_ids.emplace_back("test_file_id"));

    Status status = request_handler_.Search(GetContext(context), request->collection_name(), vectors, request->topk(),
                                            json_params, partitions, file_ids, result);

    // step 5: construct and return result
    ConstructResults(result, response);

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->RequestID().c_str(), __func__);
    SET_RESPONSE(response->mutable_status(), status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::SearchByID(::grpc::ServerContext* context, const ::milvus::grpc::SearchByIDParam* request,
                               ::milvus::grpc::TopKQueryResult* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->RequestID().c_str(), __func__);

    // step 1: partition tags
    std::vector<std::string> partitions;
    std::copy(request->partition_tag_array().begin(), request->partition_tag_array().end(),
              std::back_inserter(partitions));

    // step 2: partition tags
    std::vector<int64_t> id_array;
    for (int i = 0; i < request->id_array_size(); i++) {
        id_array.push_back(request->id_array(i));
    }

    // step 3: parse extra parameters
    milvus::json json_params;
    for (int i = 0; i < request->extra_params_size(); i++) {
        const ::milvus::grpc::KeyValuePair& extra = request->extra_params(i);
        if (extra.key() == EXTRA_PARAM_KEY) {
            json_params = json::parse(extra.value());
        }
    }

    // step 4: search vectors
    TopKQueryResult result;
    Status status = request_handler_.SearchByID(GetContext(context), request->collection_name(), id_array,
                                                request->topk(), json_params, partitions, result);

    // step 5: construct and return result
    ConstructResults(result, response);

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->RequestID().c_str(), __func__);
    SET_RESPONSE(response->mutable_status(), status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::SearchInFiles(::grpc::ServerContext* context, const ::milvus::grpc::SearchInFilesParam* request,
                                  ::milvus::grpc::TopKQueryResult* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->RequestID().c_str(), __func__);

    auto* search_request = &request->search_param();

    // step 1: copy vector data
    engine::VectorsData vectors;
    CopyRowRecords(search_request->query_record_array(), google::protobuf::RepeatedField<google::protobuf::int64>(),
                   vectors);

    // step 2: copy file id array
    std::vector<std::string> file_ids;
    std::copy(request->file_id_array().begin(), request->file_id_array().end(), std::back_inserter(file_ids));

    // step 3: partition tags
    std::vector<std::string> partitions;
    std::copy(search_request->partition_tag_array().begin(), search_request->partition_tag_array().end(),
              std::back_inserter(partitions));

    // step 4: parse extra parameters
    milvus::json json_params;
    for (int i = 0; i < search_request->extra_params_size(); i++) {
        const ::milvus::grpc::KeyValuePair& extra = search_request->extra_params(i);
        if (extra.key() == EXTRA_PARAM_KEY) {
            json_params = json::parse(extra.value());
        }
    }

    // step 5: search vectors
    TopKQueryResult result;
    Status status = request_handler_.Search(GetContext(context), search_request->collection_name(), vectors,
                                            search_request->topk(), json_params, partitions, file_ids, result);

    // step 6: construct and return result
    ConstructResults(result, response);

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->RequestID().c_str(), __func__);
    SET_RESPONSE(response->mutable_status(), status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DescribeCollection(::grpc::ServerContext* context, const ::milvus::grpc::CollectionName* request,
                                       ::milvus::grpc::CollectionSchema* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->RequestID().c_str(), __func__);

    CollectionSchema collection_schema;
    Status status =
        request_handler_.DescribeCollection(GetContext(context), request->collection_name(), collection_schema);
    response->set_collection_name(collection_schema.collection_name_);
    response->set_dimension(collection_schema.dimension_);
    response->set_index_file_size(collection_schema.index_file_size_);
    response->set_metric_type(collection_schema.metric_type_);

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->RequestID().c_str(), __func__);
    SET_RESPONSE(response->mutable_status(), status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::CountCollection(::grpc::ServerContext* context, const ::milvus::grpc::CollectionName* request,
                                    ::milvus::grpc::CollectionRowCount* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->RequestID().c_str(), __func__);

    int64_t row_count = 0;
    Status status = request_handler_.CountCollection(GetContext(context), request->collection_name(), row_count);
    response->set_collection_row_count(row_count);

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->RequestID().c_str(), __func__);
    SET_RESPONSE(response->mutable_status(), status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::ShowCollections(::grpc::ServerContext* context, const ::milvus::grpc::Command* request,
                                    ::milvus::grpc::CollectionNameList* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->RequestID().c_str(), __func__);

    std::vector<std::string> collections;
    Status status = request_handler_.ShowCollections(GetContext(context), collections);
    for (auto& collection : collections) {
        response->add_collection_names(collection);
    }

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->RequestID().c_str(), __func__);
    SET_RESPONSE(response->mutable_status(), status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::ShowCollectionInfo(::grpc::ServerContext* context, const ::milvus::grpc::CollectionName* request,
                                       ::milvus::grpc::CollectionInfo* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->RequestID().c_str(), __func__);

    std::string collection_info;
    Status status =
        request_handler_.ShowCollectionInfo(GetContext(context), request->collection_name(), collection_info);
    response->set_json_info(collection_info);

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->RequestID().c_str(), __func__);
    SET_RESPONSE(response->mutable_status(), status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::Cmd(::grpc::ServerContext* context, const ::milvus::grpc::Command* request,
                        ::milvus::grpc::StringReply* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->RequestID().c_str(), __func__);

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
            if (iter.second->RequestID() == get_request_id(context)) {
                continue;
            }
            auto request_str = RequestMap(iter.second->GetRequestType()) + "-" + iter.second->RequestID();
            requests.emplace_back(request_str);
        }
        nlohmann::json reply_json;
        reply_json["requests"] = requests;
        reply = reply_json.dump();
        response->set_string_reply(reply);
    } else {
        status = request_handler_.Cmd(GetContext(context), cmd, reply);
        response->set_string_reply(reply);
    }

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->RequestID().c_str(), __func__);
    SET_RESPONSE(response->mutable_status(), status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DeleteByID(::grpc::ServerContext* context, const ::milvus::grpc::DeleteByIDParam* request,
                               ::milvus::grpc::Status* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->RequestID().c_str(), __func__);

    // step 1: prepare id array
    std::vector<int64_t> vector_ids;
    for (int i = 0; i < request->id_array_size(); i++) {
        vector_ids.push_back(request->id_array(i));
    }

    // step 2: delete vector
    Status status = request_handler_.DeleteByID(GetContext(context), request->collection_name(), vector_ids);

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->RequestID().c_str(), __func__);
    SET_RESPONSE(response, status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::PreloadCollection(::grpc::ServerContext* context, const ::milvus::grpc::CollectionName* request,
                                      ::milvus::grpc::Status* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->RequestID().c_str(), __func__);

    Status status = request_handler_.PreloadCollection(GetContext(context), request->collection_name());

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->RequestID().c_str(), __func__);
    SET_RESPONSE(response, status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DescribeIndex(::grpc::ServerContext* context, const ::milvus::grpc::CollectionName* request,
                                  ::milvus::grpc::IndexParam* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->RequestID().c_str(), __func__);

    IndexParam param;
    Status status = request_handler_.DescribeIndex(GetContext(context), request->collection_name(), param);
    response->set_collection_name(param.collection_name_);
    response->set_index_type(param.index_type_);
    ::milvus::grpc::KeyValuePair* kv = response->add_extra_params();
    kv->set_key(EXTRA_PARAM_KEY);
    kv->set_value(param.extra_params_);

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->RequestID().c_str(), __func__);
    SET_RESPONSE(response->mutable_status(), status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DropIndex(::grpc::ServerContext* context, const ::milvus::grpc::CollectionName* request,
                              ::milvus::grpc::Status* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->RequestID().c_str(), __func__);

    Status status = request_handler_.DropIndex(GetContext(context), request->collection_name());

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->RequestID().c_str(), __func__);
    SET_RESPONSE(response, status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::CreatePartition(::grpc::ServerContext* context, const ::milvus::grpc::PartitionParam* request,
                                    ::milvus::grpc::Status* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->RequestID().c_str(), __func__);

    Status status = request_handler_.CreatePartition(GetContext(context), request->collection_name(), request->tag());

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->RequestID().c_str(), __func__);
    SET_RESPONSE(response, status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::HasPartition(::grpc::ServerContext* context, const ::milvus::grpc::PartitionParam* request,
                                 ::milvus::grpc::BoolReply* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->RequestID().c_str(), __func__);

    bool has_collection = false;

    Status status =
        request_handler_.HasPartition(GetContext(context), request->collection_name(), request->tag(), has_collection);
    response->set_bool_reply(has_collection);

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->RequestID().c_str(), __func__);
    SET_RESPONSE(response->mutable_status(), status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::ShowPartitions(::grpc::ServerContext* context, const ::milvus::grpc::CollectionName* request,
                                   ::milvus::grpc::PartitionList* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->RequestID().c_str(), __func__);

    std::vector<PartitionParam> partitions;
    Status status = request_handler_.ShowPartitions(GetContext(context), request->collection_name(), partitions);
    for (auto& partition : partitions) {
        response->add_partition_tag_array(partition.tag_);
    }

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->RequestID().c_str(), __func__);
    SET_RESPONSE(response->mutable_status(), status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DropPartition(::grpc::ServerContext* context, const ::milvus::grpc::PartitionParam* request,
                                  ::milvus::grpc::Status* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->RequestID().c_str(), __func__);

    Status status = request_handler_.DropPartition(GetContext(context), request->collection_name(), request->tag());

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->RequestID().c_str(), __func__);
    SET_RESPONSE(response, status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::Flush(::grpc::ServerContext* context, const ::milvus::grpc::FlushParam* request,
                          ::milvus::grpc::Status* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->RequestID().c_str(), __func__);

    std::vector<std::string> collection_names;
    for (int32_t i = 0; i < request->collection_name_array().size(); i++) {
        collection_names.push_back(request->collection_name_array(i));
    }
    Status status = request_handler_.Flush(GetContext(context), collection_names);

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->RequestID().c_str(), __func__);
    SET_RESPONSE(response, status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::Compact(::grpc::ServerContext* context, const ::milvus::grpc::CollectionName* request,
                            ::milvus::grpc::Status* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->RequestID().c_str(), __func__);

    double compact_threshold = 0.1;  // compact trigger threshold: delete_counts/segment_counts
    Status status = request_handler_.Compact(GetContext(context), request->collection_name(), compact_threshold);

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->RequestID().c_str(), __func__);
    SET_RESPONSE(response, status, context);

    return ::grpc::Status::OK;
}

/*******************************************New Interface*********************************************/

::grpc::Status
GrpcRequestHandler::CreateHybridCollection(::grpc::ServerContext* context, const ::milvus::grpc::Mapping* request,
                                           ::milvus::grpc::Status* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->RequestID().c_str(), __func__);

    std::vector<std::pair<std::string, engine::meta::hybrid::DataType>> field_types;
    std::vector<std::pair<std::string, uint64_t>> vector_dimensions;
    std::vector<std::pair<std::string, std::string>> field_params;
    for (uint64_t i = 0; i < request->fields_size(); ++i) {
        if (request->fields(i).type().has_vector_param()) {
            auto vector_dimension =
                std::make_pair(request->fields(i).name(), request->fields(i).type().vector_param().dimension());
            vector_dimensions.emplace_back(vector_dimension);
        } else {
            auto type = std::make_pair(request->fields(i).name(),
                                       (engine::meta::hybrid::DataType)request->fields(i).type().data_type());
            field_types.emplace_back(type);
        }
        // Currently only one extra_param
        if (request->fields(i).extra_params_size() != 0) {
            auto extra_params = std::make_pair(request->fields(i).name(), request->fields(i).extra_params(0).value());
            field_params.emplace_back(extra_params);
        } else {
            auto extra_params = std::make_pair(request->fields(i).name(), "");
            field_params.emplace_back(extra_params);
        }
    }

    Status status = request_handler_.CreateHybridCollection(GetContext(context), request->collection_name(),
                                                            field_types, vector_dimensions, field_params);

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->RequestID().c_str(), __func__);
    SET_RESPONSE(response, status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DescribeHybridCollection(::grpc::ServerContext* context,
                                             const ::milvus::grpc::CollectionName* request,
                                             ::milvus::grpc::Mapping* response) {
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->RequestID().c_str(), __func__);
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->RequestID().c_str(), __func__);
}

::grpc::Status
GrpcRequestHandler::InsertEntity(::grpc::ServerContext* context, const ::milvus::grpc::HInsertParam* request,
                                 ::milvus::grpc::HEntityIDs* response) {
    //    engine::VectorsData vectors;
    //    CopyRowRecords(request->entity().vector_data(0).value(), request->entity_id_array(), vectors);

    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->RequestID().c_str(), __func__);

    auto attr_size = request->entity().attr_data().size();
    uint64_t row_num = request->entity().row_num();
    std::vector<uint8_t> attr_data(attr_size * row_num * sizeof(int64_t));
    std::unordered_map<std::string, engine::VectorsData> vector_data;
    int64_t offset = 0;
    for (uint64_t i = 0; i < attr_size; i++) {
        auto grpc_int_size = request->entity().attr_data(i).int_value_size();
        auto grpc_double_size = request->entity().attr_data(i).double_value_size();
        if (grpc_int_size > 0) {
            memcpy(attr_data.data() + offset, request->entity().attr_data(i).int_value().data(),
                   grpc_int_size * sizeof(int64_t));
            offset += grpc_int_size * sizeof(int64_t);
        } else if (grpc_double_size > 0) {
            memcpy(attr_data.data() + offset, request->entity().attr_data(i).double_value().data(),
                   grpc_double_size * sizeof(double));
            offset += grpc_double_size * sizeof(double);
        }
    }

    std::vector<std::string> field_names;
    auto field_size = request->entity().field_names_size();
    field_names.resize(field_size - 1);
    for (uint64_t i = 0; i < field_size - 1; ++i) {
        field_names[i] = request->entity().field_names(i);
    }

    auto vector_size = request->entity().vector_data_size();
    for (uint64_t i = 0; i < vector_size; ++i) {
        engine::VectorsData vectors;
        CopyRowRecords(request->entity().vector_data(i).value(), request->entity_id_array(), vectors);
        vector_data.insert(std::make_pair(request->entity().field_names(field_size - 1), vectors));
    }

    std::string collection_name = request->collection_name();
    std::string partition_tag = request->partition_tag();
    Status status = request_handler_.InsertEntity(GetContext(context), collection_name, partition_tag, row_num,
                                                  field_names, attr_data, vector_data);

    response->mutable_entity_id_array()->Resize(static_cast<int>(vector_data.begin()->second.id_array_.size()), 0);
    memcpy(response->mutable_entity_id_array()->mutable_data(), vector_data.begin()->second.id_array_.data(),
           vector_data.begin()->second.id_array_.size() * sizeof(int64_t));

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->RequestID().c_str(), __func__);
    SET_RESPONSE(response->mutable_status(), status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::HybridSearchPB(::grpc::ServerContext* context, const ::milvus::grpc::HSearchParamPB* request,
                                   ::milvus::grpc::HQueryResult* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->RequestID().c_str(), __func__);

    auto boolean_query = std::make_shared<query::BooleanQuery>();
    auto query_ptr = std::make_shared<query::Query>();
    DeSerialization(request->general_query(), boolean_query, query_ptr);

    auto general_query = std::make_shared<query::GeneralQuery>();
    query::GenBinaryQuery(boolean_query, general_query->bin);

    Status status;

    if (!query::ValidateBinaryQuery(general_query->bin)) {
        status = Status{SERVER_INVALID_BINARY_QUERY, "Generate wrong binary query tree"};
        SET_RESPONSE(response->mutable_status(), status, context);
        return ::grpc::Status::OK;
    }

    std::vector<std::string> partition_list;
    partition_list.resize(request->partition_tag_array_size());
    for (uint64_t i = 0; i < request->partition_tag_array_size(); ++i) {
        partition_list[i] = request->partition_tag_array(i);
    }

    milvus::json json_params;
    for (int i = 0; i < request->extra_params_size(); i++) {
        const ::milvus::grpc::KeyValuePair& extra = request->extra_params(i);
        if (extra.key() == EXTRA_PARAM_KEY) {
            json_params = json::parse(extra.value());
        }
    }

    engine::QueryResult result;
    std::vector<std::string> field_names;
    status = request_handler_.HybridSearch(GetContext(context), request->collection_name(), partition_list,
                                           general_query, query_ptr, json_params, field_names, result);

    // step 6: construct and return result
    response->set_row_num(result.row_num_);
    auto grpc_entity = response->mutable_entity();
    ConstructHEntityResults(result.attrs_, result.vectors_, field_names, grpc_entity);
    grpc_entity->mutable_entity_id()->Resize(static_cast<int>(result.result_ids_.size()), 0);
    memcpy(grpc_entity->mutable_entity_id()->mutable_data(), result.result_ids_.data(),
           result.result_ids_.size() * sizeof(int64_t));

    response->mutable_distance()->Resize(static_cast<int>(result.result_distances_.size()), 0.0);
    memcpy(response->mutable_distance()->mutable_data(), result.result_distances_.data(),
           result.result_distances_.size() * sizeof(float));

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->RequestID().c_str(), __func__);
    SET_RESPONSE(response->mutable_status(), status, context);

    return ::grpc::Status::OK;
}

Status
ParseTermQuery(const nlohmann::json& term_json,
               std::unordered_map<std::string, engine::meta::hybrid::DataType> field_type,
               query::TermQueryPtr& term_query) {
    std::string field_name = term_json["field_name"].get<std::string>();
    auto term_value_json = term_json["values"];
    if (!term_value_json.is_array()) {
        std::string msg = "Term json string is not an array";
        return Status{SERVER_INVALID_DSL_PARAMETER, msg};
    }

    auto term_size = term_value_json.size();
    term_query->field_name = field_name;
    term_query->field_value.resize(term_size * sizeof(int64_t));

    switch (field_type.at(field_name)) {
        case engine::meta::hybrid::DataType::INT8: {
            std::vector<int64_t> term_value(term_size, 0);
            for (uint64_t i = 0; i < term_size; i++) {
                term_value[i] = term_value_json[i].get<int8_t>();
            }
            memcpy(term_query->field_value.data(), term_value.data(), term_size * sizeof(int64_t));
            break;
        }
        case engine::meta::hybrid::DataType::INT16: {
            std::vector<int64_t> term_value(term_size, 0);
            for (uint64_t i = 0; i < term_size; i++) {
                term_value[i] = term_value_json[i].get<int16_t>();
            }
            memcpy(term_query->field_value.data(), term_value.data(), term_size * sizeof(int64_t));
            break;
        }
        case engine::meta::hybrid::DataType::INT32: {
            std::vector<int64_t> term_value(term_size, 0);
            for (uint64_t i = 0; i < term_size; i++) {
                term_value[i] = term_value_json[i].get<int32_t>();
            }
            memcpy(term_query->field_value.data(), term_value.data(), term_size * sizeof(int64_t));
            break;
        }
        case engine::meta::hybrid::DataType::INT64: {
            std::vector<int64_t> term_value(term_size, 0);
            for (uint64_t i = 0; i < term_size; ++i) {
                term_value[i] = term_value_json[i].get<int64_t>();
            }
            memcpy(term_query->field_value.data(), term_value.data(), term_size * sizeof(int64_t));
            break;
        }
        case engine::meta::hybrid::DataType::FLOAT: {
            std::vector<double> term_value(term_size, 0);
            for (uint64_t i = 0; i < term_size; ++i) {
                term_value[i] = term_value_json[i].get<float>();
            }
            memcpy(term_query->field_value.data(), term_value.data(), term_size * sizeof(double));
            break;
        }
        case engine::meta::hybrid::DataType::DOUBLE: {
            std::vector<double> term_value(term_size, 0);
            for (uint64_t i = 0; i < term_size; ++i) {
                term_value[i] = term_value_json[i].get<double>();
            }
            memcpy(term_query->field_value.data(), term_value.data(), term_size * sizeof(double));
            break;
        }
    }
    return Status::OK();
}

Status
ParseRangeQuery(const nlohmann::json& range_json, query::RangeQueryPtr& range_query) {
    std::string field_name = range_json["field_name"];
    range_query->field_name = field_name;

    auto range_value_json = range_json["values"];
    if (range_value_json.contains("lt")) {
        query::CompareExpr compare_expr;
        compare_expr.compare_operator = query::CompareOperator::LT;
        compare_expr.operand = range_value_json["lt"].get<std::string>();
        range_query->compare_expr.emplace_back(compare_expr);
    }
    if (range_value_json.contains("lte")) {
        query::CompareExpr compare_expr;
        compare_expr.compare_operator = query::CompareOperator::LTE;
        compare_expr.operand = range_value_json["lte"].get<std::string>();
        range_query->compare_expr.emplace_back(compare_expr);
    }
    if (range_value_json.contains("eq")) {
        query::CompareExpr compare_expr;
        compare_expr.compare_operator = query::CompareOperator::EQ;
        compare_expr.operand = range_value_json["eq"].get<std::string>();
        range_query->compare_expr.emplace_back(compare_expr);
    }
    if (range_value_json.contains("ne")) {
        query::CompareExpr compare_expr;
        compare_expr.compare_operator = query::CompareOperator::NE;
        compare_expr.operand = range_value_json["ne"].get<std::string>();
        range_query->compare_expr.emplace_back(compare_expr);
    }
    if (range_value_json.contains("gt")) {
        query::CompareExpr compare_expr;
        compare_expr.compare_operator = query::CompareOperator::GT;
        compare_expr.operand = range_value_json["gt"].get<std::string>();
        range_query->compare_expr.emplace_back(compare_expr);
    }
    if (range_value_json.contains("gte")) {
        query::CompareExpr compare_expr;
        compare_expr.compare_operator = query::CompareOperator::GTE;
        compare_expr.operand = range_value_json["gte"].get<std::string>();
        range_query->compare_expr.emplace_back(compare_expr);
    }
    return Status::OK();
}

Status
GrpcRequestHandler::ProcessLeafQueryJson(const nlohmann::json& json, query::BooleanQueryPtr& query) {
    auto status = Status::OK();
    if (json.contains("term")) {
        auto leaf_query = std::make_shared<query::LeafQuery>();
        auto term_json = json["term"];
        auto term_query = std::make_shared<query::TermQuery>();
        status = ParseTermQuery(term_json, field_type_, term_query);

        leaf_query->term_query = term_query;
        query->AddLeafQuery(leaf_query);
    } else if (json.contains("range")) {
        auto leaf_query = std::make_shared<query::LeafQuery>();
        auto range_query = std::make_shared<query::RangeQuery>();
        auto range_json = json["range"];
        status = ParseRangeQuery(range_json, range_query);

        leaf_query->range_query = range_query;
        query->AddLeafQuery(leaf_query);
    } else if (json.contains("vector")) {
        auto leaf_query = std::make_shared<query::LeafQuery>();
        auto vector_json = json["vector"];

        leaf_query->vector_placeholder = vector_json.get<std::string>();
        query->AddLeafQuery(leaf_query);
    }
    return status;
}

Status
GrpcRequestHandler::ProcessBooleanQueryJson(const nlohmann::json& query_json, query::BooleanQueryPtr& boolean_query) {
    auto status = Status::OK();
    if (query_json.contains("must")) {
        boolean_query->SetOccur(query::Occur::MUST);
        auto must_json = query_json["must"];
        if (!must_json.is_array()) {
            std::string msg = "Must json string is not an array";
            return Status{SERVER_INVALID_DSL_PARAMETER, msg};
        }

        for (auto& json : must_json) {
            auto must_query = std::make_shared<query::BooleanQuery>();
            if (json.contains("must") || json.contains("should") || json.contains("must_not")) {
                status = ProcessBooleanQueryJson(json, must_query);
                if (!status.ok()) {
                    return status;
                }
                boolean_query->AddBooleanQuery(must_query);
            } else {
                status = ProcessLeafQueryJson(json, boolean_query);
                if (!status.ok()) {
                    return status;
                }
            }
        }
    } else if (query_json.contains("should")) {
        boolean_query->SetOccur(query::Occur::SHOULD);
        auto should_json = query_json["should"];
        if (!should_json.is_array()) {
            std::string msg = "Should json string is not an array";
            return Status{SERVER_INVALID_DSL_PARAMETER, msg};
        }

        for (auto& json : should_json) {
            auto should_query = std::make_shared<query::BooleanQuery>();
            if (json.contains("must") || json.contains("should") || json.contains("must_not")) {
                status = ProcessBooleanQueryJson(json, should_query);
                if (!status.ok()) {
                    return status;
                }
                boolean_query->AddBooleanQuery(should_query);
            } else {
                status = ProcessLeafQueryJson(json, boolean_query);
                if (!status.ok()) {
                    return status;
                }
            }
        }
    } else if (query_json.contains("must_not")) {
        boolean_query->SetOccur(query::Occur::MUST_NOT);
        auto should_json = query_json["must_not"];
        if (!should_json.is_array()) {
            std::string msg = "Must_not json string is not an array";
            return Status{SERVER_INVALID_DSL_PARAMETER, msg};
        }

        for (auto& json : should_json) {
            if (json.contains("must") || json.contains("should") || json.contains("must_not")) {
                auto must_not_query = std::make_shared<query::BooleanQuery>();
                status = ProcessBooleanQueryJson(json, must_not_query);
                if (!status.ok()) {
                    return status;
                }
                boolean_query->AddBooleanQuery(must_not_query);
            } else {
                status = ProcessLeafQueryJson(json, boolean_query);
                if (!status.ok()) {
                    return status;
                }
            }
        }
    } else {
        std::string msg = "Must json string doesnot include right query";
        return Status{SERVER_INVALID_DSL_PARAMETER, msg};
    }

    return status;
}

Status
GrpcRequestHandler::DeserializeJsonToBoolQuery(
    const google::protobuf::RepeatedPtrField<::milvus::grpc::VectorParam>& vector_params, const std::string& dsl_string,
    query::BooleanQueryPtr& boolean_query, std::unordered_map<std::string, query::VectorQueryPtr>& vectors) {
    nlohmann::json dsl_json = json::parse(dsl_string);

    try {
        auto status = Status::OK();
        for (size_t i = 0; i < vector_params.size(); i++) {
            std::string vector_string = vector_params.at(i).json();
            nlohmann::json vector_json = json::parse(vector_string);
            json::iterator it = vector_json.begin();
            std::string placeholder = it.key();

            auto vector_query = std::make_shared<query::VectorQuery>();
            vector_query->topk = it.value()["topk"].get<int64_t>();
            vector_query->field_name = it.value()["field_name"].get<std::string>();
            vector_query->extra_params = it.value()["params"];

            engine::VectorsData vector_data;
            CopyRowRecords(vector_params.at(i).row_record(), google::protobuf::RepeatedField<google::protobuf::int64>(),
                           vector_data);
            vector_query->query_vector.binary_data = vector_data.binary_data_;
            vector_query->query_vector.float_data = vector_data.float_data_;

            vectors.insert(std::make_pair(placeholder, vector_query));
        }

        if (dsl_json.contains("bool")) {
            auto boolean_query_json = dsl_json["bool"];
            status = ProcessBooleanQueryJson(boolean_query_json, boolean_query);
            if (!status.ok()) {
                return status;
            }
        }
        return status;
    } catch (std::exception& e) {
        return Status{SERVER_INVALID_DSL_PARAMETER, e.what()};
    }
}

::grpc::Status
GrpcRequestHandler::HybridSearch(::grpc::ServerContext* context, const ::milvus::grpc::HSearchParam* request,
                                 ::milvus::grpc::HQueryResult* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->RequestID().c_str(), __func__);

    Status status;

    status = request_handler_.DescribeHybridCollection(GetContext(context), request->collection_name(), field_type_);

    query::BooleanQueryPtr boolean_query = std::make_shared<query::BooleanQuery>();
    query::QueryPtr query_ptr = std::make_shared<query::Query>();
    std::unordered_map<std::string, query::VectorQueryPtr> vectors;

    DeserializeJsonToBoolQuery(request->vector_param(), request->dsl(), boolean_query, vectors);

    query_ptr->vectors = vectors;

    query::GeneralQueryPtr general_query = std::make_shared<query::GeneralQuery>();
    query::GenBinaryQuery(boolean_query, general_query->bin);
    query_ptr->root = general_query->bin;

    if (!query::ValidateBinaryQuery(general_query->bin)) {
        status = Status{SERVER_INVALID_BINARY_QUERY, "Generate wrong binary query tree"};
        SET_RESPONSE(response->mutable_status(), status, context);
        return ::grpc::Status::OK;
    }

    std::vector<std::string> partition_list;
    partition_list.resize(request->partition_tag_array_size());
    for (uint64_t i = 0; i < request->partition_tag_array_size(); ++i) {
        partition_list[i] = request->partition_tag_array(i);
    }

    milvus::json json_params;
    for (int i = 0; i < request->extra_params_size(); i++) {
        const ::milvus::grpc::KeyValuePair& extra = request->extra_params(i);
        if (extra.key() == EXTRA_PARAM_KEY) {
            json_params = json::parse(extra.value());
        }
    }

    engine::QueryResult result;
    std::vector<std::string> field_names;
    status = request_handler_.HybridSearch(GetContext(context), request->collection_name(), partition_list,
                                           general_query, query_ptr, json_params, field_names, result);

    // step 6: construct and return result
    response->set_row_num(result.row_num_);
    auto grpc_entity = response->mutable_entity();
    ConstructHEntityResults(result.attrs_, result.vectors_, field_names, grpc_entity);
    grpc_entity->mutable_entity_id()->Resize(static_cast<int>(result.result_ids_.size()), 0);
    memcpy(grpc_entity->mutable_entity_id()->mutable_data(), result.result_ids_.data(),
           result.result_ids_.size() * sizeof(int64_t));

    response->mutable_distance()->Resize(static_cast<int>(result.result_distances_.size()), 0.0);
    memcpy(response->mutable_distance()->mutable_data(), result.result_distances_.data(),
           result.result_distances_.size() * sizeof(float));

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->RequestID().c_str(), __func__);
    SET_RESPONSE(response->mutable_status(), status, context);

    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::GetEntityByID(::grpc::ServerContext* context, const ::milvus::grpc::VectorsIdentity* request,
                                  ::milvus::grpc::HEntity* response) {
    CHECK_NULLPTR_RETURN(request);
    LOG_SERVER_INFO_ << LogOut("Request [%s] %s begin.", GetContext(context)->RequestID().c_str(), __func__);

    std::vector<int64_t> vector_ids;
    vector_ids.reserve(request->id_array_size());
    for (int i = 0; i < request->id_array_size(); i++) {
        vector_ids.push_back(request->id_array(i));
    }

    std::vector<engine::AttrsData> attrs;
    std::vector<engine::VectorsData> vectors;
    Status status =
        request_handler_.GetEntityByID(GetContext(context), request->collection_name(), vector_ids, attrs, vectors);

    std::vector<std::string> field_names;
    ConstructHEntityResults(attrs, vectors, field_names, response);

    LOG_SERVER_INFO_ << LogOut("Request [%s] %s end.", GetContext(context)->RequestID().c_str(), __func__);
    SET_RESPONSE(response->mutable_status(), status, context);

    return ::grpc::Status::OK;
}

}  // namespace grpc
}  // namespace server
}  // namespace milvus
