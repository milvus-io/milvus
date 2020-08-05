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

#pragma once

#include <grpcpp/server_context.h>

#include <cstdint>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>

#include "grpc/gen-milvus/milvus.grpc.pb.h"
#include "grpc/gen-status/status.pb.h"
#include "opentracing/tracer.h"
#include "server/context/Context.h"
#include "server/delivery/ReqHandler.h"
#include "server/grpc_impl/interceptor/GrpcInterceptorHookHandler.h"
#include "src/utils/Status.h"

namespace milvus {
namespace server {
namespace grpc {

#define CHECK_NULLPTR_RETURN(PTR)  \
    if (nullptr == request) {      \
        return ::grpc::Status::OK; \
    }

#define SET_TRACING_TAG(STATUS, SERVER_CONTEXT)                                                                  \
    if ((STATUS).code() != ::milvus::grpc::ErrorCode::SUCCESS) {                                                 \
        GetContext((SERVER_CONTEXT))->GetTraceContext()->GetSpan()->SetTag("error", true);                       \
        GetContext((SERVER_CONTEXT))->GetTraceContext()->GetSpan()->SetTag("error_message", (STATUS).message()); \
    }

#define SET_RESPONSE(RESPONSE, STATUS, SERVER_CONTEXT)                      \
    do {                                                                    \
        if ((STATUS).ok()) {                                                \
            (RESPONSE)->set_error_code(::milvus::grpc::ErrorCode::SUCCESS); \
        } else {                                                            \
            (RESPONSE)->set_error_code(ErrorMap((STATUS).code()));          \
        }                                                                   \
        (RESPONSE)->set_reason((STATUS).message());                         \
        SET_TRACING_TAG(STATUS, SERVER_CONTEXT);                            \
    } while (false);

::milvus::grpc::ErrorCode
ErrorMap(ErrorCode code);

extern const char* EXTRA_PARAM_KEY;

class GrpcRequestHandler final : public ::milvus::grpc::MilvusService::Service, public GrpcInterceptorHookHandler {
 public:
    explicit GrpcRequestHandler(const std::shared_ptr<opentracing::Tracer>& tracer);

    void
    OnPostRecvInitialMetaData(::grpc::experimental::ServerRpcInfo* server_rpc_info,
                              ::grpc::experimental::InterceptorBatchMethods* interceptor_batch_methods) override;

    void
    OnPreSendMessage(::grpc::experimental::ServerRpcInfo* server_rpc_info,
                     ::grpc::experimental::InterceptorBatchMethods* interceptor_batch_methods) override;

    std::shared_ptr<Context>
    GetContext(::grpc::ServerContext* server_context);

    void
    SetContext(::grpc::ServerContext* server_context, const std::shared_ptr<Context>& context);

    uint64_t
    random_id() const;

    // *
    // @brief This method is used to create collection
    //
    // @param CollectionSchema, use to provide collection information to be created.
    //
    // @return Status
    ::grpc::Status
    CreateCollection(::grpc::ServerContext* context, const ::milvus::grpc::Mapping* request,
                     ::milvus::grpc::Status* response) override;
    // *
    // @brief This method is used to test collection existence.
    //
    // @param CollectionName, collection name is going to be tested.
    //
    // @return BoolReply
    ::grpc::Status
    HasCollection(::grpc::ServerContext* context, const ::milvus::grpc::CollectionName* request,
                  ::milvus::grpc::BoolReply* response) override;
    // *
    // @brief This method is used to get collection schema.
    //
    // @param CollectionName, target collection name.
    //
    // @return CollectionSchema
    ::grpc::Status
    DescribeCollection(::grpc::ServerContext* context, const ::milvus::grpc::CollectionName* request,
                       ::milvus::grpc::Mapping* response) override;
    // *
    // @brief This method is used to get collection schema.
    //
    // @param CollectionName, target collection name.
    //
    // @return CollectionRowCount
    ::grpc::Status
    CountCollection(::grpc::ServerContext* context, const ::milvus::grpc::CollectionName* request,
                    ::milvus::grpc::CollectionRowCount* response) override;
    // *
    // @brief This method is used to list all collections.
    //
    // @param Command, dummy parameter.
    //
    // @return CollectionNameList
    ::grpc::Status
    ShowCollections(::grpc::ServerContext* context, const ::milvus::grpc::Command* request,
                    ::milvus::grpc::CollectionNameList* response) override;
    // *
    // @brief This method is used to get collection detail information.
    //
    // @param CollectionName, target collection name.
    //
    // @return CollectionInfo
    ::grpc::Status
    ShowCollectionInfo(::grpc::ServerContext* context, const ::milvus::grpc::CollectionName* request,
                       ::milvus::grpc::CollectionInfo* response);

    // *
    // @brief This method is used to delete collection.
    //
    // @param CollectionName, collection name is going to be deleted.
    //
    // @return CollectionNameList
    ::grpc::Status
    DropCollection(::grpc::ServerContext* context, const ::milvus::grpc::CollectionName* request,
                   ::milvus::grpc::Status* response) override;
    // *
    // @brief This method is used to build index by collection in sync mode.
    //
    // @param IndexParam, index paramters.
    //
    // @return Status
    ::grpc::Status
    CreateIndex(::grpc::ServerContext* context, const ::milvus::grpc::IndexParam* request,
                ::milvus::grpc::Status* response) override;
    // *
    // @brief This method is used to describe index
    //
    // @param CollectionName, target collection name.
    //
    // @return IndexParam
    ::grpc::Status
    DescribeIndex(::grpc::ServerContext* context, const ::milvus::grpc::IndexParam* request,
                  ::milvus::grpc::IndexParam* response) override;
    // *
    // @brief This method is used to drop index
    //
    // @param CollectionName, target collection name.
    //
    // @return Status
    ::grpc::Status
    DropIndex(::grpc::ServerContext* context, const ::milvus::grpc::IndexParam* request,
              ::milvus::grpc::Status* response) override;
    // *
    // @brief This method is used to create partition
    //
    // @param PartitionParam, partition parameters.
    //
    // @return Status
    ::grpc::Status
    CreatePartition(::grpc::ServerContext* context, const ::milvus::grpc::PartitionParam* request,
                    ::milvus::grpc::Status* response) override;

    // *
    // @brief This method is used to test partition existence.
    //
    // @param PartitionParam, target partition.
    //
    // @return BoolReply
    ::grpc::Status
    HasPartition(::grpc::ServerContext* context, const ::milvus::grpc::PartitionParam* request,
                 ::milvus::grpc::BoolReply* response);

    // *
    // @brief This method is used to show partition information
    //
    // @param CollectionName, target collection name.
    //
    // @return PartitionList
    ::grpc::Status
    ShowPartitions(::grpc::ServerContext* context, const ::milvus::grpc::CollectionName* request,
                   ::milvus::grpc::PartitionList* response) override;
    // *
    // @brief This method is used to drop partition
    //
    // @param PartitionName, target partition name.
    //
    // @return Status
    ::grpc::Status
    DropPartition(::grpc::ServerContext* context, const ::milvus::grpc::PartitionParam* request,
                  ::milvus::grpc::Status* response) override;
    // *
    // @brief This method is used to add vector array to collection.
    //
    // @param InsertParam, insert parameters.
    //
    // @return VectorIds
    ::grpc::Status
    Insert(::grpc::ServerContext* context, const ::milvus::grpc::InsertParam* request,
           ::milvus::grpc::EntityIds* response) override;
    // *
    // @brief This method is used to get vectors data by id array.
    //
    // @param VectorsIdentity, target vector id array.
    //
    // @return VectorsData
    ::grpc::Status
    GetEntityByID(::grpc::ServerContext* context, const ::milvus::grpc::EntityIdentity* request,
                  ::milvus::grpc::Entities* response) override;

    // *
    // @brief This method is used to get vector ids from a segment
    //
    // @param GetVectorIDsParam, target collection and segment
    //
    // @return VectorIds
    ::grpc::Status
    GetEntityIDs(::grpc::ServerContext* context, const ::milvus::grpc::GetEntityIDsParam* request,
                 ::milvus::grpc::EntityIds* response) override;
    // *
    // @brief This method is used to query vector in collection.
    //
    // @param SearchParam, search parameters.
    //
    // @return TopKQueryResultList
    ::grpc::Status
    Search(::grpc::ServerContext* context, const ::milvus::grpc::SearchParam* request,
           ::milvus::grpc::QueryResult* response) override;

    // *
    // @brief This method is used to query vector in specified files.
    //
    // @param SearchInFilesParam, search in files paremeters.
    //
    // @return TopKQueryResultList
    ::grpc::Status
    SearchInSegment(::grpc::ServerContext* context, const ::milvus::grpc::SearchInSegmentParam* request,
                    ::milvus::grpc::QueryResult* response) override;

    // *
    // @brief This method is used to give the server status.
    //
    // @param Command, command string
    //
    // @return StringReply
    ::grpc::Status
    Cmd(::grpc::ServerContext* context, const ::milvus::grpc::Command* request,
        ::milvus::grpc::StringReply* response) override;

    // *
    // @brief This method is used to delete vector by id
    //
    // @param DeleteByIDParam, delete parameters.
    //
    // @return status
    ::grpc::Status
    DeleteByID(::grpc::ServerContext* context, const ::milvus::grpc::DeleteByIDParam* request,
               ::milvus::grpc::Status* response) override;

    // *
    // @brief This method is used to preload collection
    //
    // @param CollectionName, target collection name.
    //
    // @return Status
    ::grpc::Status
    PreloadCollection(::grpc::ServerContext* context, const ::milvus::grpc::CollectionName* request,
                      ::milvus::grpc::Status* response) override;

    // *
    // @brief This method is used to flush buffer into storage.
    //
    // @param FlushParam, flush parameters
    //
    // @return Status
    ::grpc::Status
    Flush(::grpc::ServerContext* context, const ::milvus::grpc::FlushParam* request,
          ::milvus::grpc::Status* response) override;

    // *
    // @brief This method is used to compact collection
    //
    // @param CollectionName, target collection name.
    //
    // @return Status
    ::grpc::Status
    Compact(::grpc::ServerContext* context, ::milvus::grpc::CompactParam* request, ::milvus::grpc::Status* response);

    /*******************************************New Interface*********************************************/

    ::grpc::Status
    SearchPB(::grpc::ServerContext* context, const ::milvus::grpc::SearchParamPB* request,
             ::milvus::grpc::QueryResult* response) override;

    void
    RegisterRequestHandler(const ReqHandler& handler) {
        req_handler_ = handler;
    }

    Status
    DeserializeJsonToBoolQuery(const google::protobuf::RepeatedPtrField<::milvus::grpc::VectorParam>& vector_params,
                               const std::string& dsl_string, query::BooleanQueryPtr& boolean_query,
                               query::QueryPtr& query_ptr);

    Status
    ProcessBooleanQueryJson(const nlohmann::json& query_json, query::BooleanQueryPtr& boolean_query,
                            query::QueryPtr& query_ptr);

    Status
    ProcessLeafQueryJson(const nlohmann::json& json, query::BooleanQueryPtr& query, std::string& field_name);

 private:
    ReqHandler req_handler_;

    std::unordered_map<std::string, std::shared_ptr<Context>> context_map_;
    std::shared_ptr<opentracing::Tracer> tracer_;

    mutable std::mt19937_64 random_num_generator_;
    mutable std::mutex random_mutex_;
    mutable std::mutex context_map_mutex_;
};

}  // namespace grpc
}  // namespace server
}  // namespace milvus
