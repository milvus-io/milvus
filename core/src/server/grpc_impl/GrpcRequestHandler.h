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

#pragma once

#include <server/context/Context.h>

#include <cstdint>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>

#include "grpc/gen-milvus/milvus.grpc.pb.h"
#include "grpc/gen-status/status.pb.h"
#include "opentracing/tracer.h"
#include "server/delivery/RequestHandler.h"
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

class GrpcRequestHandler final : public ::milvus::grpc::MilvusService::Service, public GrpcInterceptorHookHandler {
 public:
    explicit GrpcRequestHandler(const std::shared_ptr<opentracing::Tracer>& tracer);

    void
    OnPostRecvInitialMetaData(::grpc::experimental::ServerRpcInfo* server_rpc_info,
                              ::grpc::experimental::InterceptorBatchMethods* interceptor_batch_methods) override;

    void
    OnPreSendMessage(::grpc::experimental::ServerRpcInfo* server_rpc_info,
                     ::grpc::experimental::InterceptorBatchMethods* interceptor_batch_methods) override;

    const std::shared_ptr<Context>&
    GetContext(::grpc::ServerContext* server_context);

    void
    SetContext(::grpc::ServerContext* server_context, const std::shared_ptr<Context>& context);

    uint64_t
    random_id() const;

    // *
    // @brief This method is used to create table
    //
    // @param TableSchema, use to provide table information to be created.
    //
    // @return Status
    ::grpc::Status
    CreateTable(::grpc::ServerContext* context, const ::milvus::grpc::TableSchema* request,
                ::milvus::grpc::Status* response) override;
    // *
    // @brief This method is used to test table existence.
    //
    // @param TableName, table name is going to be tested.
    //
    // @return BoolReply
    ::grpc::Status
    HasTable(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
             ::milvus::grpc::BoolReply* response) override;
    // *
    // @brief This method is used to get table schema.
    //
    // @param TableName, target table name.
    //
    // @return TableSchema
    ::grpc::Status
    DescribeTable(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                  ::milvus::grpc::TableSchema* response) override;
    // *
    // @brief This method is used to get table schema.
    //
    // @param TableName, target table name.
    //
    // @return TableRowCount
    ::grpc::Status
    CountTable(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
               ::milvus::grpc::TableRowCount* response) override;
    // *
    // @brief This method is used to list all tables.
    //
    // @param Command, dummy parameter.
    //
    // @return TableNameList
    ::grpc::Status
    ShowTables(::grpc::ServerContext* context, const ::milvus::grpc::Command* request,
               ::milvus::grpc::TableNameList* response) override;
    // *
    // @brief This method is used to delete table.
    //
    // @param TableName, table name is going to be deleted.
    //
    // @return TableNameList
    ::grpc::Status
    DropTable(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
              ::milvus::grpc::Status* response) override;
    // *
    // @brief This method is used to build index by table in sync mode.
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
    // @param TableName, target table name.
    //
    // @return IndexParam
    ::grpc::Status
    DescribeIndex(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                  ::milvus::grpc::IndexParam* response) override;
    // *
    // @brief This method is used to drop index
    //
    // @param TableName, target table name.
    //
    // @return Status
    ::grpc::Status
    DropIndex(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
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
    // @brief This method is used to show partition information
    //
    // @param TableName, target table name.
    //
    // @return PartitionList
    ::grpc::Status
    ShowPartitions(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
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
    // @brief This method is used to add vector array to table.
    //
    // @param InsertParam, insert parameters.
    //
    // @return VectorIds
    ::grpc::Status
    Insert(::grpc::ServerContext* context, const ::milvus::grpc::InsertParam* request,
           ::milvus::grpc::VectorIds* response) override;
    // *
    // @brief This method is used to query vector in table.
    //
    // @param SearchParam, search parameters.
    //
    // @return TopKQueryResultList
    ::grpc::Status
    Search(::grpc::ServerContext* context, const ::milvus::grpc::SearchParam* request,
           ::milvus::grpc::TopKQueryResult* response) override;

    // *
    // @brief This method is used to query vector in specified files.
    //
    // @param SearchInFilesParam, search in files paremeters.
    //
    // @return TopKQueryResultList
    ::grpc::Status
    SearchInFiles(::grpc::ServerContext* context, const ::milvus::grpc::SearchInFilesParam* request,
                  ::milvus::grpc::TopKQueryResult* response) override;

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
    // @brief This method is used to delete vector by date range
    //
    // @param DeleteByDateParam, delete parameters.
    //
    // @return status
    ::grpc::Status
    DeleteByDate(::grpc::ServerContext* context, const ::milvus::grpc::DeleteByDateParam* request,
                 ::milvus::grpc::Status* response) override;
    // *
    // @brief This method is used to preload table
    //
    // @param TableName, target table name.
    //
    // @return Status
    ::grpc::Status
    PreloadTable(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request,
                 ::milvus::grpc::Status* response) override;

    GrpcRequestHandler&
    RegisterRequestHandler(const RequestHandler& handler) {
        request_handler_ = handler;
    }

 private:
    RequestHandler request_handler_;

    std::unordered_map<::grpc::ServerContext*, std::shared_ptr<Context>> context_map_;
    std::shared_ptr<opentracing::Tracer> tracer_;
    //    std::unordered_map<::grpc::ServerContext*, std::unique_ptr<opentracing::Span>> span_map_;

    mutable std::mt19937_64 random_num_generator_;
    mutable std::mutex random_mutex_;
    mutable std::mutex context_map_mutex_;
};

}  // namespace grpc
}  // namespace server
}  // namespace milvus
