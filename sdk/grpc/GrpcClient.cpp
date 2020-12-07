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

#include "grpc/GrpcClient.h"

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include <memory>
#include <string>
#include <vector>

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Status;

namespace milvus {
GrpcClient::GrpcClient(std::shared_ptr<::grpc::Channel>& channel)
    : stub_(::milvus::grpc::MilvusService::NewStub(channel)) {
}

GrpcClient::~GrpcClient() = default;

Status
GrpcClient::CreateCollection(const milvus::grpc::Mapping& mapping) {
    ClientContext context;
    ::milvus::grpc::Status response;
    ::grpc::Status grpc_status = stub_->CreateCollection(&context, mapping, &response);

    if (!grpc_status.ok()) {
        std::cerr << "CreateHybridCollection gRPC failed!" << std::endl;
        return Status(StatusCode::RPCFailed, grpc_status.error_message());
    }

    if (response.error_code() != 0) {
        std::cerr << response.reason() << std::endl;
        return Status(StatusCode::ServerFailed, response.reason());
    }
    return Status::OK();
}

bool
GrpcClient::HasCollection(const ::milvus::grpc::CollectionName& collection_name, Status& status) {
    ClientContext context;
    ::milvus::grpc::BoolReply response;
    ::grpc::Status grpc_status = stub_->HasCollection(&context, collection_name, &response);

    if (!grpc_status.ok()) {
        std::cerr << "HasCollection gRPC failed!" << std::endl;
        status = Status(StatusCode::RPCFailed, grpc_status.error_message());
    }
    if (response.status().error_code() != 0) {
        std::cerr << response.status().reason() << std::endl;
        status = Status(StatusCode::ServerFailed, response.status().reason());
    }
    status = Status::OK();
    return response.bool_reply();
}

Status
GrpcClient::DropCollection(const ::milvus::grpc::CollectionName& collection_name) {
    ClientContext context;
    grpc::Status response;
    ::grpc::Status grpc_status = stub_->DropCollection(&context, collection_name, &response);

    if (!grpc_status.ok()) {
        std::cerr << "DropCollection gRPC failed!" << std::endl;
        return Status(StatusCode::RPCFailed, grpc_status.error_message());
    }
    if (response.error_code() != 0) {
        std::cerr << response.reason() << std::endl;
        return Status(StatusCode::ServerFailed, response.reason());
    }

    return Status::OK();
}

Status
GrpcClient::CreateIndex(const ::milvus::grpc::IndexParam& index_param) {
    ClientContext context;
    grpc::Status response;
    ::grpc::Status grpc_status = stub_->CreateIndex(&context, index_param, &response);

    if (!grpc_status.ok()) {
        std::cerr << "CreateIndex rpc failed!" << std::endl;
        return Status(StatusCode::RPCFailed, grpc_status.error_message());
    }
    if (response.error_code() != 0) {
        std::cerr << response.reason() << std::endl;
        return Status(StatusCode::ServerFailed, response.reason());
    }

    return Status::OK();
}

Status
GrpcClient::Insert(const ::milvus::grpc::InsertParam& insert_param, ::milvus::grpc::EntityIds& entitiy_ids) {
    ClientContext context;
    ::grpc::Status grpc_status = stub_->Insert(&context, insert_param, &entitiy_ids);

    CHECK_GRPC_STATUS(grpc_status, "Insert", StatusCode::ServerFailed)
    CHECK_ERROR_CODE(entitiy_ids.status(), StatusCode::ServerFailed)

    return Status::OK();
}

Status
GrpcClient::GetEntityByID(const grpc::EntityIdentity& entity_identity, ::milvus::grpc::Entities& entities) {
    ClientContext context;
    ::grpc::Status grpc_status = stub_->GetEntityByID(&context, entity_identity, &entities);

    CHECK_GRPC_STATUS(grpc_status, "GetEntityByID", StatusCode::RPCFailed)
    CHECK_ERROR_CODE(entities.status(), StatusCode::ServerFailed)
    return Status::OK();
}

Status
GrpcClient::ListIDInSegment(const grpc::GetEntityIDsParam& param, grpc::EntityIds& entity_ids) {
    ClientContext context;
    ::grpc::Status grpc_status = stub_->GetEntityIDs(&context, param, &entity_ids);

    CHECK_GRPC_STATUS(grpc_status, "ListIDsInSegment", StatusCode::RPCFailed)
    CHECK_ERROR_CODE(entity_ids.status(), StatusCode::ServerFailed)

    return Status::OK();
}

Status
GrpcClient::Search(const ::milvus::grpc::SearchParam& search_param, ::milvus::grpc::QueryResult& topk_query_result) {
    ClientContext context;
    ::grpc::Status grpc_status = stub_->Search(&context, search_param, &topk_query_result);

    CHECK_GRPC_STATUS(grpc_status, "Search", StatusCode::RPCFailed)
    CHECK_ERROR_CODE(topk_query_result.status(), StatusCode::ServerFailed)

    return Status::OK();
}

Status
GrpcClient::GetCollectionInfo(const std::string& collection_name, ::milvus::grpc::Mapping& grpc_schema) {
    ClientContext context;
    ::milvus::grpc::CollectionName grpc_collectionname;
    grpc_collectionname.set_collection_name(collection_name);
    ::grpc::Status grpc_status = stub_->DescribeCollection(&context, grpc_collectionname, &grpc_schema);

    CHECK_GRPC_STATUS(grpc_status, "DescribeCollection", StatusCode::RPCFailed)
    CHECK_ERROR_CODE(grpc_schema.status(), StatusCode::ServerFailed)

    return Status::OK();
}

Status
GrpcClient::CountEntities(grpc::CollectionName& collection_name, ::milvus::grpc::CollectionRowCount& row_count) {
    ClientContext context;
    ::grpc::Status grpc_status = stub_->CountCollection(&context, collection_name, &row_count);

    CHECK_GRPC_STATUS(grpc_status, "CountEntities", StatusCode::RPCFailed)
    CHECK_ERROR_CODE(row_count.status(), StatusCode::ServerFailed)

    return Status::OK();
}

Status
GrpcClient::ListCollections(milvus::grpc::CollectionNameList& collection_name_list) {
    ClientContext context;
    ::milvus::grpc::Command command;
    ::grpc::Status grpc_status = stub_->ShowCollections(&context, command, &collection_name_list);

    CHECK_GRPC_STATUS(grpc_status, "ListCollections", StatusCode::RPCFailed)
    CHECK_ERROR_CODE(collection_name_list.status(), StatusCode::ServerFailed)

    return Status::OK();
}

Status
GrpcClient::GetCollectionStats(grpc::CollectionName& collection_name, grpc::CollectionInfo& collection_stats) {
    ClientContext context;
    ::milvus::grpc::Command command;
    ::grpc::Status grpc_status = stub_->ShowCollectionInfo(&context, collection_name, &collection_stats);

    CHECK_GRPC_STATUS(grpc_status, "GetCollectionStats", StatusCode::RPCFailed)
    CHECK_ERROR_CODE(collection_stats.status(), StatusCode::ServerFailed)

    return Status::OK();
}

Status
GrpcClient::Cmd(const std::string& cmd, std::string& result) {
    ClientContext context;
    ::milvus::grpc::StringReply response;
    ::milvus::grpc::Command command;
    command.set_cmd(cmd);
    ::grpc::Status grpc_status = stub_->Cmd(&context, command, &response);

    result = response.string_reply();
    CHECK_GRPC_STATUS(grpc_status, "Cmd", StatusCode::RPCFailed)
    CHECK_ERROR_CODE(response.status(), StatusCode::ServerFailed)

    return Status::OK();
}

Status
GrpcClient::LoadCollection(milvus::grpc::CollectionName& collection_name) {
    ClientContext context;
    ::milvus::grpc::Status response;
    ::grpc::Status grpc_status = stub_->PreloadCollection(&context, collection_name, &response);

    CHECK_GRPC_STATUS(grpc_status, "LoadCollection", StatusCode::RPCFailed)
    CHECK_ERROR_CODE(response, StatusCode::ServerFailed)
    return Status::OK();
}

Status
GrpcClient::DeleteEntityByID(grpc::DeleteByIDParam& delete_by_id_param) {
    ClientContext context;
    ::milvus::grpc::Status response;
    ::grpc::Status grpc_status = stub_->DeleteByID(&context, delete_by_id_param, &response);

    CHECK_GRPC_STATUS(grpc_status, "DeleteEntityByID", StatusCode::RPCFailed)
    CHECK_ERROR_CODE(response, StatusCode::ServerFailed)
    return Status::OK();
}

Status
GrpcClient::DropIndex(grpc::IndexParam& index_param) {
    ClientContext context;
    ::milvus::grpc::Status response;
    ::grpc::Status grpc_status = stub_->DropIndex(&context, index_param, &response);

    CHECK_GRPC_STATUS(grpc_status, "DropIndex", StatusCode::RPCFailed)
    CHECK_ERROR_CODE(response, StatusCode::ServerFailed)
    return Status::OK();
}

Status
GrpcClient::CreatePartition(const grpc::PartitionParam& partition_param) {
    ClientContext context;
    ::milvus::grpc::Status response;
    ::grpc::Status grpc_status = stub_->CreatePartition(&context, partition_param, &response);

    CHECK_GRPC_STATUS(grpc_status, "CreatePartition", StatusCode::RPCFailed)
    CHECK_ERROR_CODE(response, StatusCode::ServerFailed)
    return Status::OK();
}

bool
GrpcClient::HasPartition(const grpc::PartitionParam& partition_param, Status& status) const {
    ClientContext context;
    ::milvus::grpc::BoolReply response;
    ::grpc::Status grpc_status = stub_->HasPartition(&context, partition_param, &response);

    if (!grpc_status.ok()) {
        std::cerr << "HasPartition gRPC failed!" << std::endl;
        status = Status(StatusCode::RPCFailed, grpc_status.error_message());
    }
    if (response.status().error_code() != 0) {
        std::cerr << response.status().reason() << std::endl;
        status = Status(StatusCode::ServerFailed, response.status().reason());
    }
    status = Status::OK();
    return response.bool_reply();
}

Status
GrpcClient::ListPartitions(const grpc::CollectionName& collection_name, grpc::PartitionList& partition_array) const {
    ClientContext context;
    ::grpc::Status grpc_status = stub_->ShowPartitions(&context, collection_name, &partition_array);

    CHECK_GRPC_STATUS(grpc_status, "ListPartitions", StatusCode::RPCFailed)
    CHECK_ERROR_CODE(partition_array.status(), StatusCode::ServerFailed)

    return Status::OK();
}

Status
GrpcClient::DropPartition(const ::milvus::grpc::PartitionParam& partition_param) {
    ClientContext context;
    ::milvus::grpc::Status response;
    ::grpc::Status grpc_status = stub_->DropPartition(&context, partition_param, &response);

    CHECK_GRPC_STATUS(grpc_status, "DropPartition", StatusCode::RPCFailed)
    CHECK_ERROR_CODE(response, StatusCode::ServerFailed)
    return Status::OK();
}

Status
GrpcClient::Flush(const std::string& collection_name) {
    ClientContext context;

    ::milvus::grpc::FlushParam param;
    if (!collection_name.empty()) {
        param.add_collection_name_array(collection_name);
    }

    ::milvus::grpc::Status response;
    ::grpc::Status grpc_status = stub_->Flush(&context, param, &response);

    CHECK_GRPC_STATUS(grpc_status, "Flush", StatusCode::RPCFailed)
    CHECK_ERROR_CODE(response, StatusCode::ServerFailed)
    return Status::OK();
}

Status
GrpcClient::Compact(milvus::grpc::CompactParam& compact_param) {
    ClientContext context;
    ::milvus::grpc::Status response;
    ::grpc::Status grpc_status = stub_->Compact(&context, compact_param, &response);

    CHECK_GRPC_STATUS(grpc_status, "Compact", StatusCode::RPCFailed)
    CHECK_ERROR_CODE(response, StatusCode::ServerFailed)
    return Status::OK();
}

Status
GrpcClient::Disconnect() {
    stub_.release();
    return Status::OK();
}

Status
GrpcClient::SearchPB(milvus::grpc::SearchParamPB& search_param, milvus::grpc::QueryResult& result) {
    ClientContext context;
    ::grpc::Status grpc_status = stub_->SearchPB(&context, search_param, &result);

    CHECK_GRPC_STATUS(grpc_status, "SearchPB", StatusCode::RPCFailed)
    CHECK_ERROR_CODE(result.status(), StatusCode::ServerFailed)
    return Status::OK();
}

}  // namespace milvus
