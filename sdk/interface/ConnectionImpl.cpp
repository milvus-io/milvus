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

#include "interface/ConnectionImpl.h"

namespace milvus {

std::shared_ptr<Connection>
Connection::Create() {
    return std::shared_ptr<Connection>(new ConnectionImpl());
}

Status
Connection::Destroy(std::shared_ptr<milvus::Connection>& connection_ptr) {
    if (connection_ptr != nullptr) {
        return connection_ptr->Disconnect();
    }
    return Status::OK();
}

//////////////////////////////////////////////////////////////////////////////////////////////
ConnectionImpl::ConnectionImpl() {
    client_proxy_ = std::make_shared<ClientProxy>();
}

Status
ConnectionImpl::Connect(const ConnectParam& param) {
    return client_proxy_->Connect(param);
}

Status
ConnectionImpl::Connect(const std::string& uri) {
    return client_proxy_->Connect(uri);
}

Status
ConnectionImpl::Connected() const {
    return client_proxy_->Connected();
}

Status
ConnectionImpl::Disconnect() {
    return client_proxy_->Disconnect();
}

Status
ConnectionImpl::CreateCollection(const Mapping& mapping, const std::string& extra_params) {
    return client_proxy_->CreateCollection(mapping, extra_params);
}

Status
ConnectionImpl::DropCollection(const std::string& collection_name) {
    return client_proxy_->DropCollection(collection_name);
}

bool
ConnectionImpl::HasCollection(const std::string& collection_name) {
    return client_proxy_->HasCollection(collection_name);
}

Status
ConnectionImpl::ListCollections(std::vector<std::string>& collection_array) {
    return client_proxy_->ListCollections(collection_array);
}

Status
ConnectionImpl::GetCollectionInfo(const std::string& collection_name, Mapping& mapping) {
    return client_proxy_->GetCollectionInfo(collection_name, mapping);
}

Status
ConnectionImpl::GetCollectionStats(const std::string& collection_name, std::string& collection_stats) {
    return client_proxy_->GetCollectionStats(collection_name, collection_stats);
}
Status
ConnectionImpl::CountEntities(const std::string& collection_name, int64_t& row_count) {
    return client_proxy_->CountEntities(collection_name, row_count);
}

Status
ConnectionImpl::CreatePartition(const PartitionParam& partition_param) {
    return client_proxy_->CreatePartition(partition_param);
}

Status
ConnectionImpl::DropPartition(const PartitionParam& partition_param) {
    return client_proxy_->DropPartition(partition_param);
}

bool
ConnectionImpl::HasPartition(const std::string& collection_name, const std::string& partition_tag) const {
    return client_proxy_->HasPartition(collection_name, partition_tag);
}

Status
ConnectionImpl::ListPartitions(const std::string& collection_name, PartitionTagList& partition_array) const {
    return client_proxy_->ListPartitions(collection_name, partition_array);
}

Status
ConnectionImpl::CreateIndex(const IndexParam& index_param) {
    return client_proxy_->CreateIndex(index_param);
}

Status
ConnectionImpl::DropIndex(const std::string& collection_name, const std::string& field_name,
                          const std::string& index_name) const {
    return client_proxy_->DropIndex(collection_name, field_name, index_name);
}

Status
ConnectionImpl::Insert(const std::string& collection_name, const std::string& partition_tag,
                       const FieldValue& entity_array, std::vector<int64_t>& id_array) {
    return client_proxy_->Insert(collection_name, partition_tag, entity_array, id_array);
}

Status
ConnectionImpl::GetEntityByID(const std::string& collection_name, const std::vector<int64_t>& id_array,
                              std::string& entities) {
    return client_proxy_->GetEntityByID(collection_name, id_array, entities);
}

Status
ConnectionImpl::DeleteEntityByID(const std::string& collection_name, const std::vector<int64_t>& id_array) {
    return client_proxy_->DeleteEntityByID(collection_name, id_array);
}

Status
ConnectionImpl::Search(const std::string& collection_name, const std::vector<std::string>& partition_list,
                       const std::string& dsl, const VectorParam& vector_param, TopKQueryResult& query_result) {
    return client_proxy_->Search(collection_name, partition_list, dsl, vector_param, query_result);
}

Status
ConnectionImpl::SearchPB(const std::string& collection_name, const std::vector<std::string>& partition_list,
                         milvus::BooleanQueryPtr& boolean_query, const std::string& extra_params,
                         milvus::TopKQueryResult& query_result) {
}

Status
ConnectionImpl::ListIDInSegment(const std::string& collection_name, const int64_t& segment_id,
                                std::vector<int64_t>& id_array) {
    return client_proxy_->ListIDInSegment(collection_name, segment_id, id_array);
}

Status
ConnectionImpl::LoadCollection(const std::string& collection_name) const {
    return client_proxy_->LoadCollection(collection_name);
}

Status
ConnectionImpl::Flush(const std::vector<std::string>& collection_name_array) {
    return client_proxy_->Flush(collection_name_array);
}

Status
ConnectionImpl::Compact(const std::string& collection_name, const double& threshold) {
    return client_proxy_->Compact(collection_name, threshold);
}

}  // namespace milvus
