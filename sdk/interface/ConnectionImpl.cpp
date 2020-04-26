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

std::string
ConnectionImpl::ClientVersion() const {
    return client_proxy_->ClientVersion();
}

std::string
ConnectionImpl::ServerVersion() const {
    return client_proxy_->ServerVersion();
}

std::string
ConnectionImpl::ServerStatus() const {
    return client_proxy_->ServerStatus();
}

Status
ConnectionImpl::GetConfig(const std::string& node_name, std::string& value) const {
    return client_proxy_->GetConfig(node_name, value);
}

Status
ConnectionImpl::SetConfig(const std::string& node_name, const std::string& value) const {
    return client_proxy_->SetConfig(node_name, value);
}

Status
ConnectionImpl::CreateCollection(const CollectionParam& param) {
    return client_proxy_->CreateCollection(param);
}

bool
ConnectionImpl::HasCollection(const std::string& collection_name) {
    return client_proxy_->HasCollection(collection_name);
}

Status
ConnectionImpl::DropCollection(const std::string& collection_name) {
    return client_proxy_->DropCollection(collection_name);
}

Status
ConnectionImpl::CreateIndex(const IndexParam& index_param) {
    return client_proxy_->CreateIndex(index_param);
}

Status
ConnectionImpl::Insert(const std::string& collection_name, const std::string& partition_tag,
                       const std::vector<Entity>& entity_array, std::vector<int64_t>& id_array) {
    return client_proxy_->Insert(collection_name, partition_tag, entity_array, id_array);
}

Status
ConnectionImpl::GetEntityByID(const std::string& collection_name, int64_t entity_id, Entity& entity_data) {
    return client_proxy_->GetEntityByID(collection_name, entity_id, entity_data);
}

Status
ConnectionImpl::GetEntitiesByID(const std::string& collection_name,
                                const std::vector<int64_t>& id_array,
                                std::vector<Entity>& entities_data) {
    return client_proxy_->GetEntitiesByID(collection_name, id_array, entities_data);
}

Status
ConnectionImpl::GetIDsInSegment(const std::string& collection_name, const std::string& segment_name,
                                std::vector<int64_t>& id_array) {
    return client_proxy_->GetIDsInSegment(collection_name, segment_name, id_array);
}

Status
ConnectionImpl::Search(const std::string& collection_name, const PartitionTagList& partition_tag_array,
                       const std::vector<Entity>& entity_array, int64_t topk, const std::string& extra_params,
                       TopKQueryResult& topk_query_result) {
    return client_proxy_->Search(collection_name,
                                 partition_tag_array,
                                 entity_array,
                                 topk,
                                 extra_params,
                                 topk_query_result);
}

Status
ConnectionImpl::SearchByID(const std::string& collection_name, const PartitionTagList& partition_tag_array,
                           const std::vector<int64_t>& id_array, int64_t topk,
                           const std::string& extra_params, TopKQueryResult& topk_query_result) {
    return client_proxy_->SearchByID(collection_name,
                                     partition_tag_array,
                                     id_array,
                                     topk,
                                     extra_params,
                                     topk_query_result);
}

Status
ConnectionImpl::DescribeCollection(const std::string& collection_name, CollectionParam& collection_schema) {
    return client_proxy_->DescribeCollection(collection_name, collection_schema);
}

Status
ConnectionImpl::CountCollection(const std::string& collection_name, int64_t& row_count) {
    return client_proxy_->CountCollection(collection_name, row_count);
}

Status
ConnectionImpl::ShowCollections(std::vector<std::string>& collection_array) {
    return client_proxy_->ShowCollections(collection_array);
}

Status
ConnectionImpl::ShowCollectionInfo(const std::string& collection_name, std::string& collection_info) {
    return client_proxy_->ShowCollectionInfo(collection_name, collection_info);
}

Status
ConnectionImpl::DeleteByID(const std::string& collection_name, const std::vector<int64_t>& id_array) {
    return client_proxy_->DeleteByID(collection_name, id_array);
}

Status
ConnectionImpl::PreloadCollection(const std::string& collection_name) const {
    return client_proxy_->PreloadCollection(collection_name);
}

Status
ConnectionImpl::DescribeIndex(const std::string& collection_name, IndexParam& index_param) const {
    return client_proxy_->DescribeIndex(collection_name, index_param);
}

Status
ConnectionImpl::DropIndex(const std::string& collection_name) const {
    return client_proxy_->DropIndex(collection_name);
}

Status
ConnectionImpl::CreatePartition(const PartitionParam& partition_param) {
    return client_proxy_->CreatePartition(partition_param);
}

Status
ConnectionImpl::ShowPartitions(const std::string& collection_name, PartitionTagList& partition_array) const {
    return client_proxy_->ShowPartitions(collection_name, partition_array);
}

Status
ConnectionImpl::DropPartition(const PartitionParam& partition_param) {
    return client_proxy_->DropPartition(partition_param);
}

Status
ConnectionImpl::FlushCollection(const std::string& Status) {
    return client_proxy_->FlushCollection(Status);
}

Status
ConnectionImpl::Flush() {
    return client_proxy_->Flush();
}

Status
ConnectionImpl::CompactCollection(const std::string& collection_name) {
    return client_proxy_->CompactCollection(collection_name);
}

/*******************************New Interface**********************************/

Status
ConnectionImpl::CreateHybridCollection(const HMapping& mapping) {
    return client_proxy_->CreateHybridCollection(mapping);
}

Status
ConnectionImpl::InsertEntity(const std::string& collection_name,
                             const std::string& partition_tag,
                             HEntity& entities,
                             std::vector<uint64_t>& id_array) {
    return client_proxy_->InsertEntity(collection_name, partition_tag, entities, id_array);
}

Status
ConnectionImpl::HybridSearch(const std::string& collection_name,
                             const std::vector<std::string>& partition_list,
                             BooleanQueryPtr& boolean_query,
                             const std::string& extra_params,
                             TopKQueryResult& topk_query_result) {
    return client_proxy_->HybridSearch(collection_name, partition_list, boolean_query, extra_params, topk_query_result);
}

}  // namespace milvus
