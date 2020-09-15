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

#include "db/DBProxy.h"

namespace milvus {
namespace engine {

#define DB_CHECK             \
    if (db_ == nullptr) {    \
        return Status::OK(); \
    }

DBProxy::DBProxy(const DBPtr& db, const DBOptions& options) : db_(db), options_(options) {
}

Status
DBProxy::Start() {
    DB_CHECK
    return db_->Start();
}

Status
DBProxy::Stop() {
    DB_CHECK
    return db_->Stop();
}

Status
DBProxy::CreateCollection(const snapshot::CreateCollectionContext& context) {
    DB_CHECK
    return db_->CreateCollection(context);
}

Status
DBProxy::DropCollection(const std::string& collection_name) {
    DB_CHECK
    return db_->DropCollection(collection_name);
}

Status
DBProxy::HasCollection(const std::string& collection_name, bool& has_or_not) {
    DB_CHECK
    return db_->HasCollection(collection_name, has_or_not);
}

Status
DBProxy::ListCollections(std::vector<std::string>& names) {
    DB_CHECK
    return db_->ListCollections(names);
}

Status
DBProxy::GetCollectionInfo(const std::string& collection_name, snapshot::CollectionPtr& collection,
                           snapshot::FieldElementMappings& fields_schema) {
    DB_CHECK
    return db_->GetCollectionInfo(collection_name, collection, fields_schema);
}

Status
DBProxy::GetCollectionStats(const std::string& collection_name, milvus::json& collection_stats) {
    DB_CHECK
    return db_->GetCollectionStats(collection_name, collection_stats);
}

Status
DBProxy::CountEntities(const std::string& collection_name, int64_t& row_count) {
    DB_CHECK
    return db_->CountEntities(collection_name, row_count);
}

Status
DBProxy::CreatePartition(const std::string& collection_name, const std::string& partition_name) {
    DB_CHECK
    return db_->CreatePartition(collection_name, partition_name);
}

Status
DBProxy::DropPartition(const std::string& collection_name, const std::string& partition_name) {
    DB_CHECK
    return db_->DropPartition(collection_name, partition_name);
}

Status
DBProxy::HasPartition(const std::string& collection_name, const std::string& partition_tag, bool& exist) {
    DB_CHECK
    return db_->HasPartition(collection_name, partition_tag, exist);
}

Status
DBProxy::ListPartitions(const std::string& collection_name, std::vector<std::string>& partition_names) {
    DB_CHECK
    return db_->ListPartitions(collection_name, partition_names);
}

Status
DBProxy::CreateIndex(const server::ContextPtr& context, const std::string& collection_name,
                     const std::string& field_name, const CollectionIndex& index) {
    DB_CHECK
    return db_->CreateIndex(context, collection_name, field_name, index);
}

Status
DBProxy::DropIndex(const std::string& collection_name, const std::string& field_name) {
    DB_CHECK
    return db_->DropIndex(collection_name, field_name);
}

Status
DBProxy::DescribeIndex(const std::string& collection_name, const std::string& field_name, CollectionIndex& index) {
    DB_CHECK
    return db_->DescribeIndex(collection_name, field_name, index);
}

Status
DBProxy::Insert(const std::string& collection_name, const std::string& partition_name, DataChunkPtr& data_chunk,
                idx_t op_id) {
    DB_CHECK
    return db_->Insert(collection_name, partition_name, data_chunk, op_id);
}

Status
DBProxy::GetEntityByID(const std::string& collection_name, const IDNumbers& id_array,
                       const std::vector<std::string>& field_names, std::vector<bool>& valid_row,
                       DataChunkPtr& data_chunk) {
    DB_CHECK
    return db_->GetEntityByID(collection_name, id_array, field_names, valid_row, data_chunk);
}

Status
DBProxy::DeleteEntityByID(const std::string& collection_name, const engine::IDNumbers& entity_ids, idx_t op_id) {
    DB_CHECK
    return db_->DeleteEntityByID(collection_name, entity_ids, op_id);
}

Status
DBProxy::ListIDInSegment(const std::string& collection_name, int64_t segment_id, IDNumbers& entity_ids) {
    DB_CHECK
    return db_->ListIDInSegment(collection_name, segment_id, entity_ids);
}

Status
DBProxy::Query(const server::ContextPtr& context, const query::QueryPtr& query_ptr, engine::QueryResultPtr& result) {
    DB_CHECK
    return db_->Query(context, query_ptr, result);
}

Status
DBProxy::LoadCollection(const server::ContextPtr& context, const std::string& collection_name,
                        const std::vector<std::string>& field_names, bool force) {
    DB_CHECK
    return db_->LoadCollection(context, collection_name, field_names, force);
}

Status
DBProxy::Flush(const std::string& collection_name) {
    DB_CHECK
    return db_->Flush(collection_name);
}

Status
DBProxy::Flush() {
    DB_CHECK
    return db_->Flush();
}

Status
DBProxy::Compact(const server::ContextPtr& context, const std::string& collection_name, double threshold) {
    DB_CHECK
    return db_->Compact(context, collection_name, threshold);
}

bool
DBProxy::IsBuildingIndex() {
    return db_ != nullptr && db_->IsBuildingIndex();
}

}  // namespace engine
}  // namespace milvus
