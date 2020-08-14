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

#include "db/transcript/Transcript.h"
#include "db/transcript/ScriptCodec.h"
#include "db/transcript/ScriptReplay.h"

#include "utils/Exception.h"

namespace milvus {
namespace engine {

Transcript::Transcript(const DBPtr& db, const std::string& replay_script_path)
    : DBProxy(db), replay_script_path_(replay_script_path) {
    // db must implemented
    if (db == nullptr) {
        throw Exception(DB_ERROR, "null pointer");
    }
}

Status
Transcript::Start() {
    // let service start
    auto status = db_->Start();
    if (!status.ok()) {
        return status;
    }

    // replay script in necessary
    if (!replay_script_path_.empty()) {
        ScriptReplay replay;
        return replay.Replay(db_, replay_script_path_);
    }

    return Status::OK();
}

Status
Transcript::Stop() {
    return db_->Stop();
}

Status
Transcript::CreateCollection(const snapshot::CreateCollectionContext& context) {
    return db_->CreateCollection(context);
}

Status
Transcript::DropCollection(const std::string& name) {
    return db_->DropCollection(name);
}

Status
Transcript::HasCollection(const std::string& collection_name, bool& has_or_not) {
    return db_->HasCollection(collection_name, has_or_not);
}

Status
Transcript::ListCollections(std::vector<std::string>& names) {
    return db_->ListCollections(names);
}

Status
Transcript::GetCollectionInfo(const std::string& collection_name, snapshot::CollectionPtr& collection,
                              snapshot::FieldElementMappings& fields_schema) {
    return db_->GetCollectionInfo(collection_name, collection, fields_schema);
}

Status
Transcript::GetCollectionStats(const std::string& collection_name, milvus::json& collection_stats) {
    return db_->GetCollectionStats(collection_name, collection_stats);
}

Status
Transcript::CountEntities(const std::string& collection_name, int64_t& row_count) {
    return db_->CountEntities(collection_name, row_count);
}

Status
Transcript::CreatePartition(const std::string& collection_name, const std::string& partition_name) {
    return db_->CreatePartition(collection_name, partition_name);
}

Status
Transcript::DropPartition(const std::string& collection_name, const std::string& partition_name) {
    return db_->DropPartition(collection_name, partition_name);
}

Status
Transcript::HasPartition(const std::string& collection_name, const std::string& partition_tag, bool& exist) {
    return db_->HasPartition(collection_name, partition_tag, exist);
}

Status
Transcript::ListPartitions(const std::string& collection_name, std::vector<std::string>& partition_names) {
    return db_->ListPartitions(collection_name, partition_names);
}

Status
Transcript::CreateIndex(const server::ContextPtr& context, const std::string& collection_name,
                        const std::string& field_name, const CollectionIndex& index) {
    return db_->CreateIndex(context, collection_name, field_name, index);
}

Status
Transcript::DropIndex(const std::string& collection_name, const std::string& field_name) {
    return db_->DropIndex(collection_name, field_name);
}

Status
Transcript::DescribeIndex(const std::string& collection_name, const std::string& field_name, CollectionIndex& index) {
    return db_->DescribeIndex(collection_name, field_name, index);
}

Status
Transcript::Insert(const std::string& collection_name, const std::string& partition_name, DataChunkPtr& data_chunk) {
    return db_->Insert(collection_name, partition_name, data_chunk);
}

Status
Transcript::GetEntityByID(const std::string& collection_name, const IDNumbers& id_array,
                          const std::vector<std::string>& field_names, std::vector<bool>& valid_row,
                          DataChunkPtr& data_chunk) {
    return db_->GetEntityByID(collection_name, id_array, field_names, valid_row, data_chunk);
}

Status
Transcript::DeleteEntityByID(const std::string& collection_name, const engine::IDNumbers& entity_ids) {
    return db_->DeleteEntityByID(collection_name, entity_ids);
}

Status
Transcript::ListIDInSegment(const std::string& collection_name, int64_t segment_id, IDNumbers& entity_ids) {
    return db_->ListIDInSegment(collection_name, segment_id, entity_ids);
}

Status
Transcript::Query(const server::ContextPtr& context, const query::QueryPtr& query_ptr, engine::QueryResultPtr& result) {
    return db_->Query(context, query_ptr, result);
}

Status
Transcript::LoadCollection(const server::ContextPtr& context, const std::string& collection_name,
                           const std::vector<std::string>& field_names, bool force) {
    return db_->LoadCollection(context, collection_name, field_names, force);
}

Status
Transcript::Flush(const std::string& collection_name) {
    return db_->Flush(collection_name);
}

Status
Transcript::Flush() {
    return db_->Flush();
}

Status
Transcript::Compact(const server::ContextPtr& context, const std::string& collection_name, double threshold) {
    return db_->Compact(context, collection_name, threshold);
}

}  // namespace engine
}  // namespace milvus
