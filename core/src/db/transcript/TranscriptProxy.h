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

#include "db/DBProxy.h"
#include "db/transcript/ScriptRecorder.h"

#include <memory>
#include <string>
#include <vector>

namespace milvus {
namespace engine {

class TranscriptProxy : public DBProxy {
 public:
    TranscriptProxy(const DBPtr& db, const DBOptions& options);

    ScriptRecorderPtr
    GetScriptRecorder() const {
        return recorder_;
    }

    Status
    Start() override;

    Status
    Stop() override;

    Status
    CreateCollection(const snapshot::CreateCollectionContext& context) override;

    Status
    DropCollection(const std::string& collection_name) override;

    Status
    HasCollection(const std::string& collection_name, bool& has_or_not) override;

    Status
    ListCollections(std::vector<std::string>& names) override;

    Status
    GetCollectionInfo(const std::string& collection_name, snapshot::CollectionPtr& collection,
                      snapshot::FieldElementMappings& fields_schema) override;

    Status
    GetCollectionStats(const std::string& collection_name, milvus::json& collection_stats) override;

    Status
    CountEntities(const std::string& collection_name, int64_t& row_count) override;

    Status
    CreatePartition(const std::string& collection_name, const std::string& partition_name) override;

    Status
    DropPartition(const std::string& collection_name, const std::string& partition_name) override;

    Status
    HasPartition(const std::string& collection_name, const std::string& partition_tag, bool& exist) override;

    Status
    ListPartitions(const std::string& collection_name, std::vector<std::string>& partition_names) override;

    Status
    CreateIndex(const server::ContextPtr& context, const std::string& collection_name, const std::string& field_name,
                const CollectionIndex& index) override;

    Status
    DropIndex(const std::string& collection_name, const std::string& field_name) override;

    Status
    DescribeIndex(const std::string& collection_name, const std::string& field_name, CollectionIndex& index) override;

    Status
    Insert(const std::string& collection_name, const std::string& partition_name, DataChunkPtr& data_chunk,
           idx_t op_id) override;

    Status
    GetEntityByID(const std::string& collection_name, const IDNumbers& id_array,
                  const std::vector<std::string>& field_names, std::vector<bool>& valid_row,
                  DataChunkPtr& data_chunk) override;

    Status
    DeleteEntityByID(const std::string& collection_name, const engine::IDNumbers& entity_ids, idx_t op_id) override;

    Status
    ListIDInSegment(const std::string& collection_name, int64_t segment_id, IDNumbers& entity_ids) override;

    Status
    Query(const server::ContextPtr& context, const query::QueryPtr& query_ptr, engine::QueryResultPtr& result) override;

    Status
    LoadCollection(const server::ContextPtr& context, const std::string& collection_name,
                   const std::vector<std::string>& field_names, bool force) override;

    Status
    Flush(const std::string& collection_name) override;

    Status
    Flush() override;

    Status
    Compact(const server::ContextPtr& context, const std::string& collection_name, double threshold) override;

 private:
    ScriptRecorderPtr recorder_;
};

}  // namespace engine
}  // namespace milvus
