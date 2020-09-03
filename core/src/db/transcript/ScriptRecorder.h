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

#include "db/DB.h"
#include "db/transcript/ScriptFile.h"

#include <memory>
#include <string>
#include <vector>

namespace milvus {
namespace engine {

class ScriptRecorder {
 public:
    explicit ScriptRecorder(const std::string& path);

    ~ScriptRecorder();

    std::string
    GetScriptPath() const;

    Status
    CreateCollection(const snapshot::CreateCollectionContext& context);

    Status
    DropCollection(const std::string& collection_name);

    Status
    HasCollection(const std::string& collection_name, bool& has_or_not);

    Status
    ListCollections(std::vector<std::string>& names);

    Status
    GetCollectionInfo(const std::string& collection_name, snapshot::CollectionPtr& collection,
                      snapshot::FieldElementMappings& fields_schema);

    Status
    GetCollectionStats(const std::string& collection_name, milvus::json& collection_stats);

    Status
    CountEntities(const std::string& collection_name, int64_t& row_count);

    Status
    CreatePartition(const std::string& collection_name, const std::string& partition_name);

    Status
    DropPartition(const std::string& collection_name, const std::string& partition_name);

    Status
    HasPartition(const std::string& collection_name, const std::string& partition_name, bool& exist);

    Status
    ListPartitions(const std::string& collection_name, std::vector<std::string>& partition_names);

    Status
    CreateIndex(const server::ContextPtr& context, const std::string& collection_name, const std::string& field_name,
                const CollectionIndex& index);

    Status
    DropIndex(const std::string& collection_name, const std::string& field_name);

    Status
    DescribeIndex(const std::string& collection_name, const std::string& field_name, CollectionIndex& index);

    Status
    Insert(const std::string& collection_name, const std::string& partition_name, DataChunkPtr& data_chunk,
           idx_t op_id);

    Status
    GetEntityByID(const std::string& collection_name, const IDNumbers& id_array,
                  const std::vector<std::string>& field_names, std::vector<bool>& valid_row, DataChunkPtr& data_chunk);

    Status
    DeleteEntityByID(const std::string& collection_name, const engine::IDNumbers& entity_ids, idx_t op_id);

    Status
    ListIDInSegment(const std::string& collection_name, int64_t segment_id, IDNumbers& entity_ids);

    Status
    Query(const server::ContextPtr& context, const query::QueryPtr& query_ptr, engine::QueryResultPtr& result);

    Status
    LoadCollection(const server::ContextPtr& context, const std::string& collection_name,
                   const std::vector<std::string>& field_names, bool force);

    Status
    Flush(const std::string& collection_name);

    Status
    Flush();

    Status
    Compact(const server::ContextPtr& context, const std::string& collection_name, double threshold);

 private:
    ScriptFilePtr
    GetFile();

    Status
    WriteJson(milvus::json& json_obj);

 private:
    std::string script_path_;
    ScriptFilePtr file_;
};

using ScriptRecorderPtr = std::shared_ptr<ScriptRecorder>;

}  // namespace engine
}  // namespace milvus
