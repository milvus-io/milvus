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

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "db/Options.h"
#include "db/SimpleWaitNotify.h"
#include "db/SnapshotHandlers.h"
#include "db/insert/SSMemManager.h"
#include "db/merge/MergeManager.h"
#include "db/snapshot/Context.h"
#include "db/snapshot/ResourceTypes.h"
#include "db/snapshot/Resources.h"
#include "utils/Status.h"

namespace milvus {
namespace engine {

class SSDB {
 public:
    SSDB() = default;

    SSDB(const SSDB&) = delete;

    SSDB&
    operator=(const SSDB&) = delete;

    virtual ~SSDB() = default;

    virtual Status
    Start() = 0;

    virtual Status
    Stop() = 0;

    virtual Status
    CreateCollection(const snapshot::CreateCollectionContext& context) = 0;

    virtual Status
    DropCollection(const std::string& name) = 0;

    virtual Status
    DescribeCollection(
        const std::string& collection_name, snapshot::CollectionPtr& collection,
        std::unordered_map<snapshot::FieldPtr, std::vector<snapshot::FieldElementPtr>>& fields_schema) = 0;

    virtual Status
    HasCollection(const std::string& collection_name, bool& has_or_not) = 0;

    virtual Status
    AllCollections(std::vector<std::string>& names) = 0;

    virtual Status
    GetCollectionRowCount(const std::string& collection_name, uint64_t& row_count) = 0;

    virtual Status
    LoadCollection(const server::ContextPtr& context, const std::string& collection_name,
                   const std::vector<std::string>& field_names, bool force = false) = 0;

    virtual Status
    CreatePartition(const std::string& collection_name, const std::string& partition_name) = 0;

    virtual Status
    DropPartition(const std::string& collection_name, const std::string& partition_name) = 0;

    virtual Status
    ShowPartitions(const std::string& collection_name, std::vector<std::string>& partition_names) = 0;

    virtual Status
    InsertEntities(const std::string& collection_name, const std::string& partition_name, DataChunkPtr& data_chunk) = 0;

    virtual Status
    DeleteEntities(const std::string& collection_name, engine::IDNumbers entity_ids) = 0;

    virtual Status
    Flush(const std::string& collection_name) = 0;

    virtual Status
    Flush() = 0;

    virtual Status
    Compact(const server::ContextPtr& context, const std::string& collection_name, double threshold = 0.0) = 0;

    virtual Status
    GetEntityByID(const std::string& collection_name, const IDNumbers& id_array,
                  const std::vector<std::string>& field_names, DataChunkPtr& data_chunk) = 0;

    virtual Status
    GetEntityIDs(const std::string& collection_id, int64_t segment_id, IDNumbers& entity_ids) = 0;

    virtual Status
    CreateIndex(const server::ContextPtr& context, const std::string& collection_id, const std::string& field_name,
                const CollectionIndex& index) = 0;

    virtual Status
    DescribeIndex(const std::string& collection_id, const std::string& field_name, CollectionIndex& index) = 0;

    virtual Status
    DropIndex(const std::string& collection_name, const std::string& field_name) = 0;

    virtual Status
    DropIndex(const std::string& collection_id) = 0;

    virtual Status
    Query(const server::ContextPtr& context, const query::QueryPtr& query_ptr, engine::QueryResultPtr& result) = 0;
};  // SSDB

using SSDBPtr = std::shared_ptr<SSDB>;

}  // namespace engine
}  // namespace milvus
