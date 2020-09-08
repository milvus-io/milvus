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

#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_set>
#include <vector>

#include "config/ConfigMgr.h"
#include "db/snapshot/CompoundOperations.h"
#include "db/snapshot/Resources.h"
#include "segment/SegmentWriter.h"
#include "utils/Status.h"

namespace milvus {
namespace engine {

class MemAction {
 public:
    idx_t op_id_ = 0;
    std::unordered_set<idx_t> delete_ids_;
    DataChunkPtr insert_data_;
};

class MemSegment {
 public:
    MemSegment(int64_t collection_id, int64_t partition_id, const DBOptions& options);

    ~MemSegment() = default;

 public:
    Status
    Add(const DataChunkPtr& chunk, idx_t op_id);

    Status
    Delete(const std::vector<idx_t>& ids, idx_t op_id);

    int64_t
    GetCurrentMem() const {
        return current_mem_;
    }

    int64_t
    GetCurrentRowCount() const {
        return total_row_count_;
    }

    Status
    Serialize();

 private:
    Status
    CreateNewSegment(snapshot::ScopedSnapshotT& ss, std::shared_ptr<snapshot::NewSegmentOperation>& operation,
                     segment::SegmentWriterPtr& writer, idx_t max_op_id);

    Status
    ApplyDeleteToMem();

    Status
    PutChunksToWriter(const segment::SegmentWriterPtr& writer);

 private:
    int64_t collection_id_;
    int64_t partition_id_;

    DBOptions options_;
    int64_t current_mem_ = 0;

    using ActionArray = std::vector<MemAction>;
    ActionArray actions_;  // the actions array mekesure insert/delete actions executed one by one

    int64_t total_row_count_ = 0;
};

using MemSegmentPtr = std::shared_ptr<MemSegment>;

}  // namespace engine
}  // namespace milvus
