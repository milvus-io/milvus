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
#include <vector>

#include "config/ConfigMgr.h"
#include "db/insert/VectorSource.h"
#include "db/snapshot/CompoundOperations.h"
#include "db/snapshot/Resources.h"
#include "segment/SegmentWriter.h"
#include "utils/Status.h"

namespace milvus {
namespace engine {

class MemSegment {
 public:
    MemSegment(int64_t collection_id, int64_t partition_id, const DBOptions& options);

    ~MemSegment() = default;

 public:
    Status
    Add(const VectorSourcePtr& source);

    Status
    Delete(std::vector<id_t>& ids);

    int64_t
    GetCurrentMem();

    int64_t
    GetMemLeft();

    bool
    IsFull();

    Status
    Serialize(uint64_t wal_lsn);

    int64_t
    GetSegmentId() const;

 private:
    Status
    CreateSegment();

    Status
    GetSingleEntitySize(int64_t& single_size);

 private:
    int64_t collection_id_;
    int64_t partition_id_;

    std::shared_ptr<snapshot::NewSegmentOperation> operation_;
    snapshot::SegmentPtr segment_;
    DBOptions options_;
    int64_t current_mem_;

    //    ExecutionEnginePtr execution_engine_;
    segment::SegmentWriterPtr segment_writer_ptr_;
};  // SSMemTableFile

using MemSegmentPtr = std::shared_ptr<MemSegment>;

}  // namespace engine
}  // namespace milvus
