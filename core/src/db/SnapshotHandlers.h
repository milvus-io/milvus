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

#include "db/Types.h"
#include "db/snapshot/IterateHandler.h"
#include "db/snapshot/Snapshot.h"
#include "segment/Segment.h"
#include "server/context/Context.h"
#include "utils/Log.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace milvus {
namespace engine {

struct SegmentsToSearchCollector : public snapshot::SegmentCommitIterator {
    using ResourceT = snapshot::SegmentCommit;
    using BaseT = snapshot::IterateHandler<ResourceT>;
    SegmentsToSearchCollector(snapshot::ScopedSnapshotT ss, snapshot::IDS_TYPE& segment_ids);

    Status
    Handle(const typename ResourceT::Ptr&) override;

    snapshot::IDS_TYPE& segment_ids_;
};

///////////////////////////////////////////////////////////////////////////////
struct SegmentsToIndexCollector : public snapshot::SegmentCommitIterator {
    using ResourceT = snapshot::SegmentCommit;
    using BaseT = snapshot::IterateHandler<ResourceT>;
    SegmentsToIndexCollector(snapshot::ScopedSnapshotT ss, const std::string& field_name,
                             snapshot::IDS_TYPE& segment_ids, int64_t build_index_threshold);

    Status
    Handle(const typename ResourceT::Ptr&) override;

    std::string field_name_;
    snapshot::IDS_TYPE& segment_ids_;
    int64_t build_index_threshold_;
};

///////////////////////////////////////////////////////////////////////////////
struct SegmentsToMergeCollector : public snapshot::SegmentCommitIterator {
    using ResourceT = snapshot::SegmentCommit;
    using BaseT = snapshot::IterateHandler<ResourceT>;
    SegmentsToMergeCollector(snapshot::ScopedSnapshotT ss, snapshot::IDS_TYPE& segment_ids,
                             int64_t row_count_threshold);

    Status
    Handle(const typename ResourceT::Ptr&) override;

    snapshot::IDS_TYPE& segment_ids_;
    int64_t row_count_threshold_;
};

///////////////////////////////////////////////////////////////////////////////
struct GetEntityByIdSegmentHandler : public snapshot::SegmentIterator {
    using ResourceT = snapshot::Segment;
    using BaseT = snapshot::IterateHandler<ResourceT>;
    GetEntityByIdSegmentHandler(const server::ContextPtr& context, snapshot::ScopedSnapshotT ss,
                                const std::string& dir_root, const IDNumbers& ids,
                                const std::vector<std::string>& field_names, std::vector<bool>& valid_row);

    Status
    Handle(const typename ResourceT::Ptr&) override;

    Status
    PostIterate() override;

    const server::ContextPtr context_;
    const std::string dir_root_;
    const engine::IDNumbers ids_;
    const std::vector<std::string> field_names_;
    engine::DataChunkPtr data_chunk_;
    std::vector<bool>& valid_row_;

 private:
    engine::IDNumbers ids_left_;
    using IDChunkMap = std::unordered_map<idx_t, std::pair<engine::DataChunkPtr, int64_t>>;
    IDChunkMap result_map_;  // record id in which chunk, and its position within the chunk
};

///////////////////////////////////////////////////////////////////////////////
struct LoadCollectionHandler : public snapshot::SegmentIterator {
    using ResourceT = snapshot::Segment;
    using BaseT = snapshot::IterateHandler<ResourceT>;
    LoadCollectionHandler(const server::ContextPtr& context, snapshot::ScopedSnapshotT ss, const std::string& dir_root,
                          const std::vector<std::string>& field_names, bool force);

    Status
    Handle(const typename ResourceT::Ptr&) override;

    const server::ContextPtr context_;
    const std::string dir_root_;
    std::vector<std::string> field_names_;
    bool force_;
};

}  // namespace engine
}  // namespace milvus
