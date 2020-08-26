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

#include "db/merge/MergeManagerImpl.h"
#include "db/merge/MergeSimpleStrategy.h"
#include "db/merge/MergeTask.h"
#include "db/snapshot/Snapshots.h"
#include "utils/Exception.h"
#include "utils/Log.h"

#include <map>

namespace milvus {
namespace engine {

MergeManagerImpl::MergeManagerImpl(const DBOptions& options) : options_(options) {
}

Status
MergeManagerImpl::CreateStrategy(MergeStrategyType type, MergeStrategyPtr& strategy) {
    switch (type) {
        case MergeStrategyType::SIMPLE: {
            strategy = std::make_shared<MergeSimpleStrategy>();
            break;
        }
        case MergeStrategyType::LAYERED:
        case MergeStrategyType::ADAPTIVE:
        default: {
            std::string msg = "Unsupported merge strategy type: " + std::to_string(static_cast<int32_t>(type));
            LOG_ENGINE_ERROR_ << msg;
            return Status(DB_ERROR, msg);
        }
    }

    return Status::OK();
}

Status
MergeManagerImpl::MergeFiles(int64_t collection_id, MergeStrategyType type) {
    MergeStrategyPtr strategy;
    auto status = CreateStrategy(type, strategy);
    if (!status.ok()) {
        return status;
    }

    while (true) {
        snapshot::ScopedSnapshotT latest_ss;
        STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(latest_ss, collection_id));

        // collect all segments
        Partition2SegmentsMap part2seg;
        auto& segments = latest_ss->GetResources<snapshot::Segment>();
        for (auto& kv : segments) {
            part2seg[kv.second->GetPartitionId()].push_back(kv.second->GetID());
        }

        // distribute segments to groups by some strategy
        SegmentGroups segment_groups;
        auto status = strategy->RegroupSegments(latest_ss, part2seg, segment_groups);
        if (!status.ok()) {
            LOG_ENGINE_ERROR_ << "Failed to regroup segments for collection: " << latest_ss->GetName()
                              << ", continue to merge all files into one";
            return status;
        }

        // no segment to merge, exit
        if (segment_groups.empty()) {
            break;
        }

        // do merge
        for (auto& segments : segment_groups) {
            MergeTask task(options_, latest_ss, segments);
            task.Execute();
        }
    }

    return Status::OK();
}

}  // namespace engine
}  // namespace milvus
