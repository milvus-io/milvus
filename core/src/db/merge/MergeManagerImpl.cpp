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
#include "db/SnapshotUtils.h"
#include "db/SnapshotVisitor.h"
#include "db/merge/MergeLayerStrategy.h"
#include "db/merge/MergeSimpleStrategy.h"
#include "db/merge/MergeTask.h"
#include "db/snapshot/Snapshots.h"
#include "utils/Exception.h"
#include "utils/Log.h"

#include <map>
#include <utility>

namespace milvus {
namespace engine {

namespace {

void
PrintMergeResult(const Partition2SegmentsMap& part2seg, const SegmentGroups& groups) {
    static int64_t s_merge_times = 0;
    static int64_t s_merge_rows = 0;
    std::string msg;
    for (auto& group : groups) {
        msg += "[";

        std::string temp_str;
        for (auto& id : group) {
            for (auto& kv : part2seg) {
                for (auto& info : kv.second) {
                    if (info.id_ == id) {
                        if (!temp_str.empty()) {
                            temp_str += ",";
                        }
                        temp_str += std::to_string(info.row_count_);
                        s_merge_rows += info.row_count_;
                    }
                }
            }
        }
        msg += temp_str;
        msg += "]; ";
    }
    LOG_ENGINE_INFO_ << "Merge strategy results: " << msg;
    s_merge_times++;
    LOG_ENGINE_INFO_ << "Merge strategy times: " << s_merge_times;
    LOG_ENGINE_INFO_ << "Merge strategy rows: " << s_merge_rows;
}
}  // namespace

MergeManagerImpl::MergeManagerImpl(const DBOptions& options) : options_(options) {
}

Status
MergeManagerImpl::CreateStrategy(MergeStrategyType type, MergeStrategyPtr& strategy) {
    switch (type) {
        case MergeStrategyType::SIMPLE: {
            strategy = std::make_shared<MergeSimpleStrategy>();
            break;
        }
        case MergeStrategyType::LAYERED: {
            strategy = std::make_shared<MergeLayerStrategy>();
            break;
        }
        default: {
            std::string msg = "Unsupported merge strategy type: " + std::to_string(static_cast<int32_t>(type));
            LOG_ENGINE_ERROR_ << msg;
            return Status(DB_ERROR, msg);
        }
    }

    return Status::OK();
}

Status
MergeManagerImpl::MergeSegments(int64_t collection_id, MergeStrategyType type) {
    MergeStrategyPtr strategy;
    auto status = CreateStrategy(type, strategy);
    if (!status.ok()) {
        return status;
    }

    while (true) {
        snapshot::ScopedSnapshotT latest_ss;
        STATUS_CHECK(snapshot::Snapshots::GetInstance().GetSnapshot(latest_ss, collection_id));

        snapshot::IDS_TYPE segment_ids;
        SnapshotVisitor ss_visitor(latest_ss);
        ss_visitor.SegmentsToMerge(segment_ids);
        if (segment_ids.empty()) {
            break;  // nothing to merge
        }

        // collect segments info
        Partition2SegmentsMap part2seg;
        for (auto& segment_id : segment_ids) {
            auto segment_commit = latest_ss->GetSegmentCommitBySegmentId(segment_id);
            if (segment_commit == nullptr) {
                continue;  // maybe stale
            }

            SegmentInfo info(segment_id, segment_commit->GetRowCount(), segment_commit->GetCreatedTime());
            part2seg[segment_commit->GetPartitionId()].emplace_back(info);
        }

        if (part2seg.empty()) {
            break;  // nothing to merge
        }

        // get row count per segment
        auto collection = latest_ss->GetCollection();
        int64_t row_count_per_segment = 0;
        GetSegmentRowCount(collection, row_count_per_segment);

        // distribute segments to groups by some strategy
        SegmentGroups segment_groups;
        auto status = strategy->RegroupSegments(part2seg, row_count_per_segment, segment_groups);
        if (!status.ok()) {
            LOG_ENGINE_ERROR_ << "Failed to regroup segments for collection: " << latest_ss->GetName()
                              << ", continue to merge all files into one";
            return status;
        }

        // no segment to merge, exit
        if (segment_groups.empty()) {
            break;
        }

#if 0
        // print merge statistic
        PrintMergeResult(part2seg, segment_groups);
#endif

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
