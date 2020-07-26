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

#include "db/merge/MergeSimpleStrategy.h"
#include "db/snapshot/Snapshots.h"
#include "utils/Log.h"

namespace milvus {
namespace engine {

const char* ROW_COUNT_PER_SEGMENT = "row_count_per_segment";

Status
MergeSimpleStrategy::RegroupSegments(const snapshot::ScopedSnapshotT& ss, const Partition2SegmentsMap& part2segment,
                                     SegmentGroups& groups) {
    auto collection = ss->GetCollection();

    int64_t row_count_per_segment = DEFAULT_ROW_COUNT_PER_SEGMENT;
    const json params = collection->GetParams();
    if (params.find(ROW_COUNT_PER_SEGMENT) != params.end()) {
        row_count_per_segment = params[ROW_COUNT_PER_SEGMENT];
    }

    for (auto& kv : part2segment) {
        snapshot::IDS_TYPE ids;
        int64_t row_count_sum = 0;
        for (auto& id : kv.second) {
            auto segment_commit = ss->GetSegmentCommitBySegmentId(id);
            if (segment_commit == nullptr) {
                continue;  // maybe stale
            }

            ids.push_back(id);
            row_count_sum += segment_commit->GetRowCount();
            if (row_count_sum >= row_count_per_segment) {
                if (ids.size() >= 2) {
                    groups.push_back(ids);
                }
                ids.clear();
                row_count_sum = 0;
                continue;
            }
        }

        if (!ids.empty()) {
            groups.push_back(ids);
        }
    }

    return Status::OK();
}

}  // namespace engine
}  // namespace milvus
