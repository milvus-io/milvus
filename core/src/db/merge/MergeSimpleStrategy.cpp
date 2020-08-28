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
#include "utils/Log.h"

namespace milvus {
namespace engine {

Status
MergeSimpleStrategy::RegroupSegments(const Partition2SegmentsMap& part2segment, int64_t row_per_segment,
                                     SegmentGroups& groups) {
    for (auto& kv : part2segment) {
        if (kv.second.size() <= 1) {
            continue;  // no segment or only one segment, no need to merge
        }

        snapshot::IDS_TYPE ids;
        int64_t row_count_sum = 0;
        for (const SegmentInfo& segment_info : kv.second) {
            if (segment_info.row_count_ <= 0 || segment_info.row_count_ >= row_per_segment) {
                continue;  // empty segment or full segment
            }

            ids.push_back(segment_info.id_);
            row_count_sum += segment_info.row_count_;
            if (row_count_sum >= row_per_segment) {
                if (ids.size() >= 2) {
                    groups.push_back(ids);
                }
                ids.clear();
                row_count_sum = 0;
                continue;
            }
        }

        if (ids.size() >= 2) {
            groups.push_back(ids);
        }
    }

    return Status::OK();
}

}  // namespace engine
}  // namespace milvus
