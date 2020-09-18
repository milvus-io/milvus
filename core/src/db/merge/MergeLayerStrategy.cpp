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

#include "db/merge/MergeLayerStrategy.h"
#include "db/Utils.h"
#include "utils/Log.h"

#include <map>
#include <utility>

namespace milvus {
namespace engine {

namespace {
const int64_t FORCE_MERGE_THREASHOLD = 30 * 1000 * 1000;  // force merge files older this time(in microsecond)

using LayerGroups = std::map<int64_t, SegmentInfoList>;

void
ConstructLayers(LayerGroups& groups, int64_t row_count_per_segment) {
    groups.clear();

#if 0
    int64_t power = 12;
    while (true) {
        int64_t key = 1UL << power;
        power += 2;
        groups.insert(std::pair(key, SegmentInfoList()));
        if (key >= row_count_per_segment || key >= MAX_SEGMENT_ROW_COUNT) {
            break;
        }
    }
#else
    // construct layers according to row_count_per_segment
    const int64_t ratio = 5;
    groups.insert(std::pair(row_count_per_segment, SegmentInfoList()));
    int64_t quarter = row_count_per_segment / ratio;
    while (quarter > 1024) {
        groups.insert(std::pair(quarter, SegmentInfoList()));
        quarter /= ratio;
    }
#endif
}

void
PrintLayers(LayerGroups& groups) {
    std::string msg;
    for (auto& pair : groups) {
        msg += std::to_string(pair.first);
        msg += ",";
    }
    LOG_ENGINE_INFO_ << "Merge strategy layers: " << msg;
}
}  // namespace

Status
MergeLayerStrategy::RegroupSegments(const Partition2SegmentsMap& part2segment, int64_t row_per_segment,
                                    SegmentGroups& groups) {
    auto now = utils::GetMicroSecTimeStamp();
    for (auto& kv : part2segment) {
        if (kv.second.size() <= 1) {
            continue;  // no segment or only one segment, no need to merge
        }

        LayerGroups layers;
        ConstructLayers(layers, row_per_segment);

        // distribute segments to layers according to segment row count
        SegmentInfoList temp_list = kv.second;
        for (auto iter = temp_list.begin(); iter != temp_list.end();) {
            SegmentInfo& segment_info = *iter;
            if (segment_info.row_count_ <= 0 || segment_info.row_count_ >= row_per_segment) {
                iter = temp_list.erase(iter);
                continue;  // empty segment or full segment
            }

            for (auto& layer_iter : layers) {
                if (segment_info.row_count_ < layer_iter.first) {
                    layer_iter.second.push_back(segment_info);
                    break;
                }
            }

            iter = temp_list.erase(iter);
        }

        // if some segment's create time is 30 seconds ago, and it still un-merged, force merge with upper layer
        SegmentInfoList force_list;
        for (auto& pair : layers) {
            SegmentInfoList& segments = pair.second;
            if (!force_list.empty()) {
                segments.insert(segments.begin(), force_list.begin(), force_list.end());
                force_list.clear();
            }

            if (segments.size() == 1) {
                if (now - segments[0].create_on_ > FORCE_MERGE_THREASHOLD) {
                    force_list.swap(segments);
                }
            }
        }

        // merge for each layer
        for (auto& pair : layers) {
            snapshot::IDS_TYPE ids;
            int64_t row_count_sum = 0;
            SegmentInfoList& segments = pair.second;
            for (auto& segment : segments) {
                ids.push_back(segment.id_);
                row_count_sum += segment.row_count_;
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

#if 0
        // print merge statistic
        if (!groups.empty()) {
            PrintLayers(layers);
        }
#endif
    }

    return Status::OK();
}

}  // namespace engine
}  // namespace milvus
