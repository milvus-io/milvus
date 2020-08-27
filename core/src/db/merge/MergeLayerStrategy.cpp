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
#include "utils/Log.h"

#include <map>
#include <utility>

namespace milvus {
namespace engine {

namespace {
using LayerGroups = std::map<int64_t, SegmentsRowList>;

void
ConstructLayers(LayerGroups& groups, int64_t row_count_per_segment) {
    groups.clear();
    int64_t power = 12;
    while (true) {
        int64_t key = 1UL << power;
        power += 2;
        groups.insert(std::pair(key, SegmentsRowList()));
        if (key >= row_count_per_segment || key >= MAX_SEGMENT_ROW_COUNT) {
            break;
        }
    }
}
}  // namespace

Status
MergeLayerStrategy::RegroupSegments(const Partition2SegmentsMap& part2segment, int64_t rwo_per_segment,
                                    SegmentGroups& groups) {
    for (auto& kv : part2segment) {
        if (kv.second.size() <= 1) {
            continue;  // no segment or only one segment, no need to merge
        }

        LayerGroups layers;
        ConstructLayers(layers, rwo_per_segment);

        // distribute segments to layers according to segment row count
        SegmentsRowList temp_list = kv.second;
        for (SegmentsRowList::iterator iter = temp_list.begin(); iter != temp_list.end();) {
            auto segment_row = iter->second;
            if (segment_row <= 0 || segment_row >= rwo_per_segment) {
                iter = temp_list.erase(iter);
                continue;  // empty segment or full segment
            }

            for (auto layer_iter = layers.begin(); layer_iter != layers.end(); ++layer_iter) {
                if (segment_row < layer_iter->first) {
                    layer_iter->second.push_back(std::make_pair(iter->first, iter->second));
                    break;
                }
            }

            iter = temp_list.erase(iter);
        }

        // merge for each layer
        for (auto& pair : layers) {
            snapshot::IDS_TYPE ids;
            int64_t row_count_sum = 0;
            SegmentsRowList& segments = pair.second;
            for (auto& segment : segments) {
                ids.push_back(segment.first);
                row_count_sum += segment.second;
                if (row_count_sum >= rwo_per_segment) {
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
    }

    return Status::OK();
}

}  // namespace engine
}  // namespace milvus
