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

#include "db/merge/MergeAdaptiveStrategy.h"
#include "utils/Log.h"

#include <algorithm>
#include <map>
#include <utility>

namespace milvus {
namespace engine {

namespace {

// this method is to get all possible combinations of a number list
// for example, source = [1, 2, 3, 4]
// the result will be: [1,2],[1,3],[1,4],[2,3],[2,4],[3,4],[1,2,3],[1,2,4],[1,3,4],[2,3,4],[1,2,3,4]
template <typename T>
void
GetCombination(const std::vector<T>& source, size_t m, std::vector<std::vector<T>>& result) {
    if (source.empty() || m > source.size()) {
        return;
    }

    std::vector<int8_t> bitset;
    bitset.resize(source.size());
    for (int8_t i = 0; i < m; ++i) {
        bitset[i] = 1;
    }

    do {
        std::vector<T> new_combination;
        for (size_t i = 0; i < bitset.size(); ++i) {
            if (bitset[i]) {
                new_combination.push_back(source[i]);
            }
        }
        result.push_back(new_combination);
    } while (std::prev_permutation(bitset.begin(), bitset.end()));
}

template <typename T>
void
GetAllCombination(const std::vector<T>& source, std::vector<std::vector<T>>& result) {
    size_t m = source.size();
    for (size_t i = 2; i <= m; ++i) {
        GetCombination(source, i, result);
    }
}

using ID2Segment = std::map<snapshot::ID_TYPE, SegmentInfo>;
using RowSum2Segments = std::map<int64_t, snapshot::IDS_TYPE>;

void
AllPossibleRowSum(const SegmentInfoList& info_list, int64_t row_per_segment, RowSum2Segments& row_sum_ge,
                  RowSum2Segments& row_sum_lt) {
    row_sum_ge.clear();
    row_sum_lt.clear();

    snapshot::IDS_TYPE ids;
    ID2Segment id2segment;
    for (const SegmentInfo& segment_info : info_list) {
        if (segment_info.row_count_ <= 0 || segment_info.row_count_ >= row_per_segment) {
            continue;  // empty segment or full segment
        }

        ids.push_back((segment_info.id_));
        id2segment.insert(std::make_pair(segment_info.id_, segment_info));
    }

    std::vector<std::vector<snapshot::ID_TYPE>> combinations;
    GetAllCombination<snapshot::ID_TYPE>(ids, combinations);

    for (auto& ids : combinations) {
        int64_t sum = 0;
        snapshot::IDS_TYPE temp_ids;
        for (auto& id : ids) {
            auto iter = id2segment.find(id);
            if (iter == id2segment.end()) {
                sum = 0;
                break;
            }

            sum += iter->second.row_count_;
            temp_ids.push_back(id);
        }

        if (sum == 0) {
            continue;
        }

        int64_t gap = sum - row_per_segment;
        if (gap >= 0) {
            row_sum_ge.insert(std::make_pair(sum, temp_ids));
        } else {
            row_sum_lt.insert(std::make_pair(sum, temp_ids));
        }
    }
}

// this method is to get all possible combinations and calculate each combination row sum,
// compare row sum to get a best one
// fox example, there are 3 segments in info_list: [id=1, row=20000], [id=2, row=90000], [id=3, row=50000]
// assume row_per_segment = 100000
// then there will be 4 merge combinations: (id: 1,2), (id: 2,3), (id: 1,3), (id: 1,2,3)
// the row sum of the 4 combinations is: 110000, 140000, 70000, 160000
// the 110000 is closest to 100000, so (id: 1,2) is the best group
//
// another case, there are 3 segments in info_list: [id=1, row=20000], [id=2, row=30000], [id=3, row=40000]
// then there will be 4 merge combinations: (id: 1,2), (id: 2,3), (id: 1,3), (id: 1,2,3)
// the row sum of the 4 combinations is: 50000, 70000, 60000, 90000
// the 90000 is closest to 100000, so (id: 1,2,3) is the best group
void
GetBestGroup(const SegmentInfoList& info_list, int64_t row_per_segment, snapshot::IDS_TYPE& best_group) {
    best_group.clear();
    RowSum2Segments row_sum_ge, row_sum_lt;
    AllPossibleRowSum(info_list, row_per_segment, row_sum_ge, row_sum_lt);

    if (row_sum_ge.empty()) {
        if (!row_sum_lt.empty()) {
            best_group = row_sum_lt.rbegin()->second;
        }
    } else {
        best_group = row_sum_ge.begin()->second;
    }
}

// this method is to distrbute segment into reasonable groups
// fox example, there are 4 segments in info_list:
// [id=1, row=20000], [id=2, row=90000], [id=3, row=50000], [id=4, row=5000]
// assume row_per_segment = 100000
// they are distrbuted to 2 groups: (id: 1, 2), (id: 3, 4)
// the row sum of the 2 groups is: 110000, 55000
void
GetAdaptiveGroups(const SegmentInfoList& info_list, int64_t row_per_segment, SegmentGroups& groups) {
    // this is a copy since the info_list is const, and the infos will be changed later
    SegmentInfoList infos = info_list;

    snapshot::IDS_TYPE best_group;
    GetBestGroup(infos, row_per_segment, best_group);

    while (!best_group.empty()) {
        groups.emplace_back(best_group);

        for (auto iter = infos.begin(); iter != infos.end();) {
            auto found = std::find(best_group.begin(), best_group.end(), (*iter).id_);
            if (found != best_group.end()) {
                iter = infos.erase(iter);
            } else {
                ++iter;
            }
        }

        if (infos.size() <= 1) {
            break;
        }

        GetBestGroup(infos, row_per_segment, best_group);
    }
}

}  // namespace

Status
MergeAdaptiveStrategy::RegroupSegments(const Partition2SegmentsMap& part2segment, int64_t row_per_segment,
                                       SegmentGroups& groups) {
    for (auto& kv : part2segment) {
        if (kv.second.size() <= 1) {
            continue;  // no segment or only one segment, no need to merge
        }

        GetAdaptiveGroups(kv.second, row_per_segment, groups);
    }

    return Status::OK();
}

}  // namespace engine
}  // namespace milvus
