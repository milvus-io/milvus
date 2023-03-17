// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <queue>
#include <vector>
#include <functional>
#include <iostream>

#include "common/Utils.h"
#include "common/RangeSearchHelper.h"

namespace milvus {
namespace {
using ResultPair = std::pair<float, int64_t>;
}
DatasetPtr
SortRangeSearchResult(DatasetPtr data_set,
                      int64_t topk,
                      int64_t nq,
                      const std::string_view metric_type) {
    /**
     * nq: number of queries;
     * lims: the size of lims is nq + 1, lims[i+1] - lims[i] refers to the size of RangeSearch result queries[i]
     *          for example, the nq is 5. In the selected range,
     *          the size of RangeSearch result for each nq is [1, 2, 3, 4, 5],
     *          the lims will be [0, 1, 3, 6, 10, 15];
     * ids: the size of ids is lim[nq],
     *      { i(0,0), i(0,1), …, i(0,k0-1),
     *        i(1,0), i(1,1), …, i(1,k1-1),
     *          …,
     *        i(n-1,0), i(n-1,1), …, i(n-1,kn-1)},
     *      i(0,0), i(0,1), …, i(0,k0-1) means the ids of RangeSearch result queries[0], k0 equals lim[1] - lim[0];
     * dist: the size of ids is lim[nq],
     *      { d(0,0), d(0,1), …, d(0,k0-1),
     *        d(1,0), d(1,1), …, d(1,k1-1),
     *          …,
     *        d(n-1,0), d(n-1,1), …, d(n-1,kn-1)},
     *      d(0,0), d(0,1), …, d(0,k0-1) means the distances of RangeSearch result queries[0], k0 equals lim[1] - lim[0];
     */
    auto lims = GetDatasetLims(data_set);
    auto id = GetDatasetIDs(data_set);
    auto dist = GetDatasetDistance(data_set);

    // use p_id and p_dist to GenResultDataset after sorted
    auto p_id = new int64_t[topk * nq];
    memset(p_id, -1, sizeof(int64_t) * topk * nq);
    auto p_dist = new float[topk * nq];
    std::fill_n(p_dist, topk * nq, std::numeric_limits<float>::max());

    /*
         *   get result for one nq
         *   IP:   1.0        range_filter     radius
         *          |------------+---------------|       min_heap   descending_order
         *   L2:   0.0        range_filter     radius
         *          |------------+---------------|       max_heap   ascending_order
         *
    */
    std::function<bool(const ResultPair&, const ResultPair&)> cmp =
        std::less<>();
    if (IsMetricType(metric_type, knowhere::metric::IP)) {
        cmp = std::greater<>();
    }
    std::priority_queue<ResultPair, std::vector<ResultPair>, decltype(cmp)>
        sub_result(cmp);

    // The subscript of p_id and p_dist
    int cnt = 0;
    for (int i = 0; i < nq; i++) {
        // if RangeSearch answer size of one nq is less than topk, set the capacity to size
        int size = lims[i + 1] - lims[i];
        int capacity = topk > size ? size : topk;

        for (int j = lims[i]; j < lims[i + 1]; j++) {
            auto current = ResultPair(dist[j], id[j]);
            if (sub_result.size() == capacity) {
                if (cmp(sub_result.top(), current)) {
                    current = sub_result.top();
                }
                sub_result.pop();
            }
            sub_result.push(current);
        }

        for (int i = capacity + cnt - 1; i > cnt - 1; i--) {
            p_dist[i] = sub_result.top().first;
            p_id[i] = sub_result.top().second;
            sub_result.pop();
        }
        cnt += topk;
    }
    return GenResultDataset(nq, topk, p_id, p_dist);
}

void
CheckRangeSearchParam(float radius,
                      float range_filter,
                      const std::string_view metric_type) {
    /*
     *   IP:   1.0        range_filter     radius
     *          |------------+---------------|       min_heap   descending_order
     *   L2:   1.0         radius       range_filter
     *          |------------+---------------|       max_heap   ascending_order
     *
     */
    if (metric_type == knowhere::metric::IP) {
        if (range_filter < radius) {
            PanicInfo("range_filter must more than radius when IP");
        }
    } else {
        if (range_filter > radius) {
            PanicInfo("range_filter must less than radius except IP");
        }
    }
}
}  // namespace milvus
