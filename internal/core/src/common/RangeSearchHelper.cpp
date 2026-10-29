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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <queue>
#include <utility>
#include <vector>

#include "common/CheckedInputArithmetic.h"
#include "common/EasyAssert.h"
#include "common/RangeSearchHelper.h"
#include "common/Types.h"
#include "common/Utils.h"

namespace milvus {

namespace {
using ResultPair = std::pair<float, int64_t>;

struct ValidatedRangeInput {
    const size_t* lims;
    const int64_t* ids;
    const float* distances;
    size_t nq;
    size_t topk;
    size_t output_count;
};

ValidatedRangeInput
ValidateRangeInput(const DatasetPtr& data_set, int64_t nq, int64_t topk) {
    const auto nq_size = CheckedInputSize(nq, "range-search query count");
    const auto topk_size = CheckedInputSize(topk, "range-search topk");
    if (topk_size == 0) {
        ThrowInfo(ConfigInvalid, "range-search topk must be positive");
    }
    const auto output_count =
        CheckedInputProduct(nq_size, topk_size, "range-search result");
    CheckedInputProduct(output_count, sizeof(int64_t), "range-search id");
    CheckedInputProduct(output_count, sizeof(float), "range-search distance");

    if (data_set == nullptr) {
        ThrowInfo(KnowhereError,
                  "knowhere returned a null range-search dataset");
    }
    if (data_set->GetRows() != nq) {
        ThrowInfo(KnowhereError,
                  "knowhere range-search row count {} disagrees with logical "
                  "query count {}",
                  data_set->GetRows(),
                  nq);
    }

    const auto* lims = GetDatasetLims(data_set);
    if (lims == nullptr) {
        ThrowInfo(KnowhereError, "knowhere range-search result has no limits");
    }
    if (lims[0] != 0) {
        ThrowInfo(KnowhereError,
                  "knowhere range-search limits do not start at zero");
    }
    for (size_t i = 1; i <= nq_size; ++i) {
        if (lims[i] < lims[i - 1]) {
            ThrowInfo(KnowhereError,
                      "knowhere range-search limits are not monotonic");
        }
    }

    const auto hit_count = lims[nq_size];
    CheckedKnowhereBytes(hit_count, sizeof(int64_t), "range-search id");
    CheckedKnowhereBytes(hit_count, sizeof(float), "range-search distance");
    const auto* ids = GetDatasetIDs(data_set);
    const auto* distances = GetDatasetDistance(data_set);
    if (hit_count > 0 && ids == nullptr) {
        ThrowInfo(KnowhereError, "knowhere range-search result has no ids");
    }
    if (hit_count > 0 && distances == nullptr) {
        ThrowInfo(KnowhereError,
                  "knowhere range-search result has no distances");
    }
    return {lims, ids, distances, nq_size, topk_size, output_count};
}

DatasetPtr
PublishOwnedResult(int64_t nq,
                   int64_t topk,
                   std::unique_ptr<int64_t[]> ids,
                   std::unique_ptr<float[]> distances) {
    auto result = std::make_shared<Dataset>();
    // Keep the dataset non-owning while its throwing metadata setters run. If
    // any setter fails, the unique_ptrs free both buffers and the partial
    // dataset does not also delete them.
    result->SetIsOwner(false);
    result->SetRows(nq);
    result->SetDim(topk);
    result->SetIds(ids.get());
    result->SetDistance(distances.get());
    result->SetIsOwner(true);
    ids.release();
    distances.release();
    return result;
}
}  // namespace

/* Sort and return TOPK items as final range search result */
DatasetPtr
ReGenRangeSearchResult(DatasetPtr data_set,
                       int64_t topk,
                       int64_t nq,
                       const std::string& metric_type) {
    /**
     * nq: number of queries;
     * lims: the size of lims is nq + 1, lims[i+1] - lims[i] refers to the size of RangeSearch result queries[i]
     *      for example, the nq is 5. In the selected range,
     *      the size of RangeSearch result for each nq is [1, 2, 3, 4, 5],
     *      the lims will be [0, 1, 3, 6, 10, 15];
     * ids: the size of ids is lim[nq],
     *      {
     *        i(0,0), i(0,1), …, i(0,k0-1),
     *        i(1,0), i(1,1), …, i(1,k1-1),
     *        ... ...
     *        i(n-1,0), i(n-1,1), …, i(n-1,kn-1)
     *      }
     *      i(0,0), i(0,1), …, i(0,k0-1) means the ids of RangeSearch result queries[0], k0 equals lim[1] - lim[0];
     * dist: the size of ids is lim[nq],
     *      {
     *        d(0,0), d(0,1), …, d(0,k0-1),
     *        d(1,0), d(1,1), …, d(1,k1-1),
     *        ... ...
     *        d(n-1,0), d(n-1,1), …, d(n-1,kn-1)
     *      }
     *      d(0,0), d(0,1), …, d(0,k0-1) means the distances of RangeSearch result queries[0], k0 equals lim[1] - lim[0];
     */
    const auto input = ValidateRangeInput(data_set, nq, topk);

    // use p_id and p_dist to GenResultDataset after sorted
    auto p_id = std::make_unique<int64_t[]>(input.output_count);
    auto p_dist = std::make_unique<float[]>(input.output_count);
    std::fill_n(p_id.get(), input.output_count, -1);
    std::fill_n(
        p_dist.get(), input.output_count, std::numeric_limits<float>::max());

    /*
     *   get result for one nq
     *   IP:   1.0        range_filter     radius
     *          |------------+---------------|       min_heap   descending_order
     *                       |___ ___|
     *                           V
     *                          topk
     *
     *   L2:   0.0        range_filter     radius
     *          |------------+---------------|       max_heap   ascending_order
     *                       |___ ___|
     *                           V
     *                          topk
     */
    std::function<bool(const ResultPair&, const ResultPair&)> cmp =
        std::less<>();
    if (PositivelyRelated(metric_type)) {
        cmp = std::greater<>();
    }

    // The subscript of p_id and p_dist
    for (size_t i = 0; i < input.nq; ++i) {
        std::priority_queue<ResultPair, std::vector<ResultPair>, decltype(cmp)>
            pq(cmp);
        const auto capacity =
            std::min(input.lims[i + 1] - input.lims[i], input.topk);

        for (size_t j = input.lims[i]; j < input.lims[i + 1]; ++j) {
            auto curr = ResultPair(input.distances[j], input.ids[j]);
            if (pq.size() < capacity) {
                pq.push(curr);
            } else if (cmp(curr, pq.top())) {
                pq.pop();
                pq.push(curr);
            }
        }

        const auto result_base = i * input.topk;
        for (size_t rank = capacity; rank > 0; --rank) {
            auto& node = pq.top();
            const auto result_offset = result_base + rank - 1;
            p_dist[result_offset] = node.first;
            p_id[result_offset] = node.second;
            pq.pop();
        }
    }
    return PublishOwnedResult(nq, topk, std::move(p_id), std::move(p_dist));
}

void
CheckRangeSearchParam(float radius,
                      float range_filter,
                      const std::string& metric_type) {
    /*
     *   IP:   1.0        range_filter     radius
     *          |------------+---------------|       range_filter > radius
     *   L2:   0.0        range_filter     radius
     *          |------------+---------------|       range_filter < radius
     *
     */
    if (PositivelyRelated(metric_type)) {
        if (!(range_filter > radius)) {
            ThrowInfo(ErrorCode::InvalidParameter,
                      "metric type ({}), range_filter({}) must be greater than "
                      "radius({})",
                      metric_type.c_str(),
                      range_filter,
                      radius);
        }
    } else {
        if (!(range_filter < radius)) {
            ThrowInfo(ErrorCode::InvalidParameter,
                      "metric type ({}), range_filter({}) must be less than "
                      "radius({})",
                      metric_type.c_str(),
                      range_filter,
                      radius);
        }
    }
}

}  // namespace milvus
