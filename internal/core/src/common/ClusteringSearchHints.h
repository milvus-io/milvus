// Copyright (C) 2019-2026 Zilliz. All rights reserved.
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
#include <cmath>
#include <cstdint>
#include <memory>
#include <vector>
#include "knowhere/comp/search_hint.h"

namespace milvus {
// Owned by one request; the public row domain is validated before use.
struct ClusteringSearchHints {
    int64_t expected_rows;
    knowhere::BatchSearchHints queries;

    bool
    Valid(int64_t rows, int64_t nq) const {
        if (rows <= 0 || rows != expected_rows || nq <= 0 ||
            queries.size() != static_cast<size_t>(nq))
            return false;
        for (const auto& query : queries) {
            if (!query)
                continue;
            for (const auto& hint : query->ranges) {
                if (hint.offset < 0 || hint.count <= 0 || hint.offset > rows ||
                    hint.count > rows - hint.offset ||
                    !std::isfinite(hint.id_distance))
                    return false;
            }
        }
        return true;
    }
};
using ClusteringSearchHintsPtr = std::shared_ptr<const ClusteringSearchHints>;

// All pointers are borrowed only for this call, before async dispatch. Malformed
// optional metadata returns no hints, preserving the ordinary search scope.
inline ClusteringSearchHintsPtr
CopyClusteringSearchHints(int64_t rows,
                          const uint8_t* presence,
                          int64_t nq,
                          const int64_t* boundaries,
                          const int64_t* offsets,
                          const int64_t* counts,
                          const float* distances,
                          int64_t hint_count) {
    if (rows <= 0 || nq <= 0 || !presence || !boundaries || hint_count < 0 ||
        (hint_count && (!offsets || !counts || !distances)) ||
        boundaries[0] != 0 || boundaries[nq] != hint_count)
        return nullptr;
    auto result = std::make_shared<ClusteringSearchHints>();
    result->expected_rows = rows;
    result->queries.resize(nq);
    for (int64_t q = 0; q < nq; ++q) {
        const auto begin = boundaries[q], end = boundaries[q + 1];
        if (presence[q] > 1 || begin < 0 || end < begin || end > hint_count ||
            (!presence[q] && begin != end))
            return nullptr;
        if (!presence[q])
            continue;
        auto& query = result->queries[q].emplace();
        query.ranges.reserve(end - begin);
        for (auto i = begin; i < end; ++i) {
            query.ranges.push_back({offsets[i], counts[i], distances[i]});
        }
    }
    return result->Valid(rows, nq) ? result : nullptr;
}
}  // namespace milvus
