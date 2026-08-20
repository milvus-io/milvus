// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#pragma once

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <functional>
#include <numeric>
#include <utility>
#include <vector>

#include "cachinglayer/Translator.h"

namespace milvus::segcore::storagev2translator {

// Number of row groups (parquet row groups) merged into one cache cell,
// for now it is a constant.
// hierarchy: 1 group chunk <-> 1 cache cell <-> kRowGroupsPerCell row groups
constexpr size_t kRowGroupsPerCell = 4;
static_assert(kRowGroupsPerCell > 0,
              "kRowGroupsPerCell must be greater than 0");

inline std::pair<milvus::cachinglayer::ResourceUsage,
                 milvus::cachinglayer::ResourceUsage>
EstimateGroupChunkLoadingUsage(
    const std::vector<int64_t>& cell_sizes,
    const std::vector<milvus::cachinglayer::cid_t>& cids,
    bool use_mmap,
    size_t worker_count,
    int64_t batch_target_bytes) {
    if (cids.empty()) {
        return {};
    }

    std::vector<int64_t> requested_sizes;
    requested_sizes.reserve(cids.size());
    for (const auto cid : cids) {
        assert(cid < cell_sizes.size());
        requested_sizes.push_back(cell_sizes[cid]);
    }

    const auto loaded_size =
        std::accumulate(requested_sizes.begin(), requested_sizes.end(), 0LL);
    // All requested cells remain loaded. A load-pool worker owns at most one
    // batch at a time, including both Arrow reads and finalization. Bound the
    // temporary memory by one batch target per worker, while allowing a single
    // oversized cell to exceed the target.
    const auto concurrent_batches =
        std::min(requested_sizes.size(), std::max<size_t>(worker_count, 1));
    std::partial_sort(requested_sizes.begin(),
                      requested_sizes.begin() + concurrent_batches,
                      requested_sizes.end(),
                      std::greater<int64_t>());
    const auto batch_target = std::max<int64_t>(batch_target_bytes, 1);
    int64_t temporary_size = 0;
    for (size_t i = 0; i < concurrent_batches; ++i) {
        const auto batch_size = std::max(batch_target, requested_sizes[i]);
        if (batch_size >= loaded_size - temporary_size) {
            temporary_size = loaded_size;
            break;
        }
        temporary_size += batch_size;
    }

    if (use_mmap) {
        return {{0, loaded_size}, {temporary_size, loaded_size}};
    }
    return {{loaded_size, 0}, {loaded_size + temporary_size, 0}};
}

struct GroupCTMeta : public milvus::cachinglayer::Meta {
    // num_rows_until_chunk_[i] = total rows(prefix sum) in cells [0, i-1]
    // the size of num_rows_until_chunk_ is num_cells + 1
    std::vector<int64_t> num_rows_until_chunk_;
    // memory size for each group chunk(cache cell)
    std::vector<int64_t> chunk_memory_size_;
    size_t num_fields_;
    // total number of row groups
    size_t total_row_groups_;
    // Per-cell [start, end) row group range (global indices).
    // Cells never span files — each cell's row groups come from the same file.
    std::vector<std::pair<size_t, size_t>> cell_row_group_ranges_;

    GroupCTMeta(size_t num_fields,
                milvus::cachinglayer::StorageType storage_type,
                milvus::cachinglayer::CellIdMappingMode cell_id_mapping_mode,
                milvus::cachinglayer::CellDataType cell_data_type,
                CacheWarmupPolicy cache_warmup_policy,
                bool support_eviction)
        : milvus::cachinglayer::Meta(storage_type,
                                     cell_id_mapping_mode,
                                     cell_data_type,
                                     cache_warmup_policy,
                                     support_eviction),
          num_fields_(num_fields),
          total_row_groups_(0) {
    }

    // Get the range of row groups for a cell [start, end)
    std::pair<size_t, size_t>
    get_row_group_range(size_t cid) const {
        return cell_row_group_ranges_[cid];
    }
};

}  // namespace milvus::segcore::storagev2translator
