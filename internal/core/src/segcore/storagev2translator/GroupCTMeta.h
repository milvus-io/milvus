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
#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "cachinglayer/Translator.h"
#include "index/FieldChunkMetricsProvider.h"
#include "log/Log.h"
#include "segcore/CacheMetricAttribution.h"

namespace milvus::index {
class FieldChunkMetrics;
}  // namespace milvus::index

namespace milvus::segcore::storagev2translator {

// Positional, deep-owning per-cell skip metrics. For Storage V2, the loader
// keeps row groups and cells 1:1 whenever this map is non-empty, so index i is
// both row group i and cache cell i. A missing/unsupported statistic is
// represented by NoneFieldChunkMetrics and therefore always fails open.
using SkipMetricsList = milvus::SkipMetricsList;
using SkipMetricsByField = std::map<int64_t, SkipMetricsList>;

// Target average byte size per storage-v2 cache cell. Parquet row groups
// are packed into cells so that `rgs_per_cell * avg_row_group_size ≈ target`.
// Runtime-configurable via SetCellTargetSizeBytes (wired from paramtable
// `queryNode.segcore.storageV2.cellTargetSizeBytes`).
inline std::atomic<int64_t>&
cell_target_size_bytes_atomic() {
    static std::atomic<int64_t> instance{4LL * 1024 * 1024};  // init: 4 MiB
    return instance;
}

inline int64_t
GetCellTargetSizeBytes() {
    return cell_target_size_bytes_atomic().load(std::memory_order_acquire);
}

inline void
SetCellTargetSizeBytes(int64_t v) {
    if (v <= 0) {
        return;  // ignore invalid
    }
    cell_target_size_bytes_atomic().store(v, std::memory_order_release);
}

// Derive the number of row groups per cell from a byte-size target so
// that each cell holds >= 1 row group and the average cell size is
// close to cell_target_size_bytes.
// Templated so it accepts both std::vector<int64_t> (built locally by
// GroupChunkTranslator) and std::vector<uint64_t>/size_t (returned by
// milvus-storage's chunk_reader->get_chunk_estimated_size()).
template <typename T>
inline size_t
ComputeRowGroupsPerCell(const std::vector<T>& row_group_sizes,
                        int64_t cell_target_size_bytes) {
    static_assert(std::is_arithmetic_v<T>,
                  "ComputeRowGroupsPerCell expects a vector of numeric sizes");
    if (row_group_sizes.empty()) {
        return 1;
    }
    int64_t total = 0;
    for (auto s : row_group_sizes) {
        total += static_cast<int64_t>(s);
    }
    int64_t avg = total / static_cast<int64_t>(row_group_sizes.size());
    if (avg <= 0) {
        return 1;
    }
    size_t n = static_cast<size_t>(cell_target_size_bytes / avg);
    return std::max<size_t>(n, 1);
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
                bool support_eviction,
                std::string shard = "")
        : milvus::cachinglayer::Meta(
              storage_type,
              cell_id_mapping_mode,
              cell_data_type,
              cache_warmup_policy,
              support_eviction,
              std::nullopt,
              milvus::segcore::MetricAttributionFromShard(std::move(shard))),
          num_fields_(num_fields),
          total_row_groups_(0) {
    }

    // Get the range of row groups for a cell [start, end)
    std::pair<size_t, size_t>
    get_row_group_range(size_t cid) const {
        return cell_row_group_ranges_[cid];
    }

    // The only way to attach footer-derived skip metrics, so that every
    // producer inherits the positional rule instead of restating it.
    //
    // Publication is positional: entry i of a field's list must describe cache
    // cell i. A list whose length does not match the group's cell count is
    // dropped for that field rather than partially applied -- a partial vector
    // would associate bounds with the wrong cell and prune away a matching row.
    // Storage V2's GroupChunkTranslator is the only producer today; a Storage
    // V3 / Vortex footer adapter plugs in here and gets the same guarantee.
    void
    InstallSkipMetrics(SkipMetricsByField metrics,
                       size_t num_cells,
                       const std::string& debug_key) {
        for (auto it = metrics.begin(); it != metrics.end();) {
            if (it->second.size() != num_cells) {
                LOG_WARN(
                    "skip metrics/cell count mismatch for translator {}, field "
                    "{}: {} metrics vs {} cells; disabling footer pruning for "
                    "this field",
                    debug_key,
                    it->first,
                    it->second.size(),
                    num_cells);
                it = metrics.erase(it);
            } else {
                ++it;
            }
        }
        skip_metrics_by_field_ = std::move(metrics);
    }

    // One field's whole aligned list, or nullptr when it kept none. This is
    // the single map lookup a resolved FieldSkipMetricsView performs; every
    // cell-level check afterwards is a plain index into the returned vector.
    const SkipMetricsList*
    FindSkipMetricsList(int64_t field_id) const {
        auto it = skip_metrics_by_field_.find(field_id);
        return it == skip_metrics_by_field_.end() ? nullptr : &it->second;
    }

    // Positional lookup for one field's cell. Returns nullptr when the field
    // has no usable metrics, which every caller must treat as fail-open.
    const index::FieldChunkMetrics*
    FindSkipMetric(int64_t field_id, int64_t chunk_id) const {
        const auto* list = FindSkipMetricsList(field_id);
        if (list == nullptr || chunk_id < 0 ||
            static_cast<size_t>(chunk_id) >= list->size()) {
            return nullptr;
        }
        return (*list)[chunk_id].get();
    }

    // Whether the field kept a full, aligned metric list after installation.
    bool
    HasSkipMetrics(int64_t field_id) const {
        return skip_metrics_by_field_.count(field_id) > 0;
    }

 private:
    // Footer-derived metrics belong to the same immutable column-group
    // generation as the row-group/cell mapping they describe. Keeping them
    // here avoids a second segment-level field->metrics lifecycle. Private so
    // the alignment rule in InstallSkipMetrics cannot be bypassed.
    SkipMetricsByField skip_metrics_by_field_;
};

}  // namespace milvus::segcore::storagev2translator
