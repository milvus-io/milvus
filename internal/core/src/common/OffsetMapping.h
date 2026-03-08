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

#include <cstdint>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

namespace milvus {

// Bidirectional offset mapping for nullable vector storage
// Maps between logical offsets (with nulls) and physical offsets (only valid data)
// Supports two storage modes:
// - vec mode: uses vector for both L2P and P2L, efficient when valid ratio >= 10%
// - map mode: uses unordered_map for L2P, efficient when valid ratio < 10%
class OffsetMapping {
 public:
    OffsetMapping() = default;

    // Build mapping from valid_data (bool array format)
    // If use_vec is not specified, auto-select based on valid ratio (< 10% uses map)
    void
    Build(const bool* valid_data,
          int64_t total_count,
          int64_t start_logical = 0,
          int64_t start_physical = 0);

    // Build mapping incrementally (always uses vec mode for incremental builds)
    void
    BuildIncremental(const bool* valid_data,
                     int64_t count,
                     int64_t start_logical,
                     int64_t start_physical);

    // Get physical offset from logical offset. Returns -1 if null.
    int64_t
    GetPhysicalOffset(int64_t logical_offset) const;

    // Get logical offset from physical offset. Returns -1 if not found.
    int64_t
    GetLogicalOffset(int64_t physical_offset) const;

    // Check if a logical offset is valid (not null)
    bool
    IsValid(int64_t logical_offset) const;

    // Get count of valid (non-null) elements
    int64_t
    GetValidCount() const;

    // Check if mapping is enabled
    bool
    IsEnabled() const;

    // Get next physical offset (for incremental builds)
    int64_t
    GetNextPhysicalOffset() const;

    // Get total logical count (including nulls)
    int64_t
    GetTotalCount() const;

 private:
    bool enabled_{false};
    bool use_map_{false};  // true: use map for L2P, false: use vec

    // Vec mode storage (uses int32_t to save memory)
    std::vector<int32_t> l2p_vec_;  // logical -> physical, -1 means null
    std::vector<int32_t> p2l_vec_;  // physical -> logical

    // Map mode storage (for sparse valid data)
    std::unordered_map<int32_t, int32_t> l2p_map_;  // logical -> physical
    std::unordered_map<int32_t, int32_t> p2l_map_;  // physical -> logical

    int64_t valid_count_{0};
    int64_t total_count_{0};  // total logical count (including nulls)
    mutable std::shared_mutex mutex_;
};

}  // namespace milvus
