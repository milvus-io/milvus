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

#include "common/SealedOffsetMapping.h"

#include <algorithm>
#include <filesystem>
#include <limits>

namespace milvus {

namespace {

constexpr const char* O2I_MMAP_FILE = "o2i";
constexpr const char* I2O_MMAP_FILE = "i2o";

std::string
GetMmapFilePath(const std::string& mmap_dir_path, const char* filename) {
    return (std::filesystem::path(mmap_dir_path) / filename).string();
}

}  // namespace

void
SealedOffsetMapping::Build(const bool* valid_data,
                           int64_t total_count,
                           const OffsetMappingBuildOptions& options) {
    constexpr int64_t start_logical = 0;
    constexpr int64_t start_physical = 0;

    // Reset FIRST so a degenerate rebuild (null / empty input) leaves a
    // disabled empty mapping, as the header contract states, instead of
    // silently retaining the previous build.
    enabled_ = false;
    use_i2o_map_ = false;
    use_o2i_map_ = false;
    total_count_ = 0;
    valid_count_ = 0;
    l2p_vec_.Clear();
    p2l_vec_.Clear();
    l2p_map_.clear();
    p2l_map_.clear();

    if (total_count == 0 || valid_data == nullptr) {
        return;
    }

    enabled_ = true;
    total_count_ = start_logical + total_count;

    int64_t valid_count = 0;
    for (int64_t i = 0; i < total_count; ++i) {
        if (valid_data[i]) {
            ++valid_count;
        }
    }

    const bool use_sparse_mapping = valid_count * 10 < total_count;
    use_i2o_map_ = !options.enable_mmap_i2o_map && use_sparse_mapping;
    use_o2i_map_ = !options.enable_mmap_o2i_map && use_sparse_mapping;

    const int64_t required_size = start_logical + total_count;
    const int64_t required_p2l_size = start_physical + valid_count;

    const bool need_o2i_mmap =
        !use_o2i_map_ && options.enable_mmap_o2i_map && required_size > 0;
    const bool need_i2o_mmap =
        !use_i2o_map_ && options.enable_mmap_i2o_map && required_p2l_size > 0;
    if (need_i2o_mmap || need_o2i_mmap) {
        AssertInfo(!options.mmap_dir_path.empty(),
                   "offset mapping mmap dir path is empty");
    }

    if (!use_o2i_map_) {
        l2p_vec_.Resize(required_size,
                        -1,
                        options.enable_mmap_o2i_map,
                        need_o2i_mmap ? GetMmapFilePath(options.mmap_dir_path,
                                                        O2I_MMAP_FILE)
                                      : "");
    }

    if (!use_i2o_map_) {
        p2l_vec_.Resize(required_p2l_size,
                        -1,
                        options.enable_mmap_i2o_map,
                        need_i2o_mmap ? GetMmapFilePath(options.mmap_dir_path,
                                                        I2O_MMAP_FILE)
                                      : "");
    }

    int64_t physical_idx = start_physical;
    for (int64_t i = 0; i < total_count; ++i) {
        if (valid_data[i]) {
            if (use_o2i_map_) {
                l2p_map_[start_logical + i] = physical_idx;
            } else {
                l2p_vec_[start_logical + i] = physical_idx;
            }
            if (use_i2o_map_) {
                p2l_map_[physical_idx] = start_logical + i;
            } else {
                p2l_vec_[physical_idx] = start_logical + i;
            }
            ++physical_idx;
        }
    }

    valid_count_ = valid_count;
}

int64_t
SealedOffsetMapping::GetPhysicalOffset(int64_t logical_offset) const {
    return GetPhysicalOffsetInternal(logical_offset);
}

int64_t
SealedOffsetMapping::GetPhysicalOffsetInternal(int64_t logical_offset) const {
    if (!enabled_) {
        return logical_offset;
    }
    if (logical_offset < 0 || logical_offset >= total_count_) {
        return -1;
    }
    if (use_o2i_map_) {
        auto it = l2p_map_.find(static_cast<int32_t>(logical_offset));
        return it == l2p_map_.end() ? -1 : it->second;
    }
    return logical_offset < static_cast<int64_t>(l2p_vec_.size())
               ? l2p_vec_[logical_offset]
               : -1;
}

int64_t
SealedOffsetMapping::GetLogicalOffset(int64_t physical_offset) const {
    return GetLogicalOffsetInternal(physical_offset);
}

int64_t
SealedOffsetMapping::GetLogicalOffsetInternal(int64_t physical_offset) const {
    if (!enabled_) {
        return physical_offset;
    }
    if (physical_offset < 0 || physical_offset >= valid_count_) {
        return -1;
    }
    if (use_i2o_map_) {
        auto it = p2l_map_.find(static_cast<int32_t>(physical_offset));
        return it == p2l_map_.end() ? -1 : it->second;
    }
    return physical_offset < static_cast<int64_t>(p2l_vec_.size())
               ? p2l_vec_[physical_offset]
               : -1;
}

int64_t
SealedOffsetMapping::GetValidCount() const {
    return valid_count_;
}

int64_t
SealedOffsetMapping::ValidCountBelow(int64_t logical_bound) const {
    if (!enabled_) {
        return std::max<int64_t>(0, logical_bound);
    }
    if (logical_bound <= 0) {
        return 0;
    }
    if (logical_bound >= total_count_) {
        return valid_count_;
    }
    // physical -> logical is built in ascending logical order, so it is
    // strictly increasing and binary-searchable in either representation.
    int64_t lo = 0;
    int64_t hi = valid_count_;
    while (lo < hi) {
        const int64_t mid = lo + (hi - lo) / 2;
        int64_t logical;
        if (use_i2o_map_) {
            auto it = p2l_map_.find(static_cast<int32_t>(mid));
            logical = (it == p2l_map_.end())
                          ? std::numeric_limits<int64_t>::max()
                          : static_cast<int64_t>(it->second);
        } else {
            logical = p2l_vec_[mid];
        }
        if (logical < logical_bound) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return lo;
}

bool
SealedOffsetMapping::IsEnabled() const {
    return enabled_;
}

bool
SealedOffsetMapping::IsMmap() const {
    return l2p_vec_.IsMmap() || p2l_vec_.IsMmap();
}

int64_t
SealedOffsetMapping::GetTotalCount() const {
    return total_count_;
}

OffsetMapping::BitsetTransformStatus
SealedOffsetMapping::TransformBitset(const BitsetView& bitset,
                                     TargetBitmap& result) const {
    if (!enabled_) {
        result.clear();
        return BitsetTransformStatus::NoFilter;
    }
    BitsetTransformStatus status;
    if (ShouldSkipBitsetTransform(bitset, total_count_, result, status)) {
        return status;
    }

    result.resize(valid_count_, true);
    if (use_i2o_map_) {
        for (int64_t physical_idx = 0; physical_idx < valid_count_;
             ++physical_idx) {
            auto it = p2l_map_.find(static_cast<int32_t>(physical_idx));
            if (it != p2l_map_.end() &&
                it->second < static_cast<int64_t>(bitset.size())) {
                result[physical_idx] = bitset.test(it->second);
            }
        }
    } else {
        for (int64_t physical_idx = 0; physical_idx < valid_count_;
             ++physical_idx) {
            auto logical_idx = p2l_vec_[physical_idx];
            if (logical_idx >= 0 &&
                logical_idx < static_cast<int64_t>(bitset.size())) {
                result[physical_idx] = bitset.test(logical_idx);
            }
        }
    }
    return BitsetTransformStatus::Transformed;
}

void
SealedOffsetMapping::TransformOffsets(std::vector<int64_t>& offsets) const {
    if (!enabled_) {
        return;
    }
    for (auto& offset : offsets) {
        if (offset >= 0) {
            offset = GetLogicalOffsetInternal(offset);
        }
    }
}

void
SealedOffsetMapping::TransformLogicalOffsets(
    std::vector<int64_t>& offsets) const {
    if (!enabled_) {
        return;
    }
    for (auto& offset : offsets) {
        if (offset >= 0) {
            offset = GetPhysicalOffsetInternal(offset);
        }
    }
}

void
SealedOffsetMapping::FilterValidLogicalOffsets(
    const int64_t* logical_offsets,
    int64_t count,
    bool* valid_data,
    std::vector<int64_t>& physical_offsets) const {
    physical_offsets.clear();
    physical_offsets.reserve(count);
    for (int64_t i = 0; i < count; ++i) {
        const auto physical_offset =
            GetPhysicalOffsetInternal(logical_offsets[i]);
        const bool valid = physical_offset >= 0;
        valid_data[i] = valid;
        if (valid) {
            physical_offsets.push_back(physical_offset);
        }
    }
}

}  // namespace milvus
