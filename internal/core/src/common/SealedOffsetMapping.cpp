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

#include "common/EasyAssert.h"

namespace milvus {

void
SealedOffsetMapping::Build(const bool* valid_data, int64_t total_count) {
    constexpr int64_t start_logical = 0;
    constexpr int64_t start_physical = 0;

    // Reset FIRST so a degenerate rebuild (null / empty input) leaves a
    // disabled empty mapping, as the header contract states, instead of
    // silently retaining the previous build.
    enabled_ = false;
    total_count_ = 0;
    valid_count_ = 0;
    l2p_vec_.clear();
    p2l_vec_.clear();

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

    const int64_t required_size = start_logical + total_count;
    const int64_t required_p2l_size = start_physical + valid_count;

    l2p_vec_.assign(static_cast<size_t>(required_size), -1);
    p2l_vec_.assign(static_cast<size_t>(required_p2l_size), -1);

    int64_t physical_idx = start_physical;
    for (int64_t i = 0; i < total_count; ++i) {
        if (valid_data[i]) {
            l2p_vec_[start_logical + i] = physical_idx;
            p2l_vec_[physical_idx] = start_logical + i;
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
    // strictly increasing and binary-searchable.
    int64_t lo = 0;
    int64_t hi = valid_count_;
    while (lo < hi) {
        const int64_t mid = lo + (hi - lo) / 2;
        const int64_t logical = p2l_vec_[mid];
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

int64_t
SealedOffsetMapping::GetTotalCount() const {
    return total_count_;
}

OffsetMappingIdView
SealedOffsetMapping::GetPhysicalToLogicalIds(int64_t physical_offset,
                                             int64_t count) const {
    if (!enabled_ || count <= 0) {
        return {};
    }
    AssertInfo(physical_offset >= 0,
               "physical offset {} is negative",
               physical_offset);
    AssertInfo(physical_offset <= valid_count_ - count,
               "physical id range [{}, {}) exceeds valid count {}",
               physical_offset,
               physical_offset + count,
               valid_count_);
    return {p2l_vec_.data() + static_cast<size_t>(physical_offset), count};
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
