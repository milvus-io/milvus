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

#include "common/OffsetMapping.h"

#include <algorithm>

namespace milvus {

bool
OffsetMapping::IsValid(int64_t logical_offset) const {
    return GetPhysicalOffset(logical_offset) >= 0;
}

int64_t
NoOpOffsetMapping::GetPhysicalOffset(int64_t logical_offset) const {
    return logical_offset;
}

int64_t
NoOpOffsetMapping::GetLogicalOffset(int64_t physical_offset) const {
    return physical_offset;
}

int64_t
NoOpOffsetMapping::GetValidCount() const {
    return 0;
}

int64_t
NoOpOffsetMapping::ValidCountBelow(int64_t logical_bound) const {
    // No mapping: physical space == logical space.
    return std::max<int64_t>(0, logical_bound);
}

bool
NoOpOffsetMapping::IsEnabled() const {
    return false;
}

int64_t
NoOpOffsetMapping::GetTotalCount() const {
    return 0;
}

OffsetMappingIdView
NoOpOffsetMapping::GetPhysicalToLogicalIds(int64_t physical_offset,
                                           int64_t count) const {
    (void)physical_offset;
    (void)count;
    return {};
}

void
NoOpOffsetMapping::FilterValidLogicalOffsets(
    const int64_t* logical_offsets,
    int64_t count,
    bool* valid_data,
    std::vector<int64_t>& physical_offsets) const {
    physical_offsets.clear();
    physical_offsets.reserve(count);
    for (int64_t i = 0; i < count; ++i) {
        const bool valid = logical_offsets[i] >= 0;
        valid_data[i] = valid;
        if (valid) {
            physical_offsets.push_back(logical_offsets[i]);
        }
    }
}

}  // namespace milvus
