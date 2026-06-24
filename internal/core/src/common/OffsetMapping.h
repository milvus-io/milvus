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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <shared_mutex>

namespace milvus {

struct OffsetMappingP2LBuffer;

class OffsetMappingSnapshot final {
 public:
    OffsetMappingSnapshot() = default;

    bool
    IsEnabled() const;

    int64_t
    GetValidCount() const;

    int64_t
    GetTotalCount() const;

    int64_t
    GetVisiblePhysicalCount(int64_t logical_count) const;

    int64_t
    GetPhysicalOffset(int64_t logical_offset) const;

    int64_t
    GetLogicalOffset(int64_t physical_offset) const;

    bool
    IsValid(int64_t logical_offset) const;

    const int32_t*
    GetLogicalOffsets(int64_t physical_offset, size_t count) const;

 private:
    friend class OffsetMapping;

    OffsetMappingSnapshot(std::shared_ptr<const OffsetMappingP2LBuffer> p2l,
                          bool enabled,
                          int64_t valid_count,
                          int64_t total_count);

    std::shared_ptr<const OffsetMappingP2LBuffer> p2l_;
    bool enabled_{false};
    int64_t valid_count_{0};
    int64_t total_count_{0};
};

struct OffsetMappingAppendResult {
    int64_t physical_offset = 0;
    int64_t valid_count = 0;
};

class OffsetMapping final {
 public:
    OffsetMapping() = default;

    OffsetMappingAppendResult
    Append(const bool* valid_data, int64_t count, int64_t start_logical = -1);

    OffsetMappingSnapshot
    GetSnapshot() const;

 private:
    mutable std::shared_mutex mutex_;
    std::shared_ptr<OffsetMappingP2LBuffer> p2l_;

    bool enabled_{false};
    int64_t valid_count_{0};
    int64_t total_count_{0};
};

}  // namespace milvus
