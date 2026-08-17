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

#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <functional>
#include <limits>
#include <optional>
#include <vector>

#include <roaring/roaring.hh>

#include "common/EasyAssert.h"
#include "common/Types.h"

namespace milvus::index {

inline size_t
RoaringMemoryBytes(const roaring::Roaring& offsets) {
    roaring::api::roaring_statistics_t stats{};
    roaring::api::roaring_bitmap_statistics(&offsets.roaring, &stats);
    const auto container_bytes =
        static_cast<size_t>(stats.n_bytes_array_containers) +
        stats.n_bytes_run_containers + stats.n_bytes_bitset_containers;
    return sizeof(roaring::Roaring) +
           std::max(container_bytes, offsets.getSizeInBytes());
}

inline void
LoadLegacyOffsets(roaring::Roaring& offsets, const uint8_t* data, size_t size) {
    AssertInfo(size % sizeof(size_t) == 0,
               "legacy offset payload size {} is not aligned to size_t",
               size);

    offsets = roaring::Roaring();
    const auto count = size / sizeof(size_t);
    // Legacy payload is a sorted size_t[] on disk; roaring stores uint32_t, so
    // convert once into a dense buffer and bulk-insert with addMany instead of
    // per-element Roaring::add (each add does a container lookup).
    std::vector<uint32_t> buf;
    buf.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        size_t offset;
        std::memcpy(&offset, data + i * sizeof(size_t), sizeof(size_t));
        AssertInfo(offset <= std::numeric_limits<uint32_t>::max(),
                   "row offset {} exceeds uint32 range",
                   offset);
        buf.push_back(static_cast<uint32_t>(offset));
    }
    if (!buf.empty()) {
        offsets.addMany(buf.size(), buf.data());
    }
    offsets.runOptimize();
    offsets.shrinkToFit();
}

inline std::vector<size_t>
RoaringToLegacyOffsets(const roaring::Roaring& offsets) {
    std::vector<size_t> legacy;
    legacy.reserve(static_cast<size_t>(offsets.cardinality()));
    for (auto offset : offsets) {
        legacy.push_back(offset);
    }
    return legacy;
}

inline TargetBitmap
RoaringToBitset(const roaring::Roaring& offsets,
                size_t row_count,
                bool inverted = false) {
    TargetBitmap result(row_count, inverted);
    for (auto offset : offsets) {
        if (offset >= row_count) {
            break;
        }
        result.set(offset, !inverted);
    }
    return result;
}

inline void
ClearRoaringRows(const roaring::Roaring& offsets, TargetBitmap& target) {
    for (auto offset : offsets) {
        if (offset >= target.size()) {
            break;
        }
        target.reset(offset);
    }
}

inline void
apply_hits_with_filter(milvus::TargetBitmap& bitset,
                       const std::function<bool(size_t /* offset */)>& filter) {
    std::optional<size_t> result = bitset.find_first();
    while (result.has_value()) {
        size_t offset = result.value();
        bitset[offset] = filter(offset);
        result = bitset.find_next(offset);
    }
}

inline void
apply_hits_with_callback(
    milvus::TargetBitmap& bitset,
    const std::function<void(size_t /* offset */)>& callback) {
    std::optional<size_t> result = bitset.find_first();
    while (result.has_value()) {
        size_t offset = result.value();
        callback(offset);
        result = bitset.find_next(offset);
    }
}
}  // namespace milvus::index
