// Copyright (C) 2019-2024 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include "jemalloc_stats_c.h"

#include <jemalloc/jemalloc.h>
#include <cstddef>

JemallocStats
GetJemallocStats() {
    JemallocStats stats = {};

    // Refresh jemalloc stats epoch to get current values
    // Without this, stats may be stale/cached
    uint64_t epoch = 1;
    size_t epoch_sz = sizeof(epoch);
    mallctl("epoch", &epoch, &epoch_sz, &epoch, epoch_sz);

    size_t sz = sizeof(size_t);
    size_t allocated = 0;
    size_t active = 0;
    size_t metadata = 0;
    size_t resident = 0;
    size_t mapped = 0;
    size_t retained = 0;

    // Get allocated bytes (total allocated by application)
    if (mallctl("stats.allocated", &allocated, &sz, nullptr, 0) != 0) {
        return stats;
    }

    // Get active bytes (in active pages)
    if (mallctl("stats.active", &active, &sz, nullptr, 0) != 0) {
        return stats;
    }

    // Get metadata bytes
    if (mallctl("stats.metadata", &metadata, &sz, nullptr, 0) != 0) {
        return stats;
    }

    // Get resident bytes (physically resident)
    if (mallctl("stats.resident", &resident, &sz, nullptr, 0) != 0) {
        return stats;
    }

    // Get mapped bytes (virtual memory mappings)
    if (mallctl("stats.mapped", &mapped, &sz, nullptr, 0) != 0) {
        return stats;
    }

    // Get retained bytes (retained virtual memory)
    if (mallctl("stats.retained", &retained, &sz, nullptr, 0) != 0) {
        return stats;
    }

    stats.allocated = static_cast<uint64_t>(allocated);
    stats.active = static_cast<uint64_t>(active);
    stats.metadata = static_cast<uint64_t>(metadata);
    stats.resident = static_cast<uint64_t>(resident);
    stats.mapped = static_cast<uint64_t>(mapped);
    stats.retained = static_cast<uint64_t>(retained);
    stats.success = true;

    return stats;
}
