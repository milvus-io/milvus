// Copyright 2025 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "monitor/jemalloc_stats_c.h"

#ifdef MILVUS_JEMALLOC_ENABLED
#include <jemalloc/jemalloc.h>
#include <cstddef>
#include <cstring>
#endif

JemallocStats
GetJemallocStats() {
    JemallocStats stats;
    // initialize all fields to zero
#ifdef MILVUS_JEMALLOC_ENABLED
    std::memset(&stats, 0, sizeof(JemallocStats));
#else
    stats.allocated = 0;
    stats.active = 0;
    stats.metadata = 0;
    stats.resident = 0;
    stats.mapped = 0;
    stats.retained = 0;
    stats.fragmentation = 0;
    stats.overhead = 0;
#endif
    stats.success = false;

#ifdef MILVUS_JEMALLOC_ENABLED
    // refresh jemalloc stats epoch to get current values
    uint64_t epoch = 1;
    size_t epoch_sz = sizeof(epoch);
    if (mallctl("epoch", &epoch, &epoch_sz, &epoch, epoch_sz) != 0) {
        return stats;
    }

    size_t sz = sizeof(size_t);
    size_t allocated = 0;
    size_t active = 0;
    size_t metadata = 0;
    size_t resident = 0;
    size_t mapped = 0;
    size_t retained = 0;

    // get allocated bytes (total allocated by application)
    if (mallctl("stats.allocated", &allocated, &sz, nullptr, 0) != 0) {
        return stats;
    }

    // get active bytes (in active pages, includes fragmentation)
    if (mallctl("stats.active", &active, &sz, nullptr, 0) != 0) {
        return stats;
    }

    // get metadata bytes (jemalloc's own overhead)
    if (mallctl("stats.metadata", &metadata, &sz, nullptr, 0) != 0) {
        return stats;
    }

    // get resident bytes (physically resident memory, RSS)
    if (mallctl("stats.resident", &resident, &sz, nullptr, 0) != 0) {
        return stats;
    }

    // get mapped bytes (total virtual memory mappings)
    if (mallctl("stats.mapped", &mapped, &sz, nullptr, 0) != 0) {
        return stats;
    }

    // get retained bytes (virtual memory that could be returned to OS)
    if (mallctl("stats.retained", &retained, &sz, nullptr, 0) != 0) {
        return stats;
    }

    // store core metrics
    stats.allocated = static_cast<uint64_t>(allocated);
    stats.active = static_cast<uint64_t>(active);
    stats.metadata = static_cast<uint64_t>(metadata);
    stats.resident = static_cast<uint64_t>(resident);
    stats.mapped = static_cast<uint64_t>(mapped);
    stats.retained = static_cast<uint64_t>(retained);

    // fragmentation: wasted space within allocated pages
    stats.fragmentation = (active > allocated) ? (active - allocated) : 0;

    // overhead: additional memory for page alignment and metadata
    stats.overhead = (resident > active) ? (resident - active) : 0;

    stats.success = true;
#else
    // on platforms without jemalloc (e.g., macOS), return empty stats
    // with success = false to indicate jemalloc is not available
#endif

    return stats;
}
