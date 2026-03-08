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

#include <cstddef>
#include <cstring>

#ifdef __linux__
#include <dlfcn.h>
#endif

// jemalloc mallctl function type
typedef int (*mallctl_t)(const char*, void*, size_t*, void*, size_t);

// get mallctl function pointer at runtime via dlsym
// Returns nullptr if jemalloc is not loaded (e.g., via LD_PRELOAD)
static mallctl_t
get_mallctl() {
#ifdef __linux__
    static mallctl_t fn = (mallctl_t)dlsym(RTLD_DEFAULT, "mallctl");
    return fn;
#else
    return nullptr;
#endif
}

JemallocStats
GetJemallocStats() {
    JemallocStats stats;
    std::memset(&stats, 0, sizeof(JemallocStats));
    stats.success = false;

    mallctl_t mallctl_fn = get_mallctl();
    if (mallctl_fn == nullptr) {
        // jemalloc is not available, return zeros
        return stats;
    }

    // refresh jemalloc stats epoch to get current values
    uint64_t epoch = 1;
    size_t epoch_sz = sizeof(epoch);
    if (mallctl_fn("epoch", &epoch, &epoch_sz, &epoch, epoch_sz) != 0) {
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
    if (mallctl_fn("stats.allocated", &allocated, &sz, nullptr, 0) != 0) {
        return stats;
    }

    // get active bytes (in active pages, includes fragmentation)
    if (mallctl_fn("stats.active", &active, &sz, nullptr, 0) != 0) {
        return stats;
    }

    // get metadata bytes (jemalloc's own overhead)
    if (mallctl_fn("stats.metadata", &metadata, &sz, nullptr, 0) != 0) {
        return stats;
    }

    // get resident bytes (physically resident memory, RSS)
    if (mallctl_fn("stats.resident", &resident, &sz, nullptr, 0) != 0) {
        return stats;
    }

    // get mapped bytes (total virtual memory mappings)
    if (mallctl_fn("stats.mapped", &mapped, &sz, nullptr, 0) != 0) {
        return stats;
    }

    // get retained bytes (virtual memory that could be returned to OS)
    if (mallctl_fn("stats.retained", &retained, &sz, nullptr, 0) != 0) {
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

    return stats;
}
