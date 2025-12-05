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

#include "common/jemalloc_stats_c.h"

#ifdef MILVUS_JEMALLOC_ENABLED
#include <jemalloc/jemalloc.h>
#endif

JemallocStats
GetJemallocStats() {
    JemallocStats stats;
    stats.allocated = 0;
    stats.resident = 0;
    stats.cached = 0;
    stats.available = 0;

#ifdef MILVUS_JEMALLOC_ENABLED
    try {
        // Refresh jemalloc stats by advancing the epoch
        // Without this, stats.* returns stale/cached values
        uint64_t epoch = 1;
        size_t epoch_sz = sizeof(epoch);
        mallctl("epoch", &epoch, &epoch_sz, &epoch, epoch_sz);

        size_t allocated_size = 0;
        size_t resident_size = 0;
        size_t size_sz = sizeof(size_t);

        // Get allocated memory (actually used by the application)
        int ret_allocated =
            mallctl("stats.allocated", &allocated_size, &size_sz, NULL, 0);

        // Get resident memory (physical memory held by the process)
        int ret_resident =
            mallctl("stats.resident", &resident_size, &size_sz, NULL, 0);

        if (ret_allocated == 0 && ret_resident == 0) {
            stats.allocated = allocated_size;
            stats.resident = resident_size;
            stats.cached = (resident_size > allocated_size)
                               ? (resident_size - allocated_size)
                               : 0;
            stats.available = 1;
        }
    } catch (...) {
        // If any error occurs, return stats with available = 0
        stats.available = 0;
    }
#endif

    return stats;
}
