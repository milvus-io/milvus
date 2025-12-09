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

#pragma once

#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

// JemallocStats contains comprehensive jemalloc memory statistics
// All sizes are in bytes
typedef struct {
    // Core memory metrics
    uint64_t allocated;  // Total bytes allocated by the application
    uint64_t active;     // Total bytes in active pages (includes fragmentation)
    uint64_t metadata;   // Total bytes dedicated to jemalloc metadata
    uint64_t resident;   // Total bytes in physically resident data pages (RSS)
    uint64_t mapped;     // Total bytes in virtual memory mappings
    uint64_t
        retained;  // Total bytes in retained virtual memory (could be returned to OS)

    // Derived metrics for convenience
    uint64_t fragmentation;  // Internal fragmentation (active - allocated)
    uint64_t overhead;       // Memory overhead (resident - active)

    // Status flags
    bool success;  // Whether stats were successfully retrieved
} JemallocStats;

JemallocStats
GetJemallocStats();

#ifdef __cplusplus
}
#endif
