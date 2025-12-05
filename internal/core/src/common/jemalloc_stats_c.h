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

#ifdef __cplusplus
extern "C" {
#endif

// Jemalloc memory statistics
typedef struct {
    uint64_t allocated;  // Actually used memory by the application
    uint64_t resident;   // Physical memory held by the process (RSS)
    uint64_t cached;     // Cached/unreturned memory (resident - allocated)
    int available;       // 1 if jemalloc stats are available, 0 otherwise
} JemallocStats;

// Get jemalloc memory statistics
// Returns a JemallocStats structure with memory information
JemallocStats
GetJemallocStats();

#ifdef __cplusplus
}
#endif
