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

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>

// JemallocStats contains key jemalloc memory statistics
typedef struct JemallocStats {
    // Total number of bytes allocated by the application
    uint64_t allocated;
    // Total number of bytes in active pages allocated by the application
    uint64_t active;
    // Total number of bytes dedicated to metadata
    uint64_t metadata;
    // Total number of bytes in physically resident data pages mapped by the allocator
    uint64_t resident;
    // Total number of bytes in virtual memory mappings
    uint64_t mapped;
    // Total number of bytes in retained virtual memory mappings
    uint64_t retained;
    // Whether stats were successfully retrieved
    bool success;
} JemallocStats;

// GetJemallocStats retrieves current jemalloc statistics
// Returns a JemallocStats struct with the success field indicating if retrieval succeeded
JemallocStats
GetJemallocStats();

#ifdef __cplusplus
}
#endif
