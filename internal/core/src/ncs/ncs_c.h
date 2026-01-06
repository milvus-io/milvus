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

#include "common/type_c.h"
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

// Initialize the NCS singleton with a specific factory implementation
// This must be called before any NCS operations
// ncsExtras: JSON string containing factory-specific configuration (can be NULL or empty for defaults)
CStatus
initNcsSingleton(const char* ncsKind, const char* ncsExtras);

// Create a bucket with the given bucket ID
// Returns CStatus indicating success or failure
CStatus
createBucket(uint64_t bucketId);

// Delete a bucket with the given bucket ID
// Returns CStatus indicating success or failure
CStatus
deleteBucket(uint64_t bucketId);

// Check if a bucket exists
// Returns true if bucket exists, false otherwise
// The result is returned via the 'exists' output parameter
CStatus
isBucketExist(uint64_t bucketId, bool* exists);

// Get bucket status information
// Note: NcsBucketStatus is currently a placeholder (TBD: capacity, occupancy, etc.)
// This is reserved for future implementation
// CStatus getBucketNcsStatus(uint64_t bucketId, CNcsBucketStatus* status);

#ifdef __cplusplus
}
#endif
