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

#include <cstdint>

namespace milvus {

// Virtual PK utilities for external collections.
// Virtual PK format: (segmentID << 32) | offset
// This allows up to 4 billion (2^32) rows per segment.

// Name of the virtual PK field for external collections
constexpr const char* VIRTUAL_PK_FIELD_NAME = "__virtual_pk__";

// Generate a virtual primary key from segment ID and row offset.
// Only the lower 32 bits of segmentID are preserved.
inline int64_t
GetVirtualPK(int64_t segment_id, int64_t offset) {
    return (segment_id << 32) | (offset & 0xFFFFFFFF);
}

// Extract the segment ID from a virtual PK.
// Returns the lower 32 bits that were originally from the segment ID.
inline int64_t
ExtractSegmentIDFromVirtualPK(int64_t virtual_pk) {
    return virtual_pk >> 32;
}

// Extract the row offset from a virtual PK.
inline int64_t
ExtractOffsetFromVirtualPK(int64_t virtual_pk) {
    return virtual_pk & 0xFFFFFFFF;
}

// Check if a virtual PK belongs to the given segment.
// Note: Only compares the lower 32 bits of segment_id.
inline bool
IsVirtualPKFromSegment(int64_t virtual_pk, int64_t segment_id) {
    return ExtractSegmentIDFromVirtualPK(virtual_pk) == (segment_id & 0xFFFFFFFF);
}

// Get the truncated segment ID (lower 32 bits) for comparison with virtual PKs.
inline int64_t
GetTruncatedSegmentID(int64_t segment_id) {
    return segment_id & 0xFFFFFFFF;
}

}  // namespace milvus
