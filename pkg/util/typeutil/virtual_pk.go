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

package typeutil

// Virtual primary keys encode an external-table segment's row position into a
// synthetic int64 PK so deletes can be expressed without a real source PK.
//
// Format: (truncated_segmentID << 32) | offset
//
// This is the single source of truth for the Go side. The same layout is
// implemented in C++ at internal/core/src/common/VirtualPK.h; keep the two in
// sync. All Go callers (datanode external refresh, querynode segment loading,
// pkoracle segment-membership checks) MUST route through these helpers instead
// of re-deriving the bit layout.

// GetVirtualPK generates a virtual primary key from segmentID and offset.
// Only the lower 32 bits of segmentID and offset are preserved. Milvus segment
// IDs are TSO-allocated 64-bit values that typically exceed 32 bits, so segment
// truncation is expected. Offsets are constrained by practical segment row-count
// limits; a single segment above 2^32 rows would collide. Use
// IsVirtualPKFromSegment for safe segment comparison.
func GetVirtualPK(segmentID int64, offset int64) int64 {
	return ((segmentID & 0xFFFFFFFF) << 32) | (offset & 0xFFFFFFFF)
}

// ExtractSegmentIDFromVirtualPK extracts the truncated segmentID from a virtual
// PK. Uses unsigned right shift to avoid sign-extension for large segment IDs.
func ExtractSegmentIDFromVirtualPK(virtualPK int64) int64 {
	return int64(uint64(virtualPK) >> 32)
}

// ExtractOffsetFromVirtualPK extracts the row offset from a virtual PK.
func ExtractOffsetFromVirtualPK(virtualPK int64) int64 {
	return virtualPK & 0xFFFFFFFF
}

// IsVirtualPKFromSegment checks if a virtual PK belongs to the given segment.
// Only the lower 32 bits of segmentID are preserved in the virtual PK, so the
// comparison is against the truncated segment ID.
func IsVirtualPKFromSegment(virtualPK int64, segmentID int64) bool {
	return ExtractSegmentIDFromVirtualPK(virtualPK) == (segmentID & 0xFFFFFFFF)
}
