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

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVirtualPKRoundTrip(t *testing.T) {
	cases := []struct {
		name      string
		segmentID int64
		offset    int64
	}{
		{"small", 1, 0},
		{"typical_offset", 12345, 99},
		{"large_tso_segment_id", 450000000000000001, 42},
		{"max_offset", 7, 0xFFFFFFFF},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			vpk := GetVirtualPK(tc.segmentID, tc.offset)
			// segmentID is truncated to its lower 32 bits on encode.
			assert.Equal(t, tc.segmentID&0xFFFFFFFF, ExtractSegmentIDFromVirtualPK(vpk))
			assert.Equal(t, tc.offset&0xFFFFFFFF, ExtractOffsetFromVirtualPK(vpk))
			assert.True(t, IsVirtualPKFromSegment(vpk, tc.segmentID))
			assert.False(t, IsVirtualPKFromSegment(vpk, tc.segmentID+1))
		})
	}
}

func TestExtractSegmentIDFromVirtualPKNoSignExtension(t *testing.T) {
	// A segment ID whose lower 32 bits have the high bit set must not
	// sign-extend on extraction.
	segmentID := int64(0x00000000_FFFFFFFF)
	vpk := GetVirtualPK(segmentID, 1)
	assert.Equal(t, int64(0xFFFFFFFF), ExtractSegmentIDFromVirtualPK(vpk))
	assert.GreaterOrEqual(t, ExtractSegmentIDFromVirtualPK(vpk), int64(0))
}
