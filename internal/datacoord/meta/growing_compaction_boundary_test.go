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

package meta

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestGrowingCompactionBoundaryUsesCertifiedBirth(t *testing.T) {
	for _, tc := range []struct {
		name                string
		start, cursor, want uint64
		streaming           bool
	}{
		{"streaming birth before first pack", 0, 200, 200, true},
		{"legacy unknown remains conservative", 0, 0, 0, true},
		{"nonstreaming cursor is not a birth certificate", 0, 200, 0, false},
		{"preserve published first insert position", 150, 200, 150, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			m, err := NewMemoryMeta(t)
			require.NoError(t, err)
			segment := &datapb.SegmentInfo{ID: 1, CollectionID: 100, PartitionID: 10, InsertChannel: "p_100v1", State: commonpb.SegmentState_Growing, Level: datapb.SegmentLevel_L1, IsCreatedByStreaming: tc.streaming, TransformStartAfterTimetick: tc.cursor}
			if tc.start > 0 {
				segment.StartPosition = &msgpb.MsgPosition{ChannelName: "p_100v1", Timestamp: tc.start}
			}
			require.NoError(t, m.AddSegment(ctx, NewSegmentInfo(segment)))
			require.Equal(t, tc.want, m.GetEarliestStartPositionOfGrowingSegments(100, 10, "p_100v1").GetTimestamp())
		})
	}
}
