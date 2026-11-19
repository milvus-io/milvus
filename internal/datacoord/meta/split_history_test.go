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
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestSplitFiniteHistoryPartitionHoles(t *testing.T) {
	for _, tc := range []struct {
		name      string
		partition int64
		start     uint64
		importing bool
		commit    uint64
		blocked   bool
	}{
		{"straddles F", 1, 150, false, 0, true},
		{"starts at F", 1, 200, false, 0, true},
		{"unknown start", 1, 0, false, 0, true},
		{"uncommitted import", 1, 300, true, 0, true},
		{"newer history", 1, 201, false, 0, false},
		{"committed import", 1, 300, false, 400, true},
		{"different partition", 2, 100, false, 0, false},
		{"all partitions", common.AllPartitionsID, 100, false, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			m, err := NewMemoryMeta(t)
			require.NoError(t, err)
			require.NoError(t, m.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{ID: 1, CollectionID: 100, PartitionID: 1, InsertChannel: "p_100v1", Level: datapb.SegmentLevel_L1, State: commonpb.SegmentState_Flushed, TransformStartAfterTimetick: 100})))
			require.NoError(t, m.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{ID: 2, CollectionID: 100, PartitionID: tc.partition, InsertChannel: "p_100v1", Level: datapb.SegmentLevel_L0, State: commonpb.SegmentState_Flushed, StartPosition: &msgpb.MsgPosition{Timestamp: tc.start}, DmlPosition: &msgpb.MsgPosition{Timestamp: 400}, IsImporting: tc.importing, CommitTimestamp: tc.commit})))
			require.NoError(t, m.AdvanceSplitFoldedCursors(ctx, 100, "p_100v1", 200))
			want := uint64(200)
			if tc.blocked {
				want = 100
			}
			require.Equal(t, want, m.GetSegment(ctx, 1).GetTransformStartAfterTimetick())
		})
	}
}
