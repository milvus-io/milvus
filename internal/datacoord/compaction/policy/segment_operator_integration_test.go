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

package policy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/view"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// Complete data positions must survive a later position-free final commit.
// A newer growing segment must block only the Deletes that can affect its rows.
func TestGrowingDataPositionsProtectL0AndSurviveFinalCommit(t *testing.T) {
	m := &meta.Meta{Ctx: context.Background(), Segments: meta.NewSegmentsInfo(metacache.NewMetaStore(nil))}
	segment := meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID: 1, CollectionID: 1, PartitionID: 1, InsertChannel: "ch",
		State: commonpb.SegmentState_Growing, Level: datapb.SegmentLevel_L1,
		StorageVersion: storage.StorageV3,
	})
	m.Segments.SetSegment(1, segment)
	pack := &meta.UpdateSegmentPack{Meta: m, Segments: map[int64]*meta.SegmentInfo{1: segment}}
	start := &msgpb.MsgPosition{ChannelName: "ch", Timestamp: 100, MsgID: []byte{1}, WALName: commonpb.WALName_Pulsar}
	checkpoint := &msgpb.MsgPosition{ChannelName: "ch", Timestamp: 150, MsgID: []byte{2}, WALName: commonpb.WALName_Pulsar}
	require.True(t, meta.UpdateStartPosition([]*datapb.SegmentStartPosition{{SegmentID: 1, StartPosition: start}})(pack))
	require.True(t, meta.UpdateCheckPointOperator(1, []*datapb.CheckPoint{{SegmentID: 1, NumOfRows: 10, Position: checkpoint}}, true)(pack))

	label := &view.CompactionGroupLabel{CollectionID: 1, PartitionID: 1, Channel: "ch"}
	require.True(t, proto.Equal(start, m.GetEarliestStartPositionOfGrowingSegments(label.CollectionID, label.PartitionID, label.Channel)))
	policy := &L0CompactionPolicy{Meta: m}
	views := policy.GroupL0ViewsByPartChan(1, []*view.SegmentView{
		{ID: 2, Label: label, DmlPos: &msgpb.MsgPosition{Timestamp: 90}},
		{ID: 3, Label: label, DmlPos: &msgpb.MsgPosition{Timestamp: 125}},
	}, 1)
	require.Len(t, views, 1)
	eligible := views[0].(*view.LevelZeroCompactionView).L0Segments
	require.Len(t, eligible, 1)
	require.Equal(t, int64(2), eligible[0].ID)

	// Final SaveBinlogPaths and its recovery retry omit positions. In
	// particular V3 keeps its existing authoritative row count without a CP.
	for range 2 {
		require.True(t, meta.UpdateStartPosition(nil)(pack))
		require.True(t, meta.UpdateCheckPointOperator(1, nil, true)(pack))
	}
	require.True(t, proto.Equal(start, segment.GetStartPosition()))
	require.True(t, proto.Equal(checkpoint, segment.GetDmlPosition()))
	require.Equal(t, int64(10), segment.GetNumOfRows())
}
