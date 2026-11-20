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

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/view"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	kvstreamingnode "github.com/milvus-io/milvus/internal/metastore/kv/streamingnode"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestCertifiedL0Schedule(t *testing.T) {
	ctx := t.Context()
	m, err := meta.NewMemoryMeta(t)
	require.NoError(t, err)
	m.StreamingNodeCatalog = kvstreamingnode.NewCataLog(nil)
	m0 := uint64(200)
	calls := 0
	read := mockey.Mock(mockey.GetMethod(m.StreamingNodeCatalog, "GetVChannelBase")).To(func(context.Context, string, string) (*streamingpb.VChannelMeta, error) {
		calls++
		return &streamingpb.VChannelMeta{TransformMaterializedTimeTick: m0}, nil
	}).Build()
	defer read.UnPatch()
	const channel = "p1_100v0"
	add := func(id, part int64, ts uint64, level datapb.SegmentLevel, state commonpb.SegmentState) *meta.SegmentInfo {
		s := meta.NewSegmentInfo(&datapb.SegmentInfo{ID: id, CollectionID: 100, PartitionID: part, InsertChannel: channel, Level: level, State: state, StartPosition: &msgpb.MsgPosition{Timestamp: ts}, DmlPosition: &msgpb.MsgPosition{Timestamp: ts}})
		require.NoError(t, m.AddSegment(ctx, s))
		return s
	}
	candidate := add(1, 1, 100, datapb.SegmentLevel_L0, commonpb.SegmentState_Flushed)
	add(2, 2, 130, datapb.SegmentLevel_L0, commonpb.SegmentState_Flushed)
	add(3, 1, 220, datapb.SegmentLevel_L0, commonpb.SegmentState_Flushed)
	p := &L0CompactionPolicy{Meta: m}
	schedule := func() []view.CompactionView {
		before := calls
		v, err := p.scheduleViews(ctx, 100, []*meta.SegmentInfo{candidate}, 7)
		require.NoError(t, err)
		require.Equal(t, before+1, calls)
		return v
	}
	require.Len(t, schedule(), 2)
	add(4, common.AllPartitionsID, 160, datapb.SegmentLevel_L0, commonpb.SegmentState_Flushed)
	v := schedule()
	require.Len(t, v, 1)
	g := v[0].(*view.LevelZeroCompactionView)
	require.EqualValues(t, common.AllPartitionsID, g.Label.PartitionID)
	require.EqualValues(t, 200, g.MaterializedThrough)
	var ids []int64
	for _, s := range g.L0Segments {
		ids = append(ids, s.ID)
	}
	require.ElementsMatch(t, []int64{1, 2, 4}, ids)
	m.SetSegmentsCompacting(ctx, []int64{2}, true)
	require.Empty(t, schedule())
	m.SetSegmentsCompacting(ctx, []int64{2}, false)
	m0 = 0
	require.Empty(t, schedule())
	m0 = 200
	add(5, 3, 150, datapb.SegmentLevel_L1, commonpb.SegmentState_Growing)
	require.Empty(t, schedule())
}

func TestCertifiedImportedL0Range(t *testing.T) {
	seg := meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID: 9, PartitionID: 1, Level: datapb.SegmentLevel_L0, State: commonpb.SegmentState_Flushed,
		CommitTimestamp: 150, Deltalogs: []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{TimestampFrom: 99, TimestampTo: 210}}}},
	})
	require.Empty(t, certifiedL0Groups(100, "p1_100v0", 7, 200, []*meta.SegmentInfo{seg}))
	views := certifiedL0Groups(100, "p1_100v0", 7, 220, []*meta.SegmentInfo{seg})
	require.Len(t, views, 1)
	require.EqualValues(t, 210, views[0].LatestDeletePos.GetTimestamp())
}
