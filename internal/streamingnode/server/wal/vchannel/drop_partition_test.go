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

package vchannel

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/messageack"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/segment"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestCollectionBarrierFlushesAllEarlierSegmentsBeforeL0Completion(t *testing.T) {
	for _, tc := range []struct {
		name           string
		emptyPartition bool
		snapshot       bool
	}{
		{name: "drop_nonempty"},
		{name: "drop_empty", emptyPartition: true},
		{name: "snapshot_nonempty", snapshot: true},
		{name: "snapshot_empty", emptyPartition: true, snapshot: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			scheduler := &recordingVChannelScheduler{}
			lifecycle := segment.NewSegmentLifecycleWriter(nil, 1)
			var committed []int64
			patch := mockey.Mock(mockey.GetMethod(lifecycle, "CommitL1Segment")).To(func(_ context.Context, meta *streamingpb.SegmentAssignmentMeta) error {
				committed = append(committed, meta.GetSegmentId())
				return nil
			}).Build()
			defer patch.UnPatch()
			metas := map[int64]*streamingpb.SegmentAssignmentMeta{
				2: newMaterializationBlockerMeta(2, 50, false),
				// Metadata can be ahead of replay. Segments created after this request
				// must remain growing and do not block completion through 100.
				3: newMaterializationBlockerMeta(3, 150, false),
			}
			expectedCommits := []int64{2}
			if !tc.emptyPartition {
				metas[1] = newMaterializationBlockerMeta(1, 40, false)
				expectedCommits = append(expectedCommits, 1)
			}
			for _, meta := range metas {
				meta.State = streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING
			}
			metas[3].PartitionId = 2
			summary := walsummary.NewManager(walsummary.ManagerConfig{})
			module, err := NewModule(ModuleConfig{
				PChannel: "p1", VChannel: "v1",
				VChannelMeta: &streamingpb.VChannelMeta{
					Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
					CollectionInfo: &streamingpb.CollectionInfoOfVChannel{CollectionId: 1, Partitions: []*streamingpb.PartitionInfoOfVChannel{
						{PartitionId: 1, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
						{PartitionId: 2, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
					}},
				},
				Segments: metas, SegmentLifecycle: lifecycle,
				Runtime: moduleapi.Runtime{Scheduler: scheduler},
			})
			require.NoError(t, err)
			raw := message.NewDropPartitionMessageBuilderV1().WithVChannel("v1").
				WithHeader(&message.DropPartitionMessageHeader{CollectionId: 1, PartitionId: 1}).
				WithBody(&message.DropPartitionRequest{}).MustBuildMutable().WithTimeTick(100).
				WithLastConfirmed(walimplstest.NewTestMessageID(100)).IntoImmutableMessage(walimplstest.NewTestMessageID(101))
			if tc.snapshot {
				raw = message.NewCreateSnapshotMessageBuilderV2().WithVChannel("v1").
					WithHeader(&message.CreateSnapshotMessageHeader{CollectionId: 1, Name: "snapshot"}).
					WithBody(&message.CreateSnapshotMessageBody{}).MustBuildMutable().WithTimeTick(100).
					WithLastConfirmed(walimplstest.NewTestMessageID(100)).IntoImmutableMessage(walimplstest.NewTestMessageID(101))
			}
			tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
			owner := tracker.Track(raw)
			retained := owner.Clone()
			summary.ObserveMessage(ctx, raw)
			require.True(t, module.ObserveMessage(ctx, retained))
			retained.Release()
			owner.Release()
			expectedState := streamingpb.PartitionState_PARTITION_STATE_DROPPED
			if tc.snapshot {
				expectedState = streamingpb.PartitionState_PARTITION_STATE_NORMAL
			}
			require.Equal(t, expectedState, module.vchannelView.meta.CollectionInfo.Partitions[0].State)
			require.Equal(t, streamingpb.PartitionState_PARTITION_STATE_NORMAL, module.vchannelView.meta.CollectionInfo.Partitions[1].State)
			require.Zero(t, tracker.CompletedPoint().TimeTick)
			require.Len(t, scheduler.tasks, len(expectedCommits)+1, "L1 and L0 schedule independently")
			for i := range expectedCommits {
				require.NoError(t, scheduler.tasks[i].Execute(ctx))
				require.Zero(t, tracker.CompletedPoint().TimeTick, "L0 still holds the request")
			}
			require.ElementsMatch(t, expectedCommits, committed)
			require.Len(t, scheduler.tasks, len(expectedCommits)+1)
			require.NoError(t, scheduler.tasks[len(expectedCommits)].Execute(ctx))
			require.Equal(t, uint64(100), module.l0Materializer.MaterializedTimeTick())
			require.Equal(t, uint64(100), tracker.CompletedPoint().TimeTick)
			require.Equal(t, uint64(100), module.vchannelView.AssignmentMeta().GetTransformMaterializedTimeTick())
			require.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING, module.segments[3].AssignmentMeta().GetState())
		})
	}
}
