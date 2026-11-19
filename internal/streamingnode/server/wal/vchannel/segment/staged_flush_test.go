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

package segment

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type stagedFlushTestLifecycle struct {
	testSegmentLifecycle
	pending         bool
	result          *viewpb.DataVersion
	err             error
	registrations   int
	acknowledgments int
}

type retiringFlushTestLifecycle struct {
	stagedFlushTestLifecycle
	retirements int
}

func (l *retiringFlushTestLifecycle) RetireL1Segment(context.Context, *streamingpb.SegmentAssignmentMeta) (*viewpb.DataVersion, error) {
	l.retirements++
	return nil, l.err
}

func TestStagedFlushOwnerDropRetiresWithoutPublication(t *testing.T) {
	for _, tc := range []struct {
		name           string
		partition      int64
		collectionDrop bool
	}{
		{name: "owner partition", partition: 1},
		{name: "other partition", partition: 2},
		{name: "collection", partition: 2, collectionDrop: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lifecycle := &retiringFlushTestLifecycle{stagedFlushTestLifecycle: stagedFlushTestLifecycle{pending: true, result: &viewpb.DataVersion{StreamingVersion: 1}}}
			view := newSegmentViewFromMeta(&streamingpb.SegmentAssignmentMeta{
				SegmentId: 1, CollectionId: 1, PartitionId: tc.partition, Vchannel: "v1", CheckpointTimeTick: 20,
				State: streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED, FlushDataDurable: true,
				Stat: &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 5, ModifiedRows: 2},
			}, nil, runtimeConfig{lifecycle: lifecycle, owner: testSegmentOwner{}, runtime: moduleapi.Runtime{Scheduler: &recordingSegmentScheduler{}}})
			view.ResumePendingRecovery()
			raw := message.NewDropPartitionMessageBuilderV1().WithVChannel("v1").
				WithHeader(&message.DropPartitionMessageHeader{CollectionId: 1, PartitionId: 1}).WithBody(&message.DropPartitionRequest{}).
				MustBuildMutable().WithTimeTick(30).IntoImmutableMessage(walimplstest.NewTestMessageID(30))
			if tc.collectionDrop {
				raw = message.NewDropCollectionMessageBuilderV1().WithVChannel("v1").
					WithHeader(&message.DropCollectionMessageHeader{CollectionId: 1}).WithBody(&message.DropCollectionRequest{}).
					MustBuildMutable().WithTimeTick(30).IntoImmutableMessage(walimplstest.NewTestMessageID(30))
			}
			owned := message.NewOwnedImmutableMessage(raw, nil)
			dispatch := owned.Clone()
			view.Flush(t.Context(), dispatch)
			dispatch.Release()
			owned.Release()
			if tc.partition == 2 && !tc.collectionDrop {
				require.Error(t, view.pendingTasks[0].Execute(t.Context()))
				require.Zero(t, lifecycle.retirements, "a different partition's Drop must preserve this source")
				require.False(t, view.finalCommitDone.Load())
				return
			}
			lifecycle.err = merr.WrapErrServiceUnavailableMsg("retirement persistence failed")
			require.Error(t, view.pendingTasks[0].Execute(t.Context()))
			require.False(t, view.finalCommitDone.Load(), "failed retirement cannot install a child tombstone")
			lifecycle.err = nil
			require.NoError(t, view.pendingTasks[0].Execute(t.Context()))
			require.True(t, view.finalCommitDone.Load())
			require.Nil(t, view.meta.GetSealedAtDataVersion(), "Drop must not invent a query handoff version")
			require.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED, view.ConsumeDirtyAndGetSnapshot().GetState())
		})
	}
}

func TestStagedFlushRepeatedFlushReleasesAtDurability(t *testing.T) {
	for _, recovered := range []bool{false, true} {
		name := "runtime"
		if recovered {
			name = "recovered"
		}
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			lifecycle := &stagedFlushTestLifecycle{pending: true, result: &viewpb.DataVersion{StreamingVersion: 1}}
			config := runtimeConfig{lifecycle: lifecycle, owner: testSegmentOwner{}, runtime: moduleapi.Runtime{Scheduler: &recordingSegmentScheduler{}}}
			view := newSegmentViewFromMeta(&streamingpb.SegmentAssignmentMeta{
				SegmentId: 1, CollectionId: 1, Vchannel: "v1", CheckpointTimeTick: 10,
				State: streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
				Stat:  &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 5, ModifiedRows: 1},
			}, nil, config)
			observeFlush := func(timetick uint64) *bool {
				released := new(bool)
				flush := message.NewFlushMessageBuilderV2().WithVChannel("v1").WithHeader(&message.FlushMessageHeader{}).
					WithBody(&message.FlushMessageBody{}).MustBuildMutable().WithTimeTick(timetick).IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick)))
				owner := message.NewOwnedImmutableMessage(flush, func() { *released = true })
				dispatch := owner.Clone()
				view.Flush(ctx, dispatch)
				dispatch.Release()
				owner.Release()
				return released
			}
			first := observeFlush(20)
			require.False(t, *first, "Flush must retain its ACK until durable registration")
			require.NoError(t, view.pendingTasks[0].Execute(ctx))
			require.True(t, *first)
			if recovered {
				view = newSegmentViewFromMeta(view.ConsumeDirtyAndGetSnapshot(), nil, config)
				view.ResumePendingRecovery()
			}
			require.True(t, *observeFlush(30), "later Flush/DDL ACK must not wait for group publication")
			require.Empty(t, view.pendingDataHandles)
			require.False(t, view.finalCommitDone.Load())
			require.Nil(t, view.meta.GetSealedAtDataVersion())
			require.Error(t, view.pendingTasks[0].Execute(ctx), "publication is still pending")
			require.True(t, *observeFlush(40))
			lifecycle.pending = false
			lifecycle.result = &viewpb.DataVersion{StreamingVersion: 2}
			require.NoError(t, view.pendingTasks[0].Execute(ctx))
			require.True(t, view.finalCommitDone.Load())
			require.True(t, *observeFlush(50))
			require.Empty(t, view.pendingDataHandles)
		})
	}
}

func (l *stagedFlushTestLifecycle) CommitL1SegmentWithPublication(_ context.Context, meta *streamingpb.SegmentAssignmentMeta, _ uint64) (*viewpb.DataVersion, bool, error) {
	if !meta.GetFlushDataDurable() {
		l.registrations++
	}
	return l.result, l.pending, l.err
}

func (l *stagedFlushTestLifecycle) AcknowledgeFlushHandoff(_ context.Context, _ *streamingpb.SegmentAssignmentMeta) error {
	l.acknowledgments++
	return l.err
}

func TestStagedFlushDurabilityAndVersionedHandoff(t *testing.T) {
	ctx := t.Context()
	lifecycle := &stagedFlushTestLifecycle{pending: true, result: &viewpb.DataVersion{StreamingVersion: 1}}
	config := runtimeConfig{lifecycle: lifecycle, owner: testSegmentOwner{}, runtime: moduleapi.Runtime{Scheduler: &recordingSegmentScheduler{}}}
	view := newSegmentViewFromMeta(&streamingpb.SegmentAssignmentMeta{
		SegmentId: 1, CollectionId: 1, Vchannel: "v1", CheckpointTimeTick: 10,
		State: streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
		Stat:  &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 5},
	}, nil, config)
	released := false
	owned := message.NewOwnedImmutableMessage(newObserveTestInsert(t, 10, nil), func() { released = true })
	view.mu.Lock()
	view.retainDataHandleLocked(10, owned.Clone())
	commit := view.newRecoveredCommitL1SegmentTaskLocked(10)
	view.mu.Unlock()
	owned.Release()
	lifecycle.err = context.DeadlineExceeded
	require.Error(t, commit.Execute(ctx))
	require.False(t, released)
	require.False(t, view.flushDataDurable.Load())
	lifecycle.err = nil
	require.NoError(t, commit.Execute(ctx))
	require.True(t, released, "durable registration releases the Flush handle before index readiness")
	require.False(t, view.finalCommitDone.Load())
	require.Nil(t, view.meta.GetSealedAtDataVersion())
	require.True(t, view.EnsureQueryPublication(&viewpb.DataVersion{StreamingVersion: 1}))
	require.False(t, view.EnsureQueryPublication(&viewpb.DataVersion{StreamingVersion: 2}))
	_, materializing := view.L1DataMaterializationBlockerTimeTick()
	require.False(t, materializing, "L0 can materialize after L1 data is durable")
	_, growing := view.L1MaterializationBlockerTimeTick()
	require.True(t, growing, "other publications must still account for this growing query source")
	_, visible := view.VisibleSnapshot("v1", qviews.FromProtoDataVersion(lifecycle.result))
	require.True(t, visible)
	snapshot := view.ConsumeDirtyAndGetSnapshot()
	require.True(t, snapshot.GetFlushDataDurable())
	require.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED, snapshot.GetState())
	view.MarkSnapshotPersisted(snapshot)
	recovered := newSegmentViewFromMeta(proto.CloneOf(snapshot), nil, config)
	recovered.ResumePendingRecovery()
	require.Len(t, recovered.pendingTasks, 1)
	_, isAwait := recovered.pendingTasks[0].(*awaitFlushPublicationTask)
	require.True(t, isAwait, "restart polls the existing group instead of registering data again")
	await := view.pendingTasks[0]
	require.Error(t, await.Execute(ctx))
	lifecycle.pending = false
	lifecycle.result = &viewpb.DataVersion{StreamingVersion: 2}
	require.NoError(t, await.Execute(ctx))
	require.True(t, view.EnsureQueryPublication(lifecycle.result))
	require.Equal(t, 0, lifecycle.acknowledgments, "GC release must wait for the SN snapshot")
	final := view.ConsumeDirtyAndGetSnapshot()
	require.True(t, proto.Equal(lifecycle.result, final.GetSealedAtDataVersion()))
	view.MarkSnapshotPersisted(final)
	require.Len(t, view.pendingTasks, 1)
	require.NoError(t, view.pendingTasks[0].Execute(ctx))
	require.Equal(t, 1, lifecycle.acknowledgments)
	_, oldVisible := view.VisibleSnapshot("v1", qviews.DataVersion{StreamingVersion: 1})
	_, newVisible := view.VisibleSnapshot("v1", qviews.DataVersion{StreamingVersion: 2})
	require.True(t, oldVisible)
	require.False(t, newVisible)
}
