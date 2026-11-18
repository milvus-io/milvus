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
)

type stagedFlushTestLifecycle struct {
	testSegmentLifecycle
	pending         bool
	result          *viewpb.DataVersion
	err             error
	registrations   int
	acknowledgments int
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
