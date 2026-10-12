package segment

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func TestFinalCommitVersionSnapshotAndReplay(t *testing.T) {
	for _, retired := range []bool{false, true} {
		t.Run(map[bool]string{false: "published", true: "retired"}[retired], func(t *testing.T) {
			var version *viewpb.DataVersion
			if !retired {
				version = &viewpb.DataVersion{StreamingVersion: 2, CompactVersion: 3}
			}
			patch := mockey.Mock((*testSegmentLifecycle).CommitL1Segment).Return(version, nil).Build()
			defer patch.UnPatch()
			meta := &streamingpb.SegmentAssignmentMeta{
				SegmentId: 1, Vchannel: "v1", CheckpointTimeTick: 10,
				State: streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
			}
			config := runtimeConfig{lifecycle: &testSegmentLifecycle{}, owner: testSegmentOwner{}, runtime: moduleapi.Runtime{Scheduler: &recordingSegmentScheduler{}}}
			view := newSegmentViewFromMeta(meta, nil, config)
			// The old snapshot has no version: both a normal restart and a lost
			// RPC response must retry, even if the object checkpoint is unchanged.
			require.False(t, view.finalCommitDone.Load())
			require.True(t, shouldRetryRecoveredFinalCommit(meta))
			oldSnapshot := proto.Clone(meta).(*streamingpb.SegmentAssignmentMeta)
			view.mu.Lock()
			task := view.newRecoveredCommitL1SegmentTaskLocked(10)
			view.mu.Unlock()
			require.NoError(t, task.Execute(context.Background()))
			snapshot := view.ConsumeDirtyAndGetSnapshot()
			require.NotNil(t, snapshot, "version-only progress must be dirty")
			require.Equal(t, uint64(10), snapshot.GetCheckpointTimeTick())
			require.True(t, proto.Equal(version, snapshot.GetSealedAtDataVersion()))
			if retired {
				require.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED, snapshot.GetState())
			}
			view.MarkSnapshotPersisted(oldSnapshot)
			require.True(t, view.dirty, "an older snapshot cannot acknowledge the new version")
			view.MarkSnapshotPersisted(snapshot)
			require.False(t, view.dirty)
			recovered := newSegmentViewFromMeta(snapshot, nil, config)
			recovered.ResumePendingRecovery()
			require.True(t, recovered.EnsureFinalCommit())
			require.Empty(t, recovered.pendingTasks)
			require.EqualValues(t, 1, patch.Times(), "persisted completion suppresses WAL replay commits")
		})
	}
}

func TestMissingCommitVersionKeepsHandleUntilRetry(t *testing.T) {
	ctx := context.Background()
	writer := &segmentLifecycleWriter{coord: &coordStub{}}
	patch := mockey.Mock((*coordStub).SaveBinlogPaths).Return(merr.Success(), nil).Build()
	defer patch.UnPatch()
	view := newSegmentViewFromMeta(&streamingpb.SegmentAssignmentMeta{
		SegmentId: 1, Vchannel: "v1", CheckpointTimeTick: 10,
		State: streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
	}, nil, runtimeConfig{lifecycle: writer, owner: testSegmentOwner{}, runtime: moduleapi.Runtime{Scheduler: &recordingSegmentScheduler{}}})
	done := false
	owned := message.NewOwnedImmutableMessage(newObserveTestInsert(t, 11, nil), func() {
		done = true
		require.NotNil(t, view.durableMeta.GetSealedAtDataVersion())
		require.True(t, view.dirty)
	})
	view.mu.Lock()
	view.retainDataHandleLocked(11, owned.Clone())
	task := view.newRecoveredCommitL1SegmentTaskLocked(11)
	view.mu.Unlock()
	owned.Release()
	require.True(t, errors.Is(task.Execute(ctx), nodescheduler.ErrDelay))
	require.False(t, done)
	require.False(t, view.finalCommitDone.Load())
	require.Equal(t, uint64(10), view.durableMeta.GetCheckpointTimeTick())
	patch.UnPatch()
	version := &viewpb.DataVersion{StreamingVersion: 2}
	patch = mockey.Mock((*coordStub).SaveBinlogPaths).Return(dataview.FlushResultStatus(version), nil).Build()
	defer patch.UnPatch()
	require.NoError(t, task.Execute(ctx))
	require.True(t, done)
	require.True(t, proto.Equal(version, view.ConsumeDirtyAndGetSnapshot().GetSealedAtDataVersion()))
}

func TestCommitWriterDistinguishesRetirement(t *testing.T) {
	for _, status := range []*commonpb.Status{
		dataview.FlushResultStatus(nil),
		merr.Status(merr.WrapErrSegmentNotFound(1)),
	} {
		patch := mockey.Mock((*coordStub).SaveBinlogPaths).Return(status, nil).Build()
		writer := &segmentLifecycleWriter{coord: &coordStub{}}
		version, err := writer.CommitL1Segment(context.Background(), newCommitL1SegmentTestMeta())
		patch.UnPatch()
		require.NoError(t, err)
		require.Nil(t, version)
	}
}
