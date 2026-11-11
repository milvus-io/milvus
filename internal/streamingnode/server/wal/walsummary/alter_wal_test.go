package walsummary

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/messageack"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func summaryAlterWAL(timetick uint64) message.ImmutableMessage {
	return message.NewAlterWALMessageBuilderV2().WithVChannel("").
		WithHeader(&message.AlterWALMessageHeader{TargetWalName: commonpb.WALName_WoodPecker}).
		WithBody(&message.AlterWALMessageBody{}).MustBuildMutable().
		WithTimeTick(timetick).WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
}

func TestAlterWALFlushesSummaryWithoutMoreWrites(t *testing.T) {
	for _, test := range []struct {
		name       string
		withDelete bool
		threshold  uint64
	}{
		{name: "insert", threshold: 1 << 30},
		{name: "delete", withDelete: true, threshold: 1 << 30},
		{name: "size_sealing_disabled"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			manager, store := newTestManagerWithStore(t)
			scheduler := nodescheduler.New(1)
			defer scheduler.Close()
			manager.cfg.Runtime = moduleapi.Runtime{Scheduler: scheduler}
			manager.cfg.FlushMaxBytes = test.threshold
			msg := plainSummaryInsert(100)
			if test.withDelete {
				msg = newTestDeleteMessage(t, "v1", 100, 10, 1)
			}
			manager.ObserveMessage(ctx, msg)
			require.Positive(t, manager.pendingBytes)
			if test.threshold > 0 {
				require.Less(t, manager.pendingBytes, test.threshold)
			}
			require.Less(t, manager.LastAcked(), uint64(100))
			manager.ObserveMessage(ctx, summaryAlterWAL(200))
			// No pressure worker, explicit flush request or subsequent WAL message.
			require.Eventually(t, func() bool {
				return manager.LastAcked() == 200 && !manager.HasPendingWork()
			}, 5*time.Second, time.Millisecond)
			manifest, found, err := store.ReadManifest(ctx)
			require.NoError(t, err)
			require.True(t, found)
			require.Len(t, manifest.GetChunks(), 1)
			require.Equal(t, uint64(200), manifest.GetCoverage().GetEndTimeTick())
			require.Equal(t, uint64(100), manifest.GetChunks()[0].GetVchannels()[0].GetEndTimetick(),
				"the payload ends before the persisted barrier coverage")
			recovered := newTestManager(t, nextTermStore(store), 1<<30)
			require.NoError(t, recovered.Restore(ctx))
			require.Equal(t, uint64(200), recovered.LastAcked())
		})
	}
}

func TestAlterWALSummaryEmptyAndRepeatedRequests(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty", true: "pending"}[pending], func(t *testing.T) {
			ctx := context.Background()
			manager, _ := newTestManagerWithStore(t)
			if pending {
				manager.ObserveMessage(ctx, plainSummaryInsert(100))
			}
			barrier := summaryAlterWAL(200)
			manager.ObserveMessage(ctx, barrier)
			manager.ObserveMessage(ctx, barrier)
			manager.RequestFlushThrough(200)
			manager.RequestFlushThrough(100)
			require.NoError(t, drainSummary(ctx, manager))
			require.Equal(t, uint64(200), manager.LastAcked())
			chunks := len(manager.Manifest().GetChunks())
			if pending {
				require.Equal(t, 1, chunks)
			} else {
				require.Zero(t, chunks, "empty barriers need no chunk")
			}
			manager.ObserveMessage(ctx, barrier)
			manager.RequestFlushThrough(200)
			require.False(t, manager.HasPendingWork())
			require.Len(t, manager.Manifest().GetChunks(), chunks)
		})
	}
}

func TestAlterWALSummaryFailurePinsCheckpoint(t *testing.T) {
	for _, operation := range []string{"chunk", "manifest"} {
		t.Run(operation, func(t *testing.T) {
			ctx := context.Background()
			manager, _ := newTestManagerWithStore(t)
			tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
			for _, msg := range []message.ImmutableMessage{plainSummaryInsert(100), summaryAlterWAL(200)} {
				owner := tracker.Track(msg)
				manager.ObserveMessage(ctx, msg)
				owner.Release()
			}
			require.Equal(t, uint64(200), tracker.CompletedPoint().TimeTick)
			require.Len(t, manager.pendingSealed, 1)
			chunkTask := manager.pendingSealed[0].task
			if operation == "manifest" {
				require.NoError(t, chunkTask.Execute(ctx))
			}
			var patch *mockey.Mocker
			if operation == "chunk" {
				patch = mockey.Mock((*Manager).writeChunk).Return(context.DeadlineExceeded).Build()
			} else {
				patch = mockey.Mock((*Store).WriteManifest).Return(context.DeadlineExceeded).Build()
			}
			defer patch.UnPatch()
			var task nodescheduler.Task = chunkTask
			if operation == "manifest" {
				task = manager.manifestTask
			}
			requireSummaryError(t, task.Execute(ctx), nodescheduler.ErrDelay)
			manager.RequestFlushThrough(200)
			require.Less(t, manager.LastAcked(), uint64(100))
			point, _ := tracker.CheckpointThrough(manager.LastAcked())
			require.Zero(t, point.TimeTick, "completed handles cannot bypass failed Summary persistence")
			patch.UnPatch()
			require.NoError(t, drainSummary(ctx, manager))
			require.Equal(t, uint64(200), manager.LastAcked())
			point, _ = tracker.CheckpointThrough(manager.LastAcked())
			require.Equal(t, uint64(200), point.TimeTick)
			require.Equal(t, uint64(200), manager.Manifest().GetCoverage().GetEndTimeTick())
		})
	}
}

func TestTimeTickDoesNotFlushPendingSummary(t *testing.T) {
	ctx := context.Background()
	manager, _ := newTestManagerWithStore(t)
	manager.cfg.FlushMaxBytes = 1 << 30
	manager.ObserveMessage(ctx, plainSummaryInsert(100))
	for tick := uint64(101); tick < 200; tick++ {
		msg := message.NewTimeTickMessageBuilderV1().WithVChannel("").
			WithHeader(&message.TimeTickMessageHeader{}).WithBody(&message.TimeTickMsg{}).
			MustBuildMutable().WithTimeTick(tick).WithLastConfirmed(walimplstest.NewTestMessageID(int64(tick))).
			IntoImmutableMessage(walimplstest.NewTestMessageID(int64(tick + 1)))
		manager.ObserveMessage(ctx, msg)
	}
	require.Len(t, manager.pending, 1)
	require.Empty(t, manager.cfg.Runtime.Scheduler.(*recordingScheduler).tasks)
	require.Less(t, manager.LastAcked(), uint64(100))
}
