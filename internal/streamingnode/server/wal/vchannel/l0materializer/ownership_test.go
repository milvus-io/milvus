package l0materializer

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/broker"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/messageack"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

// Use the real materializer, SyncTask and MetaWriter so a writer swallowing
// registration failures cannot be hidden by a fake Materialize result.
func TestWALMaterializerRegistrationErrors(t *testing.T) {
	paramtable.Init()
	for _, tc := range []struct {
		name    string
		err     error
		ignored bool
		poison  bool
	}{
		{name: "channel retired", err: merr.WrapErrChannelNotFound("v1"), ignored: true},
		{name: "WAL migrated", err: merr.WrapErrChannelMisrouted("v1"), poison: true},
		{name: "no available assignment", err: merr.WrapErrChannelNotAvailable("v1")},
		{name: "RPC timeout", err: context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scheduler := nodescheduler.New(1)
			t.Cleanup(scheduler.Close)
			var tasks []nodescheduler.Task
			submit := mockey.Mock(mockey.GetMethod(scheduler, "Submit")).To(func(task nodescheduler.Task) nodescheduler.TaskHandle {
				tasks = append(tasks, task)
				return nil
			}).Build()
			defer submit.UnPatch()
			b := broker.NewCoordBroker(nil, 1)
			outputErr := tc.err
			registrations := 0
			save := mockey.Mock(mockey.GetMethod(b, "SaveBinlogPaths")).To(func(_ context.Context, req *datapb.SaveBinlogPathsRequest) error {
				registrations++
				require.Equal(t, datapb.SegmentLevel_L0, req.GetSegLevel())
				require.NotEmpty(t, req.GetDeltalogs())
				return outputErr
			}).Build()
			defer save.UnPatch()
			writer := NewSyncMaterializer(storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir())), allocator.NewLocalAllocator(1, 10000), syncmgr.BrokerMetaWriter(b, 1, retry.Attempts(1)))
			updates := 0
			config := WALConfig{VChannel: "p1_1v0", MaterializeMaxBytes: 1 << 20, Materializer: writer, Runtime: moduleapi.Runtime{Scheduler: scheduler}, OnMaterialized: func(uint64) { updates++ }}
			m := NewWALMaterializer(config)
			tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
			var roots []message.OwnedImmutableMessage
			for _, msg := range []message.ImmutableMessage{walDelete(100), walFlush(110), walDelete(120), walFlush(130)} {
				root := tracker.Track(msg)
				roots = append(roots, root)
				h := root.Clone()
				m.ObserveMessage(h)
				h.Release()
			}
			require.Len(t, tasks, 1)
			err := tasks[0].Execute(context.Background())
			require.Equal(t, 1, registrations, "one bounded registration attempt")
			switch {
			case tc.ignored:
				require.NoError(t, err)
				require.Equal(t, uint64(110), m.MaterializedTimeTick())
			case tc.poison:
				require.ErrorIs(t, err, merr.ErrChannelMisrouted)
				require.False(t, errors.Is(err, nodescheduler.ErrDelay))
				require.Zero(t, m.MaterializedTimeTick())
				require.Zero(t, updates)
				for _, root := range roots {
					require.True(t, root.IsPoisoned())
				}
				require.Empty(t, m.pending)
				require.Zero(t, m.pendingBytes)
				require.Nil(t, m.task)
				future := tracker.Track(walDelete(140))
				h := future.Clone()
				m.ObserveMessage(h)
				h.Release()
				require.True(t, future.IsPoisoned())
				future.Release()
				m.RequestPersistThrough(200)
				require.Len(t, tasks, 1)
				require.ErrorIs(t, tasks[0].Execute(context.Background()), merr.ErrChannelMisrouted)
				require.Equal(t, 1, registrations, "terminal task cannot write again")
			default:
				require.True(t, errors.Is(err, nodescheduler.ErrDelay))
				require.Zero(t, m.MaterializedTimeTick())
				require.Zero(t, updates)
				for _, root := range roots {
					require.False(t, root.IsPoisoned())
				}
				outputErr = nil
				require.NoError(t, tasks[0].Execute(context.Background()))
				require.Equal(t, uint64(110), m.MaterializedTimeTick())
			}
			for _, root := range roots {
				root.Release()
			}
			if tc.poison {
				require.Zero(t, tracker.CompletedPoint().TimeTick)
				// Poison belongs to the old consumer; replay by a new owner succeeds.
				outputErr = nil
				restored := NewWALMaterializer(config)
				replay := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
				walObserve(restored, replay, walDelete(100))
				walObserve(restored, replay, walFlush(110))
				require.NoError(t, tasks[len(tasks)-1].Execute(context.Background()))
				require.Equal(t, uint64(110), replay.CompletedPoint().TimeTick)
			} else {
				require.Equal(t, uint64(110), tracker.CompletedPoint().TimeTick)
				outputErr = nil
				require.Len(t, tasks, 2)
				require.NoError(t, tasks[1].Execute(context.Background()))
				require.Equal(t, uint64(130), tracker.CompletedPoint().TimeTick)
			}
		})
	}
}
