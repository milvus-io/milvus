package walsummary

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/messageack"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func TestSummaryBacklogFlushesAfterSourceAckWithoutNewMessages(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager, store := newTransformTestManagerWithStore(t)
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	manager.cfg.Runtime = moduleapi.Runtime{Scheduler: scheduler}
	manager.cfg.FlushMaxBytes = 1 << 30
	tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
	msg := newTestDeleteMessage(t, "v1", 100, 10, 1)
	owner := tracker.Track(msg)
	manager.ObserveMessage(ctx, owner.Message())
	owner.Release()
	require.Zero(t, tracker.Pending())
	require.Equal(t, uint64(100), tracker.CompletedPoint().TimeTick)
	require.Less(t, manager.LastAcked(), uint64(100))
	done := make(chan struct{})
	go func() {
		defer close(done)
		manager.Run(ctx, time.Millisecond, nil)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("summary backlog worker did not stop")
		}
	})
	require.Eventually(t, func() bool {
		return manager.LastAcked() == 100 && !manager.HasPendingWork()
	}, 5*time.Second, time.Millisecond)
	recovered := newTestManager(t, store, 1<<30)
	require.NoError(t, recovered.Restore(ctx))
	entries, err := recovered.ReadTransformEntries(ctx, "v1", 0, 100)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, uint64(100), entries[0].GetTimeTick())
}

func TestSummaryBacklogAgeAndPressure(t *testing.T) {
	manager, _ := newTransformTestManagerWithStore(t)
	ctx := context.Background()
	manager.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 100, 10, 1))
	start := manager.pendingSince
	manager.ObserveMessage(ctx, newTestDeleteMessage(t, "v2", 200, 10, 2))
	require.Equal(t, start, manager.pendingSince, "new traffic must not postpone the oldest record")
	manager.flushBacklog(start.Add(time.Second), time.Minute, false)
	require.Empty(t, manager.pendingSealed)
	manager.flushBacklog(start.Add(time.Minute), time.Minute, false)
	require.Len(t, manager.pendingSealed, 1)
	require.True(t, manager.pendingSince.IsZero())
	require.NoError(t, drainSummary(ctx, manager))
	manager.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 300, 10, 3))
	manager.flushBacklog(manager.pendingSince, time.Hour, true)
	require.Len(t, manager.pendingSealed, 1, "pressure must flush a young small batch")
	manager.flushBacklog(time.Now(), time.Hour, true)
	require.Len(t, manager.pendingSealed, 1, "an empty backlog creates no duplicate chunk")
	require.NoError(t, drainSummary(ctx, manager))
	require.Equal(t, uint64(300), manager.LastAcked())
}

func TestAsyncSchedulerPersistsAndRestores(t *testing.T) {
	ctx := context.Background()
	manager, store := newTransformTestManagerWithStore(t)
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	manager.cfg.Runtime = moduleapi.Runtime{Scheduler: scheduler}
	manager.cfg.FlushMaxBytes = 1
	for i := uint64(1); i <= 50; i++ {
		manager.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", i, 10, int64(i)))
	}
	require.Eventually(t, func() bool {
		checkpoint := manager.LastAcked()
		return checkpoint == 50 && !manager.HasPendingWork()
	}, 5*time.Second, time.Millisecond)
	recovered := newTestManager(t, store, 1<<30)
	require.NoError(t, recovered.Restore(ctx))
	entries, err := recovered.ReadTransformEntries(ctx, "v1", 0, 50)
	require.NoError(t, err)
	require.Len(t, entries, 50)
	for i, entry := range entries {
		require.Equal(t, uint64(i+1), entry.GetTimeTick())
	}
}

func TestAsyncIdempotencyReadersAcrossPendingAndDurableState(t *testing.T) {
	ctx := context.Background()
	manager, store := newTestManagerWithStore(t)
	require.Empty(t, manager.IdempotencyVChannels())
	manager.ObserveMessage(ctx, newTestIdempotentInsertMessage(t, "v1", 100, "key-1", []int64{1}, []uint32{0}))
	require.Equal(t, []string{"v1"}, manager.IdempotencyVChannels())
	manager.RequestFlushThrough(100)
	manager.ObserveMessage(ctx, newTestIdempotentInsertMessage(t, "v2", 200, "key-2", []int64{2}, []uint32{0}))
	require.Equal(t, []string{"v1", "v2"}, manager.IdempotencyVChannels())
	require.NoError(t, drainSummary(ctx, manager))
	chunk := manager.Manifest().GetChunks()[0]
	sections, err := store.ReadIdempotencySection(ctx, chunk.GetGeneration(), chunk.GetTerm(), "v1", chunk.GetVchannels()[0])
	require.NoError(t, err)
	require.Equal(t, "key-1", sections.Idempotency[0].GetKey())
	manager.ObserveMessage(ctx, newTestDropCollectionMessage(t, "v1", 300))
	manager.RequestFlushThrough(300)
	require.Equal(t, []string{"v1", "v2"}, manager.IdempotencyVChannels())
	require.NoError(t, drainSummary(ctx, manager))
	recovered := newTestManager(t, store, 1<<30)
	require.NoError(t, recovered.Restore(ctx))
	require.Equal(t, []string{"v1", "v2"}, recovered.IdempotencyVChannels())
	retained, err := recovered.ReadIdempotencyEntries(ctx, "v1", 0, 1000)
	require.NoError(t, err)
	require.Len(t, retained.Inserts, 1)
	// A missing durable object must fail restoration of the consumer window,
	// rather than silently forgetting a key that already reached the WAL.
	last := recovered.Manifest().GetChunks()[1]
	require.NoError(t, store.DeleteChunk(ctx, last.GetGeneration(), last.GetTerm()))
	_, err = recovered.ReadIdempotencyEntries(ctx, "v2", 0, 1000)
	require.Error(t, err)
	_, err = store.ReadIdempotencySection(ctx, last.GetGeneration(), last.GetTerm(), "v2", last.GetVchannels()[0])
	require.Error(t, err)
	// No consumers means no object read, even when the chunk no longer exists.
	empty, err := store.ReadIdempotencySectionsOfChunk(ctx, last.GetGeneration(), last.GetTerm(), nil)
	require.NoError(t, err)
	require.Empty(t, empty)
}

func TestAsyncRestoredFrontierAndNonRecordMessages(t *testing.T) {
	ctx := context.Background()
	manager, _ := newTestManagerWithStore(t)
	require.Zero(t, manager.LastAcked())
	manager.InitLastAcked(0)
	manager.InitLastAcked(0)
	require.Zero(t, manager.LastAcked())
	checkpoint := &utility.WALCheckpoint{MessageID: walimplstest.NewTestMessageID(10), TimeTick: 10}
	manager.InitLastAcked(checkpoint.TimeTick)
	checkpoint.TimeTick = 0
	manager.InitLastAcked(checkpoint.TimeTick)
	require.Equal(t, uint64(10), manager.LastAcked())
	manager.ObserveMessage(ctx, newTestBarrierMessage(t, "v1", 20))
	require.Equal(t, uint64(20), manager.LastAcked())
	require.False(t, manager.HasPendingWork())
}

func TestAsyncThresholdAndSourceRelease(t *testing.T) {
	manager, _ := newTransformTestManagerWithStore(t)
	manager.cfg.FlushMaxBytes = 1
	scheduler := manager.cfg.Runtime.Scheduler.(*recordingScheduler)
	var finalized bool
	observeTransformDelete(t, manager, "v1", 100, &finalized)
	require.True(t, finalized, "async summary must not retain the source message")
	require.Len(t, scheduler.tasks, 1)
	require.True(t, manager.HasPendingWork())
	require.Less(t, manager.LastAcked(), uint64(100))
	require.Empty(t, manager.Manifest().GetChunks())
	require.NoError(t, drainSummary(context.Background(), manager))
	require.Equal(t, uint64(100), manager.LastAcked())
	require.False(t, manager.HasPendingWork())
	manager.RequestFlushThrough(100)
	require.Len(t, scheduler.tasks, 2, "a covered target must not create another task")
}

func TestAsyncConfirmationIncludesBarrierBeforeLaterPendingData(t *testing.T) {
	ctx := context.Background()
	manager, _ := newTransformTestManagerWithStore(t)
	manager.InitLastAcked(1)
	manager.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 100, 10, 1))
	manager.ObserveMessage(ctx, newTestBarrierMessage(t, "v1", 200))
	require.Equal(t, uint64(1), manager.LastAcked())
	manager.RequestFlushThrough(200)
	manager.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 300, 10, 2))
	scheduler := manager.cfg.Runtime.Scheduler.(*recordingScheduler)
	require.Len(t, scheduler.tasks, 1)
	require.NoError(t, drainSummary(ctx, manager))
	require.Equal(t, uint64(200), manager.LastAcked(),
		"the sealed batch covers the barrier but cannot cover later staged data")
	require.True(t, manager.HasPendingWork())
	manager.RequestFlushThrough(300)
	require.Len(t, scheduler.tasks, 3)
	require.NoError(t, drainSummary(ctx, manager))
	require.Equal(t, uint64(300), manager.LastAcked())
}

func TestAsyncDDLDoesNotFlushOrForgetRequests(t *testing.T) {
	ctx := context.Background()
	manager, store := newTestManagerWithStore(t)
	manager.ObserveMessage(ctx, newTestIdempotentInsertMessage(t, "v1", 100, "key", []int64{1}, []uint32{0}))
	manager.ObserveMessage(ctx, newTestDropCollectionMessage(t, "v1", 200))
	require.Len(t, manager.pending, 1)
	require.Less(t, manager.LastAcked(), uint64(100))
	scheduler := manager.cfg.Runtime.Scheduler.(*recordingScheduler)
	require.Empty(t, scheduler.tasks, "DDL creates no summary persistence work")
	manager.RequestFlushThrough(200)
	require.Len(t, scheduler.tasks, 1)
	require.NoError(t, drainSummary(ctx, manager))
	_, exists, err := store.ReadManifest(ctx)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, uint64(200), manager.LastAcked())
	require.False(t, manager.HasPendingWork())
	manager.ObserveMessage(ctx, newTestDropCollectionMessage(t, "v1", 300))
	require.Equal(t, uint64(300), manager.LastAcked())
	require.Len(t, scheduler.tasks, 2, "a DDL after durable data needs no new task")
}

func TestAsyncWriteFailurePinsConfirmation(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		name := "retry"
		if terminal {
			name = "corruption"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			manager, _ := newTransformTestManagerWithStore(t)
			manager.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 100, 10, 1))
			manager.RequestFlushThrough(100)
			require.NoError(t, manager.pendingSealed[0].task.Execute(ctx))
			task := manager.manifestTask
			failure := errors.New("object store unavailable")
			if terminal {
				failure = storeCorruptedf("corrupt summary object")
			}
			patch := mockey.Mock((*Store).WriteManifest).To(func(*Store, context.Context, *streamingpb.PChannelSummaryManifest) error {
				return failure
			}).Build()
			t.Cleanup(func() { patch.UnPatch() })
			err := task.Execute(ctx)
			require.ErrorIs(t, err, failure)
			require.Contains(t, err.Error(), failure.Error())
			require.Less(t, manager.LastAcked(), uint64(100))
			if terminal {
				require.ErrorIs(t, manager.terminalErr, failure)
			}
			patch.UnPatch()
			if terminal {
				manager.ObserveMessage(ctx, newTestBarrierMessage(t, "v1", 200))
				manager.RequestFlushThrough(200)
				require.Less(t, manager.LastAcked(), uint64(100))
				require.Len(t, manager.cfg.Runtime.Scheduler.(*recordingScheduler).tasks, 2)
				return
			}
			require.True(t, errors.Is(err, nodescheduler.ErrDelay))
			require.NoError(t, task.Execute(ctx))
			require.Equal(t, uint64(100), manager.LastAcked())
			require.Len(t, manager.Manifest().GetChunks(), 1)
			require.Equal(t, uint64(0), manager.Manifest().GetChunks()[0].GetGeneration())
		})
	}
}

func TestAsyncRestorePropagatesStorageFailures(t *testing.T) {
	for _, operation := range []string{"manifest", "probe", "list"} {
		t.Run(operation, func(t *testing.T) {
			ctx := context.Background()
			original, store := newTestManagerWithStore(t)
			original.ObserveMessage(ctx, newTestIdempotentInsertMessage(t, "v1", 100, "key", []int64{1}, []uint32{0}))
			require.NoError(t, persistSummary(ctx, original))
			recovered := newTestManager(t, NewStore(store.chunkManager, store.PChannel(), 2), 1<<30)
			failure := errors.New("summary recovery storage failure")
			var patch *mockey.Mocker
			switch operation {
			case "manifest":
				patch = mockey.Mock((*Store).ReadManifestOfTerm).Return(nil, false, failure).Build()
			case "probe":
				patch = mockey.Mock((*Store).ProbeChunkForwardOfTerm).Return(nil, failure).Build()
			case "list":
				patch = mockey.Mock((*Store).ListManifestTerms).Return(nil, failure).Build()
			case "publish":
				patch = mockey.Mock((*Store).WriteManifest).Return(failure).Build()
			}
			defer patch.UnPatch()
			require.ErrorIs(t, recovered.Restore(ctx), failure)
			require.Zero(t, recovered.LastAcked(), "failed recovery must not confirm a WAL position")
			patch.UnPatch()
			require.NoError(t, recovered.Restore(ctx))
			records, err := recovered.ReadIdempotencyEntries(ctx, "v1", 0, 100)
			require.NoError(t, err)
			require.Len(t, records.Inserts, 1)
		})
	}
}

func TestSummaryStoreRemovalIsScopedToPChannel(t *testing.T) {
	ctx := context.Background()
	manager, store := newTestManagerWithStore(t)
	manager.ObserveMessage(ctx, newTestIdempotentInsertMessage(t, "v1", 100, "key", []int64{1}, []uint32{0}))
	require.NoError(t, persistSummary(ctx, manager))
	other := NewStore(store.chunkManager, store.PChannel()+"-other", 1)
	require.NoError(t, other.WriteManifest(ctx, &streamingpb.PChannelSummaryManifest{}))
	require.NoError(t, store.RemoveAllObjects(ctx))
	_, found, err := store.ReadManifest(ctx)
	require.NoError(t, err)
	require.False(t, found)
	exists, err := store.chunkManager.Exist(ctx, store.ChunkKey(0))
	require.NoError(t, err)
	require.False(t, exists)
	_, found, err = other.ReadManifest(ctx)
	require.NoError(t, err)
	require.True(t, found, "removing one pchannel must preserve another pchannel's summary")
}

func TestSummaryRejectsCorruptMetadataBeforeConsumerRecovery(t *testing.T) {
	payload, _, err := marshalChunk("p1", 1, 1, writeSections(map[string][]uint64{"v1": {100}}), testRecordCoverage(writeSections(map[string][]uint64{"v1": {100}})))
	require.NoError(t, err)
	_, footerStart, err := unmarshalChunkTail(payload)
	require.NoError(t, err)
	checksumStart := len(payload) - len(chunkFooterMagic) - 4 - sha256.Size
	for _, damage := range []string{"version", "header size", "footer length", "footer protobuf"} {
		t.Run("chunk "+damage, func(t *testing.T) {
			bad := append([]byte(nil), payload...)
			switch damage {
			case "version":
				binary.BigEndian.PutUint16(bad[8:10], codecVersion+1)
			case "header size":
				binary.BigEndian.PutUint32(bad[12:16], 0)
			case "footer length":
				binary.BigEndian.PutUint32(bad[checksumStart+sha256.Size:], uint32(len(bad)))
			case "footer protobuf":
				for i := int(footerStart); i < checksumStart; i++ {
					bad[i] = 0xff
				}
				checksum := sha256.Sum256(bad[footerStart:checksumStart])
				copy(bad[checksumStart:], checksum[:])
			}
			_, _, err := unmarshalChunk(bad)
			require.ErrorIs(t, err, ErrStoreCorrupted)
			require.NotEmpty(t, err.Error())
		})
	}
	manifest, err := marshalManifest(&streamingpb.PChannelSummaryManifest{
		Chunks: []*streamingpb.PChannelSummaryChunkIndexEntry{{Generation: 1}},
	})
	require.NoError(t, err)
	for _, damage := range []string{"version", "length", "protobuf"} {
		t.Run("manifest "+damage, func(t *testing.T) {
			bad := append([]byte(nil), manifest...)
			switch damage {
			case "version":
				binary.BigEndian.PutUint16(bad[8:10], manifestVersion+1)
			case "length":
				binary.BigEndian.PutUint32(bad[10:14], uint32(len(bad)))
			case "protobuf":
				end := len(bad) - sha256.Size
				for i := manifestHeader; i < end; i++ {
					bad[i] = 0xff
				}
				checksum := sha256.Sum256(bad[manifestHeader:end])
				copy(bad[end:], checksum[:])
			}
			_, err := unmarshalManifest(bad)
			require.ErrorIs(t, err, ErrStoreCorrupted)
			require.NotEmpty(t, err.Error())
		})
	}
}

func TestSummaryBootstrapSkipsCheckpointReplay(t *testing.T) {
	ctx := context.Background()
	m, store := newTransformTestManagerWithStore(t)
	m.InitLastAcked(100)
	m.InitLastAcked(50)
	for _, tt := range []uint64{90, 100} {
		m.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", tt, 10, 1))
		m.ObserveMessage(ctx, newTestIdempotentInsertMessage(t, "v1", tt, "old", []int64{1}, []uint32{0}))
	}
	require.Empty(t, m.pending)
	require.Nil(t, m.Manifest().Coverage, "bootstrap does not create stored history")
	require.Equal(t, uint64(100), m.LastAcked())

	m.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 101, 10, 2))
	m.ObserveMessage(ctx, newTestIdempotentInsertMessage(t, "v1", 102, "new", []int64{2}, []uint32{0}))
	require.NoError(t, persistSummary(ctx, m))
	require.Nil(t, m.terminalErr)
	require.Equal(t, uint64(101), m.Manifest().Coverage.StartTimeTick)
	require.Equal(t, uint64(102), m.Manifest().Coverage.EndTimeTick)
	require.Equal(t, uint64(102), m.LastAcked())
	entries, err := m.ReadIdempotencyEntries(ctx, "v1", 0, 200)
	require.NoError(t, err)
	require.Len(t, entries.Idempotency, 1)
	batch, err := m.ReadTransform(ctx, "v1", 0, 102, ReadLimits{})
	require.NoError(t, err)
	require.Equal(t, uint64(100), batch.FastForwardTimeTick)
	require.Len(t, batch.Entries, 1)
	restored := newTestManager(t, nextTermStore(store), 1<<30)
	require.NoError(t, restored.Restore(ctx))
	batch, err = restored.ReadTransform(ctx, "v1", 0, 102, ReadLimits{})
	require.NoError(t, err)
	require.Equal(t, uint64(100), batch.FastForwardTimeTick)
	require.Len(t, batch.Entries, 1)
}
