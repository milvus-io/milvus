package broadcaster

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func schemaTestMessage(collection int64, id uint64) message.BroadcastMutableMessage {
	return message.NewAlterCollectionMessageBuilderV2().WithHeader(&message.AlterCollectionMessageHeader{
		CollectionId: collection, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionSchema}},
	}).WithBody(&message.AlterCollectionMessageBody{Updates: &message.AlterCollectionMessageUpdates{Schema: &schemapb.CollectionSchema{Version: 2}}}).
		WithIdempotencyKey(message.NewCollectionScopedIdempotencyKey(collection, fmt.Sprint(id))).
		WithBroadcast([]string{"p" + funcutil.ControlChannelSuffix, "v1"}).MustBuildBroadcast().WithBroadcastID(id)
}

type schemaTestCatalog struct {
	metastore.StreamingCoordCataLog
}

func (*schemaTestCatalog) SaveBroadcastTask(context.Context, uint64, *streamingpb.BroadcastTask) error {
	panic("unpatched")
}

func (*schemaTestCatalog) ListBroadcastTask(context.Context) ([]*streamingpb.BroadcastTask, error) {
	panic("unpatched")
}

func (bm *broadcastTaskManager) schemaTaskCount(collectionID int64) int {
	bm.schemaTasks.mu.RLock()
	defer bm.schemaTasks.mu.RUnlock()
	if tasks := bm.schemaTasks.collections[collectionID]; tasks != nil {
		return tasks.pending.Len()
	}
	return 0
}

func requireSchemaTarget(t *testing.T, bm *broadcastTaskManager, collectionID int64, id uint64) *SchemaTarget {
	t.Helper()
	target, err := bm.GetPendingSchemaTarget(collectionID)
	require.NoError(t, err)
	if id == 0 {
		require.Nil(t, target)
	} else {
		require.NotNil(t, target)
		require.Equal(t, id, target.BroadcastID)
	}
	return target
}

func schemaTestAck(msg message.BroadcastMutableMessage, channel string, tick uint64) message.ImmutableMessage {
	for _, part := range msg.SplitIntoMutableMessage() {
		if part.VChannel() == channel {
			return part.WithReplicateHeader(&message.ReplicateHeader{
				ClusterID: "source", MessageID: walimplstest.NewTestMessageID(2), LastConfirmedMessageID: walimplstest.NewTestMessageID(1), TimeTick: tick,
			}).WithTimeTick(tick).WithLastConfirmed(walimplstest.NewTestMessageID(1)).IntoImmutableMessage(walimplstest.NewTestMessageID(2))
		}
	}
	panic("missing test channel")
}

func TestSchemaTargetPersistenceBoundary(t *testing.T) {
	mockey.PatchConvey("only successful saves publish snapshots and failed saves remain retryable", t, func() {
		paramtable.Init()
		resource.InitForTest(resource.OptStreamingCatalog(&schemaTestCatalog{}))
		bm := newBroadcastTaskManager(nil)
		defer bm.Close()
		msg := schemaTestMessage(100, 1)
		task, created := bm.getOrAddBroadcastTask(t.Context(), msg, 1, bm.resourceKeyLocker.Lock(message.NewSharedClusterResourceKey()))
		require.True(t, created)
		requireSchemaTarget(t, bm, 100, 0)
		require.Zero(t, bm.schemaTaskCount(100))
		require.Nil(t, task.schemaForTarget, "registration must not decode the body")

		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		save := mockey.Mock((*schemaTestCatalog).SaveBroadcastTask).Return(context.Canceled).Build()
		require.ErrorIs(t, task.InitializeRecovery(ctx), context.Canceled)
		require.True(t, task.dirty)
		require.Nil(t, task.schemaForTarget, "failed persistence must not populate the schema cache")
		requireSchemaTarget(t, bm, 100, 0)
		save.UnPatch()

		// Lookup while metadata I/O holds the task lock must still complete.
		entered, release := make(chan struct{}), make(chan struct{})
		save = mockey.Mock((*schemaTestCatalog).SaveBroadcastTask).To(func(_ *schemaTestCatalog, _ context.Context, _ uint64, _ *streamingpb.BroadcastTask) error {
			close(entered)
			<-release
			return nil
		}).Build()
		done := make(chan error, 1)
		go func() { done <- task.InitializeRecovery(t.Context()) }()
		<-entered
		requireSchemaTarget(t, bm, 100, 0)
		close(release)
		require.NoError(t, <-done)
		save.UnPatch()
		first := requireSchemaTarget(t, bm, 100, 1)
		require.Empty(t, first.TimeTicks, "initial persistence needs neither WAL append nor ACK")
		require.False(t, task.dirty)
		cachedSchema := task.schemaForTarget
		require.Same(t, cachedSchema, first.Schema, "readers share the immutable schema")

		// Failed ACK persistence must not leak its checkpoint into the snapshot.
		mockey.Mock(registry.CallMessageAckOnceCallbacks).Return(nil).Build()
		save = mockey.Mock((*schemaTestCatalog).SaveBroadcastTask).Return(context.Canceled).Build()
		ack := schemaTestAck(msg, "v1", 10)
		require.ErrorIs(t, task.Ack(ctx, ack), context.Canceled)
		require.Empty(t, requireSchemaTarget(t, bm, 100, 1).TimeTicks)
		save.UnPatch()
		save = mockey.Mock((*schemaTestCatalog).SaveBroadcastTask).Return(nil).Build()
		require.NoError(t, task.Ack(t.Context(), ack))
		require.NoError(t, task.Ack(t.Context(), ack), "duplicate ACK must not decode again")
		require.Same(t, cachedSchema, bm.schemaTasks.collections[100].head.Schema, "index updates must not clone the cached schema")
		snapshot := requireSchemaTarget(t, bm, 100, 1)
		require.Equal(t, map[string]uint64{"v1": 10}, snapshot.TimeTicks)
		require.Same(t, cachedSchema, snapshot.Schema)
		snapshot.TimeTicks["v1"] = 900
		again := requireSchemaTarget(t, bm, 100, 1)
		require.Same(t, cachedSchema, again.Schema)
		require.EqualValues(t, 10, again.TimeTicks["v1"])
		require.Empty(t, first.TimeTicks, "older snapshots remain immutable")
		save.UnPatch()

		save = mockey.Mock((*schemaTestCatalog).SaveBroadcastTask).Return(context.Canceled).Build()
		require.ErrorIs(t, task.MarkAckCallbackDone(ctx), context.Canceled)
		requireSchemaTarget(t, bm, 100, 1)
		save.UnPatch()
		mockey.Mock((*schemaTestCatalog).SaveBroadcastTask).Return(nil).Build()
		require.NoError(t, task.MarkAckCallbackDone(t.Context()))
		requireSchemaTarget(t, bm, 100, 0)
		require.Contains(t, bm.tasks, uint64(1), "retirement preserves the tombstone and deduplication")
		require.NoError(t, task.MarkAckCallbackDone(t.Context()))
		require.Nil(t, task.schemaForTarget, "completion releases the task's cached schema")
		require.NoError(t, task.DropTombstone(t.Context()))
		bm.removeBroadcastTask(1)
		require.Zero(t, bm.schemaTaskCount(100))
	})
}

func TestSchemaTargetSecondaryAndRecovery(t *testing.T) {
	mockey.PatchConvey("first local ACK persistence is enough; backlog survives shuffled catalog recovery", t, func() {
		paramtable.Init()
		catalog := make(map[uint64]*streamingpb.BroadcastTask)
		mockey.Mock((*schemaTestCatalog).SaveBroadcastTask).To(func(_ *schemaTestCatalog, _ context.Context, id uint64, task *streamingpb.BroadcastTask) error {
			catalog[id] = proto.Clone(task).(*streamingpb.BroadcastTask)
			return nil
		}).Build()
		mockey.Mock(registry.CallMessageAckOnceCallbacks).Return(nil).Build()
		resource.InitForTest(resource.OptStreamingCatalog(&schemaTestCatalog{}))
		bm := newBroadcastTaskManager(nil)
		defer bm.Close()
		// IDs straddle the lexicographic key boundary. Schemas deliberately share
		// a version, as legacy ALTERs need not increment it.
		for _, id := range []uint64{9, 10, 11} {
			ack := schemaTestAck(schemaTestMessage(100, id), "v1", id)
			task, found := bm.getOrCreateBroadcastTask(ack)
			require.True(t, found)
			require.Equal(t, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_REPLICATED, task.State())
			require.Equal(t, int(id-9), bm.schemaTaskCount(100), "registration alone is not durable")
			require.NoError(t, bm.Ack(t.Context(), ack))
			require.NoError(t, bm.Ack(t.Context(), ack), "duplicate ACK is idempotent")
			requireSchemaTarget(t, bm, 100, 9)
		}
		require.Equal(t, 3, bm.schemaTaskCount(100))
		// Simulate process loss: recover exclusively from the persisted task protos.
		mockey.Mock((*schemaTestCatalog).ListBroadcastTask).To(func(*schemaTestCatalog, context.Context) ([]*streamingpb.BroadcastTask, error) {
			return []*streamingpb.BroadcastTask{proto.Clone(catalog[11]).(*streamingpb.BroadcastTask), proto.Clone(catalog[9]).(*streamingpb.BroadcastTask), proto.Clone(catalog[10]).(*streamingpb.BroadcastTask)}, nil
		}).Build()
		recovered, err := RecoverBroadcaster(t.Context())
		require.NoError(t, err)
		next := recovered.(*broadcastTaskManager)
		defer next.Close()
		require.Equal(t, 3, next.schemaTaskCount(100))
		require.Equal(t, map[string]uint64{"v1": 9}, requireSchemaTarget(t, next, 100, 9).TimeTicks)
		for _, id := range []uint64{9, 10, 11} {
			requireSchemaTarget(t, next, 100, id)
			require.NoError(t, next.tasks[id].MarkAckCallbackDone(t.Context()))
			require.NoError(t, next.tasks[id].MarkAckCallbackDone(t.Context()))
			if id < 11 {
				requireSchemaTarget(t, next, 100, id+1)
			}
			next.removeBroadcastTask(id)
		}
		requireSchemaTarget(t, next, 100, 0)
		requireSchemaTarget(t, next, 999, 0)
	})
}

func TestSchemaTargetCrashBeforeIndexUpdate(t *testing.T) {
	mockey.PatchConvey("a save followed by process loss rebuilds the index without another ACK", t, func() {
		paramtable.Init()
		var persisted *streamingpb.BroadcastTask
		mockey.Mock((*schemaTestCatalog).SaveBroadcastTask).To(func(_ *schemaTestCatalog, _ context.Context, _ uint64, task *streamingpb.BroadcastTask) error {
			persisted = proto.Clone(task).(*streamingpb.BroadcastTask)
			return nil
		}).Build()
		resource.InitForTest(resource.OptStreamingCatalog(&schemaTestCatalog{}))
		bm := newBroadcastTaskManager(nil)
		defer bm.Close()
		task, _ := bm.getOrAddBroadcastTask(t.Context(), schemaTestMessage(100, 1), 1, bm.resourceKeyLocker.Lock(message.NewSharedClusterResourceKey()))
		lostUpdate := mockey.Mock((*schemaTaskIndex).update).Return().Build()
		require.NoError(t, task.InitializeRecovery(t.Context()))
		requireSchemaTarget(t, bm, 100, 0)
		lostUpdate.UnPatch()
		recovered := newBroadcastTaskManager([]*streamingpb.BroadcastTask{persisted})
		defer recovered.Close()
		requireSchemaTarget(t, recovered, 100, 1)
		// No task/manager locks are required even for the selected target.
		recovered.mu.Lock()
		recovered.tasks[1].mu.Lock()
		requireSchemaTarget(t, recovered, 100, 1)
		recovered.tasks[1].mu.Unlock()
		recovered.mu.Unlock()
	})
}

func TestSchemaTargetIndexExclusions(t *testing.T) {
	paramtable.Init()
	for _, tc := range []struct {
		name   string
		path   string
		schema *schemapb.CollectionSchema
		state  streamingpb.BroadcastTaskState
	}{
		{name: "properties", path: message.FieldMaskCollectionProperties, schema: &schemapb.CollectionSchema{}},
		{name: "missing schema", path: message.FieldMaskCollectionSchema},
		{name: "tombstone", path: message.FieldMaskCollectionSchema, state: streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE},
		{name: "done", path: message.FieldMaskCollectionSchema, state: streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_DONE},
	} {
		t.Run(tc.name, func(t *testing.T) {
			msg := message.NewAlterCollectionMessageBuilderV2().WithHeader(&message.AlterCollectionMessageHeader{
				CollectionId: 100, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{tc.path}},
			}).WithBody(&message.AlterCollectionMessageBody{Updates: &message.AlterCollectionMessageUpdates{Schema: tc.schema}}).
				WithBroadcast([]string{"v1"}).MustBuildBroadcast().WithBroadcastID(1)
			pb := createNewWaitAckBroadcastTaskFromMessage(msg, tc.state, []byte{1})
			if tc.name == "tombstone" || tc.name == "done" {
				pb.Message.Payload = []byte{0xff} // Completed bodies must not be decoded.
			}
			index := newSchemaTaskIndex()
			task := newBroadcastTaskFromProto(pb, newBroadcasterMetrics(), nil)
			require.NotPanics(t, func() { index.update(task) })
			require.Empty(t, index.collections)
		})
	}
}

// Exercise the real scheduler: schema readability must not authorize callbacks.
func TestSchemaTargetRecoveredCallbackOrder(t *testing.T) {
	mockey.PatchConvey("persisted targets are readable before ACKs, callbacks remain ordered", t, func() {
		paramtable.Init()
		mockey.Mock((*schemaTestCatalog).SaveBroadcastTask).Return(nil).Build()
		resource.InitForTest(resource.OptStreamingCatalog(&schemaTestCatalog{}))
		entered := make(chan uint64, 2)
		release := make(chan struct{})
		mockey.Mock(registry.CallMessageAckCallback).To(func(ctx context.Context, msg message.BroadcastMutableMessage, _ map[string]*message.AppendResult) error {
			entered <- msg.BroadcastHeader().BroadcastID
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}).Build()
		newTask := func(id, tick uint64, allAcked bool) *streamingpb.BroadcastTask {
			msg := schemaTestMessage(100, id).OverwriteBroadcastHeader(id, message.NewExclusiveCollectionNameResourceKey("db", "collection"))
			task := createNewWaitAckBroadcastTaskFromMessage(msg, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_REPLICATED, []byte{1, 1})
			for i, channel := range msg.BroadcastHeader().VChannels {
				task.AckedCheckpoints[i].TimeTick = tick
				if !allAcked && !funcutil.IsControlChannel(channel) {
					task.AckedVchannelBitmap[i] = 0
					task.AckedCheckpoints[i] = nil
				}
			}
			return task
		}
		// Catalog order is reversed; IDs follow serial ALTER allocation order.
		bm := newBroadcastTaskManager([]*streamingpb.BroadcastTask{newTask(200, 20, true), newTask(100, 10, false)})
		defer bm.Close()
		target, err := bm.GetPendingSchemaTarget(100)
		require.NoError(t, err)
		require.EqualValues(t, 100, target.BroadcastID)
		require.Empty(t, target.TimeTicks)
		select {
		case id := <-entered:
			t.Fatalf("callback %d ran before predecessor ACKs", id)
		default:
		}
		first := bm.tasks[100]
		first.mu.Lock()
		for i, channel := range first.header().VChannels {
			if !funcutil.IsControlChannel(channel) {
				first.task.AckedVchannelBitmap[i] = 1
				first.task.AckedCheckpoints[i] = &streamingpb.AckedCheckpoint{TimeTick: 11, MessageId: walimplstest.NewTestMessageID(1).IntoProto(), LastConfirmedMessageId: walimplstest.NewTestMessageID(1).IntoProto()}
			}
		}
		first.dirty = true
		require.NoError(t, first.saveTaskIfDirty(t.Context(), first.Logger()))
		first.closeAllAcked()
		first.mu.Unlock()
		select {
		case id := <-entered:
			require.EqualValues(t, 100, id)
		case <-time.After(5 * time.Second):
			t.Fatal("first callback did not enter")
		}
		target, err = bm.GetPendingSchemaTarget(100)
		require.NoError(t, err)
		require.EqualValues(t, 100, target.BroadcastID)
		require.Equal(t, map[string]uint64{"v1": 11}, target.TimeTicks)
		var readers sync.WaitGroup
		for n := 0; n < 8; n++ {
			readers.Add(1)
			go func() {
				defer readers.Done()
				for i := 0; i < 50; i++ {
					snapshot, err := bm.GetPendingSchemaTarget(100)
					require.NoError(t, err)
					if snapshot != nil {
						require.Contains(t, []uint64{100, 200}, snapshot.BroadcastID)
						require.EqualValues(t, 2, snapshot.Schema.Version)
						snapshot.TimeTicks["v1"] = 900
					}
				}
			}()
		}
		release <- struct{}{}
		select {
		case id := <-entered:
			require.EqualValues(t, 200, id)
		case <-time.After(5 * time.Second):
			t.Fatal("next callback did not enter")
		}
		readers.Wait()
		target, err = bm.GetPendingSchemaTarget(100)
		require.NoError(t, err)
		require.EqualValues(t, 200, target.BroadcastID)
		require.Empty(t, target.Schema.Name)
		require.EqualValues(t, 20, target.TimeTicks["v1"])
		release <- struct{}{}
		require.Eventually(t, func() bool { return bm.schemaTaskCount(100) == 0 }, 5*time.Second, time.Millisecond)
		target, err = bm.GetPendingSchemaTarget(100)
		require.NoError(t, err)
		require.Nil(t, target)
	})
}
