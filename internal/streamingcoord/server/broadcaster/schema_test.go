package broadcaster

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestSchemaTargetFollowsOrderedCallback(t *testing.T) {
	paramtable.Init()
	metrics := newBroadcasterMetrics()
	newTask := func(id, tick uint64, version int32) *broadcastTask {
		msg := message.NewAlterCollectionMessageBuilderV2().WithHeader(&message.AlterCollectionMessageHeader{
			CollectionId: 100, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionSchema}},
		}).WithBody(&message.AlterCollectionMessageBody{Updates: &message.AlterCollectionMessageUpdates{Schema: &schemapb.CollectionSchema{Version: version, Name: "original"}}}).
			WithBroadcast([]string{"p" + funcutil.ControlChannelSuffix, "v1", "v2"}).MustBuildBroadcast().WithBroadcastID(id)
		pb := createNewWaitAckBroadcastTaskFromMessage(msg, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING, []byte{1, 1, 1})
		// Broadcast construction does not preserve input channel order.
		channelTicks := map[string]uint64{"p" + funcutil.ControlChannelSuffix: tick, "v1": tick + 1, "v2": tick + 2}
		for i, channel := range msg.BroadcastHeader().VChannels {
			pb.AckedCheckpoints[i].TimeTick = channelTicks[channel]
		}
		return newBroadcastTaskFromProto(pb, metrics, nil)
	}
	// WAL order is authoritative even if recovered IDs have a different order.
	first, second := newTask(200, 10, 1), newTask(100, 20, 2)
	bm := &broadcastTaskManager{mu: &sync.Mutex{}, tasks: map[uint64]*broadcastTask{100: second, 200: first}, schemaTasks: make(schemaTaskIndex), idempotencyIndex: newIdempotencyIndex()}
	bm.schemaTasks.add(100, second)
	bm.schemaTasks.add(200, first)
	_, err := bm.GetPendingSchemaTarget(100)
	require.ErrorIs(t, err, merr.ErrServiceNotReady, "recovery alone cannot authorize a target")
	bm.admitSchemaTarget(first)
	got, err := bm.GetPendingSchemaTarget(100)
	require.NoError(t, err)
	require.EqualValues(t, 200, got.BroadcastID)
	require.EqualValues(t, 1, got.Schema.Version)
	require.Equal(t, map[string]uint64{"v1": 11, "v2": 12}, got.TimeTicks)
	got.Schema.Name = "changed"
	got.TimeTicks["v1"] = 900
	again, err := bm.GetPendingSchemaTarget(100)
	require.NoError(t, err)
	require.Equal(t, "original", again.Schema.Name)
	require.EqualValues(t, 11, again.TimeTicks["v1"])
	first.task.State = streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE
	bm.removeBroadcastTask(200)
	_, err = bm.GetPendingSchemaTarget(100)
	require.ErrorIs(t, err, merr.ErrServiceNotReady, "successor requires scheduler admission")
	bm.admitSchemaTarget(second)
	got, err = bm.GetPendingSchemaTarget(100)
	require.NoError(t, err)
	require.EqualValues(t, 2, got.Schema.Version)
	second.task.State = streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE
	got, err = bm.GetPendingSchemaTarget(100)
	require.NoError(t, err)
	require.Nil(t, got, "published metadata takes over after completion")
	bm.removeBroadcastTask(100)
	require.Empty(t, bm.schemaTasks)
	got, err = bm.GetPendingSchemaTarget(999)
	require.NoError(t, err)
	require.Nil(t, got)
}

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

func TestSchemaTargetIndexLifecycle(t *testing.T) {
	mockey.PatchConvey("schema lookup follows recovery, local and replicated registration, completion and GC", t, func() {
		paramtable.Init()
		mockey.Mock((*schemaTestCatalog).SaveBroadcastTask).Return(nil).Build()
		resource.InitForTest(resource.OptStreamingCatalog(&schemaTestCatalog{}))
		// Incomplete replicated recovery stays ineligible until callback admission.
		pending := createNewWaitAckBroadcastTaskFromMessage(schemaTestMessage(100, 1), streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_REPLICATED, []byte{0, 0})
		completed := createNewWaitAckBroadcastTaskFromMessage(schemaTestMessage(101, 2), streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE, []byte{1, 1})
		bm := newBroadcastTaskManager([]*streamingpb.BroadcastTask{pending, completed})
		defer bm.Close()
		_, err := bm.GetPendingSchemaTarget(100)
		require.ErrorIs(t, err, merr.ErrServiceNotReady)
		target, err := bm.GetPendingSchemaTarget(101)
		require.NoError(t, err)
		require.Nil(t, target)
		require.Empty(t, bm.snapshotSchemaTasks(101), "recovery must not index tombstones")
		require.Contains(t, bm.tasks, uint64(2), "recovered tombstones remain until GC")

		guards := bm.resourceKeyLocker.Lock(message.NewSharedClusterResourceKey())
		local, created := bm.getOrAddBroadcastTask(t.Context(), schemaTestMessage(102, 3), 3, guards)
		require.True(t, created)
		_, err = bm.GetPendingSchemaTarget(102)
		require.ErrorIs(t, err, merr.ErrServiceNotReady)

		msg := schemaTestMessage(103, 4).SplitIntoMutableMessage()[0].WithReplicateHeader(&message.ReplicateHeader{
			ClusterID: "source", MessageID: walimplstest.NewTestMessageID(2), LastConfirmedMessageID: walimplstest.NewTestMessageID(1), TimeTick: 10,
		}).
			WithTimeTick(10).WithLastConfirmed(walimplstest.NewTestMessageID(1)).IntoImmutableMessage(walimplstest.NewTestMessageID(2))
		replicated, created := bm.getOrCreateBroadcastTask(msg)
		require.True(t, created)
		_, err = bm.GetPendingSchemaTarget(103)
		require.ErrorIs(t, err, merr.ErrServiceNotReady)
		again, found := bm.getOrCreateBroadcastTask(msg)
		require.True(t, found)
		require.Same(t, replicated, again)

		for _, task := range []*broadcastTask{bm.tasks[1], local, replicated} {
			bm.admitSchemaTarget(task)
			target, err := bm.GetPendingSchemaTarget(task.schemaCollectionID)
			require.NoError(t, err)
			require.Equal(t, task.Header().BroadcastID, target.BroadcastID)
			snapshot := bm.snapshotSchemaTasks(task.schemaCollectionID)
			require.Len(t, snapshot, 1)
			require.NoError(t, task.MarkAckCallbackDone(t.Context()))
			require.Empty(t, bm.snapshotSchemaTasks(task.schemaCollectionID), "completion must reclaim the collection bucket before GC")
			require.Contains(t, bm.tasks, task.Header().BroadcastID, "completion must retain the tombstone")
			require.Nil(t, snapshot[0].pendingSchemaTarget(task.schemaCollectionID), "an earlier reader snapshot must reject the completed target")
			require.NoError(t, task.MarkAckCallbackDone(t.Context()), "completion replay must be idempotent")
			if task == local {
				ownerID, found := bm.idempotencyIndex.Get(local.IdempotencyScope())
				require.True(t, found, "completion must retain deduplication")
				require.EqualValues(t, 3, ownerID)
			}
			target, err = bm.GetPendingSchemaTarget(task.schemaCollectionID)
			require.NoError(t, err)
			require.Nil(t, target, "completed target must fall back even before tombstone GC")
			bm.removeBroadcastTask(task.Header().BroadcastID)
			require.Empty(t, bm.snapshotSchemaTasks(task.schemaCollectionID))
		}
		bm.removeBroadcastTask(2)
		require.Empty(t, bm.schemaTasks)
	})
}

func TestSchemaTargetLookupOnlyInspectsAdmittedTask(t *testing.T) {
	paramtable.Init()
	metrics := newBroadcasterMetrics()
	bm := &broadcastTaskManager{mu: &sync.Mutex{}, schemaTasks: make(schemaTaskIndex)}
	selected := newBroadcastTaskFromBroadcastMessage(schemaTestMessage(100, 1), metrics, nil)
	successor := newBroadcastTaskFromBroadcastMessage(schemaTestMessage(100, 2), metrics, nil)
	other := newBroadcastTaskFromBroadcastMessage(schemaTestMessage(101, 3), metrics, nil)
	for _, task := range []*broadcastTask{selected, successor, other} {
		bm.schemaTasks.add(task.Header().BroadcastID, task)
	}
	bm.admitSchemaTarget(selected)
	// Neither pending successors nor unrelated collections may be inspected.
	// In particular, reading the selected schema must not wait for their locks.
	successor.mu.Lock()
	defer successor.mu.Unlock()
	other.mu.Lock()
	defer other.mu.Unlock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		target, err := bm.GetPendingSchemaTarget(100)
		require.NoError(t, err)
		require.EqualValues(t, 1, target.BroadcastID)
		target, err = bm.GetPendingSchemaTarget(101)
		require.ErrorIs(t, err, merr.ErrServiceNotReady)
		require.Nil(t, target)
		target, err = bm.GetPendingSchemaTarget(999)
		require.NoError(t, err)
		require.Nil(t, target)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("lookup inspected a task without callback admission")
	}
}

func TestSchemaTargetCompletionKeepsNextTask(t *testing.T) {
	mockey.PatchConvey("completion removes only its own schema target", t, func() {
		paramtable.Init()
		mockey.Mock((*schemaTestCatalog).SaveBroadcastTask).Return(nil).Build()
		resource.InitForTest(resource.OptStreamingCatalog(&schemaTestCatalog{}))
		first := createNewWaitAckBroadcastTaskFromMessage(schemaTestMessage(100, 1), streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_REPLICATED, []byte{0, 0})
		second := createNewWaitAckBroadcastTaskFromMessage(schemaTestMessage(100, 2), streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_REPLICATED, []byte{0, 0})
		bm := newBroadcastTaskManager([]*streamingpb.BroadcastTask{first, second})
		defer bm.Close()

		require.NoError(t, bm.tasks[1].MarkAckCallbackDone(t.Context()))
		require.Len(t, bm.snapshotSchemaTasks(100), 1)
		_, err := bm.GetPendingSchemaTarget(100)
		require.ErrorIs(t, err, merr.ErrServiceNotReady, "the next task still requires callback admission")
		bm.admitSchemaTarget(bm.tasks[2])
		target, err := bm.GetPendingSchemaTarget(100)
		require.NoError(t, err)
		require.EqualValues(t, 2, target.BroadcastID)

		// GC of the earlier tombstone must not remove the later task's bucket.
		bm.removeBroadcastTask(1)
		require.Len(t, bm.snapshotSchemaTasks(100), 1)
		target, err = bm.GetPendingSchemaTarget(100)
		require.NoError(t, err)
		require.EqualValues(t, 2, target.BroadcastID, "old tombstone GC must preserve the admitted successor")
		require.NoError(t, bm.tasks[2].MarkAckCallbackDone(t.Context()))
		require.Empty(t, bm.schemaTasks)
		require.Contains(t, bm.tasks, uint64(2))
	})
}

func TestSchemaTargetIndexExcludesNonSchemaAlter(t *testing.T) {
	paramtable.Init()
	metrics := newBroadcasterMetrics()
	for _, tc := range []struct {
		name   string
		paths  []string
		schema *schemapb.CollectionSchema
	}{
		{name: "properties only", paths: []string{message.FieldMaskCollectionProperties}, schema: &schemapb.CollectionSchema{Version: 2}},
		{name: "missing schema", paths: []string{message.FieldMaskCollectionSchema}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			msg := message.NewAlterCollectionMessageBuilderV2().WithHeader(&message.AlterCollectionMessageHeader{
				CollectionId: 100, UpdateMask: &fieldmaskpb.FieldMask{Paths: tc.paths},
			}).WithBody(&message.AlterCollectionMessageBody{Updates: &message.AlterCollectionMessageUpdates{Schema: tc.schema}}).
				WithBroadcast([]string{"v1"}).MustBuildBroadcast().WithBroadcastID(1)
			task := newBroadcastTaskFromBroadcastMessage(msg, metrics, nil)
			index := make(schemaTaskIndex)
			index.add(1, task)
			if tc.schema == nil {
				require.Len(t, index[100].pending, 1, "index membership uses only the header")
			} else {
				require.Empty(t, index)
			}
			require.Nil(t, task.pendingSchemaTarget(100))
		})
	}
}

// Exercise recovery through the real ordered scheduler. The callback stays
// blocked so readers can inspect eligibility before publication completes.
func TestSchemaTargetRecoveredCallbackAdmission(t *testing.T) {
	mockey.PatchConvey("eligibility follows all ACKs and conflicting callback completion", t, func() {
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
		// Reversed IDs make it impossible for numeric version/ID selection to pass.
		bm := newBroadcastTaskManager([]*streamingpb.BroadcastTask{newTask(100, 20, true), newTask(200, 10, false)})
		defer bm.Close()
		_, err := bm.GetPendingSchemaTarget(100)
		require.ErrorIs(t, err, merr.ErrServiceNotReady)
		select {
		case id := <-entered:
			t.Fatalf("callback %d ran before predecessor ACKs", id)
		default:
		}
		first := bm.tasks[200]
		first.mu.Lock()
		for i, channel := range first.header().VChannels {
			if !funcutil.IsControlChannel(channel) {
				first.task.AckedVchannelBitmap[i] = 1
				first.task.AckedCheckpoints[i] = &streamingpb.AckedCheckpoint{TimeTick: 11, MessageId: walimplstest.NewTestMessageID(1).IntoProto(), LastConfirmedMessageId: walimplstest.NewTestMessageID(1).IntoProto()}
			}
		}
		first.closeAllAcked()
		first.mu.Unlock()
		select {
		case id := <-entered:
			require.EqualValues(t, 200, id)
		case <-time.After(5 * time.Second):
			t.Fatal("first callback did not enter")
		}
		target, err := bm.GetPendingSchemaTarget(100)
		require.NoError(t, err)
		require.EqualValues(t, 200, target.BroadcastID)
		require.Equal(t, map[string]uint64{"v1": 11}, target.TimeTicks)
		var readers sync.WaitGroup
		for n := 0; n < 8; n++ {
			readers.Add(1)
			go func() {
				defer readers.Done()
				for i := 0; i < 50; i++ {
					snapshot, err := bm.GetPendingSchemaTarget(100)
					if err != nil {
						require.ErrorIs(t, err, merr.ErrServiceNotReady)
						continue
					}
					if snapshot != nil {
						require.Contains(t, []uint64{100, 200}, snapshot.BroadcastID)
						snapshot.Schema.Name = "owned"
						snapshot.TimeTicks["v1"] = 900
					}
				}
			}()
		}
		release <- struct{}{}
		select {
		case id := <-entered:
			require.EqualValues(t, 100, id)
		case <-time.After(5 * time.Second):
			t.Fatal("next callback did not enter")
		}
		readers.Wait()
		target, err = bm.GetPendingSchemaTarget(100)
		require.NoError(t, err)
		require.EqualValues(t, 100, target.BroadcastID)
		require.Empty(t, target.Schema.Name)
		require.EqualValues(t, 20, target.TimeTicks["v1"])
		release <- struct{}{}
		require.Eventually(t, func() bool { return len(bm.snapshotSchemaTasks(100)) == 0 }, 5*time.Second, time.Millisecond)
		target, err = bm.GetPendingSchemaTarget(100)
		require.NoError(t, err)
		require.Nil(t, target)
	})
}

func TestSchemaTargetMissingSuccessorControlAck(t *testing.T) {
	paramtable.Init()
	metrics := newBroadcasterMetrics()
	firstMsg, secondMsg := schemaTestMessage(100, 1), schemaTestMessage(100, 2)
	firstPB := createNewWaitAckBroadcastTaskFromMessage(firstMsg, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_REPLICATED, []byte{1, 1})
	secondPB := createNewWaitAckBroadcastTaskFromMessage(secondMsg, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_REPLICATED, []byte{1, 1})
	controlIndex := -1
	for i := range firstPB.AckedCheckpoints {
		firstPB.AckedCheckpoints[i].TimeTick = 10
	}
	for i, ch := range secondMsg.BroadcastHeader().VChannels {
		secondPB.AckedCheckpoints[i].TimeTick = 20
		if funcutil.IsControlChannel(ch) {
			controlIndex = i
			secondPB.AckedCheckpoints[i] = nil
			secondPB.AckedVchannelBitmap[i] = 0
		}
	}
	require.NotEqual(t, -1, controlIndex)
	first := newBroadcastTaskFromProto(firstPB, metrics, nil)
	second := newBroadcastTaskFromProto(secondPB, metrics, nil)
	bm := &broadcastTaskManager{mu: &sync.Mutex{}, schemaTasks: make(schemaTaskIndex)}
	bm.schemaTasks.add(1, first)
	bm.schemaTasks.add(2, second)
	_, err := bm.GetPendingSchemaTarget(100)
	require.ErrorIs(t, err, merr.ErrServiceNotReady)
	bm.admitSchemaTarget(first)
	target, err := bm.GetPendingSchemaTarget(100)
	require.NoError(t, err)
	require.EqualValues(t, 1, target.BroadcastID)
	second.task.AckedCheckpoints[controlIndex] = &streamingpb.AckedCheckpoint{TimeTick: 20}
	target, err = bm.GetPendingSchemaTarget(100)
	require.NoError(t, err)
	require.EqualValues(t, 1, target.BroadcastID)
	bm.schemaTasks.remove(1, 100)
	target, err = bm.GetPendingSchemaTarget(100)
	require.Nil(t, target)
	require.ErrorIs(t, err, merr.ErrServiceNotReady)
	bm.admitSchemaTarget(second)
	target, err = bm.GetPendingSchemaTarget(100)
	require.NoError(t, err)
	require.EqualValues(t, 2, target.BroadcastID)
}

func TestSchemaTargetRecoverySkipsCompletedBody(t *testing.T) {
	paramtable.Init()
	for _, state := range []streamingpb.BroadcastTaskState{
		streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE,
		streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_DONE,
	} {
		t.Run(state.String(), func(t *testing.T) {
			msg := schemaTestMessage(100, 1)
			pb := createNewWaitAckBroadcastTaskFromMessage(msg, state, []byte{1, 1})
			pb.Message.Payload = []byte{0xff}
			require.NotPanics(t, func() {
				task := newBroadcastTaskFromProto(pb, newBroadcasterMetrics(), nil)
				index := make(schemaTaskIndex)
				index.add(1, task)
				require.Empty(t, index)
				require.Nil(t, task.pendingSchemaTarget(100))
				if state == streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE {
					bm := &broadcastTaskManager{mu: &sync.Mutex{}, tasks: map[uint64]*broadcastTask{1: task}}
					require.Empty(t, bm.GetPendingSchemaFileResources())
				}
			})
		})
	}
}

// Inspect pending membership only in lifecycle tests; production lookups use
// the scheduler-admitted target directly.
func (bm *broadcastTaskManager) snapshotSchemaTasks(collectionID int64) []*broadcastTask {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	entry := bm.schemaTasks[collectionID]
	if entry == nil {
		return nil
	}
	tasks := make([]*broadcastTask, 0, len(entry.pending))
	for _, task := range entry.pending {
		tasks = append(tasks, task)
	}
	return tasks
}
