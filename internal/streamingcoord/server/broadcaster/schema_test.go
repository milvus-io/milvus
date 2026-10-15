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
	second.schemaTargetEligible = true
	_, err = bm.GetPendingSchemaTarget(100)
	require.ErrorIs(t, err, merr.ErrServiceNotReady, "must not skip a predecessor")
	first.schemaTargetEligible = true
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
		bm := newBroadcastTaskManager(t.Context(), []*streamingpb.BroadcastTask{pending, completed})
		defer bm.Close()
		_, err := bm.GetPendingSchemaTarget(100)
		require.ErrorIs(t, err, merr.ErrServiceNotReady)
		target, err := bm.GetPendingSchemaTarget(101)
		require.NoError(t, err)
		require.Nil(t, target)

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
			task.mu.Lock()
			task.schemaTargetEligible = true
			task.mu.Unlock()
			target, err := bm.GetPendingSchemaTarget(task.schemaCollectionID)
			require.NoError(t, err)
			require.Equal(t, task.Header().BroadcastID, target.BroadcastID)
			require.NoError(t, task.MarkAckCallbackDone(t.Context()))
			target, err = bm.GetPendingSchemaTarget(task.schemaCollectionID)
			require.NoError(t, err)
			require.Nil(t, target, "completed target must fall back even before tombstone GC")
			bm.removeBroadcastTask(task.Header().BroadcastID)
			require.Empty(t, bm.snapshotSchemaTasks(task.schemaCollectionID))
		}
		bm.removeBroadcastTask(2)
		require.Empty(t, bm.schemaTasks, "GC must reclaim empty collection buckets")
	})
}

func TestSchemaTargetLookupDoesNotInspectOtherCollections(t *testing.T) {
	paramtable.Init()
	metrics := newBroadcasterMetrics()
	other := newBroadcastTaskFromBroadcastMessage(schemaTestMessage(100, 1), metrics, nil)
	bm := &broadcastTaskManager{mu: &sync.Mutex{}, tasks: map[uint64]*broadcastTask{1: other}, schemaTasks: make(schemaTaskIndex)}
	bm.schemaTasks.add(1, other)
	// A lookup for an unrelated collection must not wait on this task's lock.
	other.mu.Lock()
	defer other.mu.Unlock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		target, err := bm.GetPendingSchemaTarget(999)
		require.NoError(t, err)
		require.Nil(t, target)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("lookup inspected an unrelated schema task")
	}
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
			require.Empty(t, index)
		})
	}
}

func BenchmarkSchemaTargetLookup(b *testing.B) {
	paramtable.Init()
	metrics := newBroadcasterMetrics()
	for _, count := range []int{0, 128, 8192} {
		bm := &broadcastTaskManager{mu: &sync.Mutex{}, tasks: make(map[uint64]*broadcastTask), schemaTasks: make(schemaTaskIndex)}
		for n := 0; n < count; n++ {
			id := uint64(n + 1)
			task := newBroadcastTaskFromBroadcastMessage(schemaTestMessage(int64(n+100), id), metrics, nil)
			task.task.State = streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE
			bm.tasks[id] = task
			bm.schemaTasks.add(id, task)
		}
		b.Run(fmt.Sprintf("indexed_miss/tasks=%d", count), func(b *testing.B) {
			b.ReportAllocs()
			for n := 0; n < b.N; n++ {
				_, _ = bm.GetPendingSchemaTarget(1)
			}
		})
		// The old no-target path copied and inspected every retained task.
		b.Run(fmt.Sprintf("scan_miss/tasks=%d", count), func(b *testing.B) {
			b.ReportAllocs()
			for n := 0; n < b.N; n++ {
				for _, task := range bm.snapshotBroadcastTasks() {
					_ = task.pendingSchemaTarget(1)
				}
			}
		})
	}
}
