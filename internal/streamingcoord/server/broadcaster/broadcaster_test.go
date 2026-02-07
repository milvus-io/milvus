package broadcaster

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	internaltypes "github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/idalloc"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/replicateutil"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestBroadcaster(t *testing.T) {
	registry.ResetRegistration()
	paramtable.Init()
	paramtable.Get().StreamingCfg.WALBroadcasterTombstoneCheckInternal.SwapTempValue("10ms")
	paramtable.Get().StreamingCfg.WALBroadcasterTombstoneMaxCount.SwapTempValue("2")
	paramtable.Get().StreamingCfg.WALBroadcasterTombstoneMaxLifetime.SwapTempValue("20ms")

	mb := mock_balancer.NewMockBalancer(t)
	mb.EXPECT().ReplicateRole().Return(replicateutil.RolePrimary)
	mb.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
		time.Sleep(100 * time.Second)
		return nil
	})
	balance.Register(mb)
	registry.RegisterDropCollectionV1AckCallback(func(ctx context.Context, msg message.BroadcastResultDropCollectionMessageV1) error {
		return nil
	})

	meta := mock_metastore.NewMockStreamingCoordCataLog(t)
	meta.EXPECT().ListBroadcastTask(mock.Anything).
		RunAndReturn(func(ctx context.Context) ([]*streamingpb.BroadcastTask, error) {
			return []*streamingpb.BroadcastTask{
				createNewBroadcastTask(8, []string{"v1"}, message.NewCollectionNameResourceKey("c1")),
				createNewBroadcastTask(9, []string{"v1", "v2"}, message.NewCollectionNameResourceKey("c2")),
				createNewBroadcastTask(3, []string{"v1", "v2", "v3"}),
				createNewWaitAckBroadcastTaskFromMessage(
					createNewBroadcastMsg([]string{"v1", "v2", "v3"}).WithBroadcastID(4),
					streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
					[]byte{0x00, 0x01, 0x00}),
				createNewWaitAckBroadcastTaskFromMessage(
					createNewBroadcastMsg([]string{"v1", "v2", "v3"}).WithBroadcastID(5),
					streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
					[]byte{0x01, 0x01, 0x00}),
				createNewWaitAckBroadcastTaskFromMessage(
					createNewBroadcastMsg([]string{"v1", "v2", "v3"}).WithBroadcastID(6), // will be done directly.
					streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
					[]byte{0x01, 0x01, 0x01}),
				createNewWaitAckBroadcastTaskFromMessage(
					createNewBroadcastMsg([]string{"v1", "v2", "v3"},
						message.NewCollectionNameResourceKey("c3"),
						message.NewCollectionNameResourceKey("c4")).WithBroadcastID(7),
					streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_REPLICATED,
					[]byte{0x00, 0x00, 0x00}),
			}, nil
		}).Times(1)
	done := typeutil.NewConcurrentSet[uint64]()
	meta.EXPECT().SaveBroadcastTask(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, broadcastID uint64, bt *streamingpb.BroadcastTask) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if bt.State == streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE {
			done.Insert(broadcastID)
		}
		return nil
	})
	rc := idalloc.NewMockRootCoordClient(t)
	f := syncutil.NewFuture[internaltypes.MixCoordClient]()
	f.Set(rc)
	resource.InitForTest(resource.OptStreamingCatalog(meta), resource.OptMixCoordClient(f))

	fbc := syncutil.NewFuture[Broadcaster]()
	appended := createOpeartor(t, fbc)
	bc, err := RecoverBroadcaster(context.Background())
	fbc.Set(bc)
	assert.NoError(t, err)
	assert.NotNil(t, bc)
	assert.Eventually(t, func() bool {
		return appended.Load() == 9 && len(done.Collect()) == 6
	}, 30*time.Second, 10*time.Millisecond)

	// only task 7 is not done.
	ack(t, bc, 7, "v1")
	ack(t, bc, 7, "v1") // test already acked, make the idempotent.
	assert.Equal(t, len(done.Collect()), 6)
	ack(t, bc, 7, "v2")
	ack(t, bc, 7, "v2")
	assert.Equal(t, len(done.Collect()), 6)
	ack(t, bc, 7, "v3")
	ack(t, bc, 7, "v3")
	assert.Eventually(t, func() bool {
		return appended.Load() == 9 && len(done.Collect()) == 7
	}, 30*time.Second, 10*time.Millisecond)

	// Test broadcast here.
	broadcastWithSameRK := func() {
		var result *types.BroadcastAppendResult
		var err error
		b, err := bc.WithResourceKeys(context.Background(), message.NewCollectionNameResourceKey("c7"))
		assert.NoError(t, err)
		result, err = b.Broadcast(context.Background(), createNewBroadcastMsg([]string{"v1", "v2", "v3"}, message.NewCollectionNameResourceKey("c7")))
		assert.Equal(t, len(result.AppendResults), 3)
		assert.NoError(t, err)
	}
	go broadcastWithSameRK()
	go broadcastWithSameRK()

	assert.Eventually(t, func() bool {
		return appended.Load() == 15 && len(done.Collect()) == 9
	}, 30*time.Second, 10*time.Millisecond)

	// Test close befor broadcast
	broadcastAPI, err := bc.WithResourceKeys(context.Background(), message.NewExclusiveClusterResourceKey())
	assert.NoError(t, err)
	broadcastAPI.Close()

	broadcastAPI, err = bc.WithResourceKeys(context.Background(), message.NewExclusiveClusterResourceKey())
	assert.NoError(t, err)
	broadcastAPI.Close()

	bc.Close()
	broadcastAPI, err = bc.WithResourceKeys(context.Background())
	assert.NoError(t, err)
	_, err = broadcastAPI.Broadcast(context.Background(), nil)
	assert.Error(t, err)
	err = bc.Ack(context.Background(), mock_message.NewMockImmutableMessage(t))
	assert.Error(t, err)
}

func ack(t *testing.T, broadcaster Broadcaster, broadcastID uint64, vchannel string) {
	for {
		msg := message.NewDropCollectionMessageBuilderV1().
			WithHeader(&message.DropCollectionMessageHeader{}).
			WithBody(&msgpb.DropCollectionRequest{}).
			WithBroadcast([]string{vchannel}).
			MustBuildBroadcast().
			WithBroadcastID(broadcastID).
			SplitIntoMutableMessage()[0].
			WithTimeTick(100).
			WithLastConfirmed(walimplstest.NewTestMessageID(1)).
			IntoImmutableMessage(walimplstest.NewTestMessageID(1))

		if err := broadcaster.Ack(context.Background(), msg); err == nil {
			break
		}
	}
}

func createOpeartor(t *testing.T, broadcaster *syncutil.Future[Broadcaster]) *atomic.Int64 {
	id := atomic.NewInt64(1)
	appended := atomic.NewInt64(0)
	operator := mock_streaming.NewMockWALAccesser(t)
	f := func(ctx context.Context, msgs ...message.MutableMessage) types.AppendResponses {
		resps := types.AppendResponses{
			Responses: make([]types.AppendResponse, len(msgs)),
		}
		for idx, msg := range msgs {
			newID := walimplstest.NewTestMessageID(id.Inc())
			if rand.Int31n(10) < 3 {
				resps.Responses[idx] = types.AppendResponse{
					Error: errors.New("append failed"),
				}
				continue
			}
			resps.Responses[idx] = types.AppendResponse{
				AppendResult: &types.AppendResult{
					MessageID: newID,
					TimeTick:  uint64(time.Now().UnixMilli()),
				},
				Error: nil,
			}
			appended.Inc()

			broadcastID := msg.BroadcastHeader().BroadcastID
			vchannel := msg.VChannel()
			go func() {
				time.Sleep(time.Duration(rand.Int31n(100)) * time.Millisecond)
				ack(t, broadcaster.Get(), broadcastID, vchannel)
			}()
		}
		return resps
	}
	operator.EXPECT().AppendMessages(mock.Anything, mock.Anything).RunAndReturn(f)
	operator.EXPECT().AppendMessages(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(f)
	operator.EXPECT().AppendMessages(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(f)
	operator.EXPECT().AppendMessages(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(f)

	streaming.SetWALForTest(operator)
	return appended
}

func createNewBroadcastMsg(vchannels []string, rks ...message.ResourceKey) message.BroadcastMutableMessage {
	msg, err := message.NewDropCollectionMessageBuilderV1().
		WithHeader(&messagespb.DropCollectionMessageHeader{}).
		WithBody(&msgpb.DropCollectionRequest{}).
		WithBroadcast(vchannels).
		BuildBroadcast()
	if err != nil {
		panic(err)
	}
	return msg.OverwriteBroadcastHeader(0, rks...)
}

func createNewBroadcastTask(broadcastID uint64, vchannels []string, rks ...message.ResourceKey) *streamingpb.BroadcastTask {
	msg := createNewBroadcastMsg(vchannels).OverwriteBroadcastHeader(broadcastID, rks...)
	pb := msg.IntoMessageProto()
	return &streamingpb.BroadcastTask{
		Message: &messagespb.Message{
			Payload:    pb.Payload,
			Properties: pb.Properties,
		},
		State:               streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
		AckedVchannelBitmap: make([]byte, len(vchannels)),
	}
}

func createNewWaitAckBroadcastTaskFromMessage(
	msg message.BroadcastMutableMessage,
	state streamingpb.BroadcastTaskState,
	bitmap []byte,
) *streamingpb.BroadcastTask {
	pb := msg.IntoMessageProto()
	acks := make([]*streamingpb.AckedCheckpoint, len(bitmap))
	for i := 0; i < len(bitmap); i++ {
		if bitmap[i] != 0 {
			messageID := walimplstest.NewTestMessageID(int64(i))
			lastConfirmedMessageID := walimplstest.NewTestMessageID(int64(i))
			acks[i] = &streamingpb.AckedCheckpoint{
				MessageId:              messageID.IntoProto(),
				LastConfirmedMessageId: lastConfirmedMessageID.IntoProto(),
				TimeTick:               1,
			}
		}
	}
	return &streamingpb.BroadcastTask{
		Message: &messagespb.Message{
			Payload:    pb.Payload,
			Properties: pb.Properties,
		},
		State:               state,
		AckedVchannelBitmap: bitmap,
		AckedCheckpoints:    acks,
	}
}

func TestRecoverBroadcastTaskFromProto(t *testing.T) {
	task := createNewBroadcastTask(8, []string{"v1", "v2", "v3"}, message.NewCollectionNameResourceKey("c1"))
	b, err := proto.Marshal(task)
	require.NoError(t, err)

	task = unmarshalTask(t, b, 3)
	assert.Equal(t, task.AckedVchannelBitmap, []byte{0x00, 0x00, 0x00})
	assert.Len(t, task.AckedCheckpoints, 3)
	assert.Nil(t, task.AckedCheckpoints[0])
	assert.Nil(t, task.AckedCheckpoints[1])
	assert.Nil(t, task.AckedCheckpoints[2])

	cp := &streamingpb.AckedCheckpoint{
		MessageId:              walimplstest.NewTestMessageID(1).IntoProto(),
		LastConfirmedMessageId: walimplstest.NewTestMessageID(1).IntoProto(),
		TimeTick:               1,
	}

	task.AckedCheckpoints[2] = cp
	task.AckedVchannelBitmap[2] = 0x01
	b, err = proto.Marshal(task)
	require.NoError(t, err)
	task = unmarshalTask(t, b, 3)
	assert.Equal(t, task.AckedVchannelBitmap, []byte{0x00, 0x00, 0x01})
	assert.Len(t, task.AckedCheckpoints, 3)
	assert.Nil(t, task.AckedCheckpoints[0])
	assert.Nil(t, task.AckedCheckpoints[1])
	assert.NotNil(t, task.AckedCheckpoints[2])

	task.AckedCheckpoints[2] = nil
	task.AckedVchannelBitmap[2] = 0x0
	task.AckedCheckpoints[0] = cp
	task.AckedVchannelBitmap[0] = 0x01
	b, err = proto.Marshal(task)
	require.NoError(t, err)
	task = unmarshalTask(t, b, 3)
	assert.Equal(t, task.AckedVchannelBitmap, []byte{0x01, 0x00, 0x00})
	assert.Len(t, task.AckedCheckpoints, 3)
	assert.NotNil(t, task.AckedCheckpoints[0])
	assert.Nil(t, task.AckedCheckpoints[1])
	assert.Nil(t, task.AckedCheckpoints[2])

	task.AckedCheckpoints[0] = nil
	task.AckedVchannelBitmap[0] = 0x0
	task.AckedCheckpoints[1] = cp
	task.AckedVchannelBitmap[1] = 0x01
	b, err = proto.Marshal(task)
	require.NoError(t, err)
	task = unmarshalTask(t, b, 3)
	assert.Equal(t, task.AckedVchannelBitmap, []byte{0x00, 0x01, 0x00})
	assert.Len(t, task.AckedCheckpoints, 3)
	assert.Nil(t, task.AckedCheckpoints[0])
	assert.NotNil(t, task.AckedCheckpoints[1])
	assert.Nil(t, task.AckedCheckpoints[2])

	task.AckedVchannelBitmap = []byte{0x01, 0x01, 0x01}
	task.AckedCheckpoints = []*streamingpb.AckedCheckpoint{
		cp,
		cp,
		cp,
	}
	b, err = proto.Marshal(task)
	require.NoError(t, err)
	task = unmarshalTask(t, b, 3)
	assert.Equal(t, task.AckedVchannelBitmap, []byte{0x01, 0x01, 0x01})
	assert.Len(t, task.AckedCheckpoints, 3)
	assert.NotNil(t, task.AckedCheckpoints[0])
	assert.NotNil(t, task.AckedCheckpoints[1])
	assert.NotNil(t, task.AckedCheckpoints[2])
}

func unmarshalTask(t *testing.T, b []byte, vchannelCount int) *streamingpb.BroadcastTask {
	task := &streamingpb.BroadcastTask{}
	err := proto.Unmarshal(b, task)
	require.NoError(t, err)
	fixAckInfoFromProto(task, vchannelCount)
	return task
}

func TestGetIncompleteBroadcastTasks(t *testing.T) {
	paramtable.Init()

	metrics := newBroadcasterMetrics()
	ackScheduler := newAckCallbackScheduler(log.With())

	// Task 1: PENDING state with pending (unacked) messages -> should be returned
	pendingProto := createNewBroadcastTask(1, []string{"v1", "v2"})
	pendingTask := newBroadcastTaskFromProto(pendingProto, metrics, ackScheduler)

	// Task 2: REPLICATED state with pending (unacked) messages -> should be returned
	replicatedProto := createNewWaitAckBroadcastTaskFromMessage(
		createNewBroadcastMsg([]string{"v1", "v2", "v3"}).WithBroadcastID(2),
		streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_REPLICATED,
		[]byte{0x00, 0x00, 0x00}, // none acked
	)
	replicatedTask := newBroadcastTaskFromProto(replicatedProto, metrics, ackScheduler)

	// Task 3: PENDING state but ALL vchannels acked -> should NOT be returned (no pending messages)
	allAckedProto := createNewWaitAckBroadcastTaskFromMessage(
		createNewBroadcastMsg([]string{"v1", "v2", "v3"}).WithBroadcastID(3),
		streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
		[]byte{0x01, 0x01, 0x01}, // all acked
	)
	allAckedTask := newBroadcastTaskFromProto(allAckedProto, metrics, ackScheduler)

	// Task 4: TOMBSTONE state -> should NOT be returned
	tombstoneProto := createNewWaitAckBroadcastTaskFromMessage(
		createNewBroadcastMsg([]string{"v1", "v2"}).WithBroadcastID(4),
		streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE,
		[]byte{0x01, 0x01}, // all acked
	)
	tombstoneTask := newBroadcastTaskFromProto(tombstoneProto, metrics, ackScheduler)

	bm := &broadcastTaskManager{
		mu:    &sync.Mutex{},
		tasks: make(map[uint64]*broadcastTask),
	}
	bm.tasks[1] = pendingTask
	bm.tasks[2] = replicatedTask
	bm.tasks[3] = allAckedTask
	bm.tasks[4] = tombstoneTask

	result := bm.getIncompleteBroadcastTasks()

	// Should return exactly 2 tasks: the pending task (ID=1) and the replicated task (ID=2)
	assert.Len(t, result, 2)

	// Collect the broadcast IDs from the result
	resultIDs := make(map[uint64]struct{})
	for _, task := range result {
		resultIDs[task.Header().BroadcastID] = struct{}{}
	}
	assert.Contains(t, resultIDs, uint64(1), "PENDING task with pending messages should be returned")
	assert.Contains(t, resultIDs, uint64(2), "REPLICATED task with pending messages should be returned")
	assert.NotContains(t, resultIDs, uint64(3), "PENDING task with all vchannels acked should not be returned")
	assert.NotContains(t, resultIDs, uint64(4), "TOMBSTONE task should not be returned")
}

func TestWithSecondaryClusterResourceKey(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		registry.ResetRegistration()
		paramtable.Init()
		balance.ResetBalancer()

		mb := mock_balancer.NewMockBalancer(t)
		mb.EXPECT().ReplicateRole().Return(replicateutil.RoleSecondary).Maybe()
		mb.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
			time.Sleep(100 * time.Second)
			return nil
		}).Maybe()
		balance.Register(mb)

		meta := mock_metastore.NewMockStreamingCoordCataLog(t)
		meta.EXPECT().ListBroadcastTask(mock.Anything).Return([]*streamingpb.BroadcastTask{}, nil).Times(1)
		meta.EXPECT().SaveBroadcastTask(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		rc := idalloc.NewMockRootCoordClient(t)
		f := syncutil.NewFuture[internaltypes.MixCoordClient]()
		f.Set(rc)
		resource.InitForTest(resource.OptStreamingCatalog(meta), resource.OptMixCoordClient(f))

		mw := mock_streaming.NewMockWALAccesser(t)
		streaming.SetWALForTest(mw)

		bc, err := RecoverBroadcaster(context.Background())
		assert.NoError(t, err)

		// Should succeed on secondary cluster
		api, err := bc.WithSecondaryClusterResourceKey(context.Background())
		assert.NoError(t, err)
		assert.NotNil(t, api)
		api.Close()

		bc.Close()
	})

	t.Run("not_secondary", func(t *testing.T) {
		registry.ResetRegistration()
		paramtable.Init()
		balance.ResetBalancer()

		mb := mock_balancer.NewMockBalancer(t)
		mb.EXPECT().ReplicateRole().Return(replicateutil.RolePrimary).Maybe()
		mb.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
			time.Sleep(100 * time.Second)
			return nil
		}).Maybe()
		balance.Register(mb)

		meta := mock_metastore.NewMockStreamingCoordCataLog(t)
		meta.EXPECT().ListBroadcastTask(mock.Anything).Return([]*streamingpb.BroadcastTask{}, nil).Times(1)
		meta.EXPECT().SaveBroadcastTask(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		rc := idalloc.NewMockRootCoordClient(t)
		f := syncutil.NewFuture[internaltypes.MixCoordClient]()
		f.Set(rc)
		resource.InitForTest(resource.OptStreamingCatalog(meta), resource.OptMixCoordClient(f))

		mw := mock_streaming.NewMockWALAccesser(t)
		streaming.SetWALForTest(mw)

		bc, err := RecoverBroadcaster(context.Background())
		assert.NoError(t, err)

		// Should fail on primary cluster
		api, err := bc.WithSecondaryClusterResourceKey(context.Background())
		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrNotSecondary))
		assert.Nil(t, api)

		bc.Close()
	})

	t.Run("context_canceled", func(t *testing.T) {
		registry.ResetRegistration()
		paramtable.Init()
		balance.ResetBalancer()

		mb := mock_balancer.NewMockBalancer(t)
		mb.EXPECT().ReplicateRole().Return(replicateutil.RoleSecondary).Maybe()
		mb.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
			time.Sleep(100 * time.Second)
			return nil
		}).Maybe()
		balance.Register(mb)

		meta := mock_metastore.NewMockStreamingCoordCataLog(t)
		meta.EXPECT().ListBroadcastTask(mock.Anything).Return([]*streamingpb.BroadcastTask{}, nil).Times(1)
		meta.EXPECT().SaveBroadcastTask(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		rc := idalloc.NewMockRootCoordClient(t)
		f := syncutil.NewFuture[internaltypes.MixCoordClient]()
		f.Set(rc)
		resource.InitForTest(resource.OptStreamingCatalog(meta), resource.OptMixCoordClient(f))

		mw := mock_streaming.NewMockWALAccesser(t)
		streaming.SetWALForTest(mw)

		bc, err := RecoverBroadcaster(context.Background())
		assert.NoError(t, err)

		// Use canceled context
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		api, err := bc.WithSecondaryClusterResourceKey(ctx)
		assert.Error(t, err)
		assert.Nil(t, api)

		bc.Close()
	})
}

func createAlterReplicateConfigBroadcastMsg(vchannels []string, forcePromote bool) message.BroadcastMutableMessage {
	msg := message.NewAlterReplicateConfigMessageBuilderV2().
		WithHeader(&message.AlterReplicateConfigMessageHeader{
			ReplicateConfiguration: &commonpb.ReplicateConfiguration{},
			ForcePromote:           forcePromote,
		}).
		WithBody(&message.AlterReplicateConfigMessageBody{}).
		WithBroadcast(vchannels).
		MustBuildBroadcast()
	return msg
}

func TestIsAlterReplicateConfigMessage(t *testing.T) {
	paramtable.Init()
	metrics := newBroadcasterMetrics()
	ackScheduler := newAckCallbackScheduler(log.With())

	t.Run("alter_replicate_config_message", func(t *testing.T) {
		msg := createAlterReplicateConfigBroadcastMsg([]string{"v1"}, false).WithBroadcastID(1)
		proto := createNewWaitAckBroadcastTaskFromMessage(msg,
			streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
			[]byte{0x00})
		task := newBroadcastTaskFromProto(proto, metrics, ackScheduler)
		assert.True(t, task.IsAlterReplicateConfigMessage())
	})

	t.Run("non_alter_replicate_config_message", func(t *testing.T) {
		proto := createNewBroadcastTask(1, []string{"v1"})
		task := newBroadcastTaskFromProto(proto, metrics, ackScheduler)
		assert.False(t, task.IsAlterReplicateConfigMessage())
	})
}

func TestIsForcePromoteMessage(t *testing.T) {
	paramtable.Init()
	metrics := newBroadcasterMetrics()
	ackScheduler := newAckCallbackScheduler(log.With())

	t.Run("force_promote_true", func(t *testing.T) {
		msg := createAlterReplicateConfigBroadcastMsg([]string{"v1"}, true).WithBroadcastID(1)
		proto := createNewWaitAckBroadcastTaskFromMessage(msg,
			streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
			[]byte{0x00})
		task := newBroadcastTaskFromProto(proto, metrics, ackScheduler)
		assert.True(t, task.IsForcePromoteMessage())
	})

	t.Run("force_promote_false", func(t *testing.T) {
		msg := createAlterReplicateConfigBroadcastMsg([]string{"v1"}, false).WithBroadcastID(2)
		proto := createNewWaitAckBroadcastTaskFromMessage(msg,
			streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
			[]byte{0x00})
		task := newBroadcastTaskFromProto(proto, metrics, ackScheduler)
		assert.False(t, task.IsForcePromoteMessage())
	})

	t.Run("non_alter_replicate_config", func(t *testing.T) {
		proto := createNewBroadcastTask(3, []string{"v1"})
		task := newBroadcastTaskFromProto(proto, metrics, ackScheduler)
		assert.False(t, task.IsForcePromoteMessage())
	})
}

func TestPendingBroadcastMessages(t *testing.T) {
	paramtable.Init()
	metrics := newBroadcasterMetrics()
	ackScheduler := newAckCallbackScheduler(log.With())

	t.Run("all_pending", func(t *testing.T) {
		msg := createNewBroadcastMsg([]string{"v1", "v2", "v3"}).WithBroadcastID(1)
		proto := createNewWaitAckBroadcastTaskFromMessage(msg,
			streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
			[]byte{0x00, 0x00, 0x00})
		task := newBroadcastTaskFromProto(proto, metrics, ackScheduler)
		pending := task.PendingBroadcastMessages()
		assert.Len(t, pending, 3)
	})

	t.Run("some_acked", func(t *testing.T) {
		msg := createNewBroadcastMsg([]string{"v1", "v2", "v3"}).WithBroadcastID(2)
		proto := createNewWaitAckBroadcastTaskFromMessage(msg,
			streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
			[]byte{0x01, 0x00, 0x01})
		task := newBroadcastTaskFromProto(proto, metrics, ackScheduler)
		pending := task.PendingBroadcastMessages()
		assert.Len(t, pending, 1)
		assert.Equal(t, "v2", pending[0].VChannel())
	})

	t.Run("all_acked", func(t *testing.T) {
		msg := createNewBroadcastMsg([]string{"v1", "v2"}).WithBroadcastID(3)
		proto := createNewWaitAckBroadcastTaskFromMessage(msg,
			streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
			[]byte{0x01, 0x01})
		task := newBroadcastTaskFromProto(proto, metrics, ackScheduler)
		pending := task.PendingBroadcastMessages()
		assert.Len(t, pending, 0)
	})
}

func TestMarkIgnoreAndSave(t *testing.T) {
	paramtable.Init()

	t.Run("success", func(t *testing.T) {
		meta := mock_metastore.NewMockStreamingCoordCataLog(t)
		meta.EXPECT().SaveBroadcastTask(mock.Anything, mock.Anything, mock.Anything).Return(nil)
		rc := idalloc.NewMockRootCoordClient(t)
		f := syncutil.NewFuture[internaltypes.MixCoordClient]()
		f.Set(rc)
		resource.InitForTest(resource.OptStreamingCatalog(meta), resource.OptMixCoordClient(f))

		metrics := newBroadcasterMetrics()
		ackScheduler := newAckCallbackScheduler(log.With())

		msg := createAlterReplicateConfigBroadcastMsg([]string{"v1", "v2"}, false).WithBroadcastID(10)
		proto := createNewWaitAckBroadcastTaskFromMessage(msg,
			streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
			[]byte{0x00, 0x00})
		task := newBroadcastTaskFromProto(proto, metrics, ackScheduler)
		task.SetLogger(log.With())

		err := task.MarkIgnoreAndSave(context.Background())
		assert.NoError(t, err)

		// Verify the message now has ignore=true
		alterMsg, err := message.AsMutableAlterReplicateConfigMessageV2(task.msg)
		assert.NoError(t, err)
		assert.True(t, alterMsg.Header().Ignore)
	})

	t.Run("non_alter_replicate_config", func(t *testing.T) {
		resource.InitForTest()
		metrics := newBroadcasterMetrics()
		ackScheduler := newAckCallbackScheduler(log.With())

		proto := createNewBroadcastTask(11, []string{"v1"})
		task := newBroadcastTaskFromProto(proto, metrics, ackScheduler)
		task.SetLogger(log.With())

		err := task.MarkIgnoreAndSave(context.Background())
		assert.Error(t, err)
	})
}

func TestSortByControlChannelTimeTick(t *testing.T) {
	paramtable.Init()
	metrics := newBroadcasterMetrics()
	ackScheduler := newAckCallbackScheduler(log.With())

	// Create tasks with different control channel time ticks
	// Use "_vcchan" suffix for control channel (funcutil.IsControlChannel)
	msg1 := createNewBroadcastMsg([]string{"by-dev-1_vcchan", "v1"}).WithBroadcastID(1)
	proto1 := createNewWaitAckBroadcastTaskFromMessage(msg1,
		streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
		[]byte{0x01, 0x01})
	// Manually set the control channel checkpoint with timetick=30
	proto1.AckedCheckpoints[0] = &streamingpb.AckedCheckpoint{
		MessageId:              walimplstest.NewTestMessageID(1).IntoProto(),
		LastConfirmedMessageId: walimplstest.NewTestMessageID(1).IntoProto(),
		TimeTick:               30,
	}
	task1 := newBroadcastTaskFromProto(proto1, metrics, ackScheduler)

	msg2 := createNewBroadcastMsg([]string{"by-dev-2_vcchan", "v2"}).WithBroadcastID(2)
	proto2 := createNewWaitAckBroadcastTaskFromMessage(msg2,
		streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
		[]byte{0x01, 0x01})
	proto2.AckedCheckpoints[0] = &streamingpb.AckedCheckpoint{
		MessageId:              walimplstest.NewTestMessageID(2).IntoProto(),
		LastConfirmedMessageId: walimplstest.NewTestMessageID(2).IntoProto(),
		TimeTick:               10,
	}
	task2 := newBroadcastTaskFromProto(proto2, metrics, ackScheduler)

	msg3 := createNewBroadcastMsg([]string{"by-dev-3_vcchan", "v3"}).WithBroadcastID(3)
	proto3 := createNewWaitAckBroadcastTaskFromMessage(msg3,
		streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
		[]byte{0x01, 0x01})
	proto3.AckedCheckpoints[0] = &streamingpb.AckedCheckpoint{
		MessageId:              walimplstest.NewTestMessageID(3).IntoProto(),
		LastConfirmedMessageId: walimplstest.NewTestMessageID(3).IntoProto(),
		TimeTick:               20,
	}
	task3 := newBroadcastTaskFromProto(proto3, metrics, ackScheduler)

	tasks := []*broadcastTask{task1, task3, task2}
	sortByControlChannelTimeTick(tasks)

	// Should be sorted by control channel timetick: 10, 20, 30
	assert.Equal(t, uint64(2), tasks[0].Header().BroadcastID)
	assert.Equal(t, uint64(3), tasks[1].Header().BroadcastID)
	assert.Equal(t, uint64(1), tasks[2].Header().BroadcastID)
}

func TestFixIncompleteBroadcastsForForcePromote(t *testing.T) {
	t.Run("no_incomplete_tasks", func(t *testing.T) {
		paramtable.Init()
		registry.ResetRegistration()

		meta := mock_metastore.NewMockStreamingCoordCataLog(t)
		rc := idalloc.NewMockRootCoordClient(t)
		f := syncutil.NewFuture[internaltypes.MixCoordClient]()
		f.Set(rc)
		resource.InitForTest(resource.OptStreamingCatalog(meta), resource.OptMixCoordClient(f))

		ackScheduler := newAckCallbackScheduler(log.With())

		// Create a bm with no incomplete tasks (all acked)
		bm := &broadcastTaskManager{
			mu:    &sync.Mutex{},
			tasks: make(map[uint64]*broadcastTask),
		}
		ackScheduler.bm = bm

		err := ackScheduler.fixIncompleteBroadcastsForForcePromote(context.Background())
		assert.NoError(t, err)
	})

	t.Run("with_alter_replicate_config_tasks", func(t *testing.T) {
		paramtable.Init()
		registry.ResetRegistration()

		meta := mock_metastore.NewMockStreamingCoordCataLog(t)
		meta.EXPECT().SaveBroadcastTask(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		rc := idalloc.NewMockRootCoordClient(t)
		f := syncutil.NewFuture[internaltypes.MixCoordClient]()
		f.Set(rc)
		resource.InitForTest(resource.OptStreamingCatalog(meta), resource.OptMixCoordClient(f))

		// Set up WAL mock for AppendMessages
		mw := mock_streaming.NewMockWALAccesser(t)
		appendF := func(ctx context.Context, msgs ...message.MutableMessage) types.AppendResponses {
			resps := types.AppendResponses{Responses: make([]types.AppendResponse, len(msgs))}
			for i := range msgs {
				resps.Responses[i] = types.AppendResponse{
					AppendResult: &types.AppendResult{
						MessageID: walimplstest.NewTestMessageID(int64(i + 1)),
						TimeTick:  uint64(100 + i),
					},
				}
			}
			return resps
		}
		mw.EXPECT().AppendMessages(mock.Anything, mock.Anything).RunAndReturn(appendF).Maybe()
		mw.EXPECT().AppendMessages(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(appendF).Maybe()
		mw.EXPECT().AppendMessages(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(appendF).Maybe()
		streaming.SetWALForTest(mw)

		metrics := newBroadcasterMetrics()
		ackScheduler := newAckCallbackScheduler(log.With())

		// Create an incomplete AlterReplicateConfig task (v2 not acked)
		alterMsg := createAlterReplicateConfigBroadcastMsg([]string{"v1", "v2"}, false).WithBroadcastID(100)
		alterProto := createNewWaitAckBroadcastTaskFromMessage(alterMsg,
			streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
			[]byte{0x01, 0x00}) // v1 acked, v2 not
		alterTask := newBroadcastTaskFromProto(alterProto, metrics, ackScheduler)
		alterTask.SetLogger(log.With())

		bm := &broadcastTaskManager{
			mu:    &sync.Mutex{},
			tasks: map[uint64]*broadcastTask{100: alterTask},
		}
		ackScheduler.bm = bm

		err := ackScheduler.fixIncompleteBroadcastsForForcePromote(context.Background())
		assert.NoError(t, err)

		// Verify the task was marked with ignore=true
		parsedMsg, err := message.AsMutableAlterReplicateConfigMessageV2(alterTask.msg)
		assert.NoError(t, err)
		assert.True(t, parsedMsg.Header().Ignore)
	})

	t.Run("with_other_broadcast_tasks", func(t *testing.T) {
		paramtable.Init()
		registry.ResetRegistration()

		meta := mock_metastore.NewMockStreamingCoordCataLog(t)
		meta.EXPECT().SaveBroadcastTask(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		rc := idalloc.NewMockRootCoordClient(t)
		f := syncutil.NewFuture[internaltypes.MixCoordClient]()
		f.Set(rc)
		resource.InitForTest(resource.OptStreamingCatalog(meta), resource.OptMixCoordClient(f))

		// Set up WAL mock
		appendedCount := atomic.NewInt32(0)
		mw := mock_streaming.NewMockWALAccesser(t)
		appendF2 := func(ctx context.Context, msgs ...message.MutableMessage) types.AppendResponses {
			resps := types.AppendResponses{Responses: make([]types.AppendResponse, len(msgs))}
			for i := range msgs {
				appendedCount.Inc()
				resps.Responses[i] = types.AppendResponse{
					AppendResult: &types.AppendResult{
						MessageID: walimplstest.NewTestMessageID(int64(i + 1)),
						TimeTick:  uint64(100 + i),
					},
				}
			}
			return resps
		}
		mw.EXPECT().AppendMessages(mock.Anything, mock.Anything).RunAndReturn(appendF2).Maybe()
		mw.EXPECT().AppendMessages(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(appendF2).Maybe()
		mw.EXPECT().AppendMessages(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(appendF2).Maybe()
		streaming.SetWALForTest(mw)

		metrics := newBroadcasterMetrics()
		ackScheduler := newAckCallbackScheduler(log.With())

		// Create an incomplete DropCollection task (v2, v3 not acked)
		dropMsg := createNewBroadcastMsg([]string{"v1", "v2", "v3"}).WithBroadcastID(200)
		dropProto := createNewWaitAckBroadcastTaskFromMessage(dropMsg,
			streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
			[]byte{0x01, 0x00, 0x00}) // v1 acked, v2 & v3 not
		dropTask := newBroadcastTaskFromProto(dropProto, metrics, ackScheduler)
		dropTask.SetLogger(log.With())

		bm := &broadcastTaskManager{
			mu:    &sync.Mutex{},
			tasks: map[uint64]*broadcastTask{200: dropTask},
		}
		ackScheduler.bm = bm

		err := ackScheduler.fixIncompleteBroadcastsForForcePromote(context.Background())
		assert.NoError(t, err)

		// 2 pending messages should have been appended (v2 and v3)
		assert.Equal(t, int32(2), appendedCount.Load())
	})

	t.Run("append_failure", func(t *testing.T) {
		paramtable.Init()
		registry.ResetRegistration()

		meta := mock_metastore.NewMockStreamingCoordCataLog(t)
		meta.EXPECT().SaveBroadcastTask(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		rc := idalloc.NewMockRootCoordClient(t)
		f := syncutil.NewFuture[internaltypes.MixCoordClient]()
		f.Set(rc)
		resource.InitForTest(resource.OptStreamingCatalog(meta), resource.OptMixCoordClient(f))

		// Set up WAL mock that returns errors
		mw := mock_streaming.NewMockWALAccesser(t)
		appendErrF := func(ctx context.Context, msgs ...message.MutableMessage) types.AppendResponses {
			resps := types.AppendResponses{Responses: make([]types.AppendResponse, len(msgs))}
			for i := range msgs {
				resps.Responses[i] = types.AppendResponse{
					Error: errors.New("append failed"),
				}
			}
			return resps
		}
		mw.EXPECT().AppendMessages(mock.Anything, mock.Anything).RunAndReturn(appendErrF).Maybe()
		mw.EXPECT().AppendMessages(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(appendErrF).Maybe()
		streaming.SetWALForTest(mw)

		metrics := newBroadcasterMetrics()
		ackScheduler := newAckCallbackScheduler(log.With())

		dropMsg := createNewBroadcastMsg([]string{"v1", "v2"}).WithBroadcastID(300)
		dropProto := createNewWaitAckBroadcastTaskFromMessage(dropMsg,
			streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
			[]byte{0x01, 0x00})
		dropTask := newBroadcastTaskFromProto(dropProto, metrics, ackScheduler)
		dropTask.SetLogger(log.With())

		bm := &broadcastTaskManager{
			mu:    &sync.Mutex{},
			tasks: map[uint64]*broadcastTask{300: dropTask},
		}
		ackScheduler.bm = bm

		err := ackScheduler.fixIncompleteBroadcastsForForcePromote(context.Background())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "append failed")
	})
}

func TestDoForcePromoteFixIncompleteBroadcasts(t *testing.T) {
	t.Run("success_after_all_ack", func(t *testing.T) {
		paramtable.Init()
		registry.ResetRegistration()

		meta := mock_metastore.NewMockStreamingCoordCataLog(t)
		meta.EXPECT().SaveBroadcastTask(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		rc := idalloc.NewMockRootCoordClient(t)
		f := syncutil.NewFuture[internaltypes.MixCoordClient]()
		f.Set(rc)
		resource.InitForTest(resource.OptStreamingCatalog(meta), resource.OptMixCoordClient(f))

		mw := mock_streaming.NewMockWALAccesser(t)
		streaming.SetWALForTest(mw)

		metrics := newBroadcasterMetrics()
		ackScheduler := newAckCallbackScheduler(log.With())

		// Create a force promote task that is already all acked
		fpMsg := createAlterReplicateConfigBroadcastMsg([]string{"v1"}, true).WithBroadcastID(400)
		fpProto := createNewWaitAckBroadcastTaskFromMessage(fpMsg,
			streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
			[]byte{0x01}) // already acked
		fpTask := newBroadcastTaskFromProto(fpProto, metrics, ackScheduler)
		fpTask.SetLogger(log.With())

		// No incomplete tasks in the bm
		bm := &broadcastTaskManager{
			mu:    &sync.Mutex{},
			tasks: map[uint64]*broadcastTask{400: fpTask},
		}
		ackScheduler.bm = bm

		// doForcePromoteFixIncompleteBroadcasts should complete without error
		// since all acked and no incomplete tasks
		done := make(chan struct{})
		go func() {
			ackScheduler.doForcePromoteFixIncompleteBroadcasts(fpTask)
			close(done)
		}()

		select {
		case <-done:
			// Success
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for doForcePromoteFixIncompleteBroadcasts")
		}
	})

	t.Run("context_canceled", func(t *testing.T) {
		paramtable.Init()
		registry.ResetRegistration()

		resource.InitForTest()

		metrics := newBroadcasterMetrics()
		ackScheduler := newAckCallbackScheduler(log.With())

		// Create a force promote task that is NOT all acked
		fpMsg := createAlterReplicateConfigBroadcastMsg([]string{"v1", "v2"}, true).WithBroadcastID(401)
		fpProto := createNewWaitAckBroadcastTaskFromMessage(fpMsg,
			streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
			[]byte{0x00, 0x00}) // not acked
		fpTask := newBroadcastTaskFromProto(fpProto, metrics, ackScheduler)
		fpTask.SetLogger(log.With())

		bm := &broadcastTaskManager{
			mu:    &sync.Mutex{},
			tasks: make(map[uint64]*broadcastTask),
		}
		ackScheduler.bm = bm

		// Cancel the notifier to simulate shutdown
		done := make(chan struct{})
		go func() {
			ackScheduler.doForcePromoteFixIncompleteBroadcasts(fpTask)
			close(done)
		}()

		// Cancel the scheduler context
		ackScheduler.notifier.Cancel()

		select {
		case <-done:
			// Success - should return because context canceled
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for doForcePromoteFixIncompleteBroadcasts to exit on cancel")
		}
	})
}
