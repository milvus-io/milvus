package broadcaster

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/atomic"

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
		WithBroadcast(vchannels, rks...).
		BuildBroadcast()
	if err != nil {
		panic(err)
	}
	return msg
}

func createNewBroadcastTask(broadcastID uint64, vchannels []string, rks ...message.ResourceKey) *streamingpb.BroadcastTask {
	msg := createNewBroadcastMsg(vchannels, rks...).WithBroadcastID(broadcastID)
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
