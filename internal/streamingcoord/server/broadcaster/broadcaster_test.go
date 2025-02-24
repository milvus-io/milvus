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
	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	internaltypes "github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/idalloc"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestBroadcaster(t *testing.T) {
	paramtable.Init()

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
					streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_WAIT_ACK,
					[]byte{0x00, 0x00, 0x00}),
			}, nil
		}).Times(1)
	done := typeutil.NewConcurrentSet[uint64]()
	meta.EXPECT().SaveBroadcastTask(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, broadcastID uint64, bt *streamingpb.BroadcastTask) error {
		// may failure
		if rand.Int31n(10) < 3 {
			return errors.New("save task failed")
		}
		if bt.State == streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_DONE {
			done.Insert(broadcastID)
		}
		return nil
	})
	rc := idalloc.NewMockRootCoordClient(t)
	f := syncutil.NewFuture[internaltypes.RootCoordClient]()
	f.Set(rc)
	resource.InitForTest(resource.OptStreamingCatalog(meta), resource.OptRootCoordClient(f))

	fbc := syncutil.NewFuture[Broadcaster]()
	operator, appended := createOpeartor(t, fbc)
	bc, err := RecoverBroadcaster(context.Background(), operator)
	fbc.Set(bc)
	assert.NoError(t, err)
	assert.NotNil(t, bc)
	assert.Eventually(t, func() bool {
		return appended.Load() == 9 && len(done.Collect()) == 6 // only one task is done,
	}, 30*time.Second, 10*time.Millisecond)

	// only task 7 is not done.
	ack(bc, 7, "v1")
	assert.Equal(t, len(done.Collect()), 6)
	ack(bc, 7, "v2")
	assert.Equal(t, len(done.Collect()), 6)
	ack(bc, 7, "v3")
	assert.Eventually(t, func() bool {
		return appended.Load() == 9 && len(done.Collect()) == 7
	}, 30*time.Second, 10*time.Millisecond)

	// Test broadcast here.
	broadcastWithSameRK := func() {
		var result *types.BroadcastAppendResult
		for {
			var err error
			result, err = bc.Broadcast(context.Background(), createNewBroadcastMsg([]string{"v1", "v2", "v3"}, message.NewCollectionNameResourceKey("c7")))
			if err == nil {
				break
			}
		}
		assert.Equal(t, len(result.AppendResults), 3)
	}
	go broadcastWithSameRK()
	go broadcastWithSameRK()

	assert.Eventually(t, func() bool {
		return appended.Load() == 15 && len(done.Collect()) == 9
	}, 30*time.Second, 10*time.Millisecond)

	bc.Close()
	_, err = bc.Broadcast(context.Background(), nil)
	assert.Error(t, err)
	err = bc.Ack(context.Background(), types.BroadcastAckRequest{})
	assert.Error(t, err)
}

func ack(broadcaster Broadcaster, broadcastID uint64, vchannel string) {
	for {
		if err := broadcaster.Ack(context.Background(), types.BroadcastAckRequest{
			BroadcastID: broadcastID,
			VChannel:    vchannel,
		}); err == nil {
			break
		}
	}
}

func createOpeartor(t *testing.T, broadcaster *syncutil.Future[Broadcaster]) (*syncutil.Future[AppendOperator], *atomic.Int64) {
	id := atomic.NewInt64(1)
	appended := atomic.NewInt64(0)
	operator := mock_broadcaster.NewMockAppendOperator(t)
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
				ack(broadcaster.Get(), broadcastID, vchannel)
			}()
		}
		return resps
	}
	operator.EXPECT().AppendMessages(mock.Anything, mock.Anything).RunAndReturn(f)
	operator.EXPECT().AppendMessages(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(f)
	operator.EXPECT().AppendMessages(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(f)
	operator.EXPECT().AppendMessages(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(f)

	fOperator := syncutil.NewFuture[AppendOperator]()
	fOperator.Set(operator)
	return fOperator, appended
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
	return &streamingpb.BroadcastTask{
		Message: &messagespb.Message{
			Payload:    msg.Payload(),
			Properties: msg.Properties().ToRawMap(),
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
	return &streamingpb.BroadcastTask{
		Message: &messagespb.Message{
			Payload:    msg.Payload(),
			Properties: msg.Properties().ToRawMap(),
		},
		State:               state,
		AckedVchannelBitmap: bitmap,
	}
}
