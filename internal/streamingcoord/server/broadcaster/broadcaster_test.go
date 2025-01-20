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
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	internaltypes "github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/idalloc"
	"github.com/milvus-io/milvus/pkg/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func TestBroadcaster(t *testing.T) {
	paramtable.Init()

	meta := mock_metastore.NewMockStreamingCoordCataLog(t)
	meta.EXPECT().ListBroadcastTask(mock.Anything).
		RunAndReturn(func(ctx context.Context) ([]*streamingpb.BroadcastTask, error) {
			return []*streamingpb.BroadcastTask{
				createNewBroadcastTask(1, []string{"v1"}, message.NewCollectionNameResourceKey("c1")),
				createNewBroadcastTask(2, []string{"v1", "v2"}, message.NewCollectionNameResourceKey("c2")),
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

	operator, appended := createOpeartor(t)
	bc, err := RecoverBroadcaster(context.Background(), operator)
	assert.NoError(t, err)
	assert.NotNil(t, bc)
	assert.Eventually(t, func() bool {
		return appended.Load() == 9 && len(done.Collect()) == 1 // only one task is done,
	}, 30*time.Second, 10*time.Millisecond)

	// Test ack here
	wg := &sync.WaitGroup{}
	asyncAck(wg, bc, 1, "v1")
	asyncAck(wg, bc, 2, "v2")
	asyncAck(wg, bc, 3, "v3")
	asyncAck(wg, bc, 3, "v2")
	// repeatoperation should be ok.
	asyncAck(wg, bc, 1, "v1")
	asyncAck(wg, bc, 2, "v2")
	asyncAck(wg, bc, 3, "v3")
	asyncAck(wg, bc, 3, "v2")
	wg.Wait()

	assert.Eventually(t, func() bool {
		return len(done.Collect()) == 2
	}, 30*time.Second, 10*time.Millisecond)

	// Test broadcast here.
	var result *types.BroadcastAppendResult
	for {
		var err error
		result, err = bc.Broadcast(context.Background(), createNewBroadcastMsg([]string{"v1", "v2", "v3"}, message.NewCollectionNameResourceKey("c7")))
		if err == nil {
			break
		}
	}
	assert.Equal(t, int(appended.Load()), 12)
	assert.Equal(t, len(result.AppendResults), 3)
	assert.Eventually(t, func() bool {
		return len(done.Collect()) == 2
	}, 30*time.Second, 10*time.Millisecond)

	// Test broadcast with a already exist resource key.
	for {
		var err error
		result, err = bc.Broadcast(context.Background(), createNewBroadcastMsg([]string{"v1", "v2", "v3"}, message.NewCollectionNameResourceKey("c7")))
		if errors.Is(err, errResourceKeyHeld) {
			break
		}
	}

	// Test watch here.
	w, err := bc.NewWatcher()
	assert.NoError(t, err)
	// Test a resource key that not exist.
	assertResourceEventOK(t, w, message.NewResourceKeyAckOneBroadcastEvent(message.NewCollectionNameResourceKey("c5")))
	assertResourceEventOK(t, w, message.NewResourceKeyAckAllBroadcastEvent(message.NewCollectionNameResourceKey("c5")))
	// Test a resource key that already ack all.
	assertResourceEventOK(t, w, message.NewResourceKeyAckOneBroadcastEvent(message.NewCollectionNameResourceKey("c1")))
	assertResourceEventOK(t, w, message.NewResourceKeyAckAllBroadcastEvent(message.NewCollectionNameResourceKey("c1")))
	// Test a resource key that partially ack.
	assertResourceEventOK(t, w, message.NewResourceKeyAckOneBroadcastEvent(message.NewCollectionNameResourceKey("c2")))
	assertResourceEventNotReady(t, w, message.NewResourceKeyAckAllBroadcastEvent(message.NewCollectionNameResourceKey("c2")))
	// Test a resource key that not ack.
	readyCh := assertResourceEventUntilReady(t, w, message.NewResourceKeyAckAllBroadcastEvent(message.NewCollectionNameResourceKey("c2")))
	ack(bc, 2, "v1")
	<-readyCh
	// Test a resource key that not ack.
	assertResourceEventNotReady(t, w, message.NewResourceKeyAckOneBroadcastEvent(message.NewCollectionNameResourceKey("c3")))
	assertResourceEventNotReady(t, w, message.NewResourceKeyAckAllBroadcastEvent(message.NewCollectionNameResourceKey("c3")))
	readyCh1 := assertResourceEventUntilReady(t, w, message.NewResourceKeyAckOneBroadcastEvent(message.NewCollectionNameResourceKey("c3")))
	readyCh2 := assertResourceEventUntilReady(t, w, message.NewResourceKeyAckAllBroadcastEvent(message.NewCollectionNameResourceKey("c3")))
	ack(bc, 7, "v1")
	<-readyCh1
	select {
	case <-readyCh2:
		assert.Fail(t, "should not ready")
	case <-time.After(20 * time.Millisecond):
	}
	ack(bc, 7, "v2")
	ack(bc, 7, "v3")
	<-readyCh2

	w2, _ := bc.NewWatcher()
	w2.Close() // Close by watcher itself.
	_, ok := <-w2.EventChan()
	assert.False(t, ok)

	bc.Close()
	w.Close() // Close by broadcaster.

	result, err = bc.Broadcast(context.Background(), createNewBroadcastMsg([]string{"v1", "v2", "v3"}))
	assert.Error(t, err)
	assert.Nil(t, result)
	err = bc.Ack(context.Background(), types.BroadcastAckRequest{BroadcastID: 3, VChannel: "v1"})
	assert.Error(t, err)
	ww, err := bc.NewWatcher()
	assert.Error(t, err)
	assert.Nil(t, ww)
}

func assertResourceEventOK(t *testing.T, w Watcher, ev1 *message.BroadcastEvent) {
	w.ObserveResourceKeyEvent(context.Background(), ev1)
	ev2 := <-w.EventChan()
	assert.True(t, proto.Equal(ev1, ev2))
}

func assertResourceEventNotReady(t *testing.T, w Watcher, ev1 *message.BroadcastEvent) {
	w.ObserveResourceKeyEvent(context.Background(), ev1)
	select {
	case ev2 := <-w.EventChan():
		t.Errorf("should not receive event, %+v", ev2)
	case <-time.After(10 * time.Millisecond):
		return
	}
}

func assertResourceEventUntilReady(t *testing.T, w Watcher, ev1 *message.BroadcastEvent) <-chan struct{} {
	w.ObserveResourceKeyEvent(context.Background(), ev1)
	done := make(chan struct{})
	go func() {
		ev2 := <-w.EventChan()
		assert.True(t, proto.Equal(ev1, ev2))
		close(done)
	}()
	return done
}

func ack(bc Broadcaster, broadcastID uint64, vchannel string) {
	for {
		if err := bc.Ack(context.Background(), types.BroadcastAckRequest{
			BroadcastID: broadcastID,
			VChannel:    vchannel,
		}); err == nil {
			break
		}
	}
}

func asyncAck(wg *sync.WaitGroup, bc Broadcaster, broadcastID uint64, vchannel string) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		ack(bc, broadcastID, vchannel)
	}()
}

func createOpeartor(t *testing.T) (*syncutil.Future[AppendOperator], *atomic.Int64) {
	id := atomic.NewInt64(1)
	appended := atomic.NewInt64(0)
	operator := mock_broadcaster.NewMockAppendOperator(t)
	f := func(ctx context.Context, msgs ...message.MutableMessage) types.AppendResponses {
		resps := types.AppendResponses{
			Responses: make([]types.AppendResponse, len(msgs)),
		}
		for idx := range msgs {
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
