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
	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	internaltypes "github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/idalloc"
	"github.com/milvus-io/milvus/pkg/streaming/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/streaming/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
)

func TestBroadcaster(t *testing.T) {
	meta := mock_metastore.NewMockStreamingCoordCataLog(t)
	meta.EXPECT().ListBroadcastTask(mock.Anything).
		RunAndReturn(func(ctx context.Context) ([]*streamingpb.BroadcastTask, error) {
			return []*streamingpb.BroadcastTask{
				createNewBroadcastTask(1, []string{"v1"}),
				createNewBroadcastTask(2, []string{"v1", "v2"}),
				createNewBroadcastTask(3, []string{"v1", "v2", "v3"}),
			}, nil
		}).Times(1)
	done := atomic.NewInt64(0)
	meta.EXPECT().SaveBroadcastTask(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, bt *streamingpb.BroadcastTask) error {
		// may failure
		if rand.Int31n(10) < 5 {
			return errors.New("save task failed")
		}
		if bt.State == streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_DONE {
			done.Inc()
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
		return appended.Load() == 6 && done.Load() == 3
	}, 10*time.Second, 10*time.Millisecond)

	var result *types.BroadcastAppendResult
	for {
		var err error
		result, err = bc.Broadcast(context.Background(), createNewBroadcastMsg([]string{"v1", "v2", "v3"}))
		if err == nil {
			break
		}
	}
	assert.Equal(t, int(appended.Load()), 9)
	assert.Equal(t, len(result.AppendResults), 3)

	assert.Eventually(t, func() bool {
		return done.Load() == 4
	}, 10*time.Second, 10*time.Millisecond)

	// TODO: error path.
	bc.Close()

	result, err = bc.Broadcast(context.Background(), createNewBroadcastMsg([]string{"v1", "v2", "v3"}))
	assert.Error(t, err)
	assert.Nil(t, result)
}

func createOpeartor(t *testing.T) (AppendOperator, *atomic.Int64) {
	id := atomic.NewInt64(1)
	appended := atomic.NewInt64(0)
	operator := mock_broadcaster.NewMockAppendOperator(t)
	f := func(ctx context.Context, msgs ...message.MutableMessage) streaming.AppendResponses {
		resps := streaming.AppendResponses{
			Responses: make([]streaming.AppendResponse, len(msgs)),
		}
		for idx := range msgs {
			newID := walimplstest.NewTestMessageID(id.Inc())
			if rand.Int31n(10) < 5 {
				resps.Responses[idx] = streaming.AppendResponse{
					Error: errors.New("append failed"),
				}
				continue
			}
			resps.Responses[idx] = streaming.AppendResponse{
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
	return operator, appended
}

func createNewBroadcastMsg(vchannels []string) message.BroadcastMutableMessage {
	msg, err := message.NewDropCollectionMessageBuilderV1().
		WithHeader(&messagespb.DropCollectionMessageHeader{}).
		WithBody(&msgpb.DropCollectionRequest{}).
		WithBroadcast(vchannels).
		BuildBroadcast()
	if err != nil {
		panic(err)
	}
	return msg
}

func createNewBroadcastTask(taskID int64, vchannels []string) *streamingpb.BroadcastTask {
	msg := createNewBroadcastMsg(vchannels)
	return &streamingpb.BroadcastTask{
		TaskId: taskID,
		Message: &messagespb.Message{
			Payload:    msg.Payload(),
			Properties: msg.Properties().ToRawMap(),
		},
		State: streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
	}
}
