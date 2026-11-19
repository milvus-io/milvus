package timetick

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/ack"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/mvcc"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	streamingtypes "github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

func TestPersistedFlushReturnsDuringTimestampRefill(t *testing.T) {
	paramtable.Init()
	ctx, cancel := context.WithCancel(t.Context())
	refillStarted := make(chan struct{})
	refillAllowed := make(chan struct{})
	releaseRefill := sync.OnceFunc(func() { close(refillAllowed) })
	var workers sync.WaitGroup
	defer func() {
		cancel()
		releaseRefill()
		workers.Wait()
	}()

	var calls atomic.Int32
	var timestamps atomic.Uint64
	mix := mocks.NewMockMixCoordClient(t)
	mix.EXPECT().AllocTimestamp(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *rootcoordpb.AllocTimestampRequest, _ ...grpc.CallOption) (*rootcoordpb.AllocTimestampResponse, error) {
			if calls.Add(1) == 2 {
				close(refillStarted)
				select {
				case <-refillAllowed:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return &rootcoordpb.AllocTimestampResponse{
				Status: merr.Success(), Timestamp: timestamps.Add(uint64(req.Count)) - uint64(req.Count) + 1, Count: req.Count,
			}, nil
		})
	future := syncutil.NewFuture[types.MixCoordClient]()
	future.Set(mix)
	resource.InitForTest(t, resource.OptMixCoordClient(future))
	t.Cleanup(resource.Resource().TimeTickInspector().Close)
	impl := newConcurrentAppendInterceptor(t, t.Name())

	persisted := make(chan struct{})
	firstDone := make(chan error, 1)
	workers.Go(func() {
		appendCtx := utility.WithExtraAppendResult(ctx, &utility.ExtraAppendResult{})
		_, err := impl.DoAppend(appendCtx, newConcurrentFlush(1), func(context.Context, message.MutableMessage) (message.MessageID, error) {
			close(persisted)
			select {
			case <-refillStarted:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return walimplstest.NewTestMessageID(1), nil
		})
		firstDone <- err
	})
	select {
	case <-persisted:
	case <-time.After(5 * time.Second):
		t.Fatal("first flush did not reach the WAL")
	}
	resource.Resource().TSOAllocator().Sync()
	secondDone := make(chan error, 1)
	workers.Go(func() {
		appendCtx := utility.WithExtraAppendResult(ctx, &utility.ExtraAppendResult{})
		_, err := impl.DoAppend(appendCtx, newConcurrentFlush(2), func(context.Context, message.MutableMessage) (message.MessageID, error) {
			return walimplstest.NewTestMessageID(2), nil
		})
		secondDone <- err
	})
	select {
	case <-refillStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("second flush did not trigger a timestamp refill")
	}
	select {
	case err := <-firstDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Error("persisted flush was blocked by another append's timestamp refill")
	}
	releaseRefill()
	require.NoError(t, <-secondDone)
	workers.Wait()

	details, err := impl.operator.AckManager().SyncAndGetAcknowledged(t.Context())
	require.NoError(t, err)
	var messages []message.ImmutableMessage
	for _, detail := range details {
		if detail.Message != nil {
			messages = append(messages, detail.Message)
		}
	}
	require.Len(t, messages, 2)
	require.Less(t, messages[0].TimeTick(), messages[1].TimeTick())
	next, err := impl.operator.AckManager().Allocate(t.Context())
	require.NoError(t, err)
	require.True(t, messages[1].MessageID().EQ(next.LastConfirmedMessageID()))
	next.Ack(ack.OptSync())
}

// The append path is real; periodic WAL publication is driven separately in
// tests so it cannot consume the acknowledgement details being asserted.
func newConcurrentAppendInterceptor(t *testing.T, channel string) *timeTickAppendInterceptor {
	t.Helper()
	lastID := walimplstest.NewTestMessageID(0)
	lastTick := NewTimeTickMsg(0, lastID, 0, true).IntoImmutableMessage(lastID)
	channelInfo := streamingtypes.PChannelInfo{Name: channel}
	transactions := txn.NewTxnManager(channelInfo, nil)
	<-transactions.RecoverDone()
	operator := newTimeTickSyncOperator(&interceptors.InterceptorBuildParam{
		ChannelInfo: channelInfo, LastTimeTickMessage: lastTick, LastConfirmedMessageID: lastID,
		MVCCManager: mvcc.NewMVCCManager(0), TxnManager: transactions,
	})
	t.Cleanup(operator.Close)
	return &timeTickAppendInterceptor{operator: operator, txnManager: transactions}
}

func newConcurrentFlush(segment int64) message.MutableMessage {
	return message.NewFlushMessageBuilderV2().WithVChannel("v1").
		WithHeader(&message.FlushMessageHeader{CollectionId: 1, PartitionId: 2, SegmentId: segment}).
		WithBody(&message.FlushMessageBody{}).MustBuildMutable()
}
