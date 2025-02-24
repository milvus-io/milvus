package timetick

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/ack"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v2/mocks/streaming/mock_walimpls"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

func TestTimeTickSyncOperator(t *testing.T) {
	paramtable.Init()
	resource.InitForTest(t)

	walFuture := syncutil.NewFuture[wal.WAL]()
	msgID := walimplstest.NewTestMessageID(1)
	wimpls := mock_walimpls.NewMockWALImpls(t)
	wimpls.EXPECT().Append(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, mm message.MutableMessage) (message.MessageID, error) {
		return msgID, nil
	})
	wimpls.EXPECT().Channel().Return(types.PChannelInfo{
		Name: "test",
		Term: 1,
	})
	param := interceptors.InterceptorBuildParam{
		WALImpls: wimpls,
		WAL:      walFuture,
	}
	operator := newTimeTickSyncOperator(param)

	assert.Equal(t, "test", operator.Channel().Name)

	defer operator.Close()

	// Test the initialize.
	shouldBlock(operator.Ready())
	// after initialize, the operator should be ready, and setup the walFuture.
	operator.initialize()
	<-operator.Ready()
	l := mock_wal.NewMockWAL(t)
	l.EXPECT().Append(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, mm message.MutableMessage) (*types.AppendResult, error) {
		hint := utility.GetNotPersisted(ctx)
		assert.NotNil(t, hint)
		return &types.AppendResult{
			MessageID: hint.MessageID,
			TimeTick:  mm.TimeTick(),
		}, nil
	})
	walFuture.Set(l)

	// Test the sync operation, but there is no message to sync.
	ctx := context.Background()
	ts, err := resource.Resource().TSOAllocator().Allocate(ctx)
	assert.NoError(t, err)
	ch := operator.TimeTickNotifier().WatchAtMessageID(msgID, ts)
	shouldBlock(ch)
	// should not trigger any wal operation, but only update the timetick.
	operator.Sync(ctx)
	// should not block because timetick updates.
	<-ch

	// Test alloc a real message but not ack.
	// because the timetick message id is updated, so the old watcher should be invalidated.
	ch = operator.TimeTickNotifier().WatchAtMessageID(msgID, operator.TimeTickNotifier().Get().TimeTick)
	shouldBlock(ch)
	acker, err := operator.AckManager().Allocate(ctx)
	assert.NoError(t, err)
	// should block timetick notifier.
	ts, _ = resource.Resource().TSOAllocator().Allocate(ctx)
	ch = operator.TimeTickNotifier().WatchAtMessageID(walimplstest.NewTestMessageID(2), ts)
	shouldBlock(ch)
	// sync operation just do nothing, so there's no wal operation triggered.
	operator.Sync(ctx)

	// After ack, a wal operation will be trigger.
	acker.Ack(ack.OptMessageID(msgID), ack.OptTxnSession(nil))
	l.EXPECT().Append(mock.Anything, mock.Anything).Unset()
	l.EXPECT().Append(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, mm message.MutableMessage) (*types.AppendResult, error) {
		ts, _ := resource.Resource().TSOAllocator().Allocate(ctx)
		return &types.AppendResult{
			MessageID: walimplstest.NewTestMessageID(2),
			TimeTick:  ts,
		}, nil
	})
	// should trigger a wal operation.
	operator.Sync(ctx)
	// ch should still be blocked, because the timetick message id is updated, old message id watch is not notified.
	shouldBlock(ch)
}

func shouldBlock(ch <-chan struct{}) {
	select {
	case <-ch:
		panic("should block")
	case <-time.After(10 * time.Millisecond):
	}
}
