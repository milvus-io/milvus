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
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/mvcc"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/wab"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

func TestTimeTickSyncOperator(t *testing.T) {
	paramtable.Init()
	resource.InitForTest(t)
	ctx := context.Background()

	walFuture := syncutil.NewFuture[wal.WAL]()
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
	msgID := walimplstest.NewTestMessageID(1)
	channel := types.PChannelInfo{Name: "test", Term: 1}
	ts, _ := resource.Resource().TSOAllocator().Allocate(ctx)
	lastMsg := NewTimeTickMsg(ts, nil, 0, true)
	immutablelastMsg := lastMsg.IntoImmutableMessage(msgID)

	param := &interceptors.InterceptorBuildParam{
		ChannelInfo:         channel,
		WAL:                 walFuture,
		LastTimeTickMessage: immutablelastMsg,
		WriteAheadBuffer: wab.NewWriteAheadBuffer(
			channel.Name,
			resource.Resource().Logger().With(),
			1024,
			30*time.Second,
			immutablelastMsg,
		),
		MVCCManager: mvcc.NewMVCCManager(ts),
	}
	operator := newTimeTickSyncOperator(param)
	assert.Equal(t, "test", operator.Channel().Name)
	defer operator.Close()
	wb := operator.WriteAheadBuffer()

	ctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()

	newTs, _ := resource.Resource().TSOAllocator().Allocate(ctx)
	r, err := wb.ReadFromExclusiveTimeTick(ctx, newTs)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Nil(t, r)
	// should not trigger any wal operation, but only update the timetick.
	operator.Sync(context.Background(), false)
	r, err = wb.ReadFromExclusiveTimeTick(context.Background(), newTs)
	assert.NoError(t, err)
	// should not block because timetick updates.
	msg, err := r.Next(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, msg)
	assert.Greater(t, msg.TimeTick(), ts)

	// should trigger wal operation.
	l.EXPECT().Append(mock.Anything, mock.Anything).Unset()
	l.EXPECT().Append(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, mm message.MutableMessage) (*types.AppendResult, error) {
		return &types.AppendResult{
			MessageID: walimplstest.NewTestMessageID(1),
			TimeTick:  mm.TimeTick(),
		}, nil
	})
	operator.Sync(context.Background(), true)
}
