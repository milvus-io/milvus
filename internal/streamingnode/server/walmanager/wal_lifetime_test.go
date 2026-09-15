package walmanager

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	internaltypes "github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

func TestWALLifetime(t *testing.T) {
	channel := "test"
	mixcoord := mocks.NewMockMixCoordClient(t)
	fMixcoord := syncutil.NewFuture[internaltypes.MixCoordClient]()
	fMixcoord.Set(mixcoord)
	resource.InitForTest(
		t,
		resource.OptMixCoordClient(fMixcoord),
	)

	// Gate the term-11 open so the background task cannot converge while the
	// canceled-context assertions below run: the waiters must actually wait
	// (and observe the cancellation) instead of racing with the background
	// convergence.
	term11OpenGate := make(chan struct{})
	opener := mock_wal.NewMockOpener(t)
	opener.EXPECT().Open(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, oo *wal.OpenOption) (wal.WAL, error) {
			if oo.Channel.Term == 11 {
				<-term11OpenGate
			}
			l := mock_wal.NewMockWAL(t)
			l.EXPECT().Channel().Return(oo.Channel)
			l.EXPECT().Close().Return()
			return l, nil
		})

	wlt := newWALLifetime(opener, channel, mlog.With())
	assert.Nil(t, wlt.GetWAL())

	// Test open.
	err := wlt.Open(context.Background(), types.PChannelInfo{
		Name: channel,
		Term: 2,
	})
	assert.NoError(t, err)
	assert.NotNil(t, wlt.GetWAL())
	assert.Equal(t, channel, wlt.GetWAL().Channel().Name)
	assert.Equal(t, int64(2), wlt.GetWAL().Channel().Term)

	// Test expired term remove.
	err = wlt.Remove(context.Background(), 1)
	assertErrorOperationIgnored(t, err)
	assert.NotNil(t, wlt.GetWAL())
	assert.Equal(t, channel, wlt.GetWAL().Channel().Name)
	assert.Equal(t, int64(2), wlt.GetWAL().Channel().Term)

	// Test remove.
	err = wlt.Remove(context.Background(), 2)
	assert.NoError(t, err)
	assert.Nil(t, wlt.GetWAL())

	// Test expired term open.
	err = wlt.Open(context.Background(), types.PChannelInfo{
		Name: channel,
		Term: 1,
	})
	assertErrorOperationIgnored(t, err)
	assert.Nil(t, wlt.GetWAL())

	// Test open after close.
	err = wlt.Open(context.Background(), types.PChannelInfo{
		Name: channel,
		Term: 5,
	})
	assert.NoError(t, err)
	assert.NotNil(t, wlt.GetWAL())
	assert.Equal(t, channel, wlt.GetWAL().Channel().Name)
	assert.Equal(t, int64(5), wlt.GetWAL().Channel().Term)

	// Test overwrite open.
	err = wlt.Open(context.Background(), types.PChannelInfo{
		Name: channel,
		Term: 10,
	})
	assert.NoError(t, err)
	assert.NotNil(t, wlt.GetWAL())
	assert.Equal(t, channel, wlt.GetWAL().Channel().Name)
	assert.Equal(t, int64(10), wlt.GetWAL().Channel().Term)

	// Test context canceled.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = wlt.Open(ctx, types.PChannelInfo{
		Name: channel,
		Term: 11,
	})
	assert.ErrorIs(t, err, context.Canceled)

	err = wlt.Remove(ctx, 11)
	assert.ErrorIs(t, err, context.Canceled)

	// Release the background convergence of term 11.
	close(term11OpenGate)

	err = wlt.Open(context.Background(), types.PChannelInfo{
		Name: channel,
		Term: 11,
	})
	assertErrorOperationIgnored(t, err)

	wlt.Open(context.Background(), types.PChannelInfo{
		Name: channel,
		Term: 12,
	})
	assert.NotNil(t, wlt.GetWAL())
	assert.Equal(t, channel, wlt.GetWAL().Channel().Name)
	assert.Equal(t, int64(12), wlt.GetWAL().Channel().Term)

	wlt.Close()
}

func TestWALLifetimeRemoveAfterOpenInterruptedByAssignTimeout(t *testing.T) {
	channel := "test"
	mixcoord := mocks.NewMockMixCoordClient(t)
	fMixcoord := syncutil.NewFuture[internaltypes.MixCoordClient]()
	fMixcoord.Set(mixcoord)
	resource.InitForTest(
		t,
		resource.OptMixCoordClient(fMixcoord),
	)

	assignCtx, assignCancel := context.WithCancel(context.Background())
	opener := mock_wal.NewMockOpener(t)
	opener.EXPECT().Open(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, oo *wal.OpenOption) (wal.WAL, error) {
			// Simulate a slow wal recovery interrupted by the assign rpc reaching
			// its deadline, same error shape as recovery_stream.go.
			assignCancel()
			<-ctx.Done()
			return nil, errors.Wrap(ctx.Err(), "failed to recover from wal")
		})

	wlt := newWALLifetime(opener, channel, mlog.With())

	err := wlt.Open(assignCtx, types.PChannelInfo{
		Name: channel,
		Term: 1,
	})
	assert.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, wlt.GetWAL())

	// The balancer then removes the channel with a fresh context.
	// The wal is not open at this term, so the remove must succeed even if the
	// previous open failure wraps context.Canceled (issue #51078).
	err = wlt.Remove(context.Background(), 1)
	assert.NoError(t, err)
	assert.Nil(t, wlt.GetWAL())

	wlt.Close()
}
