package walmanager

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_flusher"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	internaltypes "github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
)

func TestWALLifetime(t *testing.T) {
	channel := "test"

	rootcoord := mocks.NewMockRootCoordClient(t)
	fRootcoord := syncutil.NewFuture[internaltypes.RootCoordClient]()
	fRootcoord.Set(rootcoord)
	datacoord := mocks.NewMockDataCoordClient(t)
	fDatacoord := syncutil.NewFuture[internaltypes.DataCoordClient]()
	fDatacoord.Set(datacoord)

	flusher := mock_flusher.NewMockFlusher(t)
	flusher.EXPECT().RegisterPChannel(mock.Anything, mock.Anything).Return(nil)
	flusher.EXPECT().UnregisterPChannel(mock.Anything).Return()

	resource.InitForTest(
		t,
		resource.OptFlusher(flusher),
		resource.OptRootCoordClient(fRootcoord),
		resource.OptDataCoordClient(fDatacoord),
	)

	opener := mock_wal.NewMockOpener(t)
	opener.EXPECT().Open(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, oo *wal.OpenOption) (wal.WAL, error) {
			l := mock_wal.NewMockWAL(t)
			l.EXPECT().Channel().Return(oo.Channel)
			l.EXPECT().Close().Return()
			return l, nil
		})

	wlt := newWALLifetime(opener, channel)
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
