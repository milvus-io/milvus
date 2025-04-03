package walmanager

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	internaltypes "github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

func TestMain(m *testing.M) {
	paramtable.Init()
	m.Run()
}

func TestManager(t *testing.T) {
	mixcoord := mocks.NewMockMixCoordClient(t)
	fMixcoord := syncutil.NewFuture[internaltypes.MixCoordClient]()
	fMixcoord.Set(mixcoord)
	resource.InitForTest(
		t,
		resource.OptMixCoordClient(fMixcoord),
	)

	opener := mock_wal.NewMockOpener(t)
	opener.EXPECT().Open(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, oo *wal.OpenOption) (wal.WAL, error) {
			l := mock_wal.NewMockWAL(t)
			l.EXPECT().Channel().Return(oo.Channel)
			l.EXPECT().Close().Return()
			return l, nil
		})
	opener.EXPECT().Close().Return()

	m := newManager(opener)
	channelName := "ch1"

	l, err := m.GetAvailableWAL(types.PChannelInfo{Name: channelName, Term: 1})
	assertErrorChannelNotExist(t, err)
	assert.Nil(t, l)

	h, err := m.GetAllAvailableChannels()
	assert.NoError(t, err)
	assert.Len(t, h, 0)

	err = m.Remove(context.Background(), types.PChannelInfo{Name: channelName, Term: 1})
	assert.NoError(t, err)

	l, err = m.GetAvailableWAL(types.PChannelInfo{Name: channelName, Term: 1})
	assertErrorChannelNotExist(t, err)
	assert.Nil(t, l)

	err = m.Open(context.Background(), types.PChannelInfo{
		Name: channelName,
		Term: 1,
	})
	assertErrorOperationIgnored(t, err)

	err = m.Open(context.Background(), types.PChannelInfo{
		Name: channelName,
		Term: 2,
	})
	assert.NoError(t, err)

	err = m.Remove(context.Background(), types.PChannelInfo{Name: channelName, Term: 1})
	assertErrorOperationIgnored(t, err)

	l, err = m.GetAvailableWAL(types.PChannelInfo{Name: channelName, Term: 1})
	assertErrorTermExpired(t, err)
	assert.Nil(t, l)

	l, err = m.GetAvailableWAL(types.PChannelInfo{Name: channelName, Term: 2})
	assert.NoError(t, err)
	assert.NotNil(t, l)

	h, err = m.GetAllAvailableChannels()
	assert.NoError(t, err)
	assert.Len(t, h, 1)

	err = m.Open(context.Background(), types.PChannelInfo{
		Name: "term2",
		Term: 3,
	})
	assert.NoError(t, err)

	h, err = m.GetAllAvailableChannels()
	assert.NoError(t, err)
	assert.Len(t, h, 2)

	m.Close()

	h, err = m.GetAllAvailableChannels()
	assertShutdownError(t, err)
	assert.Len(t, h, 0)

	err = m.Open(context.Background(), types.PChannelInfo{
		Name: "term2",
		Term: 4,
	})
	assertShutdownError(t, err)

	err = m.Remove(context.Background(), types.PChannelInfo{Name: channelName, Term: 2})
	assertShutdownError(t, err)

	l, err = m.GetAvailableWAL(types.PChannelInfo{Name: channelName, Term: 2})
	assertShutdownError(t, err)
	assert.Nil(t, l)
}

func assertShutdownError(t *testing.T, err error) {
	assert.Error(t, err)
	e := status.AsStreamingError(err)
	assert.Equal(t, e.Code, streamingpb.StreamingCode_STREAMING_CODE_ON_SHUTDOWN)
}
