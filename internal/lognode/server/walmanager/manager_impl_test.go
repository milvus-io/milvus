package walmanager

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/internal/lognode/server/wal"
	mock_wal "github.com/milvus-io/milvus/internal/mocks/lognode/server/wal"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/status"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestMain(m *testing.M) {
	paramtable.Init()
	m.Run()
}

func TestManager(t *testing.T) {
	opener := mock_wal.NewMockOpener(t)
	opener.EXPECT().Open(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, oo *wal.OpenOption) (wal.WAL, error) {
			l := mock_wal.NewMockWAL(t)
			l.EXPECT().Channel().Return(oo.Channel)
			l.EXPECT().Close().Return()
			return l, nil
		})
	opener.EXPECT().Close().Return()

	m := newManager(opener, &OpenOption{})
	channelName := "ch1"

	l, err := m.GetAvailableWAL(channelName, 1)
	assertErrorChannelNotExist(t, err)
	assert.Nil(t, l)

	h, err := m.GetAllAvailableChannels()
	assert.NoError(t, err)
	assert.Len(t, h, 0)

	err = m.Remove(context.Background(), channelName, 1)
	assert.NoError(t, err)

	l, err = m.GetAvailableWAL(channelName, 1)
	assertErrorChannelNotExist(t, err)
	assert.Nil(t, l)

	err = m.Open(context.Background(), &logpb.PChannelInfo{
		Name: channelName,
		Term: 1,
	})
	assertErrorTermExpired(t, err)

	err = m.Open(context.Background(), &logpb.PChannelInfo{
		Name: channelName,
		Term: 2,
	})
	assert.NoError(t, err)

	err = m.Remove(context.Background(), channelName, 1)
	assertErrorTermExpired(t, err)

	l, err = m.GetAvailableWAL(channelName, 1)
	assertErrorTermExpired(t, err)
	assert.Nil(t, l)

	l, err = m.GetAvailableWAL(channelName, 2)
	assert.NoError(t, err)
	assert.NotNil(t, l)

	h, err = m.GetAllAvailableChannels()
	assert.NoError(t, err)
	assert.Len(t, h, 1)

	err = m.Open(context.Background(), &logpb.PChannelInfo{
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

	err = m.Open(context.Background(), &logpb.PChannelInfo{
		Name: "term2",
		Term: 4,
	})
	assertShutdownError(t, err)

	err = m.Remove(context.Background(), channelName, 2)
	assertShutdownError(t, err)

	l, err = m.GetAvailableWAL(channelName, 2)
	assertShutdownError(t, err)
	assert.Nil(t, l)
}

func assertShutdownError(t *testing.T, err error) {
	assert.Error(t, err)
	e := status.AsLogError(err)
	assert.Equal(t, e.Code, logpb.LogCode_LOG_CODE_ON_SHUTDOWN)
}
