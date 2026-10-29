package registry

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/producer"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type mockWALManager struct {
	t           *testing.T
	unavailable <-chan struct{}
}

func (m *mockWALManager) Metrics() (*types.StreamingNodeMetrics, error) {
	return &types.StreamingNodeMetrics{}, nil
}

func (m *mockWALManager) GetAvailableWAL(channel types.PChannelInfo) (wal.WAL, error) {
	l := mock_wal.NewMockWAL(m.t)
	l.EXPECT().Append(mock.Anything, mock.Anything).Return(&types.AppendResult{}, nil)
	l.EXPECT().AppendAsync(mock.Anything, mock.Anything, mock.Anything).Return()
	l.EXPECT().Read(mock.Anything, mock.Anything).Return(mock_wal.NewMockScanner(m.t), nil)
	l.EXPECT().Unavailable().Return(m.unavailable)
	return l, nil
}

func TestGetLocalAvailableWAL(t *testing.T) {
	paramtable.Init()
	paramtable.SetLocalComponentEnabled(typeutil.StreamingNodeRole)

	unavailable := make(chan struct{})
	manager := &mockWALManager{t: t, unavailable: unavailable}
	RegisterLocalWALManager(manager)

	walInstance, err := GetLocalAvailableWAL(types.PChannelInfo{})
	assert.NoError(t, err)
	assert.NotNil(t, walInstance)
	assert.True(t, IsLocal(walInstance))
	localProducer, ok := any(walInstance).(producer.Producer)
	if assert.True(t, ok) {
		assert.Equal(t, (<-chan struct{})(unavailable), localProducer.Available())
	}

	msg, _ := message.NewTimeTickMessageBuilderV1().
		WithAllVChannel().
		WithHeader(&message.TimeTickMessageHeader{}).
		WithBody(&msgpb.TimeTickMsg{}).
		BuildMutable()
	walInstance.Append(context.Background(), msg)
	walInstance.AppendAsync(context.Background(), msg, func(ar *wal.AppendResult, err error) {})

	s, err := walInstance.Read(context.Background(), wal.ReadOption{})
	assert.NoError(t, err)
	assert.NotNil(t, walInstance)
	assert.True(t, IsLocal(s))
}
