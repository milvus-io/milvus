package replicate

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/interceptors/replicate/mock_replicates"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/replicate/replicates"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/walimplstest"
)

func TestReplicateInterceptor(t *testing.T) {
	manager := mock_replicates.NewMockReplicatesManager(t)
	acker := mock_replicates.NewMockReplicateAcker(t)
	manager.EXPECT().SwitchReplicateMode(mock.Anything, mock.Anything).Return(nil)
	manager.EXPECT().BeginReplicateMessage(mock.Anything, mock.Anything).Return(acker, nil)
	acker.EXPECT().Ack(mock.Anything).Return()

	interceptor := NewInterceptorBuilder().Build(&interceptors.InterceptorBuildParam{
		ReplicateManager: manager,
	})
	mutableMsg := message.NewAlterReplicateConfigMessageBuilderV2().
		WithHeader(&message.AlterReplicateConfigMessageHeader{}).
		WithBody(&message.AlterReplicateConfigMessageBody{}).
		WithAllVChannel().
		MustBuildMutable()

	msgID, err := interceptor.DoAppend(context.Background(), mutableMsg, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		return walimplstest.NewTestMessageID(1), nil
	})
	assert.NoError(t, err)
	assert.NotNil(t, msgID)

	mutableMsg2 := message.NewCreateDatabaseMessageBuilderV2().
		WithHeader(&message.CreateDatabaseMessageHeader{}).
		WithBody(&message.CreateDatabaseMessageBody{}).
		WithVChannel("test").
		MustBuildMutable()

	msgID2, err := interceptor.DoAppend(context.Background(), mutableMsg2, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		return walimplstest.NewTestMessageID(2), nil
	})
	assert.NoError(t, err)
	assert.NotNil(t, msgID2)

	manager.EXPECT().BeginReplicateMessage(mock.Anything, mock.Anything).Unset()
	manager.EXPECT().BeginReplicateMessage(mock.Anything, mock.Anything).Return(nil, replicates.ErrNotHandledByReplicateManager)

	msgID3, err := interceptor.DoAppend(context.Background(), mutableMsg2, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		return walimplstest.NewTestMessageID(3), nil
	})
	assert.NoError(t, err)
	assert.NotNil(t, msgID3)

	manager.EXPECT().BeginReplicateMessage(mock.Anything, mock.Anything).Unset()
	manager.EXPECT().BeginReplicateMessage(mock.Anything, mock.Anything).Return(nil, errors.New("test"))

	msgID4, err := interceptor.DoAppend(context.Background(), mutableMsg2, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		return walimplstest.NewTestMessageID(4), nil
	})
	assert.Error(t, err)
	assert.Nil(t, msgID4)

	interceptor.Close()
}
