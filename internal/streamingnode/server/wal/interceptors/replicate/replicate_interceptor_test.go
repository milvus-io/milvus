package replicate

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/interceptors/replicate/mock_replicates"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/replicate/replicates"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
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

func TestReplicateInterceptorForcePromote(t *testing.T) {
	resource.InitForTest(t)

	t.Run("ForcePromoteTriggersRollback", func(t *testing.T) {
		manager := mock_replicates.NewMockReplicatesManager(t)
		manager.EXPECT().SwitchReplicateMode(mock.Anything, mock.Anything).Return(nil)

		// Create a real TxnManager with some active transactions
		txnManager := txn.NewTxnManager(types.PChannelInfo{Name: "test"}, nil)
		<-txnManager.RecoverDone()

		// Create some active sessions to verify they get rolled back
		beginTxnMsg := message.NewBeginTxnMessageBuilderV2().
			WithVChannel("v1").
			WithHeader(&message.BeginTxnMessageHeader{KeepaliveMilliseconds: 10000}).
			WithBody(&message.BeginTxnMessageBody{}).
			MustBuildMutable().
			WithTimeTick(0)
		beginTxnMsgV2, _ := message.AsMutableBeginTxnMessageV2(beginTxnMsg)

		session1, err := txnManager.BeginNewTxn(context.Background(), beginTxnMsgV2)
		assert.NoError(t, err)
		assert.NotNil(t, session1)

		session2, err := txnManager.BeginNewTxn(context.Background(), beginTxnMsgV2)
		assert.NoError(t, err)
		assert.NotNil(t, session2)

		interceptor := NewInterceptorBuilder().Build(&interceptors.InterceptorBuildParam{
			ReplicateManager: manager,
			TxnManager:       txnManager,
		})

		// Build a message with force promote
		mutableMsg := message.NewAlterReplicateConfigMessageBuilderV2().
			WithHeader(&message.AlterReplicateConfigMessageHeader{
				ForcePromote: true,
			}).
			WithBody(&message.AlterReplicateConfigMessageBody{}).
			WithAllVChannel().
			MustBuildMutable()

		msgID, err := interceptor.DoAppend(context.Background(), mutableMsg, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			return walimplstest.NewTestMessageID(1), nil
		})
		assert.NoError(t, err)
		assert.NotNil(t, msgID)

		// Verify that all sessions have been rolled back (cleaned up from the manager)
		_, err = txnManager.GetSessionOfTxn(session1.TxnContext().TxnID)
		assert.Error(t, err, "session1 should have been rolled back")

		_, err = txnManager.GetSessionOfTxn(session2.TxnContext().TxnID)
		assert.Error(t, err, "session2 should have been rolled back")

		interceptor.Close()
	})

	t.Run("NoForcePromoteNoRollback", func(t *testing.T) {
		manager := mock_replicates.NewMockReplicatesManager(t)
		manager.EXPECT().SwitchReplicateMode(mock.Anything, mock.Anything).Return(nil)

		// Create a real TxnManager with some active transactions
		txnManager := txn.NewTxnManager(types.PChannelInfo{Name: "test"}, nil)
		<-txnManager.RecoverDone()

		// Create an active session to verify it does NOT get rolled back
		beginTxnMsg := message.NewBeginTxnMessageBuilderV2().
			WithVChannel("v1").
			WithHeader(&message.BeginTxnMessageHeader{KeepaliveMilliseconds: 10000}).
			WithBody(&message.BeginTxnMessageBody{}).
			MustBuildMutable().
			WithTimeTick(0)
		beginTxnMsgV2, _ := message.AsMutableBeginTxnMessageV2(beginTxnMsg)

		session1, err := txnManager.BeginNewTxn(context.Background(), beginTxnMsgV2)
		assert.NoError(t, err)
		assert.NotNil(t, session1)

		interceptor := NewInterceptorBuilder().Build(&interceptors.InterceptorBuildParam{
			ReplicateManager: manager,
			TxnManager:       txnManager,
		})

		// Build a message without force promote
		mutableMsg := message.NewAlterReplicateConfigMessageBuilderV2().
			WithHeader(&message.AlterReplicateConfigMessageHeader{
				ForcePromote: false,
			}).
			WithBody(&message.AlterReplicateConfigMessageBody{}).
			WithAllVChannel().
			MustBuildMutable()

		msgID, err := interceptor.DoAppend(context.Background(), mutableMsg, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			return walimplstest.NewTestMessageID(1), nil
		})
		assert.NoError(t, err)
		assert.NotNil(t, msgID)

		// Verify that the session is NOT rolled back
		session, err := txnManager.GetSessionOfTxn(session1.TxnContext().TxnID)
		assert.NoError(t, err)
		assert.NotNil(t, session, "session1 should still exist")

		interceptor.Close()
	})

	t.Run("ForcePromoteWithNilTxnManager", func(t *testing.T) {
		manager := mock_replicates.NewMockReplicatesManager(t)
		manager.EXPECT().SwitchReplicateMode(mock.Anything, mock.Anything).Return(nil)

		// Create interceptor without TxnManager (nil)
		interceptor := NewInterceptorBuilder().Build(&interceptors.InterceptorBuildParam{
			ReplicateManager: manager,
			TxnManager:       nil,
		})

		// Build a message with force promote
		mutableMsg := message.NewAlterReplicateConfigMessageBuilderV2().
			WithHeader(&message.AlterReplicateConfigMessageHeader{
				ForcePromote: true,
			}).
			WithBody(&message.AlterReplicateConfigMessageBody{}).
			WithAllVChannel().
			MustBuildMutable()

		// Should not panic even with nil TxnManager
		msgID, err := interceptor.DoAppend(context.Background(), mutableMsg, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			return walimplstest.NewTestMessageID(1), nil
		})
		assert.NoError(t, err)
		assert.NotNil(t, msgID)

		interceptor.Close()
	})

	t.Run("ForcePromoteAppendFailure", func(t *testing.T) {
		manager := mock_replicates.NewMockReplicatesManager(t)
		manager.EXPECT().SwitchReplicateMode(mock.Anything, mock.Anything).Return(nil)

		// Create a real TxnManager with some active transactions
		txnManager := txn.NewTxnManager(types.PChannelInfo{Name: "test"}, nil)
		<-txnManager.RecoverDone()

		// Create an active session
		beginTxnMsg := message.NewBeginTxnMessageBuilderV2().
			WithVChannel("v1").
			WithHeader(&message.BeginTxnMessageHeader{KeepaliveMilliseconds: 10000}).
			WithBody(&message.BeginTxnMessageBody{}).
			MustBuildMutable().
			WithTimeTick(0)
		beginTxnMsgV2, _ := message.AsMutableBeginTxnMessageV2(beginTxnMsg)

		session1, err := txnManager.BeginNewTxn(context.Background(), beginTxnMsgV2)
		assert.NoError(t, err)
		assert.NotNil(t, session1)

		interceptor := NewInterceptorBuilder().Build(&interceptors.InterceptorBuildParam{
			ReplicateManager: manager,
			TxnManager:       txnManager,
		})

		// Build a message with force promote
		mutableMsg := message.NewAlterReplicateConfigMessageBuilderV2().
			WithHeader(&message.AlterReplicateConfigMessageHeader{
				ForcePromote: true,
			}).
			WithBody(&message.AlterReplicateConfigMessageBody{}).
			WithAllVChannel().
			MustBuildMutable()

		// Simulate append failure
		appendErr := errors.New("append failed")
		msgID, err := interceptor.DoAppend(context.Background(), mutableMsg, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			return nil, appendErr
		})
		assert.Error(t, err)
		assert.Equal(t, appendErr, err)
		assert.Nil(t, msgID)

		// On append failure, transactions should NOT be rolled back
		// because rollback happens AFTER successful append (to ensure the config change is persisted first)
		session, err := txnManager.GetSessionOfTxn(session1.TxnContext().TxnID)
		assert.NoError(t, err, "session1 should still exist because append failed")
		assert.NotNil(t, session)

		interceptor.Close()
	})
}
