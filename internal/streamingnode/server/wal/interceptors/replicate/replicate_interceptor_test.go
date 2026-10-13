package replicate

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/interceptors/replicate/mock_replicates"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/interceptors/shard/mock_shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/replicate/replicates"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/mvcc"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestReplicateInterceptorAbortsRejectedTxn(t *testing.T) {
	for _, failurePoint := range []string{"", "before timetick", "WAL append"} {
		failRollback := failurePoint != ""
		name := "rollback succeeds"
		if failRollback {
			name = "rollback fails once at " + failurePoint
		}
		t.Run(name, func(t *testing.T) {
			paramtable.Init()
			resource.InitForTest(t)
			channel := types.PChannelInfo{Name: "target-rootcoord-dml_0", Term: 1}
			buffer := utility.NewTxnBuffer(mlog.With(), metricsutil.NewScanMetrics(channel).NewScannerMetrics())
			control := &streamingpb.PChannelRecoveryControlMeta{
				ReplicateConfig: &commonpb.ReplicateConfiguration{
					Clusters: []*commonpb.MilvusCluster{
						{ClusterId: "source", Pchannels: []string{"source-rootcoord-dml_0"}},
						{ClusterId: "target", Pchannels: []string{channel.Name}},
					},
					CrossClusterTopology: []*commonpb.CrossClusterTopology{
						{SourceClusterId: "source", TargetClusterId: "target"},
					},
				},
				ReplicateCheckpoint: (&utility.ReplicateCheckpoint{
					ClusterID: "source", PChannel: "source-rootcoord-dml_0",
					MessageID: walimplstest.NewTestMessageID(0), TimeTick: 1,
				}).IntoProto(),
			}
			recoverParam := &replicates.ReplicateManagerRecoverParam{
				ChannelInfo: channel, CurrentClusterID: "target",
				InitialRecoverSnapshot: &recovery.RecoverySnapshot{PChannelControl: control, TxnBuffer: buffer},
			}
			manager, err := replicates.RecoverReplicateManager(recoverParam)
			require.NoError(t, err)
			txnManager := txn.NewTxnManager(channel, nil)
			lastID := walimplstest.NewTestMessageID(0)
			lastTick := timetick.NewTimeTickMsg(1, lastID, 0, true).IntoImmutableMessage(lastID)
			shardManager := mock_shards.NewMockShardManager(t)
			shardManager.EXPECT().CheckIfCollectionExists(int64(1)).Return(nil)
			shardManager.EXPECT().ApplyDelete(mock.Anything).Return(nil)
			shardManager.EXPECT().CheckIfCollectionExists(int64(2)).Return(shards.ErrCollectionNotFound).Once()
			param := &interceptors.InterceptorBuildParam{
				ChannelInfo: channel, ReplicateManager: manager, TxnManager: txnManager,
				ShardManager: shardManager, LastTimeTickMessage: lastTick,
				LastConfirmedMessageID: lastID, MVCCManager: mvcc.NewMVCCManager(1),
			}
			r := NewInterceptorBuilder().Build(param)
			tt := timetick.NewInterceptorBuilder().Build(param)
			s := shard.NewInterceptorBuilder().Build(param)
			t.Cleanup(tt.Close)
			var persisted []message.ImmutableMessage
			rollbackAttempts := 0
			appendToWAL := func(_ context.Context, msg message.MutableMessage) (message.MessageID, error) {
				if msg.MessageType() == message.MessageTypeRollbackTxn {
					rollbackAttempts++
					require.Nil(t, msg.ReplicateHeader(), "local abort must not advance source checkpoint")
					if failurePoint == "WAL append" && rollbackAttempts == 1 {
						return nil, status.NewUnknownError("WAL unavailable")
					}
				}
				id := walimplstest.NewTestMessageID(int64(len(persisted) + 1))
				persisted = append(persisted, msg.IntoImmutableMessage(id))
				return id, nil
			}
			appendMessage := func(msg message.MutableMessage) error {
				ctx := utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
				_, err := r.DoAppend(ctx, msg, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
					if failurePoint == "before timetick" && msg.MessageType() == message.MessageTypeRollbackTxn && rollbackAttempts == 0 {
						rollbackAttempts++
						return nil, status.NewUnknownError("timestamp allocation unavailable")
					}
					return tt.DoAppend(ctx, msg, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
						return s.DoAppend(ctx, msg, appendToWAL)
					})
				})
				return err
			}
			txnCtx := message.TxnContext{TxnID: 7, Keepalive: message.TxnKeepaliveInfinite}
			withSource := func(msg message.MutableMessage, id int64) message.MutableMessage {
				return msg.WithReplicateHeader(&message.ReplicateHeader{
					ClusterID: "source", MessageID: walimplstest.NewTestMessageID(id),
					LastConfirmedMessageID: lastID, TimeTick: 10,
				})
			}
			begin := func() message.MutableMessage {
				return withSource(message.NewBeginTxnMessageBuilderV2().WithVChannel(channel.Name).
					WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).
					MustBuildMutable().WithTxnContext(txnCtx), 1)
			}
			body := func(collectionID, id int64) message.MutableMessage {
				return withSource(message.NewDeleteMessageBuilderV1().WithVChannel(channel.Name).
					WithHeader(&message.DeleteMessageHeader{CollectionId: collectionID}).WithBody(&msgpb.DeleteRequest{}).
					MustBuildMutable().WithTxnContext(txnCtx), id)
			}
			commit := func() message.MutableMessage {
				return withSource(message.NewCommitTxnMessageBuilderV2().WithVChannel(channel.Name).
					WithHeader(&message.CommitTxnMessageHeader{}).WithBody(&message.CommitTxnMessageBody{}).
					MustBuildMutable().WithTxnContext(txnCtx), 4)
			}
			require.NoError(t, appendMessage(begin()))
			require.NoError(t, appendMessage(body(1, 2)))
			err = appendMessage(body(2, 3))
			require.True(t, status.AsStreamingError(err).IsUnrecoverable())
			if failRollback {
				require.Len(t, persisted, 2)
				session, err := txnManager.GetSessionOfTxn(txnCtx.TxnID)
				require.NoError(t, err)
				require.Equal(t, message.TxnStateOnRollback, session.State())
				require.True(t, status.AsStreamingError(appendMessage(commit())).IsReplicateViolation(),
					"a pending rollback must not let CommitTxn release the guard")
				// Replaying the rejected body must retry the pending rollback.
				err = appendMessage(body(2, 3))
				require.True(t, status.AsStreamingError(err).IsTxnUnavilable())
			}
			require.Len(t, persisted, 3)
			require.Equal(t, message.MessageTypeRollbackTxn, persisted[2].MessageType())
			cp, err := manager.GetReplicateCheckpoint()
			require.NoError(t, err)
			require.EqualValues(t, 1, cp.TimeTick)
			require.True(t, status.AsStreamingError(appendMessage(commit())).IsReplicateViolation())

			// Recovery must discard the aborted transaction, including its persisted body.
			require.Empty(t, buffer.HandleImmutableMessages(persisted, persisted[2].TimeTick()))
			require.Empty(t, buffer.GetUncommittedMessageBuilder())
			recovered, err := replicates.RecoverReplicateManager(recoverParam)
			require.NoError(t, err)
			for _, rm := range []replicates.ReplicatesManager{manager, recovered} {
				next := withSource(message.NewCreateDatabaseMessageBuilderV2().WithVChannel(channel.Name).
					WithHeader(&message.CreateDatabaseMessageHeader{}).WithBody(&message.CreateDatabaseMessageBody{}).
					MustBuildMutable(), 5)
				acker, err := rm.BeginReplicateMessage(context.Background(), next)
				require.NoError(t, err, "unrelated messages must no longer be blocked")
				acker.Ack(status.NewUnknownError("do not advance checkpoint in this probe"))
			}

			// Once metadata is restored, replay from BeginTxn must succeed.
			shardManager.EXPECT().CheckIfCollectionExists(int64(2)).Return(nil).Once()
			require.NoError(t, appendMessage(begin()))
			require.NoError(t, appendMessage(body(1, 2)))
			require.NoError(t, appendMessage(body(2, 3)))
			require.NoError(t, appendMessage(commit()))
			committed := buffer.HandleImmutableMessages(persisted[3:], persisted[len(persisted)-1].TimeTick())
			require.Len(t, committed, 1)
			require.Equal(t, message.MessageTypeTxn, committed[0].MessageType())
			var collections []int64
			require.NoError(t, message.AsImmutableTxnMessage(committed[0]).RangeOver(func(msg message.ImmutableMessage) error {
				collections = append(collections, message.MustAsImmutableDeleteMessageV1(msg).Header().GetCollectionId())
				return nil
			}))
			require.Equal(t, []int64{1, 2}, collections)
			cp, err = manager.GetReplicateCheckpoint()
			require.NoError(t, err)
			require.EqualValues(t, 10, cp.TimeTick)
		})
	}
}

func TestReplicateInterceptorKeepsTxnOnTransientFailure(t *testing.T) {
	manager := mock_replicates.NewMockReplicatesManager(t)
	acker := mock_replicates.NewMockReplicateAcker(t)
	manager.EXPECT().BeginReplicateMessage(mock.Anything, mock.Anything).Return(acker, nil).Once()
	expectedErr := status.NewUnknownError("temporary WAL failure")
	acker.EXPECT().Ack(expectedErr).Return().Once()
	interceptor := NewInterceptorBuilder().Build(&interceptors.InterceptorBuildParam{ReplicateManager: manager})
	msg := message.NewDeleteMessageBuilderV1().WithVChannel("v1").
		WithHeader(&message.DeleteMessageHeader{}).WithBody(&msgpb.DeleteRequest{}).
		MustBuildMutable().WithTxnContext(message.TxnContext{TxnID: 7})
	appends := 0
	_, err := interceptor.DoAppend(context.Background(), msg, func(_ context.Context, appended message.MutableMessage) (message.MessageID, error) {
		appends++
		require.Same(t, msg, appended, "transient failure must not generate a rollback")
		return nil, expectedErr
	})
	require.ErrorIs(t, err, expectedErr)
	require.Equal(t, 1, appends)
}

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
