package replicates

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v2/util/replicateutil"
)

func TestNonReplicateManager(t *testing.T) {
	rm, err := RecoverReplicateManager(&ReplicateManagerRecoverParam{
		ChannelInfo: types.PChannelInfo{
			Name: "test1-rootcoord-dml_0",
			Term: 1,
		},
		CurrentClusterID: "test1",
		InitialRecoverSnapshot: &recovery.RecoverySnapshot{
			Checkpoint: &utility.WALCheckpoint{
				MessageID:           walimplstest.NewTestMessageID(1),
				TimeTick:            1,
				ReplicateCheckpoint: nil,
				ReplicateConfig:     nil,
			},
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, rm.Role(), replicateutil.RolePrimary)

	testSwitchReplicateMode(t, rm, "test1", "test2")
	testMessageOnPrimary(t, rm)
	testMessageOnSecondary(t, rm)
}

func TestPrimaryReplicateManager(t *testing.T) {
	rm, err := RecoverReplicateManager(&ReplicateManagerRecoverParam{
		ChannelInfo: types.PChannelInfo{
			Name: "test1-rootcoord-dml_0",
			Term: 1,
		},
		CurrentClusterID: "test1",
		InitialRecoverSnapshot: &recovery.RecoverySnapshot{
			Checkpoint: &utility.WALCheckpoint{
				MessageID:           walimplstest.NewTestMessageID(1),
				TimeTick:            1,
				ReplicateCheckpoint: nil,
				ReplicateConfig:     newReplicateConfiguration("test1", "test2"),
			},
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, rm.Role(), replicateutil.RolePrimary)

	testSwitchReplicateMode(t, rm, "test1", "test2")
	testMessageOnPrimary(t, rm)
	testMessageOnSecondary(t, rm)
}

func TestSecondaryReplicateManager(t *testing.T) {
	rm, err := RecoverReplicateManager(&ReplicateManagerRecoverParam{
		ChannelInfo: types.PChannelInfo{
			Name: "test1-rootcoord-dml_0",
			Term: 1,
		},
		CurrentClusterID: "test1",
		InitialRecoverSnapshot: &recovery.RecoverySnapshot{
			Checkpoint: &utility.WALCheckpoint{
				MessageID: walimplstest.NewTestMessageID(1),
				TimeTick:  1,
				ReplicateCheckpoint: &utility.ReplicateCheckpoint{
					ClusterID: "test2",
					PChannel:  "test2-rootcoord-dml_0",
					MessageID: walimplstest.NewTestMessageID(1),
					TimeTick:  1,
				},
				ReplicateConfig: newReplicateConfiguration("test2", "test1"),
			},
			TxnBuffer: utility.NewTxnBuffer(log.With(), metricsutil.NewScanMetrics(types.PChannelInfo{}).NewScannerMetrics()),
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, rm.Role(), replicateutil.RoleSecondary)

	testSwitchReplicateMode(t, rm, "test1", "test2")
	testMessageOnPrimary(t, rm)
	testMessageOnSecondary(t, rm)
}

func TestSecondaryReplicateManagerWithTxn(t *testing.T) {
	txnBuffer := utility.NewTxnBuffer(log.With(), metricsutil.NewScanMetrics(types.PChannelInfo{}).NewScannerMetrics())
	txnMsgs := newReplicateTxnMessage("test1", "test2", 2)

	for _, msg := range txnMsgs[0:3] {
		immutableMsg := msg.WithTimeTick(3).IntoImmutableMessage(walimplstest.NewTestMessageID(1))
		txnBuffer.HandleImmutableMessages([]message.ImmutableMessage{immutableMsg}, msg.TimeTick())
	}

	rm, err := RecoverReplicateManager(&ReplicateManagerRecoverParam{
		ChannelInfo: types.PChannelInfo{
			Name: "test1-rootcoord-dml_0",
			Term: 1,
		},
		CurrentClusterID: "test1",
		InitialRecoverSnapshot: &recovery.RecoverySnapshot{
			Checkpoint: &utility.WALCheckpoint{
				MessageID: walimplstest.NewTestMessageID(1),
				TimeTick:  1,
				ReplicateCheckpoint: &utility.ReplicateCheckpoint{
					ClusterID: "test2",
					PChannel:  "test2-rootcoord-dml_0",
					MessageID: walimplstest.NewTestMessageID(1),
					TimeTick:  1,
				},
				ReplicateConfig: newReplicateConfiguration("test2", "test1"),
			},
			TxnBuffer: txnBuffer,
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, rm.Role(), replicateutil.RoleSecondary)

	committed := false
	for _, msg := range newReplicateTxnMessage("test1", "test2", 2) {
		g, err := rm.BeginReplicateMessage(context.Background(), msg)
		if msg.MessageType() == message.MessageTypeCommitTxn && !committed {
			assert.NoError(t, err)
			assert.NotNil(t, g)
			g.Ack(nil)
			committed = true
		} else {
			assert.True(t, status.AsStreamingError(err).IsIgnoredOperation())
			assert.Nil(t, g)
		}
	}
}

func testSwitchReplicateMode(t *testing.T, rm ReplicateManager, primaryClusterID, secondaryClusterID string) {
	ctx := context.Background()

	// switch to primary
	err := rm.SwitchReplicateMode(ctx, newAlterReplicateConfigMessage(primaryClusterID, secondaryClusterID))
	assert.NoError(t, err)
	assert.Equal(t, rm.Role(), replicateutil.RolePrimary)
	cp, err := rm.GetReplicateCheckpoint()
	assert.True(t, status.AsStreamingError(err).IsReplicateViolation())
	assert.Nil(t, cp)

	// idempotent switch to primary
	err = rm.SwitchReplicateMode(ctx, newAlterReplicateConfigMessage(primaryClusterID, secondaryClusterID))
	assert.NoError(t, err)
	assert.Equal(t, rm.Role(), replicateutil.RolePrimary)
	cp, err = rm.GetReplicateCheckpoint()
	assert.True(t, status.AsStreamingError(err).IsReplicateViolation())
	assert.Nil(t, cp)

	// switch to secondary
	err = rm.SwitchReplicateMode(ctx, newAlterReplicateConfigMessage(secondaryClusterID, primaryClusterID))
	assert.NoError(t, err)
	assert.Equal(t, rm.Role(), replicateutil.RoleSecondary)
	cp, err = rm.GetReplicateCheckpoint()
	assert.NoError(t, err)
	assert.Equal(t, cp.ClusterID, secondaryClusterID)
	assert.Equal(t, cp.PChannel, secondaryClusterID+"-rootcoord-dml_0")
	assert.Nil(t, cp.MessageID)
	assert.Equal(t, cp.TimeTick, uint64(0))

	// idempotent switch to secondary
	err = rm.SwitchReplicateMode(ctx, newAlterReplicateConfigMessage(secondaryClusterID, primaryClusterID))
	assert.NoError(t, err)
	assert.Equal(t, rm.Role(), replicateutil.RoleSecondary)
	cp, err = rm.GetReplicateCheckpoint()
	assert.NoError(t, err)
	assert.Equal(t, cp.ClusterID, secondaryClusterID)
	assert.Equal(t, cp.PChannel, secondaryClusterID+"-rootcoord-dml_0")
	assert.Nil(t, cp.MessageID)
	assert.Equal(t, cp.TimeTick, uint64(0))

	// switch back to primary
	err = rm.SwitchReplicateMode(ctx, newAlterReplicateConfigMessage(primaryClusterID, secondaryClusterID))
	assert.NoError(t, err)
	assert.Equal(t, rm.Role(), replicateutil.RolePrimary)
	cp, err = rm.GetReplicateCheckpoint()
	assert.True(t, status.AsStreamingError(err).IsReplicateViolation())
	assert.Nil(t, cp)

	// idempotent switch back to primary
	err = rm.SwitchReplicateMode(ctx, newAlterReplicateConfigMessage(primaryClusterID, secondaryClusterID))
	assert.NoError(t, err)
	assert.Equal(t, rm.Role(), replicateutil.RolePrimary)
	cp, err = rm.GetReplicateCheckpoint()
	assert.True(t, status.AsStreamingError(err).IsReplicateViolation())
	assert.Nil(t, cp)

	// switch back to secondary
	err = rm.SwitchReplicateMode(ctx, newAlterReplicateConfigMessage(secondaryClusterID, primaryClusterID))
	assert.NoError(t, err)
	assert.Equal(t, rm.Role(), replicateutil.RoleSecondary)
	cp, err = rm.GetReplicateCheckpoint()
	assert.NoError(t, err)
	assert.Equal(t, cp.ClusterID, secondaryClusterID)
	assert.Equal(t, cp.PChannel, secondaryClusterID+"-rootcoord-dml_0")
	assert.Nil(t, cp.MessageID)
	assert.Equal(t, cp.TimeTick, uint64(0))

	// idempotent switch back to secondary
	err = rm.SwitchReplicateMode(ctx, newAlterReplicateConfigMessage(secondaryClusterID, primaryClusterID))
	assert.NoError(t, err)
	assert.Equal(t, rm.Role(), replicateutil.RoleSecondary)
	cp, err = rm.GetReplicateCheckpoint()
	assert.NoError(t, err)
	assert.Equal(t, cp.ClusterID, secondaryClusterID)
	assert.Equal(t, cp.PChannel, secondaryClusterID+"-rootcoord-dml_0")
	assert.Nil(t, cp.MessageID)
	assert.Equal(t, cp.TimeTick, uint64(0))

	// add a new cluster and switch to primary
	err = rm.SwitchReplicateMode(ctx, newAlterReplicateConfigMessage(primaryClusterID, secondaryClusterID, "test3"))
	assert.NoError(t, err)
	assert.Equal(t, rm.Role(), replicateutil.RolePrimary)
	cp, err = rm.GetReplicateCheckpoint()
	assert.True(t, status.AsStreamingError(err).IsReplicateViolation())
	assert.Nil(t, cp)

	// idempotent add a new cluster and switch to primary
	err = rm.SwitchReplicateMode(ctx, newAlterReplicateConfigMessage(primaryClusterID, secondaryClusterID, "test3"))
	assert.NoError(t, err)
	assert.Equal(t, rm.Role(), replicateutil.RolePrimary)
	cp, err = rm.GetReplicateCheckpoint()
	assert.True(t, status.AsStreamingError(err).IsReplicateViolation())
	assert.Nil(t, cp)

	// add a new cluster and switch to secondary
	err = rm.SwitchReplicateMode(ctx, newAlterReplicateConfigMessage(secondaryClusterID, primaryClusterID, "test3"))
	assert.NoError(t, err)
	assert.Equal(t, rm.Role(), replicateutil.RoleSecondary)
	cp, err = rm.GetReplicateCheckpoint()
	assert.NoError(t, err)
	assert.Equal(t, cp.ClusterID, secondaryClusterID)
	assert.Equal(t, cp.PChannel, secondaryClusterID+"-rootcoord-dml_0")
	assert.Nil(t, cp.MessageID)
	assert.Equal(t, cp.TimeTick, uint64(0))

	// idempotent add a new cluster and switch to secondary
	err = rm.SwitchReplicateMode(ctx, newAlterReplicateConfigMessage(secondaryClusterID, primaryClusterID, "test3"))
	assert.NoError(t, err)
	assert.Equal(t, rm.Role(), replicateutil.RoleSecondary)
	cp, err = rm.GetReplicateCheckpoint()
	assert.NoError(t, err)
	assert.Equal(t, cp.ClusterID, secondaryClusterID)
	assert.Equal(t, cp.PChannel, secondaryClusterID+"-rootcoord-dml_0")
	assert.Nil(t, cp.MessageID)
	assert.Equal(t, cp.TimeTick, uint64(0))

	// switch the primary
	err = rm.SwitchReplicateMode(ctx, newAlterReplicateConfigMessage("test3", primaryClusterID, secondaryClusterID))
	assert.NoError(t, err)
	assert.Equal(t, rm.Role(), replicateutil.RoleSecondary)
	cp, err = rm.GetReplicateCheckpoint()
	assert.NoError(t, err)
	assert.Equal(t, cp.ClusterID, "test3")
	assert.Equal(t, cp.PChannel, "test3-rootcoord-dml_0")
	assert.Nil(t, cp.MessageID)
	assert.Equal(t, cp.TimeTick, uint64(0))

	// idempotent switch the primary
	err = rm.SwitchReplicateMode(ctx, newAlterReplicateConfigMessage("test3", primaryClusterID, secondaryClusterID))
	assert.NoError(t, err)
	assert.Equal(t, rm.Role(), replicateutil.RoleSecondary)
	cp, err = rm.GetReplicateCheckpoint()
	assert.NoError(t, err)
	assert.Equal(t, cp.ClusterID, "test3")
	assert.Equal(t, cp.PChannel, "test3-rootcoord-dml_0")
	assert.Nil(t, cp.MessageID)
	assert.Equal(t, cp.TimeTick, uint64(0))
}

func testMessageOnPrimary(t *testing.T, rm ReplicateManager) {
	// switch to primary
	err := rm.SwitchReplicateMode(context.Background(), newAlterReplicateConfigMessage("test1", "test2"))
	assert.NoError(t, err)

	// Test self-controlled message
	g, err := rm.BeginReplicateMessage(context.Background(), message.NewCreateSegmentMessageBuilderV2().
		WithHeader(&message.CreateSegmentMessageHeader{}).
		WithBody(&message.CreateSegmentMessageBody{}).
		WithVChannel("test1-rootcoord-dml_0").
		MustBuildMutable())
	assert.ErrorIs(t, err, ErrNotHandledByReplicateManager)
	assert.Nil(t, g)

	// Test non-replicate message
	msg := newNonReplicateMessage("test1")
	g, err = rm.BeginReplicateMessage(context.Background(), msg)
	assert.ErrorIs(t, err, ErrNotHandledByReplicateManager)
	assert.Nil(t, g)

	// Test replicate message
	msg = newReplicateMessage("test1", "test2")
	g, err = rm.BeginReplicateMessage(context.Background(), msg)
	assert.True(t, status.AsStreamingError(err).IsReplicateViolation())
	assert.Nil(t, g)
}

func testMessageOnSecondary(t *testing.T, rm ReplicateManager) {
	// switch to secondary
	err := rm.SwitchReplicateMode(context.Background(), newAlterReplicateConfigMessage("test2", "test1"))
	assert.NoError(t, err)

	// Test wrong cluster replicates
	msg := newReplicateMessage("test1", "test3")
	g, err := rm.BeginReplicateMessage(context.Background(), msg)
	assert.True(t, status.AsStreamingError(err).IsReplicateViolation())
	assert.Nil(t, g)

	// Test self-controlled message
	g, err = rm.BeginReplicateMessage(context.Background(), message.NewCreateSegmentMessageBuilderV2().
		WithHeader(&message.CreateSegmentMessageHeader{}).
		WithBody(&message.CreateSegmentMessageBody{}).
		WithVChannel("test1-rootcoord-dml_0").
		MustBuildMutable())
	assert.ErrorIs(t, err, ErrNotHandledByReplicateManager)
	assert.Nil(t, g)

	// Test non-replicate message
	msg = newNonReplicateMessage("test1")
	g, err = rm.BeginReplicateMessage(context.Background(), msg)
	assert.True(t, status.AsStreamingError(err).IsReplicateViolation())
	assert.Nil(t, g)

	// Test replicate message
	msg = newReplicateMessage("test1", "test2")
	g, err = rm.BeginReplicateMessage(context.Background(), msg)
	assert.NoError(t, err)
	assert.NotNil(t, g)
	g.Ack(nil)

	// Test replicate message
	msg = newReplicateMessage("test1", "test2")
	g, err = rm.BeginReplicateMessage(context.Background(), msg)
	assert.True(t, status.AsStreamingError(err).IsIgnoredOperation())
	assert.Nil(t, g)

	for idx, msg := range newReplicateTxnMessage("test1", "test2", 2) {
		g, err = rm.BeginReplicateMessage(context.Background(), msg)
		if idx%2 == 0 {
			assert.NoError(t, err)
			assert.NotNil(t, g)
			g.Ack(nil)
		} else {
			assert.True(t, status.AsStreamingError(err).IsIgnoredOperation())
			assert.Nil(t, g)
		}
	}

	msg = newReplicateMessage("test1", "test2", 2)
	g, err = rm.BeginReplicateMessage(context.Background(), msg)
	assert.True(t, status.AsStreamingError(err).IsIgnoredOperation())
	assert.Nil(t, g)

	g, err = rm.BeginReplicateMessage(context.Background(), newReplicateTxnMessage("test1", "test2", 2)[0])
	assert.True(t, status.AsStreamingError(err).IsIgnoredOperation())
	assert.Nil(t, g)
}

// newReplicateConfiguration creates a valid replicate configuration for testing
func newReplicateConfiguration(primaryClusterID string, secondaryClusterID ...string) *commonpb.ReplicateConfiguration {
	clusters := []*commonpb.MilvusCluster{
		{ClusterId: primaryClusterID, Pchannels: []string{primaryClusterID + "-rootcoord-dml_0", primaryClusterID + "-rootcoord-dml_1"}},
	}
	crossClusterTopology := []*commonpb.CrossClusterTopology{}
	for _, secondaryClusterID := range secondaryClusterID {
		clusters = append(clusters, &commonpb.MilvusCluster{ClusterId: secondaryClusterID, Pchannels: []string{secondaryClusterID + "-rootcoord-dml_0", secondaryClusterID + "-rootcoord-dml_1"}})
		crossClusterTopology = append(crossClusterTopology, &commonpb.CrossClusterTopology{SourceClusterId: primaryClusterID, TargetClusterId: secondaryClusterID})
	}
	return &commonpb.ReplicateConfiguration{
		Clusters:             clusters,
		CrossClusterTopology: crossClusterTopology,
	}
}

func newAlterReplicateConfigMessage(primaryClusterID string, secondaryClusterID ...string) message.MutableAlterReplicateConfigMessageV2 {
	return message.MustAsMutableAlterReplicateConfigMessageV2(message.NewAlterReplicateConfigMessageBuilderV2().
		WithHeader(&message.AlterReplicateConfigMessageHeader{
			ReplicateConfiguration: newReplicateConfiguration(primaryClusterID, secondaryClusterID...),
		}).
		WithBody(&message.AlterReplicateConfigMessageBody{}).
		WithVChannel(primaryClusterID + "-rootcoord-dml_0").
		MustBuildMutable())
}

func newNonReplicateMessage(clusterID string) message.MutableMessage {
	return message.NewCreateDatabaseMessageBuilderV2().
		WithHeader(&message.CreateDatabaseMessageHeader{}).
		WithBody(&message.CreateDatabaseMessageBody{}).
		WithVChannel(clusterID + "-rootcoord-dml_0").
		MustBuildMutable()
}

func newReplicateMessage(clusterID string, sourceClusterID string, timetick ...uint64) message.MutableMessage {
	tt := uint64(1)
	if len(timetick) > 0 {
		tt = timetick[0]
	}
	msg := message.NewCreateDatabaseMessageBuilderV2().
		WithHeader(&message.CreateDatabaseMessageHeader{}).
		WithBody(&message.CreateDatabaseMessageBody{}).
		WithVChannel(sourceClusterID + "-rootcoord-dml_0").
		MustBuildMutable().
		WithTimeTick(tt).
		WithLastConfirmed(walimplstest.NewTestMessageID(1)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(1))

	replicateMsg := message.NewReplicateMessage(
		sourceClusterID,
		msg.IntoImmutableMessageProto(),
	)
	replicateMsg.OverwriteReplicateVChannel(
		clusterID + "-rootcoord-dml_0",
	)
	return replicateMsg
}

func newImmutableTxnMessage(clusterID string, timetick ...uint64) []message.ImmutableMessage {
	tt := uint64(1)
	if len(timetick) > 0 {
		tt = timetick[0]
	}
	immutables := []message.ImmutableMessage{
		message.NewBeginTxnMessageBuilderV2().
			WithHeader(&message.BeginTxnMessageHeader{}).
			WithBody(&message.BeginTxnMessageBody{}).
			WithVChannel(clusterID + "-rootcoord-dml_0").
			MustBuildMutable().
			WithTxnContext(message.TxnContext{
				TxnID:     message.TxnID(1),
				Keepalive: message.TxnKeepaliveInfinite,
			}).
			WithTimeTick(tt).
			WithLastConfirmed(walimplstest.NewTestMessageID(1)).
			IntoImmutableMessage(walimplstest.NewTestMessageID(1)),
		message.NewCreateDatabaseMessageBuilderV2().
			WithHeader(&message.CreateDatabaseMessageHeader{}).
			WithBody(&message.CreateDatabaseMessageBody{}).
			WithVChannel(clusterID + "-rootcoord-dml_0").
			MustBuildMutable().
			WithTxnContext(message.TxnContext{
				TxnID:     message.TxnID(1),
				Keepalive: message.TxnKeepaliveInfinite,
			}).
			WithTimeTick(tt).
			WithLastConfirmed(walimplstest.NewTestMessageID(1)).
			IntoImmutableMessage(walimplstest.NewTestMessageID(1)),
		message.NewCommitTxnMessageBuilderV2().
			WithHeader(&message.CommitTxnMessageHeader{}).
			WithBody(&message.CommitTxnMessageBody{}).
			WithVChannel(clusterID + "-rootcoord-dml_0").
			MustBuildMutable().
			WithTxnContext(message.TxnContext{
				TxnID:     message.TxnID(1),
				Keepalive: message.TxnKeepaliveInfinite,
			}).
			WithTimeTick(tt).
			WithLastConfirmed(walimplstest.NewTestMessageID(1)).
			IntoImmutableMessage(walimplstest.NewTestMessageID(1)),
	}
	return immutables
}

func newReplicateTxnMessage(clusterID string, sourceClusterID string, timetick ...uint64) []message.MutableMessage {
	immutables := newImmutableTxnMessage(sourceClusterID, timetick...)
	replicateMsgs := []message.MutableMessage{}
	for _, immutable := range immutables {
		replicateMsg := message.NewReplicateMessage(
			sourceClusterID,
			immutable.IntoImmutableMessageProto(),
		)
		replicateMsg.OverwriteReplicateVChannel(
			clusterID + "-rootcoord-dml_0",
		)
		replicateMsgs = append(replicateMsgs, replicateMsg)
		// test the idempotency
		replicateMsgs = append(replicateMsgs, replicateMsg)
	}
	return replicateMsgs
}
