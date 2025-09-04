package message_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/walimplstest"
)

func TestMutableBuilder(t *testing.T) {
	b := message.NewTimeTickMessageBuilderV1().
		WithHeader(&message.TimeTickMessageHeader{}).
		WithBody(&msgpb.TimeTickMsg{}).
		WithAllVChannel().
		MustBuildMutable()
	assert.True(t, b.IsPersisted())
	assert.Equal(t, b.VChannel(), "")
	log.Info("test", zap.Object("msg", b))

	b = message.NewTimeTickMessageBuilderV1().
		WithHeader(&message.TimeTickMessageHeader{}).
		WithBody(&msgpb.TimeTickMsg{}).
		WithNotPersisted().
		WithVChannel("v1").
		MustBuildMutable()
	assert.False(t, b.IsPersisted())
	assert.Equal(t, b.VChannel(), "v1")
	log.Info("test", zap.Object("msg", b))

	assert.Panics(t, func() {
		message.NewCreateCollectionMessageBuilderV1().WithNotPersisted()
	})
}

func TestImmutableTxnBuilder(t *testing.T) {
	txnCtx := message.TxnContext{
		TxnID:     1,
		Keepalive: time.Second,
	}
	begin := message.NewBeginTxnMessageBuilderV2().
		WithHeader(&message.BeginTxnMessageHeader{
			KeepaliveMilliseconds: 1000,
		}).
		WithBody(&message.BeginTxnMessageBody{}).
		WithVChannel("v1").
		MustBuildMutable()
	msgID := walimplstest.NewTestMessageID(1)
	immutableBegin := begin.WithTimeTick(1).WithTxnContext(txnCtx).WithLastConfirmed(msgID).IntoImmutableMessage(msgID)

	b := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(immutableBegin))
	assert.NotZero(t, b.EstimateSize())
	assert.Greater(t, b.ExpiredTimeTick(), uint64(1))

	msg := message.NewInsertMessageBuilderV1().
		WithHeader(&message.InsertMessageHeader{}).
		WithBody(&msgpb.InsertRequest{}).
		WithVChannel("v1").
		MustBuildMutable()
	mutableMsg := msg.WithTimeTick(2).WithTxnContext(txnCtx).WithLastConfirmed(msgID)
	log.Info("test", zap.Object("msg", mutableMsg))
	immutableMsg := mutableMsg.IntoImmutableMessage(msgID)
	b.Add(immutableMsg)

	commit := message.NewCommitTxnMessageBuilderV2().
		WithHeader(&message.CommitTxnMessageHeader{}).
		WithBody(&message.CommitTxnMessageBody{}).
		WithVChannel("v1").
		MustBuildMutable()
	immutableCommit := commit.WithTimeTick(3).WithTxnContext(txnCtx).WithLastConfirmed(msgID).IntoImmutableMessage(msgID)
	log.Info("test", zap.Object("msg", immutableCommit))

	assert.NotZero(t, b.EstimateSize())
	beginMsg, msgs := b.Messages()
	assert.NotEmpty(t, beginMsg)
	assert.Len(t, msgs, 1)
	immutableTxnMsg, err := b.Build(message.MustAsImmutableCommitTxnMessageV2(immutableCommit))
	assert.NoError(t, err)
	log.Info("test", zap.Object("msg", immutableTxnMsg))
}

func TestReplicateBuilder(t *testing.T) {
	msg := message.NewManualFlushMessageBuilderV2().
		WithHeader(&message.ManualFlushMessageHeader{}).
		WithBody(&message.ManualFlushMessageBody{}).
		WithBroadcast([]string{"v1", "v2"}).
		MustBuildBroadcast()

	msgs := msg.WithBroadcastID(1).SplitIntoMutableMessage()

	msgID := walimplstest.NewTestMessageID(1)
	immutableMsg := msgs[0].WithTimeTick(100).WithLastConfirmed(msgID).IntoImmutableMessage(msgID)

	replicateMsg := message.NewReplicateMessage("by-dev", immutableMsg.IntoImmutableMessageProto())
	assert.NotNil(t, replicateMsg)
	assert.Equal(t, "by-dev", replicateMsg.ReplicateHeader().ClusterID)
	assert.Equal(t, uint64(100), replicateMsg.ReplicateHeader().TimeTick)
	assert.Equal(t, "v1", replicateMsg.ReplicateHeader().VChannel)
	assert.Equal(t, "v1", replicateMsg.VChannel())
	assert.True(t, msgID.EQ(replicateMsg.ReplicateHeader().MessageID))
	assert.True(t, msgID.EQ(replicateMsg.ReplicateHeader().LastConfirmedMessageID))

	replicateMsg.OverwriteReplicateVChannel("v11", []string{"v11", "v12"})
	assert.Equal(t, "v11", replicateMsg.VChannel())
	assert.Equal(t, []string{"v11", "v12"}, replicateMsg.BroadcastHeader().VChannels)
	assert.Equal(t, uint64(1), replicateMsg.BroadcastHeader().BroadcastID)
}
