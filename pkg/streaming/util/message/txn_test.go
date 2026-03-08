package message_test

import (
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/walimplstest"
)

func TestTxn(t *testing.T) {
	txn := message.NewTxnContextFromProto(&messagespb.TxnContext{
		TxnId:                 1,
		KeepaliveMilliseconds: 1000,
	})
	assert.Equal(t, message.TxnID(1), txn.TxnID)
	assert.Equal(t, time.Second, txn.Keepalive)

	assert.Equal(t, int64(1), txn.IntoProto().TxnId)
	assert.Equal(t, int64(1000), txn.IntoProto().KeepaliveMilliseconds)

	txn = message.NewTxnContextFromProto(nil)
	assert.Nil(t, txn)
}

func TestAsImmutableTxnMessage(t *testing.T) {
	txnCtx := message.TxnContext{
		TxnID:     1,
		Keepalive: time.Second,
	}
	begin, _ := message.NewBeginTxnMessageBuilderV2().
		WithVChannel("vchan").
		WithHeader(&message.BeginTxnMessageHeader{}).
		WithBody(&message.BeginTxnMessageBody{}).
		BuildMutable()
	imBegin := begin.WithTxnContext(txnCtx).
		WithTimeTick(1).
		WithLastConfirmed(walimplstest.NewTestMessageID(1)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(1))

	beginMsg, _ := message.AsImmutableBeginTxnMessageV2(imBegin)

	insert, _ := message.NewInsertMessageBuilderV1().
		WithVChannel("vchan").
		WithHeader(&message.InsertMessageHeader{}).
		WithBody(&msgpb.InsertRequest{}).
		BuildMutable()

	commit, _ := message.NewCommitTxnMessageBuilderV2().
		WithVChannel("vchan").
		WithHeader(&message.CommitTxnMessageHeader{}).
		WithBody(&message.CommitTxnMessageBody{}).
		BuildMutable()

	imCommit := commit.WithTxnContext(txnCtx).
		WithTimeTick(3).
		WithLastConfirmed(walimplstest.NewTestMessageID(3)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(4))

	commitMsg, _ := message.AsImmutableCommitTxnMessageV2(imCommit)

	txnMsg, err := message.NewImmutableTxnMessageBuilder(beginMsg).
		Add(insert.WithTimeTick(2).WithTxnContext(txnCtx).IntoImmutableMessage(walimplstest.NewTestMessageID(2))).
		Build(commitMsg)

	assert.NoError(t, err)
	assert.NotNil(t, txnMsg)
	assert.Equal(t, uint64(3), txnMsg.TimeTick())
	assert.Equal(t, walimplstest.NewTestMessageID(4), txnMsg.MessageID())
	assert.Equal(t, walimplstest.NewTestMessageID(3), txnMsg.LastConfirmedMessageID())
	err = txnMsg.RangeOver(func(msg message.ImmutableMessage) error {
		assert.Equal(t, uint64(3), msg.TimeTick())
		return nil
	})
	assert.NoError(t, err)

	err = txnMsg.RangeOver(func(msg message.ImmutableMessage) error {
		return errors.New("error")
	})
	assert.Error(t, err)

	assert.NotNil(t, txnMsg.Commit())
	assert.Equal(t, 1, txnMsg.Size())
	assert.NotNil(t, txnMsg.Begin())
	assert.NotNil(t, message.AsImmutableTxnMessage(txnMsg))
	assert.Nil(t, message.AsImmutableTxnMessage(beginMsg))
}
