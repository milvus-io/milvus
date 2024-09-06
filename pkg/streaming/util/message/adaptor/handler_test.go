package adaptor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls/impls/rmq"
)

func TestMsgPackAdaptorHandler(t *testing.T) {
	id := rmq.NewRmqID(1)

	h := NewMsgPackAdaptorHandler()
	insertMsg := message.CreateTestInsertMessage(t, 1, 100, 10, id)
	insertImmutableMessage := insertMsg.IntoImmutableMessage(id)
	ch := make(chan *msgstream.MsgPack, 1)
	go func() {
		for msgPack := range h.Chan() {
			ch <- msgPack
		}
		close(ch)
	}()
	h.Handle(insertImmutableMessage)
	msgPack := <-ch

	assert.Equal(t, uint64(10), msgPack.BeginTs)
	assert.Equal(t, uint64(10), msgPack.EndTs)
	for _, tsMsg := range msgPack.Msgs {
		assert.Equal(t, uint64(10), tsMsg.BeginTs())
		assert.Equal(t, uint64(10), tsMsg.EndTs())
		for _, ts := range tsMsg.(*msgstream.InsertMsg).Timestamps {
			assert.Equal(t, uint64(10), ts)
		}
	}

	deleteMsg, err := message.NewDeleteMessageBuilderV1().
		WithVChannel("vchan1").
		WithHeader(&message.DeleteMessageHeader{
			CollectionId: 1,
		}).
		WithBody(&msgpb.DeleteRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_Delete,
			},
			CollectionID: 1,
			PartitionID:  1,
			Timestamps:   []uint64{10},
		}).
		BuildMutable()
	assert.NoError(t, err)

	deleteImmutableMsg := deleteMsg.
		WithTimeTick(11).
		WithLastConfirmedUseMessageID().
		IntoImmutableMessage(id)

	h.Handle(deleteImmutableMsg)
	msgPack = <-ch
	assert.Equal(t, uint64(11), msgPack.BeginTs)
	assert.Equal(t, uint64(11), msgPack.EndTs)
	for _, tsMsg := range msgPack.Msgs {
		assert.Equal(t, uint64(11), tsMsg.BeginTs())
		assert.Equal(t, uint64(11), tsMsg.EndTs())
		for _, ts := range tsMsg.(*msgstream.DeleteMsg).Timestamps {
			assert.Equal(t, uint64(11), ts)
		}
	}

	// Create a txn message
	msg, err := message.NewBeginTxnMessageBuilderV2().
		WithVChannel("vchan1").
		WithHeader(&message.BeginTxnMessageHeader{
			KeepaliveMilliseconds: 1000,
		}).
		WithBody(&message.BeginTxnMessageBody{}).
		BuildMutable()
	assert.NoError(t, err)
	assert.NotNil(t, msg)

	txnCtx := message.TxnContext{
		TxnID:     1,
		Keepalive: time.Second,
	}

	beginImmutableMsg, err := message.AsImmutableBeginTxnMessageV2(msg.WithTimeTick(9).
		WithTxnContext(txnCtx).
		WithLastConfirmedUseMessageID().
		IntoImmutableMessage(rmq.NewRmqID(2)))
	assert.NoError(t, err)

	msg, err = message.NewCommitTxnMessageBuilderV2().
		WithVChannel("vchan1").
		WithHeader(&message.CommitTxnMessageHeader{}).
		WithBody(&message.CommitTxnMessageBody{}).
		BuildMutable()
	assert.NoError(t, err)

	commitImmutableMsg, err := message.AsImmutableCommitTxnMessageV2(msg.WithTimeTick(12).
		WithTxnContext(txnCtx).
		WithTxnContext(message.TxnContext{}).
		WithLastConfirmedUseMessageID().
		IntoImmutableMessage(rmq.NewRmqID(3)))
	assert.NoError(t, err)

	txn, err := message.NewImmutableTxnMessageBuilder(beginImmutableMsg).
		Add(insertMsg.WithTxnContext(txnCtx).IntoImmutableMessage(id)).
		Add(deleteMsg.WithTxnContext(txnCtx).IntoImmutableMessage(id)).
		Build(commitImmutableMsg)
	assert.NoError(t, err)

	h.Handle(txn)
	msgPack = <-ch

	assert.Equal(t, uint64(12), msgPack.BeginTs)
	assert.Equal(t, uint64(12), msgPack.EndTs)

	// Create flush message
	msg, err = message.NewFlushMessageBuilderV2().
		WithVChannel("vchan1").
		WithHeader(&message.FlushMessageHeader{}).
		WithBody(&message.FlushMessageBody{}).
		BuildMutable()
	assert.NoError(t, err)

	flushMsg := msg.
		WithTimeTick(13).
		WithLastConfirmedUseMessageID().
		IntoImmutableMessage(rmq.NewRmqID(4))

	h.Handle(flushMsg)

	msgPack = <-ch

	assert.Equal(t, uint64(13), msgPack.BeginTs)
	assert.Equal(t, uint64(13), msgPack.EndTs)

	h.Close()
	<-ch
}
