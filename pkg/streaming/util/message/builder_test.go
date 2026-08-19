package message_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type testBodyEncoder struct {
	encodedSize func() (int, error)
	marshalTo   func([]byte) (int, error)
}

func (e *testBodyEncoder) EncodedSize() (int, error) {
	return e.encodedSize()
}

func (e *testBodyEncoder) MarshalTo(dst []byte) (int, error) {
	return e.marshalTo(dst)
}

func (e *testBodyEncoder) BodyType() *msgpb.InsertRequest {
	return nil
}

func TestMutableBuilder(t *testing.T) {
	b := message.NewTimeTickMessageBuilderV1().
		WithHeader(&message.TimeTickMessageHeader{}).
		WithBody(&msgpb.TimeTickMsg{}).
		WithAllVChannel().
		MustBuildMutable()
	assert.True(t, b.IsPersisted())
	assert.Equal(t, b.VChannel(), "")
	mlog.Info(context.TODO(), "test", mlog.Object("msg", b))

	b = message.NewTimeTickMessageBuilderV1().
		WithHeader(&message.TimeTickMessageHeader{}).
		WithBody(&msgpb.TimeTickMsg{}).
		WithNotPersisted().
		WithVChannel("v1").
		MustBuildMutable()
	assert.False(t, b.IsPersisted())
	assert.Equal(t, b.VChannel(), "v1")
	mlog.Info(context.TODO(), "test", mlog.Object("msg", b))

	assert.Panics(t, func() {
		message.NewCreateCollectionMessageBuilderV1().WithNotPersisted()
	})
}

func TestMutableBuilderWithBodyEncoder(t *testing.T) {
	body := &msgpb.InsertRequest{
		ShardName: "v1",
		RowIDs:    []int64{1, 2, 3},
	}
	expectedPayload, err := proto.Marshal(body)
	require.NoError(t, err)

	encodedSizeCalls := 0
	marshalToCalls := 0
	encoder := &testBodyEncoder{
		encodedSize: func() (int, error) {
			encodedSizeCalls++
			return len(expectedPayload), nil
		},
		marshalTo: func(dst []byte) (int, error) {
			marshalToCalls++
			assert.Len(t, dst, len(expectedPayload))
			assert.Equal(t, len(expectedPayload), cap(dst))
			return copy(dst, expectedPayload), nil
		},
	}

	msg, err := message.NewInsertMessageBuilderV1().
		WithHeader(&message.InsertMessageHeader{}).
		WithBodyEncoder(encoder).
		WithVChannel("v1").
		BuildMutable()
	require.NoError(t, err)
	assert.Equal(t, expectedPayload, msg.Payload())
	assert.Equal(t, len(expectedPayload), cap(msg.Payload()))
	assert.Equal(t, 1, encodedSizeCalls)
	assert.Equal(t, 1, marshalToCalls)

	insertMsg, err := message.AsMutableInsertMessageV1(msg)
	require.NoError(t, err)
	actualBody, err := insertMsg.Body()
	require.NoError(t, err)
	assert.True(t, proto.Equal(body, actualBody))
}

func TestMutableBuilderBodyEncoderValidation(t *testing.T) {
	build := func(encoder message.BodyEncoder[*msgpb.InsertRequest]) (message.MutableMessage, error) {
		return message.NewInsertMessageBuilderV1().
			WithHeader(&message.InsertMessageHeader{}).
			WithBodyEncoder(encoder).
			WithVChannel("v1").
			BuildMutable()
	}

	t.Run("encoded size error", func(t *testing.T) {
		expectedErr := merr.WrapErrServiceInternalMsg("encoded size failed")
		_, err := build(&testBodyEncoder{
			encodedSize: func() (int, error) { return 0, expectedErr },
			marshalTo:   func([]byte) (int, error) { panic("unexpected MarshalTo call") },
		})
		require.ErrorIs(t, err, expectedErr)
		assert.ErrorContains(t, err, "failed to get encoded body size")
	})

	t.Run("negative encoded size", func(t *testing.T) {
		_, err := build(&testBodyEncoder{
			encodedSize: func() (int, error) { return -1, nil },
			marshalTo:   func([]byte) (int, error) { panic("unexpected MarshalTo call") },
		})
		require.ErrorIs(t, err, merr.ErrServiceInternal)
		assert.ErrorContains(t, err, "negative encoded size")
	})

	t.Run("marshal error", func(t *testing.T) {
		expectedErr := merr.WrapErrServiceInternalMsg("marshal failed")
		_, err := build(&testBodyEncoder{
			encodedSize: func() (int, error) { return 1, nil },
			marshalTo:   func([]byte) (int, error) { return 0, expectedErr },
		})
		require.ErrorIs(t, err, expectedErr)
		assert.ErrorContains(t, err, "failed to marshal body")
	})

	t.Run("written size mismatch", func(t *testing.T) {
		_, err := build(&testBodyEncoder{
			encodedSize: func() (int, error) { return 2, nil },
			marshalTo:   func(dst []byte) (int, error) { return copy(dst, []byte{1}), nil },
		})
		require.ErrorIs(t, err, merr.ErrServiceInternal)
		assert.ErrorContains(t, err, "wrote 1 bytes, expected 2")
	})
}

func TestMutableBuilderBodyAndEncoderAreMutuallyExclusive(t *testing.T) {
	encoder := &testBodyEncoder{
		encodedSize: func() (int, error) { return 0, nil },
		marshalTo:   func([]byte) (int, error) { return 0, nil },
	}

	assert.PanicsWithValue(t, "message builder must set exactly one of body or body encoder", func() {
		message.NewInsertMessageBuilderV1().
			WithHeader(&message.InsertMessageHeader{}).
			WithBody(&msgpb.InsertRequest{}).
			WithBodyEncoder(encoder).
			WithVChannel("v1").
			BuildMutable()
	})

	var nilEncoder *testBodyEncoder
	assert.PanicsWithValue(t, "message builder not ready for body field", func() {
		message.NewInsertMessageBuilderV1().
			WithHeader(&message.InsertMessageHeader{}).
			WithBodyEncoder(nilEncoder).
			WithVChannel("v1").
			BuildMutable()
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
	mlog.Info(context.TODO(), "test", mlog.Object("msg", mutableMsg))
	immutableMsg := mutableMsg.IntoImmutableMessage(msgID)
	b.Add(immutableMsg)

	commit := message.NewCommitTxnMessageBuilderV2().
		WithHeader(&message.CommitTxnMessageHeader{}).
		WithBody(&message.CommitTxnMessageBody{}).
		WithVChannel("v1").
		MustBuildMutable()
	rh := message.ReplicateHeader{
		ClusterID:              "by-dev",
		MessageID:              msgID,
		LastConfirmedMessageID: msgID,
		TimeTick:               3,
		VChannel:               "v1",
	}
	immutableCommit := commit.WithTimeTick(3).WithTxnContext(txnCtx).WithLastConfirmed(msgID).WithReplicateHeader(&rh).IntoImmutableMessage(msgID)
	mlog.Info(context.TODO(), "test", mlog.Object("msg", immutableCommit))

	assert.NotZero(t, b.EstimateSize())
	beginMsg, msgs := b.Messages()
	assert.NotEmpty(t, beginMsg)
	assert.Len(t, msgs, 1)
	immutableTxnMsg, err := b.Build(message.MustAsImmutableCommitTxnMessageV2(immutableCommit))
	assert.NoError(t, err)
	assert.Equal(t, "by-dev", immutableTxnMsg.ReplicateHeader().ClusterID)
	assert.Equal(t, uint64(3), immutableTxnMsg.ReplicateHeader().TimeTick)
	assert.Equal(t, "v1", immutableTxnMsg.ReplicateHeader().VChannel)
	assert.Equal(t, msgID, immutableTxnMsg.ReplicateHeader().MessageID)
	assert.Equal(t, msgID, immutableTxnMsg.ReplicateHeader().LastConfirmedMessageID)
	mlog.Info(context.TODO(), "test", mlog.Object("msg", immutableTxnMsg))
}

func TestReplicateBuilder(t *testing.T) {
	msg := message.NewManualFlushMessageBuilderV2().
		WithHeader(&message.ManualFlushMessageHeader{}).
		WithBody(&message.ManualFlushMessageBody{}).
		WithBroadcast([]string{"v1", "v2"}).
		MustBuildBroadcast()

	msgs := msg.WithBroadcastID(1).SplitIntoMutableMessage()

	msgID := walimplstest.NewTestMessageID(1)
	var immutableMsg message.ImmutableMessage
	for _, msg := range msgs {
		if msg.VChannel() == "v1" {
			immutableMsg = msg.WithTimeTick(100).WithLastConfirmed(msgID).IntoImmutableMessage(msgID)
			break
		}
	}
	require.NotNil(t, immutableMsg)

	replicateMsg := message.MustNewReplicateMessage("by-dev", immutableMsg.IntoImmutableMessageProto())
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
