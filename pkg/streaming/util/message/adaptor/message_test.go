package adaptor

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

func TestNewMsgPackFromInsertMessage(t *testing.T) {
	id := rmq.NewRmqID(1)

	fieldCount := map[int64]int{
		3: 1000,
		4: 2000,
		5: 3000,
		6: 5000,
	}
	tt := uint64(time.Now().UnixNano())
	immutableMessages := make([]message.ImmutableMessage, 0, len(fieldCount))
	for segmentID, rowNum := range fieldCount {
		insertMsg := message.CreateTestInsertMessage(t, segmentID, rowNum, tt, id)
		immutableMessage := insertMsg.WithOldVersion().IntoImmutableMessage(id)
		immutableMessages = append(immutableMessages, immutableMessage)
	}

	pack, err := NewMsgPackFromMessage(immutableMessages...)
	assert.NoError(t, err)
	assert.NotNil(t, pack)
	assert.Equal(t, tt, pack.BeginTs)
	assert.Equal(t, tt, pack.EndTs)
	assert.Len(t, pack.Msgs, len(fieldCount))

	for _, msg := range pack.Msgs {
		insertMsg := msg.(*msgstream.InsertMsg)
		rowNum, ok := fieldCount[insertMsg.GetSegmentID()]
		assert.True(t, ok)

		assert.Len(t, insertMsg.Timestamps, rowNum)
		assert.Len(t, insertMsg.RowIDs, rowNum)
		assert.Len(t, insertMsg.FieldsData, 2)
		for _, fieldData := range insertMsg.FieldsData {
			if data := fieldData.GetScalars().GetBoolData(); data != nil {
				assert.Len(t, data.Data, rowNum)
			} else if data := fieldData.GetScalars().GetIntData(); data != nil {
				assert.Len(t, data.Data, rowNum)
			}
		}

		for _, ts := range insertMsg.Timestamps {
			assert.Equal(t, ts, tt)
		}
	}
}

func TestNewMsgPackFromCreateCollectionMessage(t *testing.T) {
	id := rmq.NewRmqID(1)

	tt := uint64(time.Now().UnixNano())
	msg := message.CreateTestCreateCollectionMessage(t, 1, tt, id)
	immutableMessage := msg.IntoImmutableMessage(id)

	pack, err := NewMsgPackFromMessage(immutableMessage)
	assert.NoError(t, err)
	assert.NotNil(t, pack)
	assert.Equal(t, tt, pack.BeginTs)
	assert.Equal(t, tt, pack.EndTs)
}

func TestNewMsgPackFromCreateSegmentMessage(t *testing.T) {
	id := rmq.NewRmqID(1)

	tt := uint64(time.Now().UnixNano())
	mutableMsg, err := message.NewCreateSegmentMessageBuilderV2().
		WithHeader(&message.CreateSegmentMessageHeader{}).
		WithBody(&message.CreateSegmentMessageBody{}).
		WithVChannel("v1").
		BuildMutable()
	assert.NoError(t, err)
	immutableCreateSegmentMsg := mutableMsg.WithTimeTick(tt).WithLastConfirmedUseMessageID().IntoImmutableMessage(id)
	pack, err := NewMsgPackFromMessage(immutableCreateSegmentMsg)
	assert.NoError(t, err)
	assert.NotNil(t, pack)
	assert.Equal(t, tt, pack.BeginTs)
	assert.Equal(t, tt, pack.EndTs)
}

func TestNewMsgPackFromCreateIndexMessage(t *testing.T) {
	id := rmq.NewRmqID(1)

	tt := uint64(time.Now().UnixNano())
	mutableMsg, err := message.NewCreateIndexMessageBuilderV2().
		WithHeader(&message.CreateIndexMessageHeader{}).
		WithBody(&message.CreateIndexMessageBody{}).
		WithVChannel("v1").
		BuildMutable()
	assert.NoError(t, err)
	immutableCreateIndexMsg := mutableMsg.WithTimeTick(tt).WithLastConfirmedUseMessageID().IntoImmutableMessage(id)
	pack, err := NewMsgPackFromMessage(immutableCreateIndexMsg)
	assert.NoError(t, err)
	assert.NotNil(t, pack)
	assert.Equal(t, tt, pack.BeginTs)
	assert.Equal(t, tt, pack.EndTs)
}

func TestDeferredConsumeMessageMetadata(t *testing.T) {
	id := rmq.NewRmqID(1)
	msg := message.NewDeleteMessageBuilderV1().
		WithHeader(&message.DeleteMessageHeader{CollectionId: 1, Rows: 1}).
		WithBody(&msgpb.DeleteRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_Delete, MsgID: 42,
				ReplicateInfo: &commonpb.ReplicateInfo{ReplicateID: "replica"},
			},
			CollectionID: 1, ShardName: "v1", Timestamps: []uint64{1},
		}).WithVChannel("v1").MustBuildMutable().
		WithTimeTick(100).WithLastConfirmed(id).IntoImmutableMessage(rmq.NewRmqID(2))
	pack, err := newConsumeMsgPackFromMessage(msg)
	require.NoError(t, err)
	require.Len(t, pack.Msgs, 1)
	deferred := pack.Msgs[0]
	require.Equal(t, commonpb.MsgType_Delete, deferred.GetType())
	require.Equal(t, "1", deferred.GetCollectionID())
	require.Equal(t, "v1", deferred.GetVChannel())
	require.Equal(t, uint64(100), deferred.GetTimestamp())
	require.Positive(t, deferred.GetSize())
	require.Equal(t, MustGetMQWrapperIDFromMessage(id).Serialize(), deferred.GetMessageID())
	require.Equal(t, pack.StartPositions[0], deferred.GetPosition())

	// Body-backed metadata decodes on demand and shares the cached result.
	require.Equal(t, int64(42), deferred.GetID())
	require.Equal(t, "replica", deferred.(interface{ GetReplicateID() string }).GetReplicateID())
	decoded, err := deferred.Unmarshal(UnmashalerDispatcher)
	require.NoError(t, err)
	pos := &msgpb.MsgPosition{ChannelName: "pchannel", MsgID: []byte{3}, Timestamp: 100}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	deferred.SetPosition(pos)
	deferred.SetTraceCtx(ctx)
	again, err := deferred.Unmarshal(UnmashalerDispatcher)
	require.NoError(t, err)
	require.Same(t, decoded, again)
	require.Same(t, pos, again.Position())
	require.Equal(t, "pchannel", deferred.GetPChannel())
	require.Equal(t, []byte{3}, deferred.GetMessageID())
	require.Same(t, ctx, again.TraceCtx())
	require.Equal(t, []uint64{100}, again.(*msgstream.DeleteMsg).Timestamps)

	// Routing metadata must remain readable even when the body cannot decode.
	wire := msg.IntoMessageProto()
	broken := message.NewImmutableMesasge(rmq.NewRmqID(2), []byte{0xff}, wire.Properties)
	pack, err = newConsumeMsgPackFromMessage(broken)
	require.NoError(t, err)
	require.Equal(t, "v1", pack.Msgs[0].GetVChannel())
	require.Equal(t, "1", pack.Msgs[0].GetCollectionID())
	_, err = pack.Msgs[0].Unmarshal(UnmashalerDispatcher)
	require.Error(t, err)
	require.Panics(t, func() { pack.Msgs[0].GetID() })
}

func TestDeferredConsumeMsgPackValidation(t *testing.T) {
	id := rmq.NewRmqID(1)
	makeInsert := func(tick uint64) message.MutableMessage {
		return message.CreateTestInsertMessage(t, 3, 1, tick, id)
	}
	t.Run("empty", func(t *testing.T) {
		pack, err := newConsumeMsgPackFromMessage()
		require.NoError(t, err)
		require.Nil(t, pack)
	})
	t.Run("invalid headers", func(t *testing.T) {
		deleted := message.NewDeleteMessageBuilderV1().
			WithHeader(&message.DeleteMessageHeader{CollectionId: 1, Rows: 1}).
			WithBody(&msgpb.DeleteRequest{}).WithVChannel("v1").MustBuildMutable().
			WithTimeTick(100).WithLastConfirmed(id)
		for _, msg := range []message.MutableMessage{makeInsert(100), deleted} {
			wire := msg.IntoMessageProto()
			wire.Properties["_h"] = "invalid header"
			broken := message.NewImmutableMesasge(id, wire.Payload, wire.Properties)
			pack, err := newConsumeMsgPackFromMessage(broken)
			require.Error(t, err)
			require.Nil(t, pack)
		}
	})
	t.Run("legacy body failure preserves valid siblings", func(t *testing.T) {
		first := makeInsert(100).WithOldVersion().IntoImmutableMessage(id)
		wire := makeInsert(101).WithOldVersion().IntoMessageProto()
		broken := message.NewImmutableMesasge(id, []byte{0xff}, wire.Properties)
		last := makeInsert(102).IntoImmutableMessage(id)
		pack, err := newConsumeMsgPackFromMessage(first, broken, last)
		require.Error(t, err)
		require.Len(t, pack.Msgs, 2)
		require.Equal(t, uint64(100), pack.BeginTs)
		require.Equal(t, uint64(102), pack.EndTs)
		require.Equal(t, uint64(100), pack.StartPositions[0].Timestamp)
		require.Equal(t, uint64(102), pack.EndPositions[0].Timestamp)
		for i, msg := range pack.Msgs {
			decoded, err := msg.Unmarshal(UnmashalerDispatcher)
			require.NoError(t, err)
			require.Equal(t, []uint64{uint64(100 + i*2)}, decoded.(*msgstream.InsertMsg).Timestamps)
		}
		pack, err = newConsumeMsgPackFromMessage(broken)
		require.Error(t, err)
		require.Nil(t, pack)
	})
}
