package adaptor

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

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

func TestNewMsgPackFromMessageRestoresTraceContext(t *testing.T) {
	traceID, err := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	require.NoError(t, err)
	spanID, err := trace.SpanIDFromHex("0102030405060708")
	require.NoError(t, err)
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID,
		SpanID:  spanID,
	}))

	id := rmq.NewRmqID(1)
	mutableMsg := message.CreateTestInsertMessage(t, 3, 1, uint64(time.Now().UnixNano()), id)
	message.InjectTraceContext(ctx, mutableMsg)
	immutableMsg := mutableMsg.WithOldVersion().IntoImmutableMessage(id)

	pack, err := NewMsgPackFromMessage(immutableMsg)
	require.NoError(t, err)
	require.NotNil(t, pack)
	require.Len(t, pack.Msgs, 1)
	assert.Equal(t, traceID, trace.SpanContextFromContext(pack.Msgs[0].TraceCtx()).TraceID())
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
