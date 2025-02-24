package adaptor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/rmq"
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
		immutableMessage := insertMsg.IntoImmutableMessage(id)
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
