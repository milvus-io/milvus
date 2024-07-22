package message_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

func TestAsSpecializedMessage(t *testing.T) {
	m, err := message.NewInsertMessageBuilderV1().
		WithMessageHeader(&message.InsertMessageHeader{
			CollectionId: 1,
			Partitions: []*message.PartitionSegmentAssignment{
				{
					PartitionId: 1,
					Rows:        100,
					BinarySize:  1000,
				},
			},
		}).
		WithPayload(&msgpb.InsertRequest{}).BuildMutable()
	assert.NoError(t, err)

	insertMsg, err := message.AsMutableInsertMessage(m)
	assert.NoError(t, err)
	assert.NotNil(t, insertMsg)
	assert.Equal(t, int64(1), insertMsg.MessageHeader().CollectionId)

	h := insertMsg.MessageHeader()
	h.Partitions[0].SegmentAssignment = &message.SegmentAssignment{
		SegmentId: 1,
	}
	insertMsg.OverwriteMessageHeader(h)

	createColMsg, err := message.AsMutableCreateCollection(m)
	assert.NoError(t, err)
	assert.Nil(t, createColMsg)

	m2 := m.IntoImmutableMessage(mock_message.NewMockMessageID(t))

	insertMsg2, err := message.AsImmutableInsertMessage(m2)
	assert.NoError(t, err)
	assert.NotNil(t, insertMsg2)
	assert.Equal(t, int64(1), insertMsg2.MessageHeader().CollectionId)
	assert.Equal(t, insertMsg2.MessageHeader().Partitions[0].SegmentAssignment.SegmentId, int64(1))

	createColMsg2, err := message.AsMutableCreateCollection(m)
	assert.NoError(t, err)
	assert.Nil(t, createColMsg2)
}
