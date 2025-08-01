package message_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/v2/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

func TestAsSpecializedMessage(t *testing.T) {
	m, err := message.NewInsertMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.InsertMessageHeader{
			CollectionId: 1,
			Partitions: []*message.PartitionSegmentAssignment{
				{
					PartitionId: 1,
					Rows:        100,
					BinarySize:  1000,
				},
			},
		}).
		WithBody(&msgpb.InsertRequest{
			CollectionID: 1,
		}).BuildMutable()
	assert.NoError(t, err)

	insertMsg, err := message.AsMutableInsertMessageV1(m)
	assert.NoError(t, err)
	assert.NotNil(t, insertMsg)
	assert.Equal(t, int64(1), insertMsg.Header().CollectionId)
	body, err := insertMsg.Body()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), body.CollectionID)

	_, err = message.AsMutableInsertMessageV1(insertMsg)
	assert.NoError(t, err)

	h := insertMsg.Header()
	h.Partitions[0].SegmentAssignment = &message.SegmentAssignment{
		SegmentId: 1,
	}
	insertMsg.OverwriteHeader(h)
	assert.True(t, insertMsg.IsPersisted())

	createColMsg, err := message.AsMutableCreateCollectionMessageV1(m)
	assert.Error(t, err)
	assert.Nil(t, createColMsg)

	id := mock_message.NewMockMessageID(t)
	id.EXPECT().String().Return("1")
	m2 := m.IntoImmutableMessage(id)

	assert.Panics(t, func() {
		_ = message.MustAsImmutableDeleteMessageV1(m2)
	})

	insertMsg2, err := message.AsImmutableInsertMessageV1(m2)
	assert.NoError(t, err)
	assert.NotNil(t, insertMsg2)
	assert.Equal(t, int64(1), insertMsg2.Header().CollectionId)
	assert.Equal(t, insertMsg2.Header().Partitions[0].SegmentAssignment.SegmentId, int64(1))
	body, err = insertMsg2.Body()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), body.CollectionID)

	// double as is ok.
	_, err = message.AsImmutableInsertMessageV1(insertMsg2)
	assert.NoError(t, err)

	insertMsg2 = message.MustAsImmutableInsertMessageV1(m2)
	assert.NotNil(t, insertMsg2)

	createColMsg2, err := message.AsMutableCreateCollectionMessageV1(m)
	assert.Error(t, err)
	assert.Nil(t, createColMsg2)

	assert.Panics(t, func() {
		message.MustAsMutableCreateCollectionMessageV1(m)
	})
}
