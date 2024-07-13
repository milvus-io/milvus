package ack

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/mocks/streaming/util/mock_message"
)

func TestDetail(t *testing.T) {
	assert.Panics(t, func() {
		newAckDetail(0, mock_message.NewMockMessageID(t))
	})
	msgID := mock_message.NewMockMessageID(t)
	msgID.EXPECT().EQ(msgID).Return(true)

	ackDetail := newAckDetail(1, msgID)
	assert.Equal(t, uint64(1), ackDetail.Timestamp)
	assert.True(t, ackDetail.LastConfirmedMessageID.EQ(msgID))
	assert.False(t, ackDetail.IsSync)
	assert.NoError(t, ackDetail.Err)

	OptSync()(ackDetail)
	assert.True(t, ackDetail.IsSync)
	OptError(errors.New("test"))(ackDetail)
	assert.Error(t, ackDetail.Err)
}
