package ack

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/pkg/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls/impls/walimplstest"
)

func TestDetail(t *testing.T) {
	assert.Panics(t, func() {
		newAckDetail(0, mock_message.NewMockMessageID(t))
	})
	msgID := walimplstest.NewTestMessageID(1)

	ackDetail := newAckDetail(1, msgID)
	assert.Equal(t, uint64(1), ackDetail.BeginTimestamp)
	assert.True(t, ackDetail.LastConfirmedMessageID.EQ(msgID))
	assert.False(t, ackDetail.IsSync)
	assert.NoError(t, ackDetail.Err)

	OptSync()(ackDetail)
	assert.True(t, ackDetail.IsSync)
	OptError(errors.New("test"))(ackDetail)
	assert.Error(t, ackDetail.Err)

	OptMessageID(walimplstest.NewTestMessageID(1))(ackDetail)
	assert.NotNil(t, ackDetail.MessageID)

	OptTxnSession(&txn.TxnSession{})(ackDetail)
	assert.NotNil(t, ackDetail.TxnSession)
}
