package utility

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestReOrderByTimeTickBuffer(t *testing.T) {
	buf := NewReOrderBuffer()
	timeticks := rand.Perm(25)
	for i, timetick := range timeticks {
		msg := mock_message.NewMockImmutableMessage(t)
		msg.EXPECT().EstimateSize().Return(1)
		msg.EXPECT().MessageID().Return(walimplstest.NewTestMessageID(int64(i)))
		msg.EXPECT().TimeTick().Return(uint64(timetick + 1))
		buf.Push(msg)
		assert.Equal(t, i+1, buf.Len())
	}

	result := buf.PopUtilTimeTick(0)
	assert.Len(t, result, 0)
	result = buf.PopUtilTimeTick(1)
	assert.Len(t, result, 1)
	for _, msg := range result {
		assert.LessOrEqual(t, msg.TimeTick(), uint64(1))
	}

	result = buf.PopUtilTimeTick(10)
	assert.Len(t, result, 9)
	for _, msg := range result {
		assert.LessOrEqual(t, msg.TimeTick(), uint64(10))
		assert.Greater(t, msg.TimeTick(), uint64(1))
	}

	result = buf.PopUtilTimeTick(25)
	assert.Len(t, result, 15)
	for _, msg := range result {
		assert.Greater(t, msg.TimeTick(), uint64(10))
		assert.LessOrEqual(t, msg.TimeTick(), uint64(25))
	}
}

func TestReOrderByTimeTickBufferDeduplicatesByWALAndMessageID(t *testing.T) {
	buf := NewReOrderBuffer()

	first := newReorderBufferTestMessage(t, message.WALNameRocksmq, "same-id", 1)
	second := newReorderBufferTestMessage(t, message.WALNameKafka, "same-id", 2)
	duplicate := newReorderBufferTestMessage(t, message.WALNameRocksmq, "same-id", 3)

	require.NoError(t, buf.Push(first))
	require.NoError(t, buf.Push(second))
	require.Error(t, buf.Push(duplicate))
	require.Equal(t, 2, buf.Len())

	require.Len(t, buf.PopUtilTimeTick(2), 2)
	require.NoError(t, buf.Push(duplicate))
	require.Equal(t, 1, buf.Len())
}

func newReorderBufferTestMessage(
	t *testing.T,
	walName message.WALName,
	marshaledID string,
	timeTick uint64,
) *mock_message.MockImmutableMessage {
	t.Helper()
	messageID := mock_message.NewMockMessageID(t)
	messageID.EXPECT().WALName().Return(walName).Maybe()
	messageID.EXPECT().Marshal().Return(marshaledID).Maybe()

	msg := mock_message.NewMockImmutableMessage(t)
	msg.EXPECT().EstimateSize().Return(1).Maybe()
	msg.EXPECT().MessageID().Return(messageID).Maybe()
	msg.EXPECT().TimeTick().Return(timeTick).Maybe()
	return msg
}
