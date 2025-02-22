package utility

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/mocks/streaming/util/mock_message"
)

func TestReOrderByTimeTickBuffer(t *testing.T) {
	buf := NewReOrderBuffer()
	timeticks := rand.Perm(25)
	for i, timetick := range timeticks {
		msg := mock_message.NewMockImmutableMessage(t)
		msg.EXPECT().EstimateSize().Return(1)
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
