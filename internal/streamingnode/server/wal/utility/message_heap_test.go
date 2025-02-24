package utility

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestImmutableMessageHeap(t *testing.T) {
	h := typeutil.NewHeap[message.ImmutableMessage](&immutableMessageHeap{})
	timeticks := rand.Perm(25)
	for _, timetick := range timeticks {
		msg := mock_message.NewMockImmutableMessage(t)
		msg.EXPECT().TimeTick().Return(uint64(timetick + 1))
		h.Push(msg)
	}

	lastOneTimeTick := uint64(0)
	for h.Len() != 0 {
		msg := h.Pop()
		assert.Greater(t, msg.TimeTick(), lastOneTimeTick)
		lastOneTimeTick = msg.TimeTick()
	}
}
