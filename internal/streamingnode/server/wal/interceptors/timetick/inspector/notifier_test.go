package inspector

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/walimplstest"
)

func TestSyncNotifier(t *testing.T) {
	n := newSyncNotifier()
	ch := n.WaitChan()
	assert.True(t, n.Get().Len() == 0)

	shouldBeBlocked(ch)

	n.AddAndNotify(types.PChannelInfo{
		Name: "test",
		Term: 1,
	})
	// should not block
	<-ch
	assert.True(t, n.Get().Len() == 1)
	assert.True(t, n.Get().Len() == 0)

	n.AddAndNotify(types.PChannelInfo{
		Name: "test",
		Term: 1,
	})
	ch = n.WaitChan()
	<-ch
}

func shouldBeBlocked(ch <-chan struct{}) {
	select {
	case <-ch:
		panic("should block")
	default:
	}
}

func TestTimeTickNotifier(t *testing.T) {
	n := NewTimeTickNotifier()
	info := n.Get()
	assert.True(t, info.IsZero())
	msgID := walimplstest.NewTestMessageID(1)
	assert.Nil(t, n.WatchAtMessageID(msgID, 0))
	n.Update(TimeTickInfo{
		MessageID:              msgID,
		TimeTick:               2,
		LastConfirmedMessageID: walimplstest.NewTestMessageID(0),
	})

	ch := n.WatchAtMessageID(msgID, 0)
	assert.NotNil(t, ch)
	<-ch // should not block.

	ch = n.WatchAtMessageID(msgID, 2)
	assert.NotNil(t, ch)
	shouldBeBlocked(ch) // should block.

	n.OnlyUpdateTs(3)
	<-ch // should not block.
	info = n.Get()
	assert.Equal(t, uint64(3), info.TimeTick)

	ch = n.WatchAtMessageID(msgID, 3)
	n.Update(TimeTickInfo{
		MessageID: walimplstest.NewTestMessageID(3),
		TimeTick:  4,
	})
	shouldBeBlocked(ch)
}
