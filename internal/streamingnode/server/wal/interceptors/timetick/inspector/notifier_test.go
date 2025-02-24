package inspector

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
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
