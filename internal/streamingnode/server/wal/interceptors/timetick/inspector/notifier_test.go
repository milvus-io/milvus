package inspector

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

func TestSyncNotifier(t *testing.T) {
	n := newSyncNotifier()
	ch := n.WaitChan()
	assert.True(t, len(n.Get()) == 0)

	shouldBeBlocked(ch)

	n.AddAndNotify(types.PChannelInfo{
		Name: "test",
		Term: 1,
	}, false)
	// should not block
	<-ch
	assert.True(t, len(n.Get()) == 1)
	assert.True(t, len(n.Get()) == 0)

	n.AddAndNotify(types.PChannelInfo{
		Name: "test",
		Term: 1,
	}, false)
	ch = n.WaitChan()
	<-ch

	n.AddAndNotify(types.PChannelInfo{
		Name: "test",
		Term: 1,
	}, true)
	n.AddAndNotify(types.PChannelInfo{
		Name: "test",
		Term: 1,
	}, false)
	ch = n.WaitChan()
	<-ch

	signals := n.Get()
	assert.Equal(t, 1, len(signals))
	for _, v := range signals {
		assert.True(t, v)
	}
}

func shouldBeBlocked(ch <-chan struct{}) {
	select {
	case <-ch:
		panic("should block")
	default:
	}
}
