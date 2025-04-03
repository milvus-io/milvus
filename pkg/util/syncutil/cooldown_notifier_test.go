package syncutil

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCooldownNotifier(t *testing.T) {
	n := NewCooldownNotifier[int](10*time.Millisecond, 1)
	n.Notify(1)
	n.Notify(2)
	k := <-n.Chan()
	assert.Equal(t, 1, k)

	select {
	case <-n.Chan():
		t.Errorf("should be blocked")
	default:
	}
	time.Sleep(20 * time.Millisecond)
	n.Notify(3)
	k = <-n.Chan()
	assert.Equal(t, 3, k)
}
