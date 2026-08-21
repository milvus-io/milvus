package broadcaster

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAckCallbackSchedulerCoalescesResourceKeyReleaseNotifications(t *testing.T) {
	s := &ackCallbackScheduler{
		triggerChan: make(chan struct{}, 1),
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 100 {
			s.notifyResourceKeyReleased()
		}
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("resource key release notifications should not block")
	}

	assert.Len(t, s.triggerChan, 1)
}
