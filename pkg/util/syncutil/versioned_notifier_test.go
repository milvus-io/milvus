package syncutil

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLatestVersionedNotifier(t *testing.T) {
	vn := NewVersionedNotifier()

	// Create a listener at the latest version
	listener := vn.Listen(VersionedListenAtLatest)

	// Start a goroutine to wait for the notification
	done := make(chan struct{})
	go func() {
		err := listener.Wait(context.Background())
		if err != nil {
			t.Errorf("Wait returned an error: %v", err)
		}
		close(done)
	}()

	// Should be blocked.
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	select {
	case <-done:
		t.Errorf("Wait returned before NotifyAll")
	case <-ctx.Done():
	}

	// Notify all listeners
	vn.NotifyAll()

	// Wait for the goroutine to finish
	<-done
}

func TestEarliestVersionedNotifier(t *testing.T) {
	vn := NewVersionedNotifier()

	// Create a listener at the latest version
	listener := vn.Listen(VersionedListenAtEarliest)

	// Should be non-blocked.
	err := listener.Wait(context.Background())
	assert.NoError(t, err)

	// Start a goroutine to wait for the notification
	done := make(chan struct{})
	go func() {
		err := listener.Wait(context.Background())
		if err != nil {
			t.Errorf("Wait returned an error: %v", err)
		}
		close(done)
	}()

	// Should be blocked.
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	select {
	case <-done:
		t.Errorf("Wait returned before NotifyAll")
	case <-ctx.Done():
	}
}

func TestTimeoutListeningVersionedNotifier(t *testing.T) {
	vn := NewVersionedNotifier()

	listener := vn.Listen(VersionedListenAtLatest)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	err := listener.Wait(ctx)
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}
