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
	useWaitChanListener := vn.Listen(VersionedListenAtLatest)

	// Start a goroutine to wait for the notification
	done := make(chan struct{})
	go func() {
		err := listener.Wait(context.Background())
		if err != nil {
			t.Errorf("Wait returned an error: %v", err)
		}
		close(done)
	}()

	done2 := make(chan struct{})
	go func() {
		ch := useWaitChanListener.WaitChan()
		<-ch
		close(done2)
	}()

	// Should be blocked.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
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
	<-done2
}

func TestEarliestVersionedNotifier(t *testing.T) {
	vn := NewVersionedNotifier()

	// Create a listener at the latest version
	listener := vn.Listen(VersionedListenAtEarliest)
	useWaitChanListener := vn.Listen(VersionedListenAtLatest)

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

	done2 := make(chan struct{})
	go func() {
		ch := useWaitChanListener.WaitChan()
		<-ch
		close(done2)
	}()

	// Should be blocked.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	select {
	case <-done:
		t.Errorf("Wait returned before NotifyAll")
	case <-done2:
		t.Errorf("WaitChan returned before NotifyAll")
	case <-ctx.Done():
	}

	// Notify all listeners
	vn.NotifyAll()

	// Wait for the goroutine to finish
	<-done
	<-done2

	// should not be blocked
	useWaitChanListener = vn.Listen(VersionedListenAtEarliest)
	<-useWaitChanListener.WaitChan()

	// should blocked
	useWaitChanListener = vn.Listen(VersionedListenAtEarliest)
	useWaitChanListener.Sync()
	select {
	case <-time.After(10 * time.Millisecond):
	case <-useWaitChanListener.WaitChan():
		t.Errorf("WaitChan returned before NotifyAll")
	}
}

func TestTimeoutListeningVersionedNotifier(t *testing.T) {
	vn := NewVersionedNotifier()

	listener := vn.Listen(VersionedListenAtLatest)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := listener.Wait(ctx)
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}
