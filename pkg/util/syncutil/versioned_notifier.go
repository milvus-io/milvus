package syncutil

import (
	"context"
	"sync"
)

const (
	VersionedListenAtEarliest versionedListenAt = -1
	VersionedListenAtLatest   versionedListenAt = -2
)

// versionedListenerAt is the position where the listener starts to listen.
type versionedListenAt int

// NewVersionedNotifier creates a new VersionedNotifier.
func NewVersionedNotifier() *VersionedNotifier {
	return &VersionedNotifier{
		inner: &versionedSignal{
			version: 0,
			cond:    NewContextCond(&sync.Mutex{}),
		},
	}
}

// versionedSignal is a signal with version.
type versionedSignal struct {
	version int
	cond    *ContextCond
}

// VersionedNotifier is a notifier with version.
// A version-based notifier, any change of version could be seen by all listeners without lost.
type VersionedNotifier struct {
	inner *versionedSignal
}

// NotifyAll notifies all listeners.
func (vn *VersionedNotifier) NotifyAll() {
	vn.inner.cond.LockAndBroadcast()
	vn.inner.version++
	vn.inner.cond.L.Unlock()
}

// Listen creates a listener at given position.
func (vn *VersionedNotifier) Listen(at versionedListenAt) *VersionedListener {
	var last int
	if at == VersionedListenAtEarliest {
		last = -1
	} else if at == VersionedListenAtLatest {
		vn.inner.cond.L.Lock()
		last = vn.inner.version
		vn.inner.cond.L.Unlock()
	}
	return &VersionedListener{
		lastNotifiedVersion: last,
		inner:               vn.inner,
	}
}

// VersionedListener is a listener with version.
type VersionedListener struct {
	lastNotifiedVersion int
	inner               *versionedSignal
}

// Wait waits for the next notification.
// If the context is canceled, it returns the error.
// Otherwise it will block until the next notification.
func (vl *VersionedListener) Wait(ctx context.Context) error {
	vl.inner.cond.L.Lock()
	for vl.lastNotifiedVersion >= vl.inner.version {
		if err := vl.inner.cond.Wait(ctx); err != nil {
			return err
		}
	}
	vl.lastNotifiedVersion = vl.inner.version
	vl.inner.cond.L.Unlock()
	return nil
}

// WaitChan returns a channel that will be closed when the next notification comes.
// Use Sync to sync the listener to the latest version to avoid redundant notify.
//
//	ch := vl.WaitChan()
//	<-ch
//	vl.Sync()
//	... make use of the notification ...
func (vl *VersionedListener) WaitChan() <-chan struct{} {
	vl.inner.cond.L.Lock()
	// Return a closed channel if the version is newer than the last notified version.
	if vl.lastNotifiedVersion < vl.inner.version {
		vl.lastNotifiedVersion = vl.inner.version
		vl.inner.cond.L.Unlock()
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	return vl.inner.cond.WaitChan()
}

// Sync syncs the listener to the latest version.
func (vl *VersionedListener) Sync() {
	vl.inner.cond.L.Lock()
	vl.lastNotifiedVersion = vl.inner.version
	vl.inner.cond.L.Unlock()
}
