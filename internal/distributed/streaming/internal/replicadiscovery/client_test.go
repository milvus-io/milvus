package replicadiscovery

import (
	"context"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

func TestClientSharesPChannelWithoutVChannelGoroutines(t *testing.T) {
	directory := NewDirectory()
	defer directory.Close()
	var attempts, ready atomic.Int64
	baseline := runtime.NumGoroutine()
	manager := NewResumingClient(func(ctx context.Context, pc string) (Stream, error) {
		attempts.Add(1)
		return directory.AcquireStream(ctx, "p", nil), nil
	})
	defer manager.Close()
	const channels = 2000
	subscriptions := make([]wal.ReplicaWatchSubscription, 0, channels)
	for i := 0; i < channels; i++ {
		vc := "p_" + strconv.Itoa(i) + "v0"
		sub, err := manager.Subscribe(vc, func(snapshot Snapshot) {
			if snapshot.Valid {
				ready.Add(1)
			}
		})
		require.NoError(t, err)
		subscriptions = append(subscriptions, sub)
	}
	require.Eventually(t, func() bool { return ready.Load() == channels }, 10*time.Second, time.Millisecond)
	require.EqualValues(t, 1, attempts.Load())
	require.Len(t, manager.watchers, 1)
	require.LessOrEqual(t, runtime.NumGoroutine()-baseline, 6, "2000 channels must share control/receive/dispatch goroutines")
	t.Logf("%d VChannels: goroutine delta=%d, physical attempts=%d", channels, runtime.NumGoroutine()-baseline, attempts.Load())
	for _, sub := range subscriptions {
		sub.Close()
	}

	_, err := manager.Subscribe("", func(Snapshot) {})
	require.Error(t, err)
	_, err = manager.Subscribe("p_1v0", nil)
	require.Error(t, err)
	manager.Close()
	_, err = manager.Subscribe("p_1v0", func(Snapshot) {})
	require.Error(t, err)
}

func TestClientCallbacksMayCloseAndAreJoined(t *testing.T) {
	d := NewDirectory()
	defer d.Close()
	manager := NewResumingClient(func(ctx context.Context, _ string) (Stream, error) { return d.AcquireStream(ctx, "p", nil), nil })
	defer manager.Close()
	sub, err := manager.Subscribe("p_1v0", func(Snapshot) {})
	require.NoError(t, err)
	defer sub.Close()
	waitReplicaSnapshot(t, sub, nil)
	same, err := manager.Subscribe("p_1v0", func(Snapshot) {})
	require.NoError(t, err)
	require.Same(t, sub.(*Subscription).state, same.(*Subscription).state)
	same.Close()
	d.Set("p_1v0", 3, &qviews.QueryViewVersion{})
	waitReplicaSnapshot(t, sub, []int64{3})
	// One blocked callback must not block state updates or create more workers.
	entered, release := make(chan struct{}), make(chan struct{})
	var blocked atomic.Bool
	sibling, err := manager.Subscribe("p_2v0", func(s Snapshot) {
		if blocked.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
	})
	require.NoError(t, err)
	<-entered
	for i := int64(0); i < 100; i++ {
		d.Set("p_1v0", 4, testUpVersion(i%2 == 0))
	}
	w := sub.(*Subscription).watcher
	w.mu.Lock()
	pending := len(w.pending)
	w.mu.Unlock()
	require.LessOrEqual(t, pending, 2)
	closed := make(chan struct{})
	go func() { manager.Close(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("Close must join an in-flight callback")
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not finish")
	}
	sibling.Close()
	require.False(t, sub.Snapshot().Valid)

	// Closing one's own subscription is nonblocking and releases local listeners.
	other := NewResumingClient(func(ctx context.Context, _ string) (Stream, error) { return d.AcquireStream(ctx, "p", nil), nil })
	defer other.Close()
	assigned := make(chan struct{})
	finished := make(chan struct{}, 1)
	var self wal.ReplicaWatchSubscription
	self, err = other.Subscribe("p_3v0", func(Snapshot) { <-assigned; self.Close(); finished <- struct{}{} })
	require.NoError(t, err)
	close(assigned)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("callback self-close deadlocked")
	}
}
