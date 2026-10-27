package replicadiscovery

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
)

func TestDirectoryStreamDirectSubscription(t *testing.T) {
	d := NewDirectory()
	defer d.Close()
	unavailable := make(chan struct{})
	stream := d.AcquireStream(t.Context(), "p", unavailable)
	defer stream.Close()
	var callbacks atomic.Int64
	sub, err := stream.Subscribe(t.Context(), "p_1v0", func(snapshot Snapshot) {
		callbacks.Add(1)
		if len(snapshot.Replicas) > 0 {
			snapshot.Replicas[0].ReplicaID = -1
		}
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return sub.Snapshot().Valid }, time.Second, time.Millisecond)
	d.Set("p_1v0", 7, &qviews.QueryViewVersion{QueryVersion: 10})
	require.Eventually(t, func() bool {
		s := sub.Snapshot()
		return len(s.Replicas) == 1 && s.Replicas[0].MaxUpViewVersion.QueryVersion == 10
	}, time.Second, time.Millisecond)
	snapshot := sub.Snapshot()
	require.EqualValues(t, 7, snapshot.Replicas[0].ReplicaID)
	snapshot.Replicas[0].ReplicaID = 99
	require.EqualValues(t, 7, sub.Snapshot().Replicas[0].ReplicaID)
	// A second handler gets current state even if no publication follows.
	sibling, err := stream.Subscribe(t.Context(), "p_1v0", func(Snapshot) {})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return sibling.Snapshot().Valid }, time.Second, time.Millisecond)
	require.NoError(t, sub.Close())
	before := callbacks.Load()
	d.Set("p_1v0", 8, &qviews.QueryViewVersion{})
	require.Eventually(t, func() bool { return len(sibling.Snapshot().Replicas) == 2 }, time.Second, time.Millisecond)
	require.Equal(t, before, callbacks.Load())
	require.False(t, sub.Snapshot().Valid)
	require.NoError(t, sibling.Close())
	d.mu.Lock()
	require.Empty(t, d.listeners)
	d.mu.Unlock()
	// WAL unavailability is independent of directory shutdown.
	close(unavailable)
	select {
	case <-stream.Done():
	case <-time.After(time.Second):
		t.Fatal("owner loss did not end physical stream")
	}
	require.Error(t, stream.Error())
	_, err = stream.Subscribe(t.Context(), "p_1v0", func(Snapshot) {})
	require.Error(t, err)
}

func TestDirectoryStreamSubscriptionValidation(t *testing.T) {
	d := NewDirectory()
	defer d.Close()
	stream := d.AcquireStream(t.Context(), "p", nil)
	defer stream.Close()
	for _, vc := range []string{"", "other_1v0"} {
		_, err := stream.Subscribe(t.Context(), vc, func(Snapshot) {})
		require.Error(t, err)
	}
	_, err := stream.Subscribe(t.Context(), "p_1v0", nil)
	require.Error(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = stream.Subscribe(ctx, "p_1v0", func(Snapshot) {})
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, stream.Close())
	require.NoError(t, stream.Close())
}
