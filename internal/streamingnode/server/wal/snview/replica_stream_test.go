package snview

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestDirectoryStreamDirectSubscription(t *testing.T) {
	d := newReplicaDirectory()
	defer d.Close()
	unavailable := make(chan struct{})
	stream := d.AcquireStream(t.Context(), "p", unavailable)
	defer stream.Close()
	var callbacks atomic.Int64
	sub, err := stream.Subscribe(t.Context(), "p_1v0", func(snapshot qviews.ReplicaSnapshot) {
		callbacks.Add(1)
		if len(snapshot.Replicas) > 0 {
			snapshot.Replicas[0].ReplicaID = -1
		}
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return sub.Snapshot().Valid }, time.Second, time.Millisecond)
	d.Set("p_1v0", 7, &qviews.QueryViewVersion{QueryVersion: 10}, false)
	require.Eventually(t, func() bool {
		s := sub.Snapshot()
		return len(s.Replicas) == 1 && s.Replicas[0].MaxUpViewVersion.QueryVersion == 10
	}, time.Second, time.Millisecond)
	snapshot := sub.Snapshot()
	require.EqualValues(t, 7, snapshot.Replicas[0].ReplicaID)
	snapshot.Replicas[0].ReplicaID = 99
	require.EqualValues(t, 7, sub.Snapshot().Replicas[0].ReplicaID)
	// A second handler gets current state even if no publication follows.
	sibling, err := stream.Subscribe(t.Context(), "p_1v0", func(qviews.ReplicaSnapshot) {})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return sibling.Snapshot().Valid }, time.Second, time.Millisecond)
	require.NoError(t, sub.Close())
	before := callbacks.Load()
	d.Set("p_1v0", 8, &qviews.QueryViewVersion{}, false)
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
	_, err = stream.Subscribe(t.Context(), "p_1v0", func(qviews.ReplicaSnapshot) {})
	require.Error(t, err)
}

func TestDirectoryStreamSubscriptionValidation(t *testing.T) {
	d := newReplicaDirectory()
	defer d.Close()
	stream := d.AcquireStream(t.Context(), "p", nil)
	defer stream.Close()
	for _, vc := range []string{"", "other_1v0"} {
		_, err := stream.Subscribe(t.Context(), vc, func(qviews.ReplicaSnapshot) {})
		require.Error(t, err)
	}
	_, err := stream.Subscribe(t.Context(), "p_1v0", nil)
	require.Error(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = stream.Subscribe(ctx, "p_1v0", func(qviews.ReplicaSnapshot) {})
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, stream.Close())
	require.NoError(t, stream.Close())
}

func TestDirectoryStreamDefersInitialSnapshotDuringPreparation(t *testing.T) {
	d := newReplicaDirectory()
	defer d.Close()
	d.Set("p_1v0", 1, nil, true)
	stream := d.AcquireStream(t.Context(), "p", nil)
	defer stream.Close()
	sub, err := stream.Subscribe(t.Context(), "p_1v0", func(qviews.ReplicaSnapshot) {})
	require.NoError(t, err)
	// Another VChannel on the same physical stream must initialize immediately.
	other, err := stream.Subscribe(t.Context(), "p_2v0", func(qviews.ReplicaSnapshot) {})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return other.Snapshot().Valid }, time.Second, time.Millisecond)
	require.False(t, sub.Snapshot().Valid)
	version := qviews.QueryViewVersion{QueryVersion: 10}
	d.Set("p_1v0", 1, &version, false)
	require.Eventually(t, func() bool { return len(sub.Snapshot().Replicas) == 1 }, time.Second, time.Millisecond)
	require.Equal(t, version, sub.Snapshot().Replicas[0].MaxUpViewVersion)

	// A later replacement preparing does not hide retirement from existing readers.
	d.Set("p_1v0", 1, nil, true)
	require.Eventually(t, func() bool { s := sub.Snapshot(); return s.Valid && len(s.Replicas) == 0 }, time.Second, time.Millisecond)
	// A new physical registration sharing that VChannel must still wait for its
	// own initialization, even though its sibling already consumed an empty list.
	next, err := stream.Subscribe(t.Context(), "p_1v0", func(qviews.ReplicaSnapshot) {})
	require.NoError(t, err)
	require.False(t, next.Snapshot().Valid)
	d.Set("p_1v0", 1, nil, false) // preparation failed or was released
	require.Eventually(t, func() bool { return next.Snapshot().Valid }, time.Second, time.Millisecond)
	require.Empty(t, next.Snapshot().Replicas)
}

func TestDirectorySubscriptionInitialGateAndRetirement(t *testing.T) {
	// Exercise the delivery decision synchronously, independent of event-loop timing.
	var results []qviews.ReplicaSnapshot
	sub := &directorySubscription{handler: func(s qviews.ReplicaSnapshot) { results = append(results, s) }}
	empty := &replicaDirectorySnapshot{VChannelReplicas: &viewpb.VChannelReplicas{Vchannel: "p_1v0"}, preparing: true}
	sub.deliver(empty)
	require.Empty(t, results)
	require.False(t, sub.snapshot.Valid)
	up := &replicaDirectorySnapshot{VChannelReplicas: &viewpb.VChannelReplicas{Replicas: []*viewpb.VChannelReplicas_Replica{{ReplicaId: 1, MaxUpViewVersion: (qviews.QueryViewVersion{}).IntoProto()}}}, preparing: true}
	sub.deliver(up)
	require.Len(t, results, 1, "an Up replica wins over other preparing versions")
	sub.deliver(empty)
	require.Len(t, results, 2, "retirement must reach previously initialized subscriptions")
	require.True(t, results[1].Valid)
	require.Empty(t, results[1].Replicas)
}
