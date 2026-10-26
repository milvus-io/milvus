package replicadiscovery

import (
	"context"
	"runtime"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestManagerSharesPChannelWithoutVChannelGoroutines(t *testing.T) {
	directory := NewDirectory()
	defer directory.Close()
	var attempts, ready atomic.Int64
	baseline := runtime.NumGoroutine()
	manager := NewManager(func(ctx context.Context, pc string) (Stream, error) {
		attempts.Add(1)
		return NewLocalStream(ctx, directory.Watch(), nil), nil
	})
	defer manager.Close()
	const channels = 2000
	subscriptions := make([]*Subscription, 0, channels)
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
	require.Eventually(t, func() bool {
		directory.mu.Lock()
		defer directory.mu.Unlock()
		return len(directory.listeners) == 0
	}, time.Second, time.Millisecond)
	_, err := manager.Subscribe("", func(Snapshot) {})
	require.Error(t, err)
	_, err = manager.Subscribe("p_1v0", nil)
	require.Error(t, err)
	manager.Close()
	_, err = manager.Subscribe("p_1v0", func(Snapshot) {})
	require.Error(t, err)
}

func TestManagerCallbacksMayCloseAndAreJoined(t *testing.T) {
	d := NewDirectory()
	defer d.Close()
	manager := NewManager(func(ctx context.Context, _ string) (Stream, error) { return NewLocalStream(ctx, d.Watch(), nil), nil })
	defer manager.Close()
	sub, err := manager.Subscribe("p_1v0", func(Snapshot) {})
	require.NoError(t, err)
	defer sub.Close()
	waitReplicaSnapshot(t, sub, nil)
	same, err := manager.Subscribe("p_1v0", func(Snapshot) {})
	require.NoError(t, err)
	require.Same(t, sub.state, same.state)
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
	w := sub.watcher
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
	other := NewManager(func(ctx context.Context, _ string) (Stream, error) { return NewLocalStream(ctx, d.Watch(), nil), nil })
	defer other.Close()
	assigned := make(chan struct{})
	finished := make(chan struct{}, 1)
	var self *Subscription
	self, err = other.Subscribe("p_3v0", func(Snapshot) { <-assigned; self.Close(); finished <- struct{}{} })
	require.NoError(t, err)
	close(assigned)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("callback self-close deadlocked")
	}
}

func TestLocalStreamOrderingAndOwnerLoss(t *testing.T) {
	d := NewDirectory()
	defer d.Close()
	unavailable := make(chan struct{})
	stream := NewLocalStream(t.Context(), d.Watch(), unavailable)
	defer stream.Close()
	require.NoError(t, stream.Send(&viewpb.WatchVChannelReplicasRequest{Vchannel: "p_1v0"}))
	snapshot, err := stream.Recv()
	require.NoError(t, err)
	require.Empty(t, replicasFromProto(snapshot.GetSnapshot().GetReplicas()))
	d.Set("p_1v0", 1, &qviews.QueryViewVersion{})
	require.NoError(t, stream.Send(&viewpb.WatchVChannelReplicasRequest{Vchannel: "p_1v0", Unsubscribe: true}))
	ack, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, "p_1v0", ack.GetUnsubscribedVchannel())
	require.NoError(t, stream.Send(&viewpb.WatchVChannelReplicasRequest{Vchannel: "p_1v0"}))
	snapshot, err = stream.Recv()
	require.NoError(t, err)
	require.Equal(t, testReplicas(1), replicasFromProto(snapshot.GetSnapshot().GetReplicas()))
	close(unavailable)
	_, err = stream.Recv()
	require.Error(t, err)
	require.Error(t, stream.Send(&viewpb.WatchVChannelReplicasRequest{Vchannel: "p_1v0"}))
}

func TestReplicaVersionNotificationLocalAndRemote(t *testing.T) {
	for _, remote := range []bool{false, true} {
		name := "local"
		if remote {
			name = "remote"
		}
		t.Run(name, func(t *testing.T) {
			endpoint, client := newDirectoryTransport(t)
			d := endpoint.directory
			var factory StreamFactory
			if remote {
				factory = func(ctx context.Context) (Stream, error) {
					stream, err := client.WatchVChannelReplicas(ctx)
					if err != nil {
						return nil, err
					}
					_, err = stream.Recv()
					if err != nil {
						return nil, err
					}
					return NewRemoteStream(stream, func() {}), nil
				}
			} else {
				factory = func(ctx context.Context) (Stream, error) { return NewLocalStream(ctx, d.Watch(), nil), nil }
			}
			w := newWatcher(t.Context(), "p", factory)
			defer w.Close()
			events := make(chan Snapshot, 16)
			sub, err := w.subscribe("p_1v0", func(s Snapshot) {
				events <- Snapshot{Valid: s.Valid, Replicas: slices.Clone(s.Replicas)}
				// The callback owns its snapshot: mutating it must not corrupt cache.
				if len(s.Replicas) > 0 {
					s.Replicas[0].MaxUpViewVersion.QueryVersion = -1
				}
			})
			require.NoError(t, err)
			defer sub.Close()
			waitReplicaSnapshot(t, sub, nil)
			for _, queryVersion := range []int64{10, 11} {
				version := qviews.QueryViewVersion{DataVersion: qviews.DataVersion{StreamingVersion: 1}, QueryVersion: queryVersion}
				d.Set("p_1v0", 7, &version)
				require.Eventually(t, func() bool {
					select {
					case event := <-events:
						return event.Valid && len(event.Replicas) == 1 && event.Replicas[0].ReplicaID == 7 && event.Replicas[0].MaxUpViewVersion == version
					default:
						return false
					}
				}, 5*time.Second, time.Millisecond)
				snapshot := sub.Snapshot()
				require.Equal(t, version, snapshot.Replicas[0].MaxUpViewVersion)
				snapshot.Replicas[0].MaxUpViewVersion.QueryVersion = -2
				require.Equal(t, version, sub.Snapshot().Replicas[0].MaxUpViewVersion)
			}
		})
	}
}
