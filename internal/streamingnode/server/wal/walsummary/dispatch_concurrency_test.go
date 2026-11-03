package walsummary

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestDispatchStreamConcurrentCatchup(t *testing.T) {
	for _, concurrency := range []int{1, 2, 4} {
		t.Run(fmt.Sprint(concurrency), func(t *testing.T) {
			params := paramtable.Get()
			cpuPatch := mockey.Mock(hardware.GetCPUNum).Return(concurrency * 4).Build()
			defer cpuPatch.UnPatch()
			key := params.StreamingCfg.TransformLogCatchupConcurrencyPerPChannel.Key
			require.NoError(t, params.Save(key, "0.25"))
			defer params.Reset(key)

			manager := NewManager(ManagerConfig{})
			count := concurrency + 2
			for i := 0; i < count; i++ {
				vc := fmt.Sprint("v", i)
				manager.ObserveMessage(t.Context(), newTestDeleteMessage(t, vc, uint64(10*i+10), 1, 1))
				manager.ObserveMessage(t.Context(), newTestDeleteMessage(t, vc, uint64(10*i+11), 1, 2))
			}
			// Capture real one-entry pages before installing the blocking read mock.
			// Mockey Origin uses shared call state and cannot be called concurrently.
			pages := make(map[string][]TransformBatch)
			for i := 0; i < count; i++ {
				vc := fmt.Sprint("v", i)
				through := uint64(10*count + 1)
				first, err := manager.ReadTransform(t.Context(), vc, 0, through, ReadLimits{MaxRows: 1})
				require.NoError(t, err)
				second, err := manager.ReadTransform(t.Context(), vc, first.CoveredThrough, through, ReadLimits{MaxRows: 1})
				require.NoError(t, err)
				pages[vc] = []TransformBatch{first, second}
			}
			entered := make(chan struct{}, count*4)
			release := make(chan struct{})
			var mu sync.Mutex
			active, peak := 0, 0
			perChannel := make(map[string]int)
			overlap := false
			patch := mockey.Mock((*Manager).ReadTransform).To(func(_ *Manager, ctx context.Context, vc string, after, _ uint64, _ ReadLimits) (TransformBatch, error) {
				mu.Lock()
				active++
				peak = max(peak, active)
				perChannel[vc]++
				overlap = overlap || perChannel[vc] > 1
				mu.Unlock()
				defer func() {
					mu.Lock()
					active--
					perChannel[vc]--
					mu.Unlock()
				}()
				entered <- struct{}{}
				select {
				case <-release:
				case <-ctx.Done():
					return TransformBatch{}, ctx.Err()
				}
				if after == 0 {
					return pages[vc][0], nil
				}
				return pages[vc][1], nil
			}).Build()
			defer patch.UnPatch()
			stream := NewDispatchStream(manager)
			defer stream.Close()
			handlers := make([]*recordingTransformHandler, count)
			for i := range handlers {
				handlers[i] = newRecordingTransformHandler()
				_, err := stream.Subscribe(t.Context(), wal.TransformLogSubscriptionOption{VChannel: fmt.Sprint("v", i), Handler: handlers[i]})
				require.NoError(t, err)
			}
			for i := 0; i < concurrency; i++ {
				select {
				case <-entered:
				case <-time.After(5 * time.Second):
					t.Fatal("configured catchup workers did not start concurrently")
				}
			}
			stream.mu.Lock()
			queued := len(stream.catchups)
			stream.mu.Unlock()
			require.Equal(t, count-concurrency, queued, "remaining subscriptions must wait in the shared queue")
			for i := range handlers {
				manager.ObserveMessage(t.Context(), newTestDeleteMessage(t, fmt.Sprint("v", i), uint64(1000+i), 1, 3))
			}
			close(release)
			for i, handler := range handlers {
				for _, tick := range []uint64{uint64(10*i + 10), uint64(10*i + 11), uint64(1000 + i)} {
					require.Equal(t, tick, nextTransformEvent(t, handler).Entry.GetTimeTick())
				}
				require.Equal(t, uint64(1000+i), nextTransformEvent(t, handler).SyncUp.TimeTick)
			}
			for i, handler := range handlers {
				manager.ObserveMessage(t.Context(), newTestDeleteMessage(t, fmt.Sprint("v", i), uint64(2000+i), 1, 4))
				require.Equal(t, uint64(2000+i), nextTransformEvent(t, handler).Entry.GetTimeTick())
				require.Empty(t, handler.events)
			}
			mu.Lock()
			gotPeak, gotOverlap := peak, overlap
			mu.Unlock()
			require.Equal(t, concurrency, gotPeak)
			require.False(t, gotOverlap, "pages of one subscription must never execute concurrently")
		})
	}
}

func TestDispatchStreamConcurrentCatchupClose(t *testing.T) {
	params := paramtable.Get()
	cpuPatch := mockey.Mock(hardware.GetCPUNum).Return(8).Build()
	defer cpuPatch.UnPatch()
	key := params.StreamingCfg.TransformLogCatchupConcurrencyPerPChannel.Key
	require.NoError(t, params.Save(key, "0.25"))
	defer params.Reset(key)
	manager := NewManager(ManagerConfig{})
	manager.ObserveMessage(t.Context(), newTestDeleteMessage(t, "v", 10, 1, 1))
	entered := make(chan struct{}, 3)
	patch := mockey.Mock((*Manager).ReadTransform).To(func(_ *Manager, ctx context.Context, _ string, _, _ uint64, _ ReadLimits) (TransformBatch, error) {
		entered <- struct{}{}
		<-ctx.Done()
		return TransformBatch{}, ctx.Err()
	}).Build()
	defer patch.UnPatch()
	stream := NewDispatchStream(manager)
	defer stream.Close()
	for i := 0; i < 3; i++ {
		_, err := stream.Subscribe(t.Context(), wal.TransformLogSubscriptionOption{VChannel: fmt.Sprint("v", i), Handler: newRecordingTransformHandler()})
		require.NoError(t, err)
	}
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("catchup workers did not enter reads")
		}
	}
	closed := make(chan error, 1)
	go func() { closed <- stream.Close() }()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("close did not join catchup workers")
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	require.Empty(t, manager.transformSubscribers)
}

func TestDispatchStreamDynamicCatchupConcurrency(t *testing.T) {
	params := paramtable.Get()
	cpuPatch := mockey.Mock(hardware.GetCPUNum).Return(4).Build()
	defer cpuPatch.UnPatch()
	key := params.StreamingCfg.TransformLogCatchupConcurrencyPerPChannel.Key
	require.NoError(t, params.Save(key, "0.25"))
	defer params.Reset(key)
	manager := NewManager(ManagerConfig{})
	manager.ObserveMessage(t.Context(), newTestDeleteMessage(t, "other", 10, 1, 1))
	entered, release := make(chan struct{}, 4), make(chan struct{})
	var releaseOnce sync.Once
	// When falls through to the real reader without Mockey Origin's shared state.
	patch := mockey.Mock((*Manager).ReadTransform).When(func(_ *Manager, ctx context.Context, _ string, _, _ uint64, _ ReadLimits) bool {
		entered <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
		}
		return false
	}).Build()
	defer patch.UnPatch()
	stream := NewDispatchStream(manager)
	defer stream.Close()
	defer releaseOnce.Do(func() { close(release) })
	handlers := make([]*recordingTransformHandler, 4)
	for i := range handlers {
		handlers[i] = newRecordingTransformHandler()
		_, err := stream.Subscribe(t.Context(), wal.TransformLogSubscriptionOption{VChannel: fmt.Sprint("v", i), Handler: handlers[i]})
		require.NoError(t, err)
	}
	waitRead := func() {
		t.Helper()
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("catchup did not start")
		}
	}
	waitRead()
	require.NoError(t, params.Save(key, "0.75"))
	waitRead()
	waitRead()
	// A resize callback must not wait for blocked readers to finish.
	require.NoError(t, params.Save(key, "0.25"))
	for i := range handlers {
		manager.ObserveMessage(t.Context(), newTestDeleteMessage(t, fmt.Sprint("v", i), uint64(100+i), 1, 2))
	}
	releaseOnce.Do(func() { close(release) })
	for i, handler := range handlers {
		require.Equal(t, uint64(100+i), nextTransformEvent(t, handler).Entry.GetTimeTick())
		require.Equal(t, uint64(100+i), nextTransformEvent(t, handler).SyncUp.TimeTick)
	}
	require.Eventually(t, func() bool { stream.mu.Lock(); defer stream.mu.Unlock(); return stream.catchupWorkers == 1 }, time.Second, time.Millisecond)
	sibling := NewDispatchStream(manager)
	defer sibling.Close()
	require.NoError(t, stream.Close())
	require.NoError(t, params.Save(key, "0.5"))
	require.Eventually(t, func() bool { sibling.mu.Lock(); defer sibling.mu.Unlock(); return sibling.catchupWorkers == 2 }, time.Second, time.Millisecond)
	stream.mu.Lock()
	require.Zero(t, stream.catchupWorkers, "config updates must not revive a closed stream")
	stream.mu.Unlock()
}
