package queryresource

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestDispatcherDynamicConcurrency(t *testing.T) {
	params := paramtable.Get()
	cpuPatch := mockey.Mock(hardware.GetCPUNum).Return(4).Build()
	defer cpuPatch.UnPatch()
	key := params.StreamingCfg.QueryViewLiveEventDispatchConcurrencyPerPChannel.Key
	require.NoError(t, params.Save(key, "0.25"))
	defer params.Reset(key)
	dispatcher := NewConfiguredDispatcher()
	entered, completed, release := make(chan struct{}, 4), make(chan struct{}, 4), make(chan struct{})
	var once sync.Once
	runtimes := make([]*QueryRuntime, 4)
	defer func() {
		once.Do(func() { close(release) })
		for _, runtime := range runtimes {
			if runtime != nil {
				runtime.Close()
			}
		}
		dispatcher.Close()
	}()
	for i := range runtimes {
		runtime := newQueryRuntime(dispatcher)
		runtimes[i] = runtime
		require.NoError(t, runtime.Initialize(t.Context(), walview.VChannelWALView{}))
		require.True(t, runtime.ObserveEvent(t.Context(), walview.VChannelResourceEvent{Barrier: func() { entered <- struct{}{}; <-release }}))
		require.True(t, runtime.ObserveEvent(t.Context(), walview.VChannelResourceEvent{Barrier: func() { completed <- struct{}{} }}))
	}
	wait := func(ch <-chan struct{}) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Fatal("dispatch did not progress")
		}
	}
	wait(entered)
	require.NoError(t, params.Save(key, "0.75"))
	wait(entered)
	wait(entered)
	require.NoError(t, params.Save(key, "0.25"))
	require.Empty(t, completed, "later events must not pass the blocked event")
	once.Do(func() { close(release) })
	for range runtimes {
		wait(completed)
	}
	require.Eventually(t, func() bool { dispatcher.mu.Lock(); defer dispatcher.mu.Unlock(); return dispatcher.workers == 1 }, time.Second, time.Millisecond)
	sibling := NewConfiguredDispatcher()
	defer sibling.Close()
	dispatcher.Close()
	require.NoError(t, params.Save(key, "0.5"))
	require.Eventually(t, func() bool { sibling.mu.Lock(); defer sibling.mu.Unlock(); return sibling.workers == 2 }, time.Second, time.Millisecond)
	dispatcher.mu.Lock()
	require.Zero(t, dispatcher.workers)
	dispatcher.mu.Unlock()
}

func TestDispatcherConfigUpdateConcurrentClose(t *testing.T) {
	params := paramtable.Get()
	key := params.StreamingCfg.QueryViewLiveEventDispatchConcurrencyPerPChannel.Key
	defer params.Reset(key)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			_ = params.Save(key, "0.25")
			_ = params.Save(key, "0.5")
		}
	}()
	for i := 0; i < 20; i++ {
		NewConfiguredDispatcher().Close()
	}
	cancel()
	wg.Wait()
}
