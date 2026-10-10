package transformlogbuffer

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestCatchupConcurrencyFromRatio(t *testing.T) {
	for _, tc := range []struct {
		cpu   int
		ratio float64
		want  int
	}{
		{1, 0.25, 1}, {3, 0.25, 1}, {6, 0.25, 1}, {8, 0.25, 2}, {8, 1.5, 12},
	} {
		got, valid := catchupConcurrencyFromRatio(tc.cpu, tc.ratio)
		require.True(t, valid)
		require.Equal(t, tc.want, got)
	}
	for _, ratio := range []float64{0, -1, math.NaN(), math.Inf(1), math.MaxFloat64} {
		_, valid := catchupConcurrencyFromRatio(8, ratio)
		require.False(t, valid)
	}
	_, valid := catchupConcurrencyFromRatio(0, 0.25)
	require.False(t, valid)
}

func catchupConfigForTest(t *testing.T) (*paramtable.ComponentParam, string) {
	t.Helper()
	params := paramtable.Get()
	item := &params.QueryViewCfg.TransformLogCatchupConcurrencyRatio
	key, previous := item.Key, item.GetValue()
	require.Equal(t, "queryView.transformLog.catchupConcurrencyRatio", key)
	require.Equal(t, "0.25", item.DefaultValue)
	require.NoError(t, params.Save(key, "0.25"))
	t.Cleanup(func() { require.NoError(t, params.Save(key, previous)) })
	patch := mockey.Mock(hardware.GetCPUNum).Return(4).Build()
	t.Cleanup(func() { patch.UnPatch() })
	return params, key
}

func TestCatchupConfigWatcherLifetime(t *testing.T) {
	params, key := catchupConfigForTest(t)
	dispatcher := paramtable.GetBaseTable().Manager().Dispatcher
	initial := len(dispatcher.Get(key))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	first := New(ctx, nil)
	otherCtx, otherCancel := context.WithCancel(t.Context())
	defer otherCancel()
	second := New(otherCtx, nil)
	require.Len(t, dispatcher.Get(key), initial+2)
	limit := func(b *Buffer) int {
		b.mu.Lock()
		defer b.mu.Unlock()
		return b.catchupConcurrency
	}
	require.Equal(t, 1, limit(first))
	require.NoError(t, params.Save(key, "0.5"))
	require.Equal(t, 2, limit(first))
	require.Equal(t, 2, limit(second))
	require.NoError(t, params.Save(key, "0"))
	require.Equal(t, 2, limit(first), "invalid live updates keep the previous limit")
	cancel()
	require.Eventually(t, func() bool { return len(dispatcher.Get(key)) == initial+1 }, time.Second, time.Millisecond)
	require.NoError(t, params.Save(key, "0.75"))
	require.Equal(t, 2, limit(first))
	require.Equal(t, 3, limit(second), "canceling one owner must not remove another owner's watcher")
	otherCancel()
	require.Eventually(t, func() bool { return len(dispatcher.Get(key)) == initial }, time.Second, time.Millisecond)
}

func TestCatchupDynamicConcurrencyPerPChannel(t *testing.T) {
	params, key := catchupConfigForTest(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	owner := New(ctx, nil)
	started := make(chan int64, 6)
	done := make(chan error, 6)
	gates := make([]chan struct{}, 6)
	var release [6]sync.Once
	for i := range gates {
		gates[i] = make(chan struct{})
	}
	unblock := func(id int64) { release[id-1].Do(func() { close(gates[id-1]) }) }
	patch := mockey.Mock((*fakeSegment).ApplyTransform).To(func(s *fakeSegment, _ context.Context, _ *streamingpb.TransformLogEntry) error {
		started <- s.id
		<-gates[s.id-1]
		return nil
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	var registrations []*registration
	t.Cleanup(func() {
		for id := int64(1); id <= 6; id++ {
			unblock(id)
		}
		for _, reg := range registrations {
			reg.Unregister()
		}
		waitDrainIdle(t, owner)
	})
	submit := func(id int64, pchannel string) {
		// Segments share their physical channel's quota.
		buf := newVChannelBuffer(owner, pchannel, pchannel+"_1v0", 0)
		buf.syncUp = true
		buf.entries = []*streamingpb.TransformLogEntry{{TimeTick: 10}}
		reg := newRegistration(buf, &fakeSegment{id: id, vchannel: buf.vchannel})
		buf.pending[id] = reg
		registrations = append(registrations, reg)
		reg.Catchup(ctx, func(err error) { done <- err })
	}
	awaitStarted := func(want ...int64) {
		t.Helper()
		var actual []int64
		for range want {
			select {
			case id := <-started:
				actual = append(actual, id)
			case <-time.After(time.Second):
				t.Fatal("queued catch-up did not start")
			}
		}
		require.ElementsMatch(t, want, actual)
	}
	submit(1, "a")
	submit(2, "b")
	awaitStarted(1, 2)
	submit(3, "a")
	submit(4, "b")
	require.Empty(t, started)
	// Growing the quota must wake existing queues without another submission.
	require.NoError(t, params.Save(key, "0.5"))
	awaitStarted(3, 4)
	require.NoError(t, params.Save(key, "0.25"))
	submit(5, "a")
	submit(6, "b")
	require.Empty(t, done, "shrinking must not finish an in-flight native Apply")
	unblock(1)
	unblock(2)
	require.NoError(t, awaitCatchup(t, done))
	require.NoError(t, awaitCatchup(t, done))
	require.Eventually(t, func() bool {
		owner.mu.Lock()
		defer owner.mu.Unlock()
		return owner.drainQueues["a"].workers == 1 && owner.drainQueues["b"].workers == 1
	}, time.Second, time.Millisecond)
	require.Empty(t, started, "excess workers must retire before dequeuing more work")
	unblock(3)
	unblock(4)
	awaitStarted(5, 6)
	unblock(5)
	unblock(6)
	for range 4 {
		require.NoError(t, awaitCatchup(t, done))
	}
	waitDrainIdle(t, owner)
}
