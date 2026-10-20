package transformlogbuffer

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestDispatchDynamicConcurrency(t *testing.T) {
	params := paramtable.Get()
	cpuPatch := mockey.Mock(hardware.GetCPUNum).Return(4).Build()
	defer cpuPatch.UnPatch()
	key := params.QueryNodeCfg.QueryViewTransformLogDrainConcurrency.Key
	require.NoError(t, params.Save(key, "0.25"))
	defer params.Reset(key)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	streams := newFakeStreamManager()
	buffer := NewConfigured(ctx, streams)
	guard, err := buffer.Acquire(ctx, newTestQueryView("v", 0))
	require.NoError(t, err)
	defer guard.Release()
	stream := streams.stream("v")
	for tick := uint64(1); tick <= 600; tick++ {
		stream.emit(wal.TransformLogStreamEvent{VChannel: "v", Entry: &streamingpb.TransformLogEntry{TimeTick: tick}})
	}
	stream.emit(wal.TransformLogStreamEvent{VChannel: "v", SyncUp: &wal.TransformLogSyncUp{TimeTick: 600}})
	entered, release := make(chan struct{}, 4), make(chan struct{})
	var once sync.Once
	patch := mockey.Mock((*fakeSegment).ApplyTransform).When(func(_ *fakeSegment, ctx context.Context, entry *streamingpb.TransformLogEntry) bool {
		if entry.GetTimeTick() == 1 {
			entered <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
		return false
	}).Build()
	defer patch.UnPatch()
	segments := make([]*fakeSegment, 4)
	regs := make([]qnview.TransformRegistration, 4)
	defer func() {
		once.Do(func() { close(release) })
		for _, reg := range regs {
			if reg != nil {
				reg.Unregister()
			}
		}
		cancel()
		buffer.closeDrain()
	}()
	for i := range segments {
		segments[i] = &fakeSegment{id: int64(i), vchannel: "v"}
		regs[i], err = buffer.RegisterSegment(ctx, segments[i])
		require.NoError(t, err)
	}
	wait := func() {
		t.Helper()
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("catchup did not start")
		}
	}
	wait()
	require.NoError(t, params.Save(key, "0.75"))
	wait()
	wait()
	require.NoError(t, params.Save(key, "0.25"))
	once.Do(func() { close(release) })
	for _, reg := range regs {
		require.NoError(t, reg.WaitCatchup(ctx))
	}
	stream.emit(wal.TransformLogStreamEvent{VChannel: "v", Entry: &streamingpb.TransformLogEntry{TimeTick: 601}})
	want := make([]uint64, 601)
	for i := range want {
		want[i] = uint64(i + 1)
	}
	for i, reg := range regs {
		require.NoError(t, reg.WaitTransformVisible(ctx, 601))
		require.Equal(t, want, segments[i].appliedTicks())
	}
	require.Eventually(t, func() bool { buffer.drainMu.Lock(); defer buffer.drainMu.Unlock(); return buffer.drainWorkers == 1 }, time.Second, time.Millisecond)
	cancel()
	require.Eventually(t, func() bool {
		buffer.drainMu.Lock()
		defer buffer.drainMu.Unlock()
		return buffer.drainClosed && buffer.drainWorkers == 0
	}, time.Second, time.Millisecond)
	require.NoError(t, params.Save(key, "0.5"))
	buffer.drainMu.Lock()
	require.Zero(t, buffer.drainWorkers)
	buffer.drainMu.Unlock()
}
