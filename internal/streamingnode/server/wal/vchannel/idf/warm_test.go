package idf

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/queryresource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// warmed reports that the aggregate is materialized and the warm has ended.
func warmed(r *oracleRuntime) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.currentStats != nil && !r.warming.Load()
}

func mockSealedResources(t *testing.T, fn func(context.Context, qviews.DataVersion) ([]*datapb.StreamingNodeBM25Resource, error)) {
	t.Helper()
	patch := mockey.Mock((*Provider).getSealedBM25Resources).To(func(_ *Provider, ctx context.Context, _ int64, _ string, version qviews.DataVersion, _ []int64, _ uint64) ([]*datapb.StreamingNodeBM25Resource, error) {
		return fn(ctx, version)
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
}

func TestWarmMaterializesTheLazyAggregateOnce(t *testing.T) {
	var calls atomic.Int32
	mockSealedResources(t, func(context.Context, qviews.DataVersion) ([]*datapb.StreamingNodeBM25Resource, error) {
		calls.Add(1)
		return nil, nil
	})
	r := newLazyOracle(t)
	r.warm()
	require.Eventually(t, func() bool { return warmed(r) }, 5*time.Second, time.Millisecond)

	// A materialized aggregate needs no further warm, and queries reuse it.
	r.warm()
	require.False(t, r.warming.Load())
	_, _, err := r.BuildIDF(context.Background(), qviews.DataVersion{}, 102, nil)
	require.NoError(t, err)
	require.Equal(t, int32(1), calls.Load())
}

func TestWarmRetriesFailures(t *testing.T) {
	var calls atomic.Int32
	mockSealedResources(t, func(context.Context, qviews.DataVersion) ([]*datapb.StreamingNodeBM25Resource, error) {
		if calls.Add(1) == 1 {
			return nil, context.DeadlineExceeded
		}
		return nil, nil
	})
	r := newLazyOracle(t)
	r.warm()
	require.Eventually(t, func() bool { return warmed(r) }, 10*time.Second, 10*time.Millisecond)
	require.Equal(t, int32(2), calls.Load())
}

func TestWarmRetriesANewerTargetAfterBackoff(t *testing.T) {
	var calls atomic.Int32
	mockSealedResources(t, func(ctx context.Context, version qviews.DataVersion) ([]*datapb.StreamingNodeBM25Resource, error) {
		calls.Add(1)
		if version.StreamingVersion == 10 {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return nil, nil
	})
	r := newLazyOracle(t)
	r.warm()
	require.Eventually(t, func() bool { return calls.Load() == 1 }, 5*time.Second, time.Millisecond)
	// A newer view cancels the attempt; the warm paces its retry with backoff.
	require.NoError(t, r.PrepareDataVersion(context.Background(), qviews.DataVersion{StreamingVersion: 11}))
	require.Never(t, func() bool { return calls.Load() > 1 }, 100*time.Millisecond, 10*time.Millisecond)
	require.Eventually(t, func() bool { return warmed(r) }, 10*time.Second, 10*time.Millisecond)
	require.Equal(t, int32(2), calls.Load())
}

func TestWarmStopsWhenTheOracleCloses(t *testing.T) {
	var calls atomic.Int32
	mockSealedResources(t, func(ctx context.Context, _ qviews.DataVersion) ([]*datapb.StreamingNodeBM25Resource, error) {
		calls.Add(1)
		<-ctx.Done()
		return nil, ctx.Err()
	})
	r := newLazyOracle(t)
	r.warm()
	require.Eventually(t, func() bool { return calls.Load() == 1 }, 5*time.Second, time.Millisecond)
	r.Close()
	require.Eventually(t, func() bool { return !r.warming.Load() }, 5*time.Second, time.Millisecond)
}

func TestWarmSkipsEagerAndClosedOracles(t *testing.T) {
	var calls atomic.Int32
	mockSealedResources(t, func(context.Context, qviews.DataVersion) ([]*datapb.StreamingNodeBM25Resource, error) {
		calls.Add(1)
		return nil, nil
	})
	eager, _ := newTestOracle(t)
	closed := newLazyOracle(t)
	closed.Close()
	for _, r := range []*oracleRuntime{eager, closed} {
		r.warm()
		require.False(t, r.warming.Load())
	}
	require.Zero(t, calls.Load())
}

func TestWarmsRunOnBoundedWorkers(t *testing.T) {
	patch := mockey.Mock(hardware.GetCPUNum).Return(1).Build()
	t.Cleanup(func() { patch.UnPatch() })
	release := make(chan struct{})
	var calls atomic.Int32
	mockSealedResources(t, func(ctx context.Context, _ qviews.DataVersion) ([]*datapb.StreamingNodeBM25Resource, error) {
		calls.Add(1)
		select {
		case <-release:
			return nil, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	first, second := newLazyOracle(t), newLazyOracle(t)
	first.warm()
	second.warm()
	require.Eventually(t, func() bool { return calls.Load() == 1 }, 5*time.Second, time.Millisecond)
	require.Never(t, func() bool { return calls.Load() > 1 }, 100*time.Millisecond, 10*time.Millisecond)
	close(release)
	require.Eventually(t, func() bool { return warmed(first) && warmed(second) }, 5*time.Second, time.Millisecond)
}

func TestViewBindingWarmsTheOraclesOfItsFields(t *testing.T) {
	params := paramtable.Get()
	key := params.QueryViewCfg.IDFLazyLoadSealedStats.Key
	require.NoError(t, params.Save(key, "true"))
	defer params.Reset(key)
	var calls atomic.Int32
	mockSealedResources(t, func(context.Context, qviews.DataVersion) ([]*datapb.StreamingNodeBM25Resource, error) {
		calls.Add(1)
		return nil, nil
	})
	runtime := &Runtime{provider: NewProvider(&mocks.MockMixCoordClient{})}
	defer runtime.Close()
	binding, err := prepareTestView(t, runtime, walview.VChannelWALView{CollectionID: 1, VChannel: "v1", Schema: fieldTestSchema(1, 102, 104)})
	require.NoError(t, err)
	require.Len(t, binding.fields, 2)
	for _, field := range binding.fields {
		require.Nil(t, field.oracle.currentStats, "a lazy oracle reads nothing before the warm")
	}
	require.Zero(t, calls.Load())

	var resource queryresource.QueryViewResource = binding
	resource.(queryresource.QueryViewResourceWarmer).Warm()
	require.Eventually(t, func() bool { return warmed(binding.fields[0].oracle) && warmed(binding.fields[1].oracle) }, 5*time.Second, time.Millisecond)
	require.Equal(t, int32(2), calls.Load())
}
