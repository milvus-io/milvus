package idf

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

func TestSealedLoadsShareBoundAndAggregate(t *testing.T) {
	limiter := syncutil.NewSemaphore(2)
	started := make(chan struct{}, 8)
	release := make(chan struct{})
	var active, peak atomic.Int32
	patch := mockey.Mock(loadSealedSegmentStats).To(func(ctx context.Context, _ storage.ChunkManager, _ *datapb.StreamingNodeBM25Resource, _ ...bm25Stats) (bm25Stats, error) {
		n := active.Add(1)
		defer active.Add(-1)
		for {
			prev := peak.Load()
			if prev >= n || peak.CompareAndSwap(prev, n) {
				break
			}
		}
		started <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		stats := storage.NewBM25Stats()
		stats.Append(map[uint32]float32{7: 2})
		return bm25Stats{102: stats}, nil
	}).Build()
	defer patch.UnPatch()
	resources := map[int64]*datapb.StreamingNodeBM25Resource{}
	for id := int64(1); id <= 4; id++ {
		resources[id] = &datapb.StreamingNodeBM25Resource{SegmentId: id}
	}
	type result struct {
		stats bm25Stats
		err   error
	}
	results := make(chan result, 2)
	for range 2 {
		p := NewProvider(nil)
		p.sealedStatsLoadLimiter = limiter
		go func() {
			stats, err := p.loadSealedContributions(context.Background(), resources, nil)
			results <- result{stats, err}
		}()
	}
	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("parallel loads did not start")
		}
	}
	require.Equal(t, 2, limiter.Current())
	close(release)
	for range 2 {
		res := <-results
		require.NoError(t, res.err)
		require.Equal(t, int64(4), res.stats[102].NumRow())
		require.Equal(t, float64(2), res.stats[102].GetAvgdl())
	}
	require.Equal(t, int32(2), peak.Load())
	require.Zero(t, limiter.Current())
}

func TestSealedLoadFailureCancelsSiblings(t *testing.T) {
	limiter := syncutil.NewSemaphore(2)
	var calls atomic.Int32
	bothStarted := make(chan struct{})
	var once sync.Once
	failure := merr.WrapErrDataIntegrityMsg("bad stats")
	patch := mockey.Mock(loadSealedSegmentStats).To(func(ctx context.Context, _ storage.ChunkManager, _ *datapb.StreamingNodeBM25Resource, _ ...bm25Stats) (bm25Stats, error) {
		if calls.Add(1) == 1 {
			<-bothStarted
			return nil, failure
		}
		once.Do(func() { close(bothStarted) })
		<-ctx.Done()
		return nil, ctx.Err()
	}).Build()
	defer patch.UnPatch()
	p := NewProvider(nil)
	p.sealedStatsLoadLimiter = limiter
	resources := map[int64]*datapb.StreamingNodeBM25Resource{1: {SegmentId: 1}, 2: {SegmentId: 2}, 3: {SegmentId: 3}}
	stats, err := p.loadSealedContributions(context.Background(), resources, nil)
	require.ErrorIs(t, err, failure)
	require.Nil(t, stats)
	require.Zero(t, limiter.Current())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = p.loadSealedContributions(ctx, resources, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, limiter.Current())
}

func TestOracleRejectsAmbiguousSealedDescriptors(t *testing.T) {
	r, resource := newTestOracle(t)
	for _, resources := range [][]*datapb.StreamingNodeBM25Resource{{nil}, {resource(20, 2), resource(20, 4)}} {
		_, err := r.indexResources(resources)
		require.ErrorIs(t, err, merr.ErrDataIntegrity)
	}
}
