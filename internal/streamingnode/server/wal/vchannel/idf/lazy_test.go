package idf

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func newLazyOracle(t *testing.T) *oracleRuntime {
	t.Helper()
	base, _ := newTestOracle(t)
	r, err := newOracleRuntime(context.Background(), base.provider, walview.VChannelWALView{
		CollectionID: 1, VChannel: "v1", Schema: base.schema, SegmentSnapshot: walview.VisibleSegmentSnapshot{DataVersion: qviews.DataVersion{StreamingVersion: 10}},
	}, nil, true)
	require.NoError(t, err)
	t.Cleanup(r.Close)
	return r
}

func TestLazyMaterializationOwnsCancellation(t *testing.T) {
	for _, cancelInitiator := range []bool{false, true} {
		t.Run(map[bool]string{true: "initiator", false: "waiter"}[cancelInitiator], func(t *testing.T) {
			started := make(chan struct{})
			release := make(chan struct{})
			var calls atomic.Int32
			patch := mockey.Mock((*Provider).getSealedBM25Resources).To(func(_ *Provider, ctx context.Context, _ int64, _ string, _ qviews.DataVersion, _ []int64, _ uint64) ([]*datapb.StreamingNodeBM25Resource, error) {
				calls.Add(1)
				close(started)
				select {
				case <-release:
					return nil, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}).Build()
			t.Cleanup(func() { patch.UnPatch() })
			r := newLazyOracle(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			initial := context.Background()
			other := ctx
			if cancelInitiator {
				initial, other = ctx, context.Background()
			}
			first, second := make(chan error, 1), make(chan error, 1)
			go func() { _, _, err := r.BuildIDF(initial, qviews.DataVersion{}, 102, nil); first <- err }()
			<-started
			go func() { _, _, err := r.BuildIDF(other, qviews.DataVersion{}, 102, nil); second <- err }()
			cancel()
			canceled, survivor := second, first
			if cancelInitiator {
				canceled, survivor = first, second
			}
			select {
			case err := <-canceled:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(time.Second):
				t.Fatal("query cancellation did not return")
			}
			close(release)
			require.NoError(t, <-survivor)
			require.Equal(t, int32(1), calls.Load())
		})
	}
}

func TestLazyMaterializationSwitchesTargetAndIncludesLiveRows(t *testing.T) {
	started := make(chan struct{})
	var calls atomic.Int32
	patch := mockey.Mock((*Provider).getSealedBM25Resources).To(func(_ *Provider, ctx context.Context, _ int64, _ string, v qviews.DataVersion, _ []int64, _ uint64) ([]*datapb.StreamingNodeBM25Resource, error) {
		calls.Add(1)
		if v.StreamingVersion == 10 {
			close(started)
			<-ctx.Done()
			return nil, ctx.Err()
		}
		require.Equal(t, int64(11), v.StreamingVersion)
		return nil, nil
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	r := newLazyOracle(t)
	result := make(chan error, 1)
	go func() { _, _, err := r.BuildIDF(context.Background(), qviews.DataVersion{}, 102, nil); result <- err }()
	<-started
	r.ApplyLiveEvent(context.Background(), walview.VChannelResourceEvent{Message: bm25Insert(t, 20, 4)})
	require.NoError(t, r.PrepareDataVersion(context.Background(), qviews.DataVersion{StreamingVersion: 11}))
	require.NoError(t, <-result)
	require.Equal(t, int32(2), calls.Load())
	require.Equal(t, float64(4), r.currentStats[102].GetAvgdl())
}

func TestLazyMaterializationFailureRetriesAndCloseCancels(t *testing.T) {
	var calls atomic.Int32
	started := make(chan struct{})
	patch := mockey.Mock((*Provider).getSealedBM25Resources).To(func(_ *Provider, ctx context.Context, _ int64, _ string, _ qviews.DataVersion, _ []int64, _ uint64) ([]*datapb.StreamingNodeBM25Resource, error) {
		if calls.Add(1) == 1 {
			return nil, context.DeadlineExceeded
		}
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	r := newLazyOracle(t)
	_, _, err := r.BuildIDF(context.Background(), qviews.DataVersion{}, 102, nil)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, r.currentStats)
	result := make(chan error, 1)
	go func() { _, _, err := r.BuildIDF(context.Background(), qviews.DataVersion{}, 102, nil); result <- err }()
	<-started
	r.Close()
	require.ErrorIs(t, <-result, context.Canceled)
	require.Equal(t, int32(2), calls.Load())
}

func TestLazyInitialPreparationDefersIOAndLaterPreparationIsSynchronous(t *testing.T) {
	var calls atomic.Int32
	patch := mockey.Mock((*Provider).getSealedBM25Resources).To(func(_ *Provider, _ context.Context, _ int64, _ string, _ qviews.DataVersion, _ []int64, _ uint64) ([]*datapb.StreamingNodeBM25Resource, error) {
		calls.Add(1)
		return nil, nil
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	r := newLazyOracle(t)
	require.NoError(t, r.PrepareDataVersion(context.Background(), qviews.DataVersion{StreamingVersion: 11}))
	require.Zero(t, calls.Load())
	_, _, err := r.BuildIDF(context.Background(), qviews.DataVersion{}, 102, nil)
	require.NoError(t, err)
	require.Equal(t, int32(1), calls.Load())
	require.NoError(t, r.PrepareDataVersion(context.Background(), qviews.DataVersion{StreamingVersion: 12}))
	require.Equal(t, int32(2), calls.Load())
	require.Equal(t, int64(12), r.currentVersion.StreamingVersion)
}
