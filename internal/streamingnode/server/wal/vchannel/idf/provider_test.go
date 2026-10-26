package idf

import (
	"context"
	"os"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

func TestRuntimeInitializationFailures(t *testing.T) {
	schema := &schemapb.CollectionSchema{Functions: []*schemapb.FunctionSchema{{Type: schemapb.FunctionType_BM25, OutputFieldIds: []int64{102}}}}
	view := walview.VChannelWALView{Schema: schema}
	for _, tc := range []struct {
		name    string
		runtime *Runtime
	}{
		{"missing provider", &Runtime{}},
		{"missing future", &Runtime{future: NewFutureProvider(nil)}},
		{"missing client", &Runtime{provider: NewProvider(nil)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Error(t, tc.runtime.Prepare(context.Background(), view))
			_, _, err := tc.runtime.BuildIDF(context.Background(), qviews.DataVersion{}, 102, nil)
			require.Error(t, err)
		})
	}
	pending := NewFutureProvider(syncutil.NewFuture[types.MixCoordClient]())
	module, err := pending.NewRuntime()
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, module.Prepare(ctx, view), context.Canceled)
	require.False(t, hasLoadedBM25Function(schema, []int64{101}))
	schema.Functions = append([]*schemapb.FunctionSchema{{Type: schemapb.FunctionType_Unknown}}, schema.Functions...)
	require.True(t, hasLoadedBM25Function(schema, nil))
}

func TestResourceResponseMustMatchRequestedView(t *testing.T) {
	version := qviews.DataVersion{StreamingVersion: 10}
	response := &datapb.GetStreamingNodeQueryViewResourcesResponse{CollectionId: 1, Vchannel: "v1", DataVersion: version.IntoProto()}
	require.NoError(t, validateResourceResponseFor(1, "v1", version, response))
	for _, tc := range []struct {
		name   string
		mutate func(*datapb.GetStreamingNodeQueryViewResourcesResponse)
	}{
		{"collection", func(r *datapb.GetStreamingNodeQueryViewResourcesResponse) { r.CollectionId = 2 }},
		{"channel", func(r *datapb.GetStreamingNodeQueryViewResourcesResponse) { r.Vchannel = "v2" }},
		{"missing version", func(r *datapb.GetStreamingNodeQueryViewResourcesResponse) { r.DataVersion = nil }},
		{"wrong version", func(r *datapb.GetStreamingNodeQueryViewResourcesResponse) { r.DataVersion.StreamingVersion++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := proto.Clone(response).(*datapb.GetStreamingNodeQueryViewResourcesResponse)
			tc.mutate(changed)
			require.Error(t, validateResourceResponseFor(1, "v1", version, changed))
		})
	}
}

func TestRuntimeCloseDuringResourceFetch(t *testing.T) {
	client := &mocks.MockMixCoordClient{}
	schema := &schemapb.CollectionSchema{Functions: []*schemapb.FunctionSchema{{Type: schemapb.FunctionType_BM25, OutputFieldIds: []int64{102}}}}
	view := walview.VChannelWALView{CollectionID: 1, VChannel: "v1", Schema: schema}
	runtime := &Runtime{provider: NewProvider(client)}
	patch := mockey.Mock((*mocks.MockMixCoordClient).GetStreamingNodeQueryViewResources).To(func(_ *mocks.MockMixCoordClient, _ context.Context, req *datapb.GetStreamingNodeQueryViewResourcesRequest, _ ...grpc.CallOption) (*datapb.GetStreamingNodeQueryViewResourcesResponse, error) {
		runtime.Close()
		return &datapb.GetStreamingNodeQueryViewResourcesResponse{Status: merr.Success(), CollectionId: req.CollectionId, Vchannel: req.Vchannel, DataVersion: req.DataVersion}, nil
	}).Build()
	defer patch.UnPatch()
	require.ErrorIs(t, runtime.Prepare(context.Background(), view), context.Canceled)
	require.Nil(t, runtime.currentOracle())
}

func TestRuntimeResourceFetchFailureIsNotReady(t *testing.T) {
	client := &mocks.MockMixCoordClient{}
	schema := &schemapb.CollectionSchema{Functions: []*schemapb.FunctionSchema{{Type: schemapb.FunctionType_BM25, OutputFieldIds: []int64{102}}}}
	view := walview.VChannelWALView{CollectionID: 1, VChannel: "v1", Schema: schema}
	for _, tc := range []struct {
		name     string
		response *datapb.GetStreamingNodeQueryViewResourcesResponse
		err      error
	}{
		{name: "RPC canceled", err: context.Canceled},
		{name: "wrong data version", response: &datapb.GetStreamingNodeQueryViewResourcesResponse{Status: merr.Success(), CollectionId: 1, Vchannel: "v1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			patch := mockey.Mock((*mocks.MockMixCoordClient).GetStreamingNodeQueryViewResources).Return(tc.response, tc.err).Build()
			defer patch.UnPatch()
			runtime := &Runtime{provider: NewProvider(client)}
			require.Error(t, runtime.Prepare(context.Background(), view))
			require.Nil(t, runtime.currentOracle())
		})
	}
}

// Refresh uses the QueryView's exact version even if Coord has newer data.
func TestOracleRefreshRejectsUnrequestedCoordinatorVersion(t *testing.T) {
	r, _ := newTestOracle(t)
	r.provider.client = &mocks.MockMixCoordClient{}
	target := qviews.DataVersion{StreamingVersion: 11}
	responseVersion := qviews.DataVersion{StreamingVersion: 12}
	patch := mockey.Mock((*mocks.MockMixCoordClient).GetStreamingNodeQueryViewResources).To(func(_ *mocks.MockMixCoordClient, _ context.Context, req *datapb.GetStreamingNodeQueryViewResourcesRequest, _ ...grpc.CallOption) (*datapb.GetStreamingNodeQueryViewResourcesResponse, error) {
		require.Equal(t, target, qviews.FromProtoDataVersion(req.GetDataVersion()))
		return &datapb.GetStreamingNodeQueryViewResourcesResponse{
			Status: merr.Success(), CollectionId: req.GetCollectionId(), Vchannel: req.GetVchannel(), DataVersion: responseVersion.IntoProto(),
		}, nil
	}).Build()
	defer patch.UnPatch()
	require.Error(t, r.PrepareDataVersion(context.Background(), target))
	require.Equal(t, qviews.DataVersion{StreamingVersion: 10}, r.currentVersion)
	responseVersion = target
	require.NoError(t, r.PrepareDataVersion(context.Background(), target))
	require.Equal(t, target, r.currentVersion)
}

func TestProviderCreatesNoopRuntimeWhenBM25IsNotLoaded(t *testing.T) {
	provider := NewProvider(nil)
	runtime, err := provider.NewRuntime()
	require.NoError(t, err)
	require.NotNil(t, runtime)
	require.NoError(t, runtime.Prepare(context.Background(), walview.VChannelWALView{
		CollectionID: 1,
		VChannel:     "ch",
		SegmentSnapshot: walview.VisibleSegmentSnapshot{
			DataVersion: qviews.DataVersion{StreamingVersion: 10},
		},
	}))
	versioned := runtime.(interface {
		PrepareDataVersion(context.Context, qviews.DataVersion) error
	})
	require.NoError(t, versioned.PrepareDataVersion(context.Background(), qviews.DataVersion{StreamingVersion: 11}))
	runtime.Close()
}

func TestSealedStatsLoadConcurrency(t *testing.T) {
	paramtable.Init()
	require.Equal(t, 64, sealedStatsLoadConcurrency(16, 4))
	require.Equal(t, 8, sealedStatsLoadConcurrency(16, 0.5))
	require.Equal(t, 1, sealedStatsLoadConcurrency(1, 0.5))
	require.Equal(t, 1, sealedStatsLoadConcurrency(0, 4))
	require.Equal(t, 1, sealedStatsLoadConcurrency(16, 0))
}

func TestProvidersShareSealedStatsLoadLimiter(t *testing.T) {
	paramtable.Init()
	provider := NewProvider(nil)
	futureProvider := NewFutureProvider(nil)
	require.Same(t, provider.sealedStatsLoadLimiter, futureProvider.sealedStatsLoadLimiter)
}

func TestSealedStatsLoadLimiterHotReload(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	key := params.QueryViewCfg.IDFSealedStatsLoadConcurrencyRatio.Key
	limiter := NewProvider(nil).sealedStatsLoadLimiter
	t.Cleanup(func() {
		for limiter.Current() > 0 {
			limiter.Release()
		}
		require.NoError(t, params.Reset(key))
	})

	cpu := hardware.GetCPUNum()
	require.NoError(t, params.Save(key, "1"))
	require.Equal(t, cpu, limiter.Cap())
	for range cpu {
		require.NoError(t, limiter.Acquire(context.Background()))
	}
	require.False(t, limiter.TryAcquire())

	require.NoError(t, params.Save(key, "2"))
	require.Equal(t, cpu*2, limiter.Cap())
	require.True(t, limiter.TryAcquire())

	require.NoError(t, params.Save(key, "1"))
	require.Equal(t, cpu, limiter.Cap())
	require.Equal(t, cpu+1, limiter.Current())
	require.False(t, limiter.TryAcquire())
	limiter.Release()
	require.False(t, limiter.TryAcquire())
	limiter.Release()
	require.True(t, limiter.TryAcquire())
	require.Same(t, limiter, NewFutureProvider(nil).sealedStatsLoadLimiter)
}

func TestMain(m *testing.M) { paramtable.Init(); os.Exit(m.Run()) }

func TestRuntimePrepareHonorsLazyConfiguration(t *testing.T) {
	for _, lazy := range []bool{false, true} {
		t.Run(strconv.FormatBool(lazy), func(t *testing.T) {
			params := paramtable.Get()
			key := params.QueryViewCfg.IDFLazyLoadSealedStats.Key
			require.NoError(t, params.Save(key, strconv.FormatBool(lazy)))
			defer params.Reset(key)
			var calls atomic.Int32
			patch := mockey.Mock((*mocks.MockMixCoordClient).GetStreamingNodeQueryViewResources).To(func(_ *mocks.MockMixCoordClient, _ context.Context, req *datapb.GetStreamingNodeQueryViewResourcesRequest, _ ...grpc.CallOption) (*datapb.GetStreamingNodeQueryViewResourcesResponse, error) {
				calls.Add(1)
				return &datapb.GetStreamingNodeQueryViewResourcesResponse{Status: merr.Success(), CollectionId: req.CollectionId, Vchannel: req.Vchannel, DataVersion: proto.Clone(req.DataVersion).(*viewpb.DataVersion)}, nil
			}).Build()
			defer patch.UnPatch()
			runtime := &Runtime{provider: NewProvider(&mocks.MockMixCoordClient{})}
			defer runtime.Close()
			require.NoError(t, runtime.Prepare(context.Background(), walview.VChannelWALView{CollectionID: 1, VChannel: "v1", Schema: &schemapb.CollectionSchema{Functions: []*schemapb.FunctionSchema{{Type: schemapb.FunctionType_BM25, OutputFieldIds: []int64{102}}}}}))
			if lazy {
				require.Zero(t, calls.Load())
			} else {
				require.Equal(t, int32(1), calls.Load())
			}
			_, _, err := runtime.BuildIDF(context.Background(), qviews.DataVersion{}, 102, nil)
			require.NoError(t, err)
			require.Equal(t, int32(1), calls.Load())
		})
	}
}
