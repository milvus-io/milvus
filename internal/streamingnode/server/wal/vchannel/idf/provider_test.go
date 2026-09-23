package idf

import (
	"context"
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
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

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
			_, _, err := tc.runtime.BuildIDF(qviews.DataVersion{}, 102, nil)
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
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	runtime := &Runtime{provider: NewProvider(client, WithNodeScheduler(scheduler))}
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
			oracle := newScheduledOracleRuntime(nil, qviews.DataVersion{})
			oracle.provider = runtime.provider
			require.Error(t, oracle.PrepareDataVersion(context.Background(), qviews.DataVersion{StreamingVersion: 1}))
			_, err := oracle.computeDiff(context.Background(), qviews.DataVersion{StreamingVersion: 1})
			require.Error(t, err)
		})
	}
}
