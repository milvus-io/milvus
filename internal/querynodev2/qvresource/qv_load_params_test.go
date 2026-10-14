package qvresource

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/indexparams"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func diskANNLoadInfo() *querypb.SegmentLoadInfo {
	return &querypb.SegmentLoadInfo{
		CollectionID: 1, SegmentID: 10, PartitionID: 100,
		IndexInfos: []*querypb.FieldIndexInfo{{
			FieldID: 101, IndexID: 1000, NumRows: 5000,
			IndexParams: []*commonpb.KeyValuePair{
				{Key: "index_type", Value: "DISKANN"},
				{Key: "dim", Value: "128"},
			},
		}},
	}
}

func TestQueryViewLoadPreparesPrivateInfoBeforeConstruction(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().QueryNodeCfg.InternalCollectionUseTakeForOutput.Key
	require.NoError(t, paramtable.Get().Save(key, "true"))
	t.Cleanup(func() { paramtable.Get().Reset(key) })

	info := diskANNLoadInfo()
	original := proto.Clone(info)
	loader := &fakeQVLoader{segment: &fakeQVSegment{id: 10, partitionID: 100}}
	var constructed *querypb.SegmentLoadInfo
	patch := mockey.Mock((*fakeQVLoader).NewSegment).To(func(_ *fakeQVLoader, _ context.Context, _ qnview.CollectionRuntime, prepared *querypb.SegmentLoadInfo) (qvLoadedSegment, error) {
		require.NotSame(t, info, prepared)
		constructed = proto.Clone(prepared).(*querypb.SegmentLoadInfo)
		return loader.segment, nil
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	physical := newQueryViewPhysicalSegmentLoader(nil, loader)
	_, err := physical.Load(context.Background(), info, fakeQVCollectionRuntime{})
	require.NoError(t, err)
	require.NotNil(t, constructed)
	require.True(t, constructed.GetUseTakeForOutput())
	params := funcutil.KeyValuePair2Map(constructed.GetIndexInfos()[0].GetIndexParams())
	require.NotEmpty(t, params[indexparams.NumLoadThreadKey])
	require.NotEmpty(t, params[indexparams.SearchCacheBudgetKey])
	require.True(t, proto.Equal(original, info), "watch snapshot must stay immutable")
	require.Len(t, loader.infos, 3)
	for _, loadedInfo := range loader.infos {
		require.True(t, proto.Equal(constructed, loadedInfo), "constructor and physical loads must use the same prepared metadata")
	}
}

func TestQueryViewLoadPreparationFailurePreservesSnapshot(t *testing.T) {
	paramtable.Init()
	info := diskANNLoadInfo()
	info.IndexInfos[0].IndexParams = info.IndexInfos[0].IndexParams[:1] // Missing required dimension.
	original := proto.Clone(info)
	loader := &fakeQVLoader{}
	physical := newQueryViewPhysicalSegmentLoader(nil, loader)
	_, err := physical.Load(context.Background(), info, fakeQVCollectionRuntime{})
	require.Error(t, err)
	require.False(t, loader.newCalled)
	require.True(t, proto.Equal(original, info))
}

func TestQueryViewPhysicalLoadFailureReleasesSegment(t *testing.T) {
	paramtable.Init()
	for _, tc := range []struct {
		name   string
		method any
	}{
		{"data", (*fakeQVLoader).LoadSegment},
		{"delta", (*fakeQVLoader).LoadDeltaLogs},
		{"bloom_filter", (*fakeQVLoader).LoadPKCandidate},
		{"finalize", (*fakeQVLoader).FinalizeLoad},
	} {
		t.Run(tc.name, func(t *testing.T) {
			patch := mockey.Mock(tc.method).Return(assert.AnError).Build()
			t.Cleanup(func() { patch.UnPatch() })
			segment := &fakeQVSegment{id: 10}
			physical := newQueryViewPhysicalSegmentLoader(nil, &fakeQVLoader{segment: segment})
			loaded, err := physical.Load(context.Background(), diskANNLoadInfo(), fakeQVCollectionRuntime{})
			require.ErrorIs(t, err, assert.AnError)
			require.Nil(t, loaded)
			require.Equal(t, 1, segment.releaseCount)
		})
	}
}

func TestQueryViewReadPreparationOwnsMetadataAndAppliesHistoricalDeletes(t *testing.T) {
	mockey.PatchConvey("private preparation uses exact runtime after applying deltalogs", t, func() {
		paramtable.Init()
		info := diskANNLoadInfo()
		original := proto.Clone(info)
		loader := &fakeQVLoader{}
		physical := newQueryViewPhysicalSegmentLoader(nil, loader)
		base := newQueryViewTransformSegment(&qvLocalSegment{segment: &segments.LocalSegment{}}, nil, "v1", 50)
		wrapper := &testTransformSegmentWrapper{TransformSegment: base}
		var calls int
		mockey.Mock((*segments.LocalSegment).PrepareReadState).To(func(_ *segments.LocalSegment, _ context.Context, _ segments.ReadState, prepared *querypb.SegmentLoadInfo, _ *segments.Collection) (segments.ReadState, error) {
			calls++
			require.True(t, loader.deltaCalled)
			require.NotSame(t, info, prepared)
			require.NotEmpty(t, funcutil.KeyValuePair2Map(prepared.IndexInfos[0].IndexParams)[indexparams.NumLoadThreadKey])
			prepared.IndexInfos[0].IndexParams = nil
			return nil, nil
		}).Build()
		_, err := physical.PrepareReadState(t.Context(), wrapper, nil, fakeQVCollectionRuntime{}, qnview.SegmentLoadInfoSnapshot{LoadInfo: info})
		require.NoError(t, err)
		require.Equal(t, 1, calls)
		require.True(t, proto.Equal(original, info))
		failure := mockey.Mock((*fakeQVLoader).LoadDeltaLogs).Return(assert.AnError).Build()
		_, err = physical.PrepareReadState(t.Context(), wrapper, nil, fakeQVCollectionRuntime{}, qnview.SegmentLoadInfoSnapshot{LoadInfo: info})
		require.ErrorIs(t, err, assert.AnError)
		require.Equal(t, 1, calls, "failed deltalog preparation must not publish a read state")
		require.True(t, proto.Equal(original, info))
		failure.UnPatch()
	})
}
