package qvresource

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
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
	physical := newQueryViewPhysicalSegmentLoader(nil, nil, loader)
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

func TestQueryViewUpdatePreparesPrivateInfo(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().QueryNodeCfg.ExternalCollectionUseTakeForOutput.Key
	require.NoError(t, paramtable.Get().Save(key, "true"))
	t.Cleanup(func() { paramtable.Get().Reset(key) })
	for _, action := range []qnview.SegmentUpdateAction{qnview.SegmentUpdateReopen, qnview.SegmentUpdateLoadIndex, qnview.SegmentUpdateReopen | qnview.SegmentUpdateLoadIndex} {
		t.Run(actionName(action), func(t *testing.T) {
			info := diskANNLoadInfo()
			original := proto.Clone(info)
			loader := &fakeQVLoader{}
			patch := mockey.Mock((*fakeQVLoader).ReopenSegment).To(func(_ *fakeQVLoader, _ context.Context, _ qvLoadedSegment, prepared *querypb.SegmentLoadInfo) error {
				require.NotSame(t, info, prepared)
				require.True(t, prepared.GetUseTakeForOutput())
				require.NotEmpty(t, funcutil.KeyValuePair2Map(prepared.IndexInfos[0].IndexParams)[indexparams.NumLoadThreadKey])
				// Reopen currently modifies its argument. This must not reach the watch snapshot.
				prepared.IndexInfos[0].IndexParams = append(prepared.IndexInfos[0].IndexParams, &commonpb.KeyValuePair{Key: "local", Value: "value"})
				return nil
			}).Build()
			t.Cleanup(func() { patch.UnPatch() })
			physical := newQueryViewPhysicalSegmentLoader(nil, nil, loader)
			segment := newQueryViewTransformSegment(&fakeQVSegment{id: 10}, nil, "v1", 50)
			err := physical.Update(context.Background(), segment, fakeQVCollectionRuntime{schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{{FieldID: 101, ExternalField: "vector"}},
			}}, qnview.SegmentLoadInfoSnapshot{LoadInfo: info}, action)
			require.NoError(t, err)
			require.True(t, proto.Equal(original, info))
			if action.Has(qnview.SegmentUpdateLoadIndex) {
				require.Len(t, loader.infos, 1)
				require.True(t, loader.infos[0].GetUseTakeForOutput())
				require.NotEmpty(t, funcutil.KeyValuePair2Map(loader.infos[0].IndexInfos[0].IndexParams)[indexparams.NumLoadThreadKey])
			}
		})
	}
}

func actionName(action qnview.SegmentUpdateAction) string {
	if action == qnview.SegmentUpdateReopen {
		return "reopen"
	}
	if action == qnview.SegmentUpdateLoadIndex {
		return "index"
	}
	return "reopen_and_index"
}

func TestQueryViewLoadPreparationFailurePreservesSnapshot(t *testing.T) {
	paramtable.Init()
	info := diskANNLoadInfo()
	info.IndexInfos[0].IndexParams = info.IndexInfos[0].IndexParams[:1] // Missing required dimension.
	original := proto.Clone(info)
	loader := &fakeQVLoader{}
	physical := newQueryViewPhysicalSegmentLoader(nil, nil, loader)
	_, err := physical.Load(context.Background(), info, fakeQVCollectionRuntime{})
	require.Error(t, err)
	require.False(t, loader.newCalled)
	require.True(t, proto.Equal(original, info))
	segment := newQueryViewTransformSegment(&fakeQVSegment{id: 10}, nil, "v1", 50)
	err = physical.Update(context.Background(), segment, fakeQVCollectionRuntime{}, qnview.SegmentLoadInfoSnapshot{LoadInfo: info}, qnview.SegmentUpdateReopen)
	require.Error(t, err)
	require.False(t, loader.reopenCalled)
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
	} {
		t.Run(tc.name, func(t *testing.T) {
			patch := mockey.Mock(tc.method).Return(assert.AnError).Build()
			t.Cleanup(func() { patch.UnPatch() })
			segment := &fakeQVSegment{id: 10}
			physical := newQueryViewPhysicalSegmentLoader(nil, nil, &fakeQVLoader{segment: segment})
			loaded, err := physical.Load(context.Background(), diskANNLoadInfo(), fakeQVCollectionRuntime{})
			require.ErrorIs(t, err, assert.AnError)
			require.Nil(t, loaded)
			require.Equal(t, 1, segment.releaseCount)
		})
	}
}

func TestQueryViewPhysicalUpdateFailurePreservesSnapshot(t *testing.T) {
	paramtable.Init()
	for _, action := range []qnview.SegmentUpdateAction{qnview.SegmentUpdateReopen, qnview.SegmentUpdateLoadIndex} {
		t.Run(actionName(action), func(t *testing.T) {
			loader := &fakeQVLoader{err: assert.AnError}
			physical := newQueryViewPhysicalSegmentLoader(nil, nil, loader)
			info := diskANNLoadInfo()
			original := proto.Clone(info)
			segment := newQueryViewTransformSegment(&fakeQVSegment{id: 10}, nil, "v1", 50)
			err := physical.Update(context.Background(), segment, fakeQVCollectionRuntime{}, qnview.SegmentLoadInfoSnapshot{LoadInfo: info}, action)
			require.ErrorIs(t, err, assert.AnError)
			require.True(t, proto.Equal(original, info))
		})
	}
}
