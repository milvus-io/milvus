package qvresource

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/util/segcore"
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

// Capture at the native boundary: preparing only the adapter's copy would not
// protect segcore from a missing DISKANN num_load_thread.
func TestQueryViewNativeLoadParameters(t *testing.T) {
	for _, external := range []bool{false, true} {
		name := "internal"
		if external {
			name = "external"
		}
		t.Run(name, func(t *testing.T) {
			patchNativeCollections(t)
			meta := collectionMetadata(1)
			cfg := paramtable.Get()
			policy := &cfg.QueryNodeCfg.InternalCollectionUseTakeForOutput
			if external {
				meta.collection.Schema.Fields[0].ExternalField = "pk"
				policy = &cfg.QueryNodeCfg.ExternalCollectionUseTakeForOutput
			}
			previous := policy.GetValue()
			require.NoError(t, cfg.Save(policy.Key, "true"))
			t.Cleanup(func() { require.NoError(t, cfg.Save(policy.Key, previous)) })
			guard := acquireRuntime(t, newQueryViewCollectionRuntimeManager(meta), 1)
			t.Cleanup(guard.Release)
			native := &lifetimeNativeSegment{}
			patchCollectionLifetime(t, mockey.Mock((*lifetimeNativeSegment).Release).Return().Build())
			var created *querypb.SegmentLoadInfo
			patchCollectionLifetime(t, mockey.Mock(segcore.CreateCSegment).To(func(req *segcore.CreateCSegmentRequest) (segcore.CSegment, error) {
				created = proto.Clone(req.LoadInfo).(*querypb.SegmentLoadInfo)
				return native, nil
			}).Build())
			var reopened *querypb.SegmentLoadInfo
			reopen := mockey.Mock((*lifetimeNativeSegment).Reopen).To(func(_ *lifetimeNativeSegment, _ context.Context, req *segcore.ReopenRequest) error {
				reopened = proto.Clone(req.LoadInfo).(*querypb.SegmentLoadInfo)
				return nil
			}).Build()
			patchCollectionLifetime(t, reopen)
			info := diskANNLoadInfo()
			original := proto.Clone(info)
			loader := realQVSegmentLoader{}
			loaded, err := loader.NewSegment(context.Background(), guard, info)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, loaded.Release(context.Background())) })
			check := func(prepared *querypb.SegmentLoadInfo) {
				t.Helper()
				require.NotNil(t, prepared)
				require.True(t, prepared.GetUseTakeForOutput())
				params := funcutil.KeyValuePair2Map(prepared.GetIndexInfos()[0].GetIndexParams())
				require.NotEmpty(t, params[indexparams.NumLoadThreadKey])
				require.NotEmpty(t, params[indexparams.SearchCacheBudgetKey])
				require.True(t, proto.Equal(original, info), "shared snapshot must remain immutable")
			}
			check(created)
			physical := newQueryViewPhysicalSegmentLoader(loader)
			segment := newQueryViewTransformSegment(loaded, "v1", 0)
			for _, action := range []qnview.SegmentUpdateAction{qnview.SegmentUpdateReopen, qnview.SegmentUpdateLoadIndex, qnview.SegmentUpdateReopen | qnview.SegmentUpdateLoadIndex} {
				before := reopen.Times()
				require.NoError(t, physical.Update(context.Background(), segment, guard, qnview.SegmentLoadInfoSnapshot{LoadInfo: info}, action))
				require.Equal(t, before+1, reopen.Times(), "combined actions must invoke native Reopen only once")
				check(reopened)
			}
		})
	}
}

func TestQueryViewInvalidLoadParametersPreserveSnapshot(t *testing.T) {
	_, guard := pinnedCollectionForTest(t)
	info := diskANNLoadInfo()
	info.IndexInfos[0].IndexParams = info.IndexInfos[0].IndexParams[:1] // Missing dim.
	original := proto.Clone(info)
	create := mockey.Mock(segcore.CreateCSegment).Return(&lifetimeNativeSegment{}, nil).Build()
	patchCollectionLifetime(t, create)
	reopen := mockey.Mock((*lifetimeNativeSegment).Reopen).Return(nil).Build()
	patchCollectionLifetime(t, reopen)
	loader := realQVSegmentLoader{}
	refs := guard.runtime.refs
	_, err := loader.NewSegment(context.Background(), guard, info)
	require.Error(t, err)
	require.Zero(t, create.Times())
	require.Equal(t, refs, guard.runtime.refs)
	require.True(t, proto.Equal(original, info))
	local := &qvLocalSegment{collection: guard, segment: &lifetimeNativeSegment{}, info: &querypb.SegmentLoadInfo{SegmentID: 10}}
	previous := local.info
	err = loader.ReopenSegment(context.Background(), local, guard, info)
	require.Error(t, err)
	require.Zero(t, reopen.Times())
	require.Same(t, previous, local.info)
	require.Equal(t, refs, guard.runtime.refs)
	require.True(t, proto.Equal(original, info))
}
