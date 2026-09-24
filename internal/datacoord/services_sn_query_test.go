package datacoord

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/dataview"
	catalogkv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestStreamingNodeResourcesRespectDataViewManifest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	version := &viewpb.DataVersion{StreamingVersion: 1}
	view := &viewpb.DataViewOfCollection{CollectionId: 1, DataVersion: version, Shards: []*viewpb.DataViewOfShard{{Vchannel: "v1", Partitions: []*viewpb.DataViewOfPartition{{PartitionId: 2, SegmentIds: []int64{3, 4}, SegmentManifestVersions: []int64{7, 0}}}}}}
	patch := mockey.Mock((*catalogkv.Catalog).ListAllDataViews).Return([]*viewpb.DataViewOfCollection{view}, nil).Build()
	defer patch.UnPatch()
	manager, err := dataview.RecoverManager(ctx, &catalogkv.Catalog{}, func(context.Context, int64) (bool, error) { return true, nil }, nil, nil, nil)
	require.NoError(t, err)
	s := &Server{dataViewManager: manager, meta: &meta{segments: NewSegmentsInfo()}}
	s.stateCode.Store(commonpb.StateCode_Healthy)
	for _, id := range []int64{3, 4} {
		s.meta.segments.SetSegment(id, NewSegmentInfo(&datapb.SegmentInfo{ID: id, CollectionID: 1, PartitionID: 2, InsertChannel: "v1", ManifestPath: packed.MarshalManifestPath("segments", 9)}))
	}
	resp, err := s.GetStreamingNodeQueryViewResources(ctx, &datapb.GetStreamingNodeQueryViewResourcesRequest{CollectionId: 1, Vchannel: "v1", DataVersion: version})
	require.NoError(t, err)
	require.NoError(t, merr.Error(resp.GetStatus()))
	require.Len(t, resp.GetBm25Resources(), 2)
	require.Equal(t, packed.MarshalManifestPath("segments", 7), resp.Bm25Resources[0].GetManifestPath(), "historical view must not read latest manifest")
	require.Equal(t, packed.MarshalManifestPath("segments", 9), resp.Bm25Resources[1].GetManifestPath(), "version zero resolves current metadata")
	require.Equal(t, packed.MarshalManifestPath("segments", 9), s.meta.GetSegment(ctx, 3).GetManifestPath(), "shared metadata stays immutable")
	resp, err = s.GetStreamingNodeQueryViewResources(ctx, &datapb.GetStreamingNodeQueryViewResourcesRequest{CollectionId: 1, Vchannel: "v1", DataVersion: &viewpb.DataVersion{StreamingVersion: 100}})
	require.NoError(t, err)
	require.Error(t, merr.Error(resp.GetStatus()))
}
