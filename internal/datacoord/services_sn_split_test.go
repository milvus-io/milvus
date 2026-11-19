package datacoord

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/split"
	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/internal/metacache"
	catalogkv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestStreamingNodeResourcesRecoverRetainedSplitSource(t *testing.T) {
	for _, tc := range []struct {
		name      string
		state     datapb.SplitShardTaskState
		channel   string
		published bool
		foreign   bool
		wantError error
	}{
		{"adopted", datapb.SplitShardTaskState_SplitShardTaskAdopted, "v2", true, false, nil},
		{"task save lags atomic adoption", datapb.SplitShardTaskState_SplitShardTaskRewriteDone, "v2", true, false, nil},
		{"not adopted", datapb.SplitShardTaskState_SplitShardTaskWindowOpen, "v2", true, false, merr.ErrServiceInternal},
		{"unpublished adoption", datapb.SplitShardTaskState_SplitShardTaskRewriteDone, "v2", false, false, merr.ErrServiceNotReady},
		{"foreign target", datapb.SplitShardTaskState_SplitShardTaskAdopted, "foreign", true, false, merr.ErrServiceInternal},
		{"retired source", datapb.SplitShardTaskState_SplitShardTaskRetired, "v2", true, false, merr.ErrServiceInternal},
		{"foreign collection", datapb.SplitShardTaskState_SplitShardTaskAdopted, "v2", true, true, merr.ErrDataIntegrity},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			oldVersion := &viewpb.DataVersion{StreamingVersion: 1}
			views := []*viewpb.DataViewOfCollection{{CollectionId: 1, DataVersion: oldVersion, Shards: []*viewpb.DataViewOfShard{
				{Vchannel: "v1", Partitions: []*viewpb.DataViewOfPartition{{PartitionId: 2, SegmentIds: []int64{3}, SegmentManifestVersions: []int64{0}}}},
				{Vchannel: "v2"},
				{Vchannel: "v3"},
			}}}
			if tc.published {
				views = append(views, &viewpb.DataViewOfCollection{CollectionId: 1, DataVersion: &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 1}, Shards: []*viewpb.DataViewOfShard{
					{Vchannel: tc.channel, Partitions: []*viewpb.DataViewOfPartition{{PartitionId: 2, SegmentIds: []int64{3}, SegmentManifestVersions: []int64{0}}}},
					{Vchannel: "v3"},
				}})
			}
			patch := mockey.Mock((*catalogkv.Catalog).ListAllDataViews).Return(views, nil).Build()
			defer patch.UnPatch()
			manager, err := dataview.RecoverManager(ctx, &catalogkv.Catalog{}, func(context.Context, int64) (bool, error) { return true, nil }, nil, nil, nil)
			require.NoError(t, err)
			catalog := &splitTestCatalog{tasks: map[int64]*datapb.SplitShardTask{7: {
				TaskId: 7, CollectionId: 1, SourceVchannel: "v1", State: tc.state,
				Targets: []*datapb.SplitShardTaskTarget{{Vchannel: "v2"}, {Vchannel: "v3"}},
			}}}
			splitStore, err := split.NewStore(ctx, catalog)
			require.NoError(t, err)
			store := metacache.NewMetaStore(nil)
			s := &Server{dataViewManager: manager, splitStore: splitStore, meta: &meta.Meta{Segments: meta.NewSegmentsInfo(store), MetaStore: store}}
			s.stateCode.Store(commonpb.StateCode_Healthy)
			info := &datapb.SegmentInfo{ID: 3, CollectionID: 1, PartitionID: 2, InsertChannel: tc.channel}
			if tc.foreign {
				info.CollectionID = 99
			}
			s.meta.Segments.SetSegment(3, meta.NewSegmentInfo(info))
			resp, err := s.GetStreamingNodeQueryViewResources(ctx, &datapb.GetStreamingNodeQueryViewResourcesRequest{CollectionId: 1, Vchannel: "v1", DataVersion: oldVersion})
			require.NoError(t, err)
			if tc.wantError != nil {
				require.ErrorIs(t, merr.Error(resp.GetStatus()), tc.wantError)
				return
			}
			require.NoError(t, merr.Error(resp.GetStatus()))
			require.Len(t, resp.GetBm25Resources(), 1)
			require.Equal(t, int64(3), resp.Bm25Resources[0].GetSegmentId())
			require.Equal(t, tc.channel, s.meta.GetSegment(ctx, 3).GetInsertChannel(), "historical read must not rewrite current ownership")
		})
	}
}
