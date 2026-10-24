package balancer

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/dataview"
	datacatalog "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	catalogkv "github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/metastore/kv/queryview"
	"github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	balancercache "github.com/milvus-io/milvus/internal/views/coord/balancer/cache"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/coordview/syncer"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/coord/loadstatus"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// Exercise the extracted components without installing QueryCoord's production
// runtime. Two pchannels represent two independent SN transport endpoints. Only
// catalog I/O and worker acknowledgements are mocked; scheduling, reference
// ownership, configuration identity and readiness use their real implementations.
func TestCoordinatorComponentsSixQNAndTwoSN(t *testing.T) {
	ctx := t.Context()
	views := []*viewpb.DataViewOfCollection{}
	native := []*api.CollectionDataView{}
	channels := map[int64][]string{}
	for id := int64(1); id <= 2; id++ {
		view := &viewpb.DataViewOfCollection{CollectionId: id, DataVersion: &viewpb.DataVersion{StreamingVersion: 1}}
		data := &api.CollectionDataView{CollectionID: id, DataVersion: qviews.DataVersion{StreamingVersion: 1}}
		for shard := 0; shard < 2; shard++ {
			channel := fmt.Sprintf("p%d_%dv0", shard, id)
			channels[id] = append(channels[id], channel)
			view.Shards = append(view.Shards, &viewpb.DataViewOfShard{Vchannel: channel})
			part := &api.PartitionDataView{PartitionID: 1}
			for segment := int64(1); segment <= 12; segment++ {
				part.Segments = append(part.Segments, &api.SegmentDataView{SegmentID: id*1000 + int64(shard)*100 + segment, PartitionID: 1, LogicalSize: 100000})
			}
			data.Shards = append(data.Shards, &api.ShardDataView{VChannel: channel, GrowingTransformStartAfterTimetick: 100, CompactedTransformStartAfterTimetick: 90, Partitions: []*api.PartitionDataView{part}})
		}
		views = append(views, view)
		native = append(native, api.PrepareCollectionDataView(data))
	}
	for _, patch := range []*mockey.Mocker{
		mockey.Mock((*datacatalog.Catalog).ListAllDataViews).Return(views, nil).Build(),
		mockey.Mock(catalogkv.Catalog.GetCollections).Return([]*querypb.CollectionLoadInfo{}, nil).Build(),
		mockey.Mock(catalogkv.Catalog.GetPartitions).Return(map[int64][]*querypb.PartitionLoadInfo{}, nil).Build(),
		mockey.Mock(catalogkv.Catalog.GetReplicas).Return([]*querypb.Replica{}, nil).Build(),
		mockey.Mock(catalogkv.Catalog.SaveCollection).Return(nil).Build(),
		mockey.Mock(catalogkv.Catalog.SaveReplica).Return(nil).Build(),
	} {
		t.Cleanup(func() { patch.UnPatch() })
	}
	refs, err := dataview.RecoverManager(ctx, datacatalog.NewCatalog(nil, "", ""), func(context.Context, int64) (bool, error) { return true, nil }, nil, nil, nil)
	require.NoError(t, err)
	store, err := loadmgr.RecoverLoadConfigStore(ctx, catalogkv.NewCatalog(nil))
	require.NoError(t, err)
	catalog := queryview.NewQueryViewCatalog(nil, "coord")
	var mu sync.Mutex
	held := true
	var pending []syncer.SyncView
	snCalls := map[string]int{}
	qnCalls := map[int64]int{}
	acknowledge := func(view syncer.SyncView) {
		response := view.View.IntoProto()
		if response.Meta.State == viewpb.QueryViewState_QueryViewStatePreparing {
			response.Meta.State = viewpb.QueryViewState_QueryViewStateReady
		}
		for _, node := range response.QueryNode {
			for _, part := range node.Partitions {
				part.ReadySegmentIds = part.SegmentIds
			}
		}
		view.OnSyncResponse(qviews.NewQueryViewAtWorkNodeFromProto(response))
	}
	for _, patch := range []*mockey.Mocker{
		mockey.Mock(mockey.GetMethod(catalog, "ListQueryViews")).Return(nil, nil).Build(),
		mockey.Mock(mockey.GetMethod(catalog, "SaveQueryViews")).Return(nil).Build(),
		mockey.Mock((*stubSyncer).SyncViews).To(func(_ *stubSyncer, _ context.Context, group syncer.SyncGroup) error {
			for _, views := range group.ViewsByNode {
				for _, view := range views {
					mu.Lock()
					delay := false
					switch node := view.View.WorkNode().(type) {
					case qviews.StreamingNode:
						snCalls[node.PChannel]++
						delay = held && node.PChannel == "p1"
					case qviews.QueryNode:
						qnCalls[node.ID]++
					}
					if delay {
						pending = append(pending, view)
					}
					mu.Unlock()
					if !delay {
						acknowledge(view)
					}
				}
			}
			return nil
		}).Build(),
	} {
		t.Cleanup(func() { patch.UnPatch() })
	}
	registry, err := coordview.RecoverShardViewRegistryWithDataViews(ctx, catalog, &stubSyncer{}, refs)
	require.NoError(t, err)
	t.Cleanup(registry.Close)
	cache := balancercache.New(nil)
	t.Cleanup(store.RegisterLoadConfigListener(cache.PublishLoadConfig))
	t.Cleanup(registry.RegisterPublicationListener(cache.PublishShard))
	for id := int64(1); id <= 6; id++ {
		cache.PublishNode(id, &NodeInfo{NodeID: id, Alive: true, ResourceGroup: "rg1"})
	}
	for _, data := range native {
		id := data.CollectionID
		require.NoError(t, store.Put(ctx, &loadmgr.LoadConfig{CollectionID: id, PartitionIDs: []int64{1}, Replicas: []*loadmgr.ReplicaAssignment{{ReplicaID: id * 10, ResourceGroup: "rg1"}, {ReplicaID: id*10 + 1, ResourceGroup: "rg1"}}}))
		cache.PublishDataView(id, data)
	}
	cache.MarkReady()
	controller := NewDefaultBalancer(cache, registry, nil, nil)
	controller.Trigger()
	require.NoError(t, controller.Reconcile(ctx))
	require.Eventually(t, func() bool {
		return loadstatus.Get(store, registry, 1, channels[1]).Loaded == 2 && loadstatus.Get(store, registry, 2, channels[2]).Loaded == 2
	}, 10*time.Second, time.Millisecond)
	for id := int64(1); id <= 2; id++ {
		progress := loadstatus.Get(store, registry, id, channels[id])
		require.EqualValues(t, 4, progress.Total)
		require.EqualValues(t, 50, progress.Percentage())
		require.False(t, progress.Ready(), "one SN's acknowledgement cannot cover the missing vchannel")
	}
	mu.Lock()
	held = false
	delayed := pending
	pending = nil
	mu.Unlock()
	for _, view := range delayed {
		acknowledge(view)
	}
	require.Eventually(t, func() bool {
		return loadstatus.Get(store, registry, 1, channels[1]).Ready() && loadstatus.Get(store, registry, 2, channels[2]).Ready()
	}, 10*time.Second, time.Millisecond)
	for id := int64(1); id <= 2; id++ {
		for _, replica := range store.GetConfig(id).Replicas {
			for _, channel := range channels[id] {
				stats := registry.Get(qviews.ShardID{ReplicaID: replica.ReplicaID, VChannel: channel}).Stats()
				require.Equal(t, store.GetConfigVersion(id), stats.UpLoadInfoVersion)
				require.NotEmpty(t, stats.UpNodes)
				for _, node := range stats.UpNodes {
					require.GreaterOrEqual(t, node, int64(1))
					require.LessOrEqual(t, node, int64(6))
				}
			}
		}
	}
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, snCalls, 2)
	require.Positive(t, snCalls["p0"])
	require.Positive(t, snCalls["p1"])
	require.Len(t, qnCalls, 6)
}
