package datacoord

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/internal/metacache"
	datacatalog "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestQueryViewManifestPublicationAdvancesDataViewWithoutWatch(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	catalog := datacatalog.NewCatalog(NewMetaMemoryKV(), "", "")
	m := &meta{ctx: ctx, catalog: catalog, segments: NewSegmentsInfo(metacache.NewMetaStore(catalog))}
	manager, err := dataview.RecoverManager(ctx, catalog, func(context.Context, int64) (bool, error) { return true, nil }, m.loadableProjection, nil, nil)
	require.NoError(t, err)
	m.dataViewManager = manager
	oldManifest := packed.MarshalManifestPath("test/base", 1)
	m.segments.SetSegment(10, NewSegmentInfo(&datapb.SegmentInfo{ID: 10, CollectionID: 1, PartitionID: 2, InsertChannel: "v", State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1, StorageVersion: storage.StorageV3, ManifestPath: oldManifest, NumOfRows: 10}))
	segments, err := m.loadableProjection(ctx, 1)
	require.NoError(t, err)
	_, err = manager.OnBootstrapCollection(ctx, dataview.BootstrapCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"v"}, Segments: segments})
	require.NoError(t, err)
	old, err := manager.Latest(ctx, 1)
	require.NoError(t, err)
	defer old.Deref()
	nextManifest := packed.MarshalManifestPath("test/base", 2)
	require.NoError(t, m.UpdateSegmentsInfo(ctx, UpdateManifest(10, nextManifest)))
	require.Eventually(t, func() bool {
		next, err := manager.Latest(ctx, 1)
		if err != nil || next == nil {
			return false
		}
		defer next.Deref()
		return next.Version().GetCompactVersion() == old.Version().GetCompactVersion()+1 && next.DataView().GetShards()[0].GetPartitions()[0].GetSegmentManifestVersions()[0] == 2
	}, 5*time.Second, time.Millisecond)
	require.EqualValues(t, 1, old.DataView().GetShards()[0].GetPartitions()[0].GetSegmentManifestVersions()[0], "retained DataView remains immutable")
}
