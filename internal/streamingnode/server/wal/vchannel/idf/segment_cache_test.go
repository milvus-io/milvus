package idf

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestLoadSealedSegmentStatsFromStorageV3Manifest(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	chunkManager := storage.NewLocalChunkManager()
	stats := storage.NewBM25Stats()
	stats.Append(map[uint32]float32{1: 2, 2: 1})
	bytes, err := stats.Serialize()
	require.NoError(t, err)
	statsPath := t.TempDir() + "/bm25-stats"
	require.NoError(t, chunkManager.Write(ctx, statsPath, bytes))

	manifestPath := packed.MarshalManifestPath("files/insert_log/1/2/3", 1)
	newResolver := mockey.Mock(packed.NewStatsResolver).To(func(path string, config *indexpb.StorageConfig) *packed.StatsResolver {
		require.Equal(t, manifestPath, path)
		require.NotNil(t, config)
		return &packed.StatsResolver{}
	}).Build()
	defer newResolver.UnPatch()
	resolvePaths := mockey.Mock((*packed.StatsResolver).BM25StatsPaths).Return(map[int64][]string{
		102: {statsPath},
	}, nil).Build()
	defer resolvePaths.UnPatch()

	loaded, err := loadSealedSegmentStats(ctx, chunkManager, &datapb.StreamingNodeBM25Resource{
		SegmentId:      3,
		StorageVersion: storage.StorageV3,
		ManifestPath:   manifestPath,
	})
	require.NoError(t, err)
	require.Contains(t, loaded, int64(102))
	require.Equal(t, int64(1), loaded[102].NumRow())
	require.Equal(t, float64(3), loaded[102].GetAvgdl())
}

func TestLoadSealedSegmentStatsRejectsStorageV3WithoutManifest(t *testing.T) {
	paramtable.Init()
	_, err := loadSealedSegmentStats(context.Background(), storage.NewLocalChunkManager(), &datapb.StreamingNodeBM25Resource{
		SegmentId:      3,
		StorageVersion: storage.StorageV3,
	})
	require.Error(t, err)
}

func TestLoadSealedSegmentStatsUsesLegacyBinlogsForStorageV2(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	chunkManager := storage.NewLocalChunkManager()
	stats := storage.NewBM25Stats()
	stats.Append(map[uint32]float32{1: 1})
	bytes, err := stats.Serialize()
	require.NoError(t, err)
	statsPath := t.TempDir() + "/bm25-stats"
	require.NoError(t, chunkManager.Write(ctx, statsPath, bytes))

	newResolver := mockey.Mock(packed.NewStatsResolver).To(func(path string, config *indexpb.StorageConfig) *packed.StatsResolver {
		require.Empty(t, path)
		require.NotNil(t, config)
		return &packed.StatsResolver{}
	}).Build()
	defer newResolver.UnPatch()

	loaded, err := loadSealedSegmentStats(ctx, chunkManager, &datapb.StreamingNodeBM25Resource{
		SegmentId:      3,
		StorageVersion: storage.StorageV2,
		ManifestPath:   "legacy-manifest-list",
		Bm25Binlogs: []*datapb.FieldBinlog{{
			FieldID: 102,
			Binlogs: []*datapb.Binlog{{LogPath: statsPath}},
		}},
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), loaded[102].NumRow())
}

// A retained old QueryView reloads a segment as growing even after StorageV3
// flush; its BM25 statistics are only referenced by the persisted manifest.
func TestRecoverGrowingBM25FromStorageV3Manifest(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	cm := storage.NewLocalChunkManager()
	stats := storage.NewBM25Stats()
	stats.Append(map[uint32]float32{7: 2})
	data, err := stats.Serialize()
	require.NoError(t, err)
	path := t.TempDir() + "/bm25"
	require.NoError(t, cm.Write(ctx, path, data))
	manifest := packed.MarshalManifestPath("segments", 3)
	patch := mockey.Mock(packed.NewStatsResolver).To(func(actual string, _ *indexpb.StorageConfig) *packed.StatsResolver {
		require.Equal(t, manifest, actual)
		return &packed.StatsResolver{}
	}).Build()
	defer patch.UnPatch()
	paths := mockey.Mock((*packed.StatsResolver).BM25StatsPaths).Return(map[int64][]string{102: {path}}, nil).Build()
	defer paths.UnPatch()
	runtime := &oracleRuntime{provider: NewProvider(nil, WithChunkManager(cm)), growingStore: newGrowingStatsStore(nil)}
	require.NoError(t, runtime.collectPersistedGrowingStats(ctx, walview.VisibleSegment{SegmentID: 20, PartitionID: 10, Data: walview.SegmentSnapshotData{PersistedStorage: &streamingpb.L1SegmentPersistedStorage{ManifestPath: manifest}}}))
	require.Equal(t, float64(2), runtime.growingStore.segments[20].stats[102].GetAvgdl())
	require.Equal(t, int64(1), runtime.growingStore.segments[20].stats[102].NumRow())
}

func TestFailedOracleInitializationReleasesSealedResources(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	cm := storage.NewLocalChunkManager()
	stats := storage.NewBM25Stats()
	stats.Append(map[uint32]float32{7: 2})
	data, err := stats.Serialize()
	require.NoError(t, err)
	path := t.TempDir() + "/bm25"
	require.NoError(t, cm.Write(ctx, path, data))
	good := &datapb.StreamingNodeBM25Resource{SegmentId: 20, StorageVersion: storage.StorageV2, Bm25Binlogs: []*datapb.FieldBinlog{{FieldID: 102, Binlogs: []*datapb.Binlog{{LogPath: path}}}}}
	bad := &datapb.StreamingNodeBM25Resource{SegmentId: 21, StorageVersion: storage.StorageV3}
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	provider := NewProvider(nil, WithChunkManager(cm), WithNodeScheduler(scheduler))
	_, err = provider.acquireSealedContributions(ctx, []*datapb.StreamingNodeBM25Resource{good, bad})
	require.Error(t, err)
	require.Empty(t, provider.sealedCache.entries)
	missing := walview.VisibleSegment{SegmentID: 22, Data: walview.SegmentSnapshotData{PersistedStorage: &streamingpb.L1SegmentPersistedStorage{Binlogs: []*streamingpb.L1SegmentBinLogs{{Bm25Binlog: []*datapb.FieldBinlog{{FieldID: 102, Binlogs: []*datapb.Binlog{{LogPath: path + "-missing"}}}}}}}}}
	_, err = newOracleRuntime(ctx, provider, walview.VChannelWALView{SegmentSnapshot: walview.VisibleSegmentSnapshot{Segments: []walview.VisibleSegment{missing}}}, []*datapb.StreamingNodeBM25Resource{good})
	require.Error(t, err)
	require.Empty(t, provider.sealedCache.entries)
	require.NoError(t, cm.Write(ctx, path, []byte("invalid statistics")))
	_, err = loadSealedSegmentStats(ctx, cm, good)
	require.Error(t, err)
}
