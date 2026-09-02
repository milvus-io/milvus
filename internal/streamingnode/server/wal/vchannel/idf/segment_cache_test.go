package idf

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
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
