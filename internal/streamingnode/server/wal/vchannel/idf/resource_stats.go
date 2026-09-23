package idf

import (
	"bufio"
	"context"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func loadSealedSegmentStats(
	ctx context.Context,
	chunkManager storage.ChunkManager,
	resource *datapb.StreamingNodeBM25Resource,
	loadedFields ...bm25Stats,
) (bm25Stats, error) {
	if resource.GetStorageVersion() >= storage.StorageV3 && resource.GetManifestPath() == "" {
		return nil, merr.WrapErrDataIntegrityMsg("storage v3 BM25 resource for segment %d has no manifest", resource.GetSegmentId())
	}
	manifestPath := ""
	if resource.GetStorageVersion() >= storage.StorageV3 {
		manifestPath = resource.GetManifestPath()
	}
	pathsByField, err := packed.NewStatsResolver(manifestPath, packed.CreateStorageConfig()).
		WithBM25Logs(resource.GetBm25Binlogs()).
		BM25StatsPaths()
	if err != nil {
		return nil, merr.Wrap(err, "resolve sealed BM25 stats paths")
	}
	stats := make(bm25Stats)
	for fieldID, paths := range pathsByField {
		if len(loadedFields) > 0 {
			if _, ok := loadedFields[0][fieldID]; !ok {
				continue
			}
		}
		fieldStats := stats.getOrCreate(fieldID)
		for _, path := range paths {
			reader, err := chunkManager.Reader(ctx, path)
			if err != nil {
				return nil, err
			}
			loaded := storage.NewBM25Stats()
			err = loaded.DeserializeFromReader(bufio.NewReaderSize(reader, 64*1024))
			closeErr := reader.Close()
			if err == nil {
				err = closeErr
			}
			if err != nil {
				return nil, err
			}
			fieldStats.Merge(loaded)
		}
	}
	return stats, nil
}
