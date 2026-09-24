package idf

import (
	"bufio"
	"context"
	"io"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	importcommon "github.com/milvus-io/milvus/internal/util/importutilv2/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
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
		var fieldStats *storage.BM25Stats
		for _, path := range paths {
			loaded, err := readBM25Stats(ctx, chunkManager, path)
			if err != nil {
				return nil, err
			}
			if fieldStats == nil {
				fieldStats = loaded
				stats[fieldID] = loaded
			} else {
				fieldStats.Merge(loaded)
			}
		}
	}
	return stats, nil
}

// Read directly from object storage. DataView references keep the descriptors
// valid until a successful replacement, so evictions do not need a local cache.
func readBM25Stats(ctx context.Context, cm storage.ChunkManager, path string) (*storage.BM25Stats, error) {
	var reader storage.FileReader
	err := retry.Do(ctx, func() error {
		var err error
		reader, err = cm.Reader(ctx, path)
		return storage.ToMilvusIoError(path, err)
	}, retry.Attempts(paramtable.Get().CommonCfg.StorageReadRetryAttempts.GetAsUint()), retry.RetryErr(merr.IsRetryableErr))
	if err != nil {
		return nil, err
	}
	stream := importcommon.NewRetryableReaderWithReopen(ctx, path, reader, importcommon.NewChunkManagerReopenReaderFunc(cm), cm.Size)
	stats := storage.NewBM25Stats()
	err = stats.DeserializeFromReader(bufio.NewReaderSize(stream, paramtable.Get().QueryNodeCfg.IDFReadBufferSize.GetAsInt()))
	closeErr := stream.Close()
	if err != nil {
		// A premature transport EOF is typed by retryableReader. Preserve its
		// retryability; only an incomplete record in a complete stream is corrupt.
		if merr.IsMilvusError(err) {
			return nil, merr.Wrapf(err, "read BM25 stats %s", path)
		}
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, merr.WrapErrSerializationFailed(err, "decode BM25 stats %s", path)
		}
		return nil, storage.ToMilvusIoError(path, err)
	}
	if closeErr != nil {
		return nil, storage.ToMilvusIoError(path, closeErr)
	}
	return stats, nil
}
