package idf

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"sync"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	importcommon "github.com/milvus-io/milvus/internal/util/importutilv2/common"
	"github.com/milvus-io/milvus/internal/util/pathutil"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

type sealedCacheKey string

// sealedBm25Stats follows the QueryNode IDF oracle ownership model: decoded
// stats are temporary, while the sealed segment keeps only immutable local
// files and the metadata needed to read them.
type sealedBm25Stats struct {
	sync.RWMutex

	removed   bool
	key       sealedCacheKey
	segmentID int64
	localDir  string
	fieldList []int64

	// refs and load are protected by segmentCache.mu.
	refs int
	load *sealedStatsLoad
}

type sealedStatsLoad struct {
	ready chan struct{}
	err   error
}

// FetchStats reads stats from the local multi-file directory and accumulates
// all files for each field. The entry lock prevents removal during the read.
func (s *sealedBm25Stats) FetchStats() (bm25Stats, error) {
	if s == nil {
		return make(bm25Stats), nil
	}
	s.RLock()
	defer s.RUnlock()
	return s.fetchStatsLocked()
}

func (s *sealedBm25Stats) fetchStatsLocked() (bm25Stats, error) {
	if s.removed {
		return nil, merr.WrapErrServiceInternalMsg("sealed BM25 stats for segment %d already removed", s.segmentID)
	}

	stats := make(bm25Stats, len(s.fieldList))
	for _, fieldID := range s.fieldList {
		fieldDir := path.Join(s.localDir, fmt.Sprintf("%d", fieldID))
		entries, err := os.ReadDir(fieldDir)
		if err != nil {
			return nil, merr.WrapErrIoFailed(fieldDir, err)
		}

		fieldStats := storage.NewBM25Stats()
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			filePath := path.Join(fieldDir, entry.Name())
			file, err := os.Open(filePath)
			if err != nil {
				return nil, merr.WrapErrIoFailed(filePath, err)
			}
			deserializeErr := fieldStats.DeserializeFromReader(bufio.NewReader(file))
			closeErr := file.Close()
			if deserializeErr != nil {
				if errors.Is(deserializeErr, io.EOF) || errors.Is(deserializeErr, io.ErrUnexpectedEOF) {
					return nil, merr.WrapErrSerializationFailed(deserializeErr, "deserialize local file %s", filePath)
				}
				return nil, merr.WrapErrIoFailed(filePath, deserializeErr)
			}
			if closeErr != nil {
				return nil, merr.WrapErrIoFailed(filePath, closeErr)
			}
		}
		stats[fieldID] = fieldStats
	}
	return stats, nil
}

func (s *sealedBm25Stats) Remove() {
	if s == nil {
		return
	}
	s.Lock()
	defer s.Unlock()
	s.removed = true
	if s.localDir == "" {
		return
	}
	if err := os.RemoveAll(s.localDir); err != nil {
		mlog.Warn(context.TODO(), "remove local BM25 stats failed", mlog.String("path", s.localDir), mlog.Err(err))
	}
}

type segmentCache struct {
	mu      sync.Mutex
	rootDir string
	entries map[sealedCacheKey]*sealedBm25Stats
}

var queryViewBM25CacheCleanup sync.Once

func newSegmentCache() *segmentCache {
	rootDir := path.Join(
		pathutil.GetPath(pathutil.BM25Path, paramtable.GetNodeID()),
		"query-view",
	)
	queryViewBM25CacheCleanup.Do(func() {
		if err := os.RemoveAll(rootDir); err != nil {
			mlog.Warn(context.TODO(), "remove stale QueryView BM25 cache failed", mlog.String("path", rootDir), mlog.Err(err))
		}
	})
	return &segmentCache{
		rootDir: rootDir,
		entries: make(map[sealedCacheKey]*sealedBm25Stats),
	}
}

func buildSealedCacheKey(resource *datapb.StreamingNodeBM25Resource) (sealedCacheKey, error) {
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(resource)
	if err != nil {
		return "", merr.WrapErrServiceInternalErr(err, "marshal sealed BM25 resource cache key")
	}
	return sealedCacheKey(encoded), nil
}

// acquire keeps the sealed segment's local files alive until the matching
// release. Decoded stats are temporary and are never retained by sealedBm25Stats.
func (c *segmentCache) acquire(
	ctx context.Context,
	chunkManager storage.ChunkManager,
	resource *datapb.StreamingNodeBM25Resource,
	needParse bool,
) (bm25Stats, *sealedBm25Stats, error) {
	if resource == nil {
		return make(bm25Stats), nil, nil
	}
	if chunkManager == nil {
		return nil, nil, merr.WrapErrServiceInternalMsg("chunk manager is unavailable for sealed BM25 stats")
	}
	key, err := buildSealedCacheKey(resource)
	if err != nil {
		return nil, nil, err
	}
	segmentID := resource.GetSegmentId()

	for {
		c.mu.Lock()
		if entry := c.entries[key]; entry != nil {
			entry.refs++
			load := entry.load
			c.mu.Unlock()
			if load != nil {
				select {
				case <-load.ready:
				case <-ctx.Done():
					c.release(entry)
					return nil, nil, ctx.Err()
				}
			}
			if err := ctx.Err(); err != nil {
				c.release(entry)
				return nil, nil, err
			}
			if load != nil && load.err != nil {
				c.release(entry)
				if merr.IsCanceledOrTimeout(load.err) {
					continue
				}
				return nil, nil, load.err
			}
			var stats bm25Stats
			if needParse {
				stats, err = entry.FetchStats()
				if err != nil {
					c.release(entry)
					return nil, nil, err
				}
			}
			if err := ctx.Err(); err != nil {
				c.release(entry)
				return nil, nil, err
			}
			return stats, entry, nil
		}

		load := &sealedStatsLoad{ready: make(chan struct{})}
		entry := &sealedBm25Stats{
			key:       key,
			segmentID: segmentID,
			refs:      1,
			load:      load,
		}
		c.entries[key] = entry
		c.mu.Unlock()

		result, loadErr := c.streamLoad(ctx, chunkManager, resource, needParse)
		if loadErr == nil {
			loadErr = ctx.Err()
		}
		c.mu.Lock()
		entry.load = nil
		load.err = loadErr
		entry.localDir = result.localDir
		entry.fieldList = result.fieldList
		remove := loadErr != nil
		if remove && c.entries[key] == entry {
			delete(c.entries, key)
		}
		close(load.ready)
		c.mu.Unlock()
		if remove {
			entry.Remove()
			return nil, nil, loadErr
		}
		if needParse {
			return result.stats, entry, nil
		}
		return nil, entry, nil
	}
}

func (c *segmentCache) release(expected *sealedBm25Stats) {
	if c == nil || expected == nil {
		return
	}
	c.mu.Lock()
	entry := c.entries[expected.key]
	if entry == nil || entry != expected {
		c.mu.Unlock()
		return
	}
	entry.refs--
	if entry.refs > 0 || entry.load != nil {
		c.mu.Unlock()
		return
	}
	delete(c.entries, expected.key)
	c.mu.Unlock()
	entry.Remove()
}

type streamLoadResult struct {
	localDir  string
	fieldList []int64
	stats     bm25Stats
}

func (c *segmentCache) streamLoad(
	ctx context.Context,
	chunkManager storage.ChunkManager,
	resource *datapb.StreamingNodeBM25Resource,
	needParse bool,
) (streamLoadResult, error) {
	pathsByField, err := sealedBM25StatsPaths(resource)
	if err != nil {
		return streamLoadResult{}, err
	}
	if len(pathsByField) == 0 {
		result := streamLoadResult{}
		if needParse {
			result.stats = make(bm25Stats)
		}
		return result, nil
	}

	if err := os.MkdirAll(c.rootDir, os.ModePerm); err != nil {
		return streamLoadResult{}, merr.WrapErrIoFailed(c.rootDir, err)
	}
	localDir, err := os.MkdirTemp(c.rootDir, fmt.Sprintf("%d-", resource.GetSegmentId()))
	if err != nil {
		return streamLoadResult{}, merr.WrapErrIoFailed(c.rootDir, err)
	}
	result := streamLoadResult{localDir: localDir}

	fieldIDs := make([]int64, 0, len(pathsByField))
	for fieldID := range pathsByField {
		fieldIDs = append(fieldIDs, fieldID)
	}
	result.fieldList = fieldIDs
	if needParse {
		result.stats = make(bm25Stats, len(fieldIDs))
	}

	for _, fieldID := range fieldIDs {
		fieldDir := path.Join(localDir, fmt.Sprintf("%d", fieldID))
		if err := os.MkdirAll(fieldDir, os.ModePerm); err != nil {
			return result, merr.WrapErrIoFailed(fieldDir, err)
		}
		var fieldStats *storage.BM25Stats
		if needParse {
			fieldStats = storage.NewBM25Stats()
		}
		for index, remotePath := range pathsByField[fieldID] {
			localFile := path.Join(fieldDir, fmt.Sprintf("%d.data", index))
			err := streamOneBM25StatsFile(ctx, chunkManager, remotePath, localFile, fieldStats)
			if err != nil {
				return result, merr.Wrapf(err, "stream BM25 stats file %s", remotePath)
			}
		}
		if needParse {
			result.stats[fieldID] = fieldStats
		}
	}
	return result, nil
}

func sealedBM25StatsPaths(resource *datapb.StreamingNodeBM25Resource) (map[int64][]string, error) {
	if resource.GetStorageVersion() >= storage.StorageV3 && resource.GetManifestPath() == "" {
		return nil, merr.WrapErrDataIntegrityMsg("storage v3 BM25 resource for segment %d has no manifest", resource.GetSegmentId())
	}
	pathsByField, err := packed.NewStatsResolver(resource.GetManifestPath(), packed.CreateStorageConfig()).
		WithBM25Logs(resource.GetBm25Binlogs()).
		BM25StatsPaths()
	if err != nil {
		return nil, merr.Wrap(err, "resolve sealed BM25 stats paths")
	}
	return pathsByField, nil
}

func streamOneBM25StatsFile(
	ctx context.Context,
	chunkManager storage.ChunkManager,
	remotePath string,
	localPath string,
	parseInto *storage.BM25Stats,
) error {
	attempts := paramtable.Get().CommonCfg.StorageReadRetryAttempts.GetAsUint()
	var reader storage.FileReader
	err := retry.Do(ctx, func() error {
		var openErr error
		reader, openErr = chunkManager.Reader(ctx, remotePath)
		return storage.ToMilvusIoError(remotePath, openErr)
	}, retry.Attempts(attempts), retry.RetryErr(merr.IsRetryableErr))
	if err != nil {
		return merr.Wrapf(normalizeBM25IOError(err, remotePath), "open remote BM25 stats file %s", remotePath)
	}
	retryableReader := importcommon.NewRetryableReaderWithReopen(
		ctx, remotePath, reader, importcommon.NewChunkManagerReopenReaderFunc(chunkManager), chunkManager.Size,
	)
	defer retryableReader.Close()

	file, err := os.Create(localPath)
	if err != nil {
		return merr.WrapErrIoFailed(localPath, err)
	}
	defer file.Close()

	if parseInto != nil {
		bufferedReader := bufio.NewReaderSize(retryableReader, paramtable.Get().QueryNodeCfg.IDFReadBufferSize.GetAsInt())
		bufferedWriter := bufio.NewWriter(file)
		err := parseInto.DeserializeFromReader(io.TeeReader(bufferedReader, bufferedWriter))
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return merr.Wrapf(err, "deserialize remote BM25 stats file %s", remotePath)
			}
			return merr.Wrapf(
				normalizeBM25IOError(err, fmt.Sprintf("%s -> %s", remotePath, localPath)),
				"deserialize remote BM25 stats file %s",
				remotePath,
			)
		}
		if err := bufferedWriter.Flush(); err != nil {
			return merr.WrapErrIoFailed(localPath, err)
		}
		if err := file.Sync(); err != nil {
			return merr.WrapErrIoFailed(localPath, err)
		}
		return nil
	}

	_, err = io.Copy(file, retryableReader)
	if err != nil {
		return merr.Wrapf(
			normalizeBM25IOError(err, fmt.Sprintf("%s -> %s", remotePath, localPath)),
			"copy remote BM25 stats file %s to %s",
			remotePath,
			localPath,
		)
	}
	if err := file.Sync(); err != nil {
		return merr.WrapErrIoFailed(localPath, err)
	}
	return nil
}

func normalizeBM25IOError(err error, key string) error {
	if errors.Is(err, merr.ErrIoUnexpectEOF) {
		// The retryable reader uses this code internally; exhausted reads retain the raw EOF contract.
		return io.ErrUnexpectedEOF
	}
	if err == nil || merr.IsCanceledOrTimeout(err) || errors.Is(err, io.ErrUnexpectedEOF) {
		return err
	}
	return storage.ToMilvusIoError(key, err)
}
