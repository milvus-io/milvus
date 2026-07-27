package idf

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type sealedCacheKey string

type sealedCacheEntry struct {
	stats bm25Stats
	refs  int
}

type segmentCache struct {
	mu      sync.Mutex
	entries map[sealedCacheKey]*sealedCacheEntry
}

func newSegmentCache() *segmentCache {
	return &segmentCache{
		entries: make(map[sealedCacheKey]*sealedCacheEntry),
	}
}

func (c *segmentCache) acquire(
	ctx context.Context,
	chunkManager storage.ChunkManager,
	resource *datapb.StreamingNodeBM25Resource,
) (bm25Stats, *segmentCacheLease, error) {
	aggregate := make(bm25Stats)
	if chunkManager == nil || resource == nil {
		return aggregate, nil, nil
	}
	key, err := buildSealedCacheKey(resource)
	if err != nil {
		return nil, nil, err
	}
	stats, err := c.retain(ctx, chunkManager, key, resource)
	if err != nil {
		return nil, nil, err
	}
	aggregate.merge(stats)
	return aggregate, &segmentCacheLease{cache: c, keys: []sealedCacheKey{key}}, nil
}

func (c *segmentCache) retain(
	ctx context.Context,
	chunkManager storage.ChunkManager,
	key sealedCacheKey,
	resource *datapb.StreamingNodeBM25Resource,
) (bm25Stats, error) {
	c.mu.Lock()
	if entry, ok := c.entries[key]; ok {
		entry.refs++
		stats := entry.stats
		c.mu.Unlock()
		return stats, nil
	}
	c.mu.Unlock()

	stats, err := loadSealedSegmentStats(ctx, chunkManager, resource)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if entry, ok := c.entries[key]; ok {
		entry.refs++
		return entry.stats, nil
	}
	c.entries[key] = &sealedCacheEntry{
		stats: stats,
		refs:  1,
	}
	return stats, nil
}

func (c *segmentCache) release(keys []sealedCacheKey) {
	if c == nil || len(keys) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, key := range keys {
		entry, ok := c.entries[key]
		if !ok {
			continue
		}
		entry.refs--
		if entry.refs <= 0 {
			delete(c.entries, key)
		}
	}
}

type segmentCacheLease struct {
	cache     *segmentCache
	keys      []sealedCacheKey
	closeOnce sync.Once
}

func (l *segmentCacheLease) Close() {
	if l == nil {
		return
	}
	l.closeOnce.Do(func() {
		l.cache.release(l.keys)
	})
}

func buildSealedCacheKey(resource *datapb.StreamingNodeBM25Resource) (sealedCacheKey, error) {
	bytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(resource)
	if err != nil {
		return "", errors.Wrap(err, "marshal bm25 sealed cache key")
	}
	return sealedCacheKey(bytes), nil
}

func loadSealedSegmentStats(
	ctx context.Context,
	chunkManager storage.ChunkManager,
	resource *datapb.StreamingNodeBM25Resource,
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
		fieldStats := stats.getOrCreate(fieldID)
		for _, path := range paths {
			bytes, err := chunkManager.Read(ctx, path)
			if err != nil {
				return nil, err
			}
			loaded, err := storage.NewBM25StatsWithBytes(bytes)
			if err != nil {
				return nil, err
			}
			fieldStats.Merge(loaded)
		}
	}
	return stats, nil
}
