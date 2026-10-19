package pathutil

import (
	"fmt"
	"path/filepath"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type PathType int

const (
	GrowingMMapPath PathType = iota
	LocalChunkPath
	BM25Path
	RootCachePath
)

const (
	// CachePathPrefix is the shared top-level directory for every node-local
	// cache below. It aliases common.LocalCacheRootPath so the segment stays
	// registered in common.LocalOnlyStorageRootSegments: under
	// common.storageType=local this directory sits directly under the
	// ChunkManager root, and import path validation refuses paths into it.
	CachePathPrefix       = common.LocalCacheRootPath
	GrowingMMapPathPrefix = "growing_mmap"
	LocalChunkPathPrefix  = "local_chunk"
	BM25PathPrefix        = "bm25"
)

func GetPath(pathType PathType, nodeID int64) string {
	rootPath := paramtable.Get().LocalStorageCfg.Path.GetValue()

	path := filepath.Join(rootPath, CachePathPrefix)
	switch pathType {
	case GrowingMMapPath:
		path = filepath.Join(path, fmt.Sprintf("%d", nodeID), GrowingMMapPathPrefix)
	case LocalChunkPath:
		path = filepath.Join(path, fmt.Sprintf("%d", nodeID), LocalChunkPathPrefix)
	case BM25Path:
		path = filepath.Join(path, fmt.Sprintf("%d", nodeID), BM25PathPrefix)
	case RootCachePath:
	}
	log.Info("Get path for", zap.Any("pathType", pathType), zap.Int64("nodeID", nodeID), zap.String("path", path))
	return path
}
