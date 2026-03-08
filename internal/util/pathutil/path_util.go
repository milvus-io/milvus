package pathutil

import (
	"fmt"
	"path/filepath"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type PathType int

const (
	GrowingMMapPath PathType = iota
	LocalChunkPath
	BM25Path
	RootCachePath
	FileResourcePath
)

const (
	CachePathPrefix        = "cache"
	GrowingMMapPathPrefix  = "growing_mmap"
	LocalChunkPathPrefix   = "local_chunk"
	BM25PathPrefix         = "bm25"
	FileResourcePathPrefix = "file_resource"
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
	case FileResourcePath:
		path = filepath.Join(path, fmt.Sprintf("%d", nodeID), FileResourcePathPrefix)
	case RootCachePath:
	}
	log.Info("Get path for", zap.Any("pathType", pathType), zap.Int64("nodeID", nodeID), zap.String("path", path))
	return path
}
