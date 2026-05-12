package metautil

import (
	"path"

	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

// IndexPathBuilder constructs index storage paths.
// Logical paths are relative to Milvus storage namespace.
// Full paths are logical paths prefixed with the configured storage root.
type IndexPathBuilder struct {
	pathVersion  indexpb.IndexStorePathVersion
	collID       int64
	partID       int64
	segID        int64
	buildID      int64
	indexVersion int64
}

// NewIndexPathBuilder creates a builder for constructing index file paths.
// pathVersion: 0 = legacy, >= 1 = collection-partitioned.
func NewIndexPathBuilder(pathVersion indexpb.IndexStorePathVersion, collID, partID, segID, buildID, indexVersion int64) *IndexPathBuilder {
	return &IndexPathBuilder{
		pathVersion:  pathVersion,
		collID:       collID,
		partID:       partID,
		segID:        segID,
		buildID:      buildID,
		indexVersion: indexVersion,
	}
}

func IsCollectionRooted(pathVersion indexpb.IndexStorePathVersion) bool {
	return pathVersion >= indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED
}

func (b *IndexPathBuilder) BuildLogicalFilePath(fileKey string) string {
	return path.Join(b.BuildLogicalPrefix(), fileKey)
}

func (b *IndexPathBuilder) BuildLogicalFilePaths(fileKeys []string) []string {
	paths := make([]string, 0, len(fileKeys))
	for _, fileKey := range fileKeys {
		paths = append(paths, b.BuildLogicalFilePath(fileKey))
	}
	return paths
}

func (b *IndexPathBuilder) BuildFullFilePath(rootPath, fileKey string) string {
	return path.Join(b.BuildFullPrefix(rootPath), fileKey)
}

func (b *IndexPathBuilder) BuildFullFilePaths(rootPath string, fileKeys []string) []string {
	paths := make([]string, 0, len(fileKeys))
	for _, fileKey := range fileKeys {
		paths = append(paths, b.BuildFullFilePath(rootPath, fileKey))
	}
	return paths
}

// v0 logical: index_files/{buildID}/{indexVersion}/{partID}/{segID}
// v1 logical: index_v1/{collID}/{partID}/{segID}/{buildID}/{indexVersion}
func (b *IndexPathBuilder) BuildLogicalPrefix() string {
	if IsCollectionRooted(b.pathVersion) {
		k := JoinIDPath(b.collID, b.partID, b.segID, b.buildID, b.indexVersion)
		return path.Join(common.SegmentIndexV1Path, k)
	}
	k := JoinIDPath(b.buildID, b.indexVersion, b.partID, b.segID)
	return path.Join(common.SegmentIndexV0Path, k)
}

// v0 full: {root}/index_files/{buildID}/{indexVersion}/{partID}/{segID}
// v1 full: {root}/index_v1/{collID}/{partID}/{segID}/{buildID}/{indexVersion}
func (b *IndexPathBuilder) BuildFullPrefix(rootPath string) string {
	return path.Join(rootPath, b.BuildLogicalPrefix())
}

func BuildFullIndexFilePath(rootPath, logicalPath string) string {
	return path.Join(rootPath, logicalPath)
}

func IndexFileKeysFromLogicalPaths(indexFilePaths []string) []string {
	keys := make([]string, 0, len(indexFilePaths))
	for _, indexFilePath := range indexFilePaths {
		keys = append(keys, path.Base(indexFilePath))
	}
	return keys
}
