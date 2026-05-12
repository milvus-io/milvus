package metautil

import (
	"path"

	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

// IndexPathBuilder constructs index storage paths. An empty root builds logical paths.
type IndexPathBuilder struct {
	rootPath     string
	pathVersion  indexpb.IndexStorePathVersion
	collID       int64
	partID       int64
	segID        int64
	buildID      int64
	indexVersion int64
}

// NewIndexPathBuilder creates a builder for constructing index file paths.
// pathVersion: 0 = legacy, >= 1 = collection-partitioned.
func NewIndexPathBuilder(rootPath string, pathVersion indexpb.IndexStorePathVersion, collID, partID, segID, buildID, indexVersion int64) *IndexPathBuilder {
	return &IndexPathBuilder{
		rootPath:     rootPath,
		pathVersion:  pathVersion,
		collID:       collID,
		partID:       partID,
		segID:        segID,
		buildID:      buildID,
		indexVersion: indexVersion,
	}
}

// NewLogicalIndexPathBuilder builds paths without storage root.
func NewLogicalIndexPathBuilder(pathVersion indexpb.IndexStorePathVersion, collID, partID, segID, buildID, indexVersion int64) *IndexPathBuilder {
	return NewIndexPathBuilder("", pathVersion, collID, partID, segID, buildID, indexVersion)
}

func IsCollectionRooted(pathVersion indexpb.IndexStorePathVersion) bool {
	return pathVersion >= indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED
}

func (b *IndexPathBuilder) BuildFilePath(fileKey string) string {
	return path.Join(b.BuildPrefix(), fileKey)
}

func (b *IndexPathBuilder) BuildFilePaths(fileKeys []string) []string {
	paths := make([]string, 0, len(fileKeys))
	for _, fileKey := range fileKeys {
		paths = append(paths, b.BuildFilePath(fileKey))
	}
	return paths
}

// v0 rooted: {root}/index_files/{buildID}/{indexVersion}/{partID}/{segID}
// v1 rooted: {root}/index_v1/{collID}/{partID}/{segID}/{buildID}/{indexVersion}
// v0 logical: index_files/{buildID}/{indexVersion}/{partID}/{segID}
// v1 logical: index_v1/{collID}/{partID}/{segID}/{buildID}/{indexVersion}
func (b *IndexPathBuilder) BuildPrefix() string {
	if IsCollectionRooted(b.pathVersion) {
		k := JoinIDPath(b.collID, b.partID, b.segID, b.buildID, b.indexVersion)
		return path.Join(b.rootPath, common.SegmentIndexV1Path, k)
	}
	k := JoinIDPath(b.buildID, b.indexVersion, b.partID, b.segID)
	return path.Join(b.rootPath, common.SegmentIndexV0Path, k)
}
