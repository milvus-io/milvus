package metautil

import (
	"path"
	"strings"

	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

// IndexPathBuilder constructs object storage paths for index files.
// All index file path construction MUST go through this builder.
// The builder reads IndexStorePathVersion from metadata to decide the path format,
// providing compile-time safety — callers cannot forget to pass the version.
type IndexPathBuilder struct {
	rootPath     string
	pathVersion  indexpb.IndexStorePathVersion
	collID       int64
	partID       int64
	segID        int64
	buildID      int64
	indexVersion int64
	segmentBase  string
}

// NewIndexPathBuilder creates a builder for constructing index file paths.
// pathVersion: 0 = build-rooted, 1 = collection-rooted, 2 = segment-rooted.
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

func IsCollectionRooted(pathVersion indexpb.IndexStorePathVersion) bool {
	return pathVersion == indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED
}

func IsSegmentRooted(pathVersion indexpb.IndexStorePathVersion) bool {
	return pathVersion == indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED
}

// WithManifestPath keeps segment-rooted artifacts under the existing manifest,
// including legacy local segments whose base differs from today's namespace.
// Other path versions retain their original independent layout.
func (b *IndexPathBuilder) WithManifestPath(manifestPath string) *IndexPathBuilder {
	if base, ok := getManifestBasePath(manifestPath); ok {
		b.segmentBase = base
	}
	return b
}

// BuildFilePath returns the full path for a single index file.
func (b *IndexPathBuilder) BuildFilePath(fileKey string) string {
	if IsSegmentRooted(b.pathVersion) {
		return b.BuildPrefix() + "/" + fileKey
	}
	return path.Join(b.BuildPrefix(), fileKey)
}

// BuildFilePaths returns full paths for multiple index files.
func (b *IndexPathBuilder) BuildFilePaths(fileKeys []string) []string {
	paths := make([]string, 0, len(fileKeys))
	for _, fileKey := range fileKeys {
		paths = append(paths, b.BuildFilePath(fileKey))
	}
	return paths
}

// BuildPrefix returns the directory prefix containing all files for this index build.
// v0: {root}/index_files/{buildID}/{indexVersion}/{partID}/{segID}
// v1: {root}/index_v1/{collID}/{partID}/{segID}/{buildID}/{indexVersion}
// v2: {root}/insert_log/{collID}/{partID}/{segID}/_index/{buildID}/{indexVersion}
func (b *IndexPathBuilder) BuildPrefix() string {
	if IsSegmentRooted(b.pathVersion) {
		if b.segmentBase != "" {
			return strings.TrimRight(b.segmentBase, "/") + "/_index/" + JoinIDPath(b.buildID, b.indexVersion)
		}
		return path.Join(b.rootPath, common.SegmentInsertLogPath,
			JoinIDPath(b.collID, b.partID, b.segID), "_index", JoinIDPath(b.buildID, b.indexVersion))
	}
	// Preserve the historical fallback for unknown future versions here;
	// IsCollectionRooted itself identifies only the v1 layout.
	if b.pathVersion >= indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED {
		k := JoinIDPath(b.collID, b.partID, b.segID, b.buildID, b.indexVersion)
		return path.Join(b.rootPath, common.SegmentIndexV1Path, k)
	}
	k := JoinIDPath(b.buildID, b.indexVersion, b.partID, b.segID)
	return path.Join(b.rootPath, common.SegmentIndexV0Path, k)
}
