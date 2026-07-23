// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package storage

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"

	snapshotio "github.com/milvus-io/milvus/internal/snapshotio"
	milvusstorage "github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	SnapshotRootPath         = "snapshots"
	SnapshotMetadataSubPath  = "metadata"
	SnapshotManifestsSubPath = "manifests"
	SnapshotFormatVersion    = snapshotio.SnapshotFormatVersion
)

// SnapshotData is the in-memory form of a stored snapshot.
type SnapshotData struct {
	SnapshotInfo *datapb.SnapshotInfo
	Collection   *datapb.CollectionDescription
	Segments     []*datapb.SegmentDescription
	Indexes      []*indexpb.IndexInfo

	MetadataPath  string
	ManifestPaths []string
	SegmentIDs    []int64
	BuildIDs      []int64
	Layout        datapb.SnapshotLayout
}

// SnapshotWriter writes snapshot metadata and segment manifests.
type SnapshotWriter struct {
	chunkManager milvusstorage.ChunkManager
}

// NewSnapshotWriter creates a snapshot writer.
func NewSnapshotWriter(cm milvusstorage.ChunkManager) *SnapshotWriter {
	return &SnapshotWriter{
		chunkManager: cm,
	}
}

// GetSnapshotPaths returns the manifest directory and metadata path for a snapshot.
func GetSnapshotPaths(rootPath string, collectionID int64, snapshotID int64) (manifestDir, metadataPath string) {
	basePath := path.Join(rootPath, SnapshotRootPath, strconv.FormatInt(collectionID, 10))
	snapshotIDStr := strconv.FormatInt(snapshotID, 10)
	manifestDir = path.Join(basePath, SnapshotManifestsSubPath, snapshotIDStr)
	metadataPath = path.Join(basePath, SnapshotMetadataSubPath, fmt.Sprintf("%s.json", snapshotIDStr))
	return manifestDir, metadataPath
}

// GetSegmentManifestPath returns the path for one segment manifest.
func GetSegmentManifestPath(manifestDir string, segmentID int64) string {
	return path.Join(manifestDir, fmt.Sprintf("%d.avro", segmentID))
}

// Save stores a referenced snapshot under the writer root.
func (w *SnapshotWriter) Save(ctx context.Context, snapshot *SnapshotData) (string, error) {
	return w.save(ctx, snapshot, w.chunkManager.RootPath(), datapb.SnapshotLayout_SnapshotLayoutReferenced)
}

// SaveToRoot saves snapshot data under a caller-provided root path.
func (w *SnapshotWriter) SaveToRoot(ctx context.Context, snapshot *SnapshotData, rootPath string, layout datapb.SnapshotLayout) (string, error) {
	return w.save(ctx, snapshot, rootPath, layout)
}

func (w *SnapshotWriter) save(
	ctx context.Context,
	snapshot *SnapshotData,
	rootPath string,
	layout datapb.SnapshotLayout,
) (string, error) {
	if snapshot == nil {
		return "", merr.WrapErrServiceInternalMsg("snapshot cannot be nil")
	}
	if snapshot.SnapshotInfo == nil {
		return "", merr.WrapErrServiceInternalMsg("snapshot info cannot be nil")
	}
	collectionID := snapshot.SnapshotInfo.GetCollectionId()
	if collectionID <= 0 {
		return "", merr.WrapErrServiceInternalMsg("invalid collection ID: %d", collectionID)
	}
	if snapshot.Collection == nil {
		return "", merr.WrapErrServiceInternalMsg("collection description cannot be nil")
	}

	snapshotID := snapshot.SnapshotInfo.GetId()
	if snapshotID <= 0 {
		return "", merr.WrapErrServiceInternalMsg("invalid snapshot ID: %d", snapshotID)
	}
	if layout == datapb.SnapshotLayout_SnapshotLayoutUnknown {
		layout = datapb.SnapshotLayout_SnapshotLayoutReferenced
	}
	snapshot.Layout = layout
	manifestDir, metadataPath := GetSnapshotPaths(rootPath, collectionID, snapshotID)

	manifestPaths := make([]string, 0, len(snapshot.Segments))
	for _, segment := range snapshot.Segments {
		manifestPath := GetSegmentManifestPath(manifestDir, segment.GetSegmentId())
		if err := w.writeSegmentManifest(ctx, manifestPath, segment); err != nil {
			return "", merr.Wrapf(err, "failed to write manifest for segment %d", segment.GetSegmentId())
		}
		manifestPaths = append(manifestPaths, manifestPath)
	}

	mlog.Info(ctx, "Successfully wrote segment manifest files",
		mlog.Int("numSegments", len(snapshot.Segments)),
		mlog.String("manifestDir", manifestDir))

	storagev2Manifests := make([]*datapb.StorageV2SegmentManifest, 0)
	for _, segment := range snapshot.Segments {
		if segment.GetManifestPath() != "" {
			storagev2Manifests = append(storagev2Manifests, &datapb.StorageV2SegmentManifest{
				SegmentId: segment.GetSegmentId(),
				Manifest:  segment.GetManifestPath(),
			})
		}
	}

	if err := w.writeMetadataFile(ctx, metadataPath, snapshot, manifestPaths, storagev2Manifests); err != nil {
		return "", merr.Wrap(err, "failed to write metadata file")
	}

	mlog.Info(ctx, "Successfully wrote metadata file",
		mlog.String("metadataPath", metadataPath))

	return metadataPath, nil
}

func (w *SnapshotWriter) writeSegmentManifest(ctx context.Context, manifestPath string, segment *datapb.SegmentDescription) error {
	binaryData, err := snapshotio.MarshalSegmentManifest(segment)
	if err != nil {
		return merr.WrapErrServiceInternalErr(err, "failed to marshal segment manifest")
	}
	if err := w.chunkManager.Write(ctx, manifestPath, binaryData); err != nil {
		return merr.Wrap(err, "failed to write segment manifest object")
	}
	return nil
}

func (w *SnapshotWriter) writeMetadataFile(ctx context.Context, metadataPath string, snapshot *SnapshotData, manifestPaths []string, storagev2Manifests []*datapb.StorageV2SegmentManifest) error {
	metadata := &datapb.SnapshotMetadata{
		FormatVersion:         int32(SnapshotFormatVersion),
		SnapshotInfo:          snapshot.SnapshotInfo,
		Collection:            snapshot.Collection,
		Indexes:               snapshot.Indexes,
		ManifestList:          manifestPaths,
		Storagev2ManifestList: storagev2Manifests,
		SegmentIds:            snapshot.SegmentIDs,
		BuildIds:              snapshot.BuildIDs,
		Layout:                snapshot.Layout,
	}

	opts := protojson.MarshalOptions{
		Multiline:       true,
		Indent:          "  ",
		UseProtoNames:   true,
		EmitUnpopulated: false,
	}
	jsonData, err := opts.Marshal(metadata)
	if err != nil {
		return merr.WrapErrServiceInternalErr(err, "failed to marshal metadata to JSON")
	}

	if err := w.chunkManager.Write(ctx, metadataPath, jsonData); err != nil {
		return merr.Wrap(err, "failed to write snapshot metadata object")
	}
	return nil
}

// Drop removes snapshot metadata and manifest files.
func (w *SnapshotWriter) Drop(ctx context.Context, metadataFilePath string) error {
	if metadataFilePath == "" {
		return merr.WrapErrServiceInternalMsg("metadata file path cannot be empty")
	}

	metadata, err := w.readMetadataFile(ctx, metadataFilePath)
	if err != nil {
		return merr.WrapErrServiceInternalErr(err, "failed to read metadata file")
	}

	snapshotID := metadata.GetSnapshotInfo().GetId()

	manifestList := metadata.GetManifestList()
	if len(manifestList) > 0 {
		if err := w.chunkManager.MultiRemove(ctx, manifestList); err != nil {
			return merr.WrapErrServiceInternalErr(err, "failed to remove manifest files")
		}
		mlog.Info(ctx, "Successfully removed manifest files",
			mlog.Int("count", len(manifestList)),
			mlog.Int64("snapshotID", snapshotID))
	}

	if err := w.chunkManager.Remove(ctx, metadataFilePath); err != nil {
		return merr.WrapErrServiceInternalErr(err, "failed to remove metadata file")
	}
	mlog.Info(ctx, "Successfully removed metadata file",
		mlog.String("metadataFilePath", metadataFilePath))

	mlog.Info(ctx, "Successfully dropped snapshot",
		mlog.Int64("snapshotID", snapshotID))
	return nil
}

func (w *SnapshotWriter) readMetadataFile(ctx context.Context, filePath string) (*datapb.SnapshotMetadata, error) {
	data, err := w.chunkManager.Read(ctx, NormalizeSnapshotObjectPath(filePath))
	if err != nil {
		return nil, merr.WrapErrServiceInternalErr(err, "failed to read metadata file")
	}

	return snapshotio.ParseSnapshotMetadata(data)
}

// SnapshotReader reads snapshot metadata and segment manifests.
type SnapshotReader struct {
	chunkManager milvusstorage.ChunkManager
}

// NewSnapshotReader creates a snapshot reader.
func NewSnapshotReader(cm milvusstorage.ChunkManager) *SnapshotReader {
	return &SnapshotReader{
		chunkManager: cm,
	}
}

// ReadSnapshot reads a snapshot by metadata path.
func (r *SnapshotReader) ReadSnapshot(ctx context.Context, metadataFilePath string, includeSegments bool) (*SnapshotData, error) {
	if metadataFilePath == "" {
		return nil, merr.WrapErrServiceInternalMsg("metadata file path cannot be empty")
	}
	normalizedMetadataPath, err := normalizeSnapshotPathReference(metadataFilePath)
	if err != nil {
		return nil, err
	}

	metadata, err := r.readMetadataFile(ctx, normalizedMetadataPath)
	if err != nil {
		return nil, merr.Wrap(err, "failed to read metadata file")
	}
	if metadata.GetSnapshotInfo() == nil {
		return nil, merr.WrapErrDataIntegrityMsg("invalid snapshot metadata: snapshot info cannot be nil")
	}
	if metadata.GetCollection() == nil {
		return nil, merr.WrapErrDataIntegrityMsg("invalid snapshot metadata: collection cannot be nil")
	}
	layout := metadata.GetLayout()
	if layout == datapb.SnapshotLayout_SnapshotLayoutUnknown {
		// Metadata written before layout was introduced is the referenced layout:
		// manifests point at the original Milvus files instead of an exported bundle.
		layout = datapb.SnapshotLayout_SnapshotLayoutReferenced
	}

	var oldRoot, newRoot string
	shouldRebase := false
	if layout == datapb.SnapshotLayout_SnapshotLayoutSelfContained {
		// A self-contained bundle can be moved to a new root as long as the
		// snapshots/.../metadata/... anchor and the bundle-internal layout remain
		// unchanged. First rebase metadata manifest paths before loading segments.
		var newRootFound bool
		newRoot, newRootFound = DeriveSnapshotRootPath(metadataFilePath)
		if !newRootFound {
			return nil, merr.WrapErrDataIntegrityMsg("invalid self-contained snapshot: cannot derive snapshot root from metadata path %q", metadataFilePath)
		}
		var oldRootFound bool
		oldRoot, oldRootFound = DeriveSnapshotRootPath(metadata.GetSnapshotInfo().GetS3Location())
		shouldRebase = oldRootFound && oldRoot != newRoot
		if shouldRebase {
			if err := RebaseSelfContainedSnapshotMetadata(metadata, oldRoot, newRoot); err != nil {
				return nil, merr.Wrap(err, "failed to rebase snapshot metadata")
			}
		}
	}
	if err := checkSnapshotMetadataPaths(
		metadata,
		validateSnapshotPathReference,
		validateSnapshotPathReference,
	); err != nil {
		return nil, err
	}

	var allSegments []*datapb.SegmentDescription
	if includeSegments {
		for _, manifestPath := range metadata.GetManifestList() {
			segment, err := r.readManifestFile(ctx, manifestPath, int(metadata.GetFormatVersion()))
			if err != nil {
				return nil, merr.Wrapf(err, "failed to read manifest file %s", manifestPath)
			}
			allSegments = append(allSegments, segment)
		}

		if err := validateSnapshotSegmentIDs(metadata.GetSegmentIds(), allSegments); err != nil {
			return nil, err
		}
		if err := applyStorageManifestPaths(metadata.GetStoragev2ManifestList(), allSegments); err != nil {
			return nil, err
		}
		for _, segment := range allSegments {
			if err := checkSegmentSnapshotPaths(segment, validateSnapshotPathReference, validateSnapshotPathReference); err != nil {
				return nil, err
			}
		}
	}

	snapshotData := &SnapshotData{
		SnapshotInfo:  metadata.GetSnapshotInfo(),
		Collection:    metadata.GetCollection(),
		Segments:      allSegments,
		Indexes:       metadata.GetIndexes(),
		MetadataPath:  metadataFilePath,
		ManifestPaths: append([]string(nil), metadata.GetManifestList()...),
		SegmentIDs:    metadata.GetSegmentIds(),
		BuildIDs:      metadata.GetBuildIds(),
		Layout:        layout,
	}

	if layout == datapb.SnapshotLayout_SnapshotLayoutSelfContained {
		if shouldRebase {
			// Segment manifests may contain data/index paths as well, so rebase
			// them after the manifest files have been read.
			if err := RebaseSelfContainedSnapshotData(snapshotData, oldRoot, newRoot); err != nil {
				return nil, merr.Wrap(err, "failed to rebase snapshot data")
			}
		}
		// Treat the metadata URI used by this read as the source of truth. The
		// original S3Location may point to the pre-relocation bundle root.
		snapshotData.SnapshotInfo.S3Location = metadataFilePath
		if err := ValidateSelfContainedSnapshotMetadata(metadataFilePath, metadata, snapshotData.Segments); err != nil {
			return nil, merr.Wrap(err, "invalid self-contained snapshot")
		}
	}

	return snapshotData, nil
}

func applyStorageManifestPaths(
	manifestMappings []*datapb.StorageV2SegmentManifest,
	segments []*datapb.SegmentDescription,
) error {
	segmentsByID := make(map[int64]*datapb.SegmentDescription, len(segments))
	for _, segment := range segments {
		segmentsByID[segment.GetSegmentId()] = segment
	}
	seen := make(map[int64]struct{}, len(manifestMappings))
	for index, mapping := range manifestMappings {
		if mapping == nil {
			return merr.WrapErrDataIntegrityMsg("storage manifest mapping at index %d cannot be nil", index)
		}
		segmentID := mapping.GetSegmentId()
		if strings.TrimSpace(mapping.GetManifest()) == "" {
			return merr.WrapErrDataIntegrityMsg("storage manifest mapping for segment %d cannot be empty", segmentID)
		}
		if _, ok := seen[segmentID]; ok {
			return merr.WrapErrDataIntegrityMsg("duplicate storage manifest mapping for segment %d", segmentID)
		}
		segment, ok := segmentsByID[segmentID]
		if !ok {
			return merr.WrapErrDataIntegrityMsg("storage manifest mapping references unknown segment %d", segmentID)
		}
		seen[segmentID] = struct{}{}
		segment.ManifestPath = mapping.GetManifest()
	}
	return nil
}

func validateSnapshotSegmentIDs(expected []int64, segments []*datapb.SegmentDescription) error {
	// Older snapshot metadata may omit segment_ids. When present, it is the
	// integrity declaration for the manifest set and must match exactly.
	expectedSet := make(map[int64]struct{}, len(expected))
	for _, segmentID := range expected {
		if _, ok := expectedSet[segmentID]; ok {
			return merr.WrapErrDataIntegrityMsg("snapshot segment IDs contain duplicate segment %d", segmentID)
		}
		expectedSet[segmentID] = struct{}{}
	}

	loadedSet := make(map[int64]struct{}, len(segments))
	for index, segment := range segments {
		if segment == nil {
			return merr.WrapErrDataIntegrityMsg("snapshot manifest at index %d produced a nil segment", index)
		}
		segmentID := segment.GetSegmentId()
		if _, ok := loadedSet[segmentID]; ok {
			return merr.WrapErrDataIntegrityMsg("snapshot segment IDs do not match manifests: duplicate manifest for segment %d", segmentID)
		}
		loadedSet[segmentID] = struct{}{}
	}
	if len(expected) == 0 {
		return nil
	}

	for _, segmentID := range expected {
		if _, ok := loadedSet[segmentID]; !ok {
			return merr.WrapErrDataIntegrityMsg("snapshot segment IDs do not match manifests: missing segment %d", segmentID)
		}
	}
	for _, segment := range segments {
		if _, ok := expectedSet[segment.GetSegmentId()]; !ok {
			return merr.WrapErrDataIntegrityMsg("snapshot segment IDs do not match manifests: unexpected segment %d", segment.GetSegmentId())
		}
	}
	return nil
}

func (r *SnapshotReader) readMetadataFile(ctx context.Context, filePath string) (*datapb.SnapshotMetadata, error) {
	data, err := r.chunkManager.Read(ctx, NormalizeSnapshotObjectPath(filePath))
	if err != nil {
		return nil, merr.Wrap(err, "failed to read metadata file")
	}

	metadata, err := snapshotio.ParseSnapshotMetadataWithVersionCheck(data)
	if err != nil {
		return nil, merr.WrapErrDataIntegrity(err, "invalid snapshot metadata")
	}
	return metadata, nil
}

func (r *SnapshotReader) readManifestFile(ctx context.Context, filePath string, formatVersion int) (*datapb.SegmentDescription, error) {
	data, err := r.chunkManager.Read(ctx, NormalizeSnapshotObjectPath(filePath))
	if err != nil {
		return nil, merr.Wrap(err, "failed to read manifest file")
	}

	segment, err := snapshotio.ParseSegmentManifest(data, formatVersion)
	if err != nil {
		return nil, merr.WrapErrDataIntegrity(err, "invalid snapshot segment manifest")
	}
	return segment, nil
}

// ListSnapshots lists stored snapshot metadata for a collection.
func (r *SnapshotReader) ListSnapshots(ctx context.Context, collectionID int64) ([]*datapb.SnapshotInfo, error) {
	if collectionID <= 0 {
		return nil, merr.WrapErrServiceInternalMsg("invalid collection ID: %d", collectionID)
	}

	basePath := path.Join(SnapshotRootPath, strconv.FormatInt(collectionID, 10))
	metadataDir := path.Join(basePath, SnapshotMetadataSubPath)

	files, _, err := milvusstorage.ListAllChunkWithPrefix(ctx, r.chunkManager, metadataDir, false)
	if err != nil {
		return nil, merr.Wrap(milvusstorage.ToMilvusIoError(metadataDir, err), "failed to list metadata files")
	}

	var snapshots []*datapb.SnapshotInfo
	for _, file := range files {
		if !strings.HasSuffix(file, ".json") {
			continue
		}

		metadata, err := r.readMetadataFile(ctx, file)
		if err != nil {
			mlog.Warn(ctx, "Failed to parse metadata file, skipping",
				mlog.String("file", file),
				mlog.Err(err))
			continue
		}

		snapshots = append(snapshots, metadata.GetSnapshotInfo())
	}

	return snapshots, nil
}
