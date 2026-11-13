// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"context"
	"path"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/snapshotio"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type SnapshotExportPlan struct {
	TargetRoot       string
	MetadataURI      string
	ManifestPaths    []string
	StorageManifests []*datapb.StorageV2SegmentManifest
}

// buildSnapshotExportFiles validates all copy/write destinations before execution.
// Avro destinations are rewritten from loaded records; DataCoord owns metadata.
func buildSnapshotExportFiles(
	ctx context.Context,
	sourceCM storage.ChunkManager,
	targetCM storage.ChunkManager,
	sourceBucket string,
	targetBucket string,
	snapshot *SnapshotData,
	targetPath string,
	targetStorageConfig *indexpb.StorageConfig,
	sourceStorageConfig *indexpb.StorageConfig,
	permits chan struct{},
) ([]FileCopy, error) {
	if snapshot == nil || snapshot.SnapshotInfo == nil {
		return nil, merr.WrapErrServiceInternalMsg("snapshot cannot be nil")
	}
	if snapshot.Collection == nil {
		return nil, merr.WrapErrDataIntegrityMsg("snapshot collection cannot be nil")
	}
	for i, segment := range snapshot.Segments {
		if segment == nil {
			return nil, merr.WrapErrDataIntegrityMsg("snapshot segment at index %d cannot be nil", i)
		}
	}
	for i, index := range snapshot.Indexes {
		if index == nil {
			return nil, merr.WrapErrDataIntegrityMsg("snapshot index at index %d cannot be nil", i)
		}
	}
	if sourceCM == nil {
		return nil, merr.WrapErrServiceInternalMsg("source chunk manager cannot be nil")
	}
	if targetCM == nil {
		return nil, merr.WrapErrServiceInternalMsg("target chunk manager cannot be nil")
	}
	// External collection manifests can reference lake fragments outside the
	// snapshot file set, so they cannot yet form a self-contained export bundle.
	if typeutil.IsExternalCollection(snapshot.Collection.GetSchema()) {
		return nil, merr.WrapErrParameterInvalidMsg("exporting external collections is not supported")
	}
	if err := ValidateSnapshotObjectPathForBucket(targetCM, "target_s3_path", targetPath, targetBucket); err != nil {
		return nil, err
	}
	targetRoot := strings.TrimSuffix(NormalizeSnapshotObjectPath(targetPath), "/")
	if targetRoot == "" {
		return nil, merr.WrapErrParameterMissingMsg("target_s3_path cannot be empty")
	}

	_, metadataObjectPath := GetSnapshotPaths(
		targetRoot,
		snapshot.SnapshotInfo.GetCollectionId(),
		snapshot.SnapshotInfo.GetId(),
	)
	_, err := BuildStorageConfigSnapshotURI(targetStorageConfig, metadataObjectPath)
	if err != nil {
		return nil, merr.Wrap(err, "failed to build snapshot metadata URI")
	}
	sourceRoot, err := snapshotExportSourceRoot(snapshot, sourceCM.RootPath())
	if err != nil {
		return nil, err
	}
	if sourceStorageConfig == nil {
		sourceStorageConfig = compaction.CreateStorageConfig()
	}
	refs, err := listSnapshotExportFiles(ctx, sourceCM, snapshot, sourceStorageConfig, sourceRoot, targetRoot, sourceBucket, permits)
	if err != nil {
		return nil, err
	}
	if len(snapshot.ManifestPaths) != len(snapshot.Segments) {
		return nil, merr.WrapErrDataIntegrityMsg("snapshot manifest count does not match segment count")
	}
	sameBucket := strings.TrimSpace(sourceBucket) == strings.TrimSpace(targetBucket)
	items := make([]FileCopy, 0, len(refs)+len(snapshot.ManifestPaths))
	for _, ref := range refs {
		dst := exportedSnapshotPath(sourceRoot, ref.NormalizedPath, targetRoot)
		if ref.Type != SnapshotFileTypeStorageV3ManifestRoot {
			items = append(items, FileCopy{
				SourcePath:      ref.NormalizedPath,
				DestinationPath: dst,
				IsMetadata:      ref.Type == SnapshotFileTypeStorageV2Manifest || ref.Type == SnapshotFileTypeStorageV3ManifestFile,
			})
		}
	}
	if sameBucket {
		if err := rejectExportObjectOverlap(snapshot, refs, sourceRoot, targetRoot, metadataObjectPath); err != nil {
			return nil, err
		}
	}
	manifestDir, _ := GetSnapshotPaths(targetRoot, snapshot.SnapshotInfo.GetCollectionId(), snapshot.SnapshotInfo.GetId())
	for i, src := range snapshot.ManifestPaths {
		if err := ValidateSnapshotObjectPathForBucket(sourceCM, "snapshot manifest", src, sourceBucket); err != nil {
			return nil, err
		}
		dst := GetSegmentManifestPath(manifestDir, snapshot.Segments[i].GetSegmentId())
		items = append(items, FileCopy{SourcePath: NormalizeSnapshotObjectPath(src), DestinationPath: dst, IsMetadata: true})
	}
	// Include the Avro manifests as well as data/index files in the shared
	// destination check. A conflicting plan must fail before any provider write.
	items, err = DeduplicateFileCopies(items)
	if err != nil {
		return nil, err
	}

	return items, nil
}

// prepareSnapshotExportMetadata validates and stages the final metadata.
func prepareSnapshotExportMetadata(
	ctx context.Context,
	targetCM storage.ChunkManager,
	snapshot *SnapshotData,
	plan *SnapshotExportPlan,
) error {
	if plan == nil || snapshot == nil || snapshot.SnapshotInfo == nil || snapshot.Collection == nil {
		return merr.WrapErrServiceInternalMsg("snapshot export plan and metadata are required")
	}
	info := proto.Clone(snapshot.SnapshotInfo).(*datapb.SnapshotInfo)
	info.PinIds, info.PinExpireAtMs = nil, nil
	info.S3Location = plan.MetadataURI
	metadata := &datapb.SnapshotMetadata{
		FormatVersion:         SnapshotFormatVersion,
		SnapshotInfo:          info,
		Collection:            snapshot.Collection,
		Indexes:               snapshot.Indexes,
		ManifestList:          plan.ManifestPaths,
		Storagev2ManifestList: plan.StorageManifests,
		SegmentIds:            snapshot.SegmentIDs,
		BuildIds:              snapshot.BuildIDs,
		Layout:                datapb.SnapshotLayout_SnapshotLayoutSelfContained,
	}
	return NewSnapshotWriter(targetCM).PrepareMetadataWithStaging(ctx, metadata, plan.MetadataURI, GetSnapshotStagingMetadataPath(plan.TargetRoot))
}

func rejectExportObjectOverlap(
	snapshot *SnapshotData,
	refs []SnapshotFileRef,
	sourceRoot string,
	targetRoot string,
	metadataURI string,
) error {
	sourceObjects := make(map[string]struct{}, len(refs)+len(snapshot.ManifestPaths)+1)
	addSource := func(objectPath string) {
		if normalized := NormalizeSnapshotObjectPath(objectPath); normalized != "" {
			sourceObjects[normalized] = struct{}{}
		}
	}
	addSource(snapshot.MetadataPath)
	if snapshot.MetadataPath == "" {
		addSource(snapshot.SnapshotInfo.GetS3Location())
	}
	for _, manifestPath := range snapshot.ManifestPaths {
		addSource(manifestPath)
	}
	for _, ref := range refs {
		if ref.Type != SnapshotFileTypeStorageV3ManifestRoot {
			addSource(ref.NormalizedPath)
		}
	}

	checkTarget := func(objectPath string) error {
		normalized := NormalizeSnapshotObjectPath(objectPath)
		if normalized == "" {
			return nil
		}
		if _, ok := sourceObjects[normalized]; ok {
			return merr.WrapErrParameterInvalidMsg(
				"export target object %q overlaps source snapshot object",
				normalized,
			)
		}
		return nil
	}
	if err := checkTarget(metadataURI); err != nil {
		return err
	}
	manifestDir, _ := GetSnapshotPaths(
		targetRoot,
		snapshot.SnapshotInfo.GetCollectionId(),
		snapshot.SnapshotInfo.GetId(),
	)
	for _, segment := range snapshot.Segments {
		if err := checkTarget(GetSegmentManifestPath(manifestDir, segment.GetSegmentId())); err != nil {
			return err
		}
	}
	for _, ref := range refs {
		if err := checkTarget(exportedSnapshotPath(sourceRoot, ref.NormalizedPath, targetRoot)); err != nil {
			return err
		}
	}
	return nil
}

// File planning, Avro rewriting and coordinator finalization use the same root.
// Loaded self-contained paths are already rebased to the current bundle.
func snapshotExportSourceRoot(snapshot *SnapshotData, sourceRoot string) (string, error) {
	if snapshot.Layout == datapb.SnapshotLayout_SnapshotLayoutSelfContained {
		bundleRoot, found := DeriveSnapshotRootPath(snapshot.MetadataPath)
		if !found {
			return "", merr.WrapErrDataIntegrityMsg("self-contained export source has no bundle root")
		}
		return path.Join(bundleRoot, ExportedSnapshotFilesPath), nil
	}
	return sourceRoot, nil
}

// marshalSnapshotExportManifest materializes target paths in the upstream V6 OCF
// schema. Clone only active segments so source records and cached snapshots stay
// unchanged; native manifests retain their relative references and copied bytes.
func marshalSnapshotExportManifest(source *datapb.SegmentDescription, sourceRoot, targetRoot string) ([]byte, error) {
	segment := proto.Clone(source).(*datapb.SegmentDescription)
	includeManifestOwned := segment.GetStorageVersion() < storage.StorageV3
	if !includeManifestOwned {
		segment.Binlogs = nil // V3 insert paths are legacy placeholders, not files.
		// The V5 flag cannot represent an unknown pre-V5 index section.
		// Older Restore must inspect and retract inherited native indexes,
		// rather than interpret the missing source flag as proven empty.
		if segment.ManifestHasIndex == nil {
			segment.ManifestHasIndex = proto.Bool(true)
		}
	}
	if err := rewriteSegmentFilePaths(segment, includeManifestOwned, includeManifestOwned, func(src, _ string) (string, error) {
		if src == "" {
			return "", nil
		}
		normalized, err := normalizeSnapshotPathReference(src)
		if err != nil {
			return "", err
		}
		return exportedSnapshotPath(sourceRoot, normalized, targetRoot), nil
	}); err != nil {
		return nil, err
	}
	data, err := snapshotio.MarshalSegmentManifest(segment)
	if err != nil {
		return nil, merr.Wrap(err, "failed to encode exported snapshot manifest")
	}
	return data, nil
}
