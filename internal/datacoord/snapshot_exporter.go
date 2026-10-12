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

package datacoord

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"hash"
	"net/url"
	"strconv"
	"strings"

	"golang.org/x/sync/errgroup"

	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const snapshotExportPlanVersion int32 = 1

type snapshotExportPlanItem struct {
	sourcePath      string
	destinationPath string
	fileType        snapshotstorage.SnapshotFileType
	sourceSize      int64
}

type snapshotExportPlan struct {
	version             int32
	fingerprint         string
	snapshotFingerprint string
	targetRoot          string
	metadataURI         string
	mappings            map[string]string
	items               []snapshotExportPlanItem
	dataBytes           int64
}

func buildSnapshotExportPlan(
	ctx context.Context,
	sourceCM storage.ChunkManager,
	targetCM storage.ChunkManager,
	sourceBucket string,
	targetBucket string,
	snapshot *snapshotstorage.SnapshotData,
	targetPath string,
	targetStorageConfig *indexpb.StorageConfig,
) (*snapshotExportPlan, error) {
	if snapshot == nil || snapshot.SnapshotInfo == nil {
		return nil, merr.WrapErrServiceInternalMsg("snapshot cannot be nil")
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
	if err := snapshotstorage.ValidateSnapshotObjectPathForBucket(targetCM, "target_s3_path", targetPath, targetBucket); err != nil {
		return nil, err
	}
	targetRoot := strings.TrimSuffix(snapshotstorage.NormalizeSnapshotObjectPath(targetPath), "/")
	if targetRoot == "" {
		return nil, merr.WrapErrParameterMissingMsg("target_s3_path cannot be empty")
	}

	refs, err := snapshotstorage.ListSnapshotDataFiles(
		ctx,
		sourceCM,
		snapshot,
		nil,
	)
	if err != nil {
		return nil, err
	}
	_, metadataObjectPath := snapshotstorage.GetSnapshotPaths(
		targetRoot,
		snapshot.SnapshotInfo.GetCollectionId(),
		snapshot.SnapshotInfo.GetId(),
	)
	metadataURI, err := snapshotstorage.BuildStorageConfigSnapshotURI(targetStorageConfig, metadataObjectPath)
	if err != nil {
		return nil, merr.Wrap(err, "failed to build snapshot metadata URI")
	}
	mappings := make(map[string]string, len(refs)*2)
	items := make([]snapshotExportPlanItem, 0, len(refs))
	for _, ref := range refs {
		dst := snapshotstorage.ExportedSnapshotPath(sourceCM, ref.NormalizedPath, targetRoot)
		// Metadata may store either the original URI string or the chunk-manager
		// object key. Record both forms so the rewrite phase is independent of
		// how a referenced snapshot was originally written.
		mappings[ref.Path] = dst
		mappings[ref.NormalizedPath] = dst
		if ref.Type != snapshotstorage.SnapshotFileTypeStorageV3ManifestRoot {
			items = append(items, snapshotExportPlanItem{
				sourcePath:      ref.NormalizedPath,
				destinationPath: dst,
				fileType:        ref.Type,
			})
		}
	}
	if strings.TrimSpace(sourceBucket) == strings.TrimSpace(targetBucket) {
		if err := rejectExportObjectOverlap(snapshot, refs, mappings, targetRoot, metadataObjectPath); err != nil {
			return nil, err
		}
	}
	dataBytes, err := populateSnapshotExportPlanSizes(
		ctx,
		sourceCM,
		items,
		Params.DataCoordCfg.SnapshotExportCopyConcurrency.GetAsInt(),
	)
	if err != nil {
		return nil, err
	}
	snapshotFingerprint, err := snapshotstorage.SnapshotFingerprint(snapshot)
	if err != nil {
		return nil, merr.Wrap(err, "failed to fingerprint source snapshot")
	}
	fingerprint := fingerprintSnapshotExportPlan(
		snapshotExportPlanVersion,
		snapshotFingerprint,
		normalizeSnapshotExportTargetIdentity(targetPath, targetBucket, targetRoot),
		items,
	)
	return &snapshotExportPlan{
		version:             snapshotExportPlanVersion,
		fingerprint:         fingerprint,
		snapshotFingerprint: snapshotFingerprint,
		targetRoot:          targetRoot,
		metadataURI:         metadataURI,
		mappings:            mappings,
		items:               items,
		dataBytes:           dataBytes,
	}, nil
}

func populateSnapshotExportPlanSizes(
	ctx context.Context,
	sourceCM storage.ChunkManager,
	items []snapshotExportPlanItem,
	concurrency int,
) (int64, error) {
	if sourceCM == nil {
		return 0, merr.WrapErrServiceInternalMsg("source chunk manager cannot be nil")
	}
	if concurrency <= 0 {
		return 0, merr.WrapErrServiceInternalMsg("snapshot export size concurrency must be positive")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(concurrency)
	for index := range items {
		group.Go(func() error {
			size, err := sourceCM.Size(groupCtx, items[index].sourcePath)
			if err != nil {
				return merr.Wrapf(err, "failed to get snapshot source object size for %s", items[index].sourcePath)
			}
			if size < 0 {
				return merr.WrapErrDataIntegrityMsg("snapshot source object has negative size: %s", items[index].sourcePath)
			}
			items[index].sourceSize = size
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return 0, err
	}
	var totalBytes int64
	for _, item := range items {
		totalBytes += item.sourceSize
	}
	return totalBytes, nil
}

func copySnapshotExportPlan(
	ctx context.Context,
	copier storage.CrossBucketCopier,
	sourceBucket string,
	targetBucket string,
	items []snapshotExportPlanItem,
	concurrency int,
) error {
	if copier == nil {
		return merr.WrapErrServiceInternalMsg("cross-bucket copier cannot be nil")
	}
	if concurrency <= 0 {
		return merr.WrapErrServiceInternalMsg("snapshot export copy concurrency must be positive")
	}
	copyGroup, copyCtx := errgroup.WithContext(ctx)
	copyGroup.SetLimit(concurrency)
	for _, item := range items {
		src := item.sourcePath
		dst := item.destinationPath
		copyGroup.Go(func() error {
			if err := copier.CopyCrossBucket(copyCtx, sourceBucket, src, targetBucket, dst); err != nil {
				return merr.Wrapf(err, "failed to copy snapshot file from %s to %s", src, dst)
			}
			return nil
		})
	}
	return copyGroup.Wait()
}

func prepareSnapshotExportPlanWithSize(
	ctx context.Context,
	targetCM storage.ChunkManager,
	snapshot *snapshotstorage.SnapshotData,
	plan *snapshotExportPlan,
) (int64, error) {
	if plan == nil {
		return 0, merr.WrapErrServiceInternalMsg("snapshot export plan cannot be nil")
	}
	// Keep metadata under targetRoot/snapshots/... so RestoreExternalSnapshot can
	// derive the bundle root without adding another API parameter.
	rewritten, err := snapshotstorage.RewriteSnapshotWithMapping(snapshot, plan.mappings, plan.targetRoot, plan.metadataURI)
	if err != nil {
		return 0, err
	}
	metadataPath, metadataBytes, err := snapshotstorage.NewSnapshotWriter(targetCM).PrepareToRootWithStaging(
		ctx,
		rewritten,
		plan.targetRoot,
		datapb.SnapshotLayout_SnapshotLayoutSelfContained,
		snapshotstorage.GetSnapshotStagingMetadataPath(plan.targetRoot),
	)
	if err != nil {
		return 0, err
	}
	if metadataPath != snapshotstorage.NormalizeSnapshotObjectPath(plan.metadataURI) {
		return 0, merr.WrapErrDataIntegrityMsg("prepared snapshot metadata path does not match the export plan")
	}
	return plan.dataBytes + metadataBytes, nil
}

func commitSnapshotExportMetadata(
	ctx context.Context,
	targetCM storage.ChunkManager,
	targetRoot string,
	metadataURI string,
) error {
	if targetCM == nil {
		return merr.WrapErrServiceInternalMsg("target chunk manager cannot be nil")
	}
	if strings.TrimSpace(targetRoot) == "" || strings.TrimSpace(metadataURI) == "" {
		return merr.WrapErrServiceInternalMsg("target root and metadata URI are required")
	}
	_, err := snapshotstorage.NewSnapshotWriter(targetCM).CommitStagedMetadata(
		ctx,
		snapshotstorage.GetSnapshotStagingMetadataPath(targetRoot),
		snapshotstorage.NormalizeSnapshotObjectPath(metadataURI),
		metadataURI,
	)
	return err
}

func cleanupSnapshotExportStagingMetadata(
	ctx context.Context,
	targetCM storage.ChunkManager,
	targetRoot string,
) error {
	if targetCM == nil || strings.TrimSpace(targetRoot) == "" {
		return nil
	}
	return targetCM.Remove(ctx, snapshotstorage.GetSnapshotStagingMetadataPath(targetRoot))
}

func normalizeSnapshotExportTargetIdentity(targetPath, targetBucket, targetRoot string) string {
	parsed, err := url.Parse(targetPath)
	if err == nil && parsed.Scheme != "" && parsed.Host != "" {
		return strings.ToLower(parsed.Scheme) + "://" + strings.ToLower(parsed.Host) + "/" + targetRoot + "|" + strings.TrimSpace(targetBucket)
	}
	return strings.TrimSpace(targetBucket) + "/" + targetRoot
}

func fingerprintSnapshotExportPlan(
	version int32,
	snapshotFingerprint string,
	targetIdentity string,
	items []snapshotExportPlanItem,
) string {
	hasher := sha256.New()
	writeExportFingerprintValue(hasher, strconv.FormatInt(int64(version), 10))
	writeExportFingerprintValue(hasher, snapshotFingerprint)
	writeExportFingerprintValue(hasher, targetIdentity)
	for _, item := range items {
		writeExportFingerprintValue(hasher, item.sourcePath)
		writeExportFingerprintValue(hasher, string(item.fileType))
		writeExportFingerprintValue(hasher, item.destinationPath)
		writeExportFingerprintValue(hasher, strconv.FormatInt(item.sourceSize, 10))
	}
	return hex.EncodeToString(hasher.Sum(nil))
}

func writeExportFingerprintValue(hasher hash.Hash, value string) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = hasher.Write(length[:])
	_, _ = hasher.Write([]byte(value))
}

func rejectExportObjectOverlap(
	snapshot *snapshotstorage.SnapshotData,
	refs []snapshotstorage.SnapshotFileRef,
	mappings map[string]string,
	targetRoot string,
	metadataURI string,
) error {
	sourceObjects := make(map[string]struct{}, len(refs)+len(snapshot.ManifestPaths)+1)
	addSource := func(objectPath string) {
		if normalized := snapshotstorage.NormalizeSnapshotObjectPath(objectPath); normalized != "" {
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
		if ref.Type != snapshotstorage.SnapshotFileTypeStorageV3ManifestRoot {
			addSource(ref.NormalizedPath)
		}
	}

	checkTarget := func(objectPath string) error {
		normalized := snapshotstorage.NormalizeSnapshotObjectPath(objectPath)
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
	manifestDir, _ := snapshotstorage.GetSnapshotPaths(
		targetRoot,
		snapshot.SnapshotInfo.GetCollectionId(),
		snapshot.SnapshotInfo.GetId(),
	)
	for _, segment := range snapshot.Segments {
		if err := checkTarget(snapshotstorage.GetSegmentManifestPath(manifestDir, segment.GetSegmentId())); err != nil {
			return err
		}
	}
	for _, dst := range mappings {
		if err := checkTarget(dst); err != nil {
			return err
		}
	}
	return nil
}
