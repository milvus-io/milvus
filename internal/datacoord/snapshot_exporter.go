package datacoord

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"golang.org/x/sync/errgroup"

	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func exportSnapshot(
	ctx context.Context,
	sourceCM storage.ChunkManager,
	targetCM storage.ChunkManager,
	copier storage.CrossBucketCopier,
	sourceBucket string,
	targetBucket string,
	snapshot *snapshotstorage.SnapshotData,
	targetPath string,
) (string, error) {
	if snapshot == nil || snapshot.SnapshotInfo == nil {
		return "", merr.WrapErrServiceInternalMsg("snapshot cannot be nil")
	}
	if sourceCM == nil {
		return "", merr.WrapErrServiceInternalMsg("source chunk manager cannot be nil")
	}
	if targetCM == nil {
		return "", merr.WrapErrServiceInternalMsg("target chunk manager cannot be nil")
	}
	if copier == nil {
		return "", merr.WrapErrServiceInternalMsg("cross-bucket copier cannot be nil")
	}
	if err := snapshotstorage.ValidateSnapshotObjectPathForBucket(targetCM, "target_s3_path", targetPath, targetBucket); err != nil {
		return "", err
	}
	targetRoot := strings.TrimSuffix(snapshotstorage.NormalizeSnapshotObjectPath(targetPath), "/")
	if targetRoot == "" {
		return "", merr.WrapErrParameterMissingMsg("target_s3_path cannot be empty")
	}

	refs, err := snapshotstorage.ListSnapshotDataFiles(
		ctx,
		sourceCM,
		snapshot,
		nil,
	)
	if err != nil {
		return "", err
	}
	_, metadataObjectPath := snapshotstorage.GetSnapshotPaths(
		targetRoot,
		snapshot.SnapshotInfo.GetCollectionId(),
		snapshot.SnapshotInfo.GetId(),
	)
	metadataURI := metadataObjectPath
	parsedTarget, parseErr := url.Parse(targetPath)
	if parseErr == nil && parsedTarget.Scheme != "" && parsedTarget.Host != "" {
		metadataURI, err = url.JoinPath(targetPath,
			snapshotstorage.SnapshotRootPath,
			fmt.Sprintf("%d", snapshot.SnapshotInfo.GetCollectionId()),
			snapshotstorage.SnapshotMetadataSubPath,
			fmt.Sprintf("%d.json", snapshot.SnapshotInfo.GetId()))
		if err != nil {
			return "", merr.WrapErrServiceInternalErr(err, "failed to build snapshot metadata URI")
		}
	}
	mappings := make(map[string]string, len(refs)*2)
	for _, ref := range refs {
		dst := snapshotstorage.ExportedSnapshotPath(sourceCM, ref.NormalizedPath, targetRoot)
		// Metadata may store either the original URI string or the chunk-manager
		// object key. Record both forms so the rewrite phase is independent of
		// how a referenced snapshot was originally written.
		mappings[ref.Path] = dst
		mappings[ref.NormalizedPath] = dst
	}
	if strings.TrimSpace(sourceBucket) == strings.TrimSpace(targetBucket) {
		if err := rejectExportObjectOverlap(snapshot, refs, mappings, targetRoot, metadataURI); err != nil {
			return "", err
		}
	}
	copyGroup, copyCtx := errgroup.WithContext(ctx)
	copyGroup.SetLimit(Params.DataCoordCfg.SnapshotExportCopyConcurrency.GetAsInt())
	for _, ref := range refs {
		if ref.Type == snapshotstorage.SnapshotFileTypeStorageV3ManifestRoot {
			// Prefix references are rewrite anchors only. They do not correspond
			// to a concrete object that can be copied.
			continue
		}
		src := ref.NormalizedPath
		dst := mappings[ref.NormalizedPath]
		copyGroup.Go(func() error {
			if err := copier.CopyCrossBucket(copyCtx, sourceBucket, src, targetBucket, dst); err != nil {
				return merr.Wrapf(err, "failed to copy snapshot file from %s to %s", src, dst)
			}
			return nil
		})
	}
	if err := copyGroup.Wait(); err != nil {
		return "", err
	}

	// Keep metadata under targetRoot/snapshots/... so RestoreExternalSnapshot can
	// derive the bundle root without adding another API parameter.
	rewritten, err := snapshotstorage.RewriteSnapshotWithMapping(snapshot, mappings, targetRoot, metadataURI)
	if err != nil {
		return "", err
	}
	// Data objects can be shared by multiple exported snapshots under one root.
	// Metadata is written last, so a failed attempt leaves only unreachable or
	// retryable objects; deleting them could corrupt an older published bundle.
	if _, err := snapshotstorage.NewSnapshotWriter(targetCM).SaveToRoot(
		ctx,
		rewritten,
		targetRoot,
		datapb.SnapshotLayout_SnapshotLayoutSelfContained,
	); err != nil {
		return "", err
	}
	mlog.Info(ctx, "export snapshot completed",
		mlog.String("snapshotName", snapshot.SnapshotInfo.GetName()),
		mlog.String("snapshotMetadataURI", snapshotstorage.RedactSnapshotObjectPath(metadataURI)))
	return metadataURI, nil
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
