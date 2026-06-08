package datacoord

import (
	"context"
	"fmt"
	"strings"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

const exportedSnapshotFilesPath = "files"

type snapshotExporter struct {
	sourceCM     storage.ChunkManager
	targetCM     storage.ChunkManager
	copier       storage.CrossBucketCopier
	sourceBucket string
	targetBucket string
}

func newSnapshotExporter(
	sourceCM storage.ChunkManager,
	targetCM storage.ChunkManager,
	copier storage.CrossBucketCopier,
	sourceBucket string,
	targetBucket string,
) *snapshotExporter {
	return &snapshotExporter{
		sourceCM:     sourceCM,
		targetCM:     targetCM,
		copier:       copier,
		sourceBucket: sourceBucket,
		targetBucket: targetBucket,
	}
}

func (e *snapshotExporter) Export(ctx context.Context, snapshot *SnapshotData, targetPath string) (string, error) {
	if snapshot == nil || snapshot.SnapshotInfo == nil {
		return "", fmt.Errorf("snapshot cannot be nil")
	}
	if e.sourceCM == nil {
		return "", fmt.Errorf("source chunk manager cannot be nil")
	}
	if e.targetCM == nil {
		return "", fmt.Errorf("target chunk manager cannot be nil")
	}
	if err := validateSnapshotObjectPathForBucket(e.targetCM, "target_s3_path", targetPath, e.targetBucket); err != nil {
		return "", err
	}
	targetRoot := strings.TrimSuffix(normalizeSnapshotObjectPath(e.targetCM, targetPath), "/")
	if targetRoot == "" {
		return "", fmt.Errorf("target_s3_path cannot be empty")
	}

	refs, err := ListSnapshotDataFiles(ctx, e.sourceCM, snapshot)
	if err != nil {
		return "", err
	}
	mappings := make(map[string]string, len(refs)*2)
	for _, ref := range refs {
		dst := exportedSnapshotPath(e.sourceCM, ref.NormalizedPath, targetRoot)
		mappings[ref.Path] = dst
		mappings[ref.NormalizedPath] = dst
	}

	for _, ref := range refs {
		if ref.Kind != SnapshotFileRefKindObject {
			continue
		}
		src := ref.NormalizedPath
		dst := mappings[ref.NormalizedPath]
		if src == dst {
			continue
		}
		if e.copier == nil {
			return "", fmt.Errorf("cross-bucket copier cannot be nil")
		}
		if err := e.copier.CopyCrossBucket(ctx, e.sourceBucket, src, e.targetBucket, dst); err != nil {
			return "", fmt.Errorf("failed to copy snapshot file from %s to %s: %w", src, dst, err)
		}
	}

	metadataURI := joinSnapshotURI(targetPath,
		SnapshotRootPath,
		fmt.Sprintf("%d", snapshot.SnapshotInfo.GetCollectionId()),
		SnapshotMetadataSubPath,
		fmt.Sprintf("%d.json", snapshot.SnapshotInfo.GetId()))
	writtenURI, err := WriteSnapshotWithMapping(ctx, e.targetCM, snapshot, mappings, SnapshotRewriteOptions{
		TargetRoot:    targetRoot,
		MetadataURI:   metadataURI,
		StrictMapping: true,
	})
	if err != nil {
		return "", err
	}
	mlog.Info(ctx, "export snapshot completed",
		zapSnapshotName(snapshot.SnapshotInfo.GetName()),
		zapSnapshotMetadataURI(writtenURI))
	return writtenURI, nil
}

func zapSnapshotName(name string) zap.Field {
	return zap.String("snapshotName", name)
}

func zapSnapshotMetadataURI(uri string) zap.Field {
	return zap.String("snapshotMetadataURI", redactSnapshotObjectPath(uri))
}
