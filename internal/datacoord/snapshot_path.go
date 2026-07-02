package datacoord

import (
	"fmt"
	"net/url"
	"path"
	"strings"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/snapshotstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

type bucketChunkManager interface {
	BucketName() string
}

func normalizeSnapshotObjectPath(cm storage.ChunkManager, objectPath string) string {
	if objectPath == "" {
		return ""
	}
	parsed, err := url.Parse(objectPath)
	if err != nil || parsed.Scheme == "" {
		return objectPath
	}
	_, key, _, err := snapshotstorage.ParseForeignURI(objectPath)
	if err != nil {
		return objectPath
	}
	return key
}

func validateSnapshotObjectPath(cm storage.ChunkManager, fieldName string, objectPath string) error {
	expectedBucket := ""
	if bucketCM, ok := cm.(bucketChunkManager); ok {
		expectedBucket = bucketCM.BucketName()
	}
	return validateSnapshotObjectPathForBucket(cm, fieldName, objectPath, expectedBucket)
}

func validateSnapshotObjectPathShape(fieldName string, objectPath string) (string, error) {
	if objectPath == "" {
		return "", fmt.Errorf("%s is required", fieldName)
	}
	parsed, err := url.Parse(objectPath)
	if err != nil {
		return "", fmt.Errorf("%s is invalid: %w", fieldName, err)
	}
	if parsed.Scheme == "" {
		return "", nil
	}
	if parsed.User != nil {
		return "", fmt.Errorf("%s must not embed credentials in the URI", fieldName)
	}
	if !isSupportedSnapshotURIScheme(parsed.Scheme) {
		return "", fmt.Errorf("%s must be an object key or supported snapshot URI", fieldName)
	}
	if parsed.Host == "" {
		return "", fmt.Errorf("%s URI must include a bucket or endpoint host", fieldName)
	}
	bucket, _, _, err := snapshotstorage.ParseForeignURI(objectPath)
	if err != nil {
		return "", fmt.Errorf("%s is invalid: %w", fieldName, err)
	}
	return bucket, nil
}

func validateSnapshotObjectPathForBucket(cm storage.ChunkManager, fieldName string, objectPath string, expectedBucket string) error {
	actualBucket, err := validateSnapshotObjectPathShape(fieldName, objectPath)
	if err != nil {
		return err
	}
	if actualBucket == "" {
		return nil
	}
	bucket := strings.TrimSpace(expectedBucket)
	if bucket == "" {
		if bucketCM, ok := cm.(bucketChunkManager); ok {
			bucket = bucketCM.BucketName()
		}
	}
	if bucket != "" && actualBucket != bucket {
		return fmt.Errorf("%s bucket %q does not match configured bucket %q", fieldName, actualBucket, bucket)
	}
	return nil
}

func isSupportedSnapshotURIScheme(scheme string) bool {
	switch strings.ToLower(scheme) {
	case "s3", "minio", "gs", "gcs", "az", "azure", "https":
		return true
	default:
		return false
	}
}

func redactSnapshotObjectPath(objectPath string) string {
	parsed, err := url.Parse(objectPath)
	if err != nil {
		return "<invalid-uri>"
	}
	if parsed.User == nil {
		return objectPath
	}
	parsed.User = url.User("redacted")
	return parsed.String()
}

func joinSnapshotURI(base string, elems ...string) string {
	if base == "" {
		return path.Join(elems...)
	}
	parsed, err := url.Parse(base)
	if err == nil && parsed.Scheme != "" {
		parts := append([]string{strings.TrimPrefix(parsed.Path, "/")}, elems...)
		joined := path.Join(parts...)
		if joined == "." {
			joined = ""
		}
		parsed.Path = "/" + joined
		return parsed.String()
	}
	parts := append([]string{base}, elems...)
	return path.Join(parts...)
}

func validateSelfContainedSnapshotMetadata(
	cm storage.ChunkManager,
	metadataFilePath string,
	metadata *datapb.SnapshotMetadata,
	segments []*datapb.SegmentDescription,
) error {
	root := deriveSnapshotRootPath(metadataFilePath)
	if root == "" {
		return fmt.Errorf("cannot derive snapshot root from metadata path %q", metadataFilePath)
	}
	root = normalizeSnapshotObjectPath(cm, root)

	checkPath := func(filePath string) error {
		if filePath == "" {
			return nil
		}
		normalized := normalizeSnapshotObjectPath(cm, filePath)
		if isSnapshotPathUnderRoot(normalized, root) {
			return nil
		}
		return fmt.Errorf("path %q is outside snapshot root %q", filePath, root)
	}

	for _, manifestPath := range metadata.GetManifestList() {
		if err := checkPath(manifestPath); err != nil {
			return err
		}
	}
	for _, manifest := range metadata.GetStoragev2ManifestList() {
		if err := checkManifestPath(manifest.GetManifest(), checkPath); err != nil {
			return err
		}
	}
	for _, segment := range segments {
		if err := checkSegmentSnapshotPaths(segment, checkPath); err != nil {
			return err
		}
	}
	return nil
}

func isSnapshotPathUnderRoot(filePath, root string) bool {
	cleanPath := path.Clean(filePath)
	cleanRoot := path.Clean(root)
	if cleanRoot == "." || cleanRoot == "/" {
		return true
	}
	return cleanPath == cleanRoot || strings.HasPrefix(cleanPath, cleanRoot+"/")
}

func checkManifestPath(manifestPath string, checkPath func(string) error) error {
	if manifestPath == "" {
		return nil
	}
	basePath, _, err := packed.UnmarshalManifestPath(manifestPath)
	if err != nil {
		return fmt.Errorf("failed to parse manifest path: %w", err)
	}
	return checkPath(basePath)
}

func checkSegmentSnapshotPaths(segment *datapb.SegmentDescription, checkPath func(string) error) error {
	for _, fieldBinlog := range segment.GetBinlogs() {
		if err := checkFieldBinlogs(fieldBinlog, checkPath); err != nil {
			return err
		}
	}
	for _, fieldBinlog := range segment.GetStatslogs() {
		if err := checkFieldBinlogs(fieldBinlog, checkPath); err != nil {
			return err
		}
	}
	for _, fieldBinlog := range segment.GetDeltalogs() {
		if err := checkFieldBinlogs(fieldBinlog, checkPath); err != nil {
			return err
		}
	}
	for _, fieldBinlog := range segment.GetBm25Statslogs() {
		if err := checkFieldBinlogs(fieldBinlog, checkPath); err != nil {
			return err
		}
	}
	for _, indexFile := range segment.GetIndexFiles() {
		for _, filePath := range indexFile.GetIndexFilePaths() {
			if err := checkPath(filePath); err != nil {
				return err
			}
		}
	}
	for _, textIndex := range segment.GetTextIndexFiles() {
		for _, filePath := range textIndex.GetFiles() {
			if err := checkPath(filePath); err != nil {
				return err
			}
		}
	}
	for _, jsonKeyIndex := range segment.GetJsonKeyIndexFiles() {
		for _, filePath := range jsonKeyIndex.GetFiles() {
			if err := checkPath(filePath); err != nil {
				return err
			}
		}
	}
	return checkManifestPath(segment.GetManifestPath(), checkPath)
}

func checkFieldBinlogs(fieldBinlog *datapb.FieldBinlog, checkPath func(string) error) error {
	for _, binlog := range fieldBinlog.GetBinlogs() {
		if err := checkPath(binlog.GetLogPath()); err != nil {
			return err
		}
	}
	return nil
}
