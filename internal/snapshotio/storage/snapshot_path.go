package storage

import (
	"net/url"
	"path"
	"strings"

	milvusstorage "github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type bucketChunkManager interface {
	BucketName() string
}

// NormalizeSnapshotObjectPath converts snapshot URIs to chunk-manager object
// keys so validation and copy code can compare URI and object-key references.
func NormalizeSnapshotObjectPath(objectPath string) string {
	if objectPath == "" {
		return ""
	}
	if !hasURITransportScheme(objectPath) {
		return objectPath
	}
	parsed, err := url.Parse(objectPath)
	if err != nil || parsed.Scheme == "" {
		return objectPath
	}
	_, key, _, err := ParseForeignURI(objectPath)
	if err != nil {
		return objectPath
	}
	return key
}

func validateSnapshotObjectPathShape(fieldName string, objectPath string) (string, error) {
	if objectPath == "" {
		return "", merr.WrapErrParameterMissingMsg("%s is required", fieldName)
	}
	if !hasURITransportScheme(objectPath) {
		if err := validateRawObjectKey(objectPath); err != nil {
			return "", merr.WrapErrParameterInvalidErr(err, "%s is invalid", fieldName)
		}
		return "", nil
	}
	parsed, err := url.Parse(objectPath)
	if err != nil {
		return "", merr.WrapErrParameterInvalidErr(err, "%s is invalid", fieldName)
	}
	if parsed.Scheme == "" {
		return "", nil
	}
	if parsed.User != nil {
		return "", merr.WrapErrParameterInvalidMsg("%s must not embed credentials in the URI", fieldName)
	}
	if parsed.Host == "" {
		return "", merr.WrapErrParameterInvalidMsg("%s URI must include a bucket or endpoint host", fieldName)
	}
	bucket, _, _, err := ParseForeignURI(objectPath)
	if err != nil {
		return "", merr.WrapErrParameterInvalidErr(err, "%s is invalid", fieldName)
	}
	return bucket, nil
}

func ValidateSnapshotObjectPathForBucket(cm milvusstorage.ChunkManager, fieldName string, objectPath string, expectedBucket string) error {
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
		return merr.WrapErrParameterInvalidMsg("%s bucket %q does not match configured bucket %q", fieldName, actualBucket, bucket)
	}
	return nil
}

// RedactSnapshotObjectPath removes embedded URI credentials before logging.
func RedactSnapshotObjectPath(objectPath string) string {
	if !hasURITransportScheme(objectPath) {
		return objectPath
	}
	parsed, err := url.Parse(objectPath)
	if err != nil {
		return "<invalid-uri>"
	}
	if parsed.User != nil {
		parsed.User = url.User("redacted")
	}
	parsed.RawQuery = ""
	parsed.ForceQuery = false
	parsed.Fragment = ""
	return parsed.String()
}

// normalizeSnapshotPathReference rejects credential-bearing URI forms and
// converts complete URIs to object keys while preserving local and relative paths.
func normalizeSnapshotPathReference(filePath string) (string, error) {
	if filePath == "" {
		return "", nil
	}
	if !hasURITransportScheme(filePath) {
		if err := validateRawObjectKey(filePath); err != nil {
			return "", merr.WrapErrDataIntegrityMsg(
				"invalid snapshot object path %q",
				RedactSnapshotObjectPath(filePath),
			)
		}
		return filePath, nil
	}
	_, objectKey, _, err := ParseForeignURI(filePath)
	if err != nil {
		return "", merr.WrapErrDataIntegrityMsg(
			"invalid snapshot object path %q",
			RedactSnapshotObjectPath(filePath),
		)
	}
	return objectKey, nil
}

func validateSnapshotPathReference(filePath string) error {
	_, err := normalizeSnapshotPathReference(filePath)
	return err
}

type snapshotURIIdentity struct{ scheme, bucket, endpoint string }

func parseSnapshotURIIdentity(objectPath string) (snapshotURIIdentity, bool, error) {
	if !hasURITransportScheme(objectPath) {
		return snapshotURIIdentity{}, false, nil
	}
	bucket, _, endpoint, err := ParseForeignURI(objectPath)
	if err != nil {
		return snapshotURIIdentity{}, false, merr.WrapErrDataIntegrityMsg(
			"invalid snapshot object path %q",
			RedactSnapshotObjectPath(objectPath),
		)
	}
	scheme, _, _ := strings.Cut(objectPath, "://")
	return snapshotURIIdentity{
		scheme:   CanonicalForeignScheme(scheme),
		bucket:   bucket,
		endpoint: strings.ToLower(endpoint),
	}, true, nil
}

func validateSnapshotURIIdentity(source snapshotURIIdentity, objectPath string) error {
	identity, completeURI, err := parseSnapshotURIIdentity(objectPath)
	if err != nil || !completeURI {
		return err
	}
	if identity == source {
		return nil
	}
	return merr.WrapErrDataIntegrityMsg(
		"snapshot object URI %q does not match metadata source storage",
		RedactSnapshotObjectPath(objectPath),
	)
}

func ValidateSelfContainedSnapshotMetadata(
	metadataFilePath string,
	metadata *datapb.SnapshotMetadata,
	segments []*datapb.SegmentDescription,
) error {
	// Exported bundles must be closed over their bundle root. A malicious or
	// malformed metadata file must not be able to make restore read files from
	// outside the relocated snapshot bundle.
	root, found := DeriveSnapshotRootPath(metadataFilePath)
	if !found {
		return merr.WrapErrDataIntegrityMsg("cannot derive snapshot root from metadata path %q", metadataFilePath)
	}
	root = NormalizeSnapshotObjectPath(root)
	dataRoot := path.Join(root, ExportedSnapshotFilesPath)

	checkPathUnderRoot := func(filePath, allowedRoot, rootName string) error {
		if filePath == "" {
			return nil
		}
		normalized, err := normalizeSnapshotPathReference(filePath)
		if err != nil {
			return err
		}
		if IsSnapshotPathUnderRoot(normalized, allowedRoot) {
			return nil
		}
		return merr.WrapErrDataIntegrityMsg(
			"path %q is outside %s %q",
			RedactSnapshotObjectPath(filePath),
			rootName,
			allowedRoot,
		)
	}
	checkSnapshotPath := func(filePath string) error {
		return checkPathUnderRoot(filePath, root, "snapshot root")
	}
	checkDataPath := func(filePath string) error {
		return checkPathUnderRoot(filePath, dataRoot, "snapshot data root")
	}

	if err := checkSnapshotMetadataPaths(metadata, checkSnapshotPath, checkDataPath); err != nil {
		return err
	}
	for _, segment := range segments {
		if err := checkSegmentSnapshotPaths(segment, checkDataPath, validateSnapshotPathReference); err != nil {
			return err
		}
	}
	return nil
}

// ValidateExternalSnapshotPaths keeps manifests and data inside the source root.
func ValidateExternalSnapshotPaths(
	metadataFilePath string,
	snapshot *SnapshotData,
	refs []SnapshotFileRef,
) error {
	if snapshot == nil {
		return merr.WrapErrDataIntegrityMsg("snapshot cannot be nil")
	}
	sourceIdentity, completeSourceURI, err := parseSnapshotURIIdentity(metadataFilePath)
	if err != nil {
		return err
	}
	root, found := DeriveSnapshotRootPath(metadataFilePath)
	if !found {
		return merr.WrapErrDataIntegrityMsg("cannot derive snapshot root from metadata path")
	}
	root = NormalizeSnapshotObjectPath(root)
	dataRoot := root
	if snapshot.Layout == datapb.SnapshotLayout_SnapshotLayoutSelfContained {
		dataRoot = path.Join(root, ExportedSnapshotFilesPath)
	}
	for _, manifestPath := range snapshot.ManifestPaths {
		if completeSourceURI {
			if err := validateSnapshotURIIdentity(sourceIdentity, manifestPath); err != nil {
				return err
			}
		}
		normalized, err := normalizeSnapshotPathReference(manifestPath)
		if err != nil {
			return err
		}
		if !IsSnapshotPathUnderRoot(normalized, root) {
			return merr.WrapErrDataIntegrityMsg(
				"snapshot manifest path %q is outside source root %q",
				RedactSnapshotObjectPath(manifestPath),
				root,
			)
		}
	}
	for _, ref := range refs {
		if completeSourceURI {
			if err := validateSnapshotURIIdentity(sourceIdentity, ref.Path); err != nil {
				return err
			}
		}
		if !IsSnapshotPathUnderRoot(ref.NormalizedPath, dataRoot) {
			return merr.WrapErrDataIntegrityMsg(
				"snapshot data path %q is outside source root %q",
				RedactSnapshotObjectPath(ref.Path),
				dataRoot,
			)
		}
	}
	return nil
}

// DeriveSnapshotRootPath returns the bundle root before the terminal
// snapshots/{collectionID}/metadata/{snapshotID}.json anchor. An empty root
// with found=true represents a bundle at the bucket root.
func DeriveSnapshotRootPath(snapshotS3Location string) (root string, found bool) {
	locationPath := snapshotS3Location
	if hasURITransportScheme(snapshotS3Location) {
		parsed, err := url.Parse(snapshotS3Location)
		if err != nil || parsed.Scheme == "" || parsed.Host == "" {
			return "", false
		}
		if _, objectKey, _, parseErr := ParseForeignURI(snapshotS3Location); parseErr == nil {
			locationPath = objectKey
		} else {
			locationPath = strings.TrimPrefix(parsed.Path, "/")
		}
	}

	cleanLocation := path.Clean(locationPath)
	if cleanLocation == "." {
		return "", false
	}

	root, _, _, found = deriveSnapshotBundleAnchor(cleanLocation)
	return root, found
}

// ValidateSnapshotMetadataLocation verifies that an external metadata URI uses
// the canonical terminal anchor and agrees with the IDs declared in metadata.
func ValidateSnapshotMetadataLocation(metadataFilePath string, snapshotInfo *datapb.SnapshotInfo) error {
	normalizedPath, err := normalizeSnapshotPathReference(metadataFilePath)
	if err != nil {
		return err
	}
	_, collectionID, snapshotID, found := deriveSnapshotBundleAnchor(normalizedPath)
	if !found {
		return merr.WrapErrDataIntegrityMsg(
			"snapshot metadata path must end with snapshots/{collectionID}/metadata/{snapshotID}.json",
		)
	}
	if collectionID != snapshotInfo.GetCollectionId() {
		return merr.WrapErrDataIntegrityMsg(
			"snapshot metadata path collection ID %d does not match snapshot_info collection ID %d",
			collectionID,
			snapshotInfo.GetCollectionId(),
		)
	}
	if snapshotID != snapshotInfo.GetId() {
		return merr.WrapErrDataIntegrityMsg(
			"snapshot metadata path snapshot ID %d does not match snapshot_info snapshot ID %d",
			snapshotID,
			snapshotInfo.GetId(),
		)
	}
	return nil
}

func IsSnapshotPathUnderRoot(filePath, root string) bool {
	cleanPath := path.Clean(filePath)
	cleanRoot := path.Clean(root)
	if cleanRoot == "." || cleanRoot == "/" {
		return true
	}
	return cleanPath == cleanRoot || strings.HasPrefix(cleanPath, cleanRoot+"/")
}

func checkSnapshotMetadataPaths(
	metadata *datapb.SnapshotMetadata,
	checkSnapshotPath func(string) error,
	checkDataPath func(string) error,
) error {
	for _, manifestPath := range metadata.GetManifestList() {
		if err := checkSnapshotPath(manifestPath); err != nil {
			return err
		}
	}
	for _, manifest := range metadata.GetStoragev2ManifestList() {
		if err := checkManifestPath(manifest.GetManifest(), checkDataPath); err != nil {
			return err
		}
	}
	return nil
}

func checkManifestPath(manifestPath string, checkPath func(string) error) error {
	if manifestPath == "" {
		return nil
	}
	// StorageV2 stores a plain object path, while StorageV3 packs its base path
	// and version in JSON.
	if !strings.HasPrefix(strings.TrimSpace(manifestPath), "{") {
		return checkPath(manifestPath)
	}
	basePath, _, err := packed.UnmarshalManifestPath(manifestPath)
	if err != nil {
		return merr.WrapErrDataIntegrity(err, "failed to parse manifest path")
	}
	return checkPath(basePath)
}

func checkSegmentSnapshotPaths(
	segment *datapb.SegmentDescription,
	checkPath func(string) error,
	checkManifestOwnedPath func(string) error,
) error {
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
	manifestOwnedPathCheck := checkPath
	if segment.GetStorageVersion() >= milvusstorage.StorageV3 {
		// StorageV3 text/JSON PB paths are placeholders. Validate URI safety, but
		// do not require them to remain under the self-contained bundle root.
		manifestOwnedPathCheck = checkManifestOwnedPath
	}
	for _, textIndex := range segment.GetTextIndexFiles() {
		for _, filePath := range textIndex.GetFiles() {
			if err := manifestOwnedPathCheck(filePath); err != nil {
				return err
			}
		}
	}
	for _, jsonKeyIndex := range segment.GetJsonKeyIndexFiles() {
		for _, filePath := range jsonKeyIndex.GetFiles() {
			if err := manifestOwnedPathCheck(filePath); err != nil {
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
