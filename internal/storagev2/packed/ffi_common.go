package packed

/*
#cgo pkg-config: milvus_core milvus-storage
#include <stdlib.h>
#include "milvus-storage/ffi_c.h"
#include "storage/loon_ffi/ffi_writer_c.h"
#include "storage/loon_ffi/external_spec_c.h"
#include "arrow/c/abi.h"
#include "arrow/c/helpers.h"
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"unsafe"

	_ "github.com/milvus-io/milvus/internal/util/cgo"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	snapshotformat "github.com/milvus-io/milvus/pkg/v3/snapshotio/format"
	"github.com/milvus-io/milvus/pkg/v3/storagev2"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Preserve sentinel identity for existing retry guards after snapshot extraction.
var (
	ErrLoonTransient = objectstorage.ErrLoonTransient
	ErrLoonPermanent = objectstorage.ErrLoonPermanent
)

// Property keys exported by milvus-storage/ffi_c.h.
var (
	PropertyFSAddress             = C.GoString(C.loon_properties_fs_address)
	PropertyFSBucketName          = C.GoString(C.loon_properties_fs_bucket_name)
	PropertyFSAccessKeyID         = C.GoString(C.loon_properties_fs_access_key_id)
	PropertyFSAccessKeyValue      = C.GoString(C.loon_properties_fs_access_key_value)
	PropertyFSRootPath            = C.GoString(C.loon_properties_fs_root_path)
	PropertyFSStorageType         = C.GoString(C.loon_properties_fs_storage_type)
	PropertyFSCloudProvider       = C.GoString(C.loon_properties_fs_cloud_provider)
	PropertyFSIAMEndpoint         = C.GoString(C.loon_properties_fs_iam_endpoint)
	PropertyFSLogLevel            = C.GoString(C.loon_properties_fs_log_level)
	PropertyFSRegion              = C.GoString(C.loon_properties_fs_region)
	PropertyFSUseSSL              = C.GoString(C.loon_properties_fs_use_ssl)
	PropertyFSSSLCACert           = C.GoString(C.loon_properties_fs_ssl_ca_cert)
	PropertyFSUseIAM              = C.GoString(C.loon_properties_fs_use_iam)
	PropertyFSUseVirtualHost      = C.GoString(C.loon_properties_fs_use_virtual_host)
	PropertyFSRequestTimeoutMS    = C.GoString(C.loon_properties_fs_request_timeout_ms)
	PropertyFSGCPCredentialJSON   = C.GoString(C.loon_properties_fs_gcp_credential_json)
	PropertyFSUseCustomPartUpload = C.GoString(C.loon_properties_fs_use_custom_part_upload)
	PropertyFSMaxConnections      = C.GoString(C.loon_properties_fs_max_connections)
	PropertyFSTLSMinVersion       = C.GoString(C.loon_properties_fs_tls_min_version)
	PropertyFSUseCRC32CChecksum   = C.GoString(C.loon_properties_fs_use_crc32c_checksum)

	PropertyWriterPolicy             = C.GoString(C.loon_properties_writer_policy)
	PropertyWriterFormat             = C.GoString(C.loon_properties_writer_format)
	PropertyWriterSchemaBasedPattern = C.GoString(C.loon_properties_writer_schema_base_patterns)
	PropertyWriterSchemaBasedFormats = "writer.split.schema_based.formats"

	// CMEK (Customer Managed Encryption Keys) writer properties
	PropertyWriterEncEnable = C.GoString(C.loon_properties_writer_enc_enable)    // Enable encryption for written data
	PropertyWriterEncKey    = C.GoString(C.loon_properties_writer_enc_key)       // Encryption key for data encryption
	PropertyWriterEncMeta   = C.GoString(C.loon_properties_writer_enc_meta)      // Encoded metadata containing zone ID, collection ID, and key version
	PropertyWriterEncAlgo   = C.GoString(C.loon_properties_writer_enc_algorithm) // Encryption algorithm (e.g., "AES_GCM_V1")
)

func writerEncryptionProperties(storagePluginContext *indexcgopb.StoragePluginContext) (map[string]string, error) {
	if storagePluginContext == nil {
		return nil, nil
	}

	var cKey *C.char
	var cMeta *C.char
	encKey := C.CString(storagePluginContext.EncryptionKey)
	defer C.free(unsafe.Pointer(encKey))

	pluginContext := C.CPluginContext{
		ez_id:         C.int64_t(storagePluginContext.EncryptionZoneId),
		collection_id: C.int64_t(storagePluginContext.CollectionId),
		key:           encKey,
	}
	status := C.GetEncParams(&pluginContext, &cKey, &cMeta)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return nil, err
	}
	defer C.free(unsafe.Pointer(cKey))
	defer C.free(unsafe.Pointer(cMeta))

	return map[string]string{
		PropertyWriterEncEnable: "true",
		PropertyWriterEncKey:    C.GoString(cKey),
		PropertyWriterEncMeta:   C.GoString(cMeta),
		PropertyWriterEncAlgo:   "AES_GCM_V1",
	}, nil
}

// ExtfsPrefixForCollection returns the per-collection extfs property prefix.
func ExtfsPrefixForCollection(collectionID int64) string {
	return fmt.Sprintf("extfs.%d.", collectionID)
}

// MakePropertiesFromStorageConfig creates a Properties object from StorageConfig.
// All configuration fields are mapped to corresponding property key-value pairs,
// with the local filesystem root normalized separately from the key prefix.
func MakePropertiesFromStorageConfig(storageConfig *indexpb.StorageConfig, extraKVs map[string]string) (*C.LoonProperties, error) {
	if storageConfig == nil {
		return nil, merr.WrapErrStorageMsg("storageConfig is required")
	}

	// Common filesystem semantics are shared with snapshot/backup native I/O.
	propertiesMap := storagev2.StorageProperties(storageConfig)
	propertiesMap[PropertyWriterFormat] = paramtable.Get().DataNodeCfg.StorageFormat.GetValue()
	externalMode := uint32(0)
	if storageConfig.GetTalonEnableForExternalTable() {
		externalMode = storageConfig.GetTalonMode()
	}
	propertiesMap["milvus.talon.external_mode"] = strconv.FormatUint(uint64(externalMode), 10)
	// Caller overrides remain last, including server-only properties.
	for key, value := range extraKVs {
		propertiesMap[key] = value
	}
	keys := make([]string, 0, len(propertiesMap))
	values := make([]string, 0, len(propertiesMap))
	for key, value := range propertiesMap {
		keys = append(keys, key)
		values = append(values, value)
	}

	// Convert to C arrays
	cKeys := make([]*C.char, len(keys))
	cValues := make([]*C.char, len(values))
	for i := range keys {
		cKeys[i] = C.CString(keys[i])
		cValues[i] = C.CString(values[i])
	}
	// Defer cleanup of all C strings
	defer func() {
		for i := range cKeys {
			C.free(unsafe.Pointer(cKeys[i]))
			C.free(unsafe.Pointer(cValues[i]))
		}
	}()

	// Create Properties using FFI
	properties := &C.LoonProperties{}
	var cKeysPtr **C.char
	var cValuesPtr **C.char
	if len(cKeys) > 0 {
		cKeysPtr = &cKeys[0]
		cValuesPtr = &cValues[0]
	}

	result := C.loon_properties_create(
		(**C.char)(unsafe.Pointer(cKeysPtr)),
		(**C.char)(unsafe.Pointer(cValuesPtr)),
		C.size_t(len(keys)),
		properties,
	)

	err := HandleLoonFFIResult(result)
	if err != nil {
		return nil, merr.WrapErrStorage(err, "loon properties_create failed")
	}
	return properties, nil
}

// FreeProperties releases a C-allocated LoonProperties object.
func FreeProperties(props *C.LoonProperties) {
	if props != nil {
		C.loon_properties_free(props)
	}
}

// MilvusTablePrimaryKeyMode describes whether a milvus-table target segment
// keeps source primary keys or uses target-generated virtual primary keys.
type MilvusTablePrimaryKeyMode int

const (
	// MilvusTablePrimaryKeyModeUnspecified keeps the legacy real-PK behavior for
	// callers that do not know the collection schema.
	MilvusTablePrimaryKeyModeUnspecified MilvusTablePrimaryKeyMode = iota
	// MilvusTablePrimaryKeyModeExternal means source primary keys are preserved.
	MilvusTablePrimaryKeyModeExternal
	// MilvusTablePrimaryKeyModeVirtual means DataNode generates virtual PKs.
	MilvusTablePrimaryKeyModeVirtual
)

func (m MilvusTablePrimaryKeyMode) usesExternalPrimaryKey() bool {
	return m != MilvusTablePrimaryKeyModeVirtual
}

// ExternalSpecContext carries the raw external-table inputs that C++
// InjectExternalSpecProperties needs to derive both extfs.{collectionID}.*
// (storage layer) and format-layer properties (e.g. reader.exttable.snapshot_id)
// from a single external_spec JSON. Zero value (CollectionID=0, Source="")
// signals an internal (non-external) collection — injectExternalSpecProperties
// treats it as a no-op.
type ExternalSpecContext struct {
	CollectionID int64
	Source       string
	Spec         string // raw JSON; C++ InjectExternalSpecProperties parses

	// MilvusTablePKMode is only used by the milvus-table format. The zero
	// value keeps the legacy real-PK behavior for direct storage helpers; callers
	// with a collection schema should set this explicitly.
	MilvusTablePKMode MilvusTablePrimaryKeyMode
}

// injectExternalSpecProperties appends External Table filesystem and
// format-layer properties onto an existing LoonProperties via the C++
// InjectExternalSpecProperties pipeline. The process-local IOPS policy is
// applied only to the extfs.<collectionID> namespace. No-op when
// externalSource is empty.
func injectExternalSpecProperties(properties *C.LoonProperties, collectionID int64,
	externalSource, externalSpec string,
) error {
	if properties == nil {
		return merr.WrapErrStorageMsg("injectExternalSpecProperties: properties is nil")
	}
	if externalSource == "" {
		return nil
	}
	params := paramtable.Get()
	cSource := C.CString(externalSource)
	defer C.free(unsafe.Pointer(cSource))
	var cSpec *C.char
	if externalSpec != "" {
		cSpec = C.CString(externalSpec)
		defer C.free(unsafe.Pointer(cSpec))
	}
	result := C.loon_properties_inject_external_spec(
		properties,
		C.int64_t(collectionID),
		cSource,
		cSpec,
		C.uint32_t(params.CommonCfg.StorageIopsInitialRate.GetAsUint32()),
		C.uint32_t(params.CommonCfg.StorageIopsMaxRate.GetAsUint32()),
	)
	if err := HandleLoonFFIResult(result); err != nil {
		return merr.WrapErrStorage(err, "loon inject_external_spec failed")
	}
	return nil
}

func HandleLoonFFIResult(ffiResult C.LoonFFIResult) error {
	defer C.loon_ffi_free_result(&ffiResult)
	if C.loon_ffi_is_success(&ffiResult) == 0 {
		errMsg := C.loon_ffi_get_errmsg(&ffiResult)
		errStr := "Unknown error"
		if errMsg != nil {
			errStr = C.GoString(errMsg)
		}
		// Classify by the err_code the FFI already carries instead of
		// flattening every failure to transient: the producer's own
		// loon_ffi_is_retryable_errcode decides, so a 404/access-denied/
		// corrupt-data failure stops retry loops instead of spinning them.
		code := int32(ffiResult.err_code)
		if C.loon_ffi_is_retryable_errcode(C.int(code)) != 0 {
			return merr.Wrapf(ErrLoonTransient,
				"FFI operation failed (code=%d): %s", code, errStr)
		}
		return merr.Wrapf(ErrLoonPermanent,
			"FFI operation failed (code=%d): %s", code, errStr)
	}
	return nil
}

type ManifestJSON = storagev2.ManifestJSON

func MarshalManifestPath(basePath string, version int64) string {
	return storagev2.MarshalManifestPath(basePath, version)
}

func marshalManifestPath(manifest ManifestJSON) string {
	// Marshaling a struct of a string and integers cannot fail.
	bs, _ := json.Marshal(manifest)
	return string(bs)
}

func UnmarshalManifestPath(manifestPath string) (string, int64, error) {
	return storagev2.UnmarshalManifestPath(manifestPath)
}

// ParseManifestPath decodes a manifest path together with the manifest size it
// may record.
func ParseManifestPath(manifestPath string) (ManifestJSON, error) {
	var manifest ManifestJSON
	err := json.Unmarshal([]byte(manifestPath), &manifest)
	return manifest, err
}

// CommittedManifestPath returns the manifest path of a committed revision,
// recording size, the byte size of its manifest object, so later reads can skip
// asking storage for it. Local storage records no size: a local read learns it
// at no cost.
func CommittedManifestPath(basePath string, version, size int64, storageType string) string {
	manifest := ManifestJSON{ManifestVersion: version, BasePath: basePath}
	if size > 0 && storageType != "local" {
		manifest.Size = size
	}
	return marshalManifestPath(manifest)
}

// openTransaction opens a loon transaction on the revision manifest names. A
// recorded manifest size lets loon read the manifest without first asking
// storage for its size. loon reads exactly that many bytes, so a size that does
// not match the manifest object fails the read.
func openTransaction(manifest ManifestJSON, cProperties *C.LoonProperties, resolveID C.int32_t, retryLimit C.uint32_t) (C.LoonTransactionHandle, error) {
	cBasePath := C.CString(manifest.BasePath)
	defer C.free(unsafe.Pointer(cBasePath))
	var handle C.LoonTransactionHandle
	result := C.loon_transaction_open(cBasePath, cProperties, C.int64_t(manifest.ManifestVersion),
		C.int64_t(manifest.Size), resolveID, retryLimit, &handle)
	return handle, HandleLoonFFIResult(result)
}

// openManifest reads the manifest of the revision manifest names.
func openManifest(manifest ManifestJSON, cProperties *C.LoonProperties) (*C.LoonManifest, error) {
	handle, err := openTransaction(manifest, cProperties, C.LOON_TRANSACTION_RESOLVE_FAIL, C.uint32_t(1) /* retry_limit */)
	if err != nil {
		return nil, err
	}
	defer C.loon_transaction_destroy(handle)
	var cManifest *C.LoonManifest
	if err := HandleLoonFFIResult(C.loon_transaction_get_manifest(handle, &cManifest)); err != nil {
		return nil, err
	}
	return cManifest, nil
}

// commitTransaction commits handle and returns the manifest path of the
// revision it wrote on basePath, recording the size of the manifest object.
func commitTransaction(handle C.LoonTransactionHandle, basePath string, storageConfig *indexpb.StorageConfig) (string, error) {
	var version, size C.int64_t
	if err := HandleLoonFFIResult(C.loon_transaction_commit(handle, &version, &size)); err != nil {
		return "", err
	}
	return CommittedManifestPath(basePath, int64(version), int64(size), storageConfig.GetStorageType()), nil
}

// CompareManifestPath compares two manifest paths by their version.
func CompareManifestPath(a, b string) (int, error) {
	if a == b {
		return 0, nil
	}

	aBase, aVer, aErr := UnmarshalManifestPath(a)
	bBase, bVer, bErr := UnmarshalManifestPath(b)

	if aErr != nil {
		return 0, merr.WrapErrStorage(aErr, "failed to parse manifest path %q", a)
	}
	if bErr != nil {
		return 0, merr.WrapErrStorage(bErr, "failed to parse manifest path %q", b)
	}

	if aBase != bBase {
		return 0, merr.WrapErrServiceInternalMsg("manifest paths have different base paths: %q vs %q", aBase, bBase)
	}

	switch {
	case aVer < bVer:
		return -1, nil
	case aVer > bVer:
		return 1, nil
	default:
		return 0, nil
	}
}

// ManifestPathKey returns the identity of a manifest path: its base path and
// version. Spellings of the same revision (JSON key order, optional fields)
// share one key, so compare and index manifest paths by this key rather than
// by their text. A value that is not a manifest path, such as an external
// table file path, is returned unchanged.
func ManifestPathKey(path string) string {
	// UnmarshalManifestPath accepts leading whitespace too.
	if !strings.HasPrefix(strings.TrimSpace(path), "{") {
		return path
	}
	basePath, version, err := UnmarshalManifestPath(path)
	if err != nil || basePath == "" {
		return path
	}
	return MarshalManifestPath(basePath, version)
}

// SameManifestPath reports whether two manifest paths name the same revision.
func SameManifestPath(a, b string) bool {
	return a == b || ManifestPathKey(a) == ManifestPathKey(b)
}

// LobFileInfo represents metadata for a LOB (Large Object) file.
// used for TEXT column compaction strategy decision (hole ratio calculation)
type LobFileInfo = snapshotformat.LobFileInfo

// AddLobFilesToTransaction adds multiple LOB files to the manifest at
// manifestPath in a single commit and returns the manifest path of the new
// revision. This is used during compaction REUSE_ALL mode to merge LOB file
// references.
func AddLobFilesToTransaction(manifestPath string, storageConfig *indexpb.StorageConfig, lobFiles []LobFileInfo) (string, error) {
	if len(lobFiles) == 0 {
		return manifestPath, nil
	}
	manifest, err := ParseManifestPath(manifestPath)
	if err != nil {
		return "", merr.WrapErrStorage(err, "failed to parse manifest path")
	}

	cProperties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
	if err != nil {
		return "", merr.Wrap(err, "failed to make properties")
	}
	defer C.loon_properties_free(cProperties)

	cTransactionHandle, err := openTransaction(manifest, cProperties, C.LOON_TRANSACTION_RESOLVE_FAIL, C.uint32_t(1) /* retry_limit */)
	if err != nil {
		return "", merr.WrapErrStorage(err, "failed to begin transaction")
	}
	defer C.loon_transaction_destroy(cTransactionHandle)

	// add all LOB files
	for _, lobFile := range lobFiles {
		cPath := C.CString(lobFile.Path)

		cLobFile := C.LoonLobFileInfo{
			path:            cPath,
			field_id:        C.int64_t(lobFile.FieldID),
			total_rows:      C.int64_t(lobFile.TotalRows),
			valid_rows:      C.int64_t(lobFile.ValidRows),
			file_size_bytes: C.int64_t(lobFile.FileSizeBytes),
		}

		result := C.loon_transaction_add_lob_file(cTransactionHandle, &cLobFile)
		C.free(unsafe.Pointer(cPath))

		if err := HandleLoonFFIResult(result); err != nil {
			return "", merr.WrapErrStorage(err, "failed to add LOB file %s", lobFile.Path)
		}
	}

	newManifestPath, err := commitTransaction(cTransactionHandle, manifest.BasePath, storageConfig)
	if err != nil {
		return "", merr.WrapErrStorage(err, "failed to commit transaction")
	}
	return newManifestPath, nil
}

// GetManifestLobFiles retrieves LOB file information from a manifest.
// this is used by compaction to calculate hole ratios for TEXT columns.
func GetManifestLobFiles(manifestPath string, storageConfig *indexpb.StorageConfig) ([]LobFileInfo, error) {
	cManifest, err := GetManifestHandle(manifestPath, storageConfig)
	if err != nil {
		return nil, merr.WrapErrStorage(err, "failed to read manifest")
	}
	defer C.loon_manifest_destroy(cManifest)

	return manifestLobFiles(cManifest), nil
}

func manifestLobFiles(cManifest *C.LoonManifest) []LobFileInfo {
	// extract LOB files from manifest
	numFiles := int(cManifest.lob_files.num_files)
	lobFiles := make([]LobFileInfo, 0, numFiles)

	if numFiles > 0 && cManifest.lob_files.files != nil {
		// convert C array to Go slice
		cFiles := unsafe.Slice(cManifest.lob_files.files, numFiles)
		for _, cFile := range cFiles {
			lobFiles = append(lobFiles, LobFileInfo{
				Path:          C.GoString(cFile.path),
				FieldID:       int64(cFile.field_id),
				TotalRows:     int64(cFile.total_rows),
				ValidRows:     int64(cFile.valid_rows),
				FileSizeBytes: int64(cFile.file_size_bytes),
			})
		}
	}

	return lobFiles
}
