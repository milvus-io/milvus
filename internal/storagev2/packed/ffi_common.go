package packed

/*
#cgo pkg-config: milvus_core milvus-storage
#include <stdlib.h>
#include "milvus-storage/ffi_c.h"
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

	"github.com/cockroachdb/errors"

	_ "github.com/milvus-io/milvus/internal/util/cgo"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
)

// ErrLoonTransient marks any failure surfaced by the loon FFI layer. Today
// milvus-storage does not expose structured error codes, so callers cannot
// distinguish a recoverable concurrent-transaction conflict from a hard IO
// error. We treat all loon failures as retryable for now and rely on a
// bounded retry budget plus outer error handling to keep the worst case
// finite.
//
// TODO(storage v3): once milvus-storage exposes explicit error codes, narrow
// this sentinel to only the concurrent-transaction case (FailResolver) and
// let other errors propagate immediately as retry.Unrecoverable.
var ErrLoonTransient = errors.New("loon FFI transient error")

// Property keys - matching milvus-storage/properties.h
const (
	PropertyFSAddress             = "fs.address"
	PropertyFSBucketName          = "fs.bucket_name"
	PropertyFSAccessKeyID         = "fs.access_key_id"
	PropertyFSAccessKeyValue      = "fs.access_key_value"
	PropertyFSRootPath            = "fs.root_path"
	PropertyFSStorageType         = "fs.storage_type"
	PropertyFSCloudProvider       = "fs.cloud_provider"
	PropertyFSIAMEndpoint         = "fs.iam_endpoint"
	PropertyFSLogLevel            = "fs.log_level"
	PropertyFSRegion              = "fs.region"
	PropertyFSUseSSL              = "fs.use_ssl"
	PropertyFSSSLCACert           = "fs.ssl_ca_cert"
	PropertyFSUseIAM              = "fs.use_iam"
	PropertyFSUseVirtualHost      = "fs.use_virtual_host"
	PropertyFSRequestTimeoutMS    = "fs.request_timeout_ms"
	PropertyFSGCPCredentialJSON   = "fs.gcp_credential_json"
	PropertyFSUseCustomPartUpload = "fs.use_custom_part_upload"
	PropertyFSMaxConnections      = "fs.max_connections"
	PropertyFSTLSMinVersion       = "fs.tls_min_version"
	PropertyFSUseCRC32CChecksum   = "fs.use_crc32c_checksum"

	PropertyWriterPolicy             = "writer.policy"
	PropertyWriterSchemaBasedPattern = "writer.split.schema_based.patterns"

	// CMEK (Customer Managed Encryption Keys) writer properties
	PropertyWriterEncEnable = "writer.enc.enable"    // Enable encryption for written data
	PropertyWriterEncKey    = "writer.enc.key"       // Encryption key for data encryption
	PropertyWriterEncMeta   = "writer.enc.meta"      // Encoded metadata containing zone ID, collection ID, and key version
	PropertyWriterEncAlgo   = "writer.enc.algorithm" // Encryption algorithm (e.g., "AES_GCM_V1")
)

// ensureHTTPScheme prepends http:// or https:// to a bare address so it stays
// consistent with use_ssl; leaves addresses that already carry a scheme alone.
func ensureHTTPScheme(address string, useSSL bool) string {
	if strings.Contains(address, "://") {
		return address
	}
	if useSSL {
		return "https://" + address
	}
	return "http://" + address
}

// ExtfsPrefixForCollection returns the per-collection extfs property prefix.
func ExtfsPrefixForCollection(collectionID int64) string {
	return fmt.Sprintf("extfs.%d.", collectionID)
}

// MakePropertiesFromStorageConfig creates a Properties object from StorageConfig
// This function converts a StorageConfig structure into a Properties object by
// calling the FFI properties_create function. All configuration fields from
// StorageConfig are mapped to corresponding key-value pairs in Properties.
func MakePropertiesFromStorageConfig(storageConfig *indexpb.StorageConfig, extraKVs map[string]string) (*C.LoonProperties, error) {
	if storageConfig == nil {
		return nil, fmt.Errorf("storageConfig is required")
	}

	// Prepare key-value pairs from StorageConfig
	var keys []string
	var values []string

	// Add non-empty string fields
	if storageConfig.GetAddress() != "" {
		keys = append(keys, PropertyFSAddress)
		values = append(values, ensureHTTPScheme(storageConfig.GetAddress(), storageConfig.GetUseSSL()))
	}
	if storageConfig.GetBucketName() != "" {
		keys = append(keys, PropertyFSBucketName)
		values = append(values, storageConfig.GetBucketName())
	}
	if storageConfig.GetAccessKeyID() != "" {
		keys = append(keys, PropertyFSAccessKeyID)
		values = append(values, storageConfig.GetAccessKeyID())
	}
	if storageConfig.GetSecretAccessKey() != "" {
		keys = append(keys, PropertyFSAccessKeyValue)
		values = append(values, storageConfig.GetSecretAccessKey())
	}
	if storageConfig.GetRootPath() != "" {
		keys = append(keys, PropertyFSRootPath)
		values = append(values, storageConfig.GetRootPath())
	}
	if storageConfig.GetStorageType() != "" {
		keys = append(keys, PropertyFSStorageType)
		values = append(values, storageConfig.GetStorageType())
	}
	if storageConfig.GetCloudProvider() != "" {
		keys = append(keys, PropertyFSCloudProvider)
		values = append(values, storageConfig.GetCloudProvider())
	}
	if storageConfig.GetIAMEndpoint() != "" {
		keys = append(keys, PropertyFSIAMEndpoint)
		values = append(values, storageConfig.GetIAMEndpoint())
	}
	keys = append(keys, PropertyFSLogLevel)
	values = append(values, "warn")

	if storageConfig.GetRegion() != "" {
		keys = append(keys, PropertyFSRegion)
		values = append(values, storageConfig.GetRegion())
	}
	if storageConfig.GetSslCACert() != "" {
		keys = append(keys, PropertyFSSSLCACert)
		values = append(values, storageConfig.GetSslCACert())
	}
	if storageConfig.GetGcpCredentialJSON() != "" {
		keys = append(keys, PropertyFSGCPCredentialJSON)
		values = append(values, storageConfig.GetGcpCredentialJSON())
	}

	// Add boolean fields
	keys = append(keys, PropertyFSUseSSL)
	if storageConfig.GetUseSSL() {
		values = append(values, "true")
	} else {
		values = append(values, "false")
	}

	keys = append(keys, PropertyFSUseIAM)
	if storageConfig.GetUseIAM() {
		values = append(values, "true")
	} else {
		values = append(values, "false")
	}

	keys = append(keys, PropertyFSUseVirtualHost)
	if storageConfig.GetUseVirtualHost() {
		values = append(values, "true")
	} else {
		values = append(values, "false")
	}

	keys = append(keys, PropertyFSUseCustomPartUpload)
	values = append(values, "true") // hardcoded to true as in the original code

	// Add integer field
	keys = append(keys, PropertyFSRequestTimeoutMS)
	values = append(values, strconv.FormatInt(storageConfig.GetRequestTimeoutMs(), 10))

	// Add TLS min version (skip "default" — consistent with C++ layer filtering)
	if v := storageConfig.GetSslTlsMinVersion(); v != "" && v != "default" {
		keys = append(keys, PropertyFSTLSMinVersion)
		values = append(values, v)
	}

	// Add CRC32C checksum
	keys = append(keys, PropertyFSUseCRC32CChecksum)
	if storageConfig.GetUseCrc32CChecksum() {
		values = append(values, "true")
	} else {
		values = append(values, "false")
	}

	// No extfs.default.* properties here. Per-collection extfs properties
	// (extfs.{collectionID}.*) are injected downstream via
	// InjectExternalSpecProperties (C++ InjectExternalSpecProperties pipeline).

	// Add extra kvs (override existing keys if present)
	for k, v := range extraKVs {
		found := false
		for i, existingKey := range keys {
			if existingKey == k {
				values[i] = v
				found = true
				break
			}
		}
		if !found {
			keys = append(keys, k)
			values = append(values, v)
		}
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
		return nil, err
	}
	return properties, nil
}

// FreeProperties releases a C-allocated LoonProperties object.
func FreeProperties(props *C.LoonProperties) {
	if props != nil {
		C.loon_properties_free(props)
	}
}

// ExternalSpecContext carries the raw external-table inputs that C++
// InjectExternalSpecProperties needs to derive both extfs.{collectionID}.*
// (storage layer) and format-layer properties (e.g. iceberg.snapshot_id)
// from a single external_spec JSON. Zero value (CollectionID=0, Source="")
// signals an internal (non-external) collection — injectExternalSpecProperties
// treats it as a no-op.
type ExternalSpecContext struct {
	CollectionID int64
	Source       string
	Spec         string // raw JSON; C++ InjectExternalSpecProperties parses
}

// injectExternalSpecProperties appends every external_spec-derived property
// (extfs.<collectionID>.* and format-layer keys) onto an existing
// LoonProperties via the C++ InjectExternalSpecProperties pipeline. No-op
// when externalSource is empty.
func injectExternalSpecProperties(properties *C.LoonProperties, collectionID int64,
	externalSource, externalSpec string,
) error {
	if properties == nil {
		return fmt.Errorf("injectExternalSpecProperties: properties is nil")
	}
	if externalSource == "" {
		return nil
	}
	cSource := C.CString(externalSource)
	defer C.free(unsafe.Pointer(cSource))
	var cSpec *C.char
	if externalSpec != "" {
		cSpec = C.CString(externalSpec)
		defer C.free(unsafe.Pointer(cSpec))
	}
	result := C.loon_properties_inject_external_spec(
		properties, C.int64_t(collectionID), cSource, cSpec)
	return HandleLoonFFIResult(result)
}

func HandleLoonFFIResult(ffiResult C.LoonFFIResult) error {
	defer C.loon_ffi_free_result(&ffiResult)
	if C.loon_ffi_is_success(&ffiResult) == 0 {
		errMsg := C.loon_ffi_get_errmsg(&ffiResult)
		errStr := "Unknown error"
		if errMsg != nil {
			errStr = C.GoString(errMsg)
		}

		return errors.Wrapf(ErrLoonTransient, "FFI operation failed: %s", errStr)
	}
	return nil
}

type ManifestJSON struct {
	ManifestVersion int64  `json:"ver"`
	BasePath        string `json:"base_path"`
}

func MarshalManifestPath(basePath string, version int64) string {
	bs, err := json.Marshal(ManifestJSON{
		ManifestVersion: version,
		BasePath:        basePath,
	})
	if err != nil {
		// json.Marshal on string+int64 struct should never fail, but log if it does
		return fmt.Sprintf(`{"ver":%d,"base_path":"%s"}`, version, basePath)
	}
	return string(bs)
}

func UnmarshalManifestPath(manifestPath string) (string, int64, error) {
	var manifestJSON ManifestJSON
	err := json.Unmarshal([]byte(manifestPath), &manifestJSON)
	if err != nil {
		return "", 0, err
	}
	return manifestJSON.BasePath, manifestJSON.ManifestVersion, nil
}

// CompareManifestPath compares two manifest paths by their version.
func CompareManifestPath(a, b string) (int, error) {
	if a == b {
		return 0, nil
	}

	aBase, aVer, aErr := UnmarshalManifestPath(a)
	bBase, bVer, bErr := UnmarshalManifestPath(b)

	if aErr != nil {
		return 0, fmt.Errorf("failed to parse manifest path %q: %w", a, aErr)
	}
	if bErr != nil {
		return 0, fmt.Errorf("failed to parse manifest path %q: %w", b, bErr)
	}

	if aBase != bBase {
		return 0, fmt.Errorf("manifest paths have different base paths: %q vs %q", aBase, bBase)
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

// LobFileInfo represents metadata for a LOB (Large Object) file.
// used for TEXT column compaction strategy decision (hole ratio calculation)
type LobFileInfo struct {
	Path          string // relative path to the LOB file
	FieldID       int64  // field ID this LOB file belongs to
	TotalRows     int64  // total number of rows in the LOB file
	ValidRows     int64  // number of valid (non-deleted) rows
	FileSizeBytes int64  // size of the LOB file in bytes
}

// AddLobFilesToTransaction adds multiple LOB files to a transaction in a single commit.
// this is used during compaction REUSE_ALL mode to merge LOB file references.
// returns the new committed version after the transaction.
func AddLobFilesToTransaction(basePath string, version int64, storageConfig *indexpb.StorageConfig, lobFiles []LobFileInfo) (int64, error) {
	if len(lobFiles) == 0 {
		return version, nil
	}

	cProperties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to make properties: %w", err)
	}
	defer C.loon_properties_free(cProperties)

	cBasePath := C.CString(basePath)
	defer C.free(unsafe.Pointer(cBasePath))

	// open transaction
	var cTransactionHandle C.LoonTransactionHandle
	result := C.loon_transaction_begin(cBasePath, cProperties, C.int64_t(version), C.int32_t(0) /* resolve_id */, C.uint32_t(1) /* retry_limit */, &cTransactionHandle)
	if err := HandleLoonFFIResult(result); err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
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

		result = C.loon_transaction_add_lob_file(cTransactionHandle, &cLobFile)
		C.free(unsafe.Pointer(cPath))

		if err := HandleLoonFFIResult(result); err != nil {
			return 0, fmt.Errorf("failed to add LOB file %s: %w", lobFile.Path, err)
		}
	}

	// commit transaction
	var committedVersion C.int64_t
	result = C.loon_transaction_commit(cTransactionHandle, &committedVersion)
	if err := HandleLoonFFIResult(result); err != nil {
		return 0, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return int64(committedVersion), nil
}

// GetManifestLobFiles retrieves LOB file information from a manifest.
// this is used by compaction to calculate hole ratios for TEXT columns.
func GetManifestLobFiles(manifestPath string, storageConfig *indexpb.StorageConfig) ([]LobFileInfo, error) {
	basePath, version, err := UnmarshalManifestPath(manifestPath)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal manifest path: %w", err)
	}

	cProperties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to make properties: %w", err)
	}
	defer C.loon_properties_free(cProperties)

	cBasePath := C.CString(basePath)
	defer C.free(unsafe.Pointer(cBasePath))

	// open transaction to get manifest
	var cTransactionHandle C.LoonTransactionHandle
	result := C.loon_transaction_begin(cBasePath, cProperties, C.int64_t(version), C.int32_t(0) /* resolve_id */, C.uint32_t(1) /* retry_limit */, &cTransactionHandle)
	if err := HandleLoonFFIResult(result); err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer C.loon_transaction_destroy(cTransactionHandle)

	// get manifest
	var cManifest *C.LoonManifest
	result = C.loon_transaction_get_manifest(cTransactionHandle, &cManifest)
	if err := HandleLoonFFIResult(result); err != nil {
		return nil, fmt.Errorf("failed to get manifest: %w", err)
	}
	defer C.loon_manifest_destroy(cManifest)

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

	return lobFiles, nil
}
