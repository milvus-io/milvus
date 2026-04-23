package packed

/*
#cgo pkg-config: milvus_core milvus-storage
#include <stdlib.h>
#include "milvus-storage/ffi_c.h"
#include "arrow/c/abi.h"
#include "arrow/c/helpers.h"
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"net/url"
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

// ensureHTTPScheme prepends "http://" to address when SSL is disabled and no scheme is present.
// Lance's BuildEndpointUrl defaults to HTTPS when no scheme is present.
// Prepend http:// when SSL is not enabled so that Lance sets allow_http=true.
func ensureHTTPScheme(address string, useSSL bool) string {
	if !useSSL && !strings.Contains(address, "://") {
		return "http://" + address
	}
	return address
}

// ExtfsPrefixForCollection returns the extfs property prefix for a specific collection.
// Each external collection uses its own namespace to avoid conflicts.
func ExtfsPrefixForCollection(collectionID int64) string {
	return fmt.Sprintf("extfs.%d.", collectionID)
}

// BuildExtfsOverrides builds a complete set of extfs properties for an external collection.
// It always copies the baseline from storageConfig so that C++ resolve_config can match
// the extfs group by address+bucket (even for same-bucket relative paths, because C++
// explore returns full URIs like aws://host:port/bucket/key).
// For cross-bucket URIs, it overrides bucket/address from the URI.
// Finally, user-specified extfs overrides from ExternalSpec take highest priority.
func BuildExtfsOverrides(externalSource string, storageConfig *indexpb.StorageConfig,
	extfsPrefix string, specExtfs map[string]string,
) map[string]string {
	overrides := make(map[string]string)

	// Always copy baseline from storageConfig so the per-collection extfs group
	// is self-contained (C++ resolve_config treats each group independently).
	copyStorageConfigToExtfs(overrides, extfsPrefix, storageConfig)

	// For cross-bucket URIs, override bucket and address from the URI.
	u, err := url.Parse(externalSource)
	if err == nil && u.Scheme != "" {
		// Override bucket from the URI path (first component after leading /).
		path := strings.TrimPrefix(u.Path, "/")
		parts := strings.SplitN(path, "/", 2)
		if len(parts) > 0 && parts[0] != "" {
			overrides[extfsPrefix+"bucket_name"] = parts[0]
		}

		// Override address from the URI host.
		if u.Host != "" {
			overrides[extfsPrefix+"address"] = ensureHTTPScheme(u.Host, storageConfig.GetUseSSL())
		}
	}

	// Apply spec extfs overrides (highest priority, keys already prefixed by caller).
	for k, v := range specExtfs {
		overrides[k] = v
	}

	return overrides
}

// copyStorageConfigToExtfs copies all relevant storage config fields to the extfs override map.
func copyStorageConfigToExtfs(overrides map[string]string, prefix string, cfg *indexpb.StorageConfig) {
	if cfg.GetStorageType() != "" {
		overrides[prefix+"storage_type"] = cfg.GetStorageType()
	}
	if cfg.GetBucketName() != "" {
		overrides[prefix+"bucket_name"] = cfg.GetBucketName()
	}
	if cfg.GetAddress() != "" {
		overrides[prefix+"address"] = ensureHTTPScheme(cfg.GetAddress(), cfg.GetUseSSL())
	}
	if cfg.GetRootPath() != "" {
		overrides[prefix+"root_path"] = cfg.GetRootPath()
	}
	if cfg.GetAccessKeyID() != "" {
		overrides[prefix+"access_key_id"] = cfg.GetAccessKeyID()
	}
	if cfg.GetSecretAccessKey() != "" {
		overrides[prefix+"access_key_value"] = cfg.GetSecretAccessKey()
	}
	if cfg.GetCloudProvider() != "" {
		overrides[prefix+"cloud_provider"] = cfg.GetCloudProvider()
	}
	if cfg.GetIAMEndpoint() != "" {
		overrides[prefix+"iam_endpoint"] = cfg.GetIAMEndpoint()
	}
	if cfg.GetRegion() != "" {
		overrides[prefix+"region"] = cfg.GetRegion()
	}
	if cfg.GetSslCACert() != "" {
		overrides[prefix+"ssl_ca_cert"] = cfg.GetSslCACert()
	}
	if cfg.GetGcpCredentialJSON() != "" {
		overrides[prefix+"gcp_credential_json"] = cfg.GetGcpCredentialJSON()
	}
	if cfg.GetUseSSL() {
		overrides[prefix+"use_ssl"] = "true"
	} else {
		overrides[prefix+"use_ssl"] = "false"
	}
	if cfg.GetUseIAM() {
		overrides[prefix+"use_iam"] = "true"
	} else {
		overrides[prefix+"use_iam"] = "false"
	}
	if cfg.GetUseVirtualHost() {
		overrides[prefix+"use_virtual_host"] = "true"
	} else {
		overrides[prefix+"use_virtual_host"] = "false"
	}
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
	// (extfs.{collectionID}.*) are passed via extraKVs by BuildExtfsOverrides.

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
