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
	"strconv"
	"strings"
	"unsafe"

	_ "github.com/milvus-io/milvus/internal/util/cgo"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
)

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

	PropertyWriterPolicy             = "writer.policy"
	PropertyWriterSchemaBasedPattern = "writer.split.schema_based.patterns"

	// CMEK (Customer Managed Encryption Keys) writer properties
	PropertyWriterEncEnable = "writer.enc.enable"    // Enable encryption for written data
	PropertyWriterEncKey    = "writer.enc.key"       // Encryption key for data encryption
	PropertyWriterEncMeta   = "writer.enc.meta"      // Encoded metadata containing zone ID, collection ID, and key version
	PropertyWriterEncAlgo   = "writer.enc.algorithm" // Encryption algorithm (e.g., "AES_GCM_V1")
)

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
		// Lance's BuildEndpointUrl defaults to HTTPS when no scheme is present.
		// Prepend http:// when SSL is not enabled so that Lance sets allow_http=true.
		fsAddr := storageConfig.GetAddress()
		if !storageConfig.GetUseSSL() && !strings.Contains(fsAddr, "://") {
			fsAddr = "http://" + fsAddr
		}
		values = append(values, fsAddr)
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

	// Add extfs.default.* properties (mirrors string fields from fs.* with extfs.default.* prefix)
	const extfsPrefix = "extfs.default."
	if storageConfig.GetStorageType() != "" {
		keys = append(keys, extfsPrefix+"storage_type")
		values = append(values, storageConfig.GetStorageType())
	}
	if storageConfig.GetStorageType() == "local" {
		keys = append(keys, extfsPrefix+"bucket_name")
		values = append(values, "local")
	} else if storageConfig.GetBucketName() != "" {
		keys = append(keys, extfsPrefix+"bucket_name")
		values = append(values, storageConfig.GetBucketName())
	}
	if storageConfig.GetAddress() != "" {
		keys = append(keys, extfsPrefix+"address")
		// Lance's BuildEndpointUrl defaults to HTTPS when no scheme is present.
		// Prepend http:// when SSL is not enabled so that Lance sets allow_http=true.
		addr := storageConfig.GetAddress()
		if !storageConfig.GetUseSSL() && !strings.Contains(addr, "://") {
			addr = "http://" + addr
		}
		values = append(values, addr)
	}
	if storageConfig.GetRootPath() != "" {
		keys = append(keys, extfsPrefix+"root_path")
		values = append(values, storageConfig.GetRootPath())
	}
	if storageConfig.GetAccessKeyID() != "" {
		keys = append(keys, extfsPrefix+"access_key_id")
		values = append(values, storageConfig.GetAccessKeyID())
	}
	if storageConfig.GetSecretAccessKey() != "" {
		keys = append(keys, extfsPrefix+"access_key_value")
		values = append(values, storageConfig.GetSecretAccessKey())
	}
	if storageConfig.GetCloudProvider() != "" {
		keys = append(keys, extfsPrefix+"cloud_provider")
		values = append(values, storageConfig.GetCloudProvider())
	}
	if storageConfig.GetIAMEndpoint() != "" {
		keys = append(keys, extfsPrefix+"iam_endpoint")
		values = append(values, storageConfig.GetIAMEndpoint())
	}
	if storageConfig.GetRegion() != "" {
		keys = append(keys, extfsPrefix+"region")
		values = append(values, storageConfig.GetRegion())
	}
	if storageConfig.GetSslCACert() != "" {
		keys = append(keys, extfsPrefix+"ssl_ca_cert")
		values = append(values, storageConfig.GetSslCACert())
	}
	if storageConfig.GetGcpCredentialJSON() != "" {
		keys = append(keys, extfsPrefix+"gcp_credential_json")
		values = append(values, storageConfig.GetGcpCredentialJSON())
	}
	// Temporarily disabled: extfs.default.* boolean properties.
	//
	// Root cause: extfs.* keys are not registered in milvus-storage's property_infos.
	// The FFI (loon_properties_create) only accepts string key-value pairs, so
	// ConvertFFIProperties stores unregistered keys as std::string (e.g.,
	// extfs.default.use_ssl = string("false")). Later, ExtractExternalFsProperties
	// copies this variant as-is to fs.use_ssl, but create_file_system_config expects
	// GetValue<bool>, which fails because the variant holds a string, not a bool.
	//
	// Workaround: omit these properties so they are absent from the extfs-mapped
	// Properties map. GetValue<bool> then falls back to the default value from
	// property_infos (false), which is correct for non-SSL environments.
	//
	// TODO: re-enable once milvus-storage fixes ExtractExternalFsProperties to
	// perform type conversion when mapping extfs.* → fs.* properties.
	//
	// keys = append(keys, extfsPrefix+"use_ssl")
	// if storageConfig.GetUseSSL() {
	// 	values = append(values, "true")
	// } else {
	// 	values = append(values, "false")
	// }
	// keys = append(keys, extfsPrefix+"use_iam")
	// if storageConfig.GetUseIAM() {
	// 	values = append(values, "true")
	// } else {
	// 	values = append(values, "false")
	// }
	// keys = append(keys, extfsPrefix+"use_virtual_host")
	// if storageConfig.GetUseVirtualHost() {
	// 	values = append(values, "true")
	// } else {
	// 	values = append(values, "false")
	// }

	// Add extra kvs
	for k, v := range extraKVs {
		keys = append(keys, k)
		values = append(values, v)
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

		return fmt.Errorf("FFI operation failed: %s", errStr)
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
// Returns:
//
//	-1 if a < b (a is older)
//	 0 if a == b (same version or both empty)
//	 1 if a > b (a is newer)
//	 error if the paths are not comparable (parse failure or different base paths)
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
