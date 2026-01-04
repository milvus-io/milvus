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
func MakePropertiesFromStorageConfig(storageConfig *indexpb.StorageConfig, extraKVs map[string]string) (*C.Properties, error) {
	if storageConfig == nil {
		return nil, fmt.Errorf("storageConfig is required")
	}

	// Prepare key-value pairs from StorageConfig
	var keys []string
	var values []string

	// Add non-empty string fields
	if storageConfig.GetAddress() != "" {
		keys = append(keys, PropertyFSAddress)
		values = append(values, storageConfig.GetAddress())
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
	// Always add log level if any string field is set (matching C++ behavior)
	keys = append(keys, PropertyFSLogLevel)
	values = append(values, "Warn")

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
	properties := &C.Properties{}
	var cKeysPtr **C.char
	var cValuesPtr **C.char
	if len(cKeys) > 0 {
		cKeysPtr = &cKeys[0]
		cValuesPtr = &cValues[0]
	}

	result := C.properties_create(
		(**C.char)(unsafe.Pointer(cKeysPtr)),
		(**C.char)(unsafe.Pointer(cValuesPtr)),
		C.size_t(len(keys)),
		properties,
	)

	err := HandleFFIResult(result)
	if err != nil {
		return nil, err
	}
	return properties, nil
}

func HandleFFIResult(ffiResult C.FFIResult) error {
	defer C.FreeFFIResult(&ffiResult)
	if C.IsSuccess(&ffiResult) == 0 {
		errMsg := C.GetErrorMessage(&ffiResult)
		errStr := "Unknown error"
		if errMsg != nil {
			errStr = C.GoString(errMsg)
		}

		return fmt.Errorf("failed to create properties: %s", errStr)
	}
	return nil
}

type ManifestJSON struct {
	ManifestVersion int64  `json:"ver"`
	BasePath        string `json:"base_path"`
}

func MarshalManifestPath(basePath string, version int64) string {
	bs, _ := json.Marshal(ManifestJSON{
		ManifestVersion: version,
		BasePath:        basePath,
	})
	return string(bs)
}

func UnmarshalManfestPath(manifestPath string) (string, int64, error) {
	var manifestJSON ManifestJSON
	err := json.Unmarshal([]byte(manifestPath), &manifestJSON)
	if err != nil {
		return "", 0, err
	}
	return manifestJSON.BasePath, manifestJSON.ManifestVersion, nil
}
