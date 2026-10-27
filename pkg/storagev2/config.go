// Copyright 2026 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storagev2

import (
	"strconv"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

// LoonLocalFSRootPath keeps complete local keys from receiving a second prefix.
// Local keys already include localStorage.path, so loon must use the namespace
// root rather than joining that prefix again. Remote keys also carry their
// prefix, but milvus-storage does not apply fs.root_path to object storage.
// The C++ counterpart is LoonFSRootPath in storage/loon_ffi/util.h.
const LoonLocalFSRootPath = "/"

func LoonFSRootPath(config *indexpb.StorageConfig) string {
	if config.GetStorageType() == "local" {
		return LoonLocalFSRootPath
	}
	return config.GetRootPath()
}

// StorageProperties maps caller-owned configuration to milvus-storage properties.
// It returns a fresh map without server globals, writer settings or overrides.
func StorageProperties(config *indexpb.StorageConfig) map[string]string {
	properties := make(map[string]string)
	for key, value := range map[string]string{
		"fs.address":                config.GetAddress(),
		"fs.bucket_name":            config.GetBucketName(),
		"fs.access_key_id":          config.GetAccessKeyID(),
		"fs.access_key_value":       config.GetSecretAccessKey(),
		"fs.root_path":              LoonFSRootPath(config),
		"fs.storage_type":           config.GetStorageType(),
		"fs.cloud_provider":         config.GetCloudProvider(),
		"fs.iam_endpoint":           config.GetIAMEndpoint(),
		"fs.log_level":              "warn",
		"fs.region":                 config.GetRegion(),
		"fs.ssl_ca_cert":            config.GetSslCACert(),
		"fs.gcp_credential_json":    config.GetGcpCredentialJSON(),
		"fs.use_ssl":                strconv.FormatBool(config.GetUseSSL()),
		"fs.use_iam":                strconv.FormatBool(config.GetUseIAM()),
		"fs.use_virtual_host":       strconv.FormatBool(config.GetUseVirtualHost()),
		"fs.use_custom_part_upload": "true",
		"fs.request_timeout_ms":     strconv.FormatInt(config.GetRequestTimeoutMs(), 10),
		"fs.use_crc32c_checksum":    strconv.FormatBool(config.GetUseCrc32CChecksum()),
		"fs.talon.coordinator":      config.GetTalonCoordinator(),
	} {
		if value != "" {
			properties[key] = value
		}
	}
	// Unset values must leave the native defaults and filesystem cache key intact.
	if config.GetMaxConnections() > 0 {
		properties["fs.max_connections"] = strconv.FormatUint(uint64(config.GetMaxConnections()), 10)
	}
	if version := config.GetSslTlsMinVersion(); version != "" && version != "default" {
		properties["fs.tls_min_version"] = version
	}
	mode := config.GetTalonMode()
	if config.GetStorageType() == "local" {
		mode = 0
	}
	properties["fs.talon.mode"] = strconv.FormatUint(uint64(mode), 10)
	for key, value := range map[string]uint32{
		"fs.talon.small_read_threshold": config.GetTalonSmallReadThreshold(),
		"fs.talon.block_size":           config.GetTalonBlockSize(),
		"fs.talon.max_idle_per_addr":    config.GetTalonMaxIdlePerAddr(),
	} {
		if value > 0 {
			properties[key] = strconv.FormatUint(uint64(value), 10)
		}
	}
	return properties
}
