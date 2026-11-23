// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package importutilv2

import (
	"net/url"
	"strconv"
	"strings"

	"github.com/minio/minio-go/v7/pkg/s3utils"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	StorageType            = "storage.type"
	StorageEndpoint        = "storage.endpoint"
	StorageBucket          = "storage.bucket"
	StorageAccessKeyID     = "storage.accessKeyID"
	StorageSecretAccessKey = "storage.secretAccessKey"
	StorageUseSSL          = "storage.useSSL"
	StorageRegion          = "storage.region"
	StorageUseVirtualHost  = "storage.useVirtualHost"
)

// SourceStorageConfig describes a validated, self-contained external S3-compatible
// source for ordinary file imports. It never inherits cluster credentials, IAM
// settings, or root paths.
type SourceStorageConfig struct {
	Endpoint        string
	Bucket          string
	AccessKeyID     string
	SecretAccessKey string
	Region          string
	UseSSL          bool
	UseVirtualHost  bool
}

// GetSourceStorageConfig parses and validates storage.* options into a typed
// SourceStorageConfig, returning (nil, nil) when no source storage option is set.
func GetSourceStorageConfig(options Options) (*SourceStorageConfig, error) {
	if !HasSourceStorageOptions(options) {
		return nil, nil
	}

	values := make(map[string]string)
	hasSnapshotContractOption := false
	for _, option := range options {
		if option == nil {
			return nil, merr.WrapErrParameterInvalidMsg("import option must not be nil")
		}
		key := option.GetKey()
		if key == SourceType || key == ExternalSpec || key == SnapshotSourceURI ||
			key == SnapshotLayout || key == PartitionMapping {
			hasSnapshotContractOption = true
		}
		if !IsSourceStorageOption(key) {
			continue
		}
		switch key {
		case StorageType, StorageEndpoint, StorageBucket, StorageAccessKeyID,
			StorageSecretAccessKey, StorageUseSSL, StorageRegion, StorageUseVirtualHost:
		default:
			return nil, merr.WrapErrParameterInvalidMsg("unsupported import storage option")
		}
		if _, exists := values[key]; exists {
			return nil, merr.WrapErrParameterInvalidMsg("duplicate import option %s", key)
		}
		values[key] = option.GetValue()
	}

	if IsBackup(options) || IsL0Import(options) || hasSnapshotContractOption {
		return nil, merr.WrapErrParameterInvalidMsg("storage.* options are only supported for ordinary file imports")
	}
	if value, ok := values[StorageType]; ok && value != "remote" {
		return nil, merr.WrapErrParameterInvalidMsg("storage.type must be remote")
	}
	for _, key := range []string{StorageEndpoint, StorageBucket, StorageAccessKeyID, StorageSecretAccessKey} {
		if strings.TrimSpace(values[key]) == "" {
			return nil, merr.WrapErrParameterInvalidMsg("%s is required for external import", key)
		}
	}

	endpoint := values[StorageEndpoint]
	u, err := url.Parse("https://" + endpoint)
	if err != nil || u.Hostname() == "" || u.User != nil || u.Path != "" || u.RawQuery != "" ||
		u.ForceQuery || u.Fragment != "" || strings.ContainsAny(endpoint, "/?#") {
		return nil, merr.WrapErrParameterInvalidMsg("storage.endpoint must be a host or host:port without a scheme or path")
	}
	if !s3utils.IsValidIP(u.Hostname()) && !s3utils.IsValidDomain(u.Hostname()) {
		return nil, merr.WrapErrParameterInvalidMsg("storage.endpoint contains an invalid host")
	}
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return nil, merr.WrapErrParameterInvalidMsg("storage.endpoint port must be between 1 and 65535")
		}
	} else if strings.HasSuffix(endpoint, ":") {
		return nil, merr.WrapErrParameterInvalidMsg("storage.endpoint port must not be empty")
	}

	if err := s3utils.CheckValidBucketName(values[StorageBucket]); err != nil {
		return nil, merr.WrapErrParameterInvalidMsg("storage.bucket must be a valid S3 bucket name")
	}

	config := &SourceStorageConfig{
		Endpoint:        endpoint,
		Bucket:          values[StorageBucket],
		AccessKeyID:     values[StorageAccessKeyID],
		SecretAccessKey: values[StorageSecretAccessKey],
		Region:          values[StorageRegion],
		UseSSL:          true,
	}
	for _, option := range []struct {
		key    string
		target *bool
	}{{StorageUseSSL, &config.UseSSL}, {StorageUseVirtualHost, &config.UseVirtualHost}} {
		if value, ok := values[option.key]; ok {
			parsed, err := strconv.ParseBool(value)
			if err != nil {
				return nil, merr.WrapErrParameterInvalidMsg("%s must be a boolean", option.key)
			}
			*option.target = parsed
		}
	}
	return config, nil
}

// HasSourceStorageOptions reports whether the request supplies an ordinary-file
// import source. Snapshot imports use their existing external_spec contract.
func HasSourceStorageOptions(options Options) bool {
	for _, option := range options {
		if IsSourceStorageOption(option.GetKey()) {
			return true
		}
	}
	return false
}

// IsSourceStorageOption also recognizes misspelled casing so credentials cannot
// silently fall back to the cluster's storage or escape redaction/cleanup.
func IsSourceStorageOption(key string) bool {
	return strings.HasPrefix(strings.ToLower(key), "storage.")
}

// ValidateSourceStorageFilePaths preserves literal S3 object keys, including
// colons, dot components, and repeated separators. No filesystem normalization
// or URI parsing is applied.
func ValidateSourceStorageFilePaths(paths []string) error {
	for _, key := range paths {
		if strings.HasPrefix(key, "/") || strings.Contains(key, "://") {
			return merr.WrapErrParameterInvalidMsg("external import files must be relative object keys without a URI scheme")
		}
		if err := s3utils.CheckValidObjectName(key); err != nil {
			return merr.WrapErrParameterInvalidErr(err, "invalid external import file key")
		}
	}
	return nil
}

// RemoveSourceStorageCredentials removes secret credentials from terminal job
// metadata while preserving non-secret structural storage.* keys so an
// external-storage job retains its external-source identity and can never
// silently fall back to cluster storage.
func RemoveSourceStorageCredentials(options Options) Options {
	filtered := make(Options, 0, len(options))
	for _, option := range options {
		if option == nil {
			continue
		}
		key := option.GetKey()
		if strings.EqualFold(key, ExternalSpec) || strings.EqualFold(key, SnapshotSourceURI) {
			continue
		}
		if IsSourceStorageOption(key) {
			switch key {
			case StorageType, StorageEndpoint, StorageBucket,
				StorageUseSSL, StorageRegion, StorageUseVirtualHost:
			default:
				continue
			}
		}
		filtered = append(filtered, option)
	}
	return filtered
}
