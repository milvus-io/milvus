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

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
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

// GetSourceStorageConfig returns nil when no source storage is specified.
// External storage is self-contained: never inherit the destination's credentials,
// IAM settings, or root path when connecting to a user-supplied endpoint.
func GetSourceStorageConfig(options Options) (*indexpb.StorageConfig, error) {
	values := make(map[string]string)
	for _, option := range options {
		key := option.GetKey()
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
	if len(values) == 0 {
		return nil, nil
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
	if err != nil || u.Hostname() == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.ContainsAny(endpoint, "/?#") {
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
	config := &indexpb.StorageConfig{
		StorageType:     "remote",
		CloudProvider:   "aws",
		Address:         endpoint,
		BucketName:      values[StorageBucket],
		AccessKeyID:     values[StorageAccessKeyID],
		SecretAccessKey: values[StorageSecretAccessKey],
		UseSSL:          true,
		Region:          values[StorageRegion],
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

// ValidateSourceStorageFilePaths preserves literal object keys, including dot
// components and repeated separators. No filesystem normalization is applied.
func ValidateSourceStorageFilePaths(paths []string) error {
	for _, key := range paths {
		if err := validateSourceStorageFilePath(key); err != nil {
			return err
		}
	}
	return nil
}

func validateSourceStorageFilePath(key string) error {
	if strings.TrimSpace(key) == "" || strings.HasPrefix(key, "/") || strings.Contains(key, "://") {
		return merr.WrapErrParameterInvalidMsg("external import files must be relative object keys without a URI scheme")
	}
	// A scheme can also be written without //, for example s3:bucket/key.
	if colon := strings.IndexByte(key, ':'); colon >= 0 {
		if slash := strings.IndexByte(key, '/'); slash < 0 || colon < slash {
			if u, err := url.Parse(key); err == nil && u.Scheme != "" {
				return merr.WrapErrParameterInvalidMsg("external import files must not contain a URI scheme")
			}
		}
	}
	return nil
}
