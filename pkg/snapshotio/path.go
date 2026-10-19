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

package snapshotio

import (
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"

	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	snapshotio "github.com/milvus-io/milvus/pkg/v3/snapshotio/format"
	"github.com/milvus-io/milvus/pkg/v3/util/externalspec"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ParseForeignURI accepts object keys, bucket-hosted URIs, and endpoint-style
// URIs. Endpoint-style forms such as minio://host/bucket/key or
// https://host/bucket/key keep the endpoint in endpointHost and take the
// bucket/container from the first path segment.
func ParseForeignURI(raw string) (bucket, objectKey, endpointHost string, err error) {
	return snapshotio.ParseObjectURI(raw, false)
}

// ParseForeignRootURI is equivalent to ParseForeignURI, but complete URIs may
// identify the bucket/container root without an object key.
func ParseForeignRootURI(raw string) (bucket, objectKey, endpointHost string, err error) {
	return snapshotio.ParseObjectURI(raw, true)
}

// snapshotTargetObjectURI retains the target's transport and authority. Storage
// clients and credentials belong to the caller, not the URI builder.
func snapshotTargetObjectURI(target, key string) (string, error) {
	bucket, root, endpoint, err := ParseForeignRootURI(target)
	if err != nil {
		return "", err
	}
	if err := validateRawObjectKey(key); err != nil {
		return "", err
	}
	if bucket == "" {
		// Preserve the existing absolute-path contract for local adapters.
		if !filepath.IsAbs(target) {
			return "", merr.WrapErrParameterInvalidMsg("target requires a complete URI or absolute local path")
		}
		return key, nil
	}
	u, _ := url.Parse(target) // Already validated by ParseForeignRootURI.
	prefix := "/"
	if endpoint != "" {
		prefix += bucket + "/"
	}
	// The existing parser normalizes separators. Reject noncanonical roots
	// rather than silently writing to a different object prefix.
	if strings.TrimSuffix(u.Path, "/") != strings.TrimSuffix(prefix+root, "/") {
		return "", merr.WrapErrParameterInvalidMsg("target URI must not contain repeated path separators")
	}
	u.Path = prefix + key
	u.RawPath = ""
	return u.String(), nil
}

// BuildInstanceSnapshotURI qualifies an instance-owned snapshot object key
// without changing the object-key form stored in the catalog.
func BuildInstanceSnapshotURI(cfg *objectstorage.Config, objectPath string) (string, error) {
	if strings.TrimSpace(objectPath) == "" {
		return "", merr.WrapErrDataIntegrityMsg("snapshot metadata location is empty")
	}
	if cfg == nil {
		return "", merr.WrapErrServiceInternalMsg("instance storage config is nil")
	}

	bucket := strings.TrimSpace(cfg.BucketName)
	objectKey := objectPath
	if hasURITransportScheme(objectPath) {
		parsedBucket, parsedKey, endpointHost, err := ParseForeignURI(objectPath)
		if err != nil {
			return "", merr.WrapErrDataIntegrity(
				err,
				"invalid stored snapshot metadata location %q",
				RedactSnapshotObjectPath(objectPath),
			)
		}
		parsedURI, err := url.Parse(objectPath)
		if err != nil {
			return "", merr.WrapErrDataIntegrity(
				err,
				"invalid stored snapshot metadata location %q",
				RedactSnapshotObjectPath(objectPath),
			)
		}
		if endpointHost != "" || CanonicalForeignScheme(parsedURI.Scheme) != "s3" {
			return objectPath, nil
		}
		if bucket == "" {
			return objectPath, nil
		}
		bucket = parsedBucket
		objectKey = parsedKey
	} else if err := validateRawObjectKey(objectPath); err != nil {
		return "", merr.WrapErrDataIntegrity(
			err,
			"invalid stored snapshot metadata location %q",
			RedactSnapshotObjectPath(objectPath),
		)
	}

	if bucket == "" {
		// Local storage has no supported external snapshot URI scheme.
		return objectPath, nil
	}
	return buildSnapshotObjectURI(cfg, bucket, objectKey)
}

// BuildStorageConfigSnapshotURI builds a credential-free URI from the
// resolved storage config used by an export worker.
func BuildStorageConfigSnapshotURI(cfg *indexpb.StorageConfig, objectPath string) (string, error) {
	if cfg == nil {
		return "", merr.WrapErrServiceInternalMsg("snapshot storage config is nil")
	}
	return buildSnapshotObjectURI(&objectstorage.Config{
		Address:                     cfg.GetAddress(),
		BucketName:                  cfg.GetBucketName(),
		UseSSL:                      cfg.GetUseSSL(),
		CloudProvider:               cfg.GetCloudProvider(),
		Region:                      cfg.GetRegion(),
		AccessKeyID:                 cfg.GetAccessKeyID(),
		SecretAccessKeyID:           cfg.GetSecretAccessKey(),
		IgnoreAzureConnectionString: true,
	}, cfg.GetBucketName(), objectPath)
}

func buildSnapshotObjectURI(cfg *objectstorage.Config, bucket, objectPath string) (string, error) {
	bucket = strings.TrimSpace(bucket)
	if bucket == "" {
		return objectPath, nil
	}
	if err := validateRawObjectKey(objectPath); err != nil {
		return "", merr.WrapErrDataIntegrity(
			err,
			"invalid snapshot object path %q",
			RedactSnapshotObjectPath(objectPath),
		)
	}
	objectKey := strings.TrimLeft(objectPath, "/")
	uri := &url.URL{}

	switch providerFamily(cfg) {
	case providerFamilyS3:
		endpoint := effectiveEndpointHost(cfg)
		if endpoint == "" {
			return "", merr.WrapErrServiceInternalMsg("snapshot storage endpoint is empty")
		}
		if strings.EqualFold(strings.TrimSpace(cfg.CloudProvider), externalspec.CloudProviderMinIO) ||
			inferStorageEndpointIdentity(endpoint).cloudProvider == "" {
			// Custom S3-compatible endpoints use the endpoint-style MinIO URI
			// accepted by both snapshot restore and external collections.
			uri.Scheme = "minio"
		} else if cfg.UseSSL {
			uri.Scheme = "https"
		} else {
			uri.Scheme = "http"
		}
		uri.Host = endpoint
		uri.Path = path.Join("/", bucket, objectKey)
	case providerFamilyGCPNative:
		endpoint := effectiveEndpointHost(cfg)
		if endpoint == "" || hostWithoutPort(endpoint) == "storage.googleapis.com" {
			uri.Scheme = "gs"
			uri.Host = bucket
			uri.Path = "/" + objectKey
		} else {
			if cfg.UseSSL {
				uri.Scheme = "https"
			} else {
				uri.Scheme = "http"
			}
			uri.Host = endpoint
			uri.Path = path.Join("/", bucket, objectKey)
		}
	case providerFamilyAzure:
		endpoint, err := effectiveAzureSnapshotEndpoint(cfg)
		if err != nil {
			return "", err
		}
		uri.Scheme = "azure"
		uri.Host = endpoint
		uri.Path = path.Join("/", bucket, objectKey)
	default:
		return "", merr.WrapErrServiceInternalMsg("snapshot storage provider is not supported")
	}

	result := uri.String()
	parsedBucket, parsedKey, _, err := ParseForeignURI(result)
	if err != nil {
		return "", merr.WrapErrServiceInternalErr(err, "failed to build snapshot metadata URI")
	}
	if parsedBucket != bucket || parsedKey != objectKey {
		return "", merr.WrapErrServiceInternalMsg("snapshot metadata URI changed its bucket or object key")
	}
	return result, nil
}

// effectiveAzureSnapshotEndpoint returns the canonical account-qualified Azure
// endpoint, "<account>.blob.<suffix>". The Milvus instance config carries the
// port minio.address was normalized with (cloud deployments typically resolve
// to "core.windows.net:443"), while snapshot URIs and endpoint identity
// parsing drop ports, so all comparisons and emitted URIs would disagree on
// the port spelling alone. Strip the scheme's default port here so every
// consumer sees one canonical form; non-default ports are preserved.
func effectiveAzureSnapshotEndpoint(cfg *objectstorage.Config) (string, error) {
	if cfg == nil {
		return "", merr.WrapErrServiceInternalMsg("Azure storage config is nil")
	}
	if !cfg.IgnoreAzureConnectionString {
		if connectionString := os.Getenv("AZURE_STORAGE_CONNECTION_STRING"); connectionString != "" {
			client, err := service.NewClientFromConnectionString(connectionString, nil)
			if err != nil {
				return "", merr.WrapErrServiceInternalErr(err, "failed to parse Azure storage connection string")
			}
			endpoint, err := url.Parse(client.URL())
			if err != nil {
				return "", merr.WrapErrServiceInternalErr(err, "failed to resolve Azure storage endpoint")
			}
			if endpoint.Host == "" {
				return "", merr.WrapErrServiceInternalMsg("resolved Azure storage endpoint has no host")
			}
			if strings.Trim(endpoint.Path, "/") != "" {
				return "", merr.WrapErrServiceInternalMsg("Azure storage endpoints with path prefixes are not supported")
			}
			return stripDefaultEndpointPort(endpoint.Host, endpoint.Scheme != "http"), nil
		}
	}

	account := strings.TrimSpace(cfg.AccessKeyID)
	suffix := strings.TrimSpace(cfg.Address)
	if account == "" || suffix == "" {
		return "", merr.WrapErrServiceInternalMsg("Azure account name and endpoint suffix are required")
	}
	return stripDefaultEndpointPort(account+".blob."+suffix, cfg.UseSSL), nil
}

func DeriveForeignRoot(direction Direction, objectKey string) (string, error) {
	objectKey = strings.Trim(objectKey, "/")
	if direction == DirectionExport || direction == DirectionCopySource {
		return objectKey, nil
	}

	// Restore has no extra API parameter for the bundle root. Derive it from
	// the canonical snapshots/{collectionID}/metadata/{snapshotID} anchor in
	// the metadata URI, and reject arbitrary layouts that do not expose it.
	root, _, _, ok := deriveSnapshotBundleAnchor(objectKey)
	if !ok {
		return "", merr.WrapErrParameterInvalidMsg(
			"restore metadata URI must end with snapshots/{collectionID}/metadata/{snapshotID}.json",
		)
	}
	return root, nil
}

func deriveSnapshotBundleAnchor(objectKey string) (root string, collectionID, snapshotID int64, ok bool) {
	return snapshotio.SnapshotBundleAnchor(objectKey)
}

// CanonicalForeignScheme returns one stable identity for supported scheme aliases.
func CanonicalForeignScheme(scheme string) string {
	switch strings.ToLower(strings.TrimSpace(scheme)) {
	case "gcs":
		return "gs"
	case "az":
		return "azure"
	default:
		return strings.ToLower(strings.TrimSpace(scheme))
	}
}

func hasURITransportScheme(raw string) bool {
	return strings.Contains(raw, "://")
}

func validateRawObjectKey(objectKey string) error { return snapshotio.ValidateObjectKey(objectKey) }
