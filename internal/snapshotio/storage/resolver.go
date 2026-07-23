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

package storage

import (
	"context"
	"strings"

	milvusstorage "github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func InstanceConfigFromParamtable(params *paramtable.ComponentParam) *objectstorage.Config {
	cfg := objectstorage.NewDefaultConfig()
	cfg.ReadRetryAttempts = params.CommonCfg.StorageReadRetryAttempts.GetAsUint()
	if params.CommonCfg.StorageType.GetValue() == "local" {
		cfg.RootPath = params.LocalStorageCfg.Path.GetValue()
		return cfg
	}

	cfg.Address = params.MinioCfg.Address.GetValue()
	cfg.BucketName = params.MinioCfg.BucketName.GetValue()
	cfg.AccessKeyID = params.MinioCfg.AccessKeyID.GetValue()
	cfg.SecretAccessKeyID = params.MinioCfg.SecretAccessKey.GetValue()
	cfg.UseSSL = params.MinioCfg.UseSSL.GetAsBool()
	cfg.SslCACert = params.MinioCfg.SslCACert.GetValue()
	cfg.SslTLSMinVersion = params.MinioCfg.SslTLSMinVersion.GetValue()
	cfg.CreateBucket = true
	cfg.RootPath = params.MinioCfg.RootPath.GetValue()
	cfg.UseIAM = params.MinioCfg.UseIAM.GetAsBool()
	cfg.CloudProvider = params.MinioCfg.CloudProvider.GetValue()
	cfg.IAMEndpoint = params.MinioCfg.IAMEndpoint.GetValue()
	cfg.UseVirtualHost = params.MinioCfg.UseVirtualHost.GetAsBool()
	cfg.Region = params.MinioCfg.Region.GetValue()
	cfg.RequestTimeoutMs = params.MinioCfg.RequestTimeoutMs.GetAsInt64()
	cfg.GcpCredentialJSON = params.MinioCfg.GcpCredentialJSON.GetValue()
	return cfg
}

func ResolveForeignStorage(
	ctx context.Context,
	instanceCfg *objectstorage.Config,
	direction Direction,
	foreignURI string,
	externalSpec string,
) (*ResolvedForeignStorage, error) {
	if instanceCfg == nil {
		return nil, merr.WrapErrParameterInvalidMsg("instance storage config is nil")
	}
	if !isRemoteInstanceConfig(instanceCfg) {
		return nil, merr.WrapErrServiceInternal(unsupportedServerSideCopyMessage)
	}

	foreignBucket, foreignRoot, uriScheme, uriEndpoint, err := parseSnapshotForeignURI(direction, foreignURI)
	if err != nil {
		return nil, err
	}
	if foreignBucket == "" {
		foreignBucket = strings.TrimSpace(instanceCfg.BucketName)
	}

	foreignCfg := cloneObjectStorageConfig(instanceCfg)
	foreignCfg.BucketName = foreignBucket
	foreignCfg.RootPath = foreignRoot
	foreignCfg.CreateBucket = direction == DirectionExport && instanceCfg.CreateBucket
	// Request-scoped clients validate access through metadata reads or the first
	// provider-side copy. A bucket-level existence probe would require broader
	// permissions such as ListBucket that the actual object operation does not.
	foreignCfg.SkipBucketCheck = true
	hasSpec, storageType, err := applySnapshotExternalSpecToConfig(
		foreignCfg,
		instanceCfg,
		uriScheme,
		uriEndpoint,
		externalSpec,
	)
	if err != nil {
		return nil, err
	}

	if strings.TrimSpace(foreignCfg.BucketName) == "" {
		return nil, merr.WrapErrServiceInternal(unsupportedServerSideCopyMessage)
	}
	if err := validateProviderEndpointPair(instanceCfg, foreignCfg, uriScheme, uriEndpoint, hasSpec); err != nil {
		return nil, err
	}
	foreignCM, err := milvusstorage.NewRemoteChunkManager(ctx, foreignCfg)
	if err != nil {
		return nil, err
	}
	var copier milvusstorage.CrossBucketCopier = foreignCM
	if hasSpec && (direction == DirectionRestore || direction == DirectionCopySource) {
		// Layer 2 restore uses foreign credentials against the target endpoint so
		// the provider can authorize both sides of the server-side copy request.
		copier, err = milvusstorage.NewRemoteChunkManager(ctx, restoreProviderCopyConfig(instanceCfg, foreignCfg))
		if err != nil {
			return nil, err
		}
	}

	return &ResolvedForeignStorage{
		ForeignBucket:        foreignBucket,
		ForeignCM:            foreignCM,
		ForeignStorageConfig: storageConfigFromObjectConfig(foreignCfg, storageType),
		Copier:               copier,
	}, nil
}

func validateProviderEndpointPair(
	instanceCfg *objectstorage.Config,
	foreignCfg *objectstorage.Config,
	uriScheme string,
	uriEndpoint string,
	hasSpec bool,
) error {
	// Only provider-side copy is implemented. Different provider families or
	// untrusted custom endpoints would require streaming through Milvus, which
	// is deliberately outside this API contract.
	instanceFamily := providerFamily(instanceCfg)
	foreignFamily := providerFamily(foreignCfg)
	if instanceFamily == providerFamilyUnknown {
		return merr.WrapErrServiceInternal(unsupportedServerSideCopyMessage)
	}
	if foreignFamily == providerFamilyUnknown || instanceFamily != foreignFamily {
		return merr.WrapErrParameterInvalidMsg(unsupportedServerSideCopyMessage)
	}
	_, schemeFamily := providerInfoFromScheme(uriScheme)
	if schemeFamily != providerFamilyUnknown && schemeFamily != foreignFamily {
		return merr.WrapErrParameterInvalidMsg(unsupportedServerSideCopyMessage)
	}

	allowlist := paramtable.Get().DataCoordCfg.SnapshotCrossBucketEndpointAllowlist.GetValue()
	instanceHost := effectiveEndpointHost(instanceCfg)
	foreignHost := effectiveEndpointHost(foreignCfg)
	provider := foreignCfg.CloudProvider
	if provider == "" {
		provider = instanceCfg.CloudProvider
	}
	region := foreignCfg.Region
	if region == "" {
		region = instanceCfg.Region
	}
	if !hasSpec && uriEndpoint != "" {
		uriHost, err := normalizeEndpointHost(uriEndpoint)
		if err != nil {
			return err
		}
		if uriHost != "" && instanceHost != "" && uriHost != instanceHost {
			return merr.WrapErrParameterInvalidMsg(unsupportedServerSideCopyMessage)
		}
	}

	switch foreignFamily {
	case providerFamilyGCPNative:
		if foreignHost == "" {
			return nil
		}
		if instanceHost == foreignHost ||
			isCanonicalCloudEndpoint(instanceHost, objectstorage.CloudProviderGCPNative, region) &&
				isCanonicalCloudEndpoint(foreignHost, objectstorage.CloudProviderGCPNative, region) {
			return nil
		}
	case providerFamilyAzure:
		if !sameAzureAccountEndpoint(instanceCfg, foreignCfg) {
			break
		}
		if foreignHost == "" || foreignHost == instanceHost {
			return nil
		}
	case providerFamilyS3:
		if endpointsCompatible(instanceHost, foreignHost, allowlist, provider, region) {
			return nil
		}
	}

	return merr.WrapErrParameterInvalidMsg(unsupportedServerSideCopyMessage)
}

func applyEndpointToConfig(cfg *objectstorage.Config, raw string) error {
	host, err := normalizeEndpointHost(raw)
	if err != nil {
		return err
	}
	if host != "" {
		cfg.Address = host
	}
	switch {
	case strings.HasPrefix(strings.ToLower(strings.TrimSpace(raw)), "https://"):
		cfg.UseSSL = true
	case strings.HasPrefix(strings.ToLower(strings.TrimSpace(raw)), "http://"):
		cfg.UseSSL = false
	}
	return nil
}

func isRemoteInstanceConfig(cfg *objectstorage.Config) bool {
	if cfg == nil {
		return false
	}
	switch providerFamily(cfg) {
	case providerFamilyGCPNative, providerFamilyAzure:
		return strings.TrimSpace(cfg.BucketName) != ""
	case providerFamilyS3:
		return strings.TrimSpace(cfg.BucketName) != "" &&
			(strings.TrimSpace(cfg.Address) != "" ||
				strings.TrimSpace(cfg.CloudProvider) != "")
	default:
		return false
	}
}

func cloneObjectStorageConfig(cfg *objectstorage.Config) *objectstorage.Config {
	if cfg == nil {
		return nil
	}
	cloned := *cfg
	return &cloned
}

func restoreProviderCopyConfig(instanceCfg, foreignCfg *objectstorage.Config) *objectstorage.Config {
	// For restore the source may need foreign credentials, but the copy request
	// must be made against the target provider endpoint so the target bucket can
	// authorize the write side of a server-side copy.
	copyCfg := cloneObjectStorageConfig(foreignCfg)
	copyCfg.Address = instanceCfg.Address
	copyCfg.BucketName = instanceCfg.BucketName
	copyCfg.RootPath = instanceCfg.RootPath
	copyCfg.UseSSL = instanceCfg.UseSSL
	copyCfg.SslCACert = instanceCfg.SslCACert
	copyCfg.SslTLSMinVersion = instanceCfg.SslTLSMinVersion
	copyCfg.CloudProvider = instanceCfg.CloudProvider
	copyCfg.UseVirtualHost = instanceCfg.UseVirtualHost
	copyCfg.Region = instanceCfg.Region
	copyCfg.RequestTimeoutMs = instanceCfg.RequestTimeoutMs
	copyCfg.ReadRetryAttempts = instanceCfg.ReadRetryAttempts
	return copyCfg
}

func storageConfigFromObjectConfig(cfg *objectstorage.Config, storageType string) *indexpb.StorageConfig {
	if storageType == "" {
		storageType = "remote"
	}
	return &indexpb.StorageConfig{
		Address:           cfg.Address,
		AccessKeyID:       cfg.AccessKeyID,
		SecretAccessKey:   cfg.SecretAccessKeyID,
		UseSSL:            cfg.UseSSL,
		BucketName:        cfg.BucketName,
		RootPath:          cfg.RootPath,
		UseIAM:            cfg.UseIAM,
		IAMEndpoint:       cfg.IAMEndpoint,
		StorageType:       storageType,
		UseVirtualHost:    cfg.UseVirtualHost,
		Region:            cfg.Region,
		CloudProvider:     cfg.CloudProvider,
		RequestTimeoutMs:  cfg.RequestTimeoutMs,
		SslCACert:         cfg.SslCACert,
		GcpCredentialJSON: cfg.GcpCredentialJSON,
		SslTlsMinVersion:  cfg.SslTLSMinVersion,
		UseCrc32CChecksum: paramtable.Get().MinioCfg.UseCRC32C.GetAsBool(),
	}
}

const (
	providerFamilyS3        = "s3"
	providerFamilyGCPNative = "gcpnative"
	providerFamilyAzure     = "azure"
	providerFamilyUnknown   = "unknown"
)

func providerFamily(cfg *objectstorage.Config) string {
	if cfg == nil {
		return providerFamilyUnknown
	}
	switch strings.ToLower(strings.TrimSpace(cfg.CloudProvider)) {
	case "", objectstorage.CloudProviderAWS, objectstorage.CloudProviderGCP,
		objectstorage.CloudProviderAliyun, objectstorage.CloudProviderTencent,
		objectstorage.CloudProviderHuawei, "minio":
		return providerFamilyS3
	case objectstorage.CloudProviderGCPNative:
		return providerFamilyGCPNative
	case objectstorage.CloudProviderAzure:
		return providerFamilyAzure
	default:
		return providerFamilyUnknown
	}
}

func providerInfoFromScheme(scheme string) (cloudProvider string, family string) {
	switch scheme {
	case "gs", "gcs":
		return objectstorage.CloudProviderGCPNative, providerFamilyGCPNative
	case "az", "azure":
		return objectstorage.CloudProviderAzure, providerFamilyAzure
	case "s3", "minio":
		return "", providerFamilyS3
	default:
		return "", providerFamilyUnknown
	}
}

func sameAzureAccountEndpoint(instanceCfg, foreignCfg *objectstorage.Config) bool {
	instanceHost := effectiveEndpointHost(instanceCfg)
	foreignHost := effectiveEndpointHost(foreignCfg)
	if instanceHost != "" && foreignHost != "" && instanceHost != foreignHost {
		return false
	}
	if instanceCfg.AccessKeyID != "" && foreignCfg.AccessKeyID != "" &&
		instanceCfg.AccessKeyID != foreignCfg.AccessKeyID {
		return false
	}
	return true
}
