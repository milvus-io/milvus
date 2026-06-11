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

package snapshotstorage

import (
	"context"
	"strings"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/externalspec"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func InstanceConfigFromParamtable(params *paramtable.ComponentParam) *objectstorage.Config {
	if params == nil {
		params = paramtable.Get()
	}

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
	if err := validateDirection(direction); err != nil {
		return nil, err
	}
	if !isRemoteInstanceConfig(instanceCfg) {
		return nil, merr.WrapErrServiceInternal(unsupportedServerSideCopyMessage)
	}

	validated, err := ValidateSnapshotForeignStorage(direction, foreignURI, externalSpec)
	if err != nil {
		return nil, err
	}

	foreignBucket := validated.ForeignBucket
	if foreignBucket == "" {
		foreignBucket = strings.TrimSpace(instanceCfg.BucketName)
	}

	foreignCfg := cloneObjectStorageConfig(instanceCfg)
	foreignCfg.BucketName = foreignBucket
	foreignCfg.RootPath = validated.ForeignRoot
	foreignCfg.CreateBucket = direction == DirectionExport && instanceCfg.CreateBucket
	if validated.HasSpec {
		if err := applyValidatedSpecToConfig(foreignCfg, instanceCfg, validated); err != nil {
			return nil, err
		}
	}

	copier, err := ResolveProviderCopyCapability(ctx, instanceCfg, foreignCfg, validated, direction)
	if err != nil {
		return nil, err
	}

	foreignCM, err := storage.NewRemoteChunkManager(ctx, foreignCfg)
	if err != nil {
		return nil, err
	}

	return &ResolvedForeignStorage{
		ForeignBucket:        foreignBucket,
		ForeignRoot:          validated.ForeignRoot,
		ForeignCM:            foreignCM,
		ForeignStorageConfig: storageConfigFromObjectConfig(foreignCfg, validated.StorageType),
		Copier:               copier,
	}, nil
}

func ResolveProviderCopyCapability(
	ctx context.Context,
	instanceCfg *objectstorage.Config,
	foreignCfg *objectstorage.Config,
	validated *ValidatedSpec,
	direction Direction,
) (storage.CrossBucketCopier, error) {
	if instanceCfg == nil || foreignCfg == nil {
		return nil, merr.WrapErrParameterInvalidMsg("storage config is nil")
	}
	if err := validateDirection(direction); err != nil {
		return nil, err
	}
	if !isRemoteInstanceConfig(instanceCfg) {
		return nil, merr.WrapErrServiceInternal(unsupportedServerSideCopyMessage)
	}
	if strings.TrimSpace(foreignCfg.BucketName) == "" {
		return nil, merr.WrapErrServiceInternal(unsupportedServerSideCopyMessage)
	}
	if err := validateProviderEndpointPair(instanceCfg, foreignCfg, validated); err != nil {
		return nil, err
	}

	copierCfg := selectProviderCopyConfig(instanceCfg, foreignCfg, validated, direction)
	return storage.NewRemoteChunkManager(ctx, copierCfg)
}

func validateProviderEndpointPair(
	instanceCfg *objectstorage.Config,
	foreignCfg *objectstorage.Config,
	validated *ValidatedSpec,
) error {
	instanceFamily := providerFamily(instanceCfg)
	foreignFamily := providerFamily(foreignCfg)
	if instanceFamily == providerFamilyUnknown || foreignFamily == providerFamilyUnknown ||
		instanceFamily != foreignFamily {
		return merr.WrapErrServiceInternal(unsupportedServerSideCopyMessage)
	}
	if validated != nil {
		if err := validateURISchemeMatchesFamily(validated, foreignFamily); err != nil {
			return err
		}
	}

	allowlist := snapshotCrossBucketEndpointAllowlist()
	instanceHost := effectiveEndpointHost(instanceCfg)
	foreignHost := effectiveEndpointHost(foreignCfg)
	provider := effectiveCloudProvider(instanceCfg, foreignCfg)
	region := foreignCfg.Region
	if region == "" {
		region = instanceCfg.Region
	}
	if validated != nil && !validated.HasSpec {
		if err := validateLayer1URIMatchesInstance(validated, instanceFamily, instanceHost); err != nil {
			return err
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
		if foreignHost == "" || foreignHost == instanceHost ||
			EndpointAllowedByAllowlist(foreignHost, instanceHost, allowlist, provider, region) {
			return nil
		}
	case providerFamilyS3:
		if endpointsCompatible(instanceHost, foreignHost, allowlist, provider, region) {
			return nil
		}
	}

	return merr.WrapErrServiceInternal(unsupportedServerSideCopyMessage)
}

func applyValidatedSpecToConfig(
	cfg *objectstorage.Config,
	instanceCfg *objectstorage.Config,
	validated *ValidatedSpec,
) error {
	if validated.Endpoint != "" {
		if err := applyEndpointToConfig(cfg, validated.Endpoint); err != nil {
			return err
		}
	}
	if validated.Scheme == "https" {
		cfg.UseSSL = true
	}
	if validated.CloudProvider != "" {
		cfg.CloudProvider = validated.CloudProvider
	} else if providerFromScheme(validated.Scheme) != "" && instanceCfg.CloudProvider != providerFromScheme(validated.Scheme) {
		cfg.CloudProvider = providerFromScheme(validated.Scheme)
	}
	if validated.Region != "" {
		cfg.Region = validated.Region
	}
	if validated.IAMEndpoint != "" {
		cfg.IAMEndpoint = validated.IAMEndpoint
	}
	if validated.UseIAMSet {
		cfg.UseIAM = validated.UseIAM
	}
	if validated.UseSSLSet {
		cfg.UseSSL = validated.UseSSL
	}
	if validated.UseVirtualHostSet {
		cfg.UseVirtualHost = validated.UseVirtualHost
	}
	if validated.SslCACert != "" {
		cfg.SslCACert = validated.SslCACert
	}
	if validated.RawAccessKeyID != "" || validated.RawSecretKey != "" {
		cfg.AccessKeyID = validated.RawAccessKeyID
		cfg.SecretAccessKeyID = validated.RawSecretKey
		cfg.UseIAM = false
	}

	if validated.Endpoint == "" && validated.CloudProvider != "" {
		derived := externalspec.DeriveEndpoint(validated.CloudProvider, validated.Region)
		if strings.TrimSpace(derived) == "" {
			if strings.EqualFold(validated.CloudProvider, objectstorage.CloudProviderGCPNative) {
				cfg.Address = ""
				return nil
			}
			return merr.WrapErrParameterInvalidMsg(
				"extfs.cloud_provider %q requires a derivable region or an endpoint in foreign URI",
				validated.CloudProvider,
			)
		}
		if err := applyEndpointToConfig(cfg, derived); err != nil {
			return err
		}
	}
	return nil
}

func applyEndpointToConfig(cfg *objectstorage.Config, raw string) error {
	host, err := NormalizeEndpointHost(raw)
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

func selectProviderCopyConfig(
	instanceCfg *objectstorage.Config,
	foreignCfg *objectstorage.Config,
	validated *ValidatedSpec,
	direction Direction,
) *objectstorage.Config {
	if validated == nil || !validated.HasLayer2 {
		return instanceCfg
	}
	if direction != DirectionRestore && direction != DirectionCopySource {
		return foreignCfg
	}

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

func providerFromScheme(scheme string) string {
	switch scheme {
	case "gs", "gcs":
		return objectstorage.CloudProviderGCPNative
	case "az", "azure":
		return objectstorage.CloudProviderAzure
	default:
		return ""
	}
}

func effectiveCloudProvider(instanceCfg, foreignCfg *objectstorage.Config) string {
	if foreignCfg != nil && foreignCfg.CloudProvider != "" {
		return foreignCfg.CloudProvider
	}
	if instanceCfg != nil {
		return instanceCfg.CloudProvider
	}
	return ""
}

func validateLayer1URIMatchesInstance(
	validated *ValidatedSpec,
	instanceFamily string,
	instanceHost string,
) error {
	schemeFamily := providerFamilyFromScheme(validated.Scheme)
	if schemeFamily != providerFamilyUnknown && schemeFamily != instanceFamily {
		return merr.WrapErrServiceInternal(unsupportedServerSideCopyMessage)
	}
	if validated.Endpoint == "" {
		return nil
	}
	uriHost, err := NormalizeEndpointHost(validated.Endpoint)
	if err != nil {
		return err
	}
	instanceHost, _ = NormalizeEndpointHost(instanceHost)
	if uriHost != "" && instanceHost != "" && uriHost != instanceHost {
		return merr.WrapErrServiceInternal(unsupportedServerSideCopyMessage)
	}
	return nil
}

func providerFamilyFromScheme(scheme string) string {
	switch scheme {
	case "gs", "gcs":
		return providerFamilyGCPNative
	case "az", "azure":
		return providerFamilyAzure
	case "s3", "minio":
		return providerFamilyS3
	default:
		return providerFamilyUnknown
	}
}

func validateURISchemeMatchesFamily(validated *ValidatedSpec, family string) error {
	schemeFamily := providerFamilyFromScheme(validated.Scheme)
	if schemeFamily != providerFamilyUnknown && schemeFamily != family {
		return merr.WrapErrServiceInternal(unsupportedServerSideCopyMessage)
	}
	return nil
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

func snapshotCrossBucketEndpointAllowlist() string {
	return paramtable.Get().DataCoordCfg.SnapshotCrossBucketEndpointAllowlist.GetValue()
}
