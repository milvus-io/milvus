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
	"encoding/json"
	"net/url"
	"strings"

	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/util/externalspec"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// #nosec G101 -- this is an external spec field name, not a credential value.
const snapshotExtfsKeyCredentialJSON = "credential_json"

// #nosec G101 -- this is an external spec field name, not a credential value.
const snapshotExtfsKeySourceSAS = "source_sas_token"

// snapshotExtfsOnlyKeys are snapshot-specific and unknown to the generic
// external-spec parser, so parseSnapshotExternalSpec strips them before that
// parse and re-injects them afterwards.
var snapshotExtfsOnlyKeys = map[string]struct{}{
	snapshotExtfsKeyCredentialJSON: {},
	snapshotExtfsKeySourceSAS:      {},
}

var snapshotExtfsKeys = map[string]struct{}{
	externalspec.ExtfsKeyAccessKeyID:    {},
	externalspec.ExtfsKeyAccessKeyValue: {},
	externalspec.ExtfsKeyUseIAM:         {},
	externalspec.ExtfsKeyRegion:         {},
	externalspec.ExtfsKeyCloudProvider:  {},
	externalspec.ExtfsKeyIAMEndpoint:    {},
	externalspec.ExtfsKeyStorageType:    {},
	externalspec.ExtfsKeySSLCACert:      {},
	externalspec.ExtfsKeyUseSSL:         {},
	externalspec.ExtfsKeyUseVirtualHost: {},
	snapshotExtfsKeyCredentialJSON:      {},
	snapshotExtfsKeySourceSAS:           {},
}

func parseSnapshotForeignURI(direction Direction, foreignURI string) (bucket, root, scheme, endpoint string, err error) {
	parseURI := ParseForeignURI
	if direction == DirectionCopySource {
		parseURI = ParseForeignRootURI
	}
	bucket, objectKey, endpointHost, err := parseURI(foreignURI)
	if err != nil {
		return "", "", "", "", err
	}
	parsedURI, err := url.Parse(foreignURI)
	if err != nil {
		return "", "", "", "", merr.WrapErrParameterInvalidMsg("invalid foreign_uri: %s", err.Error())
	}
	if direction == DirectionRestore && (parsedURI.Scheme == "" || parsedURI.Host == "") {
		return "", "", "", "", merr.WrapErrParameterInvalidMsg(
			"restore snapshot metadata URI must be a complete URI with scheme and host",
		)
	}
	foreignRoot, err := DeriveForeignRoot(direction, objectKey)
	if err != nil {
		return "", "", "", "", err
	}
	return bucket, foreignRoot, strings.ToLower(parsedURI.Scheme), endpointHost, nil
}

func applySnapshotExternalSpecToConfig(
	cfg *objectstorage.Config,
	scheme string,
	endpoint string,
	externalSpec string,
) (hasSpec bool, storageType string, err error) {
	parsed, err := parseSnapshotExternalSpec(externalSpec)
	if err != nil {
		return false, "", err
	}
	hasSpec = strings.TrimSpace(externalSpec) != ""
	if hasSpec && len(parsed.Extfs) == 0 {
		return false, "", merr.WrapErrParameterInvalidMsg(
			"external_spec.extfs is required when external_spec is set",
		)
	}

	if err := rejectUnsupportedSnapshotExtfs(parsed.Extfs); err != nil {
		return false, "", err
	}
	// Snapshot APIs intentionally mirror Milvus instance storage config:
	// instance credentials, IAM mode, or raw AK/SK. Generic role_arn,
	// service-account impersonation, and dual credential modes are not accepted
	// here because there is no corresponding instance-config contract. The one
	// exception is the Azure source SAS: it authorizes the read side of a
	// cross-account server-side copy, which no destination credential can do,
	// and is not a credential mode of its own.
	if err := validateCredentialModes(parsed.Extfs); err != nil {
		return false, "", err
	}
	uriIdentity, transportKnown, err := applySnapshotURILocationToConfig(cfg, scheme, endpoint)
	if err != nil {
		return false, "", err
	}
	if !hasSpec {
		if uriIdentity.azureAccount != "" {
			configuredEndpoint, err := effectiveAzureSnapshotEndpoint(cfg)
			if err != nil {
				return false, "", err
			}
			if !strings.EqualFold(configuredEndpoint, endpoint) {
				return false, "", merr.WrapErrParameterInvalidMsg(
					"snapshot URI Azure account does not match the instance storage credential",
				)
			}
		}
		return false, "", nil
	}

	extfs := parsed.Extfs
	cloudProvider := strings.ToLower(strings.TrimSpace(extfs[externalspec.ExtfsKeyCloudProvider]))
	region := strings.TrimSpace(extfs[externalspec.ExtfsKeyRegion])
	if cloudProvider != "" && uriIdentity.cloudProvider != "" && cloudProvider != uriIdentity.cloudProvider {
		return false, "", merr.WrapErrParameterInvalidMsg(
			"external_spec cloud_provider %q does not match snapshot URI provider %q",
			cloudProvider,
			uriIdentity.cloudProvider,
		)
	}
	if region != "" && uriIdentity.region != "" && !strings.EqualFold(region, uriIdentity.region) {
		return false, "", merr.WrapErrParameterInvalidMsg(
			"external_spec region %q does not match snapshot URI region %q",
			region,
			uriIdentity.region,
		)
	}
	if cloudProvider != "" && uriIdentity.cloudProvider == "" {
		cfg.CloudProvider = cloudProvider
	}
	if region != "" && uriIdentity.region == "" {
		cfg.Region = region
	}
	if value := strings.TrimSpace(extfs[externalspec.ExtfsKeyIAMEndpoint]); value != "" {
		cfg.IAMEndpoint = value
	}
	// Keep the instance CA because the MinIO client applies SslCACert through
	// the process-wide SSL_CERT_FILE environment variable.

	if value, set := extfs[externalspec.ExtfsKeyUseIAM]; set {
		cfg.UseIAM = value == "true"
		if cfg.UseIAM {
			// Azure uses AccessKeyID as the storage account name even when
			// authentication comes from managed identity.
			if !strings.EqualFold(cfg.CloudProvider, objectstorage.CloudProviderAzure) {
				cfg.AccessKeyID = ""
			}
			cfg.SecretAccessKeyID = ""
			cfg.GcpCredentialJSON = ""
		}
	}
	if value, set := extfs[externalspec.ExtfsKeyUseSSL]; set {
		requestedUseSSL := value == "true"
		if transportKnown && requestedUseSSL != cfg.UseSSL {
			return false, "", merr.WrapErrParameterInvalidMsg(
				"external_spec use_ssl=%t conflicts with snapshot URI transport",
				requestedUseSSL,
			)
		}
		cfg.UseSSL = requestedUseSSL
	}
	if value, set := extfs[externalspec.ExtfsKeyUseVirtualHost]; set {
		cfg.UseVirtualHost = value == "true"
	}

	accessKeyID := strings.TrimSpace(extfs[externalspec.ExtfsKeyAccessKeyID])
	secretKey := strings.TrimSpace(extfs[externalspec.ExtfsKeyAccessKeyValue])
	credentialJSON := strings.TrimSpace(extfs[snapshotExtfsKeyCredentialJSON])
	if uriIdentity.azureAccount != "" && accessKeyID != "" &&
		!strings.EqualFold(uriIdentity.azureAccount, accessKeyID) {
		return false, "", merr.WrapErrParameterInvalidMsg(
			"external_spec Azure account %q does not match snapshot URI account %q",
			accessKeyID,
			uriIdentity.azureAccount,
		)
	}
	if credentialJSON != "" {
		if !strings.EqualFold(cfg.CloudProvider, objectstorage.CloudProviderGCPNative) {
			return false, "", merr.WrapErrParameterInvalidMsg(
				"extfs.%s requires cloud_provider=%q",
				snapshotExtfsKeyCredentialJSON,
				objectstorage.CloudProviderGCPNative,
			)
		}
		cfg.GcpCredentialJSON = credentialJSON
		cfg.AccessKeyID = ""
		cfg.SecretAccessKeyID = ""
		cfg.UseIAM = false
	}
	if accessKeyID != "" || secretKey != "" {
		cfg.AccessKeyID = accessKeyID
		cfg.SecretAccessKeyID = secretKey
		cfg.GcpCredentialJSON = ""
		cfg.UseIAM = false
		if strings.EqualFold(cfg.CloudProvider, objectstorage.CloudProviderAzure) {
			cfg.IgnoreAzureConnectionString = true
		}
	}

	if sourceSAS := strings.TrimSpace(extfs[snapshotExtfsKeySourceSAS]); sourceSAS != "" {
		if !strings.EqualFold(cfg.CloudProvider, objectstorage.CloudProviderAzure) {
			return false, "", merr.WrapErrParameterInvalidMsg(
				"extfs.%s requires cloud_provider=%q",
				snapshotExtfsKeySourceSAS,
				objectstorage.CloudProviderAzure,
			)
		}
		cfg.AzureSourceSAS = strings.TrimPrefix(sourceSAS, "?")
	}

	if value := strings.ToLower(strings.TrimSpace(extfs[externalspec.ExtfsKeyStorageType])); value != "" {
		switch value {
		case "remote", "minio", "opendal":
			storageType = value
		default:
			return false, "", merr.WrapErrParameterInvalidMsg("extfs.storage_type %q is not supported for snapshot", value)
		}
	}

	if endpoint == "" && cloudProvider != "" {
		derived := externalspec.DeriveEndpoint(cloudProvider, region)
		if strings.TrimSpace(derived) == "" {
			if strings.EqualFold(cloudProvider, objectstorage.CloudProviderGCPNative) {
				cfg.Address = ""
				return true, storageType, nil
			}
			return false, "", merr.WrapErrParameterInvalidMsg(
				"extfs.cloud_provider %q requires a derivable region or an endpoint in foreign URI",
				cloudProvider,
			)
		}
		if err := applyEndpointToConfig(cfg, derived); err != nil {
			return false, "", err
		}
	}
	return true, storageType, nil
}

func applySnapshotURILocationToConfig(
	cfg *objectstorage.Config,
	scheme string,
	endpoint string,
) (storageEndpointIdentity, bool, error) {
	if cfg == nil {
		return storageEndpointIdentity{}, false, merr.WrapErrServiceInternalMsg("snapshot storage config is nil")
	}
	scheme = strings.ToLower(strings.TrimSpace(scheme))
	identity := inferStorageEndpointIdentity(endpoint)
	schemeProvider, _ := providerInfoFromScheme(scheme)
	if identity.cloudProvider != "" && schemeProvider != "" && identity.cloudProvider != schemeProvider {
		return storageEndpointIdentity{}, false, merr.WrapErrParameterInvalidMsg(
			"snapshot URI scheme %q does not match endpoint provider %q",
			scheme,
			identity.cloudProvider,
		)
	}
	if identity.cloudProvider == "" {
		identity.cloudProvider = schemeProvider
	}

	transportKnown := false
	if identity.cloudProvider == objectstorage.CloudProviderAzure {
		if endpoint != "" {
			if identity.azureSuffix == "" {
				configuredEndpoint, err := effectiveAzureSnapshotEndpoint(cfg)
				if err != nil {
					return storageEndpointIdentity{}, false, err
				}
				if !strings.EqualFold(configuredEndpoint, endpoint) {
					return storageEndpointIdentity{}, false, merr.WrapErrParameterInvalidMsg(
						"Azure snapshot URI endpoint %q does not match the configured endpoint",
						endpoint,
					)
				}
				cfg.CloudProvider = objectstorage.CloudProviderAzure
				cfg.UseSSL = true
				return identity, true, nil
			}
			cfg.Address = identity.azureSuffix
			if identity.azureAccount != "" {
				cfg.AccessKeyID = identity.azureAccount
			}
		}
		cfg.CloudProvider = objectstorage.CloudProviderAzure
		cfg.UseSSL = true
		return identity, true, nil
	}

	if endpoint != "" {
		if err := applyEndpointToConfig(cfg, endpoint); err != nil {
			return storageEndpointIdentity{}, false, err
		}
	}
	if identity.cloudProvider != "" {
		cfg.CloudProvider = identity.cloudProvider
	}
	if identity.region != "" {
		cfg.Region = identity.region
	}

	switch scheme {
	case "https":
		cfg.UseSSL = true
		transportKnown = true
	case "http":
		cfg.UseSSL = false
		transportKnown = true
	case "gs", "gcs":
		cfg.Address = ""
		cfg.UseSSL = true
		transportKnown = true
	}
	return identity, transportKnown, nil
}

func parseSnapshotExternalSpec(externalSpec string) (*externalspec.ExternalSpec, error) {
	if strings.TrimSpace(externalSpec) == "" {
		return externalspec.ParseExternalSpec(externalSpec)
	}
	var snapshotSpec externalspec.ExternalSpec
	if err := json.Unmarshal([]byte(externalSpec), &snapshotSpec); err != nil {
		return nil, merr.WrapErrParameterInvalidErr(err, "invalid external spec JSON")
	}
	snapshotOnly := make(map[string]string)
	for key := range snapshotExtfsOnlyKeys {
		if value, ok := snapshotSpec.Extfs[key]; ok {
			snapshotOnly[key] = value
			delete(snapshotSpec.Extfs, key)
		}
	}
	if len(snapshotOnly) == 0 {
		return externalspec.ParseExternalSpec(externalSpec)
	}
	sanitizedSpec, err := json.Marshal(snapshotSpec)
	if err != nil {
		return nil, merr.WrapErrParameterInvalidErr(err, "failed to validate external_spec")
	}
	parsed, err := externalspec.ParseExternalSpec(string(sanitizedSpec))
	if err != nil {
		return nil, err
	}
	if parsed.Extfs == nil {
		parsed.Extfs = make(map[string]string)
	}
	for key, value := range snapshotOnly {
		parsed.Extfs[key] = value
	}
	return parsed, nil
}

func rejectUnsupportedSnapshotExtfs(extfs map[string]string) error {
	for key := range extfs {
		if _, ok := snapshotExtfsKeys[key]; !ok {
			return merr.WrapErrParameterInvalidMsg(
				"extfs.%s is not supported for snapshot foreign storage",
				key,
			)
		}
	}
	return nil
}

func validateCredentialModes(extfs map[string]string) error {
	if useIAM, ok := extfs[externalspec.ExtfsKeyUseIAM]; ok &&
		!strings.EqualFold(strings.TrimSpace(useIAM), "true") {
		return merr.WrapErrParameterInvalidMsg(
			"extfs.use_iam=false is not supported for snapshot foreign storage; omit use_iam or use raw credentials",
		)
	}

	accessKeyID := strings.TrimSpace(extfs[externalspec.ExtfsKeyAccessKeyID])
	secretKey := strings.TrimSpace(extfs[externalspec.ExtfsKeyAccessKeyValue])
	_, accessKeyIDSet := extfs[externalspec.ExtfsKeyAccessKeyID]
	_, secretKeySet := extfs[externalspec.ExtfsKeyAccessKeyValue]
	hasRawField := accessKeyIDSet || secretKeySet
	if hasRawField && (accessKeyID == "" || secretKey == "") {
		return merr.WrapErrParameterInvalidMsg(
			"extfs.access_key_id and extfs.access_key_value must be set together and non-empty for snapshot foreign storage",
		)
	}
	hasRaw := accessKeyID != "" && secretKey != ""
	hasUseIAM := strings.EqualFold(strings.TrimSpace(extfs[externalspec.ExtfsKeyUseIAM]), "true")
	credentialJSON := strings.TrimSpace(extfs[snapshotExtfsKeyCredentialJSON])
	_, credentialJSONSet := extfs[snapshotExtfsKeyCredentialJSON]
	if credentialJSONSet && credentialJSON == "" {
		return merr.WrapErrParameterInvalidMsg(
			"extfs.%s must be non-empty for snapshot foreign storage",
			snapshotExtfsKeyCredentialJSON,
		)
	}
	hasCredentialJSON := credentialJSON != ""
	credentialModeCount := 0
	for _, enabled := range []bool{hasRaw, hasUseIAM, hasCredentialJSON} {
		if enabled {
			credentialModeCount++
		}
	}
	if credentialModeCount > 1 {
		return merr.WrapErrParameterInvalidMsg(
			"snapshot foreign storage credential modes are mutually exclusive: use_iam, raw credentials, and credential_json",
		)
	}
	sourceSAS := strings.TrimSpace(extfs[snapshotExtfsKeySourceSAS])
	_, sourceSASSet := extfs[snapshotExtfsKeySourceSAS]
	if sourceSASSet && sourceSAS == "" {
		return merr.WrapErrParameterInvalidMsg(
			"extfs.%s must be non-empty for snapshot foreign storage",
			snapshotExtfsKeySourceSAS,
		)
	}
	return nil
}
