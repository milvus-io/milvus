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
	instanceCfg *objectstorage.Config,
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
	// instance credentials, IAM mode, or raw AK/SK. Generic role_arn, SAS,
	// service-account impersonation, and dual credential modes are not accepted
	// here because there is no corresponding instance-config contract.
	if err := validateCredentialModes(parsed.Extfs); err != nil {
		return false, "", err
	}
	if !hasSpec {
		return false, "", nil
	}

	if endpoint != "" {
		if err := applyEndpointToConfig(cfg, endpoint); err != nil {
			return false, "", err
		}
	}
	if scheme == "https" {
		cfg.UseSSL = true
	}

	extfs := parsed.Extfs
	cloudProvider := strings.ToLower(strings.TrimSpace(extfs[externalspec.ExtfsKeyCloudProvider]))
	region := strings.TrimSpace(extfs[externalspec.ExtfsKeyRegion])
	if cloudProvider != "" {
		cfg.CloudProvider = cloudProvider
	} else if schemeProvider, _ := providerInfoFromScheme(scheme); schemeProvider != "" && instanceCfg.CloudProvider != schemeProvider {
		cfg.CloudProvider = schemeProvider
	}
	if region != "" {
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
		cfg.UseSSL = value == "true"
	}
	if value, set := extfs[externalspec.ExtfsKeyUseVirtualHost]; set {
		cfg.UseVirtualHost = value == "true"
	}

	accessKeyID := strings.TrimSpace(extfs[externalspec.ExtfsKeyAccessKeyID])
	secretKey := strings.TrimSpace(extfs[externalspec.ExtfsKeyAccessKeyValue])
	credentialJSON := strings.TrimSpace(extfs[snapshotExtfsKeyCredentialJSON])
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

func parseSnapshotExternalSpec(externalSpec string) (*externalspec.ExternalSpec, error) {
	if strings.TrimSpace(externalSpec) == "" {
		return externalspec.ParseExternalSpec(externalSpec)
	}
	var snapshotSpec externalspec.ExternalSpec
	if err := json.Unmarshal([]byte(externalSpec), &snapshotSpec); err != nil {
		return nil, merr.WrapErrParameterInvalidErr(err, "invalid external spec JSON")
	}
	credentialJSON, ok := snapshotSpec.Extfs[snapshotExtfsKeyCredentialJSON]
	if !ok {
		return externalspec.ParseExternalSpec(externalSpec)
	}
	delete(snapshotSpec.Extfs, snapshotExtfsKeyCredentialJSON)
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
	parsed.Extfs[snapshotExtfsKeyCredentialJSON] = credentialJSON
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
	return nil
}
