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
	"encoding/json"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus/pkg/v3/util/externalspec"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const extfsKeySASToken = "sas_token"

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
}

func ValidateExternalSpec(
	ctx context.Context,
	direction Direction,
	foreignURI string,
	externalSpec string,
) (*ValidatedSpec, error) {
	_ = ctx
	return ValidateSnapshotForeignStorage(direction, foreignURI, externalSpec)
}

func ValidateSnapshotForeignStorage(
	direction Direction,
	foreignURI string,
	externalSpec string,
) (*ValidatedSpec, error) {
	if err := validateDirection(direction); err != nil {
		return nil, err
	}

	bucket, objectKey, endpointHost, err := ParseForeignURI(foreignURI)
	if err != nil {
		return nil, err
	}
	foreignRoot, err := DeriveForeignRoot(direction, objectKey)
	if err != nil {
		return nil, err
	}

	rawExtfs, err := parseRawExtfs(externalSpec)
	if err != nil {
		return nil, err
	}
	if err := rejectUnsupportedSnapshotAuth(rawExtfs); err != nil {
		return nil, err
	}
	if err := rejectUnmappableSnapshotExtfs(rawExtfs); err != nil {
		return nil, err
	}

	parsed, err := externalspec.ParseExternalSpec(externalSpec)
	if err != nil {
		return nil, err
	}
	hasSpec := strings.TrimSpace(externalSpec) != ""
	if hasSpec && len(parsed.Extfs) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg(
			"external_spec.extfs is required when external_spec is set",
		)
	}

	if err := validateCredentialModes(parsed.Extfs); err != nil {
		return nil, err
	}

	scheme := schemeOfForeignURI(foreignURI)
	validated := &ValidatedSpec{
		Direction:     direction,
		ForeignURI:    foreignURI,
		ForeignBucket: bucket,
		ForeignRoot:   foreignRoot,
		ExternalSpec:  strings.TrimSpace(externalSpec),
		Endpoint:      endpointHost,
		Scheme:        scheme,
		HasSpec:       hasSpec,
		HasLayer2:     hasSpec,
	}
	if err := fillValidatedSpecFromExtfs(validated, parsed.Extfs); err != nil {
		return nil, err
	}
	return validated, nil
}

func validateDirection(direction Direction) error {
	switch direction {
	case DirectionExport, DirectionRestore:
		return nil
	default:
		return merr.WrapErrParameterInvalidMsg("snapshot foreign storage direction %d is not supported", direction)
	}
}

func parseRawExtfs(spec string) (map[string]any, error) {
	if strings.TrimSpace(spec) == "" {
		return nil, nil
	}
	var raw struct {
		Extfs map[string]any `json:"extfs"`
	}
	if err := json.Unmarshal([]byte(spec), &raw); err != nil {
		return nil, merr.WrapErrParameterInvalidMsg("invalid external spec JSON: %s", err.Error())
	}
	return raw.Extfs, nil
}

func rejectUnsupportedSnapshotAuth(extfs map[string]any) error {
	for _, key := range []string{
		externalspec.ExtfsKeyRoleARN,
		externalspec.ExtfsKeySessionName,
		externalspec.ExtfsKeyExternalID,
		externalspec.ExtfsKeyGCPTargetServiceAccount,
		extfsKeySASToken,
		externalspec.ExtfsKeyAnonymous,
	} {
		if _, ok := extfs[key]; ok {
			return merr.WrapErrParameterInvalidMsg(
				"extfs.%s is not supported for snapshot foreign storage",
				key,
			)
		}
	}
	return nil
}

func rejectUnmappableSnapshotExtfs(extfs map[string]any) error {
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
	modes := 0
	for _, enabled := range []bool{hasRaw, hasUseIAM} {
		if enabled {
			modes++
		}
	}
	if modes > 1 {
		return merr.WrapErrParameterInvalidMsg(
			"snapshot foreign storage credential modes are mutually exclusive: use_iam and raw credentials",
		)
	}
	return nil
}

func fillValidatedSpecFromExtfs(validated *ValidatedSpec, extfs map[string]string) error {
	if extfs == nil {
		return nil
	}
	validated.CloudProvider = strings.ToLower(strings.TrimSpace(extfs[externalspec.ExtfsKeyCloudProvider]))
	validated.Region = strings.TrimSpace(extfs[externalspec.ExtfsKeyRegion])
	validated.IAMEndpoint = strings.TrimSpace(extfs[externalspec.ExtfsKeyIAMEndpoint])
	validated.SslCACert = strings.TrimSpace(extfs[externalspec.ExtfsKeySSLCACert])
	validated.RawAccessKeyID = strings.TrimSpace(extfs[externalspec.ExtfsKeyAccessKeyID])
	validated.RawSecretKey = strings.TrimSpace(extfs[externalspec.ExtfsKeyAccessKeyValue])

	if v := strings.TrimSpace(extfs[externalspec.ExtfsKeyUseIAM]); v != "" {
		parsed, err := strconv.ParseBool(v)
		if err != nil {
			return merr.WrapErrParameterInvalidMsg("invalid extfs.use_iam: %s", err.Error())
		}
		validated.UseIAM = parsed
		validated.UseIAMSet = true
	}
	if v := strings.TrimSpace(extfs[externalspec.ExtfsKeyUseSSL]); v != "" {
		parsed, err := strconv.ParseBool(v)
		if err != nil {
			return merr.WrapErrParameterInvalidMsg("invalid extfs.use_ssl: %s", err.Error())
		}
		validated.UseSSL = parsed
		validated.UseSSLSet = true
	}
	if v := strings.TrimSpace(extfs[externalspec.ExtfsKeyUseVirtualHost]); v != "" {
		parsed, err := strconv.ParseBool(v)
		if err != nil {
			return merr.WrapErrParameterInvalidMsg("invalid extfs.use_virtual_host: %s", err.Error())
		}
		validated.UseVirtualHost = parsed
		validated.UseVirtualHostSet = true
	}
	if v := strings.ToLower(strings.TrimSpace(extfs[externalspec.ExtfsKeyStorageType])); v != "" {
		switch v {
		case "remote", "minio", "opendal":
			validated.StorageType = v
		default:
			return merr.WrapErrParameterInvalidMsg("extfs.storage_type %q is not supported for snapshot", v)
		}
	}
	return nil
}

func schemeOfForeignURI(raw string) string {
	idx := strings.Index(raw, ":")
	if idx < 0 {
		return ""
	}
	return strings.ToLower(raw[:idx])
}
