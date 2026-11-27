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

// Package specutil owns the shared external_spec parser used by API validation
// and lower storage helpers without importing the higher-level externalspec
// package.
package specutil

import (
	"bytes"
	"encoding/json"
	"sort"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// File formats supported by external collections. Mirror of LOON_FORMAT_*
// in the C++ FFI layer -- keep the two in sync.
const (
	FormatParquet      = "parquet"
	FormatLanceTable   = "lance-table"
	FormatVortex       = "vortex"
	FormatIcebergTable = "iceberg-table"
	FormatMilvusTable  = "milvus-table"
)

// ExtfsKey* are the canonical spec.extfs key names. Use these instead of
// string literals; keep in sync with kAllowedExtfsSpecKeys / kExtfsFields in
// internal/core/src/storage/loon_ffi/util.cpp.
const (
	ExtfsKeyAccessKeyID             = "access_key_id"
	ExtfsKeyAccessKeyValue          = "access_key_value"
	ExtfsKeyRoleARN                 = "role_arn"
	ExtfsKeySessionName             = "session_name"
	ExtfsKeyExternalID              = "external_id"
	ExtfsKeyUseIAM                  = "use_iam"
	ExtfsKeyAnonymous               = "anonymous"
	ExtfsKeyGCPTargetServiceAccount = "gcp_target_service_account"
	ExtfsKeyRegion                  = "region"
	ExtfsKeyCloudProvider           = "cloud_provider"
	ExtfsKeyEndpointURL             = "endpoint_url"
	ExtfsKeyBucketName              = "bucket_name"
	ExtfsKeyIAMEndpoint             = "iam_endpoint"
	ExtfsKeyStorageType             = "storage_type"
	ExtfsKeySSLCACert               = "ssl_ca_cert"
	ExtfsKeyUseSSL                  = "use_ssl"
	ExtfsKeyUseVirtualHost          = "use_virtual_host"
	ExtfsKeyLoadFrequency           = "load_frequency"
	// #nosec G101 -- configuration key name, not a credential.
	ExtfsKeyCredentialEndpoint = "credential_endpoint"
	ExtfsKeyAzureClientID      = "azure_client_id"
	ExtfsKeyAzureTenantID      = "azure_tenant_id"
	// #nosec G101 -- configuration key name, not a credential.
	ExtfsKeyAzureCredentialEndpoint = "azure_credential_endpoint"
)

// ExternalSpec represents the parsed external collection specification.
type ExternalSpec struct {
	Format     string            `json:"format"`          // one of Format* constants
	Columns    []string          `json:"columns"`         // optional: specific columns to load
	Extfs      map[string]string `json:"extfs,omitempty"` // optional: extfs config overrides
	SnapshotID *int64            `json:"snapshot_id,omitempty"`
}

// UnmarshalJSON accepts snapshot_id as either a JSON number or a decimal
// string so old clients and JSON-preserving paths parse to the same structure.
func (s *ExternalSpec) UnmarshalJSON(data []byte) error {
	type externalSpec ExternalSpec
	var raw struct {
		externalSpec
		SnapshotID json.RawMessage `json:"snapshot_id"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	*s = ExternalSpec(raw.externalSpec)
	if parsed, err := parseOptionalInt64(raw.SnapshotID); err != nil {
		return err
	} else {
		s.SnapshotID = parsed
	}
	return nil
}

// parseOptionalInt64 parses an optional integer that may be encoded as a JSON
// number, string, or null.
func parseOptionalInt64(raw json.RawMessage) (*int64, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var n int64
	if err := json.Unmarshal(raw, &n); err == nil {
		return &n, nil
	}
	var str string
	if err := json.Unmarshal(raw, &str); err != nil {
		return nil, err
	}
	parsed, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

// MarshalJSON emits snapshot_id as a string to preserve int64 precision across
// JavaScript JSON clients.
func (s ExternalSpec) MarshalJSON() ([]byte, error) {
	type externalSpec ExternalSpec
	var raw struct {
		externalSpec
		SnapshotID *string `json:"snapshot_id,omitempty"`
	}
	raw.externalSpec = externalSpec(s)
	if s.SnapshotID != nil {
		str := strconv.FormatInt(*s.SnapshotID, 10)
		raw.SnapshotID = &str
	}
	return json.Marshal(raw)
}

// supportedFormats lists the file formats supported for external collections.
var supportedFormats = map[string]bool{
	FormatParquet:      true,
	FormatLanceTable:   true,
	FormatVortex:       true,
	FormatIcebergTable: true,
	FormatMilvusTable:  true,
}

// allowedExtfsKeys gates keys permitted in ExternalSpec.extfs. Persisted in
// etcd as part of CollectionSchema. Keep in sync with C++ kAllowedExtfsSpecKeys.
var allowedExtfsKeys = map[string]bool{
	ExtfsKeyUseIAM:                  true,
	ExtfsKeyUseSSL:                  true,
	ExtfsKeyUseVirtualHost:          true,
	ExtfsKeyRegion:                  true,
	ExtfsKeyCloudProvider:           true,
	ExtfsKeyEndpointURL:             true,
	ExtfsKeyIAMEndpoint:             true,
	ExtfsKeyStorageType:             true,
	ExtfsKeySSLCACert:               true,
	ExtfsKeyAccessKeyID:             true,
	ExtfsKeyAccessKeyValue:          true,
	ExtfsKeyRoleARN:                 true,
	ExtfsKeySessionName:             true,
	ExtfsKeyExternalID:              true,
	ExtfsKeyLoadFrequency:           true,
	ExtfsKeyBucketName:              true,
	ExtfsKeyGCPTargetServiceAccount: true,
	ExtfsKeyAnonymous:               true,
	ExtfsKeyCredentialEndpoint:      true,
	ExtfsKeyAzureClientID:           true,
	ExtfsKeyAzureTenantID:           true,
	ExtfsKeyAzureCredentialEndpoint: true,
}

var booleanExtfsKeys = map[string]bool{
	ExtfsKeyUseIAM:         true,
	ExtfsKeyUseSSL:         true,
	ExtfsKeyUseVirtualHost: true,
	ExtfsKeyAnonymous:      true,
}

// specKeys are the top-level keys ExternalSpec decodes. Keep in sync with the
// json tags on ExternalSpec and kExternalSpecKeys in
// internal/core/src/storage/loon_ffi/util.cpp.
var specKeys = map[string]bool{
	"format":      true,
	"columns":     true,
	"extfs":       true,
	"snapshot_id": true,
}

// ParseExternalSpec parses and validates the JSON shape of an external spec.
func ParseExternalSpec(specStr string) (*ExternalSpec, error) {
	if specStr == "" {
		return &ExternalSpec{Format: FormatParquet}, nil
	}

	if err := CheckSpecKeys(specStr); err != nil {
		return nil, err
	}

	var spec ExternalSpec
	if err := json.Unmarshal([]byte(specStr), &spec); err != nil {
		return nil, merr.WrapErrParameterInvalidErr(err, "invalid external spec JSON")
	}

	if spec.Format == "" {
		spec.Format = FormatParquet
	}

	if !supportedFormats[spec.Format] {
		return nil, merr.WrapErrParameterInvalidMsg("unsupported format %q, supported formats: %s",
			spec.Format, strings.Join(sortedKeys(supportedFormats), ", "))
	}

	for key, val := range spec.Extfs {
		if !allowedExtfsKeys[key] {
			return nil, merr.WrapErrParameterInvalidMsg("extfs key %q is not allowed; allowed keys: %s",
				key, strings.Join(sortedKeys(allowedExtfsKeys), ", "))
		}
		if booleanExtfsKeys[key] && val != "true" && val != "false" {
			return nil, merr.WrapErrParameterInvalidMsg("extfs key %q must be \"true\" or \"false\", got %q", key, val)
		}
	}

	return &spec, nil
}

// CanonicalSpecKey returns the top-level ExternalSpec key that encoding/json
// reads key as: the one that equals key ignoring case, under the folding of
// strings.EqualFold. It returns key itself when key matches no ExternalSpec
// field, so a lone "Format" is read as "format" and "comment" stays "comment".
func CanonicalSpecKey(key string) string {
	if specKeys[key] {
		return key
	}
	for name := range specKeys {
		if strings.EqualFold(key, name) {
			return name
		}
	}
	return key
}

// CheckSpecKeys rejects a spec that sets one key twice. encoding/json reads a
// top-level key that equals an ExternalSpec field ignoring case as that field,
// lets a later occurrence override an earlier one and merges two extfs objects
// key by key, so nothing says which of two such keys should win. The error
// names the keys. It is returned when, after JSON unescaping:
//   - a top-level key or an extfs key appears more than once;
//   - two top-level keys match the same ExternalSpec field ignoring case, such
//     as "extfs" and "Extfs";
//   - an extfs key differs from an allowed extfs key only by case. extfs is a
//     map, so encoding/json keeps such a key as written and the allowlist in
//     ParseExternalSpec would reject it anyway.
//
// A lone non-canonical top-level spelling such as "Format" or "EXTFS" is
// accepted: encoding/json reads it as the canonical key, and so does the C++
// reader (InjectExternalSpecProperties in
// internal/core/src/storage/loon_ffi/util.cpp), which rejects the same specs.
// Unknown top-level keys are not folded, so only their exact repeats are
// rejected. Input that is not a JSON object is left to json.Unmarshal.
//
// ParseExternalSpec runs this check. A caller that decodes a spec into
// ExternalSpec and encodes it again before ParseExternalSpec must run it on the
// original spec, since decoding merges such keys.
func CheckSpecKeys(specStr string) error {
	keys, values, ok := objectMembers([]byte(specStr))
	if !ok {
		return nil
	}
	if err := checkTopLevelKeys(keys); err != nil {
		return err
	}
	for i, key := range keys {
		if CanonicalSpecKey(key) != "extfs" {
			continue
		}
		if extfsKeys, _, ok := objectMembers(values[i]); ok {
			return checkExtfsKeys(extfsKeys)
		}
	}
	return nil
}

// checkTopLevelKeys rejects a repeated key, and a second key that matches the
// same ExternalSpec field ignoring case.
func checkTopLevelKeys(keys []string) error {
	seen := make(map[string]bool, len(keys))
	firstByName := make(map[string]string, len(keys))
	for _, key := range keys {
		if seen[key] {
			return merr.WrapErrParameterInvalidMsg("external spec key %q is duplicated", key)
		}
		seen[key] = true
		name := CanonicalSpecKey(key)
		if !specKeys[name] {
			continue
		}
		if first, ok := firstByName[name]; ok {
			return merr.WrapErrParameterInvalidMsg(
				"external spec keys %q and %q differ only in case; set the key once", first, key)
		}
		firstByName[name] = key
	}
	return nil
}

// checkExtfsKeys rejects a repeated extfs key, and a key that differs from an
// allowed extfs key only by case.
func checkExtfsKeys(keys []string) error {
	seen := make(map[string]bool, len(keys))
	for _, key := range keys {
		if seen[key] {
			return merr.WrapErrParameterInvalidMsg("extfs key %q is duplicated", key)
		}
		seen[key] = true
		if allowedExtfsKeys[key] {
			continue
		}
		for name := range allowedExtfsKeys {
			if strings.EqualFold(key, name) {
				return merr.WrapErrParameterInvalidMsg("extfs key %q must be written as %q", key, name)
			}
		}
	}
	return nil
}

// objectMembers returns the keys, unescaped as encoding/json unescapes them,
// and the raw values of a JSON object in input order. ok is false when data is
// not a well-formed JSON object.
func objectMembers(data []byte) (keys []string, values []json.RawMessage, ok bool) {
	dec := json.NewDecoder(bytes.NewReader(data))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return nil, nil, false
	}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return nil, nil, false
		}
		key, isString := tok.(string)
		var value json.RawMessage
		if !isString || dec.Decode(&value) != nil {
			return nil, nil, false
		}
		keys = append(keys, key)
		values = append(values, value)
	}
	return keys, values, true
}

func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
