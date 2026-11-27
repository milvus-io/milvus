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
	"unicode"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// File formats supported by external collections. Mirror of LOON_FORMAT_*
// in the C++ FFI layer -- keep the two in sync.
const (
	FormatParquet        = "parquet"
	FormatLanceTable     = "lance-table"
	FormatVortex         = "vortex"
	FormatIcebergTable   = "iceberg-table"
	FormatPaimonTable    = "paimon-table"
	FormatMilvusTable    = "milvus-table"
	FormatDeltaLakeTable = "deltalake-table"
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
	ScanMode   string            `json:"scan_mode,omitempty"` // Paimon: auto, direct-file, or data-split
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
	FormatParquet:        true,
	FormatLanceTable:     true,
	FormatVortex:         true,
	FormatIcebergTable:   true,
	FormatPaimonTable:    true,
	FormatMilvusTable:    true,
	FormatDeltaLakeTable: true,
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

// knownSpecKeys are the top-level keys ExternalSpec decodes, plus
// cloud_extra, which a cloud layer in front of Milvus writes next to extfs.
var knownSpecKeys = []string{"format", "columns", "extfs", "snapshot_id", "scan_mode", "cloud_extra"}

// knownSpecKeysByFold maps foldKey of each known top-level key to the key.
var knownSpecKeysByFold = func() map[string]string {
	m := make(map[string]string, len(knownSpecKeys))
	for _, key := range knownSpecKeys {
		m[foldKey(key)] = key
	}
	return m
}()

// CanonicalSpecKey returns the known top-level key that equals key ignoring
// case (with the folding encoding/json uses for struct fields), or key itself.
func CanonicalSpecKey(key string) string {
	if canonical, ok := knownSpecKeysByFold[foldKey(key)]; ok {
		return canonical
	}
	return key
}

// ValidateExternalSpecKeys rejects a spec that sets one key more than once.
// encoding/json matches ExternalSpec fields ignoring case, so a lone "Format"
// or "EXTFS" is read as format or extfs; it stays accepted, and
// NormalizeExternalSpec gives it the canonical name for native code. Two keys
// that match the same field, such as "Extfs" and "extfs", are merged by
// encoding/json instead, so a layer that replaces only the exact "extfs" key
// would still pass the caller's other extfs keys to native code.
//
// The error names the keys. It is returned when, after JSON unescaping:
//   - a top-level key or an extfs key appears more than once;
//   - two top-level keys match the same known key ignoring case;
//   - two extfs keys are equal ignoring case.
//
// Unknown top-level keys are extension metadata that encoding/json does not
// fold, so only their exact repeats are rejected.
func ValidateExternalSpecKeys(specStr string) error {
	if specStr == "" {
		return nil
	}
	data := []byte(specStr)
	if !json.Valid(data) {
		return merr.WrapErrParameterInvalidMsg("invalid external spec JSON")
	}
	members, err := objectMembers(data)
	if err != nil {
		return err
	}
	if err := checkObjectKeys("external spec", members, func(key string) (string, bool) {
		known, ok := knownSpecKeysByFold[foldKey(key)]
		return known, ok
	}); err != nil {
		return err
	}
	for _, member := range members {
		if CanonicalSpecKey(member.key) != "extfs" {
			continue
		}
		extfs, err := objectMembers(member.value)
		if err != nil {
			return err
		}
		if err := checkObjectKeys("extfs", extfs, func(key string) (string, bool) {
			return foldKey(key), true
		}); err != nil {
			return err
		}
	}
	return nil
}

type objectMember struct {
	key   string
	value json.RawMessage
}

// objectMembers returns the members of a valid JSON object in input order,
// keeping repeated keys. Any other JSON value, including null, has none.
func objectMembers(data []byte) ([]objectMember, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	tok, err := dec.Token()
	if err != nil {
		return nil, merr.WrapErrParameterInvalidErr(err, "invalid external spec JSON")
	}
	if tok != json.Delim('{') {
		return nil, nil
	}
	var members []objectMember
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return nil, merr.WrapErrParameterInvalidErr(err, "invalid external spec JSON")
		}
		key, ok := tok.(string)
		if !ok {
			return nil, merr.WrapErrParameterInvalidMsg("invalid external spec JSON")
		}
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			return nil, merr.WrapErrParameterInvalidErr(err, "invalid external spec JSON")
		}
		members = append(members, objectMember{key: key, value: value})
	}
	return members, nil
}

// checkObjectKeys rejects a repeated key, and two different keys that group
// puts in the same group. Keys for which group returns false are only checked
// for repeats.
func checkObjectKeys(scope string, members []objectMember, group func(key string) (string, bool)) error {
	exact := make(map[string]bool, len(members))
	firstInGroup := make(map[string]string, len(members))
	for _, member := range members {
		if exact[member.key] {
			return merr.WrapErrParameterInvalidMsg("%s key %q appears more than once", scope, member.key)
		}
		exact[member.key] = true
		g, ok := group(member.key)
		if !ok {
			continue
		}
		if first, ok := firstInGroup[g]; ok {
			return merr.WrapErrParameterInvalidMsg(
				"%s keys %q and %q differ only in case; set the key once", scope, first, member.key)
		}
		firstInGroup[g] = member.key
	}
	return nil
}

// foldKey maps keys that strings.EqualFold treats as equal to the same
// string. encoding/json folds struct field names the same way.
func foldKey(key string) string {
	var b strings.Builder
	b.Grow(len(key))
	for _, r := range key {
		for {
			next := unicode.SimpleFold(r)
			if next <= r {
				r = next
				break
			}
			r = next
		}
		b.WriteRune(r)
	}
	return b.String()
}

// ParseExternalSpec parses and validates the JSON shape of an external spec.
func ParseExternalSpec(specStr string) (*ExternalSpec, error) {
	if specStr == "" {
		return &ExternalSpec{Format: FormatParquet}, nil
	}

	var spec ExternalSpec
	if err := json.Unmarshal([]byte(specStr), &spec); err != nil {
		return nil, merr.WrapErrParameterInvalidErr(err, "invalid external spec JSON")
	}
	if err := ValidateExternalSpecKeys(specStr); err != nil {
		return nil, err
	}

	if spec.Format == "" {
		spec.Format = FormatParquet
	}

	if !supportedFormats[spec.Format] {
		return nil, merr.WrapErrParameterInvalidMsg("unsupported format %q, supported formats: %s",
			spec.Format, strings.Join(sortedKeys(supportedFormats), ", "))
	}

	if spec.Format == FormatPaimonTable {
		if spec.ScanMode == "" {
			spec.ScanMode = "auto"
		}
		switch spec.ScanMode {
		case "auto", "direct-file", "data-split":
		default:
			return nil, merr.WrapErrParameterInvalidMsg(
				"unsupported Paimon scan_mode %q; allowed values: auto, direct-file, data-split", spec.ScanMode)
		}
	} else if spec.ScanMode != "" {
		return nil, merr.WrapErrParameterInvalidMsg("scan_mode is only supported for format %q", FormatPaimonTable)
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

// NormalizeExternalSpec decodes a spec and canonicalizes known top-level
// keys for case-sensitive native readers. Call it at native boundaries as well
// as for new inputs: persisted schemas may contain legacy case variants.
//
// A lone non-canonical spelling of a known key, such as "Format", is written
// under its canonical name. Specs that ValidateExternalSpecKeys rejects, such
// as "Extfs" next to "extfs", fail here too, so a persisted spec that set one
// key twice cannot reach native code as a merge of both values. Unknown values
// stay as raw JSON, preserving nested fields and numeric precision. Missing
// optional keys remain absent, so a missing scan_mode does not override reader
// defaults. API policy validation remains in ParseExternalSpec; native callers
// retain their existing handling of unknown formats and filesystem options.
func NormalizeExternalSpec(specStr string) (string, error) {
	if specStr == "" {
		return "", nil
	}
	var spec ExternalSpec
	if err := json.Unmarshal([]byte(specStr), &spec); err != nil {
		return "", merr.WrapErrParameterInvalidErr(err, "invalid external spec JSON")
	}
	if err := ValidateExternalSpecKeys(specStr); err != nil {
		return "", err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(specStr), &fields); err != nil {
		return "", merr.WrapErrParameterInvalidErr(err, "invalid external spec JSON")
	}
	// A top-level null has historically meant the default spec.
	if fields == nil {
		return specStr, nil
	}
	var snapshotID *string
	if spec.SnapshotID != nil {
		value := strconv.FormatInt(*spec.SnapshotID, 10)
		snapshotID = &value
	}
	known := map[string]any{
		"format": spec.Format, "columns": spec.Columns, "extfs": spec.Extfs,
		"snapshot_id": snapshotID, "scan_mode": spec.ScanMode,
	}
	normalized := make(map[string]any, len(fields))
	for key, raw := range fields {
		canonical, value := key, any(raw)
		for name, parsed := range known {
			if strings.EqualFold(key, name) {
				canonical, value = name, parsed
				break
			}
		}
		normalized[canonical] = value
	}
	encoded, err := json.Marshal(normalized)
	if err != nil {
		return "", merr.WrapErrServiceInternalErr(err, "marshal normalized external spec")
	}
	return string(encoded), nil
}

func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
