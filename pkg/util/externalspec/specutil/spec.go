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

package specutil

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
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
	ExtfsKeyBucketName              = "bucket_name"
	ExtfsKeyIAMEndpoint             = "iam_endpoint"
	ExtfsKeyStorageType             = "storage_type"
	ExtfsKeySSLCACert               = "ssl_ca_cert"
	ExtfsKeyUseSSL                  = "use_ssl"
	ExtfsKeyUseVirtualHost          = "use_virtual_host"
	ExtfsKeyLoadFrequency           = "load_frequency"
)

// ExternalSpec represents the parsed external collection specification.
type ExternalSpec struct {
	Format     string            `json:"format"`          // one of Format* constants
	Columns    []string          `json:"columns"`         // optional: specific columns to load
	Extfs      map[string]string `json:"extfs,omitempty"` // optional: extfs config overrides
	SnapshotID *int64            `json:"snapshot_id,omitempty"`
}

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
}

var booleanExtfsKeys = map[string]bool{
	ExtfsKeyUseIAM:         true,
	ExtfsKeyUseSSL:         true,
	ExtfsKeyUseVirtualHost: true,
	ExtfsKeyAnonymous:      true,
}

// ParseExternalSpec parses and validates the JSON shape of an external spec.
func ParseExternalSpec(specStr string) (*ExternalSpec, error) {
	if specStr == "" {
		return &ExternalSpec{Format: FormatParquet}, nil
	}

	var spec ExternalSpec
	if err := json.Unmarshal([]byte(specStr), &spec); err != nil {
		return nil, fmt.Errorf("invalid external spec JSON: %s", err.Error())
	}

	if spec.Format == "" {
		spec.Format = FormatParquet
	}

	if !supportedFormats[spec.Format] {
		return nil, fmt.Errorf("unsupported format %q, supported formats: %s",
			spec.Format, strings.Join(sortedKeys(supportedFormats), ", "))
	}

	for key, val := range spec.Extfs {
		if !allowedExtfsKeys[key] {
			return nil, fmt.Errorf("extfs key %q is not allowed; allowed keys: %s",
				key, strings.Join(sortedKeys(allowedExtfsKeys), ", "))
		}
		if booleanExtfsKeys[key] && val != "true" && val != "false" {
			return nil, fmt.Errorf("extfs key %q must be \"true\" or \"false\", got %q", key, val)
		}
	}

	return &spec, nil
}

func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
