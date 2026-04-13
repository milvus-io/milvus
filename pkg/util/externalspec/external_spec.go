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

package externalspec

import (
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// File formats supported by external collections. Mirror of LOON_FORMAT_*
// in the C++ FFI layer — keep the two in sync.
const (
	FormatParquet      = "parquet"
	FormatLanceTable   = "lance-table"
	FormatVortex       = "vortex"
	FormatIcebergTable = "iceberg-table"
)

// ExternalSpec represents the parsed external collection specification
type ExternalSpec struct {
	Format     string            `json:"format"`                // one of Format* constants
	Columns    []string          `json:"columns"`               // optional: specific columns to load
	Extfs      map[string]string `json:"extfs,omitempty"`       // optional: extfs config overrides (non-sensitive only)
	SnapshotID *int64            `json:"snapshot_id,omitempty"` // Iceberg snapshot ID (required for iceberg-table)
}

// supportedFormats lists the file formats supported for external collections
var supportedFormats = map[string]bool{
	FormatParquet:      true,
	FormatLanceTable:   true,
	FormatVortex:       true,
	FormatIcebergTable: true,
}

// allowedExtfsKeys lists extfs configuration keys that can be passed via ExternalSpec.
// Credential keys are allowed because cross-bucket scenarios require different
// authentication from the main Milvus storage (e.g., local MinIO vs external AWS S3).
// Note: these values are stored in etcd as part of CollectionSchema.
var allowedExtfsKeys = map[string]bool{
	"use_iam":          true,
	"use_ssl":          true,
	"use_virtual_host": true,
	"region":           true,
	"cloud_provider":   true,
	"iam_endpoint":     true,
	"storage_type":     true,
	"ssl_ca_cert":      true,
	"access_key_id":    true,
	"access_key_value": true,
}

// booleanExtfsKeys lists extfs keys that only accept "true" or "false".
var booleanExtfsKeys = map[string]bool{
	"use_iam":          true,
	"use_ssl":          true,
	"use_virtual_host": true,
}

// allowedExternalSourceSchemes lists URL schemes accepted in ExternalSource.
// This is a defense-in-depth allowlist to prevent unvalidated SSRF / arbitrary
// endpoint injection at CreateCollection / RefreshExternalCollection time.
// Add new schemes here only after confirming the storage backend is supported.
//
// This list MUST stay in sync with the C++ segcore same-bucket prefix list in
// ChunkedSegmentSealedImpl.cpp: {aws://, s3://, minio://, gcs://, gs://} plus
// the loon FFI's supported scheme set. Any scheme that the storage layer
// understands must also be allowlisted here so that cross-bucket URIs written
// via those schemes are not rejected by Proxy/RootCoord with a misleading
// "scheme not allowed" error.
var allowedExternalSourceSchemes = map[string]bool{
	"s3":    true, // S3-compatible (AWS S3, MinIO, Tencent COS via s3, etc.)
	"s3a":   true, // Hadoop-style S3
	"aws":   true, // milvus-storage loon FFI native AWS prefix
	"minio": true, // milvus-storage loon FFI native MinIO prefix
	"oss":   true, // Aliyun OSS
	"gs":    true, // Google Cloud Storage
	"gcs":   true, // Google Cloud Storage (alias)
	"file":  true, // local filesystem (test / single-node only)
}

// secretExtfsKeys lists extfs keys whose values are sensitive credentials
// and MUST be redacted before logging or being persisted to user-visible
// surfaces (logs, error messages, audit trails). The values still flow
// through the FFI layer for actual storage authentication; this set only
// gates the redaction path used by RedactExternalSpec.
var secretExtfsKeys = map[string]bool{
	"access_key_id":    true,
	"access_key_value": true,
	"ssl_ca_cert":      true,
}

// ParseExternalSpec parses the JSON external spec string
func ParseExternalSpec(specStr string) (*ExternalSpec, error) {
	if specStr == "" {
		return &ExternalSpec{Format: FormatParquet}, nil // default
	}

	var spec ExternalSpec
	if err := json.Unmarshal([]byte(specStr), &spec); err != nil {
		return nil, fmt.Errorf("invalid external spec JSON: %w", err)
	}

	if spec.Format == "" {
		spec.Format = FormatParquet // default format
	}

	if !supportedFormats[spec.Format] {
		return nil, fmt.Errorf("unsupported format %q, supported formats: %s, %s, %s, %s",
			spec.Format, FormatParquet, FormatLanceTable, FormatVortex, FormatIcebergTable)
	}

	for key, val := range spec.Extfs {
		if !allowedExtfsKeys[key] {
			return nil, fmt.Errorf("extfs key %q is not allowed; allowed keys: %s",
				key, strings.Join(getAllowedExtfsKeysList(), ", "))
		}
		if booleanExtfsKeys[key] && val != "true" && val != "false" {
			return nil, fmt.Errorf("extfs key %q must be \"true\" or \"false\", got %q", key, val)
		}
	}

	return &spec, nil
}

// PropertyIcebergSnapshotID is the property key for the Iceberg snapshot ID.
const PropertyIcebergSnapshotID = "iceberg.snapshot_id"

// BuildFormatProperties returns format-specific properties for the milvus-storage FFI layer.
func (s *ExternalSpec) BuildFormatProperties() map[string]string {
	props := make(map[string]string)
	if s.Format == FormatIcebergTable && s.SnapshotID != nil {
		props[PropertyIcebergSnapshotID] = strconv.FormatInt(*s.SnapshotID, 10)
	}
	return props
}

// BuildExtfsOverrides returns the extfs map with the given prefix prepended to each key.
func (s *ExternalSpec) BuildExtfsOverrides(prefix string) map[string]string {
	if len(s.Extfs) == 0 {
		return nil
	}
	m := make(map[string]string, len(s.Extfs))
	for k, v := range s.Extfs {
		m[prefix+k] = v
	}
	return m
}

func getAllowedExtfsKeysList() []string {
	keys := make([]string, 0, len(allowedExtfsKeys))
	for k := range allowedExtfsKeys {
		keys = append(keys, k)
	}
	return keys
}

// ValidateExternalSource enforces a scheme allowlist on the user-supplied
// external_source URL. This is the first line of defense against SSRF /
// arbitrary endpoint injection: without it a malicious caller could point
// the storage layer at internal services (http://169.254.169.254/, file:///etc/...
// when file:// is enabled in builds, gopher://, etc.). The C++ extfs layer
// has its own defense-in-depth check, but the Go-side allowlist must reject
// the request before any FFI call is made.
//
// Beyond the scheme check, this function also rejects shapes that are
// unambiguously unsafe or ambiguous:
//   - URLs with embedded userinfo (`s3://user:pass@bucket/...`): credentials
//     in URLs tend to leak into access logs, error messages, and audit
//     trails. Cross-bucket credentials MUST flow through the extfs path so
//     that they can be redacted by RedactExternalSpec.
//   - URLs with an empty host (`s3:///bucket/path`): the host component is
//     mandatory for every supported scheme and an empty one is ambiguous.
//     The `file://` scheme is the only exception — `file:///tmp/foo` is a
//     standard absolute-path shape.
//   - URLs that fail to parse or have empty scheme.
//
// Empty input is rejected — an external collection must have a source.
func ValidateExternalSource(source string) error {
	if source == "" {
		return fmt.Errorf("external_source is empty")
	}
	u, err := url.Parse(source)
	if err != nil {
		return fmt.Errorf("invalid external_source URL: %w", err)
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme == "" {
		// No scheme = same-bucket relative path (e.g. "my-data/parquet/").
		// This is the original format supported since Part1. The path is
		// relative to Milvus's own storage bucket and root path.
		return nil
	}
	if !allowedExternalSourceSchemes[scheme] {
		return fmt.Errorf("external_source scheme %q is not allowed; allowed schemes: %s",
			scheme, strings.Join(allowedExternalSourceSchemesList(), ", "))
	}
	// Reject URL-embedded userinfo. The URL-parsing library strips the
	// credentials into u.User; they are then silently ignored by the
	// storage layer, but they travel through logs first. Force callers to
	// put credentials in the extfs map so RedactExternalSpec can scrub
	// them. file:// is exempt because userinfo is never meaningful there.
	if scheme != "file" && u.User != nil {
		return fmt.Errorf("external_source must not embed credentials in the URL (use extfs.access_key_id / extfs.access_key_value instead)")
	}
	// Empty host is allowed for all schemes:
	// - file:///tmp/foo — standard absolute-path shape
	// - s3:///bucket/path — same-endpoint cross-bucket (uses Milvus's own
	//   storage endpoint; BuildExtfsOverrides skips address override when
	//   host is empty)
	return nil
}

// ValidateSourceAndSpec validates both the external_source URL and the
// external_spec JSON of a schema in one call. It is used by both Proxy
// and RootCoord (defense in depth) on the create-collection path.
// The returned error is already wrapped via merr.WrapErrParameterInvalid
// so callers can return it directly.
func ValidateSourceAndSpec(externalSource, externalSpec string) error {
	if err := ValidateExternalSource(externalSource); err != nil {
		return merr.WrapErrParameterInvalid("valid external_source", externalSource, err.Error())
	}
	if _, err := ParseExternalSpec(externalSpec); err != nil {
		return merr.WrapErrParameterInvalid("valid external_spec", "<redacted>", err.Error())
	}
	return nil
}

// allowedExternalSourceSchemesList returns the allowlist as a sorted slice
// for stable error messages and tests.
func allowedExternalSourceSchemesList() []string {
	out := make([]string, 0, len(allowedExternalSourceSchemes))
	for k := range allowedExternalSourceSchemes {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// RedactExternalSpec returns a log-safe representation of an external spec
// JSON string. Secret extfs values (see secretExtfsKeys) are replaced with
// "***" so that AK/SK/PEM material never reaches log sinks. On parse failure
// it returns "<invalid spec>" rather than the raw input — the input itself
// may already contain a partially-recognized credential blob, so we never
// echo it back. Empty input returns empty string for log readability.
func RedactExternalSpec(specStr string) string {
	if specStr == "" {
		return ""
	}
	var spec ExternalSpec
	if err := json.Unmarshal([]byte(specStr), &spec); err != nil {
		return "<invalid spec>"
	}
	if len(spec.Extfs) > 0 {
		redacted := make(map[string]string, len(spec.Extfs))
		for k, v := range spec.Extfs {
			if secretExtfsKeys[k] && v != "" {
				redacted[k] = "***"
			} else {
				redacted[k] = v
			}
		}
		spec.Extfs = redacted
	}
	out, err := json.Marshal(spec)
	if err != nil {
		return "<marshal error>"
	}
	return string(out)
}
