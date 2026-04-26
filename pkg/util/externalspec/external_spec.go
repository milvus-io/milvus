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

// Scheme* are URL schemes accepted in external_source.
const (
	SchemeAWS   = "aws"
	SchemeS3    = "s3"
	SchemeS3A   = "s3a"
	SchemeGS    = "gs"
	SchemeGCS   = "gcs"
	SchemeMinIO = "minio"
	SchemeOSS   = "oss"
	SchemeCOS   = "cos"
	SchemeOBS   = "obs"
	SchemeAzure = "azure"
)

// CloudProvider* are values accepted for extfs.cloud_provider.
const (
	CloudProviderAWS     = "aws"
	CloudProviderGCP     = "gcp"
	CloudProviderAliyun  = "aliyun"
	CloudProviderTencent = "tencent"
	CloudProviderHuawei  = "huawei"
	CloudProviderAzure   = "azure"
)

// ExternalSpec represents the parsed external collection specification
type ExternalSpec struct {
	Format     string            `json:"format"`                // one of Format* constants
	Columns    []string          `json:"columns"`               // optional: specific columns to load
	Extfs      map[string]string `json:"extfs,omitempty"`       // optional: extfs config overrides (non-sensitive only)
	SnapshotID *int64            `json:"snapshot_id,string,omitempty"` // Iceberg snapshot ID (required for iceberg-table); string-encoded to preserve int64 precision in JS/JSON clients
}

// supportedFormats lists the file formats supported for external collections
var supportedFormats = map[string]bool{
	FormatParquet:      true,
	FormatLanceTable:   true,
	FormatVortex:       true,
	FormatIcebergTable: true,
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
	SchemeS3:    true,
	SchemeS3A:   true,
	SchemeAWS:   true,
	SchemeMinIO: true,
	SchemeOSS:   true,
	SchemeCOS:   true,
	SchemeOBS:   true,
	SchemeGS:    true,
	SchemeGCS:   true,
	SchemeAzure: true,
}

// secretExtfsKeys lists extfs keys whose values are sensitive credentials
// and MUST be redacted before logging or being persisted to user-visible
// surfaces (logs, error messages, audit trails). The values still flow
// through the FFI layer for actual storage authentication; this set only
// gates the redaction path used by RedactExternalSpec.
var secretExtfsKeys = map[string]bool{
	ExtfsKeyAccessKeyID:    true,
	ExtfsKeyAccessKeyValue: true,
	ExtfsKeySSLCACert:      true,
	ExtfsKeyExternalID:     true, // STS shared secret; confused-deputy guard.
}

// ParseExternalSpec parses the JSON external spec string
func ParseExternalSpec(specStr string) (*ExternalSpec, error) {
	if specStr == "" {
		return &ExternalSpec{Format: FormatParquet}, nil // default
	}

	var spec ExternalSpec
	if err := json.Unmarshal([]byte(specStr), &spec); err != nil {
		return nil, merr.WrapErrParameterInvalidMsg("invalid external spec JSON: %s", err.Error())
	}

	if spec.Format == "" {
		spec.Format = FormatParquet // default format
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

func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// ValidateExternalSource requires a fully-qualified URI: non-empty,
// allowlisted scheme (SSRF guard), non-empty host, no embedded userinfo.
// Accepts two URI shapes — Milvus form (host=endpoint, path[0]=bucket) and
// AWS form (host=bucket, endpoint from spec.extfs).
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
		return fmt.Errorf("external_source must have an explicit scheme (e.g. s3://, aws://, gs://)")
	}
	if !allowedExternalSourceSchemes[scheme] {
		return fmt.Errorf("external_source scheme %q is not allowed; allowed schemes: %s",
			scheme, strings.Join(sortedKeys(allowedExternalSourceSchemes), ", "))
	}
	if u.User != nil {
		return fmt.Errorf("external_source must not embed credentials in the URL (use extfs.access_key_id / extfs.access_key_value instead)")
	}
	if u.Host == "" {
		return fmt.Errorf("external_source must have a non-empty host (e.g. s3://bucket/key or s3://endpoint/bucket/key)")
	}
	return nil
}

// ValidateSourceAndSpec validates URL + JSON shape + ValidateExtfsComplete.
// Errors are wrapped via merr.WrapErrParameterInvalid for direct return.
// Called from Proxy and RootCoord (defense in depth) on create-collection.
func ValidateSourceAndSpec(externalSource, externalSpec string) error {
	if err := ValidateExternalSource(externalSource); err != nil {
		return merr.WrapErrParameterInvalid("valid external_source", externalSource, err.Error())
	}
	spec, err := ParseExternalSpec(externalSpec)
	if err != nil {
		return merr.WrapErrParameterInvalid("valid external_spec", "<redacted>", err.Error())
	}
	if err := ValidateExtfsComplete(externalSource, spec.Extfs); err != nil {
		return merr.WrapErrParameterInvalid("valid external_spec", "<redacted>", err.Error())
	}
	return nil
}

// ValidateExtfsComplete requires spec.extfs to be self-sufficient: exactly one
// credential mode (AK/SK, role_arn, use_iam=true, gcp_target_service_account,
// anonymous=true), and region for AWS-family schemes. role_arn subsumes
// use_iam (do not double-count). No inheritance from Milvus fs.* config.
func ValidateExtfsComplete(externalSource string, extfs map[string]string) error {
	hasAKSK := extfs[ExtfsKeyAccessKeyID] != "" && extfs[ExtfsKeyAccessKeyValue] != ""
	hasAKOnly := (extfs[ExtfsKeyAccessKeyID] != "") != (extfs[ExtfsKeyAccessKeyValue] != "")
	hasRoleARN := extfs[ExtfsKeyRoleARN] != ""
	hasUseIAMAlone := extfs[ExtfsKeyUseIAM] == "true" && !hasRoleARN
	hasGCPImpersonation := extfs[ExtfsKeyGCPTargetServiceAccount] != ""
	hasAnonymous := extfs[ExtfsKeyAnonymous] == "true"

	if hasAKOnly {
		return fmt.Errorf("extfs.access_key_id and extfs.access_key_value must be set together (found one without the other)")
	}

	modes := 0
	for _, set := range []bool{hasAKSK, hasRoleARN, hasUseIAMAlone, hasGCPImpersonation, hasAnonymous} {
		if set {
			modes++
		}
	}
	if modes == 0 {
		return fmt.Errorf("extfs credential mode missing: set exactly one of {access_key_id+access_key_value}, role_arn, use_iam=true, gcp_target_service_account, or anonymous=true")
	}
	if modes > 1 {
		return fmt.Errorf("extfs credential modes are mutually exclusive: set exactly one of AK/SK, role_arn, use_iam=true, gcp_target_service_account, or anonymous=true")
	}

	// Parse once; caller's ValidateExternalSource has already guaranteed a scheme.
	u, err := url.Parse(externalSource)
	scheme := ""
	if err == nil {
		scheme = strings.ToLower(u.Scheme)
	}

	if hasGCPImpersonation {
		cp := strings.ToLower(extfs[ExtfsKeyCloudProvider])
		if scheme != "" && scheme != SchemeGS && scheme != SchemeGCS && cp != CloudProviderGCP {
			return fmt.Errorf("extfs.gcp_target_service_account is only valid for GCP (scheme=gs/gcs or cloud_provider=gcp), got scheme=%q cloud_provider=%q", scheme, cp)
		}
		sa := extfs[ExtfsKeyGCPTargetServiceAccount]
		if !strings.Contains(sa, "@") || !strings.HasSuffix(sa, ".gserviceaccount.com") {
			return fmt.Errorf("extfs.gcp_target_service_account must be a GCP service account email ending in .gserviceaccount.com, got %q", sa)
		}
	}

	if awsFamilyScheme[scheme] && extfs[ExtfsKeyRegion] == "" {
		return fmt.Errorf("extfs.region is required for scheme %q (AWS-family schemes need region for SigV4 signing)", scheme)
	}

	// Azure consistency: scheme=azure requires cloud_provider=azure (or unset),
	// and cloud_provider=azure requires scheme=azure. Pairing guards against
	// misconfigured dispatch to a non-Azure storage backend.
	cpLower := strings.ToLower(extfs[ExtfsKeyCloudProvider])
	if scheme == SchemeAzure && cpLower != "" && cpLower != CloudProviderAzure {
		return fmt.Errorf("scheme=azure requires extfs.cloud_provider to be %q or unset, got %q", CloudProviderAzure, cpLower)
	}
	if cpLower == CloudProviderAzure && scheme != "" && scheme != SchemeAzure {
		return fmt.Errorf("extfs.cloud_provider=azure requires scheme=azure, got scheme=%q", scheme)
	}

	// Two-form URI contract: Milvus-form (scheme://endpoint/bucket/key) has
	// URI.host either a cloud-family endpoint (e.g. *.amazonaws.com,
	// *.aliyuncs.com) or a custom endpoint containing path-segment count
	// >= 2. AWS-form (scheme://bucket/key) has a bucket-like URI.host (not
	// cloud-family) and endpoint must be derivable from cloud_provider +
	// region. Reject AWS-form configurations whose DeriveEndpoint returns
	// empty — the endpoint would resolve to the URI host (= bucket) at
	// runtime and fail opaquely.
	if u != nil {
		// Cloud-family URI.host → Milvus-form, URI is authoritative.
		// Covers global/accelerate/dualstack/FIPS/VPC/sovereign endpoint
		// variants that DeriveEndpoint does not produce verbatim.
		if IsCloudEndpointHost(u.Host) {
			return nil
		}
		pathSegs := len(strings.Split(strings.Trim(u.Path, "/"), "/"))
		if strings.Trim(u.Path, "/") == "" {
			pathSegs = 0
		}
		if pathSegs <= 1 {
			// AWS-form: endpoint must be derivable. Infer cloud_provider
			// from scheme when omitted (conventional mapping). minio stays
			// empty — it has no canonical cp and must use Milvus-form URI.
			effectiveCP := cpLower
			if effectiveCP == "" {
				switch scheme {
				case SchemeS3, SchemeS3A, SchemeAWS:
					effectiveCP = CloudProviderAWS
				case SchemeGS, SchemeGCS:
					effectiveCP = CloudProviderGCP
				case SchemeOSS:
					effectiveCP = CloudProviderAliyun
				case SchemeCOS:
					effectiveCP = CloudProviderTencent
				case SchemeOBS:
					effectiveCP = CloudProviderHuawei
				}
			}
			if DeriveEndpoint(effectiveCP, extfs[ExtfsKeyRegion]) == "" {
				return fmt.Errorf("cannot resolve endpoint for %q: set extfs.cloud_provider + extfs.region, or use Milvus-form URI scheme://<endpoint>/<bucket>/<key>", externalSource)
			}
		}
	}
	return nil
}

// IsCloudEndpointHost returns true when host matches a known cloud provider
// domain family (AWS / GCP / Aliyun / Tencent / Huawei / Azure — all
// endpoint variants including global, accelerate, dualstack, FIPS, VPC,
// sovereign). Used by AWS-form disambiguation: when URI.host belongs to a
// cloud family, the URI is treated as Milvus-form regardless of whether
// DeriveEndpoint(cp, region) string-matches.
// Keep list in sync with C++ IsCloudEndpointHost in loon_ffi/util.cpp.
func IsCloudEndpointHost(host string) bool {
	h := strings.ToLower(host)
	suffixes := []string{
		".amazonaws.com", ".amazonaws.com.cn",
		".googleapis.com",
		".aliyuncs.com",
		".myqcloud.com",
		".myhuaweicloud.com",
		".core.windows.net", ".core.chinacloudapi.cn",
		".core.usgovcloudapi.net", ".core.cloudapi.de",
	}
	for _, s := range suffixes {
		if strings.HasSuffix(h, s) {
			return true
		}
	}
	return false
}

// DeriveEndpoint mirrors C++ externalspec::DeriveEndpoint in
// internal/core/src/storage/loon_ffi/util.cpp. Keep the two in lockstep.
// Returns empty string when the (cloud_provider, region) pair does not
// resolve to a concrete endpoint; callers must treat empty as "not derivable"
// and require the user to supply a Milvus-form URI instead.
func DeriveEndpoint(cloudProvider, region string) string {
	cp := strings.ToLower(cloudProvider)
	switch cp {
	case CloudProviderAWS:
		if region == "" {
			return ""
		}
		if strings.HasPrefix(region, "cn-") {
			return "https://s3." + region + ".amazonaws.com.cn"
		}
		return "https://s3." + region + ".amazonaws.com"
	case CloudProviderGCP:
		return "https://storage.googleapis.com"
	case CloudProviderAliyun:
		if region == "" {
			return ""
		}
		return "https://oss-" + region + ".aliyuncs.com"
	case CloudProviderTencent:
		if region == "" {
			return ""
		}
		return "https://cos." + region + ".myqcloud.com"
	case CloudProviderHuawei:
		if region == "" {
			return ""
		}
		return "https://obs." + region + ".myhuaweicloud.com"
	case CloudProviderAzure:
		// Empty region returns empty — Milvus-form URI is required for
		// non-public Azure deployments. Public cloud callers can pass an
		// explicit region prefix ("public" etc.) or just use Milvus-form
		// URI azure://core.windows.net/<container>/<blob>.
		r := strings.ToLower(region)
		if r == "" {
			return ""
		}
		if strings.HasPrefix(r, "china") {
			return "core.chinacloudapi.cn"
		}
		if strings.HasPrefix(r, "usgov") || strings.HasPrefix(r, "usdod") {
			return "core.usgovcloudapi.net"
		}
		if strings.HasPrefix(r, "germany") {
			return "core.cloudapi.de"
		}
		return "core.windows.net"
	}
	return ""
}

// awsFamilyScheme lists schemes that use AWS SigV4 signing (region required).
var awsFamilyScheme = map[string]bool{
	SchemeS3:  true,
	SchemeS3A: true,
	SchemeAWS: true,
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
