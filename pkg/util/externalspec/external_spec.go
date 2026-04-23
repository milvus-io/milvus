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
// `address` and `bucket_name` are allowed so users can write standard AWS-style
// URIs (`s3://<bucket>/<key>`) and let the endpoint come from extfs instead of
// the URI host. When `address` is present in extfs, BuildExtfsOverrides flips
// to AWS-style URI interpretation (URI host = bucket).
//
// AWS-style activation: URI interpretation switches to AWS-style whenever
// there is an effective endpoint (see EffectiveAddressFromExtfs). That means
// `cloud_provider` + `region` alone — without an explicit `address` — also
// activate the flip, via Tier-2 derivation. Users who write a Milvus-form
// URI (`s3://<endpoint>/<bucket>/...`) alongside provider/region MUST ensure
// the URI host matches the derived endpoint exactly (the equality guard in
// NormalizeExternalSource and BuildExtfsOverrides relies on a string match),
// otherwise the URI will be silently rewritten as AWS-style and the original
// endpoint will be misinterpreted as a bucket. Self-hosted S3-compatible
// targets (MinIO, R2, Backblaze) must NOT set cloud_provider/region that
// conflicts with the true storage — use `address` explicitly instead.
// Note: these values are stored in etcd as part of CollectionSchema.
var allowedExtfsKeys = map[string]bool{
	"use_iam":             true,
	"use_ssl":             true,
	"use_virtual_host":    true,
	"region":              true,
	"cloud_provider":      true,
	"iam_endpoint":        true,
	"storage_type":        true,
	"ssl_ca_cert":         true,
	"access_key_id":       true,
	"access_key_value":    true,
	"role_arn":            true,
	"session_name":        true,
	"external_id":         true,
	"load_frequency":      true,
	"address":             true,
	"bucket_name":         true,
	"gcp_credential_json": true,
	// gcp_target_service_account is the target GCP service account email
	// for cross-tenant access via IAM service account impersonation. It is
	// the GCP equivalent of role_arn: Milvus's VM-default SA calls
	// iamcredentials.generateAccessToken to exchange for a short-lived
	// impersonated token, then uses that token to read the target bucket.
	// Requires roles/iam.serviceAccountTokenCreator on the target SA. Only
	// valid for scheme=gs/gcs or cloud_provider=gcp.
	"gcp_target_service_account": true,
	// anonymous=true is an explicit opt-in for unauthenticated (public bucket)
	// access. It is a dedicated flag rather than an inference from empty
	// credentials because the five credential modes (AK/SK, role_arn, use_iam,
	// gcp_target_service_account, anonymous) must be mutually explicit —
	// silently falling through to anonymous when credentials are missing
	// would mask configuration errors.
	"anonymous": true,
}

// booleanExtfsKeys lists extfs keys that only accept "true" or "false".
var booleanExtfsKeys = map[string]bool{
	"use_iam":          true,
	"use_ssl":          true,
	"use_virtual_host": true,
	"anonymous":        true,
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

func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// ValidateExternalSource enforces a scheme allowlist on the user-supplied
// external_source URL. This is the first line of defense against SSRF /
// arbitrary endpoint injection: without it a malicious caller could point
// the storage layer at internal services (http://169.254.169.254/,
// gopher://, etc.). The C++ extfs layer has its own defense-in-depth check,
// but the Go-side allowlist must reject the request before any FFI call is
// made.
//
// Every external_source MUST be a fully qualified URI in one of two shapes:
//   - Milvus form: `scheme://endpoint/bucket/key` — URI host is the storage
//     endpoint, first path segment is the bucket.
//   - AWS form: `scheme://bucket/key` with `spec.extfs.address` (or derivable
//     endpoint via cloud_provider + region) — URI host is the bucket, entire
//     path is the key. Mirrors aws-cli / boto3 convention.
//
// Legacy shorthand forms that relied on implicit inheritance from Milvus's
// internal storage config are intentionally rejected:
//   - bare relative path (no scheme, e.g. "my-data/parquet/")
//   - empty-host URI (e.g. "s3:///bucket/key")
//
// Both were shortcuts that implicitly pulled credentials and endpoint from
// `fs.*` baseline. External-table semantics require the caller to declare
// credentials and endpoint explicitly in spec.extfs so nothing leaks from
// Milvus's internal config into the per-collection extfs namespace.
//
// Other rejected shapes:
//   - Empty input (an external collection must have a source).
//   - Scheme outside allowedExternalSourceSchemes (SSRF guard).
//   - URLs with embedded userinfo (`s3://user:pass@bucket/...`) — credentials
//     in URLs leak through access logs; use extfs.access_key_id /
//     access_key_value (redacted by RedactExternalSpec) instead.
//   - URLs that fail to parse.
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
		return fmt.Errorf("external_source must have an explicit scheme (e.g. s3://, aws://, gs://); bare relative paths are no longer supported — write the full URI so credentials and endpoint flow through spec.extfs")
	}
	if !allowedExternalSourceSchemes[scheme] {
		return fmt.Errorf("external_source scheme %q is not allowed; allowed schemes: %s",
			scheme, strings.Join(sortedKeys(allowedExternalSourceSchemes), ", "))
	}
	if u.User != nil {
		return fmt.Errorf("external_source must not embed credentials in the URL (use extfs.access_key_id / extfs.access_key_value instead)")
	}
	if u.Host == "" {
		return fmt.Errorf("external_source must have a non-empty host; write the full URI (e.g. s3://bucket/key with spec.extfs.address, or s3://endpoint/bucket/key) — shorthand like s3:///bucket/key is no longer supported")
	}
	return nil
}

// ValidateSourceAndSpec validates both the external_source URL and the
// external_spec JSON of a schema in one call. It is used by both Proxy
// and RootCoord (defense in depth) on the create-collection path.
// The returned error is already wrapped via merr.WrapErrParameterInvalid
// so callers can return it directly.
//
// Beyond URL and JSON shape, this call also enforces ValidateExtfsComplete:
// spec.extfs must declare a credential mode (AK/SK, role_arn, use_iam, or
// anonymous) and — for AWS-family schemes — a region. Fail fast at the
// create/refresh entry point instead of letting silent mis-auth surface as
// a 403 deep inside the storage layer.
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

// ValidateExtfsComplete enforces that spec.extfs is self-sufficient — every
// field needed for authentication and endpoint resolution is either present
// in spec.extfs or mechanically derivable from (URI, cloud_provider, region).
// Nothing is inherited from Milvus's internal `fs.*` storage config.
//
// Credential mode is explicit four-way choice:
//  1. AK/SK: access_key_id AND access_key_value both non-empty
//  2. STS: role_arn non-empty (implies use_iam semantics)
//  3. Pod / default credential chain: use_iam=true (alone or with iam_endpoint)
//  4. Public bucket: anonymous=true
//
// Exactly one mode must be chosen (modes are mutually exclusive). Silently
// allowing zero modes would fall through to anonymous (SDK default) and
// 403 at runtime; enforcing choice surfaces the error at create time.
//
// Region is mandatory for AWS-family schemes (s3, s3a, aws) because SigV4
// signing requires it. Region can come from spec.extfs.region, or be
// derived from (cloud_provider, region) via DeriveEndpoint — both paths
// eventually populate the FFI `region` property.
//
// Endpoint requirement: the request must resolve to a concrete endpoint.
// Acceptable sources:
//   - spec.extfs.address (Tier-1 explicit)
//   - Tier-2 derived from (cloud_provider, region)
//   - URI host (Milvus form, handled by caller)
//
// At least one must yield a value; otherwise the caller cannot connect.
func ValidateExtfsComplete(externalSource string, extfs map[string]string) error {
	// Credential mode exclusivity — exactly one must be set.
	hasAKSK := extfs["access_key_id"] != "" && extfs["access_key_value"] != ""
	hasAKOnly := (extfs["access_key_id"] != "") != (extfs["access_key_value"] != "")
	hasRoleARN := extfs["role_arn"] != ""
	// use_iam counts as a mode only when not paired with role_arn (role_arn
	// already implies IAM-based auth via STS; do not double count).
	hasUseIAMAlone := extfs["use_iam"] == "true" && !hasRoleARN
	hasGCPImpersonation := extfs["gcp_target_service_account"] != ""
	hasAnonymous := extfs["anonymous"] == "true"

	if hasAKOnly {
		return fmt.Errorf("extfs.access_key_id and extfs.access_key_value must be set together (found one without the other)")
	}

	modes := 0
	if hasAKSK {
		modes++
	}
	if hasRoleARN {
		modes++
	}
	if hasUseIAMAlone {
		modes++
	}
	if hasGCPImpersonation {
		modes++
	}
	if hasAnonymous {
		modes++
	}
	if modes == 0 {
		return fmt.Errorf("extfs credential mode missing: set exactly one of {access_key_id+access_key_value}, role_arn, use_iam=true, gcp_target_service_account, or anonymous=true")
	}
	if modes > 1 {
		return fmt.Errorf("extfs credential modes are mutually exclusive: set exactly one of AK/SK, role_arn, use_iam=true, gcp_target_service_account, or anonymous=true")
	}
	if hasAnonymous && (extfs["access_key_id"] != "" || extfs["access_key_value"] != "" || extfs["role_arn"] != "" || extfs["use_iam"] == "true" || extfs["gcp_target_service_account"] != "") {
		return fmt.Errorf("extfs.anonymous=true is mutually exclusive with access_key_id, access_key_value, role_arn, use_iam, and gcp_target_service_account")
	}

	// gcp_target_service_account is GCP-only — only valid with scheme gs/gcs
	// or cloud_provider=gcp. Using it with an AWS-family scheme signals a
	// configuration mistake (e.g. role_arn intended, SA email pasted by
	// accident) rather than a supported combination.
	if hasGCPImpersonation {
		u, err := url.Parse(externalSource)
		if err == nil && u.Scheme != "" {
			scheme := strings.ToLower(u.Scheme)
			cp := strings.ToLower(extfs["cloud_provider"])
			if scheme != "gs" && scheme != "gcs" && cp != "gcp" {
				return fmt.Errorf("extfs.gcp_target_service_account is only valid for GCP (scheme=gs/gcs or cloud_provider=gcp), got scheme=%q cloud_provider=%q", scheme, cp)
			}
		}
		// Minimal format check: every GCP service account email ends with
		// `.gserviceaccount.com` (user-managed, default compute, google-
		// managed all share this suffix). The IAM API ultimately rejects
		// malformed values; this is a fail-fast for typos.
		sa := extfs["gcp_target_service_account"]
		if !strings.Contains(sa, "@") || !strings.HasSuffix(sa, ".gserviceaccount.com") {
			return fmt.Errorf("extfs.gcp_target_service_account must be a GCP service account email ending in .gserviceaccount.com, got %q", sa)
		}
	}

	// Region check for AWS-family schemes.
	u, err := url.Parse(externalSource)
	if err == nil && u.Scheme != "" {
		scheme := strings.ToLower(u.Scheme)
		if awsFamilyScheme[scheme] {
			// Region can be explicit or derived via Tier-2 (cloud_provider+region).
			if extfs["region"] == "" {
				return fmt.Errorf("extfs.region is required for scheme %q (AWS-family schemes need region for SigV4 signing)", scheme)
			}
		}
	}

	// Endpoint resolvability: URI host (non-empty, guaranteed by
	// ValidateExternalSource) is always a valid source, so this check is
	// trivially satisfied. Kept as a documented invariant so future changes
	// that loosen ValidateExternalSource also consider endpoint sources.
	return nil
}

// awsFamilyScheme lists schemes that use AWS SigV4 signing. Region is
// mandatory for these; non-AWS schemes (gs/gcs for GCS, oss for Aliyun,
// minio for self-hosted) use different auth mechanisms that do not require
// explicit region for request signing.
var awsFamilyScheme = map[string]bool{
	"s3":  true,
	"s3a": true,
	"aws": true,
}

// DeriveEndpoint returns the storage endpoint URL for a (cloud_provider,
// region) pair when it can be mechanically derived from documented public
// endpoint patterns. It is the second-tier source of the effective endpoint
// used by NormalizeExternalSource / BuildExtfsOverrides when the caller
// omits `spec.extfs.address`.
//
// Coverage matches milvus-storage cpp/include/milvus-storage/filesystem/fs.h
// provider enum minus Azure: AWS, GCP, Aliyun OSS, Tencent COS, Huawei OBS.
// Azure Blob is intentionally excluded because its endpoint
// (`<account>.blob.core.windows.net`) carries an account-specific subdomain
// that cannot be reconstructed from (provider, region) alone — Azure users
// must provide extfs.address explicitly. Self-hosted S3-compatible targets
// (MinIO, R2, Backblaze B2, Wasabi) fall outside the provider enum entirely
// and likewise require an explicit address.
func DeriveEndpoint(cloudProvider, region string) (string, bool) {
	switch strings.ToLower(cloudProvider) {
	case "aws":
		if region == "" {
			return "", false
		}
		// AWS China regions live under a separate top-level domain.
		if strings.HasPrefix(region, "cn-") {
			return "https://s3." + region + ".amazonaws.com.cn", true
		}
		return "https://s3." + region + ".amazonaws.com", true
	case "gcp":
		// GCS uses a single global endpoint for all regions; region is a
		// bucket property, not part of the endpoint URL.
		return "https://storage.googleapis.com", true
	case "aliyun":
		if region == "" {
			return "", false
		}
		return "https://oss-" + region + ".aliyuncs.com", true
	case "tencent":
		if region == "" {
			return "", false
		}
		return "https://cos." + region + ".myqcloud.com", true
	case "huawei":
		if region == "" {
			return "", false
		}
		return "https://obs." + region + ".myhuaweicloud.com", true
	}
	return "", false
}

// EffectiveAddressFromExtfs returns the effective endpoint for a spec extfs
// map using the three-tier priority:
//  1. explicit spec.extfs.address
//  2. derived from (cloud_provider, region) via DeriveEndpoint
//  3. "" (signaling "no extfs-supplied endpoint — defer to URI host",
//     i.e. the pre-existing Milvus-form semantics)
//
// Extfs keys are unprefixed in this call (callers at the packed FFI layer
// that work with prefixed maps are expected to strip the prefix first).
func EffectiveAddressFromExtfs(extfs map[string]string) string {
	if len(extfs) == 0 {
		return ""
	}
	if a, ok := extfs["address"]; ok && a != "" {
		return a
	}
	if d, ok := DeriveEndpoint(extfs["cloud_provider"], extfs["region"]); ok {
		return d
	}
	return ""
}

// NormalizeExternalSource rewrites an AWS-style URI (`scheme://bucket/key`) into
// the canonical Milvus form (`scheme://endpoint/bucket/key`) whenever the
// caller supplies an endpoint through ExternalSpec.extfs — either explicitly
// via `address` or derivable from `cloud_provider + region`. The canonical
// form is what gets persisted into CollectionSchema, so every downstream
// consumer (DataCoord explore, DataNode fetch, QueryNode LoadColumnGroups,
// index build) sees a URI whose host is the storage endpoint — matching the
// URI shape produced by milvus-storage explore.
//
// Three-tier priority for the effective endpoint:
//  1. `spec.extfs.address` explicit (wins outright)
//  2. Derived from `spec.extfs.cloud_provider + region` for the 5 public
//     clouds milvus-storage supports with a region-based endpoint pattern
//  3. None of the above — URI is returned unchanged (Milvus-form semantics)
//
// Equality guard: if the URI host already equals the effective endpoint, the
// user is writing a Milvus-form URI that happens to redundantly carry
// cloud_provider + region in the spec. The guard avoids misinterpreting the
// endpoint as a bucket in that case and returns the source untouched, leaving
// the existing Milvus-form path to handle it.
//
// Relative paths, empty-host URIs, and URIs whose host is not a safe bucket
// name are returned as-is so downstream validators can surface a precise
// error rather than seeing a silently-mangled URI.
func NormalizeExternalSource(externalSource, externalSpec string) string {
	if externalSource == "" || externalSpec == "" {
		return externalSource
	}
	spec, err := ParseExternalSpec(externalSpec)
	if err != nil {
		return externalSource
	}
	return RewriteAWSStyleToMilvusForm(externalSource, EffectiveAddressFromExtfs(spec.Extfs))
}

// RewriteAWSStyleToMilvusForm is the shared core rewrite used by both
// NormalizeExternalSource (ExternalSpec JSON entry point) and the prefix-aware
// variant in internal/storagev2/packed. Given a pre-computed effective
// endpoint, it rewrites an AWS-style URI (`scheme://bucket/key`) into the
// canonical Milvus form (`scheme://endpoint/bucket/key`). All safety gates
// (empty endpoint, invalid URL, empty/unsafe host, equality guard) are
// applied inside this single function so the two entry points cannot drift.
func RewriteAWSStyleToMilvusForm(externalSource, effectiveAddr string) string {
	if effectiveAddr == "" {
		return externalSource
	}
	u, err := url.Parse(externalSource)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return externalSource
	}
	if !IsSafeURIHost(u.Host) {
		return externalSource
	}
	endpoint := StripURLScheme(effectiveAddr)
	if u.Host == endpoint {
		return externalSource
	}
	normalized := &url.URL{
		Scheme:   u.Scheme,
		Host:     endpoint,
		Path:     "/" + u.Host + u.Path,
		RawQuery: u.RawQuery,
		Fragment: u.Fragment,
	}
	return normalized.String()
}

// IsSafeURIHost accepts hosts composed only of RFC-3986 unreserved + `:` (port)
// + `-` + `.` characters. The whitelist is deliberately stricter than what
// url.Parse tolerates so the reassembled URI cannot carry characters that
// would alter parsing on the C++ side (bracketed IPv6 literals, userinfo,
// whitespace, percent-encodings we did not introduce ourselves).
// Accepting a whitelist also future-proofs the check — new RFC-tolerated
// characters do not silently become valid bucket names.
func IsSafeURIHost(host string) bool {
	if host == "" {
		return false
	}
	for i := 0; i < len(host); i++ {
		c := host[i]
		switch {
		case c >= 'a' && c <= 'z':
		case c >= 'A' && c <= 'Z':
		case c >= '0' && c <= '9':
		case c == '.' || c == '-' || c == ':':
		default:
			return false
		}
	}
	return true
}

// StripURLScheme removes a leading "http://" or "https://" from address,
// leaving bare host[:port]. Matches the behavior of the FFI-side helper so
// the persisted URI does not carry a nested scheme when reassembled.
func StripURLScheme(address string) string {
	if strings.HasPrefix(address, "http://") {
		return address[len("http://"):]
	}
	if strings.HasPrefix(address, "https://") {
		return address[len("https://"):]
	}
	return address
}

// isSafeURIHost / stripURLScheme are retained as lowercase wrappers because
// existing test files reference them; delegating to the exported versions
// keeps a single implementation.
func isSafeURIHost(host string) bool       { return IsSafeURIHost(host) }
func stripURLScheme(address string) string { return StripURLScheme(address) }

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
