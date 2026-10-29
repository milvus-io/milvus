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
	"net/url"
	"sort"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/externalspec/specutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// File formats supported by external collections. Mirror of LOON_FORMAT_*
// in the C++ FFI layer — keep the two in sync.
const (
	FormatParquet      = specutil.FormatParquet
	FormatLanceTable   = specutil.FormatLanceTable
	FormatVortex       = specutil.FormatVortex
	FormatIcebergTable = specutil.FormatIcebergTable
	FormatMilvusTable  = specutil.FormatMilvusTable
)

// ExtfsKey* are the canonical spec.extfs key names. Use these instead of
// string literals; keep in sync with kAllowedExtfsSpecKeys / kExtfsFields in
// internal/core/src/storage/loon_ffi/util.cpp.
const (
	ExtfsKeyAccessKeyID             = specutil.ExtfsKeyAccessKeyID
	ExtfsKeyAccessKeyValue          = specutil.ExtfsKeyAccessKeyValue
	ExtfsKeyRoleARN                 = specutil.ExtfsKeyRoleARN
	ExtfsKeySessionName             = specutil.ExtfsKeySessionName
	ExtfsKeyExternalID              = specutil.ExtfsKeyExternalID
	ExtfsKeyUseIAM                  = specutil.ExtfsKeyUseIAM
	ExtfsKeyAnonymous               = specutil.ExtfsKeyAnonymous
	ExtfsKeyGCPTargetServiceAccount = specutil.ExtfsKeyGCPTargetServiceAccount
	ExtfsKeyRegion                  = specutil.ExtfsKeyRegion
	ExtfsKeyCloudProvider           = specutil.ExtfsKeyCloudProvider
	ExtfsKeyEndpointURL             = specutil.ExtfsKeyEndpointURL
	ExtfsKeyBucketName              = specutil.ExtfsKeyBucketName
	ExtfsKeyIAMEndpoint             = specutil.ExtfsKeyIAMEndpoint
	ExtfsKeyStorageType             = specutil.ExtfsKeyStorageType
	ExtfsKeySSLCACert               = specutil.ExtfsKeySSLCACert
	ExtfsKeyUseSSL                  = specutil.ExtfsKeyUseSSL
	ExtfsKeyUseVirtualHost          = specutil.ExtfsKeyUseVirtualHost
	ExtfsKeyLoadFrequency           = specutil.ExtfsKeyLoadFrequency
	ExtfsKeyAzureClientID           = specutil.ExtfsKeyAzureClientID
	ExtfsKeyAzureTenantID           = specutil.ExtfsKeyAzureTenantID
	ExtfsKeyAzureCredentialEndpoint = specutil.ExtfsKeyAzureCredentialEndpoint
	ExtfsKeyCredentialEndpoint      = specutil.ExtfsKeyCredentialEndpoint
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
	// CloudProviderMinIO opts the request into Milvus-form URI parsing
	// (scheme://endpoint/bucket/key). Use for self-hosted S3-compatible
	// stores (MinIO, Ceph RGW, Garage, etc.) where no canonical cloud
	// endpoint exists and the URI host must be treated as the endpoint.
	CloudProviderMinIO = "minio"
)

// validCloudProviders gates extfs.cloud_provider values. Inference from URI
// scheme was previously allowed but produced silent misconfiguration when a
// scheme mapped to multiple providers (s3:// → AWS S3 vs self-hosted MinIO).
// Forcing an explicit value moves the disambiguation to the API boundary.
var validCloudProviders = map[string]bool{
	CloudProviderAWS:     true,
	CloudProviderGCP:     true,
	CloudProviderAliyun:  true,
	CloudProviderTencent: true,
	CloudProviderHuawei:  true,
	CloudProviderAzure:   true,
	CloudProviderMinIO:   true,
}

// ExternalSpec represents the parsed external collection specification.
type ExternalSpec = specutil.ExternalSpec

// Policy is the operator's credential policy for external collections, read
// from common.security.externalCollection.* at process start. Create and
// refresh admit a spec against it in Go, and the C++ runtime re-checks the
// same policy when the collection's files are opened, so a collection created
// under an older setting is caught the next time it is read. There is no
// default: callers pass the configured policy explicitly.
type Policy struct {
	// AllowServerIdentity permits the credential modes that read the source
	// with this cluster's own cloud identity: use_iam=true, role_arn (the
	// cluster identity assumes the role), and gcp_target_service_account
	// without a credential Broker (the cluster identity impersonates it).
	AllowServerIdentity bool
	// CredentialBrokerEndpoints lists the Broker URLs a spec may name in
	// credential_endpoint (or azure_credential_endpoint). Entries match the
	// spec value exactly, ignoring a trailing slash on either side. Empty
	// disables Broker credentials.
	CredentialBrokerEndpoints []string
}

// Configuration keys behind Policy; error messages name them so a rejected
// user knows which setting the operator controls. Keep in sync with
// pkg/util/paramtable/component_param.go (externalspec cannot import
// paramtable: paramtable -> mlog -> externalspec).
const (
	policyKeyAllowServerIdentity       = "common.security.externalCollection.allowServerIdentity"
	policyKeyCredentialBrokerEndpoints = "common.security.externalCollection.credentialBrokerEndpoints"
)

// allowsBroker reports whether endpoint is listed in CredentialBrokerEndpoints.
// Comparison is exact apart from trailing slashes, so "https://b/v1" and
// "https://b/v1/" are the same Broker while "https://b/v1/x" is not.
func (p Policy) allowsBroker(endpoint string) bool {
	want := strings.TrimRight(endpoint, "/")
	for _, allowed := range p.CredentialBrokerEndpoints {
		if strings.TrimRight(allowed, "/") == want {
			return true
		}
	}
	return false
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
	"credential_json":      true, // Snapshot request-level GCP service-account JSON.
	"source_sas_token":     true, // Snapshot request-level Azure source-read SAS.
}

// ParseExternalSpec parses the JSON external spec string
func ParseExternalSpec(specStr string) (*ExternalSpec, error) {
	spec, err := specutil.ParseExternalSpec(specStr)
	if err != nil {
		return nil, merr.Wrap(err, "parse external spec")
	}
	return spec, nil
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
//
// For minio:// without extfs.endpoint_url, use
// minio://<endpoint>/<bucket>/<key> (host=endpoint).
// With extfs.endpoint_url, use minio://<bucket>/<key> (host=bucket);
// endpoint_url supplies the physical storage address.
func ValidateExternalSource(source string) error {
	if source == "" {
		return merr.WrapErrParameterInvalidMsg("external_source is empty")
	}
	u, err := url.Parse(source)
	if err != nil {
		return merr.WrapErrParameterInvalidMsg("external_source is not a valid URL")
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme == "" {
		return merr.WrapErrParameterInvalidMsg("external_source must have an explicit scheme (e.g. s3://, aws://, gs://)")
	}
	if !allowedExternalSourceSchemes[scheme] {
		return merr.WrapErrParameterInvalidMsg("external_source scheme %q is not allowed; allowed schemes: %s",
			scheme, strings.Join(sortedKeys(allowedExternalSourceSchemes), ", "))
	}
	if u.User != nil {
		return merr.WrapErrParameterInvalidMsg("external_source must not embed credentials in the URL (use extfs.access_key_id / extfs.access_key_value instead)")
	}
	if u.Host == "" {
		return merr.WrapErrParameterInvalidMsg("external_source must have a non-empty host (e.g. s3://bucket/key, s3://endpoint/bucket/key, or minio://endpoint/bucket/key)")
	}
	return nil
}

// validateEndpointURL validates the explicit endpoint mode. The presence of
// endpoint_url makes external_source use the standard scheme://bucket/key
// shape; the endpoint URL supplies the physical storage address.
func validateEndpointURL(sourceURL *url.URL, extfs map[string]string) (bool, error) {
	endpointURL, ok := extfs[ExtfsKeyEndpointURL]
	if !ok {
		return false, nil
	}
	if endpointURL == "" || strings.TrimSpace(endpointURL) != endpointURL {
		return false, merr.WrapErrParameterInvalidMsg("extfs.endpoint_url must be a non-empty URL without surrounding whitespace")
	}

	sourceScheme := ""
	if sourceURL != nil {
		sourceScheme = strings.ToLower(sourceURL.Scheme)
	}
	switch sourceScheme {
	case SchemeS3, SchemeS3A, SchemeAWS, SchemeMinIO:
	default:
		return false, merr.WrapErrParameterInvalidMsg(
			"extfs.endpoint_url is only supported for S3-compatible source schemes [aws, minio, s3, s3a], got %q",
			sourceScheme)
	}

	u, err := url.Parse(endpointURL)
	if err != nil {
		return false, merr.WrapErrParameterInvalidMsg("extfs.endpoint_url is not a valid URL")
	}
	endpointScheme := strings.ToLower(u.Scheme)
	if endpointScheme != "http" && endpointScheme != "https" {
		return false, merr.WrapErrParameterInvalidMsg("extfs.endpoint_url scheme must be http or https")
	}
	if u.Hostname() == "" {
		return false, merr.WrapErrParameterInvalidMsg("extfs.endpoint_url must have a non-empty host")
	}
	if strings.HasSuffix(u.Host, ":") {
		return false, merr.WrapErrParameterInvalidMsg("extfs.endpoint_url must not contain an empty port")
	}
	if port := u.Port(); port != "" {
		portNumber, err := strconv.Atoi(port)
		if err != nil || portNumber < 1 || portNumber > 65535 {
			return false, merr.WrapErrParameterInvalidMsg("extfs.endpoint_url has an invalid port")
		}
	}
	if u.User != nil {
		return false, merr.WrapErrParameterInvalidMsg("extfs.endpoint_url must not embed credentials")
	}
	if u.RawQuery != "" || u.ForceQuery {
		return false, merr.WrapErrParameterInvalidMsg("extfs.endpoint_url must not contain a query")
	}
	if strings.Contains(endpointURL, "#") {
		return false, merr.WrapErrParameterInvalidMsg("extfs.endpoint_url must not contain a fragment")
	}
	if escapedPath := u.EscapedPath(); escapedPath != "" && escapedPath != "/" {
		return false, merr.WrapErrParameterInvalidMsg("extfs.endpoint_url must not contain a path")
	}
	// Port() misses empty ports; any colon is invalid in a bucket-only host.
	if sourceURL != nil && strings.Contains(sourceURL.Host, ":") {
		return false, merr.WrapErrParameterInvalidMsg(
			"external_source host must be a bucket name without a port when extfs.endpoint_url is set")
	}
	if _, ok := extfs[ExtfsKeyBucketName]; ok {
		return false, merr.WrapErrParameterInvalidMsg(
			"extfs.bucket_name cannot be set with extfs.endpoint_url; the bucket comes from external_source")
	}
	if useSSL, ok := extfs[ExtfsKeyUseSSL]; ok {
		expectedUseSSL := strconv.FormatBool(endpointScheme == "https")
		if useSSL != expectedUseSSL {
			return false, merr.WrapErrParameterInvalidMsg(
				"extfs.use_ssl=%q conflicts with extfs.endpoint_url scheme %q; expected %q",
				useSSL, endpointScheme, expectedUseSSL)
		}
	}
	return true, nil
}

// ValidateSourceAndSpec validates URL + JSON shape + ValidateExtfsComplete
// under the operator's credential policy. Errors retain their typed validation
// code while adding field context. Called from Proxy and RootCoord on
// create-collection and from Proxy and DataCoord on refresh (defense in
// depth), each passing the policy built from its own paramtable.
func ValidateSourceAndSpec(externalSource, externalSpec string, policy Policy) error {
	if err := ValidateExternalSource(externalSource); err != nil {
		return merr.Wrap(err, "external_source is invalid")
	}
	spec, err := ParseExternalSpec(externalSpec)
	if err != nil {
		return merr.Wrap(err, "external_spec is invalid")
	}
	if err := ValidateExtfsComplete(externalSource, spec.Extfs, policy); err != nil {
		return merr.Wrap(err, "external_spec is invalid")
	}
	return nil
}

// RedactExternalSource returns a log-safe external source. Normal storage URIs
// are preserved for diagnostics, while malformed URIs and URI components that
// can carry credentials (userinfo, query, or fragment) are hidden completely.
func RedactExternalSource(source string) string {
	if source == "" {
		return ""
	}
	u, err := url.Parse(source)
	if err != nil || u.Opaque != "" || u.RawQuery != "" || u.Fragment != "" || ValidateExternalSource(source) != nil {
		return "<redacted>"
	}
	return source
}

// RedactExternalSpecForLog preserves approved top-level metadata while
// replacing the complete extfs authentication/configuration object. Unknown
// top-level fields are dropped because future extensions may carry secrets.
func RedactExternalSpecForLog(specStr string) string {
	if specStr == "" {
		return ""
	}

	var spec map[string]json.RawMessage
	if err := json.Unmarshal([]byte(specStr), &spec); err != nil {
		return "<redacted>"
	}

	redacted := make(map[string]json.RawMessage, 4)
	for _, key := range []string{"format", "columns", "snapshot_id"} {
		if value, ok := spec[key]; ok {
			redacted[key] = value
		}
	}
	if _, ok := spec["extfs"]; ok {
		redacted["extfs"] = json.RawMessage(`"***"`)
	}

	out, err := json.Marshal(redacted)
	if err != nil {
		return "<redacted>"
	}
	return string(out)
}

// RedactCollectionSchemaForLog returns a copy of schema with external storage
// credentials removed. Callers can safely log the result without mutating the
// schema used by request handling or distributed load paths.
func RedactCollectionSchemaForLog(schema *schemapb.CollectionSchema) *schemapb.CollectionSchema {
	if schema == nil {
		return nil
	}

	redacted := proto.Clone(schema).(*schemapb.CollectionSchema)
	redacted.ExternalSource = RedactExternalSource(redacted.GetExternalSource())
	redacted.ExternalSpec = RedactExternalSpecForLog(redacted.GetExternalSpec())
	return redacted
}

// ValidateExtfsComplete requires spec.extfs to be self-sufficient: exactly one
// credential mode (AK/SK, role_arn, use_iam=true, gcp_target_service_account
// alone or with a credential_endpoint Broker, Azure credential broker,
// anonymous=true), and region for AWS-family schemes. role_arn subsumes
// use_iam (do not double-count). No inheritance from Milvus fs.* config.
//
// The spec must also pass policy: modes that use this cluster's own cloud
// identity need AllowServerIdentity, and a Broker endpoint must be listed in
// CredentialBrokerEndpoints. Structural errors are reported first so a
// malformed spec gets its specific error; the policy gate follows.
// Both create and refresh run this check, so a policy tightened after a
// collection was created rejects it on its next refresh.
func ValidateExtfsComplete(externalSource string, extfs map[string]string, policy Policy) error {
	// Parse once; caller's ValidateExternalSource has already guaranteed a scheme.
	u, err := url.Parse(externalSource)
	scheme := ""
	if err == nil {
		scheme = strings.ToLower(u.Scheme)
	}
	hasEndpointURL, err := validateEndpointURL(u, extfs)
	if err != nil {
		return err
	}

	// extfs.cloud_provider is required and must be from the allowed set, except
	// when the source scheme or endpoint_url already selects S3-compatible
	// semantics. This is a local classification only; it is not written back to
	// external_spec. In explicit-endpoint mode, the runtime uses the generic AWS
	// S3-compatible client with endpoint override, so requiring the caller to
	// repeat a provider adds no information.
	rawCP := extfs[ExtfsKeyCloudProvider]
	cp := strings.ToLower(rawCP)
	if rawCP != cp {
		return merr.WrapErrParameterInvalidMsg(
			"extfs.cloud_provider=%q must use the canonical lowercase value %q", rawCP, cp)
	}
	if cp == "" {
		if scheme == SchemeMinIO {
			cp = CloudProviderMinIO
		} else if hasEndpointURL {
			cp = CloudProviderAWS
		}
	}
	if cp == "" {
		return merr.WrapErrParameterMissingMsg("extfs.cloud_provider is required: one of [aws, gcp, aliyun, tencent, huawei, azure, minio]; inferring from URI scheme is ambiguous (e.g. s3:// matches both AWS S3 and self-hosted MinIO)")
	}
	if !validCloudProviders[cp] {
		return merr.WrapErrParameterInvalidMsg("extfs.cloud_provider=%q is not supported: must be one of [aws, gcp, aliyun, tencent, huawei, azure, minio]", cp)
	}
	if scheme == SchemeMinIO && cp != CloudProviderMinIO {
		return merr.WrapErrParameterInvalidMsg("scheme=minio requires extfs.cloud_provider=%q, got %q", CloudProviderMinIO, cp)
	}

	// credential_endpoint is the provider-neutral Broker key; the older
	// azure_credential_endpoint stays accepted as its Azure-only alias.
	brokerEndpointKey, brokerEndpoint := ExtfsKeyCredentialEndpoint, extfs[ExtfsKeyCredentialEndpoint]
	if legacy := extfs[ExtfsKeyAzureCredentialEndpoint]; legacy != "" {
		if brokerEndpoint != "" && brokerEndpoint != legacy {
			return merr.WrapErrParameterInvalidMsg("extfs.%s and extfs.%s must not differ",
				ExtfsKeyCredentialEndpoint, ExtfsKeyAzureCredentialEndpoint)
		}
		brokerEndpointKey, brokerEndpoint = ExtfsKeyAzureCredentialEndpoint, legacy
	}
	hasAzureBroker := extfs[ExtfsKeyAzureClientID] != "" ||
		extfs[ExtfsKeyAzureTenantID] != "" ||
		extfs[ExtfsKeyAzureCredentialEndpoint] != "" ||
		(brokerEndpoint != "" && cp == CloudProviderAzure)
	hasGCPBroker := !hasAzureBroker && brokerEndpoint != ""
	if hasAzureBroker {
		if cp != CloudProviderAzure || scheme != SchemeAzure {
			return merr.WrapErrParameterInvalidMsg(
				"Azure credential broker mode requires scheme=%q and extfs.cloud_provider=%q, got scheme=%q cloud_provider=%q",
				SchemeAzure, CloudProviderAzure, scheme, cp)
		}
		for _, required := range []struct {
			key   string
			value string
		}{
			{ExtfsKeyAzureClientID, extfs[ExtfsKeyAzureClientID]},
			{ExtfsKeyAzureTenantID, extfs[ExtfsKeyAzureTenantID]},
			{brokerEndpointKey, brokerEndpoint},
			{ExtfsKeyAccessKeyID, extfs[ExtfsKeyAccessKeyID]},
			{ExtfsKeyRegion, extfs[ExtfsKeyRegion]},
		} {
			if required.value == "" {
				return merr.WrapErrParameterMissingMsg(
					"extfs.%s is required for Azure credential broker mode", required.key)
			}
		}

		if !isHTTPURL(brokerEndpoint) {
			return merr.WrapErrParameterInvalidMsg("extfs.%s must be a valid HTTP(S) URL", brokerEndpointKey)
		}
	}

	hasAKSK := extfs[ExtfsKeyAccessKeyID] != "" && extfs[ExtfsKeyAccessKeyValue] != ""
	hasAKOnly := !hasAzureBroker &&
		((extfs[ExtfsKeyAccessKeyID] != "") != (extfs[ExtfsKeyAccessKeyValue] != ""))
	hasRoleARN := extfs[ExtfsKeyRoleARN] != ""
	hasGCPImpersonation := extfs[ExtfsKeyGCPTargetServiceAccount] != ""
	// role_arn and gcp_target_service_account both use the cluster identity as
	// the source of the assumed identity, so use_iam=true adds no mode.
	hasUseIAMAlone := extfs[ExtfsKeyUseIAM] == "true" && !hasRoleARN && !hasGCPImpersonation
	hasAnonymous := extfs[ExtfsKeyAnonymous] == "true"

	if hasAKOnly {
		return merr.WrapErrParameterMissingMsg("extfs.access_key_id and extfs.access_key_value must be set together (found one without the other)")
	}

	// GCP Broker shape is checked before the mode count so a Broker spec that
	// lacks its target gets that error rather than "credential mode missing".
	if hasGCPBroker {
		if cp != CloudProviderGCP {
			return merr.WrapErrParameterInvalidMsg(
				"extfs.%s is only supported for cloud_provider gcp and azure, got %q", ExtfsKeyCredentialEndpoint, cp)
		}
		if !hasGCPImpersonation {
			return merr.WrapErrParameterMissingMsg(
				"extfs.%s is required with extfs.%s: the Broker issues tokens for that service account",
				ExtfsKeyGCPTargetServiceAccount, ExtfsKeyCredentialEndpoint)
		}
		if extfs[ExtfsKeyUseIAM] == "true" {
			return merr.WrapErrParameterInvalidMsg(
				"extfs.use_iam=true conflicts with extfs.%s: use_iam selects the cluster's own identity instead of the Broker",
				ExtfsKeyCredentialEndpoint)
		}
		if !isHTTPURL(brokerEndpoint) {
			return merr.WrapErrParameterInvalidMsg("extfs.%s must be a valid HTTP(S) URL", ExtfsKeyCredentialEndpoint)
		}
	}

	modes := 0
	for _, set := range []bool{hasAKSK, hasRoleARN, hasUseIAMAlone, hasGCPImpersonation, hasAzureBroker, hasAnonymous} {
		if set {
			modes++
		}
	}
	if modes == 0 {
		return merr.WrapErrParameterInvalidMsg("extfs credential mode missing: set exactly one of {access_key_id+access_key_value}, role_arn, use_iam=true, gcp_target_service_account (alone or with credential_endpoint), Azure credential broker, or anonymous=true")
	}
	if modes > 1 {
		return merr.WrapErrParameterInvalidMsg("extfs credential modes are mutually exclusive: set exactly one of AK/SK, role_arn, use_iam=true, gcp_target_service_account (alone or with credential_endpoint), Azure credential broker, or anonymous=true")
	}

	if hasGCPImpersonation {
		if cp != CloudProviderGCP {
			return merr.WrapErrParameterInvalidMsg("extfs.gcp_target_service_account is only valid for GCP (cloud_provider=gcp), got scheme=%q cloud_provider=%q", scheme, cp)
		}
		sa := extfs[ExtfsKeyGCPTargetServiceAccount]
		if !strings.Contains(sa, "@") || !strings.HasSuffix(sa, ".gserviceaccount.com") {
			return merr.WrapErrParameterInvalidMsg("extfs.gcp_target_service_account must be a GCP service account email ending in .gserviceaccount.com, got %q", sa)
		}
		if err := validateGCPTokenLifetime(extfs); err != nil {
			return err
		}
	}

	// Every GCP Bearer mode sends an OAuth2 token to the storage endpoint:
	// the cluster's own token (use_iam=true), an impersonated one, or one the
	// Broker issued. All of them get the HTTPS + Google-host check.
	if cp == CloudProviderGCP && (hasGCPImpersonation || extfs[ExtfsKeyUseIAM] == "true") {
		mode := "extfs." + ExtfsKeyGCPTargetServiceAccount
		if !hasGCPImpersonation {
			mode = "extfs.use_iam=true with extfs.cloud_provider=gcp"
		}
		if err := validateGCPBearerEndpoint(u, scheme, extfs, hasEndpointURL, mode); err != nil {
			return err
		}
	}

	if err := validateCredentialPolicy(extfs, brokerEndpointKey, brokerEndpoint, policy); err != nil {
		return err
	}

	if awsFamilyScheme[scheme] && extfs[ExtfsKeyRegion] == "" {
		return merr.WrapErrParameterMissingMsg("extfs.region is required for scheme %q (AWS-family schemes need region for SigV4 signing)", scheme)
	}

	// Azure consistency: scheme=azure requires cloud_provider=azure, and
	// cloud_provider=azure requires scheme=azure. Pairing guards against
	// misconfigured dispatch to a non-Azure storage backend.
	if scheme == SchemeAzure && cp != CloudProviderAzure {
		return merr.WrapErrParameterInvalidMsg("scheme=azure requires extfs.cloud_provider=%q, got %q", CloudProviderAzure, cp)
	}
	if cp == CloudProviderAzure && scheme != "" && scheme != SchemeAzure {
		return merr.WrapErrParameterInvalidMsg("extfs.cloud_provider=azure requires scheme=azure, got scheme=%q", scheme)
	}

	// Two-form URI contract: Milvus-form (scheme://endpoint/bucket/key) has
	// URI.host either a cloud-family endpoint (e.g. *.amazonaws.com,
	// *.aliyuncs.com) or a custom endpoint containing path-segment count
	// >= 2. AWS-form (scheme://bucket/key) has a bucket-like URI.host (not
	// cloud-family) and endpoint must be derivable from cloud_provider +
	// region. Reject AWS-form configurations whose DeriveEndpoint returns
	// empty — the endpoint would resolve to the URI host (= bucket) at
	// runtime and fail opaquely.
	if u != nil && !hasEndpointURL {
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
			// AWS-form: endpoint must be derivable from the user-supplied
			// cloud_provider + region. cp=minio has no canonical endpoint
			// and must use Milvus-form URI to embed the host explicitly.
			if DeriveEndpoint(cp, extfs[ExtfsKeyRegion]) == "" {
				return merr.WrapErrParameterInvalidMsg("cannot resolve endpoint for %q with cloud_provider=%q: set extfs.region, or use Milvus-form URI scheme://<endpoint>/<bucket>/<key>", externalSource, cp)
			}
		}
	}
	return nil
}

// isHTTPURL reports whether raw is an absolute http or https URL with a host.
func isHTTPURL(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && u.Host != "" && (u.Scheme == "http" || u.Scheme == "https")
}

// GCP caps impersonated token lifetimes at 3600s; milvus-storage rejects
// lifetimes outside [900, 3600] rather than clamping them.
const (
	gcpMinTokenLifetimeSeconds = 900
	gcpMaxTokenLifetimeSeconds = 3600
)

func validateGCPTokenLifetime(extfs map[string]string) error {
	raw, ok := extfs[ExtfsKeyLoadFrequency]
	if !ok || raw == "" {
		return nil
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds < gcpMinTokenLifetimeSeconds || seconds > gcpMaxTokenLifetimeSeconds {
		return merr.WrapErrParameterInvalidMsg(
			"extfs.%s must be an integer in [%d, %d] with extfs.%s, got %q", ExtfsKeyLoadFrequency,
			gcpMinTokenLifetimeSeconds, gcpMaxTokenLifetimeSeconds, ExtfsKeyGCPTargetServiceAccount, raw)
	}
	return nil
}

// usesServerIdentity reports whether extfs reads the source with this
// cluster's own cloud identity. use_iam=true and role_arn always do; a
// gcp_target_service_account does unless a credential Broker issues its token.
// Azure and GCP Broker modes present the Broker's credentials instead.
func usesServerIdentity(extfs map[string]string, brokerEndpoint string) bool {
	return extfs[ExtfsKeyUseIAM] == "true" ||
		extfs[ExtfsKeyRoleARN] != "" ||
		(extfs[ExtfsKeyGCPTargetServiceAccount] != "" && brokerEndpoint == "")
}

// validateCredentialPolicy is the policy gate that follows the structural
// checks. brokerEndpointKey names the spec key the endpoint came from so the
// message points at what the user wrote. The endpoint value itself is not
// echoed, matching the other Broker URL errors.
func validateCredentialPolicy(extfs map[string]string, brokerEndpointKey, brokerEndpoint string, policy Policy) error {
	if usesServerIdentity(extfs, brokerEndpoint) && !policy.AllowServerIdentity {
		return merr.WrapErrParameterInvalidMsg(
			"extfs reads the source with this cluster's own cloud identity (use_iam=true, role_arn, or gcp_target_service_account without a credential Broker), which this cluster does not allow: "+
				"use access keys, anonymous=true, or a credential Broker listed in %s, or have the operator set %s=true",
			policyKeyCredentialBrokerEndpoints, policyKeyAllowServerIdentity)
	}
	if brokerEndpoint != "" && !policy.allowsBroker(brokerEndpoint) {
		return merr.WrapErrParameterInvalidMsg(
			"extfs.%s is not a credential Broker this cluster allows: the operator must list it in %s (exact URL; a trailing slash is ignored)",
			brokerEndpointKey, policyKeyCredentialBrokerEndpoints)
	}
	return nil
}

// DeriveUseSSLFromScheme mirrors C++ DeriveUseSSLFromScheme in
// internal/core/src/storage/loon_ffi/util.cpp: the use_ssl the runtime applies
// when spec.extfs does not set one. Only minio:// defaults to plain HTTP.
// Keep the two in lockstep.
func DeriveUseSSLFromScheme(scheme string) bool {
	return strings.ToLower(scheme) != SchemeMinIO
}

// effectiveUseSSL follows the property layering in InjectExternalSpecProperties:
// a non-empty extfs.use_ssl wins; a missing or empty one leaves the scheme
// default in place. endpoint_url overrides both and is handled by the caller.
func effectiveUseSSL(scheme string, extfs map[string]string) bool {
	if v := extfs[ExtfsKeyUseSSL]; v != "" {
		return v == "true"
	}
	return DeriveUseSSLFromScheme(scheme)
}

// validateGCPBearerEndpoint requires the endpoint that receives an OAuth2
// bearer token to be a Google API host over HTTPS. mode names the credential
// mode for the message. The endpoint follows the resolution in
// InjectExternalSpecProperties (internal/core/src/storage/loon_ffi/util.cpp):
// extfs.endpoint_url when set, else the URI host when it is a cloud endpoint
// host (Milvus-form), else the endpoint derived for cloud_provider=gcp
// (AWS-form); use_ssl likewise comes from endpoint_url, else extfs.use_ssl,
// else the source scheme default.
func validateGCPBearerEndpoint(source *url.URL, sourceScheme string, extfs map[string]string, hasEndpointURL bool, mode string) error {
	scheme := "http"
	if effectiveUseSSL(sourceScheme, extfs) {
		scheme = "https"
	}
	host := strings.TrimPrefix(DeriveEndpoint(CloudProviderGCP, ""), "https://")
	if hasEndpointURL {
		endpoint, err := url.Parse(extfs[ExtfsKeyEndpointURL])
		if err != nil {
			return merr.WrapErrParameterInvalidMsg("extfs.endpoint_url is not a valid URL")
		}
		scheme, host = strings.ToLower(endpoint.Scheme), endpoint.Hostname()
	} else if source != nil && IsCloudEndpointHost(source.Hostname()) {
		host = source.Hostname()
	}
	if scheme != "https" {
		return merr.WrapErrParameterInvalidMsg(
			"%s requires HTTPS so the OAuth2 token is not sent in clear text: remove extfs.use_ssl=false or use an https extfs.endpoint_url",
			mode)
	}
	if !strings.HasSuffix(strings.ToLower(host), ".googleapis.com") {
		return merr.WrapErrParameterInvalidMsg(
			"%s is only supported for Google Cloud Storage endpoints (*.googleapis.com), got host %q",
			mode, host)
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

// RedactExternalSpec returns a credential-redacted representation of a
// validated external spec for user-visible responses. Known secret extfs
// values (see secretExtfsKeys) are replaced with "***", while unknown fields
// are preserved as extension metadata. Because unknown pre-validation fields
// may themselves be sensitive, external-collection request logging uses
// RedactExternalSpecForLog. On parse failure this returns "<invalid spec>"
// rather than the raw input.
func RedactExternalSpec(specStr string) string {
	if specStr == "" {
		return ""
	}
	var spec map[string]json.RawMessage
	if err := json.Unmarshal([]byte(specStr), &spec); err != nil {
		return "<invalid spec>"
	}
	if err := normalizeInt64Field(spec, "snapshot_id"); err != nil {
		return "<invalid spec>"
	}
	if err := redactExtfsSecrets(spec); err != nil {
		return "<invalid spec>"
	}
	out, err := json.Marshal(spec)
	if err != nil {
		return "<marshal error>"
	}
	return string(out)
}

func normalizeInt64Field(spec map[string]json.RawMessage, field string) error {
	raw, ok := spec[field]
	if !ok || len(raw) == 0 || string(raw) == "null" {
		return nil
	}

	var n int64
	if err := json.Unmarshal(raw, &n); err == nil {
		spec[field] = quotedInt64JSON(n)
		return nil
	}

	var str string
	if err := json.Unmarshal(raw, &str); err != nil {
		return err
	}
	parsed, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		return err
	}
	spec[field] = quotedInt64JSON(parsed)
	return nil
}

func quotedInt64JSON(v int64) json.RawMessage {
	return json.RawMessage(strconv.Quote(strconv.FormatInt(v, 10)))
}

func redactExtfsSecrets(spec map[string]json.RawMessage) error {
	extfsRaw, ok := spec["extfs"]
	if !ok || len(extfsRaw) == 0 || string(extfsRaw) == "null" {
		return nil
	}

	var extfs map[string]json.RawMessage
	if err := json.Unmarshal(extfsRaw, &extfs); err != nil {
		return err
	}
	for k, v := range extfs {
		if !secretExtfsKeys[k] {
			continue
		}
		var str string
		if err := json.Unmarshal(v, &str); err == nil && str == "" {
			continue
		}
		extfs[k] = json.RawMessage(`"***"`)
	}
	redactedExtfs, err := json.Marshal(extfs)
	if err != nil {
		return err
	}
	spec["extfs"] = redactedExtfs
	return nil
}
