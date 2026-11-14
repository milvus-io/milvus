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
	"regexp"
	"sort"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/externalspec/specutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Azure account names are interpolated into Blob/DFS hostnames.
var azureAccountNamePattern = regexp.MustCompile(`^[a-z0-9]{3,24}$`)

// File formats supported by external collections. Mirror of LOON_FORMAT_*
// in the C++ FFI layer — keep the two in sync.
const (
	FormatParquet      = specutil.FormatParquet
	FormatLanceTable   = specutil.FormatLanceTable
	FormatVortex       = specutil.FormatVortex
	FormatIcebergTable = specutil.FormatIcebergTable
	FormatPaimonTable  = specutil.FormatPaimonTable
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
	ExtfsKeyCredentialEndpoint      = specutil.ExtfsKeyCredentialEndpoint
	ExtfsKeyAzureClientID           = specutil.ExtfsKeyAzureClientID
	ExtfsKeyAzureTenantID           = specutil.ExtfsKeyAzureTenantID
	ExtfsKeyAzureCredentialEndpoint = specutil.ExtfsKeyAzureCredentialEndpoint
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

// MarshalCollectionSchema normalizes external_spec before sending a schema to
// native readers. It also handles historical schemas without mutating the
// caller's shared schema or rewriting persisted metadata.
func MarshalCollectionSchema(schema *schemapb.CollectionSchema) ([]byte, error) {
	normalized, err := specutil.NormalizeExternalSpec(schema.GetExternalSpec())
	if err != nil {
		return nil, err
	}
	if normalized != schema.GetExternalSpec() {
		schema = proto.Clone(schema).(*schemapb.CollectionSchema)
		schema.ExternalSpec = normalized
	}
	return proto.Marshal(schema)
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

// ValidateSourceAndSpec validates URL + JSON shape + ValidateExtfsComplete.
// Errors retain their typed validation code while adding field context.
// Called from Proxy and RootCoord (defense in depth) on create-collection.
// Omitting the server-owned policy denies every credential Broker.
func ValidateSourceAndSpec(externalSource, externalSpec string, policies ...*specutil.BrokerPolicy) error {
	if err := ValidateExternalSource(externalSource); err != nil {
		return merr.Wrap(err, "external_source is invalid")
	}
	spec, err := ParseExternalSpec(externalSpec)
	if err != nil {
		return merr.Wrap(err, "external_spec is invalid")
	}
	if err := ValidateExtfsComplete(externalSource, spec.Extfs); err != nil {
		return merr.Wrap(err, "external_spec is invalid")
	}
	var policy *specutil.BrokerPolicy
	if len(policies) > 0 {
		policy = policies[0]
	}
	return policy.Validate(spec.Extfs)
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
// credential mode (AK/SK, role_arn, use_iam=true, gcp_target_service_account,
// Azure credential broker, anonymous=true), and region for AWS-family
// schemes. role_arn and gcp_target_service_account subsume use_iam
// (do not double-count). No inheritance
// from Milvus fs.* config.
func ValidateExtfsComplete(externalSource string, extfs map[string]string) error {
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

	brokerEndpoint, err := specutil.ResolveBrokerEndpoint(extfs)
	if err != nil {
		return err
	}
	hasGCPBroker := brokerEndpoint != "" && cp == CloudProviderGCP
	if hasGCPBroker {
		brokerIAM := extfs[ExtfsKeyUseIAM] == "true"
		if brokerIAM && extfs[ExtfsKeyGCPTargetServiceAccount] != "" {
			return merr.WrapErrParameterInvalidMsg("extfs.gcp_target_service_account conflicts with GCP credential broker IAM")
		}
		if !brokerIAM && extfs[ExtfsKeyGCPTargetServiceAccount] == "" {
			return merr.WrapErrParameterMissingMsg("extfs.gcp_target_service_account is required for GCP credential broker without use_iam=true")
		}
		// Match native injection: the endpoint selects the Broker, and use_iam
		// selects its own identity instead of a target service account.
		for _, key := range []string{
			ExtfsKeyRoleARN, ExtfsKeyExternalID, ExtfsKeySessionName,
			ExtfsKeyAccessKeyID, ExtfsKeyAccessKeyValue,
			ExtfsKeyAzureClientID, ExtfsKeyAzureTenantID,
		} {
			if extfs[key] != "" {
				return merr.WrapErrParameterInvalidMsg("extfs.%s conflicts with GCP credential broker", key)
			}
		}
	}
	hasS3Broker := brokerEndpoint != "" && cp != CloudProviderAzure && cp != CloudProviderGCP
	if hasS3Broker {
		if rawCP == "" {
			return merr.WrapErrParameterMissingMsg("extfs.cloud_provider is required for credential broker")
		}
		if err := validateS3Broker(cp, extfs); err != nil {
			return err
		}
	}
	hasAzureBroker := cp == CloudProviderAzure && brokerEndpoint != "" || extfs[ExtfsKeyAzureClientID] != "" ||
		extfs[ExtfsKeyAzureTenantID] != "" ||
		extfs[ExtfsKeyAzureCredentialEndpoint] != ""
	if hasAzureBroker {
		if cp != CloudProviderAzure || scheme != SchemeAzure {
			return merr.WrapErrParameterInvalidMsg(
				"Azure credential broker mode requires scheme=%q and extfs.cloud_provider=%q, got scheme=%q cloud_provider=%q",
				SchemeAzure, CloudProviderAzure, scheme, cp)
		}
		brokerIAM := extfs[ExtfsKeyUseIAM] == "true"
		requiredFields := []struct {
			key   string
			value string
		}{
			{ExtfsKeyCredentialEndpoint, brokerEndpoint},
			{ExtfsKeyAccessKeyID, extfs[ExtfsKeyAccessKeyID]},
		}
		if !brokerIAM {
			for _, key := range []string{ExtfsKeyAzureClientID, ExtfsKeyAzureTenantID, ExtfsKeyRegion} {
				requiredFields = append(requiredFields, struct{ key, value string }{key, extfs[key]})
			}
		}
		for _, required := range requiredFields {
			if required.value == "" {
				return merr.WrapErrParameterMissingMsg(
					"extfs.%s is required for Azure credential broker mode", required.key)
			}
		}
		// Target mode predates Broker IAM, and refresh revalidates persisted
		// specs: it keeps accepting external_id and session_name, which Azure
		// never reads. Broker IAM is new and rejects every credential field it
		// does not use.
		forbidden := []string{ExtfsKeyAccessKeyValue, ExtfsKeyRoleARN, ExtfsKeyGCPTargetServiceAccount}
		if brokerIAM {
			forbidden = append(forbidden, ExtfsKeyExternalID, ExtfsKeySessionName, ExtfsKeyAzureClientID, ExtfsKeyAzureTenantID)
		}
		for _, key := range forbidden {
			if extfs[key] != "" {
				return merr.WrapErrParameterInvalidMsg("extfs.%s conflicts with Azure credential broker mode", key)
			}
		}
		if extfs[ExtfsKeyAnonymous] == "true" {
			return merr.WrapErrParameterInvalidMsg("extfs.anonymous=true conflicts with Azure credential broker mode")
		}
		if raw := extfs[ExtfsKeyLoadFrequency]; brokerIAM && raw != "" && !isCredentialLifetime(raw) {
			return merr.WrapErrParameterInvalidMsg("extfs.load_frequency must be an integer in [900, 3600] for Azure credential broker IAM")
		}
		if brokerIAM {
			if err := validateAzureBrokerIAMSource(u, externalSource, extfs); err != nil {
				return err
			}
		}
	}

	// Azure uses access_key_id as the storage account name even when the
	// pod's workload identity supplies credentials instead of a shared key.
	hasAzureIAM := cp == CloudProviderAzure && extfs[ExtfsKeyUseIAM] == "true"
	if hasAzureIAM && extfs[ExtfsKeyAccessKeyID] == "" {
		return merr.WrapErrParameterMissingMsg("extfs.access_key_id is required as the storage account name for Azure workload identity")
	}

	hasAKSK := extfs[ExtfsKeyAccessKeyID] != "" && extfs[ExtfsKeyAccessKeyValue] != ""
	hasAKOnly := !hasAzureBroker && !hasAzureIAM &&
		((extfs[ExtfsKeyAccessKeyID] != "") != (extfs[ExtfsKeyAccessKeyValue] != ""))
	hasRoleARN := extfs[ExtfsKeyRoleARN] != ""
	hasGCPImpersonation := extfs[ExtfsKeyGCPTargetServiceAccount] != ""
	hasUseIAMAlone := extfs[ExtfsKeyUseIAM] == "true" && !hasRoleARN && !hasGCPImpersonation && !hasAzureBroker
	hasAnonymous := extfs[ExtfsKeyAnonymous] == "true"

	if hasAKOnly {
		return merr.WrapErrParameterMissingMsg("extfs.access_key_id and extfs.access_key_value must be set together (found one without the other)")
	}

	modes := 0
	for _, set := range []bool{hasAKSK, hasRoleARN, hasUseIAMAlone, hasGCPImpersonation, hasAzureBroker, hasAnonymous} {
		if set {
			modes++
		}
	}
	if modes == 0 {
		return merr.WrapErrParameterInvalidMsg("extfs credential mode missing: set exactly one of {access_key_id+access_key_value}, role_arn, use_iam=true, gcp_target_service_account, Azure credential broker, or anonymous=true")
	}
	if modes > 1 {
		return merr.WrapErrParameterInvalidMsg("extfs credential modes are mutually exclusive: set exactly one of AK/SK, role_arn, use_iam=true, gcp_target_service_account, Azure credential broker, or anonymous=true")
	}

	if hasGCPImpersonation {
		if cp != CloudProviderGCP {
			return merr.WrapErrParameterInvalidMsg("extfs.gcp_target_service_account requires cloud_provider=gcp")
		}
		sa := extfs[ExtfsKeyGCPTargetServiceAccount]
		if !strings.Contains(sa, "@") || !strings.HasSuffix(sa, ".gserviceaccount.com") {
			return merr.WrapErrParameterInvalidMsg("extfs.gcp_target_service_account must be a GCP service account email ending in .gserviceaccount.com, got %q", sa)
		}
	}
	// Both Broker modes and direct target impersonation carry Bearer tokens.
	// Preserve endpoint admission for existing plain node IAM configurations.
	if hasGCPImpersonation || hasGCPBroker {
		if err := validateGCPTokenLifetime(extfs); err != nil {
			return err
		}
		if err := validateGCPBearerEndpoint(u, scheme, extfs, hasEndpointURL); err != nil {
			return err
		}
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

	if account := extfs[ExtfsKeyAccessKeyID]; cp == CloudProviderAzure && account != "" && !azureAccountNamePattern.MatchString(account) {
		return merr.WrapErrParameterInvalidMsg("extfs.access_key_id must be an Azure storage account name containing 3-24 lowercase letters or digits")
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
			// An Azure container name cannot contain a dot, so a dotted
			// azure:// host is the storage address, and
			// azure://core.windows.net/<container>/ is a Milvus-form URI at a
			// container root: the host is the endpoint and needs no
			// derivation. The trailing slash is required because the storage
			// URI parser needs the separator after the container. Native
			// injection splits the raw source, so a percent-encoded "%2F" is
			// not that separator.
			azureContainerRoot := cp == CloudProviderAzure && pathSegs == 1 &&
				strings.HasSuffix(u.EscapedPath(), "/") && strings.Contains(u.Hostname(), ".")
			// AWS-form: endpoint must be derivable from the user-supplied
			// cloud_provider + region. cp=minio has no canonical endpoint
			// and must use Milvus-form URI to embed the host explicitly.
			if !azureContainerRoot && DeriveEndpoint(cp, extfs[ExtfsKeyRegion]) == "" {
				return errUnresolvableEndpoint(externalSource, cp)
			}
		}
	}
	return nil
}

// errUnresolvableEndpoint names the source without a query or fragment: a
// caller may have put a signed token there, and the error is logged.
func errUnresolvableEndpoint(externalSource, cp string) error {
	return merr.WrapErrParameterInvalidMsg("cannot resolve endpoint for %q with cloud_provider=%q: set extfs.region, or use Milvus-form URI scheme://<endpoint>/<bucket>/<key>", RedactExternalSource(externalSource), cp)
}

// azureStorageAuthorities are the only addresses milvus-storage sends an
// Azure identity credential or Broker SAS to.
var azureStorageAuthorities = map[string]bool{
	"core.windows.net":       true,
	"core.chinacloudapi.cn":  true,
	"core.usgovcloudapi.net": true,
	"core.cloudapi.de":       true,
}

// azureContainerNamePattern is the Azure container naming rule the
// credential Broker enforces: 3-63 lowercase letters, digits and single
// hyphens, starting and ending with a letter or digit.
var azureContainerNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$`)

func isValidAzureContainerName(name string) bool {
	return azureContainerNamePattern.MatchString(name) && !strings.Contains(name, "--")
}

// validateAzureBrokerIAMSource admits the two source shapes that
// milvus-storage reads with a Broker SAS, over HTTPS only:
//
//   - azure://<authority>/<container>/[<path>]: an Azure container name
//     cannot contain a dot, so a dotted host is the address. It must be one of
//     the Azure Storage authorities exactly as milvus-storage compares it, and
//     the storage URI parser needs the separator after the container.
//   - azure://<container>/[<path>]: the region supplies the authority.
//
// The Broker rejects invalid container names, so both shapes check them here.
// Native injection splits the raw source, so the container is read from the
// escaped path: a percent-encoded separator or letter is not the name the
// reader will use. Broker IAM is new, so rejecting other shapes fails no
// persisted spec.
func validateAzureBrokerIAMSource(source *url.URL, externalSource string, extfs map[string]string) error {
	if extfs[ExtfsKeyUseSSL] == "false" {
		return merr.WrapErrParameterInvalidMsg("extfs.use_ssl=false conflicts with Azure credential broker IAM: the SAS is only sent over HTTPS")
	}
	if source == nil {
		return nil
	}
	if strings.Contains(source.Host, ".") {
		if !azureStorageAuthorities[source.Host] {
			return merr.WrapErrParameterInvalidMsg(
				"Azure credential broker IAM requires the source host to be a lowercase Azure Storage authority without a port (core.windows.net, core.chinacloudapi.cn, core.usgovcloudapi.net or core.cloudapi.de), got %q: use azure://<authority>/<container>/<path>, or azure://<container>/<path> with extfs.region",
				source.Host)
		}
		container, _, hasSeparator := strings.Cut(strings.TrimPrefix(source.EscapedPath(), "/"), "/")
		if !hasSeparator || !isValidAzureContainerName(container) {
			return merr.WrapErrParameterInvalidMsg(
				"Azure credential broker IAM requires azure://%s/<container>/[<path>] with a valid container name followed by \"/\", got container %q",
				source.Host, container)
		}
		return nil
	}
	if !isValidAzureContainerName(source.Host) {
		return merr.WrapErrParameterInvalidMsg(
			"Azure credential broker IAM requires the source host to be an Azure Storage authority or a valid container name, got %q",
			source.Host)
	}
	if extfs[ExtfsKeyRegion] == "" {
		return errUnresolvableEndpoint(externalSource, CloudProviderAzure)
	}
	return nil
}

// Broker and GCP target credentials accept lifetimes in [900, 3600], matching
// the milvus-storage provider contract.
const (
	minCredentialLifetimeSeconds = 900
	maxCredentialLifetimeSeconds = 3600
)

// isCredentialLifetime reports whether raw is a plain decimal number of
// seconds in [900, 3600]. milvus-storage parses the value with
// std::from_chars, which rejects a sign, so "+900" must fail admission instead
// of failing every read.
func isCredentialLifetime(raw string) bool {
	if strings.TrimLeft(raw, "0123456789") != "" {
		return false
	}
	seconds, err := strconv.ParseInt(raw, 10, 32)
	return err == nil && seconds >= minCredentialLifetimeSeconds && seconds <= maxCredentialLifetimeSeconds
}

func validateGCPTokenLifetime(extfs map[string]string) error {
	raw := extfs[ExtfsKeyLoadFrequency]
	if raw == "" || isCredentialLifetime(raw) {
		return nil
	}
	return merr.WrapErrParameterInvalidMsg(
		"extfs.%s must be an integer in [%d, %d] for a GCP target or credential broker, got %q", ExtfsKeyLoadFrequency,
		minCredentialLifetimeSeconds, maxCredentialLifetimeSeconds, raw)
}

// effectiveUseSSL follows the property layering in InjectExternalSpecProperties
// (internal/core/src/storage/loon_ffi/util.cpp): a non-empty extfs.use_ssl
// wins; a missing or empty one leaves the scheme default in place, which is
// plain HTTP only for minio:// (C++ DeriveUseSSLFromScheme). endpoint_url
// overrides both and is handled by the caller. Keep the two in lockstep.
func effectiveUseSSL(scheme string, extfs map[string]string) bool {
	if v := extfs[ExtfsKeyUseSSL]; v != "" {
		return v == "true"
	}
	return strings.ToLower(scheme) != SchemeMinIO
}

// validateGCPBearerEndpoint requires the endpoint that receives an OAuth2
// bearer token to be a Google API host over HTTPS. The endpoint follows the
// resolution in InjectExternalSpecProperties
// (internal/core/src/storage/loon_ffi/util.cpp): extfs.endpoint_url when set,
// else the URI host when it is a cloud endpoint host (Milvus-form), else the
// endpoint derived for cloud_provider=gcp (AWS-form); use_ssl likewise comes
// from endpoint_url, else extfs.use_ssl, else the source scheme default.
func validateGCPBearerEndpoint(source *url.URL, sourceScheme string, extfs map[string]string, hasEndpointURL bool) error {
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
	} else if source != nil && IsCloudEndpointHost(source.Host) {
		host = source.Hostname()
	}
	if scheme != "https" {
		return merr.WrapErrParameterInvalidMsg(
			"GCP bearer credentials require HTTPS so the OAuth2 token is not sent in clear text: remove extfs.use_ssl=false or use an https extfs.endpoint_url")
	}
	if !strings.HasSuffix(strings.ToLower(host), ".googleapis.com") {
		return merr.WrapErrParameterInvalidMsg(
			"GCP bearer credentials are only supported for Google Cloud Storage endpoints (*.googleapis.com), got host %q",
			host)
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
		// Microsoft Cloud Germany (core.cloudapi.de) closed in October 2021;
		// germanywestcentral and germanynorth are public Azure regions.
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

// validateS3Broker validates the effective mode before ordinary credential counting.
func validateS3Broker(cp string, extfs map[string]string) error {
	if cp != CloudProviderAWS && cp != CloudProviderAliyun && cp != CloudProviderTencent {
		return merr.WrapErrParameterInvalidMsg("credential broker requires cloud_provider=aws, aliyun, tencent, gcp or azure")
	}
	if extfs[ExtfsKeyRegion] == "" {
		return merr.WrapErrParameterMissingMsg("extfs.region is required for credential broker")
	}
	if extfs[ExtfsKeyUseIAM] != "true" && extfs[ExtfsKeyRoleARN] == "" {
		return merr.WrapErrParameterMissingMsg("extfs.role_arn is required for credential broker AssumeRole")
	}
	forbidden := []string{ExtfsKeyAccessKeyID, ExtfsKeyAccessKeyValue, ExtfsKeyGCPTargetServiceAccount, ExtfsKeyAzureClientID, ExtfsKeyAzureTenantID}
	if extfs[ExtfsKeyUseIAM] == "true" {
		forbidden = append(forbidden, ExtfsKeyRoleARN, ExtfsKeyExternalID, ExtfsKeySessionName)
	}
	for _, key := range forbidden {
		if extfs[key] != "" {
			return merr.WrapErrParameterInvalidMsg("extfs.%s conflicts with credential broker mode", key)
		}
	}
	if extfs[ExtfsKeyAnonymous] == "true" {
		return merr.WrapErrParameterInvalidMsg("anonymous conflicts with credential broker mode")
	}
	if value := extfs[ExtfsKeyLoadFrequency]; value != "" && !isCredentialLifetime(value) {
		return merr.WrapErrParameterInvalidMsg("extfs.load_frequency must be an integer in [900, 3600] for credential broker")
	}
	return nil
}
