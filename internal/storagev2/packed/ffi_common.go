package packed

/*
#cgo pkg-config: milvus_core milvus-storage
#include <stdlib.h>
#include "milvus-storage/ffi_c.h"
#include "arrow/c/abi.h"
#include "arrow/c/helpers.h"
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"unsafe"

	"github.com/cockroachdb/errors"

	_ "github.com/milvus-io/milvus/internal/util/cgo"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/externalspec"
)

// ErrLoonTransient marks any failure surfaced by the loon FFI layer. Today
// milvus-storage does not expose structured error codes, so callers cannot
// distinguish a recoverable concurrent-transaction conflict from a hard IO
// error. We treat all loon failures as retryable for now and rely on a
// bounded retry budget plus outer error handling to keep the worst case
// finite.
//
// TODO(storage v3): once milvus-storage exposes explicit error codes, narrow
// this sentinel to only the concurrent-transaction case (FailResolver) and
// let other errors propagate immediately as retry.Unrecoverable.
var ErrLoonTransient = errors.New("loon FFI transient error")

// Property keys - matching milvus-storage/properties.h
const (
	PropertyFSAddress             = "fs.address"
	PropertyFSBucketName          = "fs.bucket_name"
	PropertyFSAccessKeyID         = "fs.access_key_id"
	PropertyFSAccessKeyValue      = "fs.access_key_value"
	PropertyFSRootPath            = "fs.root_path"
	PropertyFSStorageType         = "fs.storage_type"
	PropertyFSCloudProvider       = "fs.cloud_provider"
	PropertyFSIAMEndpoint         = "fs.iam_endpoint"
	PropertyFSLogLevel            = "fs.log_level"
	PropertyFSRegion              = "fs.region"
	PropertyFSUseSSL              = "fs.use_ssl"
	PropertyFSSSLCACert           = "fs.ssl_ca_cert"
	PropertyFSUseIAM              = "fs.use_iam"
	PropertyFSUseVirtualHost      = "fs.use_virtual_host"
	PropertyFSRequestTimeoutMS    = "fs.request_timeout_ms"
	PropertyFSGCPCredentialJSON   = "fs.gcp_credential_json"
	PropertyFSUseCustomPartUpload = "fs.use_custom_part_upload"
	PropertyFSMaxConnections      = "fs.max_connections"
	PropertyFSTLSMinVersion       = "fs.tls_min_version"
	PropertyFSUseCRC32CChecksum   = "fs.use_crc32c_checksum"

	PropertyWriterPolicy             = "writer.policy"
	PropertyWriterSchemaBasedPattern = "writer.split.schema_based.patterns"

	// CMEK (Customer Managed Encryption Keys) writer properties
	PropertyWriterEncEnable = "writer.enc.enable"    // Enable encryption for written data
	PropertyWriterEncKey    = "writer.enc.key"       // Encryption key for data encryption
	PropertyWriterEncMeta   = "writer.enc.meta"      // Encoded metadata containing zone ID, collection ID, and key version
	PropertyWriterEncAlgo   = "writer.enc.algorithm" // Encryption algorithm (e.g., "AES_GCM_V1")
)

// ensureHTTPScheme prepends an explicit scheme to a bare address so the address
// and use_ssl flag are always self-consistent. Lance's BuildEndpointUrl defaults
// to HTTPS when no scheme is present, which silently contradicts use_ssl=false
// (plaintext MinIO) and leaves the TLS direction underspecified. Normalize:
//   - address already has "://": leave it (explicit caller scheme wins).
//   - no scheme + useSSL=true : prepend "https://".
//   - no scheme + useSSL=false: prepend "http://".
func ensureHTTPScheme(address string, useSSL bool) string {
	if strings.Contains(address, "://") {
		return address
	}
	if useSSL {
		return "https://" + address
	}
	return "http://" + address
}

// ExtfsPrefixForCollection returns the extfs property prefix for a specific collection.
// Each external collection uses its own namespace to avoid conflicts.
func ExtfsPrefixForCollection(collectionID int64) string {
	return fmt.Sprintf("extfs.%d.", collectionID)
}

// extfsField describes one per-collection extfs property. The `isBool` flag
// drives zero-init: string fields default to "", bool-valued fields to "false".
type extfsField struct {
	name   string
	isBool bool
}

// extfsFields is the complete list of per-collection extfs properties that
// live under `extfs.<collectionID>.*`. The list is the contract between the
// Go writer (BuildExtfsOverrides) and the C++ reader (InjectExtfsProperties /
// resolve_config) — the two sides MUST stay in lock-step.
//
// Defaults are zero values: empty string for strings, "false" for bools.
// Authentication fields intentionally default to empty so an unauthenticated
// request reaches the storage layer and fails with a clear 403, rather than
// silently picking up Milvus's internal `fs.*` credentials and widening the
// credential scope without user intent. Callers MUST provide credentials
// through spec.extfs explicitly (AK/SK, role_arn, use_iam=true, or
// anonymous=true — see ValidateExtfsComplete).
var extfsFields = []extfsField{
	{"storage_type", false},
	{"bucket_name", false},
	{"address", false},
	{"root_path", false},
	{"access_key_id", false},
	{"access_key_value", false},
	{"cloud_provider", false},
	{"iam_endpoint", false},
	{"region", false},
	{"ssl_ca_cert", false},
	{"gcp_credential_json", false},
	{"gcp_target_service_account", false},
	{"use_ssl", true},
	{"use_iam", true},
	{"use_virtual_host", true},
}

// BuildExtfsOverrides builds the complete extfs.<collectionID>.* property map
// for an external collection using a three-layer model:
//
//	Layer 0: zero-initialize every extfs field. No inheritance from
//	         storageConfig / `fs.*` — per-collection extfs must be
//	         self-describing, with credentials declared in spec.extfs.
//	         This prevents the Milvus-internal auth mode (e.g. useIAM=true
//	         for the cluster's own bucket) from silently leaking into a
//	         user's external-table credential scope.
//
//	Layer 1: derive bucket / address / storage_type / use_ssl from the
//	         validated external_source URI. The URI is the single source of
//	         non-credential identity data.
//
//	Layer 2: apply spec.extfs (highest priority) — credentials, region,
//	         SSL CA, opaque flags. Empty values are skipped so a
//	         present-but-empty user key cannot clobber a Layer-1 derivation.
//
// Two URI shapes are supported:
//   - Milvus form: scheme://endpoint/bucket/key → URI host is the endpoint,
//     path[0] is the bucket.
//   - AWS form: scheme://bucket/key with spec.extfs.address (or Tier-2
//     derivation from cloud_provider + region) → URI host is the bucket,
//     endpoint comes from spec.extfs.
//
// The caller MUST have run ValidateExternalSource (and ideally
// ValidateExtfsComplete) before invoking this; by the time BuildExtfsOverrides
// runs, externalSource is guaranteed to have a non-empty scheme and host.
// If the invariant is violated (etcd corruption or a code path that bypassed
// the validator), this function panics — mirrors the C++ AssertInfo on the
// same invariant in InjectExtfsProperties, so both sides fail loudly at the
// adjacent site rather than propagating a half-formed config downstream.
func BuildExtfsOverrides(externalSource string,
	specExtfs map[string]string, extfsPrefix string,
) map[string]string {
	overrides := make(map[string]string, len(extfsFields)+len(specExtfs))

	// Layer 0: zero-initialize every bool-valued extfs field. String fields
	// are NOT zero-initialized — milvus-storage rejects empty values for
	// enum-constrained properties like cloud_provider ("value '' not in
	// allowed set"). Missing keys are treated as "not set" by loon, which is
	// the intended semantics; the old `fs.*` baseline inheritance is already
	// removed in C++ InjectExtfsProperties, so dropping string zero-init does
	// not re-open the leak — Layer 1 still overwrites the URI-derived fields
	// and Layer 2 still applies explicit spec overrides.
	for _, f := range extfsFields {
		if f.isBool {
			overrides[extfsPrefix+f.name] = "false"
		}
	}

	// Layer 1: derive from URI.
	u, err := url.Parse(externalSource)
	if err != nil || u.Scheme == "" || u.Host == "" {
		panic(fmt.Sprintf(
			"BuildExtfsOverrides: external_source failed validator invariant "+
				"(scheme+host required): %q (parse err=%v); caller must run "+
				"ValidateExternalSource before invoking",
			externalSource, err))
	}

	scheme := strings.ToLower(u.Scheme)
	overrides[extfsPrefix+"storage_type"] = normalizeStorageType(scheme)
	// Final use_ssl must account for spec-level override before we build the
	// address URL. Otherwise Layer 2 flips use_ssl=false but Layer 1's
	// address still carries an `https://` prefix — Lance's object_store uses
	// that prefixed address as the endpoint verbatim and fails with HTTPS
	// errors against a plaintext MinIO on :9000.
	useSSLStr := deriveUseSSL(scheme)
	if v, ok := specExtfs[extfsPrefix+"use_ssl"]; ok && v != "" {
		useSSLStr = v
	}
	overrides[extfsPrefix+"use_ssl"] = useSSLStr
	useSSL := useSSLStr == "true"

	effectiveAddr := effectiveSpecAddress(specExtfs, extfsPrefix)
	// AWS-style activation: user supplied an endpoint via spec.extfs
	// (explicit address or Tier-2 derivation) AND the URI host is not the
	// same string as the endpoint (equality guard: user wrote Milvus-form
	// redundantly).
	awsStyle := effectiveAddr != "" && u.Host != externalspec.StripURLScheme(effectiveAddr)
	if awsStyle {
		overrides[extfsPrefix+"bucket_name"] = u.Host
		overrides[extfsPrefix+"address"] = effectiveAddr
	} else {
		// Milvus form: host is endpoint, path[0] is bucket.
		path := strings.TrimPrefix(u.Path, "/")
		parts := strings.SplitN(path, "/", 2)
		if len(parts) > 0 && parts[0] != "" {
			overrides[extfsPrefix+"bucket_name"] = parts[0]
		}
		overrides[extfsPrefix+"address"] = ensureHTTPScheme(u.Host, useSSL)
	}

	// Layer 2: spec.extfs overrides. Empty values skipped so a present-but-
	// empty user key cannot clobber the Layer-1 derivation (e.g. the user
	// provided spec.extfs.address="" — that must not erase the URI-derived
	// address).
	for k, v := range specExtfs {
		if v == "" {
			continue
		}
		overrides[k] = v
	}

	return overrides
}

// normalizeStorageType maps a URI scheme to the storage_type string understood
// by the milvus-storage FFI layer. Every scheme in allowedExternalSourceSchemes
// is a remote object store; the driver is dispatched inside milvus-storage by
// scheme prefix rather than storage_type. Local paths go through a different
// entry point (MakeInternalLocalProperies) and never reach this function.
// The parameter is accepted for symmetry with the C++ side and to document
// the "scheme in → storage_type out" contract.
func normalizeStorageType(scheme string) string {
	_ = scheme
	return "remote"
}

// deriveUseSSL returns the default SSL flag implied by the URI scheme. AWS /
// public cloud schemes default to TLS; MinIO defaults to plaintext because
// self-hosted MinIO clusters are commonly deployed without TLS on a private
// network. Users can always override via spec.extfs.use_ssl.
func deriveUseSSL(scheme string) string {
	switch scheme {
	case "minio":
		return "false"
	}
	return "true"
}

// effectiveSpecAddress returns the effective endpoint for a prefixed spec
// extfs map using the 3-tier priority:
//  1. spec.extfs.<prefix>address (explicit)
//  2. DeriveEndpoint(<prefix>cloud_provider, <prefix>region) (5 public clouds)
//  3. "" (defer to URI host — Milvus-form semantics)
//
// Mirror of externalspec.EffectiveAddressFromExtfs but adapted to the
// prefix-per-key layout used at the packed FFI layer.
func effectiveSpecAddress(specExtfs map[string]string, extfsPrefix string) string {
	if len(specExtfs) == 0 {
		return ""
	}
	if v, ok := specExtfs[extfsPrefix+"address"]; ok && v != "" {
		return v
	}
	if d, ok := externalspec.DeriveEndpoint(
		specExtfs[extfsPrefix+"cloud_provider"],
		specExtfs[extfsPrefix+"region"]); ok {
		return d
	}
	return ""
}

// NormalizeExternalSource is the prefix-aware entry point on the packed/FFI
// side. It resolves the effective endpoint from a prefixed spec extfs map and
// delegates the actual URI rewrite to externalspec.RewriteAWSStyleToMilvusForm
// so the two entry points (pkg/externalspec JSON variant, packed prefix-map
// variant) share a single implementation of the safety gates (empty-endpoint,
// empty/unsafe host, equality guard, url.URL-based reassembly).
func NormalizeExternalSource(externalSource string, specExtfs map[string]string, extfsPrefix string) string {
	return externalspec.RewriteAWSStyleToMilvusForm(
		externalSource,
		effectiveSpecAddress(specExtfs, extfsPrefix),
	)
}

// isSafeURIHost / stripURLScheme kept as local aliases so existing callers and
// tests in this package continue to compile; both delegate to the exported
// externalspec helpers, which are the single source of truth.
func isSafeURIHost(host string) bool       { return externalspec.IsSafeURIHost(host) }
func stripURLScheme(address string) string { return externalspec.StripURLScheme(address) }

// MakePropertiesFromStorageConfig creates a Properties object from StorageConfig
// This function converts a StorageConfig structure into a Properties object by
// calling the FFI properties_create function. All configuration fields from
// StorageConfig are mapped to corresponding key-value pairs in Properties.
func MakePropertiesFromStorageConfig(storageConfig *indexpb.StorageConfig, extraKVs map[string]string) (*C.LoonProperties, error) {
	if storageConfig == nil {
		return nil, fmt.Errorf("storageConfig is required")
	}

	// Prepare key-value pairs from StorageConfig
	var keys []string
	var values []string

	// Add non-empty string fields
	if storageConfig.GetAddress() != "" {
		keys = append(keys, PropertyFSAddress)
		values = append(values, ensureHTTPScheme(storageConfig.GetAddress(), storageConfig.GetUseSSL()))
	}
	if storageConfig.GetBucketName() != "" {
		keys = append(keys, PropertyFSBucketName)
		values = append(values, storageConfig.GetBucketName())
	}
	if storageConfig.GetAccessKeyID() != "" {
		keys = append(keys, PropertyFSAccessKeyID)
		values = append(values, storageConfig.GetAccessKeyID())
	}
	if storageConfig.GetSecretAccessKey() != "" {
		keys = append(keys, PropertyFSAccessKeyValue)
		values = append(values, storageConfig.GetSecretAccessKey())
	}
	if storageConfig.GetRootPath() != "" {
		keys = append(keys, PropertyFSRootPath)
		values = append(values, storageConfig.GetRootPath())
	}
	if storageConfig.GetStorageType() != "" {
		keys = append(keys, PropertyFSStorageType)
		values = append(values, storageConfig.GetStorageType())
	}
	if storageConfig.GetCloudProvider() != "" {
		keys = append(keys, PropertyFSCloudProvider)
		values = append(values, storageConfig.GetCloudProvider())
	}
	if storageConfig.GetIAMEndpoint() != "" {
		keys = append(keys, PropertyFSIAMEndpoint)
		values = append(values, storageConfig.GetIAMEndpoint())
	}
	keys = append(keys, PropertyFSLogLevel)
	values = append(values, "warn")

	if storageConfig.GetRegion() != "" {
		keys = append(keys, PropertyFSRegion)
		values = append(values, storageConfig.GetRegion())
	}
	if storageConfig.GetSslCACert() != "" {
		keys = append(keys, PropertyFSSSLCACert)
		values = append(values, storageConfig.GetSslCACert())
	}
	if storageConfig.GetGcpCredentialJSON() != "" {
		keys = append(keys, PropertyFSGCPCredentialJSON)
		values = append(values, storageConfig.GetGcpCredentialJSON())
	}

	// Add boolean fields
	keys = append(keys, PropertyFSUseSSL)
	if storageConfig.GetUseSSL() {
		values = append(values, "true")
	} else {
		values = append(values, "false")
	}

	keys = append(keys, PropertyFSUseIAM)
	if storageConfig.GetUseIAM() {
		values = append(values, "true")
	} else {
		values = append(values, "false")
	}

	keys = append(keys, PropertyFSUseVirtualHost)
	if storageConfig.GetUseVirtualHost() {
		values = append(values, "true")
	} else {
		values = append(values, "false")
	}

	keys = append(keys, PropertyFSUseCustomPartUpload)
	values = append(values, "true") // hardcoded to true as in the original code

	// Add integer field
	keys = append(keys, PropertyFSRequestTimeoutMS)
	values = append(values, strconv.FormatInt(storageConfig.GetRequestTimeoutMs(), 10))

	// Add TLS min version (skip "default" — consistent with C++ layer filtering)
	if v := storageConfig.GetSslTlsMinVersion(); v != "" && v != "default" {
		keys = append(keys, PropertyFSTLSMinVersion)
		values = append(values, v)
	}

	// Add CRC32C checksum
	keys = append(keys, PropertyFSUseCRC32CChecksum)
	if storageConfig.GetUseCrc32CChecksum() {
		values = append(values, "true")
	} else {
		values = append(values, "false")
	}

	// No extfs.default.* properties here. Per-collection extfs properties
	// (extfs.{collectionID}.*) are passed via extraKVs by BuildExtfsOverrides.

	// Add extra kvs (override existing keys if present)
	for k, v := range extraKVs {
		found := false
		for i, existingKey := range keys {
			if existingKey == k {
				values[i] = v
				found = true
				break
			}
		}
		if !found {
			keys = append(keys, k)
			values = append(values, v)
		}
	}

	// Convert to C arrays
	cKeys := make([]*C.char, len(keys))
	cValues := make([]*C.char, len(values))
	for i := range keys {
		cKeys[i] = C.CString(keys[i])
		cValues[i] = C.CString(values[i])
	}
	// Defer cleanup of all C strings
	defer func() {
		for i := range cKeys {
			C.free(unsafe.Pointer(cKeys[i]))
			C.free(unsafe.Pointer(cValues[i]))
		}
	}()

	// Create Properties using FFI
	properties := &C.LoonProperties{}
	var cKeysPtr **C.char
	var cValuesPtr **C.char
	if len(cKeys) > 0 {
		cKeysPtr = &cKeys[0]
		cValuesPtr = &cValues[0]
	}

	result := C.loon_properties_create(
		(**C.char)(unsafe.Pointer(cKeysPtr)),
		(**C.char)(unsafe.Pointer(cValuesPtr)),
		C.size_t(len(keys)),
		properties,
	)

	err := HandleLoonFFIResult(result)
	if err != nil {
		return nil, err
	}
	return properties, nil
}

// FreeProperties releases a C-allocated LoonProperties object.
func FreeProperties(props *C.LoonProperties) {
	if props != nil {
		C.loon_properties_free(props)
	}
}

func HandleLoonFFIResult(ffiResult C.LoonFFIResult) error {
	defer C.loon_ffi_free_result(&ffiResult)
	if C.loon_ffi_is_success(&ffiResult) == 0 {
		errMsg := C.loon_ffi_get_errmsg(&ffiResult)
		errStr := "Unknown error"
		if errMsg != nil {
			errStr = C.GoString(errMsg)
		}

		return errors.Wrapf(ErrLoonTransient, "FFI operation failed: %s", errStr)
	}
	return nil
}

type ManifestJSON struct {
	ManifestVersion int64  `json:"ver"`
	BasePath        string `json:"base_path"`
}

func MarshalManifestPath(basePath string, version int64) string {
	bs, err := json.Marshal(ManifestJSON{
		ManifestVersion: version,
		BasePath:        basePath,
	})
	if err != nil {
		// json.Marshal on string+int64 struct should never fail, but log if it does
		return fmt.Sprintf(`{"ver":%d,"base_path":"%s"}`, version, basePath)
	}
	return string(bs)
}

func UnmarshalManifestPath(manifestPath string) (string, int64, error) {
	var manifestJSON ManifestJSON
	err := json.Unmarshal([]byte(manifestPath), &manifestJSON)
	if err != nil {
		return "", 0, err
	}
	return manifestJSON.BasePath, manifestJSON.ManifestVersion, nil
}

// CompareManifestPath compares two manifest paths by their version.
func CompareManifestPath(a, b string) (int, error) {
	if a == b {
		return 0, nil
	}

	aBase, aVer, aErr := UnmarshalManifestPath(a)
	bBase, bVer, bErr := UnmarshalManifestPath(b)

	if aErr != nil {
		return 0, fmt.Errorf("failed to parse manifest path %q: %w", a, aErr)
	}
	if bErr != nil {
		return 0, fmt.Errorf("failed to parse manifest path %q: %w", b, bErr)
	}

	if aBase != bBase {
		return 0, fmt.Errorf("manifest paths have different base paths: %q vs %q", aBase, bBase)
	}

	switch {
	case aVer < bVer:
		return -1, nil
	case aVer > bVer:
		return 1, nil
	default:
		return 0, nil
	}
}
