// Copyright 2023 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <nlohmann/json.hpp>
#include <stdint.h>
#include <algorithm>
#include <initializer_list>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include "arrow/result.h"
#include "arrow/status.h"
#include "simdjson.h"
#include "common/EasyAssert.h"
#include "log/Log.h"
#include "common/type_c.h"
#include "milvus-storage/common/extend_status.h"
#include "milvus-storage/ffi_c.h"
#include "milvus-storage/ffi_internal/ffi_error_code.h"
#include "milvus-storage/ffi_internal/result.h"
#include "milvus-storage/filesystem/fs.h"
#include "milvus-storage/properties.h"
#include "milvus-storage/transaction/transaction.h"
#include "nlohmann/json_fwd.hpp"
#include "storage/Types.h"
#include "storage/loon_ffi/external_spec_c.h"
#include "storage/loon_ffi/property_singleton.h"
#include "storage/loon_ffi/util.h"

using json = nlohmann::json;

std::shared_ptr<milvus_storage::api::Properties>
MakeInternalPropertiesFromStorageConfig(CStorageConfig c_storage_config) {
    auto properties_map = std::make_shared<milvus_storage::api::Properties>();

    // Add non-null string fields
    if (c_storage_config.address != nullptr) {
        milvus_storage::api::SetValue(
            *properties_map, PROPERTY_FS_ADDRESS, c_storage_config.address);
    }
    if (c_storage_config.bucket_name != nullptr) {
        milvus_storage::api::SetValue(*properties_map,
                                      PROPERTY_FS_BUCKET_NAME,
                                      c_storage_config.bucket_name);
    }
    if (c_storage_config.access_key_id != nullptr) {
        milvus_storage::api::SetValue(*properties_map,
                                      PROPERTY_FS_ACCESS_KEY_ID,
                                      c_storage_config.access_key_id);
    }
    if (c_storage_config.access_key_value != nullptr) {
        milvus_storage::api::SetValue(*properties_map,
                                      PROPERTY_FS_ACCESS_KEY_VALUE,
                                      c_storage_config.access_key_value);
    }
    if (c_storage_config.root_path != nullptr) {
        milvus_storage::api::SetValue(
            *properties_map, PROPERTY_FS_ROOT_PATH, c_storage_config.root_path);
    }
    if (c_storage_config.storage_type != nullptr) {
        milvus_storage::api::SetValue(*properties_map,
                                      PROPERTY_FS_STORAGE_TYPE,
                                      c_storage_config.storage_type);
    }
    if (c_storage_config.cloud_provider != nullptr) {
        milvus_storage::api::SetValue(*properties_map,
                                      PROPERTY_FS_CLOUD_PROVIDER,
                                      c_storage_config.cloud_provider);
    }
    if (c_storage_config.iam_endpoint != nullptr) {
        milvus_storage::api::SetValue(*properties_map,
                                      PROPERTY_FS_IAM_ENDPOINT,
                                      c_storage_config.iam_endpoint);
    }
    if (c_storage_config.log_level != nullptr) {
        milvus_storage::api::SetValue(
            *properties_map, PROPERTY_FS_LOG_LEVEL, "Warn");
    }
    if (c_storage_config.region != nullptr) {
        milvus_storage::api::SetValue(
            *properties_map, PROPERTY_FS_REGION, c_storage_config.region);
    }
    if (c_storage_config.sslCACert != nullptr) {
        milvus_storage::api::SetValue(*properties_map,
                                      PROPERTY_FS_SSL_CA_CERT,
                                      c_storage_config.sslCACert);
    }
    if (c_storage_config.gcp_credential_json != nullptr) {
        milvus_storage::api::SetValue(*properties_map,
                                      PROPERTY_FS_GCP_CREDENTIAL_JSON,
                                      c_storage_config.gcp_credential_json);
    }

    // Add boolean fields
    milvus_storage::api::SetValue(*properties_map,
                                  PROPERTY_FS_USE_SSL,
                                  c_storage_config.useSSL ? "true" : "false");
    milvus_storage::api::SetValue(*properties_map,
                                  PROPERTY_FS_USE_IAM,
                                  c_storage_config.useIAM ? "true" : "false");
    milvus_storage::api::SetValue(
        *properties_map,
        PROPERTY_FS_USE_VIRTUAL_HOST,
        c_storage_config.useVirtualHost ? "true" : "false");
    milvus_storage::api::SetValue(
        *properties_map,
        PROPERTY_FS_USE_CUSTOM_PART_UPLOAD,
        c_storage_config.use_custom_part_upload ? "true" : "false");

    // Add integer fields
    milvus_storage::api::SetValue(
        *properties_map,
        PROPERTY_FS_REQUEST_TIMEOUT_MS,
        std::to_string(c_storage_config.requestTimeoutMs).c_str());
    // Absent when unset: leave the key absent so milvus-storage applies its
    // registered default instead of taking an explicit 0.
    if (c_storage_config.max_connections > 0) {
        milvus_storage::api::SetValue(
            *properties_map,
            PROPERTY_FS_MAX_CONNECTIONS,
            std::to_string(c_storage_config.max_connections).c_str());
    }

    if (c_storage_config.tls_min_version != nullptr) {
        std::string tls_ver(c_storage_config.tls_min_version);
        if (!tls_ver.empty() && tls_ver != "default") {
            milvus_storage::api::SetValue(*properties_map,
                                          PROPERTY_FS_TLS_MIN_VERSION,
                                          c_storage_config.tls_min_version);
        }
    }

    milvus_storage::api::SetValue(
        *properties_map,
        PROPERTY_FS_USE_CRC32C_CHECKSUM,
        c_storage_config.use_crc32c_checksum ? "true" : "false");

    // Carry the configured arrow reader prebuffer limits into every
    // freshly-built properties map. Index build (index_c.cpp) and the FFI
    // readers construct properties per task through this helper instead of
    // going through LoonFFIPropertiesSingleton's cached map, so without this
    // the common.arrow.reader.* configuration never reaches those reads and
    // external-table index builds are stuck with arrow's default range
    // coalescing (effectively one in-flight S3 range per file).
    milvus::storage::LoonFFIPropertiesSingleton::GetInstance()
        .ApplyArrowReaderConfig(*properties_map);

    return properties_map;
}

// kAllowedExtfsSpecKeys is the C++-side defense-in-depth allowlist for extfs
// keys that a user-supplied external_spec.extfs JSON object is allowed to
// override. This MUST stay in lock-step with allowedExtfsKeys in
// pkg/util/externalspec/external_spec.go. The Go side is the primary filter;
// this set exists so that any future bypass of the Go validation (new
// internal callers, bugs, admin tools) cannot turn external_spec into an
// arbitrary property-injection point into milvus-storage.
//
// Any key outside this set is dropped with a warning — we do not fail the
// request because the Go side should have rejected it already, so reaching
// here means either a programming error or an attempted bypass.
static const std::unordered_set<std::string> kAllowedExtfsSpecKeys = {
    "use_iam",
    "use_ssl",
    "use_virtual_host",
    "region",
    "cloud_provider",
    "endpoint_url",
    "iam_endpoint",
    "storage_type",
    "ssl_ca_cert",
    "access_key_id",
    "access_key_value",
    "role_arn",
    "session_name",
    "external_id",
    "load_frequency",
    "gcp_target_service_account",
    "bucket_name",
    "anonymous",
    "azure_client_id",
    "azure_tenant_id",
    "azure_credential_endpoint",
};

// kExtfsFields is the contract with Go extfsFields in
// internal/storagev2/packed/ffi_common.go. Keep in lockstep.
static const std::vector<std::pair<std::string, bool /*is_bool*/>>
    kExtfsFields = {
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
        {"gcp_target_service_account", false},
        {"use_ssl", true},
        {"use_iam", true},
        {"use_virtual_host", true},
        {"anonymous", true},
};

static std::string
ExternalFsPropertyKey(const std::string& extfs_prefix,
                      const char* fs_property) {
    constexpr std::string_view fs_prefix = PROPERTY_FS_PREFIX;
    std::string_view property(fs_property);
    AssertInfo(property.substr(0, fs_prefix.size()) == fs_prefix,
               "Invalid filesystem property key: {}",
               std::string(property));
    return extfs_prefix + std::string(property.substr(fs_prefix.size()));
}

// kExtfsInheritedFsFields lists fs.* properties that extfs.{collectionID}.*
// inherits verbatim from the process-level storage config.
//
// milvus-storage builds each external filesystem purely from its own
// extfs.<name>.* keys (ExtractExternalFsProperties) — there is no fallback to
// fs.*, and a missing key silently takes the library's built-in default. So
// anything that is server-side capacity/tuning config rather than part of the
// external source's identity has to be copied across explicitly, or it is a
// no-op on every external read.
//
// These are deliberately NOT in kAllowedExtfsSpecKeys: they come from
// milvus.yaml, not from a user-supplied external_spec.
static const std::vector<const char*> kExtfsInheritedFsFields = {
    PROPERTY_FS_MAX_CONNECTIONS,
};

// PropertyValueAsString renders a PropertyVariant back to the string form
// SetValue accepts. Both variant shapes reach here: the C++ index-build path
// goes through SetValue, which stores registry-typed values (max_connections
// is UINT32), while loon_properties_inject_external_spec rebuilds the map
// from a flat LoonProperties and stores everything as std::string.
static std::optional<std::string>
PropertyValueAsString(const milvus_storage::api::Properties& properties,
                      const char* key) {
    auto it = properties.find(key);
    if (it == properties.end()) {
        return std::nullopt;
    }
    return std::visit(
        [](const auto& v) -> std::optional<std::string> {
            using T = std::decay_t<decltype(v)>;
            if constexpr (std::is_same_v<T, std::string>) {
                return v.empty() ? std::nullopt : std::optional<std::string>(v);
            } else if constexpr (std::is_same_v<T, bool>) {
                return std::optional<std::string>(v ? "true" : "false");
            } else if constexpr (std::is_integral_v<T>) {
                return std::optional<std::string>(std::to_string(v));
            } else {
                // vector<string> / nullptr_t: no scalar rendering, and no
                // inherited field uses them.
                return std::nullopt;
            }
        },
        it->second);
}

static std::string
DeriveUseSSLFromScheme(const std::string& scheme) {
    if (scheme == "minio") {
        return "false";
    }
    return "true";
}

// DeriveEndpoint: mirror of Go externalspec.DeriveEndpoint. Keep in lockstep.
static std::string
DeriveEndpoint(const std::string& cloud_provider, const std::string& region) {
    std::string cp = cloud_provider;
    std::transform(cp.begin(), cp.end(), cp.begin(), [](unsigned char c) {
        return std::tolower(c);
    });
    if (cp == "aws") {
        if (region.empty()) {
            return "";
        }
        if (region.rfind("cn-", 0) == 0) {
            return "https://s3." + region + ".amazonaws.com.cn";
        }
        return "https://s3." + region + ".amazonaws.com";
    }
    if (cp == "gcp") {
        return "https://storage.googleapis.com";
    }
    if (cp == "aliyun") {
        if (region.empty()) {
            return "";
        }
        return "https://oss-" + region + ".aliyuncs.com";
    }
    if (cp == "tencent") {
        if (region.empty()) {
            return "";
        }
        return "https://cos." + region + ".myqcloud.com";
    }
    if (cp == "huawei") {
        if (region.empty()) {
            return "";
        }
        return "https://obs." + region + ".myhuaweicloud.com";
    }
    if (cp == "azure") {
        // Azure endpoint derivation: "region" slot selects sovereign cloud.
        // AzureFileSystemProducer concatenates ".blob."/".dfs." onto
        // config_.address verbatim, so the returned value is a bare authority.
        //
        // Azurite / Azure Stack Hub: use Milvus-form URI
        // (azure://<custom-host>/<container>/<blob>) instead of derive.
        std::string r = region;
        std::transform(r.begin(), r.end(), r.begin(), [](unsigned char c) {
            return std::tolower(c);
        });
        if (r.empty()) {
            // Empty region → not derivable. Caller must supply Milvus-form URI.
            return "";
        }
        if (r.rfind("china", 0) == 0) {
            return "core.chinacloudapi.cn";
        }
        if (r.rfind("usgov", 0) == 0 || r.rfind("usdod", 0) == 0) {
            return "core.usgovcloudapi.net";
        }
        if (r.rfind("germany", 0) == 0) {
            return "core.cloudapi.de";
        }
        return "core.windows.net";
    }
    return "";
}

static std::string
StripURIScheme(const std::string& addr) {
    auto pos = addr.find("://");
    return (pos != std::string::npos) ? addr.substr(pos + 3) : addr;
}

// IsCloudEndpointHost returns true when `host` matches a known cloud provider
// domain family. Used to identify Milvus-form URIs whose host is an endpoint
// variant (global / accelerate / dual-stack / VPC / FIPS / sovereign) that
// DeriveEndpoint does not produce verbatim. When the host belongs to a cloud
// family, the URI is Milvus-form regardless of whether DeriveEndpoint(cp,
// region) equals the host string. Keep list in sync with Go
// externalspec.IsCloudEndpointHost.
static bool
IsCloudEndpointHost(const std::string& host) {
    std::string h = host;
    std::transform(h.begin(), h.end(), h.begin(), [](unsigned char c) {
        return std::tolower(c);
    });
    auto ends_with = [&](const std::string& suffix) {
        return h.size() >= suffix.size() &&
               h.compare(h.size() - suffix.size(), suffix.size(), suffix) == 0;
    };
    // AWS (global, regional, accelerate, dualstack, FIPS, VPC, China).
    if (ends_with(".amazonaws.com") || ends_with(".amazonaws.com.cn")) {
        return true;
    }
    // GCP Cloud Storage.
    if (ends_with(".googleapis.com")) {
        return true;
    }
    // Aliyun OSS (public, internal, accelerate, VPC).
    if (ends_with(".aliyuncs.com")) {
        return true;
    }
    // Tencent Cloud COS.
    if (ends_with(".myqcloud.com")) {
        return true;
    }
    // Huawei Cloud OBS.
    if (ends_with(".myhuaweicloud.com")) {
        return true;
    }
    // Azure Blob Storage (public, China, US Gov, Germany).
    if (ends_with(".core.windows.net") || ends_with(".core.chinacloudapi.cn") ||
        ends_with(".core.usgovcloudapi.net") ||
        ends_with(".core.cloudapi.de")) {
        return true;
    }
    return false;
}

static void
InjectExternalSpecProperties(
    milvus_storage::api::Properties& properties,
    int64_t collection_id,
    const std::string& external_source,
    const std::string& external_spec,
    const milvus::storage::ExternalIopsConfig& iops_config) {
    std::string extfs_prefix = std::string(PROPERTY_EXTFS_PREFIX) +
                               std::to_string(collection_id) + ".";

    // Layer 0: zero-init bool fields only (milvus-storage rejects empty
    // strings on enum-constrained keys like cloud_provider).
    //
    // "anonymous" is intentionally skipped: milvus-storage's fs.* property
    // registry does not yet declare PROPERTY_FS_ANONYMOUS, so any SetValue
    // on "extfs.<cid>.anonymous" hits strict "undefined key" in
    // ExtractExternalFsProperties (iceberg explore path). Skipping the
    // default zero-init keeps the slot absent — equivalent to anonymous=false
    // — so the default credential path stays unaffected. Once the upstream
    // registry adds the key, this skip can be removed without further
    // changes (and user-supplied anonymous=true via Layer 2 will start
    // working automatically — we deliberately do NOT drop it on Layer 2 so
    // the future fix is a one-line revert here).
    for (const auto& [name, is_bool] : kExtfsFields) {
        if (is_bool && name != "anonymous") {
            properties[extfs_prefix + name] = std::string("false");
        }
    }

    // IOPS limiting is an External Table read policy, not an attribute of the
    // internal filesystem. Keep it out of fs.* and stamp it only into the
    // per-collection extfs namespace. These keys are intentionally absent
    // from kAllowedExtfsSpecKeys, so external_spec cannot override them.
    const auto initial_rate_key =
        ExternalFsPropertyKey(extfs_prefix, PROPERTY_FS_IOPS_INITIAL_RATE);
    const auto max_rate_key =
        ExternalFsPropertyKey(extfs_prefix, PROPERTY_FS_IOPS_MAX_RATE);
    milvus_storage::api::SetValue(
        properties,
        initial_rate_key.c_str(),
        std::to_string(iops_config.initial_rate).c_str());
    milvus_storage::api::SetValue(properties,
                                  max_rate_key.c_str(),
                                  std::to_string(iops_config.max_rate).c_str());

    // Layer 0b: inherit process-level fs.* tuning. Position relative to the
    // later layers is arbitrary — none of these keys is in
    // kAllowedExtfsSpecKeys nor derived from the URI, so no other layer
    // writes them; it sits next to Layer 0 because both are defaults rather
    // than external-source-derived values.
    // "0" is treated as unset, like the empty string: for every fs.* tunable
    // inherited here, 0 means "producer did not set it" (the convention in
    // ChunkManager.cpp, MinioChunkManager.cpp and the Go producer). Mirroring
    // it would be worse than not mirroring at all — before this key existed on
    // the extfs side, ExtractExternalFsProperties fell back to the library
    // default (100 connections), whereas an explicit "0" overrides it and
    // caps the external S3 client at max(io_capacity, 25). The producers are
    // guarded too; this is the last line of defence for the read path this
    // whole change exists to widen.
    for (const auto* fs_key : kExtfsInheritedFsFields) {
        auto value = PropertyValueAsString(properties, fs_key);
        if (!value.has_value() || *value == "0") {
            continue;
        }
        const auto extfs_key = ExternalFsPropertyKey(extfs_prefix, fs_key);
        milvus_storage::api::SetValue(
            properties, extfs_key.c_str(), value->c_str());
    }

    // Layer 1: derive bucket / address / storage_type / use_ssl from URI.
    // Caller must have run Go ValidateExternalSource; malformed here
    // signals etcd corruption or bypass — fail adjacent to bad input.
    auto scheme_end = external_source.find("://");
    AssertInfo(scheme_end != std::string::npos,
               "external_source for collection {} missing scheme: {}",
               collection_id,
               external_source);

    std::string scheme = external_source.substr(0, scheme_end);
    auto rest = external_source.substr(scheme_end + 3);
    auto slash_pos = rest.find('/');
    // No slash after authority (e.g. "s3://mybucket"): entire rest is host,
    // path is empty. Only assert against the pathological "scheme://" case
    // where rest itself is empty.
    std::string host =
        (slash_pos != std::string::npos) ? rest.substr(0, slash_pos) : rest;
    AssertInfo(!host.empty(),
               "external_source for collection {} has empty host: {}",
               collection_id,
               external_source);

    std::string path_part =
        (slash_pos != std::string::npos) ? rest.substr(slash_pos + 1) : "";
    auto bucket_end = path_part.find('/');
    std::string path_bucket = (bucket_end != std::string::npos)
                                  ? path_part.substr(0, bucket_end)
                                  : path_part;

    milvus_storage::api::SetValue(
        properties, (extfs_prefix + "storage_type").c_str(), "remote");
    milvus_storage::api::SetValue(properties,
                                  (extfs_prefix + "use_ssl").c_str(),
                                  DeriveUseSSLFromScheme(scheme).c_str());

    // Default to Milvus-form (URI.host as endpoint). Layer 3 may rewrite
    // to AWS-form when DeriveEndpoint(cp, region) differs from URI.host.
    std::string address = host;
    if (address.find("://") == std::string::npos &&
        DeriveUseSSLFromScheme(scheme) == "false") {
        address = "http://" + address;
    }
    milvus_storage::api::SetValue(
        properties, (extfs_prefix + "address").c_str(), address.c_str());
    if (!path_bucket.empty()) {
        milvus_storage::api::SetValue(properties,
                                      (extfs_prefix + "bucket_name").c_str(),
                                      path_bucket.c_str());
    }

    // Layer 2: apply spec.extfs JSON. Gate every key through
    // kAllowedExtfsSpecKeys — defense-in-depth against property injection
    // if a caller bypasses Go ParseExternalSpec. Note: "address" is NOT in
    // the allowlist. endpoint_url is captured below and translated to the
    // internal address property instead of being forwarded verbatim.
    std::string spec_cloud_provider;
    std::string spec_region;
    std::string spec_endpoint_url;
    bool has_spec_endpoint_url = false;
    if (!external_spec.empty()) {
        try {
            simdjson::ondemand::parser parser;
            simdjson::padded_string padded(external_spec);
            auto doc = parser.iterate(padded);
            auto extfs_obj = doc.find_field("extfs");
            if (extfs_obj.error() == simdjson::SUCCESS) {
                for (auto field : extfs_obj.get_object()) {
                    try {
                        auto key = std::string(field.unescaped_key().value());
                        if (kAllowedExtfsSpecKeys.find(key) ==
                            kAllowedExtfsSpecKeys.end()) {
                            LOG_WARN(
                                "extfs key '{}' not in allowlist, dropped "
                                "(collection_id={}); Go-side validation "
                                "should have already rejected this",
                                key,
                                collection_id);
                            continue;
                        }
                        auto val =
                            std::string(field.value().get_string().value());
                        if (key == "endpoint_url") {
                            has_spec_endpoint_url = true;
                            spec_endpoint_url = val;
                            continue;
                        }
                        if (val.empty()) {
                            continue;
                        }
                        if (key == "cloud_provider") {
                            spec_cloud_provider = val;
                        } else if (key == "region") {
                            spec_region = val;
                        }
                        // "minio" is a Milvus-only cloud_provider sentinel
                        // meaning "self-hosted S3-compatible, do not derive
                        // endpoint and do not swap host". loon's allowlist
                        // does not include it, so propagate the value only
                        // for our swap-decision logic above and drop it
                        // before reaching loon.
                        if (key == "cloud_provider" && val == "minio") {
                            continue;
                        }
                        milvus_storage::api::SetValue(
                            properties,
                            (extfs_prefix + key).c_str(),
                            val.c_str());
                    } catch (const simdjson::simdjson_error& per_field) {
                        LOG_WARN(
                            "extfs field parse failed, skipping "
                            "(collection_id={}): {}",
                            collection_id,
                            per_field.what());
                        continue;
                    }
                }
            }
        } catch (const simdjson::simdjson_error& e) {
            LOG_ERROR(
                "Failed to parse external_spec for extfs overrides "
                "(collection_id={}): {}",
                collection_id,
                e.what());
            ThrowInfo(milvus::ErrorCode::UnexpectedError,
                      "external_spec parse failed for collection {}: {}",
                      collection_id,
                      e.what());
        }
    }

    // Layer 3: resolve the effective endpoint.
    //
    // endpoint_url selects explicit-endpoint mode. In this mode
    // external_source has the standard scheme://bucket/key shape: URI.host is
    // the bucket, while endpoint_url is the physical S3-compatible address.
    // The Go API boundary validates the full URL contract. The checks here are
    // defense in depth for corrupted metadata or callers that bypass Go.
    if (has_spec_endpoint_url) {
        std::string effective_endpoint = spec_endpoint_url;
        if (!effective_endpoint.empty() && effective_endpoint.back() == '/') {
            effective_endpoint.pop_back();
        }
        auto endpoint_scheme_end = effective_endpoint.find("://");
        AssertInfo(endpoint_scheme_end != std::string::npos,
                   "extfs.endpoint_url for collection {} missing scheme: {}",
                   collection_id,
                   spec_endpoint_url);
        std::string endpoint_scheme =
            effective_endpoint.substr(0, endpoint_scheme_end);
        std::transform(endpoint_scheme.begin(),
                       endpoint_scheme.end(),
                       endpoint_scheme.begin(),
                       [](unsigned char c) { return std::tolower(c); });
        AssertInfo(endpoint_scheme == "http" || endpoint_scheme == "https",
                   "extfs.endpoint_url for collection {} must use http or "
                   "https: {}",
                   collection_id,
                   spec_endpoint_url);
        std::string endpoint_authority =
            effective_endpoint.substr(endpoint_scheme_end + 3);
        AssertInfo(
            !endpoint_authority.empty() &&
                endpoint_authority.find_first_of("/@?#") == std::string::npos,
            "extfs.endpoint_url for collection {} must contain only "
            "scheme and authority: {}",
            collection_id,
            spec_endpoint_url);

        effective_endpoint = endpoint_scheme + "://" + endpoint_authority;
        milvus_storage::api::SetValue(properties,
                                      (extfs_prefix + "address").c_str(),
                                      effective_endpoint.c_str());
        milvus_storage::api::SetValue(
            properties, (extfs_prefix + "bucket_name").c_str(), host.c_str());
        milvus_storage::api::SetValue(
            properties,
            (extfs_prefix + "use_ssl").c_str(),
            endpoint_scheme == "https" ? "true" : "false");
    } else {
        // AWS-form disambiguation via derived-endpoint vs URI.host.
        //   Compute DeriveEndpoint(cp, region). If non-empty and different
        //   from URI.host, source is AWS-form (scheme://bucket/key) — swap
        //   URI.host into bucket_name and write derived endpoint as address.
        //   Otherwise Milvus-form (URI.host == endpoint) or unresolvable —
        //   keep Layer1 values.
        //
        //   Path-segment heuristics are unreliable: AWS-form S3 keys may
        //   contain '/' (`s3://bucket/deep/key.parquet`) and Milvus-form URIs
        //   may omit keys (`s3://endpoint/bucket`). Comparing derive result
        //   with host handles both cases correctly — the derived endpoint
        //   string is a stable cloud-provider identifier, not dependent on
        //   path structure.
        // Swap decision uses ONLY the user-supplied cloud_provider, never a
        // scheme-inferred fallback. Self-hosted MinIO with
        // `s3://localhost:9000/bucket/...` is Milvus-form; inferring
        // cloud_provider=aws from scheme=s3 would produce
        // derived=https://s3.<region>.amazonaws.com, falsely classify host
        // as a bucket, and swap. When the user does not declare
        // cloud_provider, DeriveEndpoint returns empty and the URI is
        // treated as Milvus-form (host is endpoint).
        std::string derived = DeriveEndpoint(spec_cloud_provider, spec_region);
        // Cloud-family host suffix check: if URI.host ends with a known cloud
        // provider domain (AWS, GCP, Aliyun, Tencent, Huawei, Azure — all
        // endpoint variants including global/accelerate/dualstack/FIPS/VPC),
        // the URI is Milvus-form and URI.host is authoritative. Skip swap
        // regardless of whether DeriveEndpoint string-matches. This prevents
        // misclassifying `s3://s3.amazonaws.com/bucket/key` as AWS-form when
        // spec.region is a regional value.
        bool uri_host_is_cloud_endpoint = IsCloudEndpointHost(host);
        if (!uri_host_is_cloud_endpoint && !derived.empty() &&
            StripURIScheme(derived) != host) {
            milvus_storage::api::SetValue(
                properties,
                (extfs_prefix + "bucket_name").c_str(),
                host.c_str());
            milvus_storage::api::SetValue(properties,
                                          (extfs_prefix + "address").c_str(),
                                          derived.c_str());
        }
    }

    // Reconcile bare address with final use_ssl. Addresses that already
    // carry a scheme win. Azure is exempt: AzureFileSystemProducer does
    // `options.blob_storage_authority = ".blob." + config_.address`, so the
    // address must be a bare authority (e.g. "core.windows.net"); prepending
    // a scheme would break the concatenation.
    auto final_cp = milvus_storage::api::GetValue<std::string>(
        properties, (extfs_prefix + "cloud_provider").c_str());
    bool is_azure = final_cp.ok() && *final_cp == "azure";
    auto final_use_ssl = milvus_storage::api::GetValue<std::string>(
        properties, (extfs_prefix + "use_ssl").c_str());
    auto final_address = milvus_storage::api::GetValue<std::string>(
        properties, (extfs_prefix + "address").c_str());
    if (!is_azure && final_use_ssl.ok() && final_address.ok() &&
        final_address->find("://") == std::string::npos) {
        const char* scheme =
            (*final_use_ssl == "true") ? "https://" : "http://";
        milvus_storage::api::SetValue(properties,
                                      (extfs_prefix + "address").c_str(),
                                      (scheme + *final_address).c_str());
    }

    // Format-layer: emit per-format properties derived from spec.format.
    // Currently only Iceberg-table → iceberg.snapshot_id. Future formats
    // (Lance version, Iceberg branch, etc.) land here.
    if (external_spec.empty()) {
        return;
    }
    try {
        simdjson::ondemand::parser parser;
        simdjson::padded_string padded(external_spec);
        auto doc = parser.iterate(padded);
        std::string_view format_view;
        if (doc.find_field("format").get_string().get(format_view) ==
            simdjson::SUCCESS) {
            std::string format{format_view};
            if (format == "iceberg-table") {
                auto snapshot_field = doc.find_field("snapshot_id");
                int64_t snapshot_id = 0;
                auto int_err = snapshot_field.get_int64().get(snapshot_id);
                if (int_err == simdjson::INCORRECT_TYPE) {
                    std::string_view snapshot_view;
                    if (snapshot_field.get_string().get(snapshot_view) ==
                        simdjson::SUCCESS) {
                        try {
                            snapshot_id =
                                std::stoll(std::string(snapshot_view));
                            int_err = simdjson::SUCCESS;
                        } catch (const std::exception& e) {
                            LOG_WARN(
                                "iceberg snapshot_id parse failed "
                                "(collection_id={}): {}",
                                collection_id,
                                e.what());
                        }
                    }
                }
                if (int_err == simdjson::SUCCESS) {
                    milvus_storage::api::SetValue(
                        properties,
                        "iceberg.snapshot_id",
                        std::to_string(snapshot_id).c_str());
                }
            }
        }
    } catch (const simdjson::simdjson_error& e) {
        // Top-level parse failure already caught by the extfs block above;
        // this second pass only runs after the first succeeded.
        LOG_WARN("format-props parse failed (collection_id={}): {}",
                 collection_id,
                 e.what());
    }
}

void
InjectExternalSpecProperties(milvus_storage::api::Properties& properties,
                             int64_t collection_id,
                             const std::string& external_source,
                             const std::string& external_spec) {
    // Native C++ callers rely on the process policy initialized during
    // component startup.
    const auto iops_config =
        milvus::storage::LoonFFIPropertiesSingleton::GetInstance()
            .GetExternalIopsConfig();
    InjectExternalSpecProperties(
        properties, collection_id, external_source, external_spec, iops_config);
}

std::shared_ptr<milvus_storage::api::Properties>
MakeInternalLocalProperies(const char* c_path) {
    auto properties_map = std::make_shared<milvus_storage::api::Properties>();

    milvus_storage::api::SetValue(
        *properties_map, PROPERTY_FS_STORAGE_TYPE, "local");

    milvus_storage::api::SetValue(
        *properties_map, PROPERTY_FS_ROOT_PATH, c_path);

    return properties_map;
}

CStorageConfig
ToCStorageConfig(const milvus::storage::StorageConfig& config) {
    return CStorageConfig{config.address.c_str(),
                          config.bucket_name.c_str(),
                          config.access_key_id.c_str(),
                          config.access_key_value.c_str(),
                          config.root_path.c_str(),
                          config.storage_type.c_str(),
                          config.cloud_provider.c_str(),
                          config.iam_endpoint.c_str(),
                          config.log_level.c_str(),
                          config.region.c_str(),
                          config.useSSL,
                          config.sslCACert.c_str(),
                          config.useIAM,
                          config.useVirtualHost,
                          config.requestTimeoutMs,
                          config.gcp_credential_json.c_str(),
                          false,  // this field does not exist in StorageConfig
                          config.max_connections,
                          config.tls_min_version.c_str(),
                          config.use_crc32c_checksum};
}

std::shared_ptr<milvus_storage::api::Manifest>
GetLoonManifest(
    const std::string& path,
    const std::shared_ptr<milvus_storage::api::Properties>& properties) {
    try {
        // Parse the JSON string
        json j = json::parse(path);

        // Extract base_path & version
        std::string base_path = j.at("base_path").get<std::string>();
        int64_t version = j.at("ver").get<int64_t>();

        auto fs_result =
            milvus_storage::FilesystemCache::getInstance().get(*properties);
        if (!fs_result.ok()) {
            auto error = milvus_storage::ToSegcoreError(fs_result.status());
            ThrowInfo(error.get_error_code(),
                      "Failed to get filesystem: {}",
                      error.what());
        }
        auto fs = std::move(fs_result.ValueOrDie());

        auto transaction_result =
            milvus_storage::api::transaction::Transaction::Open(
                fs,
                base_path,
                version,
                milvus_storage::api::transaction::FailResolver,
                1);
        if (!transaction_result.ok()) {
            auto error =
                milvus_storage::ToSegcoreError(transaction_result.status());
            ThrowInfo(error.get_error_code(),
                      "Failed to open transaction: {}",
                      error.what());
        }
        auto transaction = std::move(transaction_result.ValueOrDie());
        auto manifest_result = transaction->GetManifest();
        if (!manifest_result.ok()) {
            auto error =
                milvus_storage::ToSegcoreError(manifest_result.status());
            ThrowInfo(error.get_error_code(),
                      "Failed to get manifest: {}",
                      error.what());
        }
        auto current_manifest = manifest_result.ValueOrDie();
        return current_manifest;
    } catch (const json::parse_error& e) {
        throw std::runtime_error(
            std::string("Failed to parse manifest JSON: ") + e.what());
    } catch (const json::out_of_range& e) {
        throw std::runtime_error(
            std::string("Missing required field in manifest: ") + e.what());
    } catch (const json::type_error& e) {
        throw std::runtime_error(
            std::string("Invalid field type in manifest: ") + e.what());
    }
}

// ==================== C-ABI: external spec injection ====================
//
// Bridges Go callers into the C++ InjectExternalSpecProperties pipeline so
// that URI parsing, endpoint derivation, AWS-form rewriting, allowlist
// enforcement, and format-property derivation (iceberg.snapshot_id etc.)
// all live in a single implementation driven by the raw external_spec JSON.

extern "C" LoonFFIResult
loon_properties_inject_external_spec(LoonProperties* properties,
                                     int64_t collection_id,
                                     const char* external_source,
                                     const char* external_spec,
                                     uint32_t iops_initial_rate,
                                     uint32_t iops_max_rate) {
    if (properties == nullptr) {
        RETURN_ERROR(LOON_INVALID_ARGS, "properties is null");
    }
    if (external_source == nullptr || external_source[0] == '\0') {
        RETURN_SUCCESS();  // no-op for internal (non-external) collections
    }
    if (iops_initial_rate == 0) {
        RETURN_ERROR(LOON_INVALID_ARGS,
                     "external IOPS initial rate must be greater than zero");
    }
    try {
        // Load existing flat LoonProperties into a typed Properties map.
        milvus_storage::api::Properties props;
        for (size_t i = 0; i < properties->count; ++i) {
            const auto& p = properties->properties[i];
            if (p.key != nullptr && p.value != nullptr) {
                props[p.key] = std::string(p.value);
            }
        }

        // Keep Go-provided values call-local. Publishing them to the singleton
        // would couple concurrent FFI and native reads through global state.
        const milvus::storage::ExternalIopsConfig iops_config{iops_initial_rate,
                                                              iops_max_rate};
        InjectExternalSpecProperties(props,
                                     collection_id,
                                     external_source,
                                     external_spec ? external_spec : "",
                                     iops_config);

        // Rebuild LoonProperties from the merged map. Free old entries first
        // so ownership stays with loon_properties_free.
        for (size_t i = 0; i < properties->count; ++i) {
            free(properties->properties[i].key);
            free(properties->properties[i].value);
        }
        free(properties->properties);
        properties->properties = nullptr;
        properties->count = 0;

        size_t n = props.size();
        if (n == 0) {
            RETURN_SUCCESS();
        }
        auto* arr =
            static_cast<LoonProperty*>(malloc(sizeof(LoonProperty) * n));
        if (arr == nullptr) {
            RETURN_ERROR(LOON_MEMORY_ERROR,
                         "failed to malloc LoonProperty array of size ",
                         n);
        }
        size_t i = 0;
        for (const auto& kv : props) {
            arr[i].key = strdup(kv.first.c_str());
            const auto* sval = std::get_if<std::string>(&kv.second);
            arr[i].value = strdup(sval ? sval->c_str() : "");
            ++i;
        }
        properties->properties = arr;
        properties->count = n;
        RETURN_SUCCESS();
    } catch (std::exception& e) {
        RETURN_EXCEPTION(e.what());
    }
}
