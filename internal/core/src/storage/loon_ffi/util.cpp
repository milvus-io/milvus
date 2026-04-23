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
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "arrow/result.h"
#include "arrow/status.h"
#include "simdjson.h"
#include "common/EasyAssert.h"
#include "log/Log.h"
#include "common/type_c.h"
#include "milvus-storage/filesystem/fs.h"
#include "milvus-storage/properties.h"
#include "milvus-storage/transaction/transaction.h"
#include "nlohmann/json_fwd.hpp"
#include "storage/Types.h"
#include "storage/loon_ffi/util.h"

using json = nlohmann::json;

std::shared_ptr<LoonProperties>
MakePropertiesFromStorageConfig(CStorageConfig c_storage_config) {
    // Prepare key-value pairs from CStorageConfig
    std::vector<const char*> keys;
    std::vector<const char*> values;

    // Lance's BuildEndpointUrl defaults to HTTPS when no scheme is present.
    // Prepend http:// when SSL is not enabled so that Lance sets allow_http=true.
    std::string fs_address;
    if (c_storage_config.address != nullptr) {
        fs_address = c_storage_config.address;
        if (!c_storage_config.useSSL &&
            fs_address.find("://") == std::string::npos) {
            fs_address = "http://" + fs_address;
        }
    }

    // Add non-null string fields
    if (c_storage_config.address != nullptr) {
        keys.emplace_back(PROPERTY_FS_ADDRESS);
        values.emplace_back(fs_address.c_str());
    }
    if (c_storage_config.bucket_name != nullptr) {
        keys.emplace_back(PROPERTY_FS_BUCKET_NAME);
        values.emplace_back(c_storage_config.bucket_name);
    }
    if (c_storage_config.access_key_id != nullptr) {
        keys.emplace_back(PROPERTY_FS_ACCESS_KEY_ID);
        values.emplace_back(c_storage_config.access_key_id);
    }
    if (c_storage_config.access_key_value != nullptr) {
        keys.emplace_back(PROPERTY_FS_ACCESS_KEY_VALUE);
        values.emplace_back(c_storage_config.access_key_value);
    }
    if (c_storage_config.root_path != nullptr) {
        keys.emplace_back(PROPERTY_FS_ROOT_PATH);
        values.emplace_back(c_storage_config.root_path);
    }
    if (c_storage_config.storage_type != nullptr) {
        keys.emplace_back(PROPERTY_FS_STORAGE_TYPE);
        values.emplace_back(c_storage_config.storage_type);
    }
    if (c_storage_config.cloud_provider != nullptr) {
        keys.emplace_back(PROPERTY_FS_CLOUD_PROVIDER);
        values.emplace_back(c_storage_config.cloud_provider);
    }
    if (c_storage_config.iam_endpoint != nullptr) {
        keys.emplace_back(PROPERTY_FS_IAM_ENDPOINT);
        values.emplace_back(c_storage_config.iam_endpoint);
    }
    if (c_storage_config.log_level != nullptr) {
        keys.emplace_back(PROPERTY_FS_LOG_LEVEL);
        values.emplace_back("Warn");
    }
    if (c_storage_config.region != nullptr) {
        keys.emplace_back(PROPERTY_FS_REGION);
        values.emplace_back(c_storage_config.region);
    }
    if (c_storage_config.sslCACert != nullptr) {
        keys.emplace_back(PROPERTY_FS_SSL_CA_CERT);
        values.emplace_back(c_storage_config.sslCACert);
    }
    if (c_storage_config.gcp_credential_json != nullptr) {
        keys.emplace_back(PROPERTY_FS_GCP_CREDENTIAL_JSON);
        values.emplace_back(c_storage_config.gcp_credential_json);
    }

    // Add boolean fields
    keys.emplace_back(PROPERTY_FS_USE_SSL);
    values.emplace_back(c_storage_config.useSSL ? "true" : "false");

    keys.emplace_back(PROPERTY_FS_USE_IAM);
    values.emplace_back(c_storage_config.useIAM ? "true" : "false");

    keys.emplace_back(PROPERTY_FS_USE_VIRTUAL_HOST);
    values.emplace_back(c_storage_config.useVirtualHost ? "true" : "false");

    keys.emplace_back(PROPERTY_FS_USE_CUSTOM_PART_UPLOAD);
    values.emplace_back(c_storage_config.use_custom_part_upload ? "true"
                                                                : "false");

    // Add integer field
    std::string timeout_str = std::to_string(c_storage_config.requestTimeoutMs);
    keys.emplace_back(PROPERTY_FS_REQUEST_TIMEOUT_MS);
    values.emplace_back(timeout_str.c_str());

    std::string max_connections_str =
        std::to_string(c_storage_config.max_connections);
    keys.emplace_back(PROPERTY_FS_MAX_CONNECTIONS);
    values.emplace_back(max_connections_str.c_str());

    if (c_storage_config.tls_min_version != nullptr) {
        std::string tls_ver(c_storage_config.tls_min_version);
        if (!tls_ver.empty() && tls_ver != "default") {
            keys.emplace_back(PROPERTY_FS_TLS_MIN_VERSION);
            values.emplace_back(c_storage_config.tls_min_version);
        }
    }
    // No extfs.default.* properties in FFI path. Per-collection extfs properties
    // (extfs.{collectionID}.*) are passed as extraKVs by Go-side BuildExtfsOverrides.

    keys.emplace_back(PROPERTY_FS_USE_CRC32C_CHECKSUM);
    values.emplace_back(c_storage_config.use_crc32c_checksum ? "true"
                                                             : "false");

    // Create Properties using FFI
    auto properties = std::make_shared<LoonProperties>();
    LoonFFIResult result = loon_properties_create(
        keys.data(), values.data(), keys.size(), properties.get());

    if (!loon_ffi_is_success(&result)) {
        auto message = loon_ffi_get_errmsg(&result);
        // Copy the error message before freeing the LoonFFIResult
        std::string error_msg = message ? message : "Unknown error";
        loon_ffi_free_result(&result);
        throw std::runtime_error(error_msg);
    }

    loon_ffi_free_result(&result);
    return properties;
}

std::shared_ptr<milvus_storage::api::Properties>
MakeInternalPropertiesFromStorageConfig(CStorageConfig c_storage_config) {
    auto properties_map = std::make_shared<milvus_storage::api::Properties>();

    // Lance's BuildEndpointUrl defaults to HTTPS when no scheme is present.
    // Prepend http:// when SSL is not enabled so that Lance sets allow_http=true.
    std::string fs_address_internal;
    if (c_storage_config.address != nullptr) {
        fs_address_internal = c_storage_config.address;
        if (!c_storage_config.useSSL &&
            fs_address_internal.find("://") == std::string::npos) {
            fs_address_internal = "http://" + fs_address_internal;
        }
    }

    // Add non-null string fields
    if (c_storage_config.address != nullptr) {
        milvus_storage::api::SetValue(
            *properties_map, PROPERTY_FS_ADDRESS, fs_address_internal.c_str());
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
    milvus_storage::api::SetValue(
        *properties_map,
        PROPERTY_FS_MAX_CONNECTIONS,
        std::to_string(c_storage_config.max_connections).c_str());

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
    "iam_endpoint",
    "storage_type",
    "ssl_ca_cert",
    "access_key_id",
    "access_key_value",
};

void
InjectExtfsProperties(milvus_storage::api::Properties& properties,
                      int64_t collection_id,
                      const std::string& external_source,
                      const std::string& external_spec) {
    std::string extfs_prefix = "extfs." + std::to_string(collection_id) + ".";

    // Layer 1: Copy fs.* baseline to extfs.{collID}.*
    static const std::vector<std::string> prop_names = {
        "storage_type",
        "bucket_name",
        "address",
        "root_path",
        "access_key_id",
        "access_key_value",
        "cloud_provider",
        "iam_endpoint",
        "region",
        "ssl_ca_cert",
        "gcp_credential_json",
        "use_ssl",
        "use_iam",
        "use_virtual_host",
    };
    for (const auto& prop : prop_names) {
        auto it = properties.find("fs." + prop);
        if (it != properties.end()) {
            // Copy value before inserting: operator[] may rehash the map,
            // invalidating it->second reference.
            auto val = it->second;
            if (auto* b = std::get_if<bool>(&val)) {
                properties[extfs_prefix + prop] =
                    std::string(*b ? "true" : "false");
            } else {
                properties[extfs_prefix + prop] = std::move(val);
            }
        }
    }

    // Layer 2: Override bucket/address from URI (cross-bucket only).
    //
    // The caller already validated external_source via ValidateExternalSource
    // on the Go side, so reaching here with a malformed URI means either
    // etcd corruption or a bypass of the validation path. We treat both as
    // hard errors: returning silently with empty bucket/host would leave the
    // caller's properties pointing at the wrong bucket (the "fs.*" baseline
    // copied in Layer 1) and produce a confusing S3 403/404 deep inside the
    // Reader. AssertInfo here keeps the failure adjacent to the bad input.
    auto scheme_end = external_source.find("://");
    if (scheme_end != std::string::npos) {
        auto rest = external_source.substr(scheme_end + 3);
        auto slash_pos = rest.find('/');
        std::string host =
            (slash_pos != std::string::npos) ? rest.substr(0, slash_pos) : "";
        std::string path_part = (slash_pos != std::string::npos)
                                    ? rest.substr(slash_pos + 1)
                                    : rest;
        auto bucket_end = path_part.find('/');
        std::string bucket = (bucket_end != std::string::npos)
                                 ? path_part.substr(0, bucket_end)
                                 : path_part;

        // Empty host is valid for same-endpoint cross-bucket URIs like
        // s3:///external-bucket/path — Go ValidateExternalSource allows this
        // and BuildExtfsOverrides skips the address override in this case.
        // Only hard-reject a missing bucket, which is always a logic error.
        AssertInfo(!bucket.empty(),
                   "external_source for collection {} has empty bucket: {}",
                   collection_id,
                   external_source);

        milvus_storage::api::SetValue(
            properties, (extfs_prefix + "bucket_name").c_str(), bucket.c_str());

        // Only override address when host is non-empty (mirrors Go behaviour).
        // For s3:///bucket/path (empty host), we keep the baseline address from
        // Layer 1 so the request is sent to Milvus's own storage endpoint.
        if (!host.empty()) {
            std::string address = host;
            if (address.find("://") == std::string::npos) {
                address = "http://" + address;
            }
            milvus_storage::api::SetValue(properties,
                                          (extfs_prefix + "address").c_str(),
                                          address.c_str());
        }
    }

    // Layer 3: Apply extfs overrides from external_spec JSON
    //
    // Security: gate every key through kAllowedExtfsSpecKeys. Keys outside
    // the allowlist are dropped with a warning rather than written into
    // milvus-storage Properties. This is defense-in-depth: the Go-side
    // ParseExternalSpec already enforces the same allowlist at every known
    // entry point (proxy, rootcoord, datanode task); this check exists so a
    // future caller that forgets to run that validation cannot turn
    // external_spec into an arbitrary property-injection point.
    if (!external_spec.empty()) {
        try {
            simdjson::ondemand::parser parser;
            simdjson::padded_string padded(external_spec);
            auto doc = parser.iterate(padded);
            auto extfs_obj = doc.find_field("extfs");
            if (extfs_obj.error() == simdjson::SUCCESS) {
                for (auto field : extfs_obj.get_object()) {
                    // Per-field try/catch so a single malformed value (e.g.
                    // a bool where a string was expected) skips that field
                    // instead of aborting iteration and silently dropping
                    // every subsequent allowlisted key. This is defense in
                    // depth: Go-side ParseExternalSpec already rejects any
                    // non-string extfs value at json.Unmarshal time, so in
                    // practice this branch is unreachable — but we prefer a
                    // per-field drop with a loud warning over a silent
                    // whole-iteration abort if that guarantee ever changes.
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
            // Top-level JSON parse failure means the spec is structurally
            // broken; the Go side validates this at every entry point so
            // reaching here implies etcd corruption or a bypass. Promote to
            // ERROR (not WARN) and fail fast — silently using the Layer 1+2
            // baseline would authenticate against a partially-overridden
            // config that the user never asked for.
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
        AssertInfo(fs_result.ok(),
                   "Failed to get filesystem: {}",
                   fs_result.status().ToString());
        auto fs = std::move(fs_result.ValueOrDie());

        auto transaction_result =
            milvus_storage::api::transaction::Transaction::Open(
                fs,
                base_path,
                version,
                milvus_storage::api::transaction::FailResolver,
                1);
        AssertInfo(transaction_result.ok(),
                   "Failed to open transaction: {}",
                   transaction_result.status().ToString());
        auto transaction = std::move(transaction_result.ValueOrDie());
        auto manifest_result = transaction->GetManifest();
        AssertInfo(manifest_result.ok(),
                   "Failed to get manifest: {}",
                   manifest_result.status().ToString());
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