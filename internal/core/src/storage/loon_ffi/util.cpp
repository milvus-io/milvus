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

#include <log/Log.h>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>
#include <nlohmann/json.hpp>
#include "common/common_type_c.h"
#include "common/type_c.h"
#include "milvus-storage/properties.h"
#include "milvus-storage/manifest.h"
#include "milvus-storage/transaction/transaction.h"
#include "storage/loon_ffi/util.h"

using json = nlohmann::json;

std::shared_ptr<LoonProperties>
MakePropertiesFromStorageConfig(CStorageConfig c_storage_config) {
    // Prepare key-value pairs from CStorageConfig
    std::vector<const char*> keys;
    std::vector<const char*> values;

    // Add non-null string fields
    if (c_storage_config.address != nullptr) {
        keys.emplace_back(PROPERTY_FS_ADDRESS);
        values.emplace_back(c_storage_config.address);
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
    milvus_storage::api::SetValue(
        *properties_map,
        PROPERTY_FS_MAX_CONNECTIONS,
        std::to_string(c_storage_config.max_connections).c_str());

    return properties_map;
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
                          config.max_connections};
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