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
#include "common/common_type_c.h"
#include "common/type_c.h"
#include "milvus-storage/properties.h"
#include "storage/loon_ffi/util.h"

std::shared_ptr<Properties>
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
    auto properties = std::make_shared<Properties>();
    FFIResult result = properties_create(
        keys.data(), values.data(), keys.size(), properties.get());

    if (!IsSuccess(&result)) {
        auto message = GetErrorMessage(&result);
        // Copy the error message before freeing the FFIResult
        std::string error_msg = message ? message : "Unknown error";
        FreeFFIResult(&result);
        throw std::runtime_error(error_msg);
    }

    FreeFFIResult(&result);
    return properties;
}