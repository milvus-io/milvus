// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include "storage/StorageV2FSCache.h"
#include <future>
#include <mutex>
#include <shared_mutex>
#include "milvus-storage/filesystem/fs.h"
#include "milvus-storage/properties.h"
#include "log/Log.h"

namespace milvus::storage {

StorageV2FSCache&
StorageV2FSCache::Instance() {
    static StorageV2FSCache instance;
    return instance;
}

milvus_storage::ArrowFileSystemPtr
StorageV2FSCache::Get(const Key& key) {
    // Convert Key to api::Properties and delegate to FilesystemCache
    // This ensures all filesystems are managed by a single cache with metrics support
    milvus_storage::api::Properties props;
    props[PROPERTY_FS_ADDRESS] = key.address;
    props[PROPERTY_FS_BUCKET_NAME] = key.bucket_name;
    props[PROPERTY_FS_ACCESS_KEY_ID] = key.access_key_id;
    props[PROPERTY_FS_ACCESS_KEY_VALUE] = key.access_key_value;
    props[PROPERTY_FS_ROOT_PATH] = key.root_path;
    props[PROPERTY_FS_STORAGE_TYPE] = key.storage_type;
    props[PROPERTY_FS_CLOUD_PROVIDER] = key.cloud_provider;
    props[PROPERTY_FS_IAM_ENDPOINT] = key.iam_endpoint;
    props[PROPERTY_FS_LOG_LEVEL] = key.log_level;
    props[PROPERTY_FS_REGION] = key.region;
    props[PROPERTY_FS_USE_SSL] = key.useSSL;
    props[PROPERTY_FS_SSL_CA_CERT] = key.sslCACert;
    props[PROPERTY_FS_USE_IAM] = key.useIAM;
    props[PROPERTY_FS_USE_VIRTUAL_HOST] = key.useVirtualHost;
    props[PROPERTY_FS_REQUEST_TIMEOUT_MS] = key.requestTimeoutMs;
    props[PROPERTY_FS_GCP_NATIVE_WITHOUT_AUTH] = key.gcp_native_without_auth;
    props[PROPERTY_FS_GCP_CREDENTIAL_JSON] = key.gcp_credential_json;
    props[PROPERTY_FS_USE_CUSTOM_PART_UPLOAD] = key.use_custom_part_upload;
    props[PROPERTY_FS_MAX_CONNECTIONS] = key.max_connections;

    LOG_INFO(
        "StorageV2FSCache::Get: address={}, bucket={}, root_path={}, "
        "storage_type={}",
        key.address,
        key.bucket_name,
        key.root_path,
        key.storage_type);

    auto result = milvus_storage::FilesystemCache::getInstance().get(props, "");

    if (!result.ok()) {
        LOG_WARN("create arrow file system failed, error: {}",
                 result.status().ToString());
        return nullptr;
    }

    LOG_INFO("StorageV2FSCache::Get: got filesystem successfully");
    return result.ValueOrDie();
}

}  // namespace milvus::storage