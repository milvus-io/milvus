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

#pragma once

#include "milvus-storage/filesystem/fs.h"

namespace milvus::storage {

// Facade for storage v2 filesystem using storage config as key.
// This class delegates to milvus_storage::FilesystemCache internally,
// providing a unified filesystem cache with metrics support.
class StorageV2FSCache {
 public:
    struct Key {
        std::string address;
        std::string bucket_name;
        std::string access_key_id;
        std::string access_key_value;
        std::string root_path;
        std::string storage_type;
        std::string cloud_provider;
        std::string iam_endpoint;
        std::string log_level;
        std::string region;
        bool useSSL = false;
        std::string sslCACert;
        bool useIAM = false;
        bool useVirtualHost = false;
        int64_t requestTimeoutMs = 3000;
        bool gcp_native_without_auth = false;
        std::string gcp_credential_json = "";
        bool use_custom_part_upload = true;
        uint32_t max_connections = 100;
    };

 public:
    // returns singleton of StorageV2FSCache
    static StorageV2FSCache&
    Instance();

    // Get filesystem from the unified FilesystemCache.
    // Converts Key to api::Properties and delegates to FilesystemCache::get().
    milvus_storage::ArrowFileSystemPtr
    Get(const Key& key);

    virtual ~StorageV2FSCache() = default;
};
}  // namespace milvus::storage
