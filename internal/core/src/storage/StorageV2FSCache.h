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

#include <future>
#include <shared_mutex>

#include "milvus-storage/filesystem/fs.h"
#include <tbb/concurrent_unordered_map.h>

namespace milvus::storage {

// cache for storage v2 filesystem using storage config as key.
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

        bool
        operator==(const Key& other) const {
            return address == other.address &&
                   bucket_name == other.bucket_name &&
                   access_key_id == other.access_key_id &&
                   access_key_value == other.access_key_value &&
                   root_path == other.root_path &&
                   storage_type == other.storage_type &&
                   cloud_provider == other.cloud_provider &&
                   iam_endpoint == other.iam_endpoint &&
                   log_level == other.log_level && region == other.region &&
                   useSSL == other.useSSL && sslCACert == other.sslCACert &&
                   useIAM == other.useIAM &&
                   useVirtualHost == other.useVirtualHost &&
                   requestTimeoutMs == other.requestTimeoutMs &&
                   gcp_native_without_auth == other.gcp_native_without_auth &&
                   gcp_credential_json == other.gcp_credential_json &&
                   use_custom_part_upload == other.use_custom_part_upload &&
                   max_connections == other.max_connections;
        }
    };

    struct KeyHasher {
        size_t
        operator()(const Key& k) const noexcept {
            size_t hash = 0;
            hash_combine(hash, k.address);
            hash_combine(hash, k.bucket_name);
            hash_combine(hash, k.access_key_id);
            hash_combine(hash, k.access_key_value);
            hash_combine(hash, k.root_path);
            hash_combine(hash, k.storage_type);
            hash_combine(hash, k.cloud_provider);
            hash_combine(hash, k.iam_endpoint);
            hash_combine(hash, k.log_level);
            hash_combine(hash, k.region);
            hash_combine(hash, k.useSSL);
            hash_combine(hash, k.sslCACert);
            hash_combine(hash, k.useIAM);
            hash_combine(hash, k.useVirtualHost);
            hash_combine(hash, k.requestTimeoutMs);
            hash_combine(hash, k.gcp_native_without_auth);
            hash_combine(hash, k.gcp_credential_json);
            hash_combine(hash, k.use_custom_part_upload);
            hash_combine(hash, k.max_connections);
            return hash;
        }

     private:
        template <typename T>
        void
        hash_combine(size_t& seed, const T& v) const {
            std::hash<T> hasher;
            seed ^= hasher(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        }
    };

    // singleflight item using promise and future
    using Value =
        std::pair<std::promise<milvus_storage::ArrowFileSystemPtr>,
                  std::shared_future<milvus_storage::ArrowFileSystemPtr>>;

 public:
    // returns singleton of StorageV2FSCache
    static StorageV2FSCache&
    Instance();

    milvus_storage::ArrowFileSystemPtr
    Get(const Key& key);

    virtual ~StorageV2FSCache() = default;

 private:
    std::shared_mutex mutex_;
    tbb::concurrent_unordered_map<Key, Value, KeyHasher> concurrent_map_;
};
}  // namespace milvus::storage
