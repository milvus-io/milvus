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

namespace milvus::storage {

StorageV2FSCache&
StorageV2FSCache::Instance() {
    static StorageV2FSCache instance;
    return instance;
}

milvus_storage::ArrowFileSystemPtr
StorageV2FSCache::Get(const Key& key) {
    auto it = concurrent_map_.find(key);
    if (it != concurrent_map_.end()) {
        return it->second.second.get();
    }

    std::promise<milvus_storage::ArrowFileSystemPtr> p;
    std::shared_future<milvus_storage::ArrowFileSystemPtr> f = p.get_future();

    auto iter = concurrent_map_.emplace(key, Value(std::move(p), f));
    if (!iter.second) {
        return iter.first->second.second.get();
    }

    // singleflight create fs for one key
    milvus_storage::ArrowFileSystemConfig conf;
    conf.address = std::string(key.address);
    conf.bucket_name = std::string(key.bucket_name);
    conf.access_key_id = std::string(key.access_key_id);
    conf.access_key_value = std::string(key.access_key_value);
    conf.root_path = std::string(key.root_path);
    conf.storage_type = std::string(key.storage_type);
    conf.cloud_provider = std::string(key.cloud_provider);
    conf.iam_endpoint = std::string(key.iam_endpoint);
    conf.log_level = std::string(key.log_level);
    conf.region = std::string(key.region);
    conf.useSSL = key.useSSL;
    conf.sslCACert = std::string(key.sslCACert);
    conf.useIAM = key.useIAM;
    conf.useVirtualHost = key.useVirtualHost;
    conf.requestTimeoutMs = key.requestTimeoutMs;
    conf.gcp_credential_json = std::string(key.gcp_credential_json);
    conf.use_custom_part_upload = key.use_custom_part_upload;
    auto result = milvus_storage::CreateArrowFileSystem(conf);

    if (!result.ok()) {
        iter.first->second.first.set_value(nullptr);
        std::unique_lock lck(mutex_);
        concurrent_map_.unsafe_erase(iter.first);
        return nullptr;
    }
    auto fs = result.value();
    iter.first->second.first.set_value(fs);
    return fs;
}

}  // namespace milvus::storage