// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

namespace milvus {
namespace cache {

template <typename ItemObj>
CacheMgr<ItemObj>::CacheMgr() {
}

template <typename ItemObj>
CacheMgr<ItemObj>::~CacheMgr() {
}

template <typename ItemObj>
uint64_t
CacheMgr<ItemObj>::ItemCount() const {
    if (cache_ == nullptr) {
        LOG_SERVER_ERROR_ << "Cache doesn't exist";
        return 0;
    }
    return (uint64_t)(cache_->size());
}

template <typename ItemObj>
bool
CacheMgr<ItemObj>::ItemExists(const std::string& key) {
    if (cache_ == nullptr) {
        LOG_SERVER_ERROR_ << "Cache doesn't exist";
        return false;
    }
    return cache_->exists(key);
}

template <typename ItemObj>
ItemObj
CacheMgr<ItemObj>::GetItem(const std::string& key) {
    if (cache_ == nullptr) {
        LOG_SERVER_ERROR_ << "Cache doesn't exist";
        return nullptr;
    }
    return cache_->get(key);
}

template <typename ItemObj>
void
CacheMgr<ItemObj>::InsertItem(const std::string& key, const ItemObj& data) {
    if (cache_ == nullptr) {
        LOG_SERVER_ERROR_ << "Cache doesn't exist";
        return;
    }
    cache_->insert(key, data);
}

template <typename ItemObj>
void
CacheMgr<ItemObj>::EraseItem(const std::string& key) {
    if (cache_ == nullptr) {
        LOG_SERVER_ERROR_ << "Cache doesn't exist";
        return;
    }
    cache_->erase(key);
}

template <typename ItemObj>
bool
CacheMgr<ItemObj>::Reserve(const int64_t size) {
    if (cache_ == nullptr) {
        LOG_SERVER_ERROR_ << "Cache doesn't exist";
        return false;
    }
    return cache_->reserve(size);
}

template <typename ItemObj>
void
CacheMgr<ItemObj>::PrintInfo() {
    if (cache_ == nullptr) {
        LOG_SERVER_ERROR_ << "Cache doesn't exist";
        return;
    }
    cache_->print();
}

template <typename ItemObj>
void
CacheMgr<ItemObj>::ClearCache() {
    if (cache_ == nullptr) {
        LOG_SERVER_ERROR_ << "Cache doesn't exist";
        return;
    }
    cache_->clear();
}

template <typename ItemObj>
int64_t
CacheMgr<ItemObj>::CacheUsage() const {
    if (cache_ == nullptr) {
        LOG_SERVER_ERROR_ << "Cache doesn't exist";
        return 0;
    }
    return cache_->usage();
}

template <typename ItemObj>
int64_t
CacheMgr<ItemObj>::CacheCapacity() const {
    if (cache_ == nullptr) {
        LOG_SERVER_ERROR_ << "Cache doesn't exist";
        return 0;
    }
    return cache_->capacity();
}

template <typename ItemObj>
void
CacheMgr<ItemObj>::SetCapacity(int64_t capacity) {
    if (cache_ == nullptr) {
        LOG_SERVER_ERROR_ << "Cache doesn't exist";
        return;
    }
    cache_->set_capacity(capacity);
}

}  // namespace cache
}  // namespace milvus
