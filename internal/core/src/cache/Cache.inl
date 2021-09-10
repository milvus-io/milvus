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

constexpr double DEFAULT_THRESHOLD_PERCENT = 0.7;

template <typename ItemObj>
Cache<ItemObj>::Cache(int64_t capacity, int64_t cache_max_count, const std::string& header)
    : header_(header),
      usage_(0),
      capacity_(capacity),
      freemem_percent_(DEFAULT_THRESHOLD_PERCENT),
      lru_(cache_max_count) {
}

template <typename ItemObj>
void
Cache<ItemObj>::set_capacity(int64_t capacity) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (capacity > 0) {
        capacity_ = capacity;
        free_memory_internal(capacity);
    }
}

template <typename ItemObj>
size_t
Cache<ItemObj>::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return lru_.size();
}

template <typename ItemObj>
bool
Cache<ItemObj>::exists(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    return lru_.exists(key);
}

template <typename ItemObj>
ItemObj
Cache<ItemObj>::get(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!lru_.exists(key)) {
        return nullptr;
    }
    return lru_.get(key);
}

template <typename ItemObj>
void
Cache<ItemObj>::insert(const std::string& key, const ItemObj& item) {
    std::lock_guard<std::mutex> lock(mutex_);
    insert_internal(key, item);
}

template <typename ItemObj>
void
Cache<ItemObj>::erase(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    erase_internal(key);
}

template <typename ItemObj>
bool
Cache<ItemObj>::reserve(const int64_t item_size) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (item_size > capacity_) {
        LOG_SERVER_ERROR_ << header_ << " item size " << (item_size >> 20) << "MB too big to insert into cache capacity"
                         << (capacity_ >> 20) << "MB";
        return false;
    }
    if (item_size > capacity_ - usage_) {
        free_memory_internal(capacity_ - item_size);
    }
    return true;
}

template <typename ItemObj>
void
Cache<ItemObj>::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    lru_.clear();
    usage_ = 0;
    LOG_SERVER_DEBUG_ << header_ << " Clear cache !";
}


template <typename ItemObj>
void
Cache<ItemObj>::print() {
    std::lock_guard<std::mutex> lock(mutex_);
    size_t cache_count = lru_.size();
    // for (auto it = lru_.begin(); it != lru_.end(); ++it) {
    //     LOG_SERVER_DEBUG_ << it->first;
    // }
    LOG_SERVER_DEBUG_ << header_ << " [item count]: " << cache_count << ", [usage] " << (usage_ >> 20)
                     << "MB, [capacity] " << (capacity_ >> 20) << "MB";
}

template <typename ItemObj>
void
Cache<ItemObj>::insert_internal(const std::string& key, const ItemObj& item) {
    if (item == nullptr) {
        return;
    }

    size_t item_size = item->Size();

    // if key already exist, subtract old item size
    if (lru_.exists(key)) {
        const ItemObj& old_item = lru_.get(key);
        usage_ -= old_item->Size();
    }

    // plus new item size
    usage_ += item_size;

    // if usage exceed capacity, free some items
    if (usage_ > capacity_) {
        LOG_SERVER_DEBUG_ << header_ << " Current usage " << (usage_ >> 20) << "MB is too high for capacity "
                         << (capacity_ >> 20) << "MB, start free memory";
        free_memory_internal(capacity_);
    }

    // insert new item
    lru_.put(key, item);
    LOG_SERVER_DEBUG_ << header_ << " Insert " << key << " size: " << (item_size >> 20) << "MB into cache";
    LOG_SERVER_DEBUG_ << header_ << " Count: " << lru_.size() << ", Usage: " << (usage_ >> 20) << "MB, Capacity: "
                     << (capacity_ >> 20) << "MB";
}

template <typename ItemObj>
void
Cache<ItemObj>::erase_internal(const std::string& key) {
    if (!lru_.exists(key)) {
        return;
    }

    const ItemObj& item = lru_.get(key);
    size_t item_size = item->Size();

    lru_.erase(key);

    usage_ -= item_size;
    LOG_SERVER_DEBUG_ << header_ << " Erase " << key << " size: " << (item_size >> 20) << "MB from cache";
    LOG_SERVER_DEBUG_ << header_ << " Count: " << lru_.size() << ", Usage: " << (usage_ >> 20) << "MB, Capacity: "
                     << (capacity_ >> 20) << "MB";
}

template <typename ItemObj>
void
Cache<ItemObj>::free_memory_internal(const int64_t target_size) {
    int64_t threshold = std::min((int64_t)(capacity_ * freemem_percent_), target_size);
    int64_t delta_size = usage_ - threshold;
    if (delta_size <= 0) {
        delta_size = 1;  // ensure at least one item erased
    }

    std::set<std::string> key_array;
    int64_t released_size = 0;

    auto it = lru_.rbegin();
    while (it != lru_.rend() && released_size < delta_size) {
        auto& key = it->first;
        auto& obj_ptr = it->second;

        key_array.emplace(key);
        released_size += obj_ptr->Size();
        ++it;
    }

    LOG_SERVER_DEBUG_ << header_ << " To be released memory size: " << (released_size >> 20) << "MB";

    for (auto& key : key_array) {
        erase_internal(key);
    }
}

}  // namespace cache
}  // namespace milvus
