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
constexpr double WARNING_THRESHOLD_PERCENT = 0.9;
constexpr double BIG_ITEM_THRESHOLD_PERCENT = 0.1;

template <typename ItemObj>
Cache<ItemObj>::Cache(int64_t capacity, int64_t cache_max_count, const std::string& header)
    : usage_(0),
      capacity_(capacity),
      header_(header),
      freemem_percent_(DEFAULT_THRESHOLD_PERCENT),
      lru_(cache_max_count) {
}

template <typename ItemObj>
void
Cache<ItemObj>::set_capacity(int64_t capacity) {
    if (capacity > 0) {
        capacity_ = capacity;
        free_memory();
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
    if (item == nullptr) {
        return;
    }

    size_t item_size = item->Size();
    // calculate usage
    {
        std::lock_guard<std::mutex> lock(mutex_);

        // if key already exist, subtract old item size
        if (lru_.exists(key)) {
            const ItemObj& old_item = lru_.get(key);
            usage_ -= old_item->Size();
        }

        // plus new item size
        usage_ += item_size;
    }

    // if usage exceed capacity, free some items
    if (usage_ > capacity_ ||
        (item_size > (int64_t)(capacity_ * BIG_ITEM_THRESHOLD_PERCENT) &&
         usage_ > (int64_t)(capacity_ * WARNING_THRESHOLD_PERCENT))) {
        SERVER_LOG_DEBUG << header_ << " Current usage " << (usage_ >> 20) << "MB is too high for capacity "
                         << (capacity_ >> 20) << "MB, start free memory";
        free_memory();
    }

    // insert new item
    {
        std::lock_guard<std::mutex> lock(mutex_);

        lru_.put(key, item);
        SERVER_LOG_DEBUG << header_ << " Insert " << key << " size: " << (item_size >> 20) << "MB into cache";
        SERVER_LOG_DEBUG << header_ << " Count: " << lru_.size() << ", Usage: " << (usage_ >> 20) << "MB, Capacity: "
                         << (capacity_ >> 20) << "MB";
    }
}

template <typename ItemObj>
void
Cache<ItemObj>::erase(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!lru_.exists(key)) {
        return;
    }

    const ItemObj& item = lru_.get(key);
    size_t item_size = item->Size();

    lru_.erase(key);

    usage_ -= item_size;
    SERVER_LOG_DEBUG << header_ << " Erase " << key << " size: " << (item_size >> 20) << "MB from cache";
    SERVER_LOG_DEBUG << header_ << " Count: " << lru_.size() << ", Usage: " << (usage_ >> 20) << "MB, Capacity: "
                     << (capacity_ >> 20) << "MB";
}

template <typename ItemObj>
void
Cache<ItemObj>::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    lru_.clear();
    usage_ = 0;
    SERVER_LOG_DEBUG << header_ << " Clear cache !";
}

/* free memory space when CACHE occupation exceed its capacity */
template <typename ItemObj>
void
Cache<ItemObj>::free_memory() {
    // if (usage_ <= capacity_)
    //     return;

    int64_t threshhold = capacity_ * freemem_percent_;
    int64_t delta_size = usage_ - threshhold;
    if (delta_size <= 0) {
        delta_size = 1;  // ensure at least one item erased
    }

    std::set<std::string> key_array;
    int64_t released_size = 0;

    {
        std::lock_guard<std::mutex> lock(mutex_);

        auto it = lru_.rbegin();
        while (it != lru_.rend() && released_size < delta_size) {
            auto& key = it->first;
            auto& obj_ptr = it->second;

            key_array.emplace(key);
            released_size += obj_ptr->Size();
            ++it;
        }
    }

    SERVER_LOG_DEBUG << header_ << " To be released memory size: " << (released_size >> 20) << "MB";

    for (auto& key : key_array) {
        erase(key);
    }
}

template <typename ItemObj>
void
Cache<ItemObj>::print() {
    size_t cache_count = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        cache_count = lru_.size();
#if 0
        for (auto it = lru_.begin(); it != lru_.end(); ++it) {
            SERVER_LOG_DEBUG << it->first;
        }
#endif
    }

    SERVER_LOG_DEBUG << header_ << " [item count]: " << cache_count << ", [usage] " << (usage_ >> 20)
                     << "MB, [capacity] " << (capacity_ >> 20) << "MB";
}

}  // namespace cache
}  // namespace milvus
