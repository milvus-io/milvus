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

#include "exec/expression/ExprCache.h"

namespace milvus {
namespace exec {

std::atomic<bool> ExprResCacheManager::enabled_{false};

ExprResCacheManager&
ExprResCacheManager::Instance() {
    static ExprResCacheManager instance;
    return instance;
}

void
ExprResCacheManager::SetEnabled(bool enabled) {
    enabled_.store(enabled);
}

bool
ExprResCacheManager::IsEnabled() {
    return enabled_.load();
}

void
ExprResCacheManager::SetCapacityBytes(size_t capacity_bytes) {
    capacity_bytes_.store(capacity_bytes);
    EnsureCapacity();
}

size_t
ExprResCacheManager::GetCapacityBytes() const {
    return capacity_bytes_.load();
}

size_t
ExprResCacheManager::GetCurrentBytes() const {
    return current_bytes_.load();
}

size_t
ExprResCacheManager::GetEntryCount() const {
    return concurrent_map_.size();
}

bool
ExprResCacheManager::Get(const Key& key, Value& out_value) {
    if (!IsEnabled()) {
        return false;
    }

    auto it = concurrent_map_.find(key);
    if (it == concurrent_map_.end()) {
        return false;
    }

    out_value = it->second.value;
    {
        std::lock_guard<std::mutex> lru_lock(lru_mutex_);
        lru_list_.splice(lru_list_.begin(), lru_list_, it->second.lru_it);
    }

    LOG_DEBUG("get expr res cache, segment_id: {}, key: {}",
              key.segment_id,
              key.signature);
    return true;
}

void
ExprResCacheManager::Put(const Key& key, const Value& value) {
    if (!IsEnabled()) {
        return;
    }

    size_t estimated_bytes = EstimateBytes(value);
    auto stored_value = value;
    stored_value.bytes = estimated_bytes;

    auto it = concurrent_map_.find(key);
    if (it != concurrent_map_.end()) {
        auto old_bytes = it->second.value.bytes;
        it->second.value = stored_value;

        {
            std::lock_guard<std::mutex> lru_lock(lru_mutex_);
            lru_list_.splice(lru_list_.begin(), lru_list_, it->second.lru_it);
            current_bytes_.fetch_add(estimated_bytes - old_bytes);
        }
    } else {
        ListIt list_it;
        {
            std::lock_guard<std::mutex> lru_lock(lru_mutex_);
            lru_list_.push_front(key);
            list_it = lru_list_.begin();
            current_bytes_.fetch_add(estimated_bytes);
        }

        Entry entry(stored_value, list_it);
        concurrent_map_.emplace(key, std::move(entry));
    }

    if (current_bytes_.load() > capacity_bytes_.load()) {
        EnsureCapacity();
    }

    LOG_DEBUG("put expr res cache, segment_id: {}, key: {}",
              key.segment_id,
              key.signature);
}

void
ExprResCacheManager::Clear() {
    std::lock_guard<std::mutex> lru_lock(lru_mutex_);

    concurrent_map_.clear();
    lru_list_.clear();
    current_bytes_.store(0);
}

size_t
ExprResCacheManager::EstimateBytes(const Value& v) const {
    size_t bytes = sizeof(Value);
    if (v.result) {
        bytes += (v.result->size() + 7) / 8;
    }
    if (v.valid_result) {
        bytes += (v.valid_result->size() + 7) / 8;
    }
    return bytes;
}

void
ExprResCacheManager::EnsureCapacity() {
    std::lock_guard<std::mutex> lru_lock(lru_mutex_);
    while (current_bytes_.load() > capacity_bytes_.load() &&
           !lru_list_.empty()) {
        const auto& back_key = lru_list_.back();
        auto it = concurrent_map_.find(back_key);
        if (it != concurrent_map_.end()) {
            current_bytes_.fetch_sub(it->second.value.bytes);
            concurrent_map_.unsafe_erase(it);
        }
        lru_list_.pop_back();
    }
}

size_t
ExprResCacheManager::EraseSegment(int64_t segment_id) {
    size_t erased = 0;
    std::lock_guard<std::mutex> lru_lock(lru_mutex_);
    for (auto it = lru_list_.begin(); it != lru_list_.end();) {
        if (it->segment_id == segment_id) {
            auto map_it = concurrent_map_.find(*it);
            if (map_it != concurrent_map_.end()) {
                current_bytes_.fetch_sub(map_it->second.value.bytes);
                concurrent_map_.unsafe_erase(map_it);
            }
            it = lru_list_.erase(it);
            ++erased;
        } else {
            ++it;
        }
    }

    LOG_INFO("erase segment cache, segment_id: {}, erased: {} entries",
             segment_id,
             erased);
    return erased;
}

void
ExprResCacheManager::Init(size_t capacity_bytes, bool enabled) {
    SetEnabled(enabled);
    Instance().SetCapacityBytes(capacity_bytes);
}

}  // namespace exec
}  // namespace milvus