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

std::atomic<bool> ExprResCacheManager::enabled_{true};

ExprResCacheManager&
ExprResCacheManager::Instance() {
    static ExprResCacheManager instance;
    return instance;
}

void
ExprResCacheManager::SetEnabled(bool enabled) {
    enabled_.store(enabled, std::memory_order_relaxed);
}

bool
ExprResCacheManager::IsEnabled() {
    return enabled_.load(std::memory_order_relaxed);
}

void
ExprResCacheManager::SetCapacityBytes(size_t capacity_bytes) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    capacity_bytes_ = capacity_bytes;
    EnsureCapacity();
}

size_t
ExprResCacheManager::GetCapacityBytes() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return capacity_bytes_;
}

size_t
ExprResCacheManager::GetCurrentBytes() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return current_bytes_;
}

size_t
ExprResCacheManager::GetEntryCount() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return map_.size();
}

bool
ExprResCacheManager::Get(const Key& key, Value& out_value) {
    if (!IsEnabled()) {
        return false;
    }
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto it = map_.find(key);
    if (it == map_.end()) {
        return false;
    }
    // Move to front
    lru_list_.splice(lru_list_.begin(), lru_list_, it->second.it);
    out_value = it->second.value;
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
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto it = map_.find(key);
    if (it != map_.end()) {
        // Update existing
        current_bytes_ -= it->second.value.bytes;
        it->second.value = value;
        it->second.value.bytes = EstimateBytes(it->second.value);
        current_bytes_ += it->second.value.bytes;
        lru_list_.splice(lru_list_.begin(), lru_list_, it->second.it);
    } else {
        // Insert new
        lru_list_.push_front(key);
        Entry entry;
        entry.it = lru_list_.begin();
        entry.value = value;
        entry.value.bytes = EstimateBytes(entry.value);
        map_.emplace(key, std::move(entry));
        current_bytes_ += map_[key].value.bytes;
    }
    EnsureCapacity();
    LOG_DEBUG("put expr res cache, segment_id: {}, key: {}",
              key.segment_id,
              key.signature);
}

void
ExprResCacheManager::Clear() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    lru_list_.clear();
    map_.clear();
    current_bytes_ = 0;
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
    while (current_bytes_ > capacity_bytes_ && !lru_list_.empty()) {
        const auto& back_key = lru_list_.back();
        auto it = map_.find(back_key);
        if (it != map_.end()) {
            current_bytes_ -= it->second.value.bytes;
            map_.erase(it);
        }
        lru_list_.pop_back();
    }
}

size_t
ExprResCacheManager::EraseSegment(int64_t segment_id) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    size_t erased = 0;
    for (auto it = lru_list_.begin(); it != lru_list_.end();) {
        if (it->segment_id == segment_id) {
            auto map_it = map_.find(*it);
            if (map_it != map_.end()) {
                current_bytes_ -= map_it->second.value.bytes;
                map_.erase(map_it);
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
