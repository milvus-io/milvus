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

#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/EasyAssert.h"
#include "common/Types.h"
#include "common/mol_c.h"
#include "log/Log.h"

namespace milvus {
namespace exec {

enum class MolCacheEntryState : uint8_t {
    kUninitialized = 0,
    kReady = 1,
    kUnavailable = 2,
};

class SimpleMolCache {
 public:
    ~SimpleMolCache() {
        for (auto handle : mol_handles_) {
            if (handle) {
                FreeMolHandle(handle);
            }
        }
    }

    // Append a lazy cache slot during field loading or insert.
    // Valid rows start as uninitialized and are deserialized on first use.
    // Null rows are marked unavailable so they are skipped without reparsing.
    void
    AppendLazySlot(bool valid) {
        std::lock_guard<std::shared_mutex> lock(mutex_);
        mol_handles_.push_back(nullptr);
        states_.push_back(valid ? MolCacheEntryState::kUninitialized
                                : MolCacheEntryState::kUnavailable);
    }

    // Get shared lock for batch operations (RAII)
    std::shared_lock<std::shared_mutex>
    AcquireReadLock() const {
        return std::shared_lock<std::shared_mutex>(mutex_);
    }

    // Get MolHandle by offset without locking (use with AcquireReadLock)
    MolHandle
    GetByOffsetUnsafe(size_t offset) const {
        if (offset >= mol_handles_.size()) {
            ThrowInfo(UnexpectedError,
                      "MolCache offset {} out of range: {}",
                      offset,
                      mol_handles_.size());
        }
        return mol_handles_[offset];
    }

    MolCacheEntryState
    GetStateByOffsetUnsafe(size_t offset) const {
        if (offset >= states_.size()) {
            ThrowInfo(UnexpectedError,
                      "MolCache offset {} out of range: {}",
                      offset,
                      states_.size());
        }
        return states_[offset];
    }

    std::pair<MolHandle, MolCacheEntryState>
    PublishByOffset(size_t offset, MolHandle handle) {
        std::lock_guard<std::shared_mutex> lock(mutex_);
        if (offset >= states_.size()) {
            if (handle) {
                FreeMolHandle(handle);
            }
            ThrowInfo(UnexpectedError,
                      "MolCache offset {} out of range: {}",
                      offset,
                      states_.size());
        }

        auto state = states_[offset];
        if (state == MolCacheEntryState::kReady) {
            if (handle) {
                FreeMolHandle(handle);
            }
            return {mol_handles_[offset], state};
        }
        if (state == MolCacheEntryState::kUnavailable) {
            if (handle) {
                FreeMolHandle(handle);
            }
            return {nullptr, state};
        }

        if (handle) {
            mol_handles_[offset] = handle;
            states_[offset] = MolCacheEntryState::kReady;
            return {handle, MolCacheEntryState::kReady};
        }

        states_[offset] = MolCacheEntryState::kUnavailable;
        return {nullptr, MolCacheEntryState::kUnavailable};
    }

    size_t
    Size() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return mol_handles_.size();
    }


 private:
    SimpleMolCache(const SimpleMolCache&) = delete;
    SimpleMolCache& operator=(const SimpleMolCache&) = delete;

 public:
    SimpleMolCache() = default;
    SimpleMolCache(SimpleMolCache&&) = default;

 private:
    mutable std::shared_mutex mutex_;
    std::vector<MolHandle> mol_handles_;
    std::vector<MolCacheEntryState> states_;
};

class SimpleMolCacheManager {
 public:
    static SimpleMolCacheManager&
    Instance() {
        static SimpleMolCacheManager instance;
        return instance;
    }

    SimpleMolCacheManager() = default;

    SimpleMolCache&
    GetOrCreateCache(int64_t segment_id, FieldId field_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto key = MakeCacheKey(segment_id, field_id);
        auto it = caches_.find(key);
        if (it != caches_.end()) {
            return *(it->second);
        }

        auto cache = std::make_unique<SimpleMolCache>();
        auto* cache_ptr = cache.get();
        caches_.emplace(key, std::move(cache));
        return *cache_ptr;
    }

    SimpleMolCache*
    GetCache(int64_t segment_id, FieldId field_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto key = MakeCacheKey(segment_id, field_id);
        auto it = caches_.find(key);
        if (it != caches_.end()) {
            return it->second.get();
        }
        return nullptr;
    }

    void
    RemoveSegmentCaches(int64_t segment_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto segment_prefix = std::to_string(segment_id) + "_";
        auto it = caches_.begin();
        while (it != caches_.end()) {
            if (it->first.substr(0, segment_prefix.length()) ==
                segment_prefix) {
                it = caches_.erase(it);
            } else {
                ++it;
            }
        }
    }

 private:
    static inline std::string
    MakeCacheKey(int64_t segment_id, FieldId field_id) {
        return std::to_string(segment_id) + "_" +
               std::to_string(field_id.get());
    }

    SimpleMolCacheManager(const SimpleMolCacheManager&) = delete;
    SimpleMolCacheManager& operator=(const SimpleMolCacheManager&) = delete;

    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::unique_ptr<SimpleMolCache>> caches_;
};

}  // namespace exec
}  // namespace milvus
