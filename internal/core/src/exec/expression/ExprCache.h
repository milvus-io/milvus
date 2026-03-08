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

#include <cstddef>
#include <cstdint>
#include <atomic>
#include <list>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <tbb/concurrent_unordered_map.h>

#include "common/Types.h"
#include "log/Log.h"

namespace milvus {
namespace exec {

// Process-level LRU cache for expression result bitsets.
class ExprResCacheManager {
 public:
    struct Key {
        int64_t segment_id{0};
        std::string signature;  // expr signature including parameters

        bool
        operator==(const Key& other) const {
            return segment_id == other.segment_id &&
                   signature == other.signature;
        }
    };

    struct KeyHasher {
        size_t
        operator()(const Key& k) const noexcept {
            std::hash<int64_t> h1;
            std::hash<std::string> h2;
            return (h1(k.segment_id) * 1315423911u) ^ h2(k.signature);
        }
    };

    struct Value {
        std::shared_ptr<TargetBitmap> result;        // filter result bits
        std::shared_ptr<TargetBitmap> valid_result;  // valid bits
        int64_t active_count{0};                     // active count when cached
        size_t bytes{0};  // approximate size in bytes
    };

 public:
    static ExprResCacheManager&
    Instance();
    static void
    SetEnabled(bool enabled);
    static bool
    IsEnabled();
    static void
    Init(size_t capacity_bytes, bool enabled);

    void
    SetCapacityBytes(size_t capacity_bytes);
    size_t
    GetCapacityBytes() const;
    size_t
    GetCurrentBytes() const;
    size_t
    GetEntryCount() const;

    // Try to get cached value. If found, returns true and fills out_value.
    bool
    Get(const Key& key, Value& out_value);

    // Insert or update cache entry. The provided value.result must be non-null.
    void
    Put(const Key& key, const Value& value);

    void
    Clear();

    // Erase all cache entries of a specific segment. Returns number of erased entries.
    size_t
    EraseSegment(int64_t segment_id);

 private:
    ExprResCacheManager() = default;

    size_t
    EstimateBytes(const Value& v) const;
    void
    EnsureCapacity();

 private:
    static std::atomic<bool> enabled_;
    std::atomic<size_t> capacity_bytes_{256ull * 1024ull *
                                        1024ull};  // default 256MB
    std::atomic<size_t> current_bytes_{0};

    mutable std::mutex lru_mutex_;
    std::list<Key> lru_list_;
    using ListIt = std::list<Key>::iterator;
    struct Entry {
        Value value;
        ListIt lru_it;

        Entry() = default;
        Entry(const Value& v, ListIt it) : value(v), lru_it(it) {
        }
    };

    tbb::concurrent_unordered_map<Key, Entry, KeyHasher> concurrent_map_;
};

// Helper API: erase all cache for a given segment id, returns erased entry count
inline size_t
EraseSegmentCache(int64_t segment_id) {
    if (!ExprResCacheManager::IsEnabled()) {
        LOG_INFO("expr res cache is disabled, skip erase segment cache");
        return 0;
    }
    return ExprResCacheManager::Instance().EraseSegment(segment_id);
}

}  // namespace exec
}  // namespace milvus
