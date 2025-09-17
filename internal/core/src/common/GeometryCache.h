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

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>

#include "common/Geometry.h"
#include "common/Types.h"

namespace milvus {
namespace exec {

// Custom hash function for segment id + field id pair
struct SegmentFieldHash {
    std::size_t
    operator()(const std::pair<int64_t, int64_t>& p) const {
        return std::hash<int64_t>{}(p.first) ^
               (std::hash<int64_t>{}(p.second) << 1);
    }
};

// Simple WKB-based Geometry cache for avoiding repeated WKB->Geometry conversions
class SimpleGeometryCache {
 public:
    // Get or create Geometry from WKB data
    std::shared_ptr<const Geometry>
    GetOrCreate(const std::string_view& wkb_data) {
        // Use WKB data content as key (could be optimized with hash later)
        std::string key(wkb_data);

        // Try read-only access first (most common case)
        {
            std::shared_lock<std::shared_mutex> lock(mutex_);
            auto it = cache_.find(key);
            if (it != cache_.end()) {
                return it->second;
            }
        }

        // Cache miss: create new Geometry with write lock
        std::lock_guard<std::shared_mutex> lock(mutex_);

        // Double-check after acquiring write lock
        auto it = cache_.find(key);
        if (it != cache_.end()) {
            return it->second;
        }

        // Construct new Geometry
        try {
            auto geometry = std::make_shared<const Geometry>(wkb_data.data(),
                                                             wkb_data.size());
            cache_.emplace(key, geometry);
            return geometry;
        } catch (...) {
            // Return nullptr on construction failure
            return nullptr;
        }
    }

    // Clear all cached geometries
    void
    Clear() {
        std::lock_guard<std::shared_mutex> lock(mutex_);
        cache_.clear();
    }

    // Get cache statistics
    size_t
    Size() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return cache_.size();
    }

 private:
    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, std::shared_ptr<const Geometry>> cache_;
};

// Global cache instance per segment+field
class SimpleGeometryCacheManager {
 public:
    static SimpleGeometryCacheManager&
    Instance() {
        static SimpleGeometryCacheManager instance;
        return instance;
    }

    SimpleGeometryCacheManager() = default;

    SimpleGeometryCache&
    GetCache(int64_t segment_id, FieldId field_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto key = std::make_pair(segment_id, field_id.get());
        auto it = caches_.find(key);
        if (it != caches_.end()) {
            return *(it->second);
        }

        auto cache = std::make_unique<SimpleGeometryCache>();
        auto* cache_ptr = cache.get();
        caches_.emplace(key, std::move(cache));
        return *cache_ptr;
    }

    void
    RemoveCache(int64_t segment_id, FieldId field_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto key = std::make_pair(segment_id, field_id.get());
        caches_.erase(key);
    }

    // Remove all caches for a segment (useful when segment is destroyed)
    void
    RemoveSegmentCaches(int64_t segment_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = caches_.begin();
        while (it != caches_.end()) {
            if (it->first.first == segment_id) {
                it = caches_.erase(it);
            } else {
                ++it;
            }
        }
    }

    // Get cache statistics for monitoring
    struct CacheStats {
        size_t total_caches = 0;
        size_t total_geometries = 0;
    };

    CacheStats
    GetStats() const {
        std::lock_guard<std::mutex> lock(mutex_);
        CacheStats stats;
        stats.total_caches = caches_.size();
        for (const auto& [key, cache] : caches_) {
            stats.total_geometries += cache->Size();
        }
        return stats;
    }

 private:
    SimpleGeometryCacheManager(const SimpleGeometryCacheManager&) = delete;
    SimpleGeometryCacheManager&
    operator=(const SimpleGeometryCacheManager&) = delete;

    mutable std::mutex mutex_;
    std::unordered_map<std::pair<int64_t, int64_t>,
                       std::unique_ptr<SimpleGeometryCache>,
                       SegmentFieldHash>
        caches_;
};

}  // namespace exec
}  // namespace milvus
