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
#include <unordered_map>
#include <vector>

#include "common/EasyAssert.h"
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

// Vector-based Geometry cache that maintains original field data order
class SimpleGeometryCache {
 public:
    // Append WKB data during field loading
    void
    AppendData(const char* wkb_data, size_t size) {
        std::lock_guard<std::shared_mutex> lock(mutex_);

        if (size == 0 || wkb_data == nullptr) {
            // Handle null/empty geometry - add invalid geometry
            geometries_.emplace_back();
        } else {
            try {
                // Create geometry directly in the vector
                geometries_.emplace_back(wkb_data, size);
            } catch (const std::exception& e) {
                PanicInfo(UnexpectedError,
                          "Failed to construct geometry from WKB data: {}",
                          e.what());
            }
        }
    }

    // Get Geometry by offset (thread-safe read for filtering)
    const Geometry*
    GetByOffset(size_t offset) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);

        if (offset >= geometries_.size()) {
            return nullptr;
        }

        const auto& geometry = geometries_[offset];
        return geometry.IsValid() ? &geometry : nullptr;
    }

    // Get total number of loaded geometries
    size_t
    Size() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return geometries_.size();
    }

    // Clear all cached geometries
    void
    Clear() {
        std::lock_guard<std::shared_mutex> lock(mutex_);
        geometries_.clear();
    }

    // Check if cache is loaded
    bool
    IsLoaded() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return !geometries_.empty();
    }

 private:
    mutable std::shared_mutex mutex_;   // For read/write operations
    std::vector<Geometry> geometries_;  // Direct storage of Geometry objects
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
        size_t loaded_caches = 0;
        size_t total_geometries = 0;
    };

    CacheStats
    GetStats() const {
        std::lock_guard<std::mutex> lock(mutex_);
        CacheStats stats;
        stats.total_caches = caches_.size();
        for (const auto& [key, cache] : caches_) {
            if (cache->IsLoaded()) {
                stats.loaded_caches++;
                stats.total_geometries += cache->Size();
            }
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

// Convenient global functions for direct access to geometry cache
inline const Geometry*
GetGeometryByOffset(int64_t segment_id, FieldId field_id, size_t offset) {
    auto& cache = exec::SimpleGeometryCacheManager::Instance().GetCache(
        segment_id, field_id);
    return cache.GetByOffset(offset);
}

inline void
RemoveGeometryCache(int64_t segment_id, FieldId field_id) {
    exec::SimpleGeometryCacheManager::Instance().RemoveCache(segment_id,
                                                             field_id);
}

inline void
RemoveSegmentGeometryCaches(int64_t segment_id) {
    exec::SimpleGeometryCacheManager::Instance().RemoveSegmentCaches(
        segment_id);
}

}  // namespace milvus
