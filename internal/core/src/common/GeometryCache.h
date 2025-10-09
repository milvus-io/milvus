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
#include <unordered_map>
#include <vector>

#include "common/EasyAssert.h"
#include "common/Geometry.h"
#include "common/Types.h"
#include "geos_c.h"
#include "log/Log.h"

namespace milvus {
namespace exec {

// Helper function to create cache key from segment_id and field_id
inline std::string
MakeCacheKey(int64_t segment_id, FieldId field_id) {
    return std::to_string(segment_id) + "_" + std::to_string(field_id.get());
}

// Vector-based Geometry cache that maintains original field data order
class SimpleGeometryCache {
 public:
    // Append WKB data during field loading
    void
    AppendData(GEOSContextHandle_t ctx, const char* wkb_data, size_t size) {
        std::lock_guard<std::shared_mutex> lock(mutex_);

        if (size == 0 || wkb_data == nullptr) {
            // Handle null/empty geometry - add invalid geometry
            geometries_.emplace_back();
        } else {
            try {
                // Create geometry with cache's context
                geometries_.emplace_back(ctx, wkb_data, size);
            } catch (const std::exception& e) {
                ThrowInfo(UnexpectedError,
                          "Failed to construct geometry from WKB data: {}",
                          e.what());
            }
        }
    }

    // Get shared lock for batch operations (RAII)
    std::shared_lock<std::shared_mutex>
    AcquireReadLock() const {
        return std::shared_lock<std::shared_mutex>(mutex_);
    }

    // Get Geometry by offset without locking (use with AcquireReadLock)
    const Geometry*
    GetByOffsetUnsafe(size_t offset) const {
        if (offset >= geometries_.size()) {
            ThrowInfo(UnexpectedError,
                      "offset {} is out of range: {}",
                      offset,
                      geometries_.size());
        }

        const auto& geometry = geometries_[offset];
        return geometry.IsValid() ? &geometry : nullptr;
    }

    // Get Geometry by offset (thread-safe read for filtering)
    const Geometry*
    GetByOffset(size_t offset) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return GetByOffsetUnsafe(offset);
    }

    // Get total number of loaded geometries
    size_t
    Size() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return geometries_.size();
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
        auto key = MakeCacheKey(segment_id, field_id);
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
    RemoveCache(GEOSContextHandle_t ctx, int64_t segment_id, FieldId field_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto key = MakeCacheKey(segment_id, field_id);
        caches_.erase(key);
    }

    // Remove all caches for a segment (useful when segment is destroyed)
    void
    RemoveSegmentCaches(GEOSContextHandle_t ctx, int64_t segment_id) {
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
    std::unordered_map<std::string, std::unique_ptr<SimpleGeometryCache>>
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
RemoveGeometryCache(GEOSContextHandle_t ctx,
                    int64_t segment_id,
                    FieldId field_id) {
    exec::SimpleGeometryCacheManager::Instance().RemoveCache(
        ctx, segment_id, field_id);
}

inline void
RemoveSegmentGeometryCaches(GEOSContextHandle_t ctx, int64_t segment_id) {
    exec::SimpleGeometryCacheManager::Instance().RemoveSegmentCaches(
        ctx, segment_id);
}

}  // namespace milvus
