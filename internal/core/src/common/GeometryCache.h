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

// Vector-based Geometry cache that maintains original field data order.
//
// The cache owns its own GEOS context: every cached Geometry is built and
// destroyed with ctx_, so the cache is fully self-contained and its lifetime
// is independent of the segment that populated it. Combined with the manager
// handing out shared_ptr<SimpleGeometryCache>, an in-flight query keeps the
// cache (and its context) alive even if the owning segment is dropped and
// RemoveSegmentCaches() runs concurrently.
class SimpleGeometryCache {
 public:
    // InitGEOSContext translates an allocation failure into a retriable
    // MemAllocateFailed (GEOS_init_r throws bad_alloc on OOM, it never
    // returns nullptr -- see the helper's comment).
    SimpleGeometryCache() : ctx_(InitGEOSContext("geometry cache")) {
    }

    ~SimpleGeometryCache() {
        // Destroy the cached geometries (each calls GEOSGeom_destroy_r(ctx_,
        // ...)) while ctx_ is still alive, then release the context.
        {
            std::lock_guard<std::shared_mutex> lock(mutex_);
            geometries_.clear();
        }
        if (ctx_ != nullptr) {
            GEOS_finish_r(ctx_);
            ctx_ = nullptr;
        }
    }

    // The cache owns a GEOS context, so it is neither copyable nor movable.
    SimpleGeometryCache(const SimpleGeometryCache&) = delete;
    SimpleGeometryCache&
    operator=(const SimpleGeometryCache&) = delete;

    // Append WKB data during field loading.
    //
    // A row with corrupt (unparseable) WKB is appended as an INVALID entry --
    // GetByOffsetUnsafe() returns nullptr for it and every reader skips it --
    // instead of throwing. Throwing here would make a single corrupt row fail
    // the whole segment load whenever the geometry cache is enabled (the write
    // paths deliberately keep such rows: add_geometry / bulk_load index a
    // placeholder MBR rather than dropping them, so they DO reach the cache).
    // The entry must still be appended: the cache is addressed by absolute
    // segment offset, so dropping it would shift every later row. A transient
    // resource failure (reader allocation) still throws a retriable system
    // error via TryParseFromWkb -- that is not bad data. One exception we
    // cannot tell apart: an OOM INSIDE GEOS parsing surfaces as the same
    // nullptr as corrupt WKB and is deliberately classified as bad data here
    // (see the KNOWN LIMIT note on TryParseFromWkb). See PR #50951 review.
    void
    AppendData(const char* wkb_data, size_t size) {
        std::lock_guard<std::shared_mutex> lock(mutex_);

        if (size == 0 || wkb_data == nullptr) {
            // Handle null/empty geometry - add invalid geometry
            geometries_.emplace_back();
            return;
        }
        Geometry geometry;
        if (!geometry.TryParseFromWkb(ctx_, wkb_data, size)) {
            LOG_WARN(
                "unparseable WKB at cache offset {}; caching an invalid "
                "placeholder entry, readers will skip it",
                geometries_.size());
            geometries_.emplace_back();
            return;
        }
        geometries_.push_back(std::move(geometry));
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

    // Get total number of loaded geometries without locking (use with
    // AcquireReadLock). Calling Size() while already holding the read lock
    // would recursively acquire the non-reentrant shared_mutex -- UB, and a
    // real deadlock once a writer is queued on this writer-preferring mutex.
    size_t
    SizeUnsafe() const {
        return geometries_.size();
    }

    // Check if cache is loaded
    bool
    IsLoaded() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return !geometries_.empty();
    }

 private:
    // ctx_ is declared first so it is destroyed last (after geometries_),
    // guaranteeing the Geometry destructors still see a live context.
    GEOSContextHandle_t ctx_{nullptr};  // Context owned by this cache
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

    // Returns a shared_ptr so callers keep the cache alive for the duration of
    // their use even if RemoveCache/RemoveSegmentCaches drops it concurrently.
    std::shared_ptr<SimpleGeometryCache>
    GetOrCreateCache(int64_t segment_id, FieldId field_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto key = MakeCacheKey(segment_id, field_id);
        auto it = caches_.find(key);
        if (it != caches_.end()) {
            return it->second;
        }

        auto cache = std::make_shared<SimpleGeometryCache>();
        caches_.emplace(key, cache);
        return cache;
    }

    std::shared_ptr<SimpleGeometryCache>
    GetCache(int64_t segment_id, FieldId field_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto key = MakeCacheKey(segment_id, field_id);
        auto it = caches_.find(key);
        if (it != caches_.end()) {
            return it->second;
        }
        return nullptr;
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
    std::unordered_map<std::string, std::shared_ptr<SimpleGeometryCache>>
        caches_;
};

}  // namespace exec

}  // namespace milvus
