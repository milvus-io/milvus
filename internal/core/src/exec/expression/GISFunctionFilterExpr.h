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

#include <fmt/core.h>
#include <unordered_map>
#include <shared_mutex>
#include <memory>

#include "common/FieldDataInterface.h"
#include "common/Vector.h"
#include "common/Geometry.h"
#include "exec/expression/Expr.h"
#include "expr/ITypeExpr.h"
#include "segcore/SegmentInterface.h"

namespace milvus {
namespace exec {

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

    SimpleGeometryCache&
    GetCache(const void* segment_addr, FieldId field_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto key = std::make_pair(segment_addr, field_id.get());
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
    RemoveCache(const void* segment_addr, FieldId field_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto key = std::make_pair(segment_addr, field_id.get());
        caches_.erase(key);
    }

    // Remove all caches for a segment (useful when segment is destroyed)
    void
    RemoveSegmentCaches(const void* segment_addr) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = caches_.begin();
        while (it != caches_.end()) {
            if (it->first.first == segment_addr) {
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
    mutable std::mutex mutex_;
    std::unordered_map<std::pair<const void*, int64_t>,
                       std::unique_ptr<SimpleGeometryCache>,
                       std::hash<std::pair<const void*, int64_t>>>
        caches_;
};

class PhyGISFunctionFilterExpr : public SegmentExpr {
 public:
    PhyGISFunctionFilterExpr(
        const std::vector<std::shared_ptr<Expr>>& input,
        const std::shared_ptr<const milvus::expr::GISFunctionFilterExpr>& expr,
        const std::string& name,
        const segcore::SegmentInternalInterface* segment,
        int64_t active_count,
        int64_t batch_size,
        int32_t consistency_level)
        : SegmentExpr(std::move(input),
                      name,
                      segment,
                      expr->column_.field_id_,
                      expr->column_.nested_path_,
                      DataType::GEOMETRY,
                      active_count,
                      batch_size,
                      consistency_level),
          expr_(expr) {
    }

    void
    Eval(EvalCtx& context, VectorPtr& result) override;

    std::optional<milvus::expr::ColumnInfo>
    GetColumnInfo() const override {
        return expr_->column_;
    }

 private:
    VectorPtr
    EvalForIndexSegment();

    VectorPtr
    EvalForDataSegment();

 private:
    std::shared_ptr<const milvus::expr::GISFunctionFilterExpr> expr_;

    /*
     * Segment-level cache: run a single R-Tree Query for all index chunks to
     * obtain coarse candidate bitmaps. Subsequent batches reuse these cached
     * results to avoid repeated ScalarIndex::Query calls per chunk.
     */
    // whether coarse results have been prefetched once
    bool coarse_cached_ = false;
    // global coarse bitmap (segment-level)
    TargetBitmap coarse_global_;
    // global not-null bitmap (segment-level)
    TargetBitmap coarse_valid_global_;
};
}  //namespace exec
}  // namespace milvus
