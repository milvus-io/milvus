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

#include <mutex>
#include <shared_mutex>
#include <vector>

#include "common/EasyAssert.h"
#include "common/Geometry.h"
#include "geos_c.h"

namespace milvus {
namespace exec {

// Vector-based Geometry cache that maintains original field data order
class SimpleGeometryCache {
 public:
    SimpleGeometryCache() : ctx_(GEOS_init_r()) {
        AssertInfo(ctx_ != nullptr, "Failed to initialize GEOS context");
    }

    ~SimpleGeometryCache() {
        // Geometry instances must be destroyed before their GEOS context.
        geometries_.clear();
        GEOS_finish_r(ctx_);
    }

    SimpleGeometryCache(const SimpleGeometryCache&) = delete;
    SimpleGeometryCache&
    operator=(const SimpleGeometryCache&) = delete;

    void
    Reserve(size_t size) {
        std::lock_guard<std::shared_mutex> lock(mutex_);
        geometries_.reserve(size);
    }

    // Append WKB data during field loading
    void
    AppendData(const char* wkb_data, size_t size) {
        std::lock_guard<std::shared_mutex> lock(mutex_);

        if (size == 0 || wkb_data == nullptr) {
            // Handle null/empty geometry - add invalid geometry
            geometries_.emplace_back();
        } else {
            try {
                // Create geometry with cache's context
                geometries_.emplace_back(ctx_, wkb_data, size);
            } catch (const std::exception& e) {
                ThrowInfo(UnexpectedError,
                          "Failed to construct geometry from WKB data: {}",
                          e.what());
            }
        }
    }

    // Set WKB data at its segment-level row offset. Growing inserts can be
    // acknowledged out of order, so cache layout must follow reserved offsets
    // instead of callback completion order.
    void
    SetData(size_t offset, const char* wkb_data, size_t size) {
        std::lock_guard<std::shared_mutex> lock(mutex_);
        if (geometries_.size() <= offset) {
            geometries_.resize(offset + 1);
        }
        if (size == 0 || wkb_data == nullptr) {
            geometries_[offset] = Geometry();
            return;
        }
        try {
            geometries_[offset] = Geometry(ctx_, wkb_data, size);
        } catch (const std::exception& e) {
            ThrowInfo(UnexpectedError,
                      "Failed to construct geometry from WKB data: {}",
                      e.what());
        }
    }

    // Hold a shared lock while evaluating cached Geometry objects so their
    // addresses remain stable. GEOS operations themselves must use the
    // calling query thread's context rather than the cache construction
    // context, allowing concurrent readers.
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
    // The cache owns the GEOS context used by every cached Geometry. This
    // lets a shared cache safely outlive the segment/runtime that published
    // it.
    GEOSContextHandle_t ctx_;
    mutable std::shared_mutex mutex_;  // For read/write operations
    std::vector<Geometry> geometries_;
};

}  // namespace exec

}  // namespace milvus
