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

#include <geos_c.h>
#include <memory>
#include "common/EasyAssert.h"
#include "common/Geometry.h"

namespace milvus {

/**
 * PreparedGeometry wraps GEOS PreparedGeometry for accelerated repeated spatial predicate evaluation.
 *
 * GEOS prepared geometries pre-compute spatial indexes and other structures that make
 * repeated intersection/containment tests faster than raw geometry operations.
 *
 * Use case: When testing one geometry (e.g., a query polygon) against many other geometries
 * (e.g., candidate points from R-tree), prepare the query geometry once and reuse it.
 *
 * Example:
 *   Geometry query_polygon(ctx, wkt);
 *   PreparedGeometry prepared(ctx, query_polygon);
 *
 *   for (auto& candidate : candidates) {
 *       if (prepared.intersects(candidate)) {  // faster than query_polygon.intersects(candidate)
 *           results.push_back(candidate);
 *       }
 *   }
 *
 * Thread safety: PreparedGeometry is NOT thread-safe. Each thread should create its own
 * PreparedGeometry instance, or access must be synchronized externally.
 */
class PreparedGeometry {
 public:
    // Default constructor creates invalid prepared geometry
    PreparedGeometry() : prepared_(nullptr), ctx_(nullptr) {
    }

    /**
     * Construct PreparedGeometry from an existing Geometry.
     * The source Geometry must remain valid for the lifetime of this PreparedGeometry.
     *
     * @param ctx GEOS context handle (must match the Geometry's context)
     * @param geom Source geometry to prepare
     */
    explicit PreparedGeometry(GEOSContextHandle_t ctx, const Geometry& geom)
        : ctx_(ctx), prepared_(nullptr) {
        if (!geom.IsValid()) {
            return;
        }

        prepared_ = GEOSPrepare_r(ctx, geom.GetGeometry());
        AssertInfo(prepared_ != nullptr,
                   "Failed to create GEOS prepared geometry");
    }

    /**
     * Construct PreparedGeometry from a raw GEOS geometry pointer.
     * The source geometry must remain valid for the lifetime of this PreparedGeometry.
     *
     * @param ctx GEOS context handle
     * @param geom Raw GEOS geometry pointer
     */
    explicit PreparedGeometry(GEOSContextHandle_t ctx, const GEOSGeometry* geom)
        : ctx_(ctx), prepared_(nullptr) {
        if (geom == nullptr) {
            return;
        }

        prepared_ = GEOSPrepare_r(ctx, geom);
        AssertInfo(prepared_ != nullptr,
                   "Failed to create GEOS prepared geometry");
    }

    ~PreparedGeometry() {
        if (prepared_ != nullptr) {
            GEOSPreparedGeom_destroy_r(ctx_, prepared_);
        }
    }

    // Non-copyable (prepared geometry references the original)
    PreparedGeometry(const PreparedGeometry&) = delete;
    PreparedGeometry&
    operator=(const PreparedGeometry&) = delete;

    // Moveable
    PreparedGeometry(PreparedGeometry&& other) noexcept
        : ctx_(other.ctx_), prepared_(other.prepared_) {
        other.prepared_ = nullptr;
        other.ctx_ = nullptr;
    }

    PreparedGeometry&
    operator=(PreparedGeometry&& other) noexcept {
        if (this != &other) {
            if (prepared_ != nullptr) {
                GEOSPreparedGeom_destroy_r(ctx_, prepared_);
            }
            ctx_ = other.ctx_;
            prepared_ = other.prepared_;
            other.prepared_ = nullptr;
            other.ctx_ = nullptr;
        }
        return *this;
    }

    bool
    IsValid() const {
        return prepared_ != nullptr;
    }

    // ============================================================================
    // Prepared spatial predicates - faster than unprepared versions
    // ============================================================================

    /**
     * Tests if the prepared geometry intersects with the other geometry.
     * This is the most commonly used predicate for spatial filtering.
     */
    bool
    intersects(const Geometry& other) const {
        if (!IsValid() || !other.IsValid()) {
            return false;
        }
        return GEOSPreparedIntersects_r(ctx_, prepared_, other.GetGeometry()) ==
               1;
    }

    bool
    intersects(const GEOSGeometry* other) const {
        if (!IsValid() || other == nullptr) {
            return false;
        }
        return GEOSPreparedIntersects_r(ctx_, prepared_, other) == 1;
    }

    /**
     * Tests if the prepared geometry contains the other geometry.
     * Note: For ST_Contains queries, prepare the container (larger geometry).
     */
    bool
    contains(const Geometry& other) const {
        if (!IsValid() || !other.IsValid()) {
            return false;
        }
        return GEOSPreparedContains_r(ctx_, prepared_, other.GetGeometry()) ==
               1;
    }

    bool
    contains(const GEOSGeometry* other) const {
        if (!IsValid() || other == nullptr) {
            return false;
        }
        return GEOSPreparedContains_r(ctx_, prepared_, other) == 1;
    }

    /**
     * Tests if the prepared geometry contains the other geometry properly
     * (no boundary intersection).
     */
    bool
    containsProperly(const Geometry& other) const {
        if (!IsValid() || !other.IsValid()) {
            return false;
        }
        return GEOSPreparedContainsProperly_r(
                   ctx_, prepared_, other.GetGeometry()) == 1;
    }

    bool
    containsProperly(const GEOSGeometry* other) const {
        if (!IsValid() || other == nullptr) {
            return false;
        }
        return GEOSPreparedContainsProperly_r(ctx_, prepared_, other) == 1;
    }

    /**
     * Tests if the prepared geometry covers the other geometry.
     */
    bool
    covers(const Geometry& other) const {
        if (!IsValid() || !other.IsValid()) {
            return false;
        }
        return GEOSPreparedCovers_r(ctx_, prepared_, other.GetGeometry()) == 1;
    }

    bool
    covers(const GEOSGeometry* other) const {
        if (!IsValid() || other == nullptr) {
            return false;
        }
        return GEOSPreparedCovers_r(ctx_, prepared_, other) == 1;
    }

    /**
     * Tests if the prepared geometry is covered by the other geometry.
     */
    bool
    coveredBy(const Geometry& other) const {
        if (!IsValid() || !other.IsValid()) {
            return false;
        }
        return GEOSPreparedCoveredBy_r(ctx_, prepared_, other.GetGeometry()) ==
               1;
    }

    bool
    coveredBy(const GEOSGeometry* other) const {
        if (!IsValid() || other == nullptr) {
            return false;
        }
        return GEOSPreparedCoveredBy_r(ctx_, prepared_, other) == 1;
    }

    /**
     * Tests if the prepared geometry crosses the other geometry.
     */
    bool
    crosses(const Geometry& other) const {
        if (!IsValid() || !other.IsValid()) {
            return false;
        }
        return GEOSPreparedCrosses_r(ctx_, prepared_, other.GetGeometry()) == 1;
    }

    bool
    crosses(const GEOSGeometry* other) const {
        if (!IsValid() || other == nullptr) {
            return false;
        }
        return GEOSPreparedCrosses_r(ctx_, prepared_, other) == 1;
    }

    /**
     * Tests if the prepared geometry is disjoint from the other geometry.
     */
    bool
    disjoint(const Geometry& other) const {
        if (!IsValid() || !other.IsValid()) {
            return false;
        }
        return GEOSPreparedDisjoint_r(ctx_, prepared_, other.GetGeometry()) ==
               1;
    }

    bool
    disjoint(const GEOSGeometry* other) const {
        if (!IsValid() || other == nullptr) {
            return false;
        }
        return GEOSPreparedDisjoint_r(ctx_, prepared_, other) == 1;
    }

    /**
     * Tests if the prepared geometry overlaps with the other geometry.
     */
    bool
    overlaps(const Geometry& other) const {
        if (!IsValid() || !other.IsValid()) {
            return false;
        }
        return GEOSPreparedOverlaps_r(ctx_, prepared_, other.GetGeometry()) ==
               1;
    }

    bool
    overlaps(const GEOSGeometry* other) const {
        if (!IsValid() || other == nullptr) {
            return false;
        }
        return GEOSPreparedOverlaps_r(ctx_, prepared_, other) == 1;
    }

    /**
     * Tests if the prepared geometry touches the other geometry.
     */
    bool
    touches(const Geometry& other) const {
        if (!IsValid() || !other.IsValid()) {
            return false;
        }
        return GEOSPreparedTouches_r(ctx_, prepared_, other.GetGeometry()) == 1;
    }

    bool
    touches(const GEOSGeometry* other) const {
        if (!IsValid() || other == nullptr) {
            return false;
        }
        return GEOSPreparedTouches_r(ctx_, prepared_, other) == 1;
    }

    /**
     * Tests if the prepared geometry is within the other geometry.
     * Note: For ST_Within queries where you're testing if candidates are within
     * a query polygon, prepare the query polygon and use contains() instead.
     */
    bool
    within(const Geometry& other) const {
        if (!IsValid() || !other.IsValid()) {
            return false;
        }
        return GEOSPreparedWithin_r(ctx_, prepared_, other.GetGeometry()) == 1;
    }

    bool
    within(const GEOSGeometry* other) const {
        if (!IsValid() || other == nullptr) {
            return false;
        }
        return GEOSPreparedWithin_r(ctx_, prepared_, other) == 1;
    }

    // ============================================================================
    // Non-prepared predicates (no prepared version available in GEOS)
    // These fall back to regular geometry operations
    // ============================================================================

    /**
     * Tests if the prepared geometry equals the other geometry.
     * Note: GEOS doesn't provide a prepared version of equals, so this requires
     * the original geometry to be passed.
     */
    static bool
    equals(GEOSContextHandle_t ctx,
           const GEOSGeometry* geom,
           const Geometry& other) {
        if (geom == nullptr || !other.IsValid()) {
            return false;
        }
        return GEOSEquals_r(ctx, geom, other.GetGeometry()) == 1;
    }

 private:
    GEOSContextHandle_t ctx_;
    const GEOSPreparedGeometry* prepared_;
};

}  // namespace milvus
