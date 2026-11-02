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
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <memory>
#include <cmath>
#include <new>
#include <string>
#include "common/EasyAssert.h"
#include "log/Log.h"

namespace milvus {

/**
 * Best-effort time throttle for logs on per-row geometry paths.
 *
 * The geometry write and refine paths log one line per offending row. A
 * segment full of corrupt or topologically invalid geometries would otherwise
 * emit one line per row per query (refine) or per rebuild (write) -- millions
 * of identical lines that bury everything else. Each call site owns a static
 * counter; this returns true at most once per interval per counter. It proves
 * the condition is occurring; it does not count occurrences.
 */
inline bool
ShouldLogGeometryThrottled(std::atomic<int64_t>& last_log_us,
                           int64_t interval_us = 10LL * 1000 * 1000) {
    auto now_us = std::chrono::duration_cast<std::chrono::microseconds>(
                      std::chrono::steady_clock::now().time_since_epoch())
                      .count();
    auto previous = last_log_us.load(std::memory_order_relaxed);
    return now_us - previous >= interval_us &&
           last_log_us.compare_exchange_strong(
               previous, now_us, std::memory_order_relaxed);
}

/**
 * Reduce a tri-state GEOS predicate result to bool, observably.
 *
 * The GEOS capi binary predicates (GEOSIntersects_r, GEOSPreparedContains_r,
 * GEOSisValid_r, ...) return char 1 = true, 0 = false, 2 = an exception was
 * caught inside GEOS's execute() guard (a topology error on data GEOS cannot
 * evaluate, or an allocation failure swallowed the same way -- see the KNOWN
 * LIMIT note on Geometry::TryParseFromWkb). A bare `== 1` silently maps 2 to
 * "no match", turning an evaluation failure into an unlogged false negative.
 * Mapping 2 to false is still the deliberate choice -- the filter paths'
 * single-row-leniency contract says one bad row must not fail the whole
 * query, matching how corrupt WKB is skipped -- but it must be visible, so
 * the exception case is logged.
 *
 * The log is time-throttled because this sits on the exact-refinement hot
 * path: it can run once per candidate row per query, so a segment full of
 * topologically invalid geometries would otherwise emit millions of identical
 * warnings for a single full-scan predicate. The throttle is process-wide and
 * best-effort -- its job is to prove the condition is occurring, not to count
 * occurrences.
 */
inline bool
GeosPredicateIsTrue(char result, const char* op) {
    if (result == 2) {
        static std::atomic<int64_t> last_log_us{0};
        if (ShouldLogGeometryThrottled(last_log_us)) {
            LOG_WARN(
                "GEOS predicate {} raised an exception; treating the row as "
                "not matching (further occurrences suppressed briefly)",
                op);
        }
        return false;
    }
    return result == 1;
}

/**
 * Create a GEOS context, translating allocation failure into a retriable
 * system error.
 *
 * GEOS_init_r() allocates its context with a bare `new` (it is NOT wrapped in
 * the capi execute() guard), so on OOM it throws std::bad_alloc and never
 * returns nullptr. Without this translation the bad_alloc is not a
 * SegcoreError, so it would bypass the `catch (const SegcoreError&)` rethrow
 * guards on the index write paths and collapse into a non-retryable
 * UnexpectedError at the cgo boundary -- inverting the transient-vs-permanent
 * classification for what is a textbook transient failure.
 */
inline GEOSContextHandle_t
InitGEOSContext(const char* purpose) {
    try {
        return GEOS_init_r();
    } catch (const std::bad_alloc&) {
        ThrowInfo(ErrorCode::MemAllocateFailed,
                  "out of memory initializing GEOS context for {}",
                  purpose);
    }
}

/**
 * Test-only one-shot fault injection for the clone path below.
 *
 * The only realistic failure of GEOSGeom_clone_r() is allocation failure,
 * which a unit test cannot provoke, so the classification of that failure
 * would otherwise be untestable. Lives next to the call site so both share one
 * flag; production code never sets it.
 */
inline std::atomic<bool>&
GeometryCloneFailureForTesting() {
    static std::atomic<bool> flag{false};
    return flag;
}

/**
 * Deep-clone a geometry into `ctx`, translating clone failure into a retriable
 * system error.
 *
 * GEOSGeom_clone_r() runs inside the capi execute() guard, which catches
 * std::exception -- std::bad_alloc included -- and returns nullptr rather than
 * propagating it (geos_ts_c.cpp). So an OOM on this path NEVER surfaces as an
 * exception that a `catch (const std::bad_alloc&)` net could translate, and
 * reporting the nullptr with a plain AssertInfo would classify a textbook
 * transient failure as a non-retryable UnexpectedError (2001).
 *
 * On the search path that is worse than just losing the retry: 2001 is not
 * retriable in merr's segcore table, and lb_policy treats a non-retriable
 * failure by BLACKLISTING the serving node for the channel
 * (shardclient/lb_policy.go) instead of merely excluding it from this one
 * request and failing over to another replica. Memory pressure would then
 * evict healthy nodes from routing.
 *
 * Callers clone geometries that already parsed and validated, so a nullptr
 * here is attributable to resource failure -- there is none of the bad-data /
 * OOM ambiguity that TryParseFromWkb documents as a KNOWN LIMIT. Same
 * reasoning as InitGEOSContext above.
 */
inline GEOSGeometry*
CloneGeometryOrThrow(GEOSContextHandle_t ctx, const GEOSGeometry* geom) {
    GEOSGeometry* cloned = GEOSGeom_clone_r(ctx, geom);
    if (GeometryCloneFailureForTesting().exchange(false) && cloned != nullptr) {
        GEOSGeom_destroy_r(ctx, cloned);
        cloned = nullptr;
    }
    if (cloned == nullptr) {
        ThrowInfo(ErrorCode::MemAllocateFailed,
                  "out of memory cloning geometry");
    }
    return cloned;
}

/**
 * RAII holder for a GEOS context and the reader/geometry commonly held with
 * it. Any code path that can throw between GEOS_init_r and GEOS_finish_r --
 * a std::bad_alloc from a container op (now translated to a retriable error
 * and retried, so a leak repeats once per attempt and compounds the OOM), a
 * throwing Geometry constructor, an AssertInfo -- must hold its resources
 * here instead of relying on trailing cleanup calls. Destruction order
 * matters: geometry and reader are destroyed while the context is still
 * alive.
 */
struct ScopedGeosResources {
    GEOSContextHandle_t ctx{nullptr};
    GEOSWKBReader* reader{nullptr};
    GEOSGeometry* geom{nullptr};

    explicit ScopedGeosResources(const char* purpose)
        : ctx(InitGEOSContext(purpose)) {
    }
    ScopedGeosResources(const ScopedGeosResources&) = delete;
    ScopedGeosResources&
    operator=(const ScopedGeosResources&) = delete;

    ~ScopedGeosResources() {
        release_geom();
        if (reader != nullptr) {
            GEOSWKBReader_destroy_r(ctx, reader);
            reader = nullptr;
        }
        if (ctx != nullptr) {
            GEOS_finish_r(ctx);
            ctx = nullptr;
        }
    }

    void
    release_geom() {
        if (geom != nullptr) {
            GEOSGeom_destroy_r(ctx, geom);
            geom = nullptr;
        }
    }
};

/**
 * Get a thread-local GEOS context handle for thread-safe operations.
 *
 * GEOS context handles are NOT thread-safe - concurrent operations on the same
 * context can cause crashes or data corruption. This function provides each thread
 * with its own context that is lazily initialized and automatically cleaned up
 * when the thread exits.
 *
 * Use this instead of segment_->get_ctx() when thread safety is required.
 */
inline GEOSContextHandle_t
GetThreadLocalGEOSContext() {
    thread_local struct ThreadLocalContext {
        GEOSContextHandle_t ctx;
        ThreadLocalContext() : ctx(InitGEOSContext("thread-local geometry")) {
        }
        ~ThreadLocalContext() {
            if (ctx) {
                GEOS_finish_r(ctx);
            }
        }
    } tls;
    return tls.ctx;
}

class Geometry {
 public:
    // Default constructor creates invalid geometry
    Geometry() : geometry_(nullptr), ctx_(nullptr) {
    }

    ~Geometry() {
        if (geometry_ != nullptr) {
            GEOSGeom_destroy_r(ctx_, geometry_);
        }
    }

    // Constructor from WKB data
    explicit Geometry(GEOSContextHandle_t ctx, const void* wkb, size_t size)
        : ctx_(ctx) {
        GEOSWKBReader* reader = GEOSWKBReader_create_r(ctx);
        AssertInfo(reader != nullptr, "Failed to create GEOS WKB reader");

        GEOSGeometry* geom = GEOSWKBReader_read_r(
            ctx, reader, static_cast<const unsigned char*>(wkb), size);
        GEOSWKBReader_destroy_r(ctx, reader);

        AssertInfo(geom != nullptr,
                   "Failed to construct geometry from WKB data");
        geometry_ = geom;
    }

    // Constructor from WKT string
    explicit Geometry(GEOSContextHandle_t ctx, const char* wkt) : ctx_(ctx) {
        GEOSWKTReader* reader = GEOSWKTReader_create_r(ctx);
        AssertInfo(reader != nullptr, "Failed to create GEOS WKT reader");

        GEOSGeometry* geom = GEOSWKTReader_read_r(ctx, reader, wkt);
        GEOSWKTReader_destroy_r(ctx, reader);

        AssertInfo(geom != nullptr,
                   "Failed to construct geometry from WKT data");
        geometry_ = geom;
    }

    // WKB parse into this instance that returns false ONLY for bad data
    // (unparseable WKB), leaving this invalid (IsValid() == false), instead of
    // the throwing WKB constructor's AssertInfo. Mirrors the geometry cache's
    // GetByOffset() == nullptr contract so exact refinement can
    // `continue` past a corrupt/placeholder row in every configuration
    // (geometry cache on or off) rather than throwing and failing the whole
    // query.
    //
    // A transient resource failure (null context / reader allocation) is NOT
    // bad data: collapsing it into `false` would silently filter out a
    // perfectly valid row (a silent false negative). Those cases throw a
    // retriable system error instead, matching RTreeIndexWrapper::add_geometry.
    //
    // KNOWN LIMIT: the transient/bad-data split above only covers failures we
    // can observe BEFORE parsing. GEOS's capi execute() wrapper catches every
    // exception thrown inside GEOSWKBReader_read_r -- including std::bad_alloc
    // -- and returns nullptr, so a transient OOM DURING parsing is
    // indistinguishable from unparseable WKB at this boundary (telling them
    // apart would require installing a GEOS error handler and parsing message
    // strings). Such a row is therefore classified as bad data with no retry:
    // the geometry cache leaves it without a geometry, the filter paths
    // evaluate it to false, and the index stamps a placeholder MBR. Accepted
    // tradeoff: parse-time OOM is rare and the blast radius is a single row.
    // (The cache does not freeze that outcome: if the same offset is written
    // again -- e.g. its batch is retried -- the row is re-parsed, see
    // SimpleGeometryCache::AppendDataAt.)
    bool
    TryParseFromWkb(GEOSContextHandle_t ctx, const void* wkb, size_t size) {
        if (ctx == nullptr) {
            ThrowInfo(ErrorCode::MemAllocateFailed,
                      "null GEOS context while parsing WKB");
        }
        if (geometry_ != nullptr) {
            GEOSGeom_destroy_r(ctx_, geometry_);
            geometry_ = nullptr;
        }
        ctx_ = ctx;
        GEOSWKBReader* reader = GEOSWKBReader_create_r(ctx);
        if (reader == nullptr) {
            ThrowInfo(ErrorCode::MemAllocateFailed,
                      "failed to create GEOS WKB reader");
        }
        geometry_ = GEOSWKBReader_read_r(
            ctx, reader, static_cast<const unsigned char*>(wkb), size);
        GEOSWKBReader_destroy_r(ctx, reader);
        return geometry_ != nullptr;
    }

    // Copy assignment (deep clone). Releases any geometry this instance
    // already owns before taking a clone of `other`, so the two instances
    // never share a raw GEOSGeometry* (which would double-free on
    // destruction).
    //
    // CONSTRAINT: copying drives GEOS through the SOURCE instance's context
    // (GEOSGeom_clone_r(other.ctx_, ...)) and the copy keeps that context.
    // Therefore a cache-owned Geometry must never be copied from a query
    // thread: (1) the cache's shared context is not thread-safe, so the clone
    // call itself is a data race, and (2) a copy that outlives the cache's
    // shared_ptr holds a dangling context. Query threads must use Clone(ctx)
    // with their own (thread-local) context instead.
    //
    // These operators cannot simply be `= delete`d, but not because ingest
    // copies Geometry values -- it does not. Geometry ingest stores WKB
    // strings: FieldData<Geometry> derives from FieldDataGeometryImpl, which
    // is FieldDataImpl<std::string, true>, and storage::CreateFieldData builds
    // exactly that for GEOMETRY. FieldDataImpl<Geometry, true> has no user
    // anywhere in the tree. What forces the operators is its explicit
    // instantiation (`template class FieldDataImpl<Geometry, true>;` in
    // FieldData.cpp), which instantiates the non-trivially-copyable
    // std::copy_n branch of FillFieldData over Geometry -- so deleting them
    // breaks the build even though nothing calls it.
    //
    // The only live copy sites are two std::any hops on the query path: the
    // dataset Set in GISFunctionFilterExpr and the Get<Geometry>() in
    // RTreeIndex::Query.
    Geometry&
    operator=(const Geometry& other) {
        if (this != &other) {
            if (geometry_ != nullptr) {
                GEOSGeom_destroy_r(ctx_, geometry_);
                geometry_ = nullptr;
            }
            ctx_ = other.ctx_;
            if (other.IsValid()) {
                geometry_ = CloneGeometryOrThrow(other.ctx_, other.geometry_);
            }
        }
        return *this;
    }

    // Copy constructor (deep clone). Same CONSTRAINT as copy assignment above:
    // never copy a cache-owned Geometry from a query thread; use Clone(ctx).
    Geometry(const Geometry& other) : ctx_(other.ctx_) {
        // nullptr first: CloneGeometryOrThrow can throw, and an unset
        // geometry_ would be read by nothing (the object never completes
        // construction) but leaves the member indeterminate for any future
        // catch-and-inspect caller.
        geometry_ = nullptr;
        if (other.IsValid()) {
            geometry_ = CloneGeometryOrThrow(other.ctx_, other.geometry_);
        }
    }

    // Explicit deep clone into the CALLER's context. All GEOS work runs
    // through `ctx`, never through this instance's context, and the returned
    // Geometry is bound to `ctx` -- this is the only safe way to duplicate a
    // cache-owned Geometry from a query thread (pass
    // GetThreadLocalGEOSContext()), and the clone stays valid after the
    // cache is gone.
    //
    // NOTE: no production caller today -- the filter paths borrow cached
    // geometries by pointer and never copy them, which is cheaper.
    // This is not leftover dead code: it exists so that the next caller that
    // does need a copy has a correct path, instead of reaching for the copy
    // constructor above and silently cloning through the cache's shared
    // context. Exercised by GeometryTest.
    Geometry
    Clone(GEOSContextHandle_t ctx) const {
        Geometry cloned;
        cloned.ctx_ = ctx;
        if (IsValid()) {
            cloned.geometry_ = CloneGeometryOrThrow(ctx, geometry_);
        }
        return cloned;
    }

    // Move constructor (transfers ownership, no GEOS allocation). Being
    // noexcept lets std::vector relocate by move instead of clone-then-destroy.
    Geometry(Geometry&& other) noexcept
        : geometry_(other.geometry_), ctx_(other.ctx_) {
        other.geometry_ = nullptr;
    }

    // Move assignment (transfers ownership, releases any existing geometry).
    Geometry&
    operator=(Geometry&& other) noexcept {
        if (this != &other) {
            if (geometry_ != nullptr) {
                GEOSGeom_destroy_r(ctx_, geometry_);
            }
            geometry_ = other.geometry_;
            ctx_ = other.ctx_;
            other.geometry_ = nullptr;
        }
        return *this;
    }

    bool
    IsValid() const {
        return geometry_ != nullptr;
    }

    // Get raw GEOS geometry pointer (for cache management)
    GEOSGeometry*
    GetRawGeometry() const {
        return geometry_;
    }

    GEOSGeometry*
    GetGeometry() const {
        return geometry_;
    }

    // Spatial relation operations using GEOS API.
    //
    // Each predicate has an overload that takes an explicit GEOS context. GEOS
    // context handles are NOT thread-safe, and cache-owned Geometry instances
    // share one context across concurrent queries, so callers on a shared
    // cached geometry MUST pass their own per-thread context (see
    // GetThreadLocalGEOSContext) instead of relying on the stored ctx_.
    // The no-context overloads keep using the instance's own context and are
    // only safe when this instance is not shared across threads.
    bool
    equals(const Geometry& other) const {
        return equals(other, ctx_);
    }

    bool
    equals(const Geometry& other, GEOSContextHandle_t ctx) const {
        if (!IsValid() || !other.IsValid()) {
            return false;
        }
        char result = GEOSEquals_r(ctx, geometry_, other.geometry_);
        return GeosPredicateIsTrue(result, "equals");
    }

    bool
    touches(const Geometry& other) const {
        return touches(other, ctx_);
    }

    bool
    touches(const Geometry& other, GEOSContextHandle_t ctx) const {
        if (!IsValid() || !other.IsValid()) {
            return false;
        }
        char result = GEOSTouches_r(ctx, geometry_, other.geometry_);
        return GeosPredicateIsTrue(result, "touches");
    }

    bool
    overlaps(const Geometry& other) const {
        return overlaps(other, ctx_);
    }

    bool
    overlaps(const Geometry& other, GEOSContextHandle_t ctx) const {
        if (!IsValid() || !other.IsValid()) {
            return false;
        }
        char result = GEOSOverlaps_r(ctx, geometry_, other.geometry_);
        return GeosPredicateIsTrue(result, "overlaps");
    }

    bool
    crosses(const Geometry& other) const {
        return crosses(other, ctx_);
    }

    bool
    crosses(const Geometry& other, GEOSContextHandle_t ctx) const {
        if (!IsValid() || !other.IsValid()) {
            return false;
        }
        char result = GEOSCrosses_r(ctx, geometry_, other.geometry_);
        return GeosPredicateIsTrue(result, "crosses");
    }

    bool
    contains(const Geometry& other) const {
        return contains(other, ctx_);
    }

    bool
    contains(const Geometry& other, GEOSContextHandle_t ctx) const {
        if (!IsValid() || !other.IsValid()) {
            return false;
        }
        char result = GEOSContains_r(ctx, geometry_, other.geometry_);
        return GeosPredicateIsTrue(result, "contains");
    }

    bool
    intersects(const Geometry& other) const {
        return intersects(other, ctx_);
    }

    bool
    intersects(const Geometry& other, GEOSContextHandle_t ctx) const {
        if (!IsValid() || !other.IsValid()) {
            return false;
        }
        char result = GEOSIntersects_r(ctx, geometry_, other.geometry_);
        return GeosPredicateIsTrue(result, "intersects");
    }

    bool
    within(const Geometry& other) const {
        return within(other, ctx_);
    }

    bool
    within(const Geometry& other, GEOSContextHandle_t ctx) const {
        if (!IsValid() || !other.IsValid()) {
            return false;
        }
        char result = GEOSWithin_r(ctx, geometry_, other.geometry_);
        return GeosPredicateIsTrue(result, "within");
    }

    // Distance within check using GEOS distance calculation
    bool
    dwithin(const Geometry& other, double distance) const {
        return dwithin(other, distance, ctx_);
    }

    bool
    dwithin(const Geometry& other,
            double distance,
            GEOSContextHandle_t ctx) const {
        if (!IsValid() || !other.IsValid()) {
            return false;
        }

        // An empty operand is never within any distance of anything (PostGIS
        // ST_DWithin semantics). GEOS reports a distance of 0 to an empty
        // geometry, so without this check every empty row (POINT EMPTY,
        // POLYGON EMPTY, ...) would match at any distance -- and it would do
        // so only on the un-indexed path, because the R-Tree stores a
        // placeholder MBR for empty rows that no query envelope away from the
        // origin ever reaches (issue #54138). Returning false here is what
        // lets the index coarse pass and the exact predicate agree for empty
        // rows regardless of which placeholder an index builder recorded.
        // GEOSisEmpty_r is tri-state (1/0/2 = exception); 2 maps to "not a
        // match" like every other predicate here.
        if (GEOSisEmpty_r(ctx, geometry_) != 0 ||
            GEOSisEmpty_r(ctx, other.geometry_) != 0) {
            return false;
        }

        // Get geometry types
        int thisType = GEOSGeomTypeId_r(ctx, geometry_);
        int otherType = GEOSGeomTypeId_r(ctx, other.geometry_);

        // Ensure other geometry is a point
        AssertInfo(otherType == GEOS_POINT, "other geometry is not a point");

        // For point-to-point, use Haversine formula for accuracy
        if (thisType == GEOS_POINT) {
            double thisX, thisY, otherX, otherY;
            if (GEOSGeomGetX_r(ctx, geometry_, &thisX) == 1 &&
                GEOSGeomGetY_r(ctx, geometry_, &thisY) == 1 &&
                GEOSGeomGetX_r(ctx, other.geometry_, &otherX) == 1 &&
                GEOSGeomGetY_r(ctx, other.geometry_, &otherY) == 1) {
                double actual_distance =
                    haversine_distance_meters(thisY, thisX, otherY, otherX);
                return actual_distance <= distance;
            }
        }

        // For other geometry types, use GEOS distance (in degrees)
        double geos_distance;
        if (GEOSDistance_r(ctx, geometry_, other.geometry_, &geos_distance) ==
            1) {
            // Get query point coordinates for conversion reference
            double query_lat, query_lon;
            if (GEOSGeomGetX_r(ctx, other.geometry_, &query_lon) == 1 &&
                GEOSGeomGetY_r(ctx, other.geometry_, &query_lat) == 1) {
                double distance_in_meters =
                    degrees_to_meters_at_location(geos_distance, query_lat);
                return distance_in_meters <= distance;
            }
        }

        // Reached only when GEOS distance/coordinate extraction failed (those
        // calls return 0 on an exception caught inside GEOS). Same deliberate
        // exception->false mapping as GeosPredicateIsTrue: keep the row-level
        // leniency, but make the failure visible -- and throttled, because
        // this is the same per-candidate-row refine hot path (one ST_DWithin
        // over a segment of degenerate geometries would otherwise emit one
        // line per row).
        static std::atomic<int64_t> last_dwithin_log_us{0};
        if (ShouldLogGeometryThrottled(last_dwithin_log_us)) {
            LOG_WARN(
                "GEOS distance/coordinate extraction failed in dwithin; "
                "treating the row as not matching (further occurrences "
                "suppressed briefly)");
        }
        return false;
    }

    // Axis-aligned lon/lat box, the unit the R-Tree coarse pass works in.
    struct Envelope {
        double min_x;  // longitude
        double min_y;  // latitude
        double max_x;
        double max_y;
    };

    // The coarse search region of one ST_DWITHIN query: the union of `count`
    // boxes (one, or two when the spherical cap wraps around the
    // antimeridian). The R-Tree reader takes one box per lookup, so the
    // caller looks up each box and ORs the candidate bitmaps.
    struct DWithinSearchBoxes {
        static constexpr size_t kMaxBoxes = 2;
        std::array<Envelope, kMaxBoxes> boxes{};
        size_t count = 0;

        const Envelope*
        begin() const {
            return boxes.data();
        }
        const Envelope*
        end() const {
            return boxes.data() + count;
        }
    };

    /**
     * The lon/lat envelope that contains EVERY geometry dwithin() can accept
     * for `query_point` and `distance_meters` -- the coarse pre-filter the
     * R-Tree index runs before exact refinement.
     *
     * dwithin() measures distance two different ways (see above): point rows
     * by great-circle haversine on a sphere of radius kEarthRadiusMeters,
     * every other row by planar GEOS distance in degrees scaled by
     * degrees_to_meters_at_location at the query latitude. A coarse box is
     * only correct if it is a superset of BOTH acceptance regions, so this is
     * the one place that derives it, from the same constants the predicate
     * uses. The historical box (111320 m/deg, longitude / cos(lat)) was
     * sized for neither metric and silently dropped rows near the distance
     * boundary on the indexed path (issue #54138).
     *
     *  - Point rows: the spherical cap of angular radius d/R. Its latitude
     *    extent is exactly +-d/R; its longitude extent is
     *    asin(sin(d/R) / cos(lat)) unless the cap contains a pole, in which
     *    case every longitude is reachable.
     *  - Other rows: a planar disc of radius d / (111320 * f(lat)) degrees in
     *    lon/lat space. Any geometry within that planar distance has a vertex
     *    inside the +-r box, so its envelope intersects the box.
     *
     * The primary box is the union of the two extents plus a tiny margin
     * that absorbs rounding on the boundary. Its longitude range is not
     * clamped to [-180, 180], so it stays a superset of the planar disc.
     *
     * Antimeridian: haversine measures longitude modulo 360, so a spherical
     * cap that crosses +-180 continues on the other side, while the planar
     * GEOS metric does not wrap. Only the cap's overflow is mapped back, as a
     * second box at the opposite edge -- e.g. POINT (179.999 0) with 1000 m
     * yields a sliver on each side of the antimeridian instead of every
     * longitude in the latitude band. A cap that contains a pole is widened
     * to the full [-180, 180] range in one box, as is a query whose own
     * longitude lies outside [-180, 180]. Row coordinates stored outside the
     * valid lon/lat range are not covered (haversine wraps them, an
     * axis-aligned box cannot).
     *
     * Returns false only when the query point has no coordinates (empty
     * geometry); dwithin() then accepts nothing, so the caller should treat
     * the candidate set as empty.
     */
    static bool
    DWithinSearchEnvelope(GEOSContextHandle_t ctx,
                          const GEOSGeometry* query_point,
                          double distance_meters,
                          DWithinSearchBoxes& out) {
        AssertInfo(query_point != nullptr, "query point is null");
        double query_lon = 0, query_lat = 0;
        if (GEOSGeomGetX_r(ctx, query_point, &query_lon) != 1 ||
            GEOSGeomGetY_r(ctx, query_point, &query_lat) != 1) {
            return false;
        }
        const double d = std::max(distance_meters, 0.0);

        // Rounding margin: the exact predicate compares `distance <= d` in
        // meters while the box is in degrees; a row sitting exactly on the
        // boundary must not fall off the box through conversion error.
        constexpr double kRelativeMargin = 1e-9;
        constexpr double kAbsoluteMarginDeg = 1e-9;  // ~0.1 mm

        // (1) point rows: spherical cap under the haversine metric.
        const double angular = d / kEarthRadiusMeters;  // radians
        const double cap_lat_deg = angular * 180.0 / kPi;
        double cap_lon_deg;
        bool full_longitude = false;
        const double lat_rad = query_lat * kPi / 180.0;
        // Both pole tests are made with the margin already applied. When d is
        // the pole distance to the ulp, cap_lat_deg and the sin/cos ratio
        // each land a few ulp below their exact values; asin has infinite
        // slope at 1, so a 1-ulp deficit in the ratio already costs ~1e-6 deg
        // of longitude, far more than a margin added after asin can give
        // back. Padding the inputs instead keeps the box a superset.
        if (std::fabs(query_lat) + cap_lat_deg * (1.0 + kRelativeMargin) +
                    kAbsoluteMarginDeg >=
                90.0 ||
            std::cos(lat_rad) <= 0.0) {
            full_longitude = true;  // cap contains or touches a pole
            cap_lon_deg = 180.0;
        } else {
            const double ratio = std::sin(angular) / std::cos(lat_rad);
            const double padded =
                ratio * (1.0 + kRelativeMargin) + kRelativeMargin;
            if (padded >= 1.0) {
                full_longitude = true;
                cap_lon_deg = 180.0;
            } else {
                cap_lon_deg = std::asin(padded) * 180.0 / kPi;
            }
        }

        // (2) other rows: planar disc in degree space under the scaled GEOS
        // metric. degrees_to_meters_at_location is monotonic in the degree
        // distance, so inverting it at `d` gives the acceptance radius.
        const double meters_per_degree =
            degrees_to_meters_at_location(1.0, query_lat);
        const double planar_deg = d / meters_per_degree;

        auto pad = [](double half_deg) {
            return half_deg * (1.0 + kRelativeMargin) + kAbsoluteMarginDeg;
        };
        const double lat_half = pad(std::max(cap_lat_deg, planar_deg));
        const double lon_half = pad(std::max(cap_lon_deg, planar_deg));

        out = DWithinSearchBoxes{};
        Envelope& primary = out.boxes[out.count++];
        primary.min_y = query_lat - lat_half;
        primary.max_y = query_lat + lat_half;
        primary.min_x = query_lon - lon_half;
        primary.max_x = query_lon + lon_half;
        if (full_longitude || query_lon < -180.0 || query_lon > 180.0) {
            // The cap contains a pole, or the query longitude itself is out
            // of range (haversine still wraps it): every longitude may hold
            // a matching point. Keep any overflow rather than clamping so the
            // box stays a superset of the planar disc as well.
            primary.min_x = std::min(primary.min_x, -180.0);
            primary.max_x = std::max(primary.max_x, 180.0);
            return true;
        }

        // The spherical cap's longitude span [query_lon -+ cap_lon_half] is
        // below 180 deg wide here (asin < 90 deg) and query_lon is in range,
        // so it overflows at most one edge; map that overflow to the other
        // edge. Only point rows are measured by haversine, so the wrapped box
        // takes the cap's own latitude extent, not the planar one.
        const double cap_lon_half = pad(cap_lon_deg);
        const double cap_lat_half = pad(cap_lat_deg);
        double wrapped_min_x = 0, wrapped_max_x = 0;
        if (query_lon + cap_lon_half > 180.0) {
            wrapped_min_x = -180.0;
            wrapped_max_x = query_lon + cap_lon_half - 360.0;
        } else if (query_lon - cap_lon_half < -180.0) {
            wrapped_min_x = query_lon - cap_lon_half + 360.0;
            wrapped_max_x = 180.0;
        } else {
            return true;
        }
        Envelope& wrapped = out.boxes[out.count++];
        wrapped.min_x = wrapped_min_x;
        wrapped.max_x = wrapped_max_x;
        wrapped.min_y = query_lat - cap_lat_half;
        wrapped.max_y = query_lat + cap_lat_half;
        return true;
    }

    bool
    is_valid() const {
        return is_valid(ctx_);
    }

    bool
    is_valid(GEOSContextHandle_t ctx) const {
        if (!IsValid()) {
            return false;
        }
        return GeosPredicateIsTrue(GEOSisValid_r(ctx, geometry_), "is_valid");
    }

 private:
    // Constants shared by the exact dwithin() metrics and the coarse
    // DWithinSearchEnvelope(). Keep them together: the envelope is only a
    // superset of the predicate while both are derived from the same numbers.
    static constexpr double kPi = 3.14159265358979323846;
    static constexpr double kEarthRadiusMeters = 6371000.0;
    static constexpr double kMetersPerDegreeLat = 111320.0;

    // Convert degrees distance to meters using approximate location
    static double
    degrees_to_meters_at_location(double degrees_distance, double center_lat) {
        // For small distances, approximate using latitude-adjusted conversion
        double latRad = center_lat * kPi / 180.0;
        double avgMetersPerDegree =
            kMetersPerDegreeLat *
            std::sqrt((1.0 + std::cos(latRad) * std::cos(latRad)) / 2.0);

        return degrees_distance * avgMetersPerDegree;
    }

 public:
    // Haversine formula to calculate great-circle distance between two points
    // on Earth. This is the exact metric dwithin() applies to point rows; it
    // is public so tests can place rows exactly on the distance boundary.
    static double
    haversine_distance_meters(double lat1,
                              double lon1,
                              double lat2,
                              double lon2) {
        const double R = kEarthRadiusMeters;
        const double PI = kPi;

        // Convert degrees to radians
        double lat1_rad = lat1 * PI / 180.0;
        double lon1_rad = lon1 * PI / 180.0;
        double lat2_rad = lat2 * PI / 180.0;
        double lon2_rad = lon2 * PI / 180.0;

        // Haversine formula
        double dlat = lat2_rad - lat1_rad;
        double dlon = lon2_rad - lon1_rad;

        double a = std::sin(dlat / 2.0) * std::sin(dlat / 2.0) +
                   std::cos(lat1_rad) * std::cos(lat2_rad) *
                       std::sin(dlon / 2.0) * std::sin(dlon / 2.0);
        double c = 2.0 * std::atan2(std::sqrt(a), std::sqrt(1.0 - a));

        return R * c;  // Distance in meters
    }

    // Export to WKT string
    std::string
    to_wkt_string() const {
        if (!IsValid()) {
            return "";
        }

        GEOSWKTWriter* writer = GEOSWKTWriter_create_r(ctx_);
        AssertInfo(writer != nullptr, "Failed to create GEOS WKT writer");

        char* wkt = GEOSWKTWriter_write_r(ctx_, writer, geometry_);
        GEOSWKTWriter_destroy_r(ctx_, writer);

        if (!wkt) {
            return "";
        }

        std::string result(wkt);
        GEOSFree_r(ctx_, wkt);
        return result;
    }

    // Export to WKB string (for test)
    std::string
    to_wkb_string() const {
        if (!IsValid()) {
            return "";
        }

        GEOSWKBWriter* writer = GEOSWKBWriter_create_r(ctx_);
        AssertInfo(writer != nullptr, "Failed to create GEOS WKB writer");

        size_t size;
        unsigned char* wkb =
            GEOSWKBWriter_write_r(ctx_, writer, geometry_, &size);
        GEOSWKBWriter_destroy_r(ctx_, writer);

        if (!wkb) {
            ThrowInfo(UnexpectedError, "Failed to create GEOS WKB writer");
        }

        std::string result(reinterpret_cast<const char*>(wkb), size);
        GEOSFree_r(ctx_, wkb);
        return result;
    }

 private:
    GEOSGeometry* geometry_;  // Raw pointer, managed by cache
    GEOSContextHandle_t ctx_;
};

}  // namespace milvus