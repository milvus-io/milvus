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

#include <gtest/gtest.h>
#include <atomic>
#include <string>
#include <thread>
#include <vector>

#include "common/Geometry.h"
#include "common/GeometryCache.h"
#include "common/Types.h"
#include "geos_c.h"

using milvus::FieldId;
using milvus::Geometry;
using milvus::exec::SimpleGeometryCacheManager;

namespace {

std::string
MakePointWkb(double x, double y) {
    auto ctx = GEOS_init_r();
    std::string wkt =
        "POINT (" + std::to_string(x) + " " + std::to_string(y) + ")";
    std::string wkb = Geometry(ctx, wkt.c_str()).to_wkb_string();
    GEOS_finish_r(ctx);
    return wkb;
}

std::string
MakeWkbFromWkt(const char* wkt) {
    auto ctx = GEOS_init_r();
    std::string wkb = Geometry(ctx, wkt).to_wkb_string();
    GEOS_finish_r(ctx);
    return wkb;
}

// A query holding the cache shared_ptr must keep it (and its geometries) alive
// even when the owning segment is dropped and RemoveSegmentCaches() runs
// concurrently. The pre-fix manager returned a raw pointer into a unique_ptr
// map, so erasing the entry freed the cache under any in-flight reader.
TEST(GeometryCacheLifetime, SharedPtrOutlivesSegmentRemoval) {
    auto& mgr = SimpleGeometryCacheManager::Instance();
    const int64_t seg_id = 900000001;
    const FieldId field_id(101);
    const std::string wkb = MakePointWkb(1.0, 1.0);

    auto cache = mgr.GetOrCreateCache(seg_id, field_id);
    cache->AppendData(wkb.data(), wkb.size());
    ASSERT_EQ(cache->Size(), 1u);

    // Segment torn down: entry removed from the manager map.
    mgr.RemoveSegmentCaches(nullptr, seg_id);
    EXPECT_EQ(mgr.GetCache(seg_id, field_id), nullptr);

    // The cache we still hold remains alive and readable (no use-after-free).
    {
        auto lock = cache->AcquireReadLock();
        const Geometry* g = cache->GetByOffsetUnsafe(0);
        ASSERT_NE(g, nullptr);
        EXPECT_TRUE(g->IsValid());
    }
    // cache drops here -> SimpleGeometryCache destroys its geometries with its
    // own context, independent of the (already gone) segment context.
}

// The cache builds and destroys geometries with its own context; no external
// (segment) context is required to outlive it.
TEST(GeometryCacheLifetime, CacheOwnsItsContext) {
    auto& mgr = SimpleGeometryCacheManager::Instance();
    const int64_t seg_id = 900000002;
    const FieldId field_id(7);
    const std::string wkb = MakePointWkb(2.0, 2.0);

    auto cache = mgr.GetOrCreateCache(seg_id, field_id);
    cache->AppendData(wkb.data(), wkb.size());
    cache->AppendData(nullptr, 0);  // null geometry
    EXPECT_EQ(cache->Size(), 2u);
    {
        auto lock = cache->AcquireReadLock();
        EXPECT_NE(cache->GetByOffsetUnsafe(0), nullptr);
        EXPECT_EQ(cache->GetByOffsetUnsafe(1), nullptr);  // null -> nullptr
    }

    mgr.RemoveSegmentCaches(nullptr, seg_id);
    // Destroying the held cache here must not touch any external context.
}

// Stress: a reader repeatedly fetches and reads the cache while a writer keeps
// re-creating and dropping it. Must not crash / use-after-free (ASAN/TSAN).
TEST(GeometryCacheLifetime, ConcurrentGetAndRemove) {
    auto& mgr = SimpleGeometryCacheManager::Instance();
    const int64_t seg_id = 900000003;
    const FieldId field_id(9);
    const std::string wkb = MakePointWkb(3.0, 3.0);

    std::atomic<bool> stop{false};
    std::thread reader([&]() {
        while (!stop.load()) {
            auto c = mgr.GetCache(seg_id, field_id);
            if (c) {
                auto lock = c->AcquireReadLock();
                // Size() would re-acquire the shared_mutex this thread already
                // holds via the read lock -- recursive shared locking is UB
                // and deadlocks against the concurrently queued writer.
                if (c->SizeUnsafe() > 0) {
                    const Geometry* g = c->GetByOffsetUnsafe(0);
                    (void)g;
                }
            }
        }
    });

    for (int i = 0; i < 300; ++i) {
        auto c = mgr.GetOrCreateCache(seg_id, field_id);
        c->AppendData(wkb.data(), wkb.size());
        mgr.RemoveSegmentCaches(nullptr, seg_id);
    }

    stop.store(true);
    reader.join();
    mgr.RemoveSegmentCaches(nullptr, seg_id);
}

// Regression for PR #50951 review (GeometryCache.h AppendData): a corrupt
// (unparseable, non-empty) WKB row must be cached as an INVALID placeholder
// entry -- readers see nullptr and skip it -- instead of throwing. Before the
// fix AppendData rethrew UnexpectedError, so with the geometry cache enabled a
// single corrupt row failed the entire segment load (LoadFieldData ->
// LoadGeometryCache), the exact row shape the placeholder-MBR write paths
// deliberately keep. Offsets of later rows must stay aligned.
TEST(GeometryCacheLifetime, CorruptWkbCachedAsInvalidPlaceholder) {
    auto& mgr = SimpleGeometryCacheManager::Instance();
    const int64_t seg_id = 900000005;
    const FieldId field_id(13);
    const std::string good = MakePointWkb(4.0, 4.0);
    std::string corrupt = good;
    corrupt.resize(corrupt.size() / 2);  // truncate -> unparseable

    auto cache = mgr.GetOrCreateCache(seg_id, field_id);
    ASSERT_NO_THROW({
        cache->AppendData(good.data(), good.size());
        cache->AppendData(corrupt.data(), corrupt.size());
        cache->AppendData(good.data(), good.size());
    });
    // The corrupt row occupies its offset (no shift of later rows).
    ASSERT_EQ(cache->Size(), 3u);
    {
        auto lock = cache->AcquireReadLock();
        EXPECT_NE(cache->GetByOffsetUnsafe(0), nullptr);
        // Corrupt row -> invalid entry -> nullptr, same contract as null rows;
        // every reader skips it.
        EXPECT_EQ(cache->GetByOffsetUnsafe(1), nullptr);
        EXPECT_NE(cache->GetByOffsetUnsafe(2), nullptr);
    }

    mgr.RemoveSegmentCaches(nullptr, seg_id);
}

// Regression for the shared cache-context concurrency defect: cache-owned
// Geometry instances all carry the cache's single GEOS context, which is not
// thread-safe. The GIS filter path evaluates predicates on those shared
// geometries under a *shared* read lock, so concurrent queries must each drive
// GEOS through their own per-thread context (the context-taking predicate
// overloads) rather than the geometry's stored context. This test mirrors that
// usage: many threads read the same cached geometry at once and evaluate
// predicates on per-thread contexts; results must stay correct.
//
// What each sanitizer can actually prove here (the earlier claim that "ASAN
// surfaces a data race" was wrong -- ASAN is not a race detector): TSAN flags a
// regression back to the shared context directly, as an unsynchronized access.
// ASAN only catches it indirectly, and only once the shared context's mutable
// state (error handler slots, reader scratch buffers) is corrupted badly enough
// to produce a heap error. This suite runs under ASAN in CI; the TSAN evidence
// for these paths is recorded in the PR, produced with a one-off
// thread-sanitized GEOS build (there is no wired TSAN target in the repo yet).
// The cached rows deliberately span every GEOS envelope shape, because the
// envelope is the one piece of geometry state a predicate can WRITE:
// GeometryCollection (and so MULTI* / GEOMETRYCOLLECTION) declares a `mutable
// Envelope` with a lazy getter (GeometryCollection.h:192-197), while Point /
// LineString / Polygon expose theirs read-only. A single-shape (POINT) test
// therefore could not have covered the multi-part path at all.
//
// For the pinned GEOS 3.12.0 the lazy branch turns out to be unreachable for
// parsed geometries -- the primary constructor initializes the envelope eagerly
// (`envelope(computeEnvelopeInternal())`, GeometryCollection.cpp:65), so query
// threads only ever read it -- which is why no writer-side warm-up is needed
// here. These rows pin that: if a future GEOS release makes the getter lazy in
// practice, a TSAN run of this test reports the write.
TEST(GeometryCacheConcurrency, PredicatesUsePerThreadContext) {
    auto& mgr = SimpleGeometryCacheManager::Instance();
    const int64_t seg_id = 900000004;
    const FieldId field_id(11);
    const std::vector<std::string> wkbs = {
        MakePointWkb(1.0, 1.0),
        MakeWkbFromWkt(
            "MULTIPOLYGON(((0 0,0 4,4 4,4 0,0 0)),((6 6,6 8,8 8,8 6,6 6)))"),
        MakeWkbFromWkt("GEOMETRYCOLLECTION(POINT(1 1),LINESTRING(0 0,3 3))"),
        MakeWkbFromWkt("MULTIPOLYGON EMPTY"),
    };
    // Row 0 is the only one equal to the probe point; rows 0-2 all intersect
    // it; the empty row intersects nothing.
    const std::vector<bool> expect_intersects = {true, true, true, false};

    auto cache = mgr.GetOrCreateCache(seg_id, field_id);
    for (const auto& wkb : wkbs) {
        cache->AppendData(wkb.data(), wkb.size());
    }
    ASSERT_EQ(cache->Size(), wkbs.size());

    constexpr int kThreads = 8;
    constexpr int kIters = 5000;
    std::atomic<bool> go{false};
    std::atomic<int> failures{0};

    std::vector<std::thread> workers;
    for (int t = 0; t < kThreads; ++t) {
        workers.emplace_back([&, t]() {
            // Each thread has its own GEOS context and its own query geometries.
            GEOSContextHandle_t ctx = milvus::GetThreadLocalGEOSContext();
            Geometry match(ctx, "POINT (1 1)");
            Geometry miss(ctx, "POINT (9 9)");
            while (!go.load(std::memory_order_relaxed)) {
            }
            for (int i = 0; i < kIters; ++i) {
                auto lock = cache->AcquireReadLock();
                for (size_t off = 0; off < wkbs.size(); ++off) {
                    const Geometry* g = cache->GetByOffsetUnsafe(off);
                    if (g == nullptr) {
                        failures.fetch_add(1, std::memory_order_relaxed);
                        continue;
                    }
                    // Drive predicates on THIS thread's context, not g's stored
                    // (cache-shared) context — exactly what the fixed filter
                    // path does. Results must match the geometry's semantics.
                    bool eq = g->equals(match, ctx);
                    bool inter = g->intersects(match, ctx);
                    bool inter_miss = g->intersects(miss, ctx);
                    if (eq != (off == 0) || inter != expect_intersects[off] ||
                        inter_miss) {
                        failures.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            }
        });
    }

    go.store(true, std::memory_order_relaxed);
    for (auto& w : workers) {
        w.join();
    }

    EXPECT_EQ(failures.load(), 0);
    mgr.RemoveSegmentCaches(nullptr, seg_id);
}

}  // namespace
