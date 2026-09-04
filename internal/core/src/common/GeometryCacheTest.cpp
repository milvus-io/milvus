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
#include <chrono>
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

// Two distinct segment OBJECTS; in production these come from
// SegmentInternalInterface::segment_instance_uid().
constexpr uint64_t kInstanceA = 1;
constexpr uint64_t kInstanceB = 2;

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

    auto cache = mgr.GetOrCreateCache(kInstanceA, seg_id, field_id);
    cache->AppendDataAt(0, wkb.data(), wkb.size());
    ASSERT_EQ(cache->Size(), 1u);

    // Segment torn down: entry removed from the manager map.
    mgr.RemoveSegmentCaches(kInstanceA, seg_id);
    EXPECT_EQ(mgr.GetCache(kInstanceA, seg_id, field_id), nullptr);

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

    auto cache = mgr.GetOrCreateCache(kInstanceA, seg_id, field_id);
    cache->AppendDataAt(0, wkb.data(), wkb.size());
    cache->AppendDataAt(1, nullptr, 0);  // null geometry
    EXPECT_EQ(cache->Size(), 2u);
    {
        auto lock = cache->AcquireReadLock();
        EXPECT_NE(cache->GetByOffsetUnsafe(0), nullptr);
        EXPECT_EQ(cache->GetByOffsetUnsafe(1), nullptr);  // null -> nullptr
    }

    mgr.RemoveSegmentCaches(kInstanceA, seg_id);
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
            auto c = mgr.GetCache(kInstanceA, seg_id, field_id);
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
        auto c = mgr.GetOrCreateCache(kInstanceA, seg_id, field_id);
        c->AppendDataAt(0, wkb.data(), wkb.size());
        mgr.RemoveSegmentCaches(kInstanceA, seg_id);
    }

    stop.store(true);
    reader.join();
    mgr.RemoveSegmentCaches(kInstanceA, seg_id);
}

// Regression for PR #50951 review (GeometryCache.h AppendDataAt): a corrupt
// (unparseable, non-empty) WKB row must be cached as an INVALID placeholder
// entry -- readers see nullptr and skip it -- instead of throwing. Before the
// fix AppendDataAt rethrew UnexpectedError, so with the geometry cache enabled a
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

    auto cache = mgr.GetOrCreateCache(kInstanceA, seg_id, field_id);
    ASSERT_NO_THROW({
        cache->AppendDataAt(0, good.data(), good.size());
        cache->AppendDataAt(1, corrupt.data(), corrupt.size());
        cache->AppendDataAt(2, good.data(), good.size());
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

    mgr.RemoveSegmentCaches(kInstanceA, seg_id);
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

    auto cache = mgr.GetOrCreateCache(kInstanceA, seg_id, field_id);
    for (size_t i = 0; i < wkbs.size(); ++i) {
        cache->AppendDataAt(i, wkbs[i].data(), wkbs[i].size());
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
    mgr.RemoveSegmentCaches(kInstanceA, seg_id);
}

// Regression for PR #50951 review (round Df4a298c5f4): the cache is published
// in the manager map before it is populated, and AppendDataAt can throw a
// retriable MemAllocateFailed mid-batch. With the old tail append, a retry
// after such a partial write appended AFTER the leftover prefix, shifting
// every subsequent row's absolute offset -- GetByOffsetUnsafe returned the
// wrong geometry with no error. Offset-addressed writes must instead be
// idempotent: re-running the same batch overwrites the same slots and
// alignment never drifts.
TEST(GeometryCacheLifetime, RetryAfterPartialWriteKeepsOffsetsAligned) {
    auto& mgr = SimpleGeometryCacheManager::Instance();
    const int64_t seg_id = 900000007;
    const FieldId field_id(17);
    const std::vector<std::string> wkbs = {
        MakePointWkb(0.0, 0.0),
        MakePointWkb(1.0, 1.0),
        MakePointWkb(2.0, 2.0),
        MakePointWkb(3.0, 3.0),
    };

    auto cache = mgr.GetOrCreateCache(kInstanceA, seg_id, field_id);
    // Simulate a first attempt that dies mid-batch (rows 0-1 written, then a
    // retriable throw before rows 2-3).
    cache->AppendDataAt(0, wkbs[0].data(), wkbs[0].size());
    cache->AppendDataAt(1, wkbs[1].data(), wkbs[1].size());

    // The retry re-runs the WHOLE batch from row 0, exactly like a re-driven
    // LoadGeometryCache/BuildGeometryCacheFor{Load,Insert} would.
    for (size_t i = 0; i < wkbs.size(); ++i) {
        cache->AppendDataAt(i, wkbs[i].data(), wkbs[i].size());
    }

    // No duplicated prefix, no shifted offsets: row i still holds point (i,i).
    ASSERT_EQ(cache->Size(), wkbs.size());
    auto ctx = GEOS_init_r();
    {
        auto lock = cache->AcquireReadLock();
        for (size_t i = 0; i < wkbs.size(); ++i) {
            const Geometry* g = cache->GetByOffsetUnsafe(i);
            ASSERT_NE(g, nullptr) << "offset " << i;
            Geometry probe(
                ctx,
                ("POINT (" + std::to_string(i) + " " + std::to_string(i) + ")")
                    .c_str());
            EXPECT_TRUE(g->equals(probe, ctx)) << "offset " << i;
        }
    }
    GEOS_finish_r(ctx);
    mgr.RemoveSegmentCaches(kInstanceA, seg_id);
}

// Offset-addressed writes tolerate out-of-order arrival: a later batch may
// land before an earlier one (or an earlier batch may have failed and not yet
// been retried). Slots that were skipped over stay default-invalid -- readers
// see nullptr and skip the row -- and are filled in place once their write
// arrives.
TEST(GeometryCacheLifetime, OutOfOrderWritesFillGapsInPlace) {
    auto& mgr = SimpleGeometryCacheManager::Instance();
    const int64_t seg_id = 900000008;
    const FieldId field_id(19);
    const std::string early = MakePointWkb(1.0, 1.0);
    const std::string late = MakePointWkb(9.0, 9.0);

    auto cache = mgr.GetOrCreateCache(kInstanceA, seg_id, field_id);
    // Row 3 arrives first; rows 0-2 are still gaps.
    cache->AppendDataAt(3, late.data(), late.size());
    ASSERT_EQ(cache->Size(), 4u);
    {
        auto lock = cache->AcquireReadLock();
        EXPECT_EQ(cache->GetByOffsetUnsafe(0), nullptr);
        EXPECT_EQ(cache->GetByOffsetUnsafe(2), nullptr);
        EXPECT_NE(cache->GetByOffsetUnsafe(3), nullptr);
    }

    // The earlier batch lands afterwards and fills its own slots.
    cache->AppendDataAt(0, early.data(), early.size());
    cache->AppendDataAt(1, nullptr, 0);
    cache->AppendDataAt(2, early.data(), early.size());
    ASSERT_EQ(cache->Size(), 4u);
    {
        auto lock = cache->AcquireReadLock();
        EXPECT_NE(cache->GetByOffsetUnsafe(0), nullptr);
        EXPECT_EQ(cache->GetByOffsetUnsafe(1), nullptr);  // real null row
        EXPECT_NE(cache->GetByOffsetUnsafe(2), nullptr);
        EXPECT_NE(cache->GetByOffsetUnsafe(3), nullptr);
    }
    mgr.RemoveSegmentCaches(kInstanceA, seg_id);
}

// Regression for PR #50951 review (rounds D604486a968 and Dfc0be51e9e): two
// live segment OBJECTS can carry the same logical segment id -- a growing and
// a sealed twin during handoff, and two sealed instances of different versions
// while the replaced one is released asynchronously
// (querynodev2/segments/manager.go:409-441). Keying the cache on the segment
// id (with or without the segment type) makes the arriving instance reuse the
// departing instance's entry, and the departing instance's destructor then
// erases the cache the still-serving instance depends on -- silently degrading
// every later GIS query on it to per-row WKB re-parsing, with no rebuild path.
// Keying on segment_instance_uid covers both shapes; this test drives the
// same-type (version replacement) one, which the type-bearing key missed.
TEST(GeometryCacheLifetime, SameSegmentIdDifferentInstancesDoNotShareOrEvict) {
    auto& mgr = SimpleGeometryCacheManager::Instance();
    const int64_t seg_id = 900000009;
    const FieldId field_id(23);
    const std::string old_wkb = MakePointWkb(1.0, 1.0);
    const std::string new_wkb = MakePointWkb(2.0, 2.0);

    auto departing = mgr.GetOrCreateCache(kInstanceA, seg_id, field_id);
    auto arriving = mgr.GetOrCreateCache(kInstanceB, seg_id, field_id);
    // Same logical segment id and field, different objects -> distinct caches
    // (the arriving instance must NOT reuse the departing one's entry).
    EXPECT_NE(departing.get(), arriving.get());

    departing->AppendDataAt(0, old_wkb.data(), old_wkb.size());
    arriving->AppendDataAt(0, new_wkb.data(), new_wkb.size());

    // The replaced instance is released asynchronously while the new one is
    // already serving; its teardown must not touch the new one's cache.
    mgr.RemoveSegmentCaches(kInstanceA, seg_id);
    EXPECT_EQ(mgr.GetCache(kInstanceA, seg_id, field_id), nullptr);
    auto still_there = mgr.GetCache(kInstanceB, seg_id, field_id);
    ASSERT_NE(still_there, nullptr);
    EXPECT_EQ(still_there.get(), arriving.get());
    {
        auto lock = still_there->AcquireReadLock();
        EXPECT_NE(still_there->GetByOffsetUnsafe(0), nullptr);
    }

    mgr.RemoveSegmentCaches(kInstanceB, seg_id);
}

// GetByOffsetUnsafe must answer "no geometry here" with nullptr rather than
// throwing: on a growing segment the R-Tree is fed before the cache, so a
// concurrent query sized by the index Count() can legitimately probe an offset
// the cache has not reached yet. Throwing a non-retriable UnexpectedError out
// of that read path would fail the whole query.
TEST(GeometryCacheLifetime, OutOfRangeOffsetReturnsNullptrNotThrow) {
    auto& mgr = SimpleGeometryCacheManager::Instance();
    const int64_t seg_id = 900000010;
    const FieldId field_id(29);
    const std::string wkb = MakePointWkb(3.0, 3.0);

    auto cache = mgr.GetOrCreateCache(kInstanceA, seg_id, field_id);
    cache->AppendDataAt(0, wkb.data(), wkb.size());

    auto lock = cache->AcquireReadLock();
    EXPECT_NE(cache->GetByOffsetUnsafe(0), nullptr);
    EXPECT_EQ(cache->GetByOffsetUnsafe(1), nullptr);
    EXPECT_EQ(cache->GetByOffsetUnsafe(1000000), nullptr);
}

// Regression for issue #52191: the cache's shared_mutex must not let a
// continuous stream of overlapping readers starve a writer.
//
// SimpleGeometryCache is read under AcquireReadLock() by every GIS expression
// (GISFunctionFilterExpr / GISConjunctExpr hold it across a whole batch), while
// the growing-segment insert path writes through AppendDataAt(). With a
// reader-preferring rwlock -- which is what libstdc++'s std::shared_mutex maps
// to on Linux, since glibc's pthread_rwlock_t defaults to
// PTHREAD_RWLOCK_PREFER_READER_NP -- a writer only ever acquires during a
// window in which the reader count drops to zero. Under sustained query load
// that window may never occur, and the insert stalls indefinitely.
//
// The readers below deliberately overlap: each re-acquires immediately after
// releasing, so with enough of them the reader count is essentially never zero.
// Readers are stopped BEFORE joining the writer, so a starved writer makes this
// test fail on the elapsed-time assertion instead of hanging forever.
TEST(GeometryCacheConcurrency, WriterIsNotStarvedByOverlappingReaders) {
    auto& mgr = SimpleGeometryCacheManager::Instance();
    const int64_t seg_id = 900000011;
    const FieldId field_id(31);
    const std::string wkb = MakePointWkb(7.0, 7.0);

    auto cache = mgr.GetOrCreateCache(kInstanceA, seg_id, field_id);
    cache->AppendDataAt(0, wkb.data(), wkb.size());

    constexpr int kReaders = 8;
    // Generous: with a write-preferring lock the writer is admitted in about a
    // millisecond, so this only trips on genuine starvation, not on a loaded
    // CI machine.
    constexpr auto kWriterBudget = std::chrono::seconds(5);

    std::atomic<bool> stop{false};
    std::atomic<int> readers_running{0};
    std::vector<std::thread> readers;
    readers.reserve(kReaders);
    for (int i = 0; i < kReaders; ++i) {
        readers.emplace_back([&]() {
            readers_running.fetch_add(1);
            while (!stop.load(std::memory_order_relaxed)) {
                auto lock = cache->AcquireReadLock();
                // Mirror the expression paths: real work is done while the
                // read lock is held, so the lock is held for a while.
                for (int k = 0; k < 64; ++k) {
                    const Geometry* g = cache->GetByOffsetUnsafe(0);
                    (void)g;
                }
            }
        });
    }

    // Let the readers reach steady state so the reader count stays above zero.
    while (readers_running.load() < kReaders) {
        std::this_thread::yield();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    std::atomic<bool> write_done{false};
    auto started = std::chrono::steady_clock::now();
    std::thread writer([&]() {
        cache->AppendDataAt(1, wkb.data(), wkb.size());
        write_done.store(true, std::memory_order_relaxed);
    });

    while (!write_done.load(std::memory_order_relaxed) &&
           std::chrono::steady_clock::now() - started < kWriterBudget) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    auto elapsed = std::chrono::steady_clock::now() - started;
    bool acquired = write_done.load(std::memory_order_relaxed);

    // Stop the readers first: once they drain, even a starved writer completes,
    // so join() cannot hang regardless of the outcome asserted below.
    stop.store(true, std::memory_order_relaxed);
    writer.join();
    for (auto& t : readers) {
        t.join();
    }

    EXPECT_TRUE(acquired)
        << "writer did not acquire the cache lock within "
        << std::chrono::duration_cast<std::chrono::milliseconds>(kWriterBudget)
               .count()
        << " ms while " << kReaders
        << " overlapping readers held the shared lock -- the writer is being "
           "starved (issue #52191)";
    EXPECT_LT(
        std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(),
        std::chrono::duration_cast<std::chrono::milliseconds>(kWriterBudget)
            .count());

    mgr.RemoveSegmentCaches(kInstanceA, seg_id);
}

}  // namespace
