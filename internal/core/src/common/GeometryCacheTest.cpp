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
using milvus::exec::GeometryChunkStore;
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
        const Geometry* g = cache->GetByOffset(0);
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
        EXPECT_NE(cache->GetByOffset(0), nullptr);
        EXPECT_EQ(cache->GetByOffset(1), nullptr);  // null -> nullptr
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
                // Reads take no lock at all; the shared_ptr above is the
                // only thing keeping the cache alive under a concurrent
                // RemoveSegmentCaches.
                if (c->Size() > 0) {
                    const Geometry* g = c->GetByOffset(0);
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
        EXPECT_NE(cache->GetByOffset(0), nullptr);
        // Corrupt row -> invalid entry -> nullptr, same contract as null rows;
        // every reader skips it.
        EXPECT_EQ(cache->GetByOffset(1), nullptr);
        EXPECT_NE(cache->GetByOffset(2), nullptr);
    }

    mgr.RemoveSegmentCaches(kInstanceA, seg_id);
}

// Regression for the shared cache-context concurrency defect: cache-owned
// Geometry instances all carry the cache's single GEOS context, which is not
// thread-safe. The GIS filter path evaluates predicates on those shared
// geometries with no lock at all, so concurrent queries must each drive GEOS
// through their own per-thread context (the context-taking predicate
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
                for (size_t off = 0; off < wkbs.size(); ++off) {
                    const Geometry* g = cache->GetByOffset(off);
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
// every subsequent row's absolute offset -- GetByOffset returned the wrong
// geometry with no error. Offset-addressed writes must instead be idempotent:
// re-running the same batch addresses the same slots (the ones it already
// published are simply skipped) and alignment never drifts.
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
        for (size_t i = 0; i < wkbs.size(); ++i) {
            const Geometry* g = cache->GetByOffset(i);
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
        EXPECT_EQ(cache->GetByOffset(0), nullptr);
        EXPECT_EQ(cache->GetByOffset(2), nullptr);
        EXPECT_NE(cache->GetByOffset(3), nullptr);
    }

    // The earlier batch lands afterwards and fills its own slots.
    cache->AppendDataAt(0, early.data(), early.size());
    cache->AppendDataAt(1, nullptr, 0);
    cache->AppendDataAt(2, early.data(), early.size());
    ASSERT_EQ(cache->Size(), 4u);
    {
        EXPECT_NE(cache->GetByOffset(0), nullptr);
        EXPECT_EQ(cache->GetByOffset(1), nullptr);  // real null row
        EXPECT_NE(cache->GetByOffset(2), nullptr);
        EXPECT_NE(cache->GetByOffset(3), nullptr);
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
    { EXPECT_NE(still_there->GetByOffset(0), nullptr); }

    mgr.RemoveSegmentCaches(kInstanceB, seg_id);
}

// GetByOffset must answer "no geometry here" with nullptr rather than
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

    EXPECT_NE(cache->GetByOffset(0), nullptr);
    EXPECT_EQ(cache->GetByOffset(1), nullptr);
    EXPECT_EQ(cache->GetByOffset(1000000), nullptr);
}

// Regression for issue #52191: a continuous stream of overlapping readers must
// never delay a writer.
//
// SimpleGeometryCache is read by every GIS expression (GISFunctionFilterExpr /
// GISConjunctExpr walk it row by row for a whole batch) while the
// growing-segment insert path writes through AppendDataAt(). The cache used to
// serialize the two on one rwlock, with readers holding the shared side across
// a whole batch; under sustained query load the reader count essentially never
// dropped to zero, and on a reader-preferring rwlock -- which is what
// libstdc++'s std::shared_mutex maps to on Linux, since glibc's
// pthread_rwlock_t defaults to PTHREAD_RWLOCK_PREFER_READER_NP -- the writer
// was never admitted and the insert stalled indefinitely.
//
// Reads are now lock-free (chunked storage that never relocates, slots
// published with a release store), and the writer's mutex is one no reader
// ever takes, so the two paths do not interact at all. The readers below
// deliberately hammer the read path for the whole duration of the write.
//
// Readers are stopped BEFORE joining the writer, so a blocked writer makes this
// test fail on the elapsed-time assertion instead of hanging forever.
TEST(GeometryCacheConcurrency, WriterIsNotStarvedByOverlappingReaders) {
    auto& mgr = SimpleGeometryCacheManager::Instance();
    const int64_t seg_id = 900000011;
    const FieldId field_id(31);
    const std::string wkb = MakePointWkb(7.0, 7.0);

    auto cache = mgr.GetOrCreateCache(kInstanceA, seg_id, field_id);
    cache->AppendDataAt(0, wkb.data(), wkb.size());

    constexpr int kReaders = 8;
    // Generous: the writer takes an uncontended mutex and one release store,
    // so this only trips on genuine starvation, not on a loaded CI machine.
    constexpr auto kWriterBudget = std::chrono::seconds(5);

    std::atomic<bool> stop{false};
    std::atomic<int> readers_running{0};
    std::vector<std::thread> readers;
    readers.reserve(kReaders);
    for (int i = 0; i < kReaders; ++i) {
        readers.emplace_back([&]() {
            readers_running.fetch_add(1);
            while (!stop.load(std::memory_order_relaxed)) {
                // Mirror the expression paths: a long run of per-row reads,
                // which used to be one long-held shared lock.
                for (int k = 0; k < 64; ++k) {
                    const Geometry* g = cache->GetByOffset(0);
                    (void)g;
                }
            }
        });
    }

    // Let the readers reach steady state so the read path is genuinely busy.
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

    // Stop the readers first: with the old rwlock even a starved writer
    // completes once they drain, so join() cannot hang regardless of the
    // outcome asserted below.
    stop.store(true, std::memory_order_relaxed);
    writer.join();
    for (auto& t : readers) {
        t.join();
    }

    EXPECT_TRUE(acquired)
        << "writer did not publish within "
        << std::chrono::duration_cast<std::chrono::milliseconds>(kWriterBudget)
               .count()
        << " ms while " << kReaders
        << " readers hammered the read path -- the writer is being starved "
           "(issue #52191)";
    EXPECT_LT(
        std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(),
        std::chrono::duration_cast<std::chrono::milliseconds>(kWriterBudget)
            .count());
    EXPECT_NE(cache->GetByOffset(1), nullptr);

    mgr.RemoveSegmentCaches(kInstanceA, seg_id);
}

// The other half of #52191: a writer must not delay readers either. Readers
// run while a writer appends thousands of rows, and every geometry a reader
// does observe must be fully built -- a torn or half-published slot would show
// up here as a predicate mismatch (and as a data race under TSAN).
TEST(GeometryCacheConcurrency, ReadersNeverObserveAPartiallyPublishedSlot) {
    auto& mgr = SimpleGeometryCacheManager::Instance();
    const int64_t seg_id = 900000012;
    const FieldId field_id(32);
    const std::string wkb = MakePointWkb(1.0, 1.0);

    // Spans several chunks, so the readers race chunk allocation too.
    constexpr size_t kRows = GeometryChunkStore::kChunkSize * 3 + 7;
    constexpr int kReaders = 4;

    auto cache = mgr.GetOrCreateCache(kInstanceA, seg_id, field_id);

    std::atomic<bool> stop{false};
    std::atomic<int> failures{0};
    std::vector<std::thread> readers;
    readers.reserve(kReaders);
    for (int i = 0; i < kReaders; ++i) {
        readers.emplace_back([&]() {
            GEOSContextHandle_t ctx = milvus::GetThreadLocalGEOSContext();
            Geometry probe(ctx, "POINT (1 1)");
            while (!stop.load(std::memory_order_relaxed)) {
                for (size_t off = 0; off < kRows; ++off) {
                    const Geometry* g = cache->GetByOffset(off);
                    if (g == nullptr) {
                        continue;  // not published yet: legitimate
                    }
                    if (!g->equals(probe, ctx)) {
                        failures.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            }
        });
    }

    // First every row lands unparseable (what a swallowed parse-time OOM
    // looks like), then the "retry" writes the real WKB over the same offsets.
    // Readers race both passes, including the heal of each unpublished slot.
    std::string corrupt = wkb;
    corrupt.resize(corrupt.size() / 2);
    for (size_t off = 0; off < kRows; ++off) {
        cache->AppendDataAt(off, corrupt.data(), corrupt.size());
    }
    for (size_t off = 0; off < kRows; ++off) {
        cache->AppendDataAt(off, wkb.data(), wkb.size());
    }

    stop.store(true, std::memory_order_relaxed);
    for (auto& t : readers) {
        t.join();
    }

    EXPECT_EQ(failures.load(), 0);
    EXPECT_EQ(cache->Size(), kRows);
    for (size_t off = 0; off < kRows; ++off) {
        ASSERT_NE(cache->GetByOffset(off), nullptr) << "offset " << off;
    }

    mgr.RemoveSegmentCaches(kInstanceA, seg_id);
}

// Lock-free reads are only sound because a published slot is frozen: its
// address never moves (chunks are never relocated) and its contents are never
// rewritten. Both halves are pinned here.
TEST(GeometryCacheLifetime, PublishedSlotsAreFrozenAndStable) {
    auto& mgr = SimpleGeometryCacheManager::Instance();
    const int64_t seg_id = 900000013;
    const FieldId field_id(33);
    const std::string first = MakePointWkb(1.0, 1.0);
    const std::string second = MakePointWkb(2.0, 2.0);

    auto cache = mgr.GetOrCreateCache(kInstanceA, seg_id, field_id);
    cache->AppendDataAt(0, first.data(), first.size());
    const Geometry* pinned = cache->GetByOffset(0);
    ASSERT_NE(pinned, nullptr);

    GEOSContextHandle_t ctx = milvus::GetThreadLocalGEOSContext();
    Geometry expected(ctx, "POINT (1 1)");
    ASSERT_TRUE(pinned->equals(expected, ctx));

    // FIRST VALID WRITER WINS: an absolute offset denotes one immutable row
    // and a successful parse is a pure function of its bytes, so a second
    // write is skipped rather than mutating a slot a reader may be holding.
    cache->AppendDataAt(0, second.data(), second.size());
    EXPECT_EQ(cache->GetByOffset(0), pinned);
    EXPECT_TRUE(pinned->equals(expected, ctx));

    // ...and neither does an invalid/null rewrite.
    cache->AppendDataAt(0, nullptr, 0);
    EXPECT_EQ(cache->GetByOffset(0), pinned);
    EXPECT_TRUE(pinned->equals(expected, ctx));

    // Growing past several chunks must not move the slot pinned above, which
    // a std::vector<Geometry> + resize() would have done on every growth.
    for (size_t off = 1; off <= GeometryChunkStore::kChunkSize * 2; ++off) {
        cache->AppendDataAt(off, first.data(), first.size());
    }
    EXPECT_EQ(cache->GetByOffset(0), pinned);
    EXPECT_TRUE(pinned->equals(expected, ctx));

    mgr.RemoveSegmentCaches(kInstanceA, seg_id);
}

// An unparseable outcome is NOT frozen. GEOS swallows an OOM inside
// GEOSWKBReader_read_r and returns nullptr, so a transient failure looks
// exactly like corrupt WKB (the KNOWN LIMIT note on TryParseFromWkb). When a
// later row of the same batch throws a retriable MemAllocateFailed, the batch
// is retried over the same absolute offsets; that retry must re-parse the
// earlier row and heal it. Freezing the invalid outcome would silently drop
// the row from every ST_* result for the segment's lifetime (and flip it to a
// false positive under a negated predicate) -- a regression against the
// unconditional overwrite the cache used before it went lock-free.
TEST(GeometryCacheLifetime, InvalidRowsAreRewrittenByARetry) {
    auto& mgr = SimpleGeometryCacheManager::Instance();
    const int64_t seg_id = 900000015;
    const FieldId field_id(35);
    const std::string good = MakePointWkb(3.0, 4.0);
    std::string transient = good;
    transient.resize(transient.size() / 2);  // parses to nullptr, like an OOM

    auto cache = mgr.GetOrCreateCache(kInstanceA, seg_id, field_id);

    // First attempt: row 0 hits the "transient" failure, row 1 is null.
    cache->AppendDataAt(0, transient.data(), transient.size());
    cache->AppendDataAt(1, nullptr, 0);
    EXPECT_EQ(cache->GetByOffset(0), nullptr);
    EXPECT_EQ(cache->GetByOffset(1), nullptr);
    EXPECT_EQ(cache->Size(), 2u);

    // The retried batch re-derives the real bytes for both offsets.
    cache->AppendDataAt(0, good.data(), good.size());
    cache->AppendDataAt(1, good.data(), good.size());

    GEOSContextHandle_t ctx = milvus::GetThreadLocalGEOSContext();
    Geometry expected(ctx, "POINT (3 4)");
    for (size_t off : {size_t{0}, size_t{1}}) {
        const Geometry* g = cache->GetByOffset(off);
        ASSERT_NE(g, nullptr) << "offset " << off << " was not healed";
        EXPECT_TRUE(g->equals(expected, ctx)) << "offset " << off;
    }
    EXPECT_EQ(cache->Size(), 2u);

    // Once healed, the slot is frozen like any other valid slot.
    const Geometry* healed = cache->GetByOffset(0);
    cache->AppendDataAt(0, transient.data(), transient.size());
    EXPECT_EQ(cache->GetByOffset(0), healed);

    mgr.RemoveSegmentCaches(kInstanceA, seg_id);
}

// Absolute offsets are sparse in practice (a batch reserves a range and may
// land before an earlier one), and the chunk directory grows in geometric
// slabs. Write across both boundaries and check that gaps read as "no geometry
// here" while every written row survives.
TEST(GeometryCacheLifetime, SparseOffsetsAcrossChunkAndSlabBoundaries) {
    auto& mgr = SimpleGeometryCacheManager::Instance();
    const int64_t seg_id = 900000014;
    const FieldId field_id(34);
    const std::string wkb = MakePointWkb(5.0, 5.0);

    constexpr size_t kChunk = GeometryChunkStore::kChunkSize;
    const std::vector<size_t> offsets = {
        0,
        kChunk - 1,
        kChunk,         // second chunk, first slab
        kChunk * 17,    // second slab (slab 0 holds 16 chunks)
        kChunk * 496,   // sixth slab (slabs 0..4 hold 496 chunks)
        kChunk * 1000,  // and well past it
    };

    auto cache = mgr.GetOrCreateCache(kInstanceA, seg_id, field_id);
    // Deliberately out of order, newest offset first.
    for (auto it = offsets.rbegin(); it != offsets.rend(); ++it) {
        cache->AppendDataAt(*it, wkb.data(), wkb.size());
    }

    EXPECT_EQ(cache->Size(), offsets.back() + 1);
    for (size_t off : offsets) {
        ASSERT_NE(cache->GetByOffset(off), nullptr) << "offset " << off;
    }
    // Gaps, including ones inside an allocated chunk and ones in a slab that
    // was never allocated at all.
    for (size_t off : {size_t{1},
                       kChunk + 1,
                       kChunk * 17 + 1,
                       kChunk * 2,
                       kChunk * 600,
                       kChunk * 1000 + 1}) {
        EXPECT_EQ(cache->GetByOffset(off), nullptr) << "offset " << off;
    }
    // Far outside the addressable range: still an answer, never a throw.
    EXPECT_EQ(cache->GetByOffset(~size_t{0}), nullptr);

    mgr.RemoveSegmentCaches(kInstanceA, seg_id);
}

}  // namespace
