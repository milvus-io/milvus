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
                if (c->Size() > 0) {
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

}  // namespace
