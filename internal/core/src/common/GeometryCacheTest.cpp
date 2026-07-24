// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "common/GeometryCache.h"

#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <memory>
#include <string>

#include "common/Geometry.h"

namespace milvus::exec {
namespace {

std::string
WkbFromWkt(const char* wkt) {
    auto ctx = GEOS_init_r();
    std::string wkb;
    {
        Geometry geometry(ctx, wkt);
        wkb = geometry.to_wkb_string();
    }
    GEOS_finish_r(ctx);
    return wkb;
}

TEST(SimpleGeometryCache, PreservesSegmentOffsets) {
    auto point = WkbFromWkt("POINT (1 2)");
    auto polygon = WkbFromWkt("POLYGON ((0 0, 3 0, 3 3, 0 3, 0 0))");

    SimpleGeometryCache cache;
    cache.AppendData(point.data(), point.size());
    cache.SetData(3, polygon.data(), polygon.size());

    ASSERT_EQ(cache.Size(), 4);
    auto lock = cache.AcquireReadLock();
    ASSERT_NE(cache.GetByOffsetUnsafe(0), nullptr);
    EXPECT_EQ(cache.GetByOffsetUnsafe(1), nullptr);
    EXPECT_EQ(cache.GetByOffsetUnsafe(2), nullptr);
    ASSERT_NE(cache.GetByOffsetUnsafe(3), nullptr);
    EXPECT_NE(cache.GetByOffsetUnsafe(0)->to_wkt_string().find("POINT"),
              std::string::npos);
}

TEST(SimpleGeometryCache, SharedOwnerControlsLifetime) {
    auto cache = std::make_shared<SimpleGeometryCache>();
    auto point = WkbFromWkt("POINT (0 0)");
    cache->AppendData(point.data(), point.size());

    std::weak_ptr<SimpleGeometryCache> weak = cache;
    std::shared_ptr<const SimpleGeometryCache> snapshot = cache;
    cache.reset();

    ASSERT_FALSE(weak.expired());
    EXPECT_EQ(snapshot->Size(), 1);

    snapshot.reset();
    EXPECT_TRUE(weak.expired());
}

TEST(SimpleGeometryCache, ReadLockAllowsConcurrentQueries) {
    SimpleGeometryCache cache;
    auto first_reader = cache.AcquireReadLock();
    auto second_reader = std::async(std::launch::async, [&cache] {
        auto lock = cache.AcquireReadLock();
        return true;
    });

    EXPECT_EQ(second_reader.wait_for(std::chrono::seconds(1)),
              std::future_status::ready);
    first_reader.unlock();
    EXPECT_TRUE(second_reader.get());
}

}  // namespace
}  // namespace milvus::exec
