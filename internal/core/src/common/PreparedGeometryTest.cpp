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
#include <cmath>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "common/Geometry.h"
#include "common/PreparedGeometry.h"
#include "geos_c.h"
#include "gtest/gtest.h"

namespace milvus {
namespace {

class PreparedGeometryTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        ctx_ = GEOS_init_r();
    }

    void
    TearDown() override {
        GEOS_finish_r(ctx_);
    }

    GEOSContextHandle_t ctx_;
};

TEST_F(PreparedGeometryTest, BasicIntersects) {
    // Create a polygon
    Geometry polygon(ctx_, "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))");
    ASSERT_TRUE(polygon.IsValid());

    // Create prepared geometry from polygon
    PreparedGeometry prepared(ctx_, polygon);
    ASSERT_TRUE(prepared.IsValid());

    // Point inside polygon
    Geometry point_inside(ctx_, "POINT(5 5)");
    EXPECT_TRUE(prepared.intersects(point_inside));

    // Point outside polygon
    Geometry point_outside(ctx_, "POINT(15 15)");
    EXPECT_FALSE(prepared.intersects(point_outside));

    // Point on boundary
    Geometry point_boundary(ctx_, "POINT(0 5)");
    EXPECT_TRUE(prepared.intersects(point_boundary));
}

TEST_F(PreparedGeometryTest, Contains) {
    // Create a polygon
    Geometry polygon(ctx_, "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))");
    PreparedGeometry prepared(ctx_, polygon);

    // Point inside - should be contained
    Geometry point_inside(ctx_, "POINT(5 5)");
    EXPECT_TRUE(prepared.contains(point_inside));

    // Point outside - should not be contained
    Geometry point_outside(ctx_, "POINT(15 15)");
    EXPECT_FALSE(prepared.contains(point_outside));

    // Point on boundary - contains returns false for boundary points
    Geometry point_boundary(ctx_, "POINT(0 5)");
    EXPECT_FALSE(prepared.contains(point_boundary));
}

TEST_F(PreparedGeometryTest, Within) {
    // Create a small polygon inside a larger one
    Geometry large_polygon(ctx_, "POLYGON((0 0, 100 0, 100 100, 0 100, 0 0))");
    Geometry small_polygon(ctx_,
                           "POLYGON((10 10, 20 10, 20 20, 10 20, 10 10))");

    // Prepare the small polygon, test if it's within the large one
    PreparedGeometry prepared_small(ctx_, small_polygon);
    EXPECT_TRUE(prepared_small.within(large_polygon));

    // Prepare the large polygon, test if it's within the small one (should be false)
    PreparedGeometry prepared_large(ctx_, large_polygon);
    EXPECT_FALSE(prepared_large.within(small_polygon));
}

TEST_F(PreparedGeometryTest, PredicateSemantics) {
    // Test that prepared predicates give same results as non-prepared
    // This is important for correctness verification

    Geometry polygon(ctx_, "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))");
    Geometry point(ctx_, "POINT(5 5)");

    PreparedGeometry prepared_polygon(ctx_, polygon);
    PreparedGeometry prepared_point(ctx_, point);

    // intersects is symmetric
    EXPECT_EQ(polygon.intersects(point), prepared_polygon.intersects(point));
    EXPECT_EQ(point.intersects(polygon), prepared_point.intersects(polygon));

    // contains/within are inverses
    // polygon.contains(point) should equal prepared_polygon.contains(point)
    EXPECT_EQ(polygon.contains(point), prepared_polygon.contains(point));

    // point.within(polygon) should equal prepared_point.within(polygon)
    EXPECT_EQ(point.within(polygon), prepared_point.within(polygon));

    // Verify the inverse relationship
    // polygon.contains(point) == point.within(polygon)
    EXPECT_EQ(polygon.contains(point), point.within(polygon));
}

TEST_F(PreparedGeometryTest, PerformanceBenefit) {
    // This test demonstrates the use case: prepare once, test many times
    // Create a complex polygon (many vertices)
    std::string wkt = "POLYGON((";
    for (int i = 0; i < 100; i++) {
        double angle = 2.0 * 3.14159 * i / 100.0;
        double x = 50.0 + 40.0 * cos(angle);
        double y = 50.0 + 40.0 * sin(angle);
        if (i > 0)
            wkt += ", ";
        wkt += std::to_string(x) + " " + std::to_string(y);
    }
    wkt +=
        ", " + std::to_string(50.0 + 40.0) + " " + std::to_string(50.0) + "))";

    Geometry complex_polygon(ctx_, wkt.c_str());
    ASSERT_TRUE(complex_polygon.IsValid());

    PreparedGeometry prepared(ctx_, complex_polygon);
    ASSERT_TRUE(prepared.IsValid());

    // Test many points against the prepared polygon
    int inside_count = 0;
    for (int i = 0; i < 1000; i++) {
        double x = (i % 100);
        double y = (i / 10);
        std::string point_wkt =
            "POINT(" + std::to_string(x) + " " + std::to_string(y) + ")";
        Geometry point(ctx_, point_wkt.c_str());
        if (prepared.intersects(point)) {
            inside_count++;
        }
    }

    // Just verify we got some results (exact count depends on geometry)
    EXPECT_GT(inside_count, 0);
    EXPECT_LT(inside_count, 1000);
}

TEST_F(PreparedGeometryTest, InvalidGeometry) {
    // Test handling of invalid geometry
    PreparedGeometry prepared_default;
    EXPECT_FALSE(prepared_default.IsValid());

    Geometry valid_point(ctx_, "POINT(5 5)");
    EXPECT_FALSE(prepared_default.intersects(valid_point));
    EXPECT_FALSE(prepared_default.contains(valid_point));
}

TEST_F(PreparedGeometryTest, MoveSemantics) {
    Geometry polygon(ctx_, "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))");
    PreparedGeometry prepared1(ctx_, polygon);
    ASSERT_TRUE(prepared1.IsValid());

    // Move construct
    PreparedGeometry prepared2(std::move(prepared1));
    EXPECT_TRUE(prepared2.IsValid());
    EXPECT_FALSE(prepared1.IsValid());  // moved-from should be invalid

    // Move assign
    PreparedGeometry prepared3;
    prepared3 = std::move(prepared2);
    EXPECT_TRUE(prepared3.IsValid());
    EXPECT_FALSE(prepared2.IsValid());

    // Verify the moved geometry still works
    Geometry point(ctx_, "POINT(5 5)");
    EXPECT_TRUE(prepared3.intersects(point));
}

TEST_F(PreparedGeometryTest, AllPredicates) {
    // Test all available prepared predicates
    Geometry polygon(ctx_, "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))");
    Geometry point_inside(ctx_, "POINT(5 5)");
    Geometry point_outside(ctx_, "POINT(15 15)");
    Geometry touching_line(ctx_, "LINESTRING(10 5, 15 5)");

    PreparedGeometry prepared(ctx_, polygon);

    // intersects
    EXPECT_TRUE(prepared.intersects(point_inside));
    EXPECT_FALSE(prepared.intersects(point_outside));

    // contains
    EXPECT_TRUE(prepared.contains(point_inside));
    EXPECT_FALSE(prepared.contains(point_outside));

    // containsProperly
    EXPECT_TRUE(prepared.containsProperly(point_inside));

    // covers
    EXPECT_TRUE(prepared.covers(point_inside));
    EXPECT_FALSE(prepared.covers(point_outside));

    // disjoint (opposite of intersects)
    EXPECT_FALSE(prepared.disjoint(point_inside));
    EXPECT_TRUE(prepared.disjoint(point_outside));

    // touches
    EXPECT_TRUE(prepared.touches(touching_line));
    EXPECT_FALSE(prepared.touches(point_inside));
}

// Tests for GetThreadLocalGEOSContext
TEST(ThreadLocalGEOSContextTest, ReturnsValidContext) {
    GEOSContextHandle_t ctx = GetThreadLocalGEOSContext();
    ASSERT_NE(ctx, nullptr);

    // Verify the context works by creating a geometry
    Geometry point(ctx, "POINT(1 2)");
    EXPECT_TRUE(point.IsValid());
}

TEST(ThreadLocalGEOSContextTest, ReturnsSameContextOnSameThread) {
    GEOSContextHandle_t ctx1 = GetThreadLocalGEOSContext();
    GEOSContextHandle_t ctx2 = GetThreadLocalGEOSContext();
    GEOSContextHandle_t ctx3 = GetThreadLocalGEOSContext();

    // Should return the same context handle each time on the same thread
    EXPECT_EQ(ctx1, ctx2);
    EXPECT_EQ(ctx2, ctx3);
}

TEST(ThreadLocalGEOSContextTest, DifferentThreadsGetDifferentContexts) {
    std::mutex mutex;
    std::condition_variable cv;
    std::set<GEOSContextHandle_t> contexts;
    constexpr int num_threads = 4;
    int ready_count = 0;
    bool all_ready = false;

    auto worker = [&]() {
        GEOSContextHandle_t ctx = GetThreadLocalGEOSContext();

        // Verify context is valid by using it
        Geometry point(ctx, "POINT(5 5)");
        EXPECT_TRUE(point.IsValid());

        {
            std::lock_guard<std::mutex> lock(mutex);
            contexts.insert(ctx);
            ready_count++;
        }
        cv.notify_all();

        // Wait for all threads to record their contexts before exiting
        // This ensures all threads are alive simultaneously
        {
            std::unique_lock<std::mutex> lock(mutex);
            cv.wait(lock, [&] { return ready_count == num_threads; });
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back(worker);
    }

    for (auto& t : threads) {
        t.join();
    }

    // Each thread should have gotten a different context
    EXPECT_EQ(contexts.size(), static_cast<size_t>(num_threads));
}

TEST(ThreadLocalGEOSContextTest, ConcurrentGeometryOperations) {
    // Test that concurrent operations using thread-local contexts don't interfere
    auto worker = [](int thread_id) {
        GEOSContextHandle_t ctx = GetThreadLocalGEOSContext();

        // Each thread creates and tests geometries
        // Use coordinates strictly inside the polygon (avoid boundary)
        for (int i = 0; i < 100; i++) {
            std::string wkt = "POINT(" +
                              std::to_string(thread_id * 10 + i + 1) + " " +
                              std::to_string(i + 1) + ")";
            Geometry point(ctx, wkt.c_str());
            EXPECT_TRUE(point.IsValid());

            Geometry polygon(ctx,
                             "POLYGON((0 0, 1000 0, 1000 1000, 0 1000, 0 0))");
            EXPECT_TRUE(polygon.IsValid());
            EXPECT_TRUE(polygon.contains(point));
        }
    };

    constexpr int num_threads = 4;
    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back(worker, i);
    }

    for (auto& t : threads) {
        t.join();
    }
}

TEST(ThreadLocalGEOSContextTest, ConcurrentPreparedGeometry) {
    // Test PreparedGeometry with thread-local contexts
    auto worker = []() {
        GEOSContextHandle_t ctx = GetThreadLocalGEOSContext();

        Geometry polygon(ctx, "POLYGON((0 0, 100 0, 100 100, 0 100, 0 0))");
        PreparedGeometry prepared(ctx, polygon);
        ASSERT_TRUE(prepared.IsValid());

        for (int i = 0; i < 100; i++) {
            std::string wkt = "POINT(" + std::to_string(i % 50 + 25) + " " +
                              std::to_string(i % 50 + 25) + ")";
            Geometry point(ctx, wkt.c_str());
            EXPECT_TRUE(prepared.contains(point));
        }
    };

    constexpr int num_threads = 4;
    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back(worker);
    }

    for (auto& t : threads) {
        t.join();
    }
}

}  // namespace
}  // namespace milvus
