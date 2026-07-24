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
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/Geometry.h"
#include "geos_c.h"

using milvus::Geometry;

namespace {

class GeometryValueSemanticsTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        ctx_ = GEOS_init_r();
    }
    void
    TearDown() override {
        GEOS_finish_r(ctx_);
    }
    GEOSContextHandle_t ctx_{nullptr};
};

// Copy-assignment must deep-clone. The pre-fix implementation copied the raw
// GEOSGeometry* shallowly, so the source and destination shared one pointer
// and the second destructor double-freed it (caught here by ASAN; without ASAN
// the shallow copy still leaks the destination's previous geometry).
TEST_F(GeometryValueSemanticsTest, CopyAssignmentDeepClones) {
    Geometry a(ctx_, "POINT (1 2)");
    Geometry b(ctx_, "POINT (3 4)");
    ASSERT_TRUE(a.IsValid());
    ASSERT_TRUE(b.IsValid());

    b = a;  // must clone a into b and release b's previous geometry

    ASSERT_TRUE(b.IsValid());
    // Independent underlying geometries, never a shared raw pointer.
    EXPECT_NE(a.GetRawGeometry(), b.GetRawGeometry());
    EXPECT_EQ(a.to_wkb_string(), b.to_wkb_string());
    // Both destruct at scope end and must each free exactly once.
}

TEST_F(GeometryValueSemanticsTest, SelfCopyAssignmentIsSafe) {
    Geometry a(ctx_, "POINT (7 8)");
    const std::string before = a.to_wkb_string();
    Geometry& alias = a;
    a = alias;  // self-assignment must not destroy its own geometry
    ASSERT_TRUE(a.IsValid());
    EXPECT_EQ(a.to_wkb_string(), before);
}

// Clone(ctx) is the query-thread-safe duplication path: all GEOS work runs
// through the caller-supplied context and the clone is bound to it, so a
// cache-owned Geometry can be duplicated without touching the cache's shared
// (non-thread-safe) context and the clone survives the cache.
TEST_F(GeometryValueSemanticsTest, CloneDeepClonesIntoCallerContext) {
    Geometry a(ctx_, "POINT (1 2)");
    ASSERT_TRUE(a.IsValid());

    GEOSContextHandle_t other_ctx = GEOS_init_r();
    ASSERT_NE(other_ctx, nullptr);
    {
        Geometry b = a.Clone(other_ctx);

        ASSERT_TRUE(b.IsValid());
        // Independent underlying geometries, never a shared raw pointer.
        EXPECT_NE(a.GetRawGeometry(), b.GetRawGeometry());
        EXPECT_EQ(a.to_wkb_string(), b.to_wkb_string());
        // b destructs here, via other_ctx; a destructs later via ctx_. Each
        // must free exactly once.
    }
    GEOS_finish_r(other_ctx);
    ASSERT_TRUE(a.IsValid());
}

TEST_F(GeometryValueSemanticsTest, CloneOfInvalidGeometryIsInvalid) {
    Geometry a;
    ASSERT_FALSE(a.IsValid());
    Geometry b = a.Clone(ctx_);
    EXPECT_FALSE(b.IsValid());
    EXPECT_EQ(b.GetRawGeometry(), nullptr);
}

TEST_F(GeometryValueSemanticsTest, MoveConstructorTransfersOwnership) {
    Geometry a(ctx_, "POINT (5 6)");
    GEOSGeometry* raw = a.GetRawGeometry();

    Geometry b(std::move(a));

    EXPECT_FALSE(a.IsValid());  // moved-from is emptied
    EXPECT_EQ(a.GetRawGeometry(), nullptr);
    ASSERT_TRUE(b.IsValid());
    EXPECT_EQ(b.GetRawGeometry(), raw);  // pointer transferred, not cloned
}

TEST_F(GeometryValueSemanticsTest, MoveAssignmentReleasesExisting) {
    Geometry a(ctx_, "POINT (5 6)");
    GEOSGeometry* raw = a.GetRawGeometry();
    Geometry b(ctx_, "POINT (9 9)");

    b = std::move(a);  // frees b's old geometry, takes a's

    EXPECT_FALSE(a.IsValid());
    ASSERT_TRUE(b.IsValid());
    EXPECT_EQ(b.GetRawGeometry(), raw);
}

// std::vector reallocation must relocate Geometry elements safely. With the
// noexcept move constructor it relocates by move; either way no element's
// GEOSGeometry* may be freed twice.
TEST_F(GeometryValueSemanticsTest, VectorGrowthDoesNotDoubleFree) {
    std::vector<Geometry> v;
    for (int i = 0; i < 1000; ++i) {
        std::string wkt =
            "POINT (" + std::to_string(i) + " " + std::to_string(i) + ")";
        v.emplace_back(ctx_, wkt.c_str());
    }
    ASSERT_EQ(v.size(), 1000u);
    EXPECT_TRUE(v.front().IsValid());
    EXPECT_TRUE(v.back().IsValid());
    EXPECT_NE(v.front().GetRawGeometry(), v.back().GetRawGeometry());
}

// --- TryParseFromWkb classification boundary (see PR #50951 review) ---
//
// The contract splits failures into two buckets: bad data returns false
// (caller skips the row), a transient resource failure observable BEFORE
// parsing throws a retriable SegcoreError (the row must not be silently
// filtered). These tests fault-inject each bucket we can reach from a unit
// test. The one case that cannot be injected here -- an OOM INSIDE
// GEOSWKBReader_read_r, which GEOS's execute() wrapper swallows into the
// same nullptr as corrupt WKB -- is documented as a KNOWN LIMIT on
// TryParseFromWkb and is intentionally classified as bad data.

TEST_F(GeometryValueSemanticsTest, TryParseFromWkbBadDataReturnsFalse) {
    const unsigned char corrupt[] = {0xde, 0xad, 0xbe, 0xef, 0x00};
    Geometry g;
    EXPECT_FALSE(g.TryParseFromWkb(ctx_, corrupt, sizeof(corrupt)));
    EXPECT_FALSE(g.IsValid());

    // Truncated-but-plausible WKB (header of a point, payload cut off) is
    // the placeholder shape the write paths keep; still bad data -> false.
    Geometry valid(ctx_, "POINT (1 2)");
    const std::string wkb = valid.to_wkb_string();
    ASSERT_GT(wkb.size(), 5u);
    EXPECT_FALSE(g.TryParseFromWkb(ctx_, wkb.data(), 5));
    EXPECT_FALSE(g.IsValid());

    // The same instance stays reusable: a good payload parses after a bad one.
    EXPECT_TRUE(g.TryParseFromWkb(ctx_, wkb.data(), wkb.size()));
    EXPECT_TRUE(g.IsValid());
}

// --- Tri-state GEOS predicate results (PR #50951 review, round Df4a298c5f4) --

// GEOS binary predicates return char 1/0/2, where 2 means an exception was
// caught inside GEOS's execute() guard. GeosPredicateIsTrue must map 2 to
// false (row-level leniency: one unevaluable row must not fail the query)
// while logging it -- never to true.
TEST(GeosPredicateResult, TriStateMapping) {
    EXPECT_TRUE(milvus::GeosPredicateIsTrue(1, "test"));
    EXPECT_FALSE(milvus::GeosPredicateIsTrue(0, "test"));
    EXPECT_FALSE(milvus::GeosPredicateIsTrue(2, "test"));
}

// Drive the exception path through a real predicate: GEOS relate-family
// predicates (touches/crosses/...) throw IllegalArgumentException for
// GEOMETRYCOLLECTION inputs, which execute() converts to a return of 2. The
// predicate must come back false -- an unevaluable row does not match -- and
// must not crash or return true.
TEST_F(GeometryValueSemanticsTest, PredicateExceptionEvaluatesToFalse) {
    Geometry collection(ctx_,
                        "GEOMETRYCOLLECTION(POINT(1 1),LINESTRING(0 0,2 2))");
    Geometry point(ctx_, "POINT (1 1)");
    ASSERT_TRUE(collection.IsValid());
    ASSERT_TRUE(point.IsValid());

    EXPECT_FALSE(collection.touches(point, ctx_));
    EXPECT_FALSE(collection.crosses(point, ctx_));
}

TEST_F(GeometryValueSemanticsTest,
       TryParseFromWkbNullContextThrowsRetriableSystemError) {
    Geometry valid(ctx_, "POINT (1 2)");
    const std::string wkb = valid.to_wkb_string();

    // Inject the transient bucket: a null context is a resource failure, NOT
    // bad data -- it must throw MemAllocateFailed (retriable), never return
    // false (which would silently filter a perfectly valid row).
    Geometry g;
    try {
        g.TryParseFromWkb(nullptr, wkb.data(), wkb.size());
        FAIL() << "expected SegcoreError";
    } catch (const milvus::SegcoreError& e) {
        EXPECT_EQ(e.get_error_code(), milvus::ErrorCode::MemAllocateFailed);
    }
}

}  // namespace
