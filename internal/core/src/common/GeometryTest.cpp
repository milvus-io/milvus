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

}  // namespace
