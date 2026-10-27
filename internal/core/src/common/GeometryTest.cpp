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
#include <functional>
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

// Same transient/permanent classification, applied to the clone paths.
// GEOSGeom_clone_r() reports allocation failure by RETURNING nullptr -- the
// capi execute() guard catches std::bad_alloc and swallows it -- so no
// `catch (const std::bad_alloc&)` net upstream can ever see it, and the three
// clone sites have to classify it themselves. Reporting it with a plain
// AssertInfo yields UnexpectedError(2001), which merr does not mark retriable;
// on the search path lb_policy then blacklists the serving node for the
// channel instead of failing over to another replica, so memory pressure would
// evict healthy nodes from routing. Pin the retriable code at every site.
TEST_F(GeometryValueSemanticsTest, CloneFailureIsClassifiedAsRetriableOOM) {
    Geometry src(ctx_, "POINT (1 2)");
    ASSERT_TRUE(src.IsValid());

    auto expect_mem_allocate_failed = [](const char* what,
                                         std::function<void()> clone) {
        milvus::GeometryCloneFailureForTesting().store(true);
        try {
            clone();
            milvus::GeometryCloneFailureForTesting().store(false);
            FAIL() << what << " must throw when the clone fails";
        } catch (const milvus::SegcoreError& e) {
            EXPECT_EQ(e.get_error_code(), milvus::ErrorCode::MemAllocateFailed)
                << what << " must report the retriable OOM code";
        }
    };

    expect_mem_allocate_failed("copy construction",
                               [&]() { Geometry copy(src); });
    expect_mem_allocate_failed("copy assignment", [&]() {
        Geometry dst;
        dst = src;
    });
    expect_mem_allocate_failed("Clone(ctx)",
                               [&]() { auto c = src.Clone(ctx_); });

    // The hook is one-shot, so the fault injection cannot leak into unrelated
    // tests -- and cloning still works normally afterwards.
    ASSERT_FALSE(milvus::GeometryCloneFailureForTesting().load());
    Geometry copy(src);
    ASSERT_TRUE(copy.IsValid());
    EXPECT_NE(copy.GetRawGeometry(), src.GetRawGeometry());
    EXPECT_EQ(copy.to_wkb_string(), src.to_wkb_string());
}

// --- ST_DWithin semantics and the coarse/exact contract (issue #54138) ---

// GEOS reports a distance of 0 to an empty geometry, so an unguarded dwithin
// accepted every empty row at any distance on the raw path while the R-Tree
// (placeholder MBR at the origin) never surfaced it as a candidate. PostGIS
// returns false for an empty operand; so must we, on either side.
TEST_F(GeometryValueSemanticsTest, DWithinEmptyOperandIsFalse) {
    const Geometry query(ctx_, "POINT (-60 -40)");
    ASSERT_TRUE(query.IsValid());
    for (const char* wkt : {"POINT EMPTY",
                            "LINESTRING EMPTY",
                            "POLYGON EMPTY",
                            "MULTIPOINT EMPTY",
                            "GEOMETRYCOLLECTION EMPTY"}) {
        Geometry empty(ctx_, wkt);
        ASSERT_TRUE(empty.IsValid()) << wkt;
        EXPECT_FALSE(empty.dwithin(query, 1.0, ctx_)) << wkt;
        EXPECT_FALSE(empty.dwithin(query, 1e12, ctx_)) << wkt;
    }

    // Empty QUERY point: nothing is within any distance of it either.
    Geometry row(ctx_, "POINT (-60 -40)");
    Geometry empty_query(ctx_, "POINT EMPTY");
    EXPECT_FALSE(row.dwithin(empty_query, 1e12, ctx_));

    // Non-empty rows keep their semantics.
    Geometry near(ctx_, "POINT (-60 -40.0001)");
    EXPECT_TRUE(near.dwithin(query, 100.0, ctx_));
    EXPECT_FALSE(near.dwithin(query, 1.0, ctx_));
    Geometry line(ctx_, "LINESTRING (-61 -40, -59 -40)");
    EXPECT_TRUE(line.dwithin(query, 1.0, ctx_));
}

namespace {

struct LonLatBox {
    double min_x, min_y, max_x, max_y;
};

bool
EnvelopeOf(GEOSContextHandle_t ctx, const Geometry& g, LonLatBox& out) {
    return GEOSGeom_getXMin_r(ctx, g.GetGeometry(), &out.min_x) == 1 &&
           GEOSGeom_getYMin_r(ctx, g.GetGeometry(), &out.min_y) == 1 &&
           GEOSGeom_getXMax_r(ctx, g.GetGeometry(), &out.max_x) == 1 &&
           GEOSGeom_getYMax_r(ctx, g.GetGeometry(), &out.max_y) == 1;
}

bool
Intersects(const Geometry::Envelope& a, const LonLatBox& b) {
    return a.min_x <= b.max_x && b.min_x <= a.max_x && a.min_y <= b.max_y &&
           b.min_y <= a.max_y;
}

// The R-Tree coarse pass keeps a row when its MBR meets ANY box of the region.
bool
Intersects(const Geometry::DWithinSearchBoxes& region, const LonLatBox& b) {
    for (const auto& env : region) {
        if (Intersects(env, b)) {
            return true;
        }
    }
    return false;
}

// Total longitude width of the region, in degrees.
double
LongitudeWidth(const Geometry::DWithinSearchBoxes& region) {
    double width = 0;
    for (const auto& env : region) {
        width += env.max_x - env.min_x;
    }
    return width;
}

// Wrap a longitude into [-180, 180], where valid rows are stored.
double
NormalizeLon(double lon) {
    while (lon > 180.0) {
        lon -= 360.0;
    }
    while (lon < -180.0) {
        lon += 360.0;
    }
    return lon;
}

// Great-circle destination point (sphere), used to place rows exactly on the
// haversine boundary at an arbitrary bearing.
std::pair<double, double>
Destination(double lat_deg, double lon_deg, double bearing_deg, double meters) {
    const double R = 6371000.0, PI = 3.14159265358979323846;
    double lat = lat_deg * PI / 180, lon = lon_deg * PI / 180,
           brg = bearing_deg * PI / 180, ang = meters / R;
    double lat2 = std::asin(std::sin(lat) * std::cos(ang) +
                            std::cos(lat) * std::sin(ang) * std::cos(brg));
    double lon2 =
        lon + std::atan2(std::sin(brg) * std::sin(ang) * std::cos(lat),
                         std::cos(ang) - std::sin(lat) * std::sin(lat2));
    return {lon2 * 180 / PI, lat2 * 180 / PI};
}

}  // namespace

// The three points the issue reporter measured as missed by the indexed path
// with ST_DWITHIN(geo, 'POINT (115 35)', 500000): all accepted by the exact
// predicate, all outside the historical coarse box
// (109.516823 30.508444 120.483177 39.491556).
TEST_F(GeometryValueSemanticsTest, DWithinSearchEnvelopeCoversIssueRows) {
    const Geometry query(ctx_, "POINT (115 35)");
    const double d = 500000.0;
    Geometry::DWithinSearchBoxes region;
    ASSERT_TRUE(
        Geometry::DWithinSearchEnvelope(ctx_, query.GetGeometry(), d, region));
    // Far from the antimeridian and the poles: a single box.
    ASSERT_EQ(region.count, 1u);
    const auto& env = region.boxes[0];

    // Must strictly contain the old box (no regression in what was found).
    EXPECT_LT(env.min_x, 109.516823);
    EXPECT_LT(env.min_y, 30.508444);
    EXPECT_GT(env.max_x, 120.483177);
    EXPECT_GT(env.max_y, 39.491556);

    for (const char* wkt : {"POINT (109.513509 35.327246)",
                            "POINT (120.490847 35.204498)",
                            "POINT (115.028171 30.507155)",
                            // non-point rows use the scaled planar metric,
                            // which at lat 35 reaches ~4.91 deg north while
                            // the old box stopped at 4.49 deg.
                            "POLYGON ((114.9 39.8, 115.1 39.8, 115.1 39.9, "
                            "114.9 39.9, 114.9 39.8))",
                            "LINESTRING (114.5 30.2, 115.5 30.2)"}) {
        Geometry row(ctx_, wkt);
        ASSERT_TRUE(row.IsValid()) << wkt;
        ASSERT_TRUE(row.dwithin(query, d, ctx_)) << wkt;
        LonLatBox box{};
        ASSERT_TRUE(EnvelopeOf(ctx_, row, box)) << wkt;
        EXPECT_TRUE(Intersects(env, box)) << wkt;
    }

    // And it must still prune: a row well beyond the distance stays outside.
    Geometry far(ctx_, "POINT (125 35)");
    ASSERT_FALSE(far.dwithin(query, d, ctx_));
    LonLatBox far_box{};
    ASSERT_TRUE(EnvelopeOf(ctx_, far, far_box));
    EXPECT_FALSE(Intersects(env, far_box));
}

// Superset property, sampled: for query points spread over latitudes and
// distances, every row the exact predicate accepts must intersect the
// envelope. Rows are placed on and around the boundary of each metric, where
// the historical box failed.
TEST_F(GeometryValueSemanticsTest,
       DWithinSearchEnvelopeIsSupersetOfExactPredicate) {
    struct Case {
        double lon, lat, meters;
    };
    const std::vector<Case> cases = {
        {115, 35, 500000},
        {0, 0, 1000},
        {10, 60, 2000000},
        {-120, -75, 300000},
        {179.5, 10, 200000},
        // The cap crosses the antimeridian: the rows past +-180 are only
        // covered by the wrapped box (review of #54294).
        {179.999, 0, 1000},
        {-179.95, 40, 50000},
        {180, -20, 100000},
        {-180, 60, 300000},
        {5, 85, 800000},
        {30, 45, 0.5},
        {-45, 20, 50},
    };
    const std::vector<double> bearings = {0,
                                          30,
                                          45,
                                          60,
                                          90,
                                          120,
                                          135,
                                          150,
                                          180,
                                          210,
                                          225,
                                          240,
                                          270,
                                          300,
                                          315,
                                          330};
    for (const auto& c : cases) {
        const std::string qwkt = "POINT (" + std::to_string(c.lon) + " " +
                                 std::to_string(c.lat) + ")";
        const Geometry query(ctx_, qwkt.c_str());
        Geometry::DWithinSearchBoxes env;
        ASSERT_TRUE(Geometry::DWithinSearchEnvelope(
            ctx_, query.GetGeometry(), c.meters, env))
            << qwkt;

        int accepted = 0;
        for (double b : bearings) {
            for (double scale : {0.0, 0.5, 0.999, 1.0, 1.001, 1.5}) {
                // Point rows: exactly on / around the haversine circle.
                auto [plon, plat] =
                    Destination(c.lat, c.lon, b, c.meters * scale);
                // Valid point rows are stored in [-180, 180]: a destination
                // past the antimeridian is stored on the other side.
                std::string pwkt = "POINT (" +
                                   std::to_string(NormalizeLon(plon)) + " " +
                                   std::to_string(plat) + ")";
                Geometry point(ctx_, pwkt.c_str());
                ASSERT_TRUE(point.IsValid()) << pwkt;
                if (point.dwithin(query, c.meters, ctx_)) {
                    ++accepted;
                    LonLatBox box{};
                    ASSERT_TRUE(EnvelopeOf(ctx_, point, box));
                    EXPECT_TRUE(Intersects(env, box))
                        << "query " << qwkt << " d=" << c.meters
                        << " accepted point " << pwkt << " outside envelope";
                }

                // Non-point rows: a short segment whose nearest vertex sits
                // at the same lon/lat offset as the point above (planar
                // metric), plus a tiny polygon around it.
                double dx = (plon - c.lon) * scale, dy = (plat - c.lat) * scale;
                double vx = c.lon + dx, vy = c.lat + dy;
                std::string lwkt = "LINESTRING (" + std::to_string(vx) + " " +
                                   std::to_string(vy) + ", " +
                                   std::to_string(vx + 0.01) + " " +
                                   std::to_string(vy + 0.01) + ")";
                std::string gwkt =
                    "POLYGON ((" + std::to_string(vx) + " " +
                    std::to_string(vy) + ", " + std::to_string(vx + 0.01) +
                    " " + std::to_string(vy) + ", " +
                    std::to_string(vx + 0.01) + " " +
                    std::to_string(vy + 0.01) + ", " + std::to_string(vx) +
                    " " + std::to_string(vy) + "))";
                for (const auto& w : {lwkt, gwkt}) {
                    Geometry row(ctx_, w.c_str());
                    ASSERT_TRUE(row.IsValid()) << w;
                    if (row.dwithin(query, c.meters, ctx_)) {
                        ++accepted;
                        LonLatBox box{};
                        ASSERT_TRUE(EnvelopeOf(ctx_, row, box));
                        EXPECT_TRUE(Intersects(env, box))
                            << "query " << qwkt << " d=" << c.meters
                            << " accepted row " << w << " outside envelope";
                    }
                }
            }
        }
        EXPECT_GT(accepted, 0) << "no row accepted for " << qwkt
                               << "; the sample does not exercise the "
                                  "boundary";
    }
}

TEST_F(GeometryValueSemanticsTest, DWithinSearchEnvelopeEdgeCases) {
    // Empty query point: no envelope (caller yields zero candidates).
    Geometry empty(ctx_, "POINT EMPTY");
    Geometry::DWithinSearchBoxes region;
    EXPECT_FALSE(
        Geometry::DWithinSearchEnvelope(ctx_, empty.GetGeometry(), 10, region));

    // A cap containing a pole must span every longitude, in one box.
    Geometry polar(ctx_, "POINT (5 85)");
    ASSERT_TRUE(Geometry::DWithinSearchEnvelope(
        ctx_, polar.GetGeometry(), 800000, region));
    ASSERT_EQ(region.count, 1u);
    const auto& env = region.boxes[0];
    EXPECT_LE(env.min_x, -180.0);
    EXPECT_GE(env.max_x, 180.0);
    EXPECT_GE(env.max_y, 90.0);

    // Crossing the antimeridian adds a wrapped box at the opposite edge
    // instead of widening to every longitude.
    Geometry edge(ctx_, "POINT (179.5 10)");
    ASSERT_TRUE(Geometry::DWithinSearchEnvelope(
        ctx_, edge.GetGeometry(), 200000, region));
    ASSERT_EQ(region.count, 2u);
    EXPECT_GT(region.boxes[0].min_x, 177.0);
    EXPECT_GT(region.boxes[0].max_x, 180.0);  // unclamped primary box
    EXPECT_EQ(region.boxes[1].min_x, -180.0);
    EXPECT_GT(region.boxes[1].max_x, -180.0);
    EXPECT_LT(region.boxes[1].max_x, -178.0);

    // The reviewer's example: 1 km around a point 111 m from the
    // antimeridian is two slivers, not a 360 deg latitude band. Mirror it on
    // the west side, and check that the far side of the antimeridian is
    // reachable while an antipodal row is not.
    struct WrapCase {
        const char* query_wkt;
        const char* across_wkt;  // just across +-180, within 1 km
        double wrapped_min_x, wrapped_max_x;
    };
    for (const WrapCase& wc :
         {WrapCase{"POINT (179.999 0)", "POINT (-179.996 0)", -180.0, -179.9},
          WrapCase{"POINT (-179.999 0)", "POINT (179.996 0)", 179.9, 180.0}}) {
        SCOPED_TRACE(wc.query_wkt);
        Geometry q(ctx_, wc.query_wkt);
        ASSERT_TRUE(Geometry::DWithinSearchEnvelope(
            ctx_, q.GetGeometry(), 1000, region));
        ASSERT_EQ(region.count, 2u);
        EXPECT_LT(LongitudeWidth(region), 0.03);  // vs 360 before
        EXPECT_GE(region.boxes[1].min_x, wc.wrapped_min_x);
        EXPECT_LE(region.boxes[1].max_x, wc.wrapped_max_x);

        Geometry across(ctx_, wc.across_wkt);
        ASSERT_TRUE(across.dwithin(q, 1000, ctx_));
        LonLatBox box{};
        ASSERT_TRUE(EnvelopeOf(ctx_, across, box));
        EXPECT_TRUE(Intersects(region, box));

        Geometry antipodal(ctx_, "POINT (0 0)");
        ASSERT_TRUE(EnvelopeOf(ctx_, antipodal, box));
        EXPECT_FALSE(Intersects(region, box));
    }

    // A query exactly on the antimeridian: +180 and -180 are the same
    // meridian, so both edges are covered even at a near-zero distance.
    // Not exactly 0: haversine puts the two meridians ~1.6e-9 m apart,
    // since sin(pi) is not 0 in floating point.
    constexpr double kNearZeroMeters = 1e-6;
    Geometry on_edge(ctx_, "POINT (180 0)");
    ASSERT_TRUE(Geometry::DWithinSearchEnvelope(
        ctx_, on_edge.GetGeometry(), kNearZeroMeters, region));
    ASSERT_EQ(region.count, 2u);
    {
        Geometry same(ctx_, "POINT (-180 0)");
        ASSERT_TRUE(same.dwithin(on_edge, kNearZeroMeters, ctx_));
        LonLatBox box{};
        ASSERT_TRUE(EnvelopeOf(ctx_, same, box));
        EXPECT_TRUE(Intersects(region, box));
    }

    // A query longitude outside [-180, 180] keeps the full-range fallback:
    // haversine still wraps it, so the box cannot be placed by offset alone.
    Geometry outside(ctx_, "POINT (190 0)");
    ASSERT_TRUE(Geometry::DWithinSearchEnvelope(
        ctx_, outside.GetGeometry(), 1000, region));
    ASSERT_EQ(region.count, 1u);
    EXPECT_LE(region.boxes[0].min_x, -180.0);
    EXPECT_GE(region.boxes[0].max_x, 180.0);

    // A cap that touches a pole to within rounding: d is the pole distance
    // to the ulp, so cap_lat_deg and the sin/cos ratio in the envelope both
    // land a few ulp short of their exact values. The raw path accepts the
    // pole row (distance <= d holds bit for bit); an envelope whose margin is
    // applied only after asin stops ~1e-6 deg short of the full longitude
    // range and prunes it on the indexed path (review of #54294).
    struct PoleCase {
        const char* query_wkt;
        double query_lat, query_lon;
        const char* row_wkt;
        double row_lat, row_lon;
    };
    for (const PoleCase& pc :
         {PoleCase{"POINT (0 -29)", -29, 0, "POINT (180 -90)", -90, 180},
          PoleCase{"POINT (0 29)", 29, 0, "POINT (180 90)", 90, 180}}) {
        SCOPED_TRACE(pc.query_wkt);
        Geometry pole_query(ctx_, pc.query_wkt);
        Geometry pole_row(ctx_, pc.row_wkt);
        // Same argument order as Geometry::dwithin (row first, then query).
        const double pole_distance = Geometry::haversine_distance_meters(
            pc.row_lat, pc.row_lon, pc.query_lat, pc.query_lon);
        ASSERT_TRUE(pole_row.dwithin(pole_query, pole_distance, ctx_));
        ASSERT_TRUE(Geometry::DWithinSearchEnvelope(
            ctx_, pole_query.GetGeometry(), pole_distance, region));
        ASSERT_EQ(region.count, 1u);
        EXPECT_LE(env.min_x, -180.0);
        EXPECT_GE(env.max_x, 180.0);
        if (pc.row_lat < 0) {
            EXPECT_LE(env.min_y, -90.0);
        } else {
            EXPECT_GE(env.max_y, 90.0);
        }
    }

    // Negative distance is treated as zero: a degenerate box at the point.
    Geometry q(ctx_, "POINT (10 20)");
    ASSERT_TRUE(
        Geometry::DWithinSearchEnvelope(ctx_, q.GetGeometry(), -5, region));
    ASSERT_EQ(region.count, 1u);
    EXPECT_NEAR(env.min_x, 10.0, 1e-6);
    EXPECT_NEAR(env.max_x, 10.0, 1e-6);
    EXPECT_NEAR(env.min_y, 20.0, 1e-6);
    EXPECT_NEAR(env.max_y, 20.0, 1e-6);
}

}  // namespace
