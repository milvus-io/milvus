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
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <new>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "RTreeIndexSerialization.h"
#include "RTreeIndexWrapper.h"
#include "common/EasyAssert.h"
#include "common/Geometry.h"
#include "geos_c.h"
#include "gtest/gtest.h"
#include "pb/plan.pb.h"
#include "test_utils/Constants.h"

class RTreeIndexWrapperTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        // Create test directory
        test_dir_ = TestLocalPath + "rtree_test";
        std::filesystem::create_directories(test_dir_);

        // Initialize GEOS
        ctx_ = GEOS_init_r();
    }

    void
    TearDown() override {
        // Clean up test directory
        std::filesystem::remove_all(test_dir_);

        // Clean up GEOS
        GEOS_finish_r(ctx_);
    }

    // Helper function to create a simple point WKB using GEOS
    std::string
    create_point_wkb(double x, double y) {
        std::string wkt =
            "POINT (" + std::to_string(x) + " " + std::to_string(y) + ")";
        milvus::Geometry geom(ctx_, wkt.c_str());
        return geom.to_wkb_string();
    }

    // Helper function to create a simple polygon WKB using GEOS
    std::string
    create_polygon_wkb(const std::vector<std::pair<double, double>>& points) {
        std::string wkt = "POLYGON ((";
        for (size_t i = 0; i < points.size(); ++i) {
            if (i > 0)
                wkt += ", ";
            wkt += std::to_string(points[i].first) + " " +
                   std::to_string(points[i].second);
        }
        wkt += "))";

        milvus::Geometry geom(ctx_, wkt.c_str());
        return geom.to_wkb_string();
    }

    std::string test_dir_;
    GEOSContextHandle_t ctx_;
};

TEST_F(RTreeIndexWrapperTest, TestBuildAndLoad) {
    std::string index_path = test_dir_ + "/test_index";

    // Test building index
    {
        milvus::index::RTreeIndexWrapper wrapper(index_path, true);

        // Add some test geometries
        auto point1_wkb = create_point_wkb(1.0, 1.0);
        auto point2_wkb = create_point_wkb(2.0, 2.0);
        auto point3_wkb = create_point_wkb(3.0, 3.0);

        wrapper.add_geometry(
            reinterpret_cast<const uint8_t*>(point1_wkb.data()),
            point1_wkb.size(),
            0);
        wrapper.add_geometry(
            reinterpret_cast<const uint8_t*>(point2_wkb.data()),
            point2_wkb.size(),
            1);
        wrapper.add_geometry(
            reinterpret_cast<const uint8_t*>(point3_wkb.data()),
            point3_wkb.size(),
            2);

        wrapper.finish();
    }

    // Test loading index
    {
        milvus::index::RTreeIndexWrapper wrapper(index_path, false);
        wrapper.load();

        // Create a query geometry (polygon that contains points 1 and 2)
        auto query_polygon_wkb = create_polygon_wkb(
            {{0.0, 0.0}, {2.5, 0.0}, {2.5, 2.5}, {0.0, 2.5}, {0.0, 0.0}});

        milvus::Geometry query_geom(
            ctx_,
            reinterpret_cast<const void*>(query_polygon_wkb.data()),
            query_polygon_wkb.size());

        std::vector<int64_t> candidates;
        wrapper.query_candidates(
            milvus::proto::plan::GISFunctionFilterExpr_GISOp_Intersects,
            query_geom.GetGeometry(),
            ctx_,
            candidates);

        // Should find points 1 and 2, but not point 3
        EXPECT_EQ(candidates.size(), 2);
        EXPECT_TRUE(std::find(candidates.begin(), candidates.end(), 0) !=
                    candidates.end());
        EXPECT_TRUE(std::find(candidates.begin(), candidates.end(), 1) !=
                    candidates.end());
        EXPECT_TRUE(std::find(candidates.begin(), candidates.end(), 2) ==
                    candidates.end());
    }
}

TEST_F(RTreeIndexWrapperTest, TestQueryOperations) {
    std::string index_path = test_dir_ + "/test_query_index";

    // Build index with various geometries
    {
        milvus::index::RTreeIndexWrapper wrapper(index_path, true);

        // Add a polygon
        auto polygon_wkb = create_polygon_wkb(
            {{0.0, 0.0}, {10.0, 0.0}, {10.0, 10.0}, {0.0, 10.0}, {0.0, 0.0}});
        wrapper.add_geometry(
            reinterpret_cast<const uint8_t*>(polygon_wkb.data()),
            polygon_wkb.size(),
            0);

        // Add some points
        auto point1_wkb = create_point_wkb(5.0, 5.0);    // Inside polygon
        auto point2_wkb = create_point_wkb(15.0, 15.0);  // Outside polygon
        auto point3_wkb = create_point_wkb(1.0, 1.0);    // Inside polygon

        wrapper.add_geometry(
            reinterpret_cast<const uint8_t*>(point1_wkb.data()),
            point1_wkb.size(),
            1);
        wrapper.add_geometry(
            reinterpret_cast<const uint8_t*>(point2_wkb.data()),
            point2_wkb.size(),
            2);
        wrapper.add_geometry(
            reinterpret_cast<const uint8_t*>(point3_wkb.data()),
            point3_wkb.size(),
            3);

        wrapper.finish();
    }

    // Test queries
    {
        milvus::index::RTreeIndexWrapper wrapper(index_path, false);
        wrapper.load();

        // Query with a small polygon that intersects with the large polygon
        auto query_polygon_wkb = create_polygon_wkb(
            {{4.0, 4.0}, {6.0, 4.0}, {6.0, 6.0}, {4.0, 6.0}, {4.0, 4.0}});

        milvus::Geometry query_geom(
            ctx_,
            reinterpret_cast<const void*>(query_polygon_wkb.data()),
            query_polygon_wkb.size());

        std::vector<int64_t> candidates;
        wrapper.query_candidates(
            milvus::proto::plan::GISFunctionFilterExpr_GISOp_Intersects,
            query_geom.GetGeometry(),
            ctx_,
            candidates);

        // Should find the large polygon and point1, but not point2 or point3
        EXPECT_EQ(candidates.size(), 2);
        EXPECT_TRUE(std::find(candidates.begin(), candidates.end(), 0) !=
                    candidates.end());
        EXPECT_TRUE(std::find(candidates.begin(), candidates.end(), 1) !=
                    candidates.end());
        EXPECT_TRUE(std::find(candidates.begin(), candidates.end(), 2) ==
                    candidates.end());
        EXPECT_TRUE(std::find(candidates.begin(), candidates.end(), 3) ==
                    candidates.end());
    }
}

TEST_F(RTreeIndexWrapperTest, TestInvalidWKB) {
    std::string index_path = test_dir_ + "/test_invalid_wkb";

    milvus::index::RTreeIndexWrapper wrapper(index_path, true);

    // Test with invalid WKB data
    std::vector<uint8_t> invalid_wkb = {0x01, 0x02, 0x03, 0x04};  // Invalid WKB

    // This should not crash and should handle the error gracefully
    wrapper.add_geometry(invalid_wkb.data(), invalid_wkb.size(), 0);

    wrapper.finish();
}

TEST_F(RTreeIndexWrapperTest, FinishReportsBinaryWriteFailure) {
    std::string index_path = test_dir_ + "/test_write_failure";
    milvus::index::RTreeIndexWrapper wrapper(index_path, true);

    // A directory at the binary-file path makes ofstream::open fail
    // deterministically without depending on process permissions.
    std::filesystem::create_directory(index_path + ".bgi");
    try {
        wrapper.finish();
        FAIL() << "expected R-Tree binary write failure";
    } catch (const milvus::SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), milvus::FileWriteFailed);
    }
}

TEST_F(RTreeIndexWrapperTest, FinishReportsBinaryCloseFailure) {
    std::string index_path = test_dir_ + "/test_close_failure";
    milvus::index::RTreeIndexWrapper wrapper(index_path, true);
    const std::string payload = create_point_wkb(1.0, 1.0);
    wrapper.add_geometry(
        reinterpret_cast<const uint8_t*>(payload.data()), payload.size(), 0);

    // A write error that the filesystem only reports at close(2) used to be
    // swallowed by ~basic_ofstream and returned as Success, uploading a
    // truncated .bgi as a successfully built index.
    RTreeSerializer::CloseFailureForTesting().store(true);
    try {
        wrapper.finish();
        FAIL() << "expected R-Tree binary close failure";
    } catch (const milvus::SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), milvus::FileWriteFailed);
    }
    // One-shot: the flag must not leak into the tests that follow.
    EXPECT_FALSE(RTreeSerializer::CloseFailureForTesting().load());
}

TEST_F(RTreeIndexWrapperTest, LoadReportsMissingBinaryAsReadFailure) {
    std::string index_path = test_dir_ + "/test_missing_binary";
    milvus::index::RTreeIndexWrapper wrapper(index_path, false);

    try {
        wrapper.load();
        FAIL() << "expected missing R-Tree binary failure";
    } catch (const milvus::SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), milvus::FileReadFailed);
    }
}

TEST_F(RTreeIndexWrapperTest, LoadReportsCorruptBinaryAsDataFormatBroken) {
    std::string index_path = test_dir_ + "/test_corrupt_binary";
    {
        milvus::index::RTreeIndexWrapper builder(index_path, true);
        builder.finish();
    }

    const auto binary_path = index_path + ".bgi";
    std::ifstream in(binary_path, std::ios::binary | std::ios::ate);
    ASSERT_TRUE(in.is_open());
    const auto original_size = in.tellg();
    ASSERT_GT(original_size, 1);
    std::string bytes(static_cast<size_t>(original_size), '\0');
    in.seekg(0);
    in.read(bytes.data(), original_size);
    ASSERT_TRUE(in.good());
    in.close();

    bytes.resize(bytes.size() / 2);
    {
        std::ofstream out(binary_path, std::ios::binary | std::ios::trunc);
        ASSERT_TRUE(out.is_open());
        out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        ASSERT_TRUE(out.good());
    }

    milvus::index::RTreeIndexWrapper wrapper(index_path, false);
    try {
        wrapper.load();
        FAIL() << "expected corrupt R-Tree binary failure";
    } catch (const milvus::SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), milvus::DataFormatBroken);
    }
}

TEST_F(RTreeIndexWrapperTest, EmptyGeometryIsIndexedWithoutUndefinedMBR) {
    std::string index_path = test_dir_ + "/test_empty_geometry";
    milvus::index::RTreeIndexWrapper wrapper(index_path, true);

    // An empty geometry has no envelope: GEOSGeom_get{X,Y}{Min,Max}_r fail and
    // leave the coordinates uninitialized. The wrapper must not insert a
    // garbage MBR; it indexes the row with a deterministic placeholder so the
    // row count stays consistent with the segment.
    std::string empty_wkb =
        milvus::Geometry(ctx_, "POLYGON EMPTY").to_wkb_string();
    ASSERT_FALSE(empty_wkb.empty());
    wrapper.add_geometry(reinterpret_cast<const uint8_t*>(empty_wkb.data()),
                         empty_wkb.size(),
                         0);

    auto point_wkb = create_point_wkb(10.0, 10.0);
    wrapper.add_geometry(reinterpret_cast<const uint8_t*>(point_wkb.data()),
                         point_wkb.size(),
                         1);

    // Both rows indexed (no row silently dropped).
    EXPECT_EQ(wrapper.count(), 2);

    // A query far from the placeholder/origin must only return the real point;
    // the empty geometry must not spuriously match.
    auto query_polygon_wkb = create_polygon_wkb(
        {{9.0, 9.0}, {11.0, 9.0}, {11.0, 11.0}, {9.0, 11.0}, {9.0, 9.0}});
    milvus::Geometry query_geom(
        ctx_,
        reinterpret_cast<const void*>(query_polygon_wkb.data()),
        query_polygon_wkb.size());
    std::vector<int64_t> candidates;
    wrapper.query_candidates(
        milvus::proto::plan::GISFunctionFilterExpr_GISOp_Intersects,
        query_geom.GetGeometry(),
        ctx_,
        candidates);
    EXPECT_NE(std::find(candidates.begin(), candidates.end(), 1),
              candidates.end());
    EXPECT_EQ(std::find(candidates.begin(), candidates.end(), 0),
              candidates.end());

    wrapper.finish();
}

TEST_F(RTreeIndexWrapperTest, InsertExceptionRebuildsTreeBeforeReuse) {
    std::string index_path = test_dir_ + "/test_index_insert_recovery";
    milvus::index::RTreeIndexWrapper wrapper(index_path, true);

    auto point0_wkb = create_point_wkb(1.0, 1.0);
    auto point1_wkb = create_point_wkb(2.0, 2.0);
    wrapper.add_geometry(reinterpret_cast<const uint8_t*>(point0_wkb.data()),
                         point0_wkb.size(),
                         0);

    // Simulate Boost's worst documented exception state: the tree mutation
    // completed, then the operation threw. The wrapper must discard that tree
    // and rebuild from the authoritative committed values before any read.
    wrapper.SetThrowAfterInsertForTesting(true);
    EXPECT_THROW(wrapper.add_geometry(
                     reinterpret_cast<const uint8_t*>(point1_wkb.data()),
                     point1_wkb.size(),
                     1),
                 std::bad_alloc);

    auto query_polygon_wkb = create_polygon_wkb(
        {{0.5, 0.5}, {2.5, 0.5}, {2.5, 2.5}, {0.5, 2.5}, {0.5, 0.5}});
    milvus::Geometry query_geom(
        ctx_,
        reinterpret_cast<const void*>(query_polygon_wkb.data()),
        query_polygon_wkb.size());
    std::vector<int64_t> candidates;
    wrapper.query_candidates(
        milvus::proto::plan::GISFunctionFilterExpr_GISOp_Intersects,
        query_geom.GetGeometry(),
        ctx_,
        candidates);
    EXPECT_EQ(candidates, std::vector<int64_t>({0}));
    EXPECT_EQ(wrapper.count(), 1);

    // The failed offset was not committed and can be retried on the rebuilt
    // tree without duplicating or losing the previously committed row.
    wrapper.add_geometry(reinterpret_cast<const uint8_t*>(point1_wkb.data()),
                         point1_wkb.size(),
                         1);
    EXPECT_EQ(wrapper.count(), 2);
    candidates.clear();
    wrapper.query_candidates(
        milvus::proto::plan::GISFunctionFilterExpr_GISOp_Intersects,
        query_geom.GetGeometry(),
        ctx_,
        candidates);
    std::sort(candidates.begin(), candidates.end());
    EXPECT_EQ(candidates, std::vector<int64_t>({0, 1}));
}

// count() is served from entry_count_ instead of rtree_.size(), so it must be
// published by every path that changes the committed row set. The build path
// (incremental inserts) and the load path (which deserializes straight into
// the tree and never fills values_) publish it from different sources, so pin
// both here at the wrapper boundary -- the index-level tests only reach them
// through RTreeIndex::Count()'s max() of its own row counter.
TEST_F(RTreeIndexWrapperTest, CountTracksCommittedRowsOnBuildAndLoadPaths) {
    std::string index_path = test_dir_ + "/test_index_count_paths";
    constexpr int kRows = 3;

    {
        milvus::index::RTreeIndexWrapper wrapper(index_path, true);
        EXPECT_EQ(wrapper.count(), 0);

        for (int i = 0; i < kRows; ++i) {
            auto wkb = create_point_wkb(static_cast<double>(i),
                                        static_cast<double>(i));
            wrapper.add_geometry(
                reinterpret_cast<const uint8_t*>(wkb.data()), wkb.size(), i);
            EXPECT_EQ(wrapper.count(), static_cast<int64_t>(i + 1));
        }
        wrapper.finish();
    }

    {
        milvus::index::RTreeIndexWrapper wrapper(index_path, false);
        // Nothing is published before load(): a load-mode wrapper starts empty.
        EXPECT_EQ(wrapper.count(), 0);
        wrapper.load();
        EXPECT_EQ(wrapper.count(), static_cast<int64_t>(kRows));
    }
}

// Pins the lock POLICY, not just the presence of a lock: rtree_mutex_ must
// stay write-priority. add_geometry takes it exclusively once per row while
// every concurrent search takes it shared, so under a reader-preferring lock
// (std::shared_mutex is a glibc pthread rwlock, PREFER_READER by default) a
// steady stream of overlapping searches barges ahead of the insert thread and
// starves it -- in production that blocks the vchannel's flowgraph consumer
// and freezes the channel's time-tick, i.e. read traffic taking down the write
// path.
//
// The readers here are deliberately unthrottled: that is the pressure the
// writer has to survive. The deadline is very generous (the writer's own work
// is milliseconds) and exists only so a policy regression fails in bounded
// time with a clear message, instead of hanging until the CI shard timeout.
TEST_F(RTreeIndexWrapperTest, WriterIsNotStarvedByContinuousReaders) {
    std::string index_path = test_dir_ + "/test_index_writer_priority";
    milvus::index::RTreeIndexWrapper wrapper(index_path, true);

    constexpr int kRows = 1000;
    constexpr auto kWriterDeadline = std::chrono::seconds(120);

    // Pre-generate every payload on this thread: a GEOSContextHandle_t is not
    // shareable across threads, and it keeps the writer loop measuring lock
    // contention rather than WKB encoding.
    std::vector<std::string> payloads;
    payloads.reserve(kRows + 1);
    for (int i = 0; i <= kRows; ++i) {
        payloads.push_back(
            create_point_wkb(static_cast<double>(i), static_cast<double>(i)));
    }

    // Seed row 0 so the readers query a non-empty tree from the first
    // iteration.
    wrapper.add_geometry(reinterpret_cast<const uint8_t*>(payloads[0].data()),
                         payloads[0].size(),
                         0);

    std::atomic<bool> stop{false};
    std::atomic<int> writer_progress{0};
    std::atomic<int64_t> reader_iters{0};

    std::vector<std::thread> readers;
    for (int t = 0; t < 4; ++t) {
        readers.emplace_back([&]() {
            auto ctx = GEOS_init_r();
            // The inner scope is load-bearing: ~Geometry calls
            // GEOSGeom_destroy_r(ctx_, ...), and locals are destroyed AFTER
            // the last statement of the enclosing block. With query_geom
            // declared beside ctx, the trailing GEOS_finish_r would free the
            // context first and the destructor would run against it -- a
            // use-after-free on every reader thread.
            {
                // A box covering every inserted point.
                milvus::Geometry query_geom(
                    ctx,
                    "POLYGON ((-1 -1, 100000 -1, 100000 100000, -1 100000, -1 "
                    "-1))");
                std::vector<int64_t> candidates;
                while (!stop.load(std::memory_order_relaxed)) {
                    wrapper.query_candidates(
                        milvus::proto::plan::
                            GISFunctionFilterExpr_GISOp_Intersects,
                        query_geom.GetGeometry(),
                        ctx,
                        candidates);
                    reader_iters.fetch_add(1, std::memory_order_relaxed);
                }
            }
            GEOS_finish_r(ctx);
        });
    }

    // Single writer, mirroring the per-segment serialized insert pipeline.
    std::thread writer([&]() {
        for (int i = 1; i <= kRows; ++i) {
            wrapper.add_geometry(
                reinterpret_cast<const uint8_t*>(payloads[i].data()),
                payloads[i].size(),
                i);
            writer_progress.fetch_add(1, std::memory_order_relaxed);
        }
    });

    const auto deadline = std::chrono::steady_clock::now() + kWriterDeadline;
    while (writer_progress.load(std::memory_order_relaxed) < kRows &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    const int progressed = writer_progress.load(std::memory_order_relaxed);

    stop.store(true, std::memory_order_relaxed);
    writer.join();
    for (auto& th : readers) {
        th.join();
    }

    EXPECT_EQ(progressed, kRows)
        << "the insert thread was starved by concurrent readers: only "
        << progressed << " of " << kRows
        << " rows were inserted within the deadline -- is rtree_mutex_ still "
           "write-priority?";
    EXPECT_GT(reader_iters.load(), 0);
    EXPECT_EQ(wrapper.count(), static_cast<int64_t>(kRows + 1));
}
