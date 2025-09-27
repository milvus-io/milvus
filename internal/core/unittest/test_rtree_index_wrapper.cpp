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
#include <filesystem>
#include <vector>
#include "index/RTreeIndexWrapper.h"
#include "common/Geometry.h"

class RTreeIndexWrapperTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        // Create test directory
        test_dir_ = "/tmp/rtree_test";
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