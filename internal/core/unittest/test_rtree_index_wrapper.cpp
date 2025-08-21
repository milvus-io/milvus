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
#include "common/Types.h"
#include "gdal.h"

class RTreeIndexWrapperTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        // Create test directory
        test_dir_ = "/tmp/rtree_test";
        std::filesystem::create_directories(test_dir_);

        // Initialize GDAL
        GDALAllRegister();
    }

    void
    TearDown() override {
        // Clean up test directory
        std::filesystem::remove_all(test_dir_);

        // Clean up GDAL
        GDALDestroyDriverManager();
    }

    // Helper function to create a simple point WKB
    std::vector<uint8_t>
    create_point_wkb(double x, double y) {
        // WKB format for a point: byte order (1) + geometry type (1) + coordinates (16 bytes)
        std::vector<uint8_t> wkb = {
            0x01,  // Little endian
            0x01,
            0x00,
            0x00,
            0x00,  // Point geometry type
        };

        // Add X coordinate (8 bytes, little endian double)
        uint8_t* x_bytes = reinterpret_cast<uint8_t*>(&x);
        wkb.insert(wkb.end(), x_bytes, x_bytes + sizeof(double));

        // Add Y coordinate (8 bytes, little endian double)
        uint8_t* y_bytes = reinterpret_cast<uint8_t*>(&y);
        wkb.insert(wkb.end(), y_bytes, y_bytes + sizeof(double));

        return wkb;
    }

    // Helper function to create a simple polygon WKB
    std::vector<uint8_t>
    create_polygon_wkb(const std::vector<std::pair<double, double>>& points) {
        // WKB format for a polygon
        std::vector<uint8_t> wkb = {
            0x01,  // Little endian
            0x03,
            0x00,
            0x00,
            0x00,  // Polygon geometry type
            0x01,
            0x00,
            0x00,
            0x00,  // 1 ring
        };

        // Add number of points in the ring
        uint32_t num_points = static_cast<uint32_t>(points.size());
        uint8_t* num_points_bytes = reinterpret_cast<uint8_t*>(&num_points);
        wkb.insert(
            wkb.end(), num_points_bytes, num_points_bytes + sizeof(uint32_t));

        // Add points
        for (const auto& point : points) {
            double x = point.first;
            double y = point.second;

            uint8_t* x_bytes = reinterpret_cast<uint8_t*>(&x);
            wkb.insert(wkb.end(), x_bytes, x_bytes + sizeof(double));

            uint8_t* y_bytes = reinterpret_cast<uint8_t*>(&y);
            wkb.insert(wkb.end(), y_bytes, y_bytes + sizeof(double));
        }

        return wkb;
    }

    std::string test_dir_;
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

        wrapper.add_geometry(point1_wkb.data(), point1_wkb.size(), 0);
        wrapper.add_geometry(point2_wkb.data(), point2_wkb.size(), 1);
        wrapper.add_geometry(point3_wkb.data(), point3_wkb.size(), 2);

        wrapper.finish();
    }

    // Test loading index
    {
        milvus::index::RTreeIndexWrapper wrapper(index_path, false);
        wrapper.load();

        // Create a query geometry (polygon that contains points 1 and 2)
        auto query_polygon_wkb = create_polygon_wkb(
            {{0.0, 0.0}, {2.5, 0.0}, {2.5, 2.5}, {0.0, 2.5}, {0.0, 0.0}});

        OGRGeometry* query_geom = nullptr;
        OGRGeometryFactory::createFromWkb(query_polygon_wkb.data(),
                                          nullptr,
                                          &query_geom,
                                          query_polygon_wkb.size());

        ASSERT_NE(query_geom, nullptr);

        std::vector<int64_t> candidates;
        wrapper.query_candidates(
            milvus::proto::plan::GISFunctionFilterExpr_GISOp_Intersects,
            *query_geom,
            candidates);

        // Should find points 1 and 2, but not point 3
        EXPECT_EQ(candidates.size(), 2);
        EXPECT_TRUE(std::find(candidates.begin(), candidates.end(), 0) !=
                    candidates.end());
        EXPECT_TRUE(std::find(candidates.begin(), candidates.end(), 1) !=
                    candidates.end());
        EXPECT_TRUE(std::find(candidates.begin(), candidates.end(), 2) ==
                    candidates.end());

        OGRGeometryFactory::destroyGeometry(query_geom);
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
        wrapper.add_geometry(polygon_wkb.data(), polygon_wkb.size(), 0);

        // Add some points
        auto point1_wkb = create_point_wkb(5.0, 5.0);    // Inside polygon
        auto point2_wkb = create_point_wkb(15.0, 15.0);  // Outside polygon
        auto point3_wkb = create_point_wkb(1.0, 1.0);    // Inside polygon

        wrapper.add_geometry(point1_wkb.data(), point1_wkb.size(), 1);
        wrapper.add_geometry(point2_wkb.data(), point2_wkb.size(), 2);
        wrapper.add_geometry(point3_wkb.data(), point3_wkb.size(), 3);

        wrapper.finish();
    }

    // Test queries
    {
        milvus::index::RTreeIndexWrapper wrapper(index_path, false);
        wrapper.load();

        // Query with a small polygon that intersects with the large polygon
        auto query_polygon_wkb = create_polygon_wkb(
            {{4.0, 4.0}, {6.0, 4.0}, {6.0, 6.0}, {4.0, 6.0}, {4.0, 4.0}});

        OGRGeometry* query_geom = nullptr;
        OGRGeometryFactory::createFromWkb(query_polygon_wkb.data(),
                                          nullptr,
                                          &query_geom,
                                          query_polygon_wkb.size());

        ASSERT_NE(query_geom, nullptr);

        std::vector<int64_t> candidates;
        wrapper.query_candidates(
            milvus::proto::plan::GISFunctionFilterExpr_GISOp_Intersects,
            *query_geom,
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

        OGRGeometryFactory::destroyGeometry(query_geom);
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