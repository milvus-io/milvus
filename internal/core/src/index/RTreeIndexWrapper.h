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

#pragma once

#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>
#include <boost/geometry.hpp>
#include <boost/geometry/index/rtree.hpp>
#include <geos_c.h>
#include "pb/plan.pb.h"

// Forward declaration to avoid pulling heavy field data headers here
namespace milvus {
class FieldDataBase;
}

namespace milvus::index {

namespace bg = boost::geometry;
namespace bgi = boost::geometry::index;

/**
 * @brief Wrapper class for boost R-Tree functionality
 * 
 * This class provides a simplified interface to boost library,
 * handling the creation, management, and querying of R-Tree spatial indexes
 * for geometric data in Milvus.
 */
class RTreeIndexWrapper {
 public:
    /**
     * @brief Constructor for RTreeIndexWrapper
     * @param path Path for storing index files
     * @param is_build_mode Whether this is for building new index or loading existing one
     */
    explicit RTreeIndexWrapper(std::string& path, bool is_build_mode);

    /**
     * @brief Destructor
     */
    ~RTreeIndexWrapper();

    void
    add_geometry(const uint8_t* wkb_data, size_t len, int64_t row_offset);

    /**
     * @brief Bulk load geometries from field data (WKB strings) into a new R-Tree.
     *        This API will create the R-Tree via createAndBulkLoadNewRTree internally.
     * @param field_datas Vector of field data blocks containing WKB strings
     * @param nullable Whether the field allows nulls (null rows are skipped but offset still advances)
     */
    void
    bulk_load_from_field_data(
        const std::vector<std::shared_ptr<::milvus::FieldDataBase>>&
            field_datas,
        bool nullable);

    /**
     * @brief Finish building the index and flush to disk
     */
    void
    finish();

    /**
     * @brief Load existing index from disk
     */
    void
    load();

    /**
     * @brief Query candidates based on spatial operation
     * @param op Spatial operation type
     * @param query_geom Query geometry
     * @param candidate_offsets Output vector of candidate row offsets
     */
    void
    query_candidates(proto::plan::GISFunctionFilterExpr_GISOp op,
                     const GEOSGeometry* query_geom,
                     GEOSContextHandle_t ctx,
                     std::vector<int64_t>& candidate_offsets);

    /**
     * @brief Get the total number of geometries in the index
     * @return Number of geometries
     */
    int64_t
    count() const;

    // Boost rtree does not use index/leaf capacities; keep only fill factor for
    // compatibility (no-op currently)

 private:
    /**
     * @brief Get bounding box from GEOS geometry
     * @param geom Input geometry
     * @param ctx GEOS context handle
     * @param minX Output minimum X coordinate
     * @param minY Output minimum Y coordinate
     * @param maxX Output maximum X coordinate
     * @param maxY Output maximum Y coordinate
     */
    void
    get_bounding_box(const GEOSGeometry* geom,
                     GEOSContextHandle_t ctx,
                     double& minX,
                     double& minY,
                     double& maxX,
                     double& maxY);

 private:
    // Boost.Geometry types and in-memory structures
    using Point = bg::model::point<double, 2, bg::cs::cartesian>;
    using Box = bg::model::box<Point>;
    using Value = std::pair<Box, int64_t>;  // (MBR, row_offset)
    using RTree = bgi::rtree<Value, bgi::rstar<16>>;

    RTree rtree_{};
    std::vector<Value> values_;
    std::string index_path_;
    bool is_build_mode_;

    // Flag to guard against repeated invocations which could otherwise attempt to release resources multiple times (e.g. BuildWithRawDataForUT() calls finish(), and Upload() may call it again).
    bool finished_ = false;

    // Serialize access to rtree_
    mutable std::shared_mutex rtree_mutex_;

    // R-Tree parameters
    uint32_t dimension_ = 2;
};

}  // namespace milvus::index