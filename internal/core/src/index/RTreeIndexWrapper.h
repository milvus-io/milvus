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

#include <boost/geometry/index/rtree.hpp>
#include <geos_c.h>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <string>
#include <utility>
#include <unordered_set>
#include <vector>

#include "boost/geometry/core/cs.hpp"
#include "boost/geometry/core/static_assert.hpp"
#include "boost/geometry/geometries/box.hpp"
#include "boost/geometry/geometries/point.hpp"
#include "boost/geometry/index/parameters.hpp"
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

    /**
     * @brief Get the estimated memory usage of the R-tree index
     * @return Memory usage in bytes
     */
    int64_t
    ByteSize() const;

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
    // Returns false (leaving the outputs unspecified) when GEOS cannot compute
    // an envelope, e.g. for an empty geometry. Callers must not use the box on
    // a false return.
    bool
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

    // Insert one (box, row_offset) entry with rtree_mutex_ already held.
    // Idempotent per row_offset and all-or-nothing: a retried batch (the
    // segcore caller translates a mid-batch bad_alloc into a retriable
    // MemAllocateFailed and re-drives the whole batch) must not duplicate the
    // rows that already committed -- Boost R-tree accepts duplicate values,
    // and duplicates inflate count() past the segment row space.
    void
    insert_value_locked(const Box& box, int64_t row_offset);

    RTree rtree_{};
    std::vector<Value> values_;
    // Offsets already committed via insert_value_locked (add_geometry path
    // only; guarded by rtree_mutex_). bulk_load_from_field_data does not
    // maintain it: that path stages into a local vector and publishes by swap,
    // so a failed attempt leaves nothing behind to deduplicate against.
    std::unordered_set<int64_t> written_offsets_;
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