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

#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/box.hpp>
#include <boost/geometry/geometries/point.hpp>
#include <boost/geometry/index/rtree.hpp>
#include <geos_c.h>

// Boost geometry engine, composed by the spatial builder and reader. Separate
// build/query types keep disjoint operations off the wrong lifecycle interface.
// Query operations use native SpatialOp rather than protobuf plan enums.

namespace milvus::index {

class RTreeQueryEngine;

namespace rtree_detail {
namespace bg = boost::geometry;
namespace bgi = boost::geometry::index;

using Point = bg::model::point<double, 2, bg::cs::cartesian>;
using Box = bg::model::box<Point>;
// (minimum bounding rectangle, row offset)
using Value = std::pair<Box, int64_t>;
using RTree = bgi::rtree<Value, bgi::rstar<16>>;
}  // namespace rtree_detail

// Build side. One-shot, single-threaded from the builder's point of view.
class RTreeBuildEngine {
 public:
    explicit RTreeBuildEngine(std::string index_path);

    ~RTreeBuildEngine();

    // WKB in, MBR + row offset out. Rows whose WKB fails to parse are SKIPPED
    // while the offset still advances (RTreeIndexWrapper.cpp:134-151) — that
    // silent skip is load-bearing for offset alignment; keep it, and keep it
    // documented.
    void
    AddGeometry(const uint8_t* wkb, size_t len, int64_t row_offset);

    // Bulk packing algorithm; much faster than repeated insert
    // (RTreeIndexWrapper.cpp:165-166).
    void
    BulkLoad(std::vector<rtree_detail::Value> values);

    // Writes `<index_path>.bgi` and `<index_path>.meta.json`.
    void
    Finish();

    // Growing publication consumes an unpersisted build window into an
    // immutable in-memory query generation. The rvalue qualification prevents
    // later writes through the consumed build engine.
    std::vector<rtree_detail::Value>
    TakeValues() &&;

    int64_t
    Count() const;

    const std::string&
    IndexPath() const;

 private:
    rtree_detail::RTree rtree_;
    std::vector<rtree_detail::Value> values_;
    std::string index_path_;
    GEOSContextHandle_t geos_context_{nullptr};
    GEOSWKBReader* wkb_reader_{nullptr};
    bool finished_{false};
    uint32_t dimension_{2};
};

// Query-side engine, immutable after Load and safe for concurrent reads.
class RTreeQueryEngine {
 public:
    // Build one immutable in-memory query shard without staging files.
    static std::shared_ptr<const RTreeQueryEngine>
    Create(std::vector<rtree_detail::Value> values);

    explicit RTreeQueryEngine(std::string index_path);

    ~RTreeQueryEngine();

    void
    Load();

    // MBR coarse filter. The exact relation is exec's job.
    // Returns false when the query has no usable MBR. The reader must fall
    // back to all non-null rows in that case to preserve superset semantics.
    bool
    QueryCandidates(const GEOSGeometry* query_geom,
                    GEOSContextHandle_t ctx,
                    std::vector<int64_t>& candidate_offsets) const;

    // Allocation-free candidate traversal for sealed readers. Returns false
    // when no usable query MBR exists and the caller must use a safe fallback.
    bool
    ForEachCandidate(const GEOSGeometry* query_geom,
                     GEOSContextHandle_t ctx,
                     const std::function<void(int64_t)>& visitor) const;

    int64_t
    Count() const;

    // Validate the row coordinates embedded in the archive against the
    // authoritative segment coordinate count supplied by the caller.
    void
    ValidateCoordinates(int64_t total_num_rows,
                        const std::vector<size_t>& null_offsets) const;

    // Heap bytes derived from the actual loaded Boost node count and node
    // allocation type. This is not serialized/archive byte size.
    int64_t
    ByteSize() const;

    // Used only to compact immutable growing shards. Existing row coordinates
    // and boxes are copied; the source engine remains unchanged for old pins.
    void
    AppendValues(std::vector<rtree_detail::Value>& output) const;

 private:
    explicit RTreeQueryEngine(std::vector<rtree_detail::Value> values);

    void
    UpdateHeapBytes();

    rtree_detail::RTree rtree_;
    std::string index_path_;
    uint32_t dimension_{2};
    int64_t heap_bytes_{0};
};

}  // namespace milvus::index
