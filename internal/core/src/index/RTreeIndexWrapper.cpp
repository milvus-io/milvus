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

#include <nlohmann/json.hpp>
#include <algorithm>
#include <exception>
#include <filesystem>
#include <fstream>
#include <initializer_list>
#include <iterator>
#include <map>
#include <mutex>

#include "RTreeIndexSerialization.h"
#include "RTreeIndexWrapper.h"
#include "boost/container/vector.hpp"
#include "boost/geometry/index/detail/predicates.hpp"
#include "boost/geometry/index/detail/rtree/node/node_elements.hpp"
#include "boost/geometry/index/detail/rtree/node/variant_visitor.hpp"
#include "boost/geometry/index/predicates.hpp"
#include "boost/variant/detail/apply_visitor_unary.hpp"
#include "common/EasyAssert.h"
#include "common/FieldDataInterface.h"
#include "fmt/core.h"
#include "geos_c.h"
#include "glog/logging.h"
#include "log/Log.h"
#include "nlohmann/json_fwd.hpp"
#include "pb/plan.pb.h"

namespace milvus::index {

RTreeIndexWrapper::RTreeIndexWrapper(std::string& path, bool is_build_mode)
    : index_path_(path), is_build_mode_(is_build_mode) {
    if (is_build_mode_) {
        std::filesystem::path dir_path =
            std::filesystem::path(path).parent_path();
        if (!dir_path.empty()) {
            std::filesystem::create_directories(dir_path);
        }
        // Start with an empty rtree for dynamic insertions
        rtree_ = RTree();
    }
}

RTreeIndexWrapper::~RTreeIndexWrapper() = default;

void
RTreeIndexWrapper::add_geometry(const uint8_t* wkb_data,
                                size_t len,
                                int64_t row_offset) {
    // Acquire write lock to protect rtree_
    std::unique_lock<std::shared_mutex> guard(rtree_mutex_);

    AssertInfo(is_build_mode_, "Cannot add geometry in load mode");

    // Index a deterministic placeholder MBR for a row whose WKB payload is
    // genuinely unparseable (empty or corrupt DATA), without dropping it. The
    // R-tree is only a coarse filter and exact refinement tolerates the
    // placeholder (Geometry::TryParseFromWkb -> skip) in every configuration,
    // so a placeholder never yields a wrong result -- but dropping the row
    // would permanently desynchronize the index row count from the segment row
    // count, which then trips the coarse-bitmap bounds guard in
    // EvalForIndexSegment on EVERY subsequent geometry query against this
    // segment. This mirrors the empty-geometry handling below and bulk_load.
    //
    // This is ONLY for bad data. A transient resource failure (GEOS context /
    // reader allocation) on a perfectly valid geometry must NOT take this path:
    // a (0,0) placeholder would permanently mis-locate a good row, silently
    // dropping it from every query that does not cover the origin. Those cases
    // throw instead, so the insert fails and can be retried. See PR #50951.
    //
    // Tradeoff: Point(0, 0) is a legal coordinate (Null Island), so any query
    // whose bounding box covers the origin pulls every placeholder row in this
    // segment into the candidate set and pays exact refinement for it (which
    // then discards the row). World-scale bbox queries almost always cover the
    // origin, so segments with many empty/corrupt geometries make such queries
    // proportionally more expensive. Correctness is unaffected.
    auto index_placeholder_mbr = [&]() {
        Value val(Box(Point(0, 0), Point(0, 0)), row_offset);
        values_.push_back(val);
        rtree_.insert(val);
    };

    // Parse WKB data using GEOS for consistency
    GEOSContextHandle_t ctx = GEOS_init_r();
    if (ctx == nullptr) {
        // Transient resource failure on a valid geometry -- fail the insert so
        // it can be retried, rather than permanently mis-indexing a good row.
        ThrowInfo(ErrorCode::MemAllocateFailed,
                  "Failed to initialize GEOS context for row {}",
                  row_offset);
    }

    GEOSWKBReader* reader = GEOSWKBReader_create_r(ctx);
    if (reader == nullptr) {
        GEOS_finish_r(ctx);
        // Transient resource failure -- see above; throw rather than placeholder.
        ThrowInfo(ErrorCode::MemAllocateFailed,
                  "Failed to create GEOS WKB reader for row {}",
                  row_offset);
    }

    GEOSGeometry* geom = GEOSWKBReader_read_r(ctx, reader, wkb_data, len);
    GEOSWKBReader_destroy_r(ctx, reader);

    if (geom == nullptr) {
        GEOS_finish_r(ctx);
        LOG_ERROR(
            "Failed to parse WKB data for row {}; indexing a placeholder MBR "
            "to keep the index row count consistent",
            row_offset);
        index_placeholder_mbr();
        return;
    }

    // Get bounding box. On failure (e.g. empty geometry) keep a deterministic
    // placeholder MBR and still index the row: the R-tree is only a coarse
    // filter, the exact predicate refines it out, and dropping the row here
    // would desynchronize the index row count from the segment row count.
    double minX = 0, minY = 0, maxX = 0, maxY = 0;
    if (!get_bounding_box(geom, ctx, minX, minY, maxX, maxY)) {
        LOG_WARN(
            "geometry at row {} has no computable envelope (empty?); indexing "
            "with a placeholder MBR, exact refinement will filter it",
            row_offset);
    }

    // Create Boost box and insert
    Box box(Point(minX, minY), Point(maxX, maxY));
    Value val(box, row_offset);
    values_.push_back(val);
    rtree_.insert(val);

    // Clean up
    GEOSGeom_destroy_r(ctx, geom);
    GEOS_finish_r(ctx);
}

// No IDataStream; bulk-load implemented directly for Boost R-tree

void
RTreeIndexWrapper::bulk_load_from_field_data(
    const std::vector<std::shared_ptr<::milvus::FieldDataBase>>& field_datas,
    bool nullable) {
    // Acquire write lock to protect rtree_ creation and modification
    std::unique_lock<std::shared_mutex> guard(rtree_mutex_);

    AssertInfo(is_build_mode_, "Cannot bulk load in load mode");

    // Initialize GEOS context for bulk operations. A transient allocation
    // failure here would otherwise silently drop EVERY row from the index --
    // throw so the build fails and can be retried instead.
    GEOSContextHandle_t ctx = GEOS_init_r();
    if (ctx == nullptr) {
        ThrowInfo(ErrorCode::MemAllocateFailed,
                  "Failed to initialize GEOS context for bulk load");
    }

    GEOSWKBReader* reader = GEOSWKBReader_create_r(ctx);
    if (reader == nullptr) {
        GEOS_finish_r(ctx);
        ThrowInfo(ErrorCode::MemAllocateFailed,
                  "Failed to create GEOS WKB reader for bulk load");
    }

    // NOTE: non-null rows with an empty or unparseable WKB payload are indexed
    // with a deterministic placeholder MBR (like the growing add_geometry
    // path), NOT dropped. Growing and sealed now agree -- neither write path
    // drops a row -- so the index row count stays in lockstep with the segment
    // rows on both, and exact refinement tolerates the placeholder
    // (Geometry::TryParseFromWkb -> skip) in every configuration, cache on or
    // off. A corrupt/empty row can never satisfy exact refinement, so the
    // placeholder is always refined out and correctness is unaffected; the only
    // cost is that an origin-covering query pays refinement for it (see the
    // add_geometry tradeoff note). Genuinely null rows are still skipped below.
    std::vector<Value> local_values;
    local_values.reserve(1024);
    int64_t absolute_offset = 0;
    const auto index_placeholder = [&](int64_t offset) {
        local_values.emplace_back(Box(Point(0, 0), Point(0, 0)), offset);
    };
    for (const auto& fd : field_datas) {
        const auto n = fd->get_num_rows();
        const bool is_nullable_effective = nullable || fd->IsNullable();
        for (int64_t i = 0; i < n; ++i, ++absolute_offset) {
            if (is_nullable_effective && !fd->is_valid(i)) {
                continue;
            }
            const auto* wkb_str =
                static_cast<const std::string*>(fd->RawValue(i));
            if (wkb_str == nullptr || wkb_str->empty()) {
                index_placeholder(absolute_offset);
                continue;
            }

            GEOSGeometry* geom = GEOSWKBReader_read_r(
                ctx,
                reader,
                reinterpret_cast<const unsigned char*>(wkb_str->data()),
                wkb_str->size());
            if (geom == nullptr) {
                index_placeholder(absolute_offset);
                continue;
            }

            // See add_geometry(): keep a deterministic placeholder MBR for a
            // geometry without a computable envelope so the row stays indexed
            // and the row count remains consistent.
            double minX = 0, minY = 0, maxX = 0, maxY = 0;
            if (!get_bounding_box(geom, ctx, minX, minY, maxX, maxY)) {
                LOG_WARN(
                    "geometry at row {} has no computable envelope (empty?); "
                    "indexing with a placeholder MBR",
                    absolute_offset);
            }
            GEOSGeom_destroy_r(ctx, geom);

            Box box(Point(minX, minY), Point(maxX, maxY));
            local_values.emplace_back(box, absolute_offset);
        }
    }

    // Clean up GEOS resources
    GEOSWKBReader_destroy_r(ctx, reader);
    GEOS_finish_r(ctx);
    values_.swap(local_values);
    rtree_ = RTree(values_.begin(), values_.end());
    LOG_INFO("R-Tree bulk load (Boost) completed with {} entries",
             values_.size());
}

void
RTreeIndexWrapper::finish() {
    // Acquire write lock to protect rtree_ modification and cleanup
    // Guard against repeated invocations which could otherwise attempt to
    // release resources multiple times (e.g. BuildWithRawDataForUT() calls
    // finish(), and Upload() may call it again).
    std::unique_lock<std::shared_mutex> guard(rtree_mutex_);
    if (finished_) {
        LOG_DEBUG("RTreeIndexWrapper::finish() called more than once, skip.");
        return;
    }

    AssertInfo(is_build_mode_, "Cannot finish in load mode");

    // Persist to disk: write meta and binary data file
    try {
        // Write binary rtree data
        RTreeSerializer::saveBinary(rtree_, index_path_ + ".bgi");

        // Write meta json
        nlohmann::json meta;
        meta["dimension"] = dimension_;
        meta["count"] = static_cast<uint64_t>(values_.size());

        std::ofstream ofs(index_path_ + ".meta.json", std::ios::trunc);
        if (ofs.fail()) {
            ThrowInfo(ErrorCode::FileOpenFailed,
                      "Failed to open R-Tree meta file: {}.meta.json",
                      index_path_);
        }
        if (!(ofs << meta.dump())) {
            ThrowInfo(ErrorCode::FileWriteFailed,
                      "Failed to write R-Tree meta file: {}.meta.json",
                      index_path_);
        }
        ofs.close();
        LOG_INFO("R-Tree meta written: {}.meta.json", index_path_);
    } catch (const std::exception& e) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  fmt::format("Failed to write R-Tree files: {}", e.what()));
    }

    finished_ = true;

    LOG_INFO("R-Tree index (Boost) finished building and saved to {}",
             index_path_);
}

void
RTreeIndexWrapper::load() {
    // Acquire write lock to protect rtree_ initialization during loading
    std::unique_lock<std::shared_mutex> guard(rtree_mutex_);

    AssertInfo(!is_build_mode_, "Cannot load in build mode");

    try {
        // Read meta (optional)
        try {
            std::ifstream ifs(index_path_ + ".meta.json");
            if (ifs.good()) {
                auto meta = nlohmann::json::parse(ifs);
                // index/leaf capacities are ignored for Boost implementation
                if (meta.contains("dimension"))
                    dimension_ = meta["dimension"].get<uint32_t>();
            }
        } catch (const std::exception& e) {
            LOG_WARN("Failed to read meta json: {}", e.what());
        }

        // Read binary data
        RTreeSerializer::loadBinary(rtree_, index_path_ + ".bgi");

        LOG_INFO("R-Tree index (Boost) loaded from {}", index_path_);
    } catch (const std::exception& e) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  fmt::format("Failed to load R-Tree index from {}: {}",
                              index_path_,
                              e.what()));
    }
}

void
RTreeIndexWrapper::query_candidates(proto::plan::GISFunctionFilterExpr_GISOp op,
                                    const GEOSGeometry* query_geom,
                                    GEOSContextHandle_t ctx,
                                    std::vector<int64_t>& candidate_offsets) {
    candidate_offsets.clear();

    // Get bounding box of query geometry. An empty/degenerate query geometry
    // has no envelope. For the spatial predicates (Intersects / Within /
    // Contains / Touches / Overlaps / Crosses) it intersects nothing, so there
    // are no candidates. ST_Equals is different: an empty query geometry must
    // still match empty FIELD geometries, which this index stores with a
    // placeholder MBR (see add_geometry) -- returning zero candidates for
    // Equals would be a false negative versus the un-indexed data path, where
    // GEOSEquals(empty, empty) is true. So for Equals, fall back to the full
    // candidate set and let exact refinement keep only the true matches.
    //
    // NOTE: this fallback is an INTENTIONAL full scan. A single
    // ST_Equals(field, 'POLYGON EMPTY') degenerates into an exact-refinement
    // pass over every indexed row of the segment. That cost is accepted to
    // preserve correctness; do not "optimize" the branch away without an
    // alternative way to find placeholder-MBR rows.
    double minX, minY, maxX, maxY;
    if (!get_bounding_box(query_geom, ctx, minX, minY, maxX, maxY)) {
        if (op == proto::plan::GISFunctionFilterExpr_GISOp_Equals) {
            std::shared_lock<std::shared_mutex> guard(rtree_mutex_);
            candidate_offsets.reserve(rtree_.size());
            for (const auto& v : rtree_) {
                candidate_offsets.push_back(v.second);
            }
        }
        return;
    }

    // Create query box
    Box query_box(Point(minX, minY), Point(maxX, maxY));

    // Perform coarse intersection query
    std::vector<Value> results;
    {
        std::shared_lock<std::shared_mutex> guard(rtree_mutex_);
        rtree_.query(boost::geometry::index::intersects(query_box),
                     std::back_inserter(results));
    }
    candidate_offsets.reserve(results.size());
    for (const auto& v : results) {
        candidate_offsets.push_back(v.second);
    }

    LOG_DEBUG("R-Tree query returned {} candidates for operation {}",
              candidate_offsets.size(),
              static_cast<int>(op));
}

bool
RTreeIndexWrapper::get_bounding_box(const GEOSGeometry* geom,
                                    GEOSContextHandle_t ctx,
                                    double& minX,
                                    double& minY,
                                    double& maxX,
                                    double& maxY) {
    AssertInfo(geom != nullptr, "Geometry is null");
    AssertInfo(ctx != nullptr, "GEOS context is null");

    // GEOSGeom_get{X,Y}{Min,Max}_r return 0 on failure (e.g. empty geometry)
    // and leave the output untouched; using such uninitialized coordinates
    // would insert a garbage MBR into the R-tree. Report failure instead.
    if (GEOSGeom_getXMin_r(ctx, geom, &minX) == 0 ||
        GEOSGeom_getXMax_r(ctx, geom, &maxX) == 0 ||
        GEOSGeom_getYMin_r(ctx, geom, &minY) == 0 ||
        GEOSGeom_getYMax_r(ctx, geom, &maxY) == 0) {
        return false;
    }
    return true;
}

int64_t
RTreeIndexWrapper::count() const {
    // rtree_ is mutated by add_geometry()/bulk_load_from_field_data() under a
    // write lock; reading its size concurrently must take the shared lock too,
    // otherwise a growing-segment search races with incremental inserts.
    std::shared_lock<std::shared_mutex> guard(rtree_mutex_);
    return static_cast<int64_t>(rtree_.size());
}

int64_t
RTreeIndexWrapper::ByteSize() const {
    std::shared_lock<std::shared_mutex> guard(rtree_mutex_);
    int64_t total = 0;

    // values_: vector<Value> where Value = std::pair<Box, int64_t>
    // Box = bg::model::box<Point> = 2 Points = 2 * 2 * sizeof(double) = 32 bytes
    // Value = Box + int64_t = 32 + 8 = 40 bytes
    total += values_.capacity() * sizeof(Value);

    // rtree_ internal structure (nodes, pointers, MBRs)
    // R*-tree with max 16 entries per node has overhead per entry
    // Estimated ~18 bytes per entry for internal tree structure
    total += rtree_.size() * 18;

    return total;
}

// index/leaf capacity setters removed; not applicable for Boost rtree
}  // namespace milvus::index