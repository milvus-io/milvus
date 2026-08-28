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

#include "common/EasyAssert.h"
#include "log/Log.h"
#include "pb/plan.pb.h"
#include <filesystem>
#include <fstream>
#include <mutex>
#include <new>
#include <nlohmann/json.hpp>
#include "common/FieldDataInterface.h"
#include "common/Geometry.h"
#include "RTreeIndexWrapper.h"
#include "RTreeIndexSerialization.h"

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
        entry_count_.store(0, std::memory_order_release);
    }
}

RTreeIndexWrapper::~RTreeIndexWrapper() = default;

void
RTreeIndexWrapper::ensure_rtree_consistent_locked() const {
    if (!rtree_needs_rebuild_) {
        return;
    }

    // Construct before swap so another allocation failure leaves the poisoned
    // tree isolated and the rebuild flag set for a later retry.
    try {
        RTree rebuilt(values_.begin(), values_.end());
        rtree_.swap(rebuilt);
        rtree_needs_rebuild_ = false;
    } catch (const std::bad_alloc&) {
        ThrowInfo(ErrorCode::MemAllocateFailed,
                  "out of memory rebuilding R-Tree after failed insert");
    }
}

std::shared_lock<folly::SharedMutexWritePriority>
RTreeIndexWrapper::lock_consistent_rtree_for_read() const {
    for (;;) {
        std::shared_lock<folly::SharedMutexWritePriority> read_guard(
            rtree_mutex_);
        if (!rtree_needs_rebuild_) {
            return read_guard;
        }

        read_guard.unlock();
        std::unique_lock<folly::SharedMutexWritePriority> write_guard(
            rtree_mutex_);
        ensure_rtree_consistent_locked();
    }
}

void
RTreeIndexWrapper::insert_value_locked(const Box& box, int64_t row_offset) {
    // values_ is the authoritative committed set; rtree_ is a derived
    // structure that Boost may leave inconsistent when insert throws (it
    // documents only the basic guarantee). So the two are kept in step by
    // rolling values_ back and flagging the tree for a rebuild -- NOT by
    // trusting the tree to be unmodified:
    //   1. values_.push_back may throw -- nothing else mutated yet;
    //   2. rtree_.insert may throw -- roll values_ back (pop_back, noexcept)
    //      and mark the tree for rebuild from values_ before its next use.
    Value val(box, row_offset);
    values_.push_back(val);
    try {
        rtree_.insert(val);
        if (throw_after_insert_for_testing_) {
            throw_after_insert_for_testing_ = false;
            throw std::bad_alloc();
        }
    } catch (...) {
        values_.pop_back();
        rtree_needs_rebuild_ = true;
        throw;
    }
    // Published last: the row is only committed once both structures accepted
    // it, so a lock-free count() never reports a row that was rolled back.
    entry_count_.store(static_cast<int64_t>(values_.size()),
                       std::memory_order_release);
}

void
RTreeIndexWrapper::add_geometry(const uint8_t* wkb_data,
                                size_t len,
                                int64_t row_offset) {
    // Acquire write lock to protect rtree_
    std::unique_lock<folly::SharedMutexWritePriority> guard(rtree_mutex_);

    AssertInfo(is_build_mode_, "Cannot add geometry in load mode");
    ensure_rtree_consistent_locked();

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
    // throw instead, so the insert fails and can be retried.
    //
    // Tradeoff: Point(0, 0) is a legal coordinate (Null Island), so any query
    // whose bounding box covers the origin pulls every placeholder row in this
    // segment into the candidate set and pays exact refinement for it (which
    // then discards the row). World-scale bbox queries almost always cover the
    // origin, so segments with many empty/corrupt geometries make such queries
    // proportionally more expensive. Correctness is unaffected.
    auto index_placeholder_mbr = [&]() {
        insert_value_locked(Box(Point(0, 0), Point(0, 0)), row_offset);
    };

    // Parse WKB data using GEOS for consistency. InitGEOSContext throws a
    // retriable MemAllocateFailed on a transient allocation failure, so a
    // valid geometry fails the insert (and can be retried) rather than being
    // permanently mis-indexed. All GEOS resources are scoped: if any
    // container op below throws (retried by the caller), nothing leaks.
    ScopedGeosResources geos("add_geometry");

    geos.reader = GEOSWKBReader_create_r(geos.ctx);
    if (geos.reader == nullptr) {
        // Transient resource failure -- see above; throw rather than placeholder.
        ThrowInfo(ErrorCode::MemAllocateFailed,
                  "Failed to create GEOS WKB reader for row {}",
                  row_offset);
    }

    geos.geom = GEOSWKBReader_read_r(geos.ctx, geos.reader, wkb_data, len);

    if (geos.geom == nullptr) {
        // nullptr here is *usually* unparseable WKB, but GEOS's execute()
        // wrapper also swallows a transient OOM during parsing into the same
        // nullptr -- the two are indistinguishable at this boundary, and both
        // are deliberately classified as bad data (see the KNOWN LIMIT note
        // on Geometry::TryParseFromWkb).
        static std::atomic<int64_t> last_parse_log_us{0};
        if (ShouldLogGeometryThrottled(last_parse_log_us)) {
            LOG_ERROR(
                "Failed to parse WKB data for row {}; indexing a placeholder "
                "MBR to keep the index row count consistent (further "
                "occurrences suppressed briefly)",
                row_offset);
        } else {
            LOG_DEBUG("Failed to parse WKB data for row {}", row_offset);
        }
        index_placeholder_mbr();
        return;
    }

    // Get bounding box. On failure (e.g. empty geometry) keep a deterministic
    // placeholder MBR and still index the row: the R-tree is only a coarse
    // filter, the exact predicate refines it out, and dropping the row here
    // would desynchronize the index row count from the segment row count.
    double minX = 0, minY = 0, maxX = 0, maxY = 0;
    if (!get_bounding_box(geos.geom, geos.ctx, minX, minY, maxX, maxY)) {
        static std::atomic<int64_t> last_envelope_log_us{0};
        if (ShouldLogGeometryThrottled(last_envelope_log_us)) {
            LOG_WARN(
                "geometry at row {} has no computable envelope (empty?); "
                "indexing with a placeholder MBR, exact refinement will "
                "filter it (further occurrences suppressed briefly)",
                row_offset);
        } else {
            LOG_DEBUG("geometry at row {} has no computable envelope",
                      row_offset);
        }
    }
    // The geometry is no longer needed once the MBR is extracted; release it
    // before the (potentially throwing) container ops.
    geos.release_geom();

    // Create Boost box and insert (idempotent per offset; a throwing Boost tree
    // is rebuilt from committed values before reuse).
    insert_value_locked(Box(Point(minX, minY), Point(maxX, maxY)), row_offset);
}

// No IDataStream; bulk-load implemented directly for Boost R-tree

void
RTreeIndexWrapper::bulk_load_from_field_data(
    const std::vector<std::shared_ptr<::milvus::FieldDataBase>>& field_datas,
    bool nullable) {
    // Acquire write lock to protect rtree_ creation and modification
    std::unique_lock<folly::SharedMutexWritePriority> guard(rtree_mutex_);

    AssertInfo(is_build_mode_, "Cannot bulk load in load mode");

    // Initialize GEOS context for bulk operations. A transient allocation
    // failure here would otherwise silently drop EVERY row from the index --
    // InitGEOSContext throws a retriable error so the build fails and can be
    // retried instead. Scoped so a throwing container op inside the loop
    // (e.g. local_values.emplace_back on OOM) cannot leak the reader/context
    // across the retry.
    ScopedGeosResources geos("bulk load");

    geos.reader = GEOSWKBReader_create_r(geos.ctx);
    if (geos.reader == nullptr) {
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

            geos.geom = GEOSWKBReader_read_r(
                geos.ctx,
                geos.reader,
                reinterpret_cast<const unsigned char*>(wkb_str->data()),
                wkb_str->size());
            if (geos.geom == nullptr) {
                // Same classification as add_geometry(): unparseable WKB and
                // a GEOS-swallowed parse-time OOM both surface as nullptr and
                // both get a placeholder MBR (see Geometry::TryParseFromWkb).
                index_placeholder(absolute_offset);
                continue;
            }

            // See add_geometry(): keep a deterministic placeholder MBR for a
            // geometry without a computable envelope so the row stays indexed
            // and the row count remains consistent.
            double minX = 0, minY = 0, maxX = 0, maxY = 0;
            if (!get_bounding_box(
                    geos.geom, geos.ctx, minX, minY, maxX, maxY)) {
                static std::atomic<int64_t> last_bulk_envelope_log_us{0};
                if (ShouldLogGeometryThrottled(last_bulk_envelope_log_us)) {
                    LOG_WARN(
                        "geometry at row {} has no computable envelope "
                        "(empty?); indexing with a placeholder MBR (further "
                        "occurrences suppressed briefly)",
                        absolute_offset);
                } else {
                    LOG_DEBUG("geometry at row {} has no computable envelope",
                              absolute_offset);
                }
            }
            // Release before the (potentially throwing) emplace_back.
            geos.release_geom();

            Box box(Point(minX, minY), Point(maxX, maxY));
            local_values.emplace_back(box, absolute_offset);
        }
    }

    // Publish transactionally: build the tree from the staged values FIRST
    // (its bulk ctor can throw bad_alloc), then install both structures with
    // non-throwing swap/move -- a failed attempt leaves values_/rtree_
    // untouched, so the retried bulk load starts from a consistent state.
    RTree new_tree(local_values.begin(), local_values.end());
    values_.swap(local_values);
    rtree_.swap(new_tree);
    rtree_needs_rebuild_ = false;
    entry_count_.store(static_cast<int64_t>(values_.size()),
                       std::memory_order_release);
    LOG_INFO("R-Tree bulk load (Boost) completed with {} entries",
             values_.size());
}

void
RTreeIndexWrapper::finish() {
    // Acquire write lock to protect rtree_ modification and cleanup
    // Guard against repeated invocations which could otherwise attempt to
    // release resources multiple times (e.g. BuildWithRawDataForUT() calls
    // finish(), and Upload() may call it again).
    std::unique_lock<folly::SharedMutexWritePriority> guard(rtree_mutex_);
    if (finished_) {
        LOG_DEBUG("RTreeIndexWrapper::finish() called more than once, skip.");
        return;
    }

    AssertInfo(is_build_mode_, "Cannot finish in load mode");
    ensure_rtree_consistent_locked();

    // Persist to disk: write meta and binary data file
    try {
        // Write binary rtree data. The serializer reports failures instead of
        // throwing, so checking its result is part of the persistence
        // contract: an index without a durable tree must never be uploaded as
        // successfully built.
        auto binary_path = index_path_ + ".bgi";
        auto save_result = RTreeSerializer::saveBinary(rtree_, binary_path);
        if (save_result != RTreeSerializer::BinaryIOResult::Success) {
            ThrowInfo(ErrorCode::FileWriteFailed,
                      "Failed to write R-Tree binary file: {}",
                      binary_path);
        }

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
        // Check close(), not just the insertion above: a delayed-allocation
        // filesystem reports ENOSPC/EIO for the flushed blocks at close(2),
        // and an unchecked close leaves a truncated meta.json looking written.
        ofs.close();
        if (!ofs.good()) {
            ThrowInfo(ErrorCode::FileWriteFailed,
                      "Failed to close R-Tree meta file: {}.meta.json",
                      index_path_);
        }
        LOG_INFO("R-Tree meta written: {}.meta.json", index_path_);
    } catch (const SegcoreError&) {
        throw;
    } catch (const std::bad_alloc&) {
        ThrowInfo(ErrorCode::MemAllocateFailed,
                  "out of memory writing R-Tree files for {}",
                  index_path_);
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
    std::unique_lock<folly::SharedMutexWritePriority> guard(rtree_mutex_);

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
        } catch (const std::bad_alloc&) {
            throw;
        } catch (const std::exception& e) {
            LOG_WARN("Failed to read meta json: {}", e.what());
        }

        // Deserialize into a temporary tree. A truncated archive can mutate
        // its destination before reporting failure; only swap it into the live
        // wrapper after the whole archive has been validated.
        auto binary_path = index_path_ + ".bgi";
        RTree loaded;
        auto load_result = RTreeSerializer::loadBinary(loaded, binary_path);
        if (load_result == RTreeSerializer::BinaryIOResult::OpenFailed ||
            load_result == RTreeSerializer::BinaryIOResult::StreamFailed) {
            ThrowInfo(ErrorCode::FileReadFailed,
                      "Failed to read R-Tree binary file: {}",
                      binary_path);
        }
        if (load_result == RTreeSerializer::BinaryIOResult::ArchiveFailed) {
            ThrowInfo(ErrorCode::DataFormatBroken,
                      "Corrupt R-Tree binary file: {}",
                      binary_path);
        }
        rtree_.swap(loaded);
        rtree_needs_rebuild_ = false;
        // The load path deserializes straight into the tree and never fills
        // values_, so the tree is the only row-count source here.
        entry_count_.store(static_cast<int64_t>(rtree_.size()),
                           std::memory_order_release);

        LOG_INFO("R-Tree index (Boost) loaded from {}", index_path_);
    } catch (const SegcoreError&) {
        throw;
    } catch (const std::bad_alloc&) {
        ThrowInfo(ErrorCode::MemAllocateFailed,
                  "out of memory loading R-Tree index from {}",
                  index_path_);
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
    if (throw_on_query_for_testing_.exchange(false)) {
        throw std::bad_alloc();
    }

    candidate_offsets.clear();

    // The shared lock below is taken ONLY around the statements that touch
    // rtree_. Everything else here -- the envelope of the caller's query
    // geometry, and copying offsets out of the local `results` -- is
    // thread-private, and holding the lock across it would stretch the read
    // critical section (the copy is proportional to the candidate count, so on
    // a large growing segment it is the dominant part) for no benefit. Readers
    // that hold the lock longer than necessary are exactly what makes the
    // per-row insert lock hard to acquire.
    //
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
            auto tree_guard = lock_consistent_rtree_for_read();
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
        auto tree_guard = lock_consistent_rtree_for_read();
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
    // Deliberately lock-free. rtree_ is mutated by add_geometry() /
    // bulk_load_from_field_data() / load() under the exclusive lock, so
    // reading rtree_.size() would need the shared lock -- and every
    // growing-segment search calls this once (RTreeIndex::Query sizes its
    // bitmap by Count()), so that acquisition lands squarely on the search
    // path and adds to the read pressure the insert thread has to push
    // through. entry_count_ is published by each of those mutation points
    // while they hold the lock, which is all a row count needs.
    return entry_count_.load(std::memory_order_acquire);
}

int64_t
RTreeIndexWrapper::ByteSize() const {
    auto guard = lock_consistent_rtree_for_read();
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

void
RTreeIndexWrapper::SetThrowAfterInsertForTesting(bool enabled) {
    std::unique_lock<folly::SharedMutexWritePriority> guard(rtree_mutex_);
    throw_after_insert_for_testing_ = enabled;
}

void
RTreeIndexWrapper::SetThrowOnQueryForTesting(bool enabled) {
    throw_on_query_for_testing_.store(enabled);
}

// index/leaf capacity setters removed; not applicable for Boost rtree
}  // namespace milvus::index
