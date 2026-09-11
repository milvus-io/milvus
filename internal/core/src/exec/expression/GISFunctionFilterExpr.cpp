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

#include "GISFunctionFilterExpr.h"

#include <fmt/core.h>
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <iosfwd>
#include <optional>
#include <string_view>

#include "bitset/bitset.h"
#include "bitset/detail/element_vectorized.h"
#include "common/EasyAssert.h"
#include "common/Geometry.h"
#include "common/GeometryCache.h"
#include "common/OpContext.h"
#include "common/PreparedGeometry.h"
#include "common/Types.h"
#include "common/Utils.h"
#include "geos_c.h"
#include "index/Index.h"
#include "index/Meta.h"
#include "index/ScalarIndex.h"
#include "log/Log.h"
#include "knowhere/dataset.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"
#include "storage/MmapManager.h"
#include "storage/Types.h"

namespace milvus {
namespace exec {

void
PhyGISFunctionFilterExpr::DetermineExecPath() {
    SegmentExpr::DetermineExecPath();
    if (exec_path_ != ExprExecPath::ScalarIndex) {
        return;
    }
    // STIsValid operation cannot use index
    if (expr_->op_ == proto::plan::GISFunctionFilterExpr_GISOp_STIsValid) {
        exec_path_ = ExprExecPath::RawData;
    }
}

void
PhyGISFunctionFilterExpr::Eval(EvalCtx& context, VectorPtr& result) {
    WaitPrefetch();
    AssertInfo(expr_->column_.data_type_ == DataType::GEOMETRY,
               "unsupported data type: {}",
               expr_->column_.data_type_);
    if (exec_path_ == ExprExecPath::ScalarIndex) {
        result = EvalForIndexSegment();
    } else {
        result = EvalForDataSegment(context);
    }
}

VectorPtr
PhyGISFunctionFilterExpr::EvalForDataSegment(EvalCtx& context) {
    AssertInfo(context.get_offset_input() == nullptr,
               "GIS raw-data path does not support offset input");
    const auto op = expr_->op_;
    std::optional<Geometry> right_source;
    switch (op) {
        case proto::plan::GISFunctionFilterExpr_GISOp_STIsValid:
            break;
        case proto::plan::GISFunctionFilterExpr_GISOp_Equals:
        case proto::plan::GISFunctionFilterExpr_GISOp_Touches:
        case proto::plan::GISFunctionFilterExpr_GISOp_Overlaps:
        case proto::plan::GISFunctionFilterExpr_GISOp_Crosses:
        case proto::plan::GISFunctionFilterExpr_GISOp_Contains:
        case proto::plan::GISFunctionFilterExpr_GISOp_Intersects:
        case proto::plan::GISFunctionFilterExpr_GISOp_Within:
        case proto::plan::GISFunctionFilterExpr_GISOp_DWithin:
            right_source.emplace(GetThreadLocalGEOSContext(),
                                 expr_->geometry_wkt_.c_str());
            break;
        default:
            ThrowInfo(NotImplemented,
                      "internal error: unknown GIS op : {}",
                      static_cast<int>(op));
    }
    const Geometry* right = right_source ? &*right_source : nullptr;
    auto geometry_cache = segment_->GetGeometryCache(field_id_);

    // Growing segments without mmap store std::string; sealed segments and
    // growing segments with mmap expose std::string_view.
    if (segment_->type() == SegmentType::Growing &&
        !storage::MmapManager::GetInstance()
             .GetMmapConfig()
             .growing_enable_mmap) {
        return EvalKernel<std::string>(
            context,
            GeometryScanKernel<std::string>{
                op, right, expr_->distance_, std::move(geometry_cache)},
            /*element_level=*/false);
    }
    return EvalKernel<std::string_view>(
        context,
        GeometryScanKernel<std::string_view>{
            op, right, expr_->distance_, std::move(geometry_cache)},
        /*element_level=*/false);
}

// Helper function to calculate bounding box for range_within query optimization
// Creates a rectangular bounding box around a query point with given distance in meters
static Geometry
create_bounding_box_for_dwithin(GEOSContextHandle_t ctx,
                                const Geometry& query_point,
                                double distance_meters) {
    double query_lon, query_lat;

    AssertInfo(GEOSGeomGetX_r(ctx, query_point.GetGeometry(), &query_lon) == 1,
               "Failed to get X coordinate from query point");
    AssertInfo(GEOSGeomGetY_r(ctx, query_point.GetGeometry(), &query_lat) == 1,
               "Failed to get Y coordinate from query point");

    const double metersPerDegreeLat = 111320.0;

    // Calculate latitude offset (relatively constant)
    double latOffset = distance_meters / metersPerDegreeLat;

    // Calculate longitude offset (varies with latitude)
    double latRad = query_lat * M_PI / 180.0;
    double lonOffset =
        distance_meters / (metersPerDegreeLat * std::cos(latRad));

    // Calculate bounding box coordinates
    double minLon = query_lon - lonOffset;
    double maxLon = query_lon + lonOffset;
    double minLat = query_lat - latOffset;
    double maxLat = query_lat + latOffset;

    // Create WKT POLYGON for bounding box
    std::string bboxWKT = fmt::format(
        "POLYGON(({:.6f} {:.6f}, {:.6f} {:.6f}, {:.6f} {:.6f}, {:.6f} {:.6f}, "
        "{:.6f} {:.6f}))",
        minLon,
        minLat,  // Bottom-left
        maxLon,
        minLat,  // Bottom-right
        maxLon,
        maxLat,  // Top-right
        minLon,
        maxLat,  // Top-left
        minLon,
        minLat  // Close the ring
    );

    return Geometry(ctx, bboxWKT.c_str());
}

VectorPtr
PhyGISFunctionFilterExpr::EvalForIndexSegment() {
    AssertInfo(num_index_chunk_ == 1, "num_index_chunk_ should be 1");
    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    // Use thread-local GEOS context for thread safety - segment_->get_ctx() is shared
    // and not safe for concurrent access from multiple query threads
    GEOSContextHandle_t ctx = GetThreadLocalGEOSContext();

    Geometry query_geometry = Geometry(ctx, expr_->geometry_wkt_.c_str());

    // Prepare the query geometry once for accelerated repeated predicate evaluation.
    PreparedGeometry prepared_query(ctx, query_geometry);

    /* ------------------------------------------------------------------
     * Prefetch: if coarse results are not cached yet, run a single R-Tree
     * query for all index chunks and cache their coarse bitmaps.
     * ------------------------------------------------------------------*/

    // Evaluate geometry operation using PreparedGeometry for supported operations.
    // Note on predicate semantics when using prepared query:
    // - Symmetric predicates (intersects, touches, overlaps, crosses): prepared_query.op(left) == left.op(query)
    // - contains/within swap: left.contains(query) == prepared_query.within(left)
    //                         left.within(query) == prepared_query.contains(left)
    // - equals, dwithin: no prepared version, fall back to regular Geometry
    // `left` is frequently a cache-owned geometry whose stored context is
    // shared across concurrent queries, so the unprepared fallbacks inside the
    // helper must run on the per-thread context captured above rather than on
    // left's own context.
    auto evaluate_geometry_prepared = [this,
                                       &prepared_query,
                                       &query_geometry,
                                       ctx](const Geometry& left) -> bool {
        return EvaluateGISPreparedOp(expr_->op_,
                                     prepared_query,
                                     query_geometry,
                                     left,
                                     expr_->distance_,
                                     ctx);
    };

    TargetBitmap batch_result;
    TargetBitmap batch_valid;
    int processed_rows = 0;

    if (!coarse_cached_) {
        using Index = index::ScalarIndex<std::string>;

        // Prepare shared dataset for index query (coarse candidate set by R-Tree)
        auto ds = std::make_shared<milvus::Dataset>();
        ds->Set(milvus::index::OPERATOR_TYPE, expr_->op_);

        // For range_within operations, use bounding box for coarse filtering
        if (expr_->op_ == proto::plan::GISFunctionFilterExpr_GISOp_DWithin) {
            // Create bounding box geometry for index coarse filtering
            Geometry bbox_geometry = create_bounding_box_for_dwithin(
                ctx, query_geometry, expr_->distance_);

            ds->Set(milvus::index::MATCH_VALUE, bbox_geometry);

            // Note: Distance is not used for bounding box intersection query
        } else {
            // For other operations, use original geometry
            ds->Set(milvus::index::MATCH_VALUE, query_geometry);
        }

        // Query segment-level R-Tree index **once** since each chunk shares the same index
        auto scalar_index = dynamic_cast<const Index*>(pinned_index_[0].get());
        auto* idx_ptr = const_cast<Index*>(scalar_index);

        {
            auto tmp = idx_ptr->Query(ds);
            coarse_global_ = std::move(tmp);
        }
        // Self-heal an index that reports fewer rows than the segment holds.
        //
        // RTreeIndex::Load recomputes the row count from the deserialized
        // tree, so an index built before empty/unparseable geometries were
        // kept as placeholder entries under-reports the segment row space.
        // Those old builders advanced absolute_offset even when they dropped
        // a row, so the missing entries can be interior holes rather than a
        // trailing suffix. Count() reveals how many entries are missing, not
        // where they were. Once the index is short, therefore, no bit in its
        // candidate bitmap is trustworthy as a negative: promote the entire
        // active row space to candidates and let exact refinement settle it.
        //
        // The INDEX validity bitmap cannot be filled with true, though:
        // Count() is an entry count while null offsets are absolute row ids, so
        // a genuine NULL beyond Count() is absent from parameterless
        // IsNotNull(). Filling it with true would turn NULL into non-NULL and
        // make NOT ST_* return it as a match. Ask the index to project its
        // absolute null offsets into the segment row space instead.
        //
        // Full refinement beats asserting here: this is an expected upgrade state,
        // not a Milvus bug, and failing every geometry query on the segment
        // until someone manually rebuilds the index is a far worse outcome
        // than a slightly slower correct answer.
        //
        // The promotion rule itself lives in PromoteShortGISCoarseBitmap so
        // the fusion coarse path (PhyGISCoarseConjunctExpr::RunRTreeQuery)
        // applies the identical rule instead of a tail-only pad.
        auto coarse_rows = static_cast<int64_t>(coarse_global_.size());
        if (PromoteShortGISCoarseBitmap(coarse_global_, active_count_)) {
            static std::atomic<int64_t> last_short_index_log_us{0};
            if (ShouldLogGeometryThrottled(last_short_index_log_us)) {
                LOG_WARN(
                    "R-Tree index for field {} reports {} rows but the "
                    "segment holds {}; treating all segment rows as "
                    "candidates for exact refinement because the {} missing "
                    "entries may be interior holes. This index predates "
                    "placeholder-MBR indexing of empty/unparseable "
                    "geometries. Every geometry query on this segment now "
                    "refines the whole column (read in {}-row batches) "
                    "instead of R-Tree candidates -- rebuild the index to "
                    "restore pruning (further occurrences suppressed "
                    "briefly).",
                    field_id_.get(),
                    coarse_rows,
                    active_count_,
                    active_count_ - coarse_rows,
                    batch_size_);
            }

            // null_offset_ uses absolute segment row ids, while Count() on a
            // legacy index can under-report after older builders dropped
            // non-null empty/corrupt rows. Ask the R-Tree to lay validity out
            // directly in the segment row space so every absolute NULL offset
            // survives independently of the short entry count.
            coarse_valid_global_ = idx_ptr->IsNotNull(active_count_);
        } else {
            coarse_valid_global_ = idx_ptr->IsNotNull();
        }

        coarse_cached_ = true;
    }

    if (cached_index_chunk_res_ == nullptr) {
        // Reuse segment-level coarse cache directly
        auto& coarse = coarse_global_;
        // Exact refinement with lambda functions for code reuse
        TargetBitmap refined(coarse.size());

        // Lambda: Evaluate geometry operation (shared by both segment types)

        // Lambda: Collect hit offsets from coarse bitmap
        auto collect_hits = [&coarse]() -> std::vector<int64_t> {
            std::vector<int64_t> hit_offsets;
            hit_offsets.reserve(coarse.count());
            for (size_t i = 0; i < coarse.size(); ++i) {
                if (coarse[i]) {
                    hit_offsets.emplace_back(static_cast<int64_t>(i));
                }
            }
            return hit_offsets;
        };

        // Lambda: Process sealed segment data using bulk_subscript with SimpleGeometryCache
        auto process_sealed_data = [&](const std::vector<int64_t>&
                                           hit_offsets) {
            if (hit_offsets.empty())
                return;

            // Get simple geometry cache for this segment+field
            auto geometry_cache = segment_->GetGeometryCache(field_id_);
            if (geometry_cache) {
                auto cache_lock = geometry_cache->AcquireReadLock();
                for (size_t i = 0; i < hit_offsets.size(); ++i) {
                    const auto pos = hit_offsets[i];

                    auto cached_geometry =
                        geometry_cache->GetByOffsetUnsafe(pos);
                    // skip invalid geometry
                    if (cached_geometry == nullptr) {
                        continue;
                    }
                    // Use prepared geometry for faster evaluation
                    bool result = evaluate_geometry_prepared(*cached_geometry);

                    if (result) {
                        refined.set(pos);
                    }
                }
            } else {
                // Read the candidates in batch_size_-row groups rather than
                // one bulk_subscript over every hit. A single call
                // materializes one GeometryArray holding a copy of every
                // candidate's WKB; when the candidate set is the whole
                // segment -- the legacy short-index self-heal above, or a
                // globe-covering query bbox -- that is a full-column copy
                // (hundreds of MB on a million-row segment) held for the
                // duration of refinement, per query, per concurrent thread.
                // Chunking bounds the live copy to one group at a time with
                // no change in results: every hit is still read exactly once,
                // in offset order.
                GEOSContextHandle_t local_ctx = GetThreadLocalGEOSContext();
                // batch_size_ > 0 is asserted by SegmentExpr's ctor.
                const size_t group_rows = static_cast<size_t>(batch_size_);
                for (size_t begin = 0; begin < hit_offsets.size();
                     begin += group_rows) {
                    const size_t count =
                        std::min(group_rows, hit_offsets.size() - begin);
                    auto data_array = segment_->bulk_subscript(
                        op_ctx_, field_id_, hit_offsets.data() + begin, count);

                    auto geometry_array = static_cast<
                        const milvus::proto::schema::GeometryArray*>(
                        &data_array->scalars().geometry_data());
                    const auto& valid_data =
                        GetFieldDataRowValidData(*data_array);

                    for (size_t i = 0; i < count; ++i) {
                        const auto pos = hit_offsets[begin + i];

                        // Skip invalid data
                        if (!valid_data.empty() && !valid_data[i]) {
                            continue;
                        }

                        const auto& wkb_data = geometry_array->data(i);
                        Geometry left;
                        if (!left.TryParseFromWkb(
                                local_ctx, wkb_data.data(), wkb_data.size())) {
                            // Unparseable WKB -- e.g. a placeholder row that
                            // add_geometry / bulk_load keep (instead of
                            // dropping) to hold the index row count. It can
                            // never satisfy exact refinement, so skip it,
                            // mirroring the cache branch's
                            // GetByOffsetUnsafe() == nullptr skip above.
                            // MUST NOT throw: with the geometry cache off
                            // (the default), such rows reach refinement as
                            // R-Tree candidates whenever the query bbox
                            // covers the placeholder MBR at the origin, and
                            // the throwing Geometry(ctx, wkb) ctor would fail
                            // the entire query.
                            continue;
                        }
                        // Use prepared geometry for faster evaluation
                        bool result = evaluate_geometry_prepared(left);

                        if (result) {
                            refined.set(pos);
                        }
                    }
                }
            }
        };

        auto hit_offsets = collect_hits();
        process_sealed_data(hit_offsets);

        // Cache refined result for reuse by subsequent batches
        cached_index_chunk_res_ =
            std::make_shared<TargetBitmap>(std::move(refined));
    }

    if (segment_->type() == SegmentType::Sealed) {
        auto data_pos = current_index_chunk_pos_;
        // Batch size is driven by the SEGMENT's rows, deliberately not
        // clamped to the coarse bitmap's length. Clamping here would silently
        // truncate the batch whenever the index is short, which then trips
        // the trailing processed_rows == real_batch_size assertion with a
        // generic message and makes the diagnostic below unreachable (its
        // predicate would be satisfied by construction).
        auto size =
            std::min(size_per_chunk_ - data_pos, batch_size_ - processed_rows);

        // Backstop only. The known cause of a short coarse bitmap -- a
        // legacy index that under-reports its row count -- is expanded to the
        // full active row space where the bitmap is built, so reaching this means
        // something else is wrong. TargetBitmap::append range-checks only its
        // start offset, never start + count, so an unchecked short bitmap
        // would be read past the end.
        AssertInfo(
            data_pos >= 0 && size >= 0 &&
                data_pos + size <=
                    static_cast<int64_t>(cached_index_chunk_res_->size()) &&
                data_pos + size <=
                    static_cast<int64_t>(coarse_valid_global_.size()),
            "sealed geometry coarse bitmap too small: pos {} + size {} "
            "exceeds result {} / valid {} even after padding the coarse "
            "bitmap to the segment's active row count.",
            data_pos,
            size,
            cached_index_chunk_res_->size(),
            coarse_valid_global_.size());

        batch_result.append(*cached_index_chunk_res_, data_pos, size);
        batch_valid.append(coarse_valid_global_, data_pos, size);
        processed_rows += size;
        current_index_chunk_pos_ += size;
    } else {
        for (size_t i = current_data_chunk_; i < num_data_chunk_; i++) {
            auto data_pos =
                (i == current_data_chunk_) ? current_data_chunk_pos_ : 0;
            int64_t size = ChunkSize(field_id_, i) - data_pos;
            size = std::min(size, real_batch_size - processed_rows);

            if (size > 0) {
                // The coarse bitmaps are sized by the index row count
                // (RTreeIndex::Count()), while `size` is driven by the
                // segment's active rows. The Insert path indexes a row
                // (AppendingIndex) before the ack-responder makes it
                // searchable, and neither write path drops a row -- both
                // add_geometry and bulk_load_from_field_data index a
                // placeholder MBR for empty/unparseable WKB rather than
                // dropping it -- so active_count <=
                // index Count() must always hold. Guard it explicitly: a violated
                // invariant must surface as a clear error, never as an
                // out-of-bounds read or fabricated results (a row reported
                // result=false is a silent wrong answer, and flips to a false
                // positive under a negated predicate).
                AssertInfo(
                    static_cast<int64_t>(current_index_chunk_pos_ + size) <=
                            static_cast<int64_t>(
                                cached_index_chunk_res_->size()) &&
                        static_cast<int64_t>(current_index_chunk_pos_ + size) <=
                            static_cast<int64_t>(coarse_valid_global_.size()),
                    "growing geometry coarse bitmap too small: pos {} + size "
                    "{} exceeds result {} / valid {} (index row count lagged "
                    "segment active rows)",
                    current_index_chunk_pos_,
                    size,
                    cached_index_chunk_res_->size(),
                    coarse_valid_global_.size());
                batch_result.append(
                    *cached_index_chunk_res_, current_index_chunk_pos_, size);
                batch_valid.append(
                    coarse_valid_global_, current_index_chunk_pos_, size);
            }
            // Update with actual processed size
            processed_rows += size;
            current_index_chunk_pos_ += size;

            if (processed_rows >= real_batch_size) {
                current_data_chunk_ = i;
                current_data_chunk_pos_ = data_pos + size;
                break;
            }
        }
    }

    AssertInfo(processed_rows == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               processed_rows,
               real_batch_size);
    AssertInfo(batch_result.size() == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               batch_result.size(),
               real_batch_size);
    AssertInfo(batch_valid.size() == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               batch_valid.size(),
               real_batch_size);
    if (segment_->type() != SegmentType::Sealed) {
        CommitLegacyDataProgress(processed_rows);
        AssertInfo(current_data_global_pos_ == current_index_chunk_pos_,
                   "growing GIS data cursor at {}, index cursor at {}",
                   current_data_global_pos_,
                   current_index_chunk_pos_);
    }
    return std::make_shared<ColumnVector>(std::move(batch_result),
                                          std::move(batch_valid));
}

}  //namespace exec
}  // namespace milvus
