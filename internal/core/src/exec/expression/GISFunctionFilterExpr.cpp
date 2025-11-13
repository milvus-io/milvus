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
#include <cstdlib>
#include "common/EasyAssert.h"
#include "common/Geometry.h"
#include "common/Types.h"
#include "pb/plan.pb.h"
#include <cmath>
#include <fmt/core.h>
namespace milvus {
namespace exec {

#define GEOMETRY_EXECUTE_SUB_BATCH_WITH_COMPARISON(_DataType, method)       \
    auto execute_sub_batch = [this](const _DataType* data,                  \
                                    const bool* valid_data,                 \
                                    const int32_t* offsets,                 \
                                    const int32_t* segment_offsets,         \
                                    const int size,                         \
                                    TargetBitmapView res,                   \
                                    TargetBitmapView valid_res,             \
                                    const Geometry& right_source) {         \
        AssertInfo(segment_offsets != nullptr,                              \
                   "segment_offsets should not be nullptr");                \
        auto* geometry_cache =                                              \
            SimpleGeometryCacheManager::Instance().GetCache(                \
                this->segment_->get_segment_id(), field_id_);               \
        if (geometry_cache) {                                               \
            auto cache_lock = geometry_cache->AcquireReadLock();            \
            for (int i = 0; i < size; ++i) {                                \
                if (valid_data != nullptr && !valid_data[i]) {              \
                    res[i] = valid_res[i] = false;                          \
                    continue;                                               \
                }                                                           \
                auto absolute_offset = segment_offsets[i];                  \
                auto cached_geometry =                                      \
                    geometry_cache->GetByOffsetUnsafe(absolute_offset);     \
                AssertInfo(cached_geometry != nullptr,                      \
                           "cached geometry is nullptr");                   \
                res[i] = cached_geometry->method(right_source);             \
            }                                                               \
        } else {                                                            \
            GEOSContextHandle_t ctx_ = GEOS_init_r();                       \
            for (int i = 0; i < size; ++i) {                                \
                if (valid_data != nullptr && !valid_data[i]) {              \
                    res[i] = valid_res[i] = false;                          \
                    continue;                                               \
                }                                                           \
                res[i] = Geometry(ctx_, data[i].data(), data[i].size())     \
                             .method(right_source);                         \
            }                                                               \
            GEOS_finish_r(ctx_);                                            \
        }                                                                   \
    };                                                                      \
    int64_t processed_size = ProcessDataChunks<_DataType, true>(            \
        execute_sub_batch, std::nullptr_t{}, res, valid_res, right_source); \
    AssertInfo(processed_size == real_batch_size,                           \
               "internal error: expr processed rows {} not equal "          \
               "expect batch size {}",                                      \
               processed_size,                                              \
               real_batch_size);                                            \
    return res_vec;
// Specialized macro for distance-based operations (ST_DWITHIN)
#define GEOMETRY_EXECUTE_SUB_BATCH_WITH_COMPARISON_DISTANCE(_DataType, method) \
    auto execute_sub_batch = [this](const _DataType* data,                     \
                                    const bool* valid_data,                    \
                                    const int32_t* offsets,                    \
                                    const int32_t* segment_offsets,            \
                                    const int size,                            \
                                    TargetBitmapView res,                      \
                                    TargetBitmapView valid_res,                \
                                    const Geometry& right_source) {            \
        AssertInfo(segment_offsets != nullptr,                                 \
                   "segment_offsets should not be nullptr");                   \
        auto* geometry_cache =                                                 \
            SimpleGeometryCacheManager::Instance().GetCache(                   \
                this->segment_->get_segment_id(), field_id_);                  \
        if (geometry_cache) {                                                  \
            auto cache_lock = geometry_cache->AcquireReadLock();               \
            for (int i = 0; i < size; ++i) {                                   \
                if (valid_data != nullptr && !valid_data[i]) {                 \
                    res[i] = valid_res[i] = false;                             \
                    continue;                                                  \
                }                                                              \
                auto absolute_offset = segment_offsets[i];                     \
                auto cached_geometry =                                         \
                    geometry_cache->GetByOffsetUnsafe(absolute_offset);        \
                AssertInfo(cached_geometry != nullptr,                         \
                           "cached geometry is nullptr");                      \
                res[i] =                                                       \
                    cached_geometry->method(right_source, expr_->distance_);   \
            }                                                                  \
        } else {                                                               \
            GEOSContextHandle_t ctx_ = GEOS_init_r();                          \
            for (int i = 0; i < size; ++i) {                                   \
                if (valid_data != nullptr && !valid_data[i]) {                 \
                    res[i] = valid_res[i] = false;                             \
                    continue;                                                  \
                }                                                              \
                res[i] = Geometry(ctx_, data[i].data(), data[i].size())        \
                             .method(right_source, expr_->distance_);          \
            }                                                                  \
            GEOS_finish_r(ctx_);                                               \
        }                                                                      \
    };                                                                         \
    int64_t processed_size = ProcessDataChunks<_DataType, true>(               \
        execute_sub_batch, std::nullptr_t{}, res, valid_res, right_source);    \
    AssertInfo(processed_size == real_batch_size,                              \
               "internal error: expr processed rows {} not equal "             \
               "expect batch size {}",                                         \
               processed_size,                                                 \
               real_batch_size);                                               \
    return res_vec;
void
PhyGISFunctionFilterExpr::Eval(EvalCtx& context, VectorPtr& result) {
    AssertInfo(expr_->column_.data_type_ == DataType::GEOMETRY,
               "unsupported data type: {}",
               expr_->column_.data_type_);
    if (SegmentExpr::CanUseIndex()) {
        result = EvalForIndexSegment();
    } else {
        result = EvalForDataSegment();
    }
}

VectorPtr
PhyGISFunctionFilterExpr::EvalForDataSegment() {
    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }
    auto res_vec = std::make_shared<ColumnVector>(
        TargetBitmap(real_batch_size), TargetBitmap(real_batch_size));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);
    valid_res.set();

    auto right_source =
        Geometry(segment_->get_ctx(), expr_->geometry_wkt_.c_str());

    // Choose underlying data type according to segment type to avoid element
    // size mismatch: Sealed segments and growing segments with mmap use std::string_view;
    // Growing segments without mmap use std::string.
    switch (expr_->op_) {
        case proto::plan::GISFunctionFilterExpr_GISOp_Equals: {
            if (segment_->type() == SegmentType::Growing &&
                !storage::MmapManager::GetInstance()
                     .GetMmapConfig()
                     .growing_enable_mmap) {
                GEOMETRY_EXECUTE_SUB_BATCH_WITH_COMPARISON(std::string, equals);
            } else {
                GEOMETRY_EXECUTE_SUB_BATCH_WITH_COMPARISON(std::string_view,
                                                           equals);
            }
        }
        case proto::plan::GISFunctionFilterExpr_GISOp_Touches: {
            if (segment_->type() == SegmentType::Growing &&
                !storage::MmapManager::GetInstance()
                     .GetMmapConfig()
                     .growing_enable_mmap) {
                GEOMETRY_EXECUTE_SUB_BATCH_WITH_COMPARISON(std::string,
                                                           touches);
            } else {
                GEOMETRY_EXECUTE_SUB_BATCH_WITH_COMPARISON(std::string_view,
                                                           touches);
            }
        }
        case proto::plan::GISFunctionFilterExpr_GISOp_Overlaps: {
            if (segment_->type() == SegmentType::Growing &&
                !storage::MmapManager::GetInstance()
                     .GetMmapConfig()
                     .growing_enable_mmap) {
                GEOMETRY_EXECUTE_SUB_BATCH_WITH_COMPARISON(std::string,
                                                           overlaps);
            } else {
                GEOMETRY_EXECUTE_SUB_BATCH_WITH_COMPARISON(std::string_view,
                                                           overlaps);
            }
        }
        case proto::plan::GISFunctionFilterExpr_GISOp_Crosses: {
            if (segment_->type() == SegmentType::Growing &&
                !storage::MmapManager::GetInstance()
                     .GetMmapConfig()
                     .growing_enable_mmap) {
                GEOMETRY_EXECUTE_SUB_BATCH_WITH_COMPARISON(std::string,
                                                           crosses);
            } else {
                GEOMETRY_EXECUTE_SUB_BATCH_WITH_COMPARISON(std::string_view,
                                                           crosses);
            }
        }
        case proto::plan::GISFunctionFilterExpr_GISOp_Contains: {
            if (segment_->type() == SegmentType::Growing &&
                !storage::MmapManager::GetInstance()
                     .GetMmapConfig()
                     .growing_enable_mmap) {
                GEOMETRY_EXECUTE_SUB_BATCH_WITH_COMPARISON(std::string,
                                                           contains);
            } else {
                GEOMETRY_EXECUTE_SUB_BATCH_WITH_COMPARISON(std::string_view,
                                                           contains);
            }
        }
        case proto::plan::GISFunctionFilterExpr_GISOp_Intersects: {
            if (segment_->type() == SegmentType::Growing &&
                !storage::MmapManager::GetInstance()
                     .GetMmapConfig()
                     .growing_enable_mmap) {
                GEOMETRY_EXECUTE_SUB_BATCH_WITH_COMPARISON(std::string,
                                                           intersects);
            } else {
                GEOMETRY_EXECUTE_SUB_BATCH_WITH_COMPARISON(std::string_view,
                                                           intersects);
            }
        }
        case proto::plan::GISFunctionFilterExpr_GISOp_Within: {
            if (segment_->type() == SegmentType::Growing &&
                !storage::MmapManager::GetInstance()
                     .GetMmapConfig()
                     .growing_enable_mmap) {
                GEOMETRY_EXECUTE_SUB_BATCH_WITH_COMPARISON(std::string, within);
            } else {
                GEOMETRY_EXECUTE_SUB_BATCH_WITH_COMPARISON(std::string_view,
                                                           within);
            }
        }
        case proto::plan::GISFunctionFilterExpr_GISOp_DWithin: {
            if (segment_->type() == SegmentType::Growing &&
                !storage::MmapManager::GetInstance()
                     .GetMmapConfig()
                     .growing_enable_mmap) {
                GEOMETRY_EXECUTE_SUB_BATCH_WITH_COMPARISON_DISTANCE(std::string,
                                                                    dwithin);
            } else {
                GEOMETRY_EXECUTE_SUB_BATCH_WITH_COMPARISON_DISTANCE(
                    std::string_view, dwithin);
            }
        }
        default: {
            ThrowInfo(NotImplemented,
                      "internal error: unknown GIS op : {}",
                      expr_->op_);
        }
    }
    return res_vec;
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

    Geometry query_geometry =
        Geometry(segment_->get_ctx(), expr_->geometry_wkt_.c_str());

    /* ------------------------------------------------------------------
     * Prefetch: if coarse results are not cached yet, run a single R-Tree
     * query for all index chunks and cache their coarse bitmaps.
     * ------------------------------------------------------------------*/

    auto evaluate_geometry = [this](const Geometry& left,
                                    const Geometry& query_geometry) -> bool {
        switch (expr_->op_) {
            case proto::plan::GISFunctionFilterExpr_GISOp_Equals:
                return left.equals(query_geometry);
            case proto::plan::GISFunctionFilterExpr_GISOp_Touches:
                return left.touches(query_geometry);
            case proto::plan::GISFunctionFilterExpr_GISOp_Overlaps:
                return left.overlaps(query_geometry);
            case proto::plan::GISFunctionFilterExpr_GISOp_Crosses:
                return left.crosses(query_geometry);
            case proto::plan::GISFunctionFilterExpr_GISOp_Contains:
                return left.contains(query_geometry);
            case proto::plan::GISFunctionFilterExpr_GISOp_Intersects:
                return left.intersects(query_geometry);
            case proto::plan::GISFunctionFilterExpr_GISOp_Within:
                return left.within(query_geometry);
            case proto::plan::GISFunctionFilterExpr_GISOp_DWithin:
                return left.dwithin(query_geometry, expr_->distance_);
            default:
                ThrowInfo(NotImplemented, "unknown GIS op : {}", expr_->op_);
        }
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
                segment_->get_ctx(), query_geometry, expr_->distance_);

            ds->Set(milvus::index::MATCH_VALUE, bbox_geometry);

            // Note: Distance is not used for bounding box intersection query
        } else {
            // For other operations, use original geometry
            ds->Set(
                milvus::index::MATCH_VALUE,
                Geometry(segment_->get_ctx(), expr_->geometry_wkt_.c_str()));
        }

        // Query segment-level R-Tree index **once** since each chunk shares the same index
        auto scalar_index = dynamic_cast<const Index*>(pinned_index_[0].get());
        auto* idx_ptr = const_cast<Index*>(scalar_index);

        {
            auto tmp = idx_ptr->Query(ds);
            coarse_global_ = std::move(tmp);
        }
        {
            auto tmp_valid = idx_ptr->IsNotNull();
            coarse_valid_global_ = std::move(tmp_valid);
        }

        coarse_cached_ = true;
    }

    if (cached_index_chunk_res_ == nullptr) {
        // Reuse segment-level coarse cache directly
        auto& coarse = coarse_global_;
        auto& chunk_valid = coarse_valid_global_;
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
            auto* geometry_cache =
                SimpleGeometryCacheManager::Instance().GetCache(
                    segment_->get_segment_id(), field_id_);
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
                    bool result =
                        evaluate_geometry(*cached_geometry, query_geometry);

                    if (result) {
                        refined.set(pos);
                    }
                }
            } else {
                milvus::OpContext op_ctx;
                auto data_array = segment_->bulk_subscript(
                    &op_ctx, field_id_, hit_offsets.data(), hit_offsets.size());

                auto geometry_array =
                    static_cast<const milvus::proto::schema::GeometryArray*>(
                        &data_array->scalars().geometry_data());
                const auto& valid_data = data_array->valid_data();

                GEOSContextHandle_t ctx = GEOS_init_r();
                for (size_t i = 0; i < hit_offsets.size(); ++i) {
                    const auto pos = hit_offsets[i];

                    // Skip invalid data
                    if (!valid_data.empty() && !valid_data[i]) {
                        continue;
                    }

                    const auto& wkb_data = geometry_array->data(i);
                    Geometry left(ctx, wkb_data.data(), wkb_data.size());
                    bool result = evaluate_geometry(left, query_geometry);

                    if (result) {
                        refined.set(pos);
                    }
                }
                GEOS_finish_r(ctx);
            }
        };

        auto hit_offsets = collect_hits();
        process_sealed_data(hit_offsets);

        // Cache refined result for reuse by subsequent batches
        cached_index_chunk_res_ =
            std::make_shared<TargetBitmap>(std::move(refined));
    }

    if (segment_->type() == SegmentType::Sealed) {
        auto size = ProcessIndexOneChunk(batch_result,
                                         batch_valid,
                                         0,
                                         *cached_index_chunk_res_,
                                         coarse_valid_global_,
                                         processed_rows);
        processed_rows += size;
        current_index_chunk_pos_ = current_index_chunk_pos_ + size;
    } else {
        for (size_t i = current_data_chunk_; i < num_data_chunk_; i++) {
            auto data_pos =
                (i == current_data_chunk_) ? current_data_chunk_pos_ : 0;
            int64_t size = segment_->chunk_size(field_id_, i) - data_pos;
            size = std::min(size, real_batch_size - processed_rows);

            if (size > 0) {
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
    return std::make_shared<ColumnVector>(std::move(batch_result),
                                          std::move(batch_valid));
}

}  //namespace exec
}  // namespace milvus