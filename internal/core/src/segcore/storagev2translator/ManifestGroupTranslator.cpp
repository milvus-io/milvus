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

#include "segcore/storagev2translator/ManifestGroupTranslator.h"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "NamedType/named_type_impl.hpp"
#include "arrow/api.h"
#include "cachinglayer/Utils.h"
#include "common/Chunk.h"
#include "common/ChunkWriter.h"
#include "common/Common.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "storage/StatusToErrorCode.h"
#include "common/FieldMeta.h"
#include "common/GroupChunk.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "fmt/core.h"
#include "fmt/ranges.h"
#include "folly/coro/BlockingWait.h"
#include "glog/logging.h"
#include "log/Log.h"
#include "milvus-storage/common/constants.h"
#include "milvus-storage/common/extend_status.h"
#include "milvus-storage/reader.h"
#include "segcore/Utils.h"
#include "segcore/memory_planner.h"
#include "segcore/storagev2translator/AsyncLoadPipeline.h"
#include "segcore/storagev2translator/GroupCTMeta.h"
#include "segcore/storagev2translator/StorageV2Config.h"
#include "storage/LoadOverheadController.h"
#include "storage/LocalFileIOPool.h"
#include "storage/Util.h"

namespace milvus::segcore::storagev2translator {

// See GroupChunkTranslator.cpp for explanation of g_mmap_path_generation.
static std::atomic<uint64_t> g_manifest_mmap_path_generation{0};

ColumnSizeEstimateResult
FetchColumnSizeEstimates(milvus_storage::api::ChunkReader& chunk_reader) {
    auto column_size_result = chunk_reader.get_chunk_column_estimated_size();
    if (!column_size_result.ok()) {
        return {nullptr, column_size_result.status().ToString()};
    }
    auto sizes = std::make_shared<const ColumnSizeEstimateMatrix>(
        std::move(column_size_result).ValueOrDie());
    return {std::move(sizes), ""};
}

ManifestGroupTranslator::ManifestGroupTranslator(
    const int64_t segment_id,
    const GroupChunkType group_chunk_type,
    const int64_t column_group_index,
    std::shared_ptr<milvus_storage::api::ChunkReader> chunk_reader,
    const std::unordered_map<FieldId, FieldMeta>& field_metas,
    std::unordered_map<std::string, std::vector<FieldId>>
        physical_column_field_ids,
    const std::vector<std::string>& column_group_columns,
    const std::vector<std::string>& projected_columns,
    const bool use_mmap,
    const bool mmap_populate,
    const std::string& mmap_dir_path,
    const int64_t num_fields,
    const milvus::proto::common::LoadPriority load_priority,
    const bool eager_load,
    const std::string& warmup_policy,
    const std::string& cache_key_suffix,
    const int64_t fallback_bytes_per_row,
    std::string shard,
    std::optional<ColumnSizeEstimateResult> column_size_estimate,
    const MmapChunkWritebackMode writeback_mode,
    const bool enable_async_load)
    : segment_id_(segment_id),
      group_chunk_type_(group_chunk_type),
      column_group_index_(column_group_index),
      chunk_reader_(std::move(chunk_reader)),
      key_(cache_key_suffix.empty()
               ? fmt::format("seg_{}_cg_{}", segment_id, column_group_index)
               : fmt::format("seg_{}_cg_{}_{}",
                             segment_id,
                             column_group_index,
                             cache_key_suffix)),
      field_metas_(field_metas),
      physical_column_field_ids_(std::move(physical_column_field_ids)),
      mmap_dir_path_(mmap_dir_path),
      meta_(num_fields,
            use_mmap ? milvus::cachinglayer::StorageType::DISK
                     : milvus::cachinglayer::StorageType::MEMORY,
            milvus::cachinglayer::CellIdMappingMode::IDENTICAL,
            milvus::segcore::getCellDataType(
                /* is_vector */
                [&]() {
                    for (const auto& [fid, field_meta] : field_metas_) {
                        if (IsVectorDataType(field_meta.get_data_type())) {
                            return true;
                        }
                    }
                    return false;
                }(),
                /* is_index */ false),
            // Use getCacheWarmupPolicy to resolve: user setting > global config
            milvus::segcore::getCacheWarmupPolicy(
                warmup_policy,
                /* is_vector */
                [&]() {
                    for (const auto& [fid, field_meta] : field_metas_) {
                        if (IsVectorDataType(field_meta.get_data_type())) {
                            return true;
                        }
                    }
                    return false;
                }(),
                /* is_index */ false,
                /* in_load_list*/ eager_load),
            /* support_eviction */ true,
            std::move(shard)),
      use_mmap_(use_mmap),
      mmap_populate_(mmap_populate),
      has_array_field_(std::any_of(field_metas_.begin(),
                                   field_metas_.end(),
                                   [](const auto& field) {
                                       return field.second.get_data_type() ==
                                              DataType::ARRAY;
                                   })),
      writeback_mode_(writeback_mode),
      load_priority_(load_priority),
      enable_async_load_(enable_async_load),
      async_read_window_bytes_(StorageV2AsyncLoadReadWindowSizeBytes()) {
    auto rows_result = chunk_reader_->get_chunk_rows();
    if (!rows_result.ok()) {
        auto error = milvus_storage::ToSegcoreError(rows_result.status());
        ThrowInfo(error.get_error_code(),
                  "get row group rows failed: {}",
                  error.what());
    }
    const auto& row_group_rows = rows_result.ValueOrDie();

    enum class SizeEstimateSource {
        PROJECTED_COLUMNS,
        SAMPLED_FALLBACK,
        TOTAL_FALLBACK,
        LAST_RESORT,
    };

    std::vector<uint64_t> row_group_sizes(row_group_rows.size(), 0);
    std::vector<uint64_t> projected_row_group_sizes(row_group_rows.size(), 0);
    std::string projected_estimate_error;
    std::string total_estimate_error;
    bool projected_estimate_available = !projected_columns.empty();
    bool used_total_estimate = false;
    const bool full_projection = projected_columns == column_group_columns;
    auto materialized_field_count = [&](const std::string& column_name) {
        const auto mapping = physical_column_field_ids_.find(column_name);
        AssertInfo(mapping != physical_column_field_ids_.end(),
                   "physical column {} has no logical field mapping",
                   column_name);
        return std::max<size_t>(1,
                                std::count_if(mapping->second.begin(),
                                              mapping->second.end(),
                                              [](FieldId field_id) {
                                                  return field_id != RowFieldID;
                                              }));
    };
    size_t max_materialized_field_count = 1;
    for (const auto& column_name : projected_columns) {
        max_materialized_field_count =
            std::max(max_materialized_field_count,
                     materialized_field_count(column_name));
    }
    auto scale_total_estimates_for_aliases = [&](std::vector<uint64_t>* sizes,
                                                 std::string* error) {
        for (size_t i = 0; i < sizes->size(); ++i) {
            if ((*sizes)[i] > std::numeric_limits<uint64_t>::max() /
                                  max_materialized_field_count) {
                *error = fmt::format(
                    "column alias sizes exceed the uint64_t range at row "
                    "group {}",
                    i);
                return false;
            }
        }
        for (auto& size : *sizes) {
            size *= max_materialized_field_count;
        }
        return true;
    };
    if (!projected_estimate_available) {
        projected_estimate_error = "projection contains no columns";
    }

    if (projected_estimate_available) {
        if (!column_size_estimate.has_value()) {
            column_size_estimate = FetchColumnSizeEstimates(*chunk_reader_);
        }

        projected_estimate_error = column_size_estimate->error;
        if (!projected_estimate_error.empty() ||
            column_size_estimate->sizes == nullptr) {
            projected_estimate_available = false;
            if (projected_estimate_error.empty()) {
                projected_estimate_error =
                    "column size estimate result contains no data";
            }
        } else {
            const auto& all_column_sizes = *column_size_estimate->sizes;
            if (all_column_sizes.size() != column_group_columns.size()) {
                projected_estimate_available = false;
                projected_estimate_error = fmt::format(
                    "column count mismatched, expected {}, actual {}",
                    column_group_columns.size(),
                    all_column_sizes.size());
            } else {
                std::unordered_map<std::string, size_t> column_indices;
                column_indices.reserve(column_group_columns.size());
                for (size_t i = 0; i < column_group_columns.size(); ++i) {
                    const auto& column_name = column_group_columns[i];
                    if (!column_indices.emplace(column_name, i).second) {
                        projected_estimate_available = false;
                        projected_estimate_error =
                            fmt::format("duplicate column in column group: {}",
                                        column_name);
                        break;
                    }
                }

                std::vector<bool> selected_columns(column_group_columns.size(),
                                                   false);
                for (const auto& column_name : projected_columns) {
                    if (!projected_estimate_available) {
                        break;
                    }
                    auto it = column_indices.find(column_name);
                    if (it == column_indices.end()) {
                        projected_estimate_available = false;
                        projected_estimate_error = fmt::format(
                            "projected column is not in column group: {}",
                            column_name);
                        break;
                    }
                    const auto column_index = it->second;
                    if (selected_columns[column_index]) {
                        projected_estimate_available = false;
                        projected_estimate_error = fmt::format(
                            "duplicate projected column: {}", column_name);
                        break;
                    }
                    selected_columns[column_index] = true;

                    const auto& column_sizes = all_column_sizes[column_index];
                    const auto output_copies =
                        materialized_field_count(column_name);
                    if (column_sizes.size() !=
                        projected_row_group_sizes.size()) {
                        projected_estimate_available = false;
                        projected_estimate_error = fmt::format(
                            "column {} row group count mismatched, expected "
                            "{}, actual {}",
                            column_name,
                            projected_row_group_sizes.size(),
                            column_sizes.size());
                        break;
                    }
                    for (size_t i = 0; i < projected_row_group_sizes.size();
                         ++i) {
                        const auto remaining =
                            std::numeric_limits<uint64_t>::max() -
                            projected_row_group_sizes[i];
                        if (column_sizes[i] > remaining / output_copies) {
                            projected_estimate_available = false;
                            projected_estimate_error = fmt::format(
                                "projected column sizes exceed the uint64_t "
                                "range at row group {}",
                                i);
                            break;
                        }
                        projected_row_group_sizes[i] +=
                            column_sizes[i] * output_copies;
                    }
                }
            }
        }
    }

    // Keep the old full-projection behavior as a fallback for formats that do
    // not expose per-column estimates. The aggregate may include physical-only
    // fields, so logical column estimates always take precedence when present.
    if (!projected_estimate_available && full_projection) {
        auto total_size_result = chunk_reader_->get_chunk_estimated_size();
        if (!total_size_result.ok()) {
            total_estimate_error = total_size_result.status().ToString();
        } else {
            const auto& total_sizes = total_size_result.ValueOrDie();
            if (total_sizes.size() != projected_row_group_sizes.size()) {
                total_estimate_error = fmt::format(
                    "row group count mismatched, expected {}, actual {}",
                    projected_row_group_sizes.size(),
                    total_sizes.size());
            } else {
                projected_row_group_sizes = total_sizes;
                projected_estimate_available =
                    scale_total_estimates_for_aliases(
                        &projected_row_group_sizes, &total_estimate_error);
                used_total_estimate = projected_estimate_available;
                if (!projected_estimate_available) {
                    projected_row_group_sizes.assign(total_sizes.size(), 0);
                }
            }
        }
    }

    SizeEstimateSource size_estimate_source;
    if (projected_estimate_available) {
        // TODO: Lance single-page variable-width estimates can be lower than
        // decoded Arrow memory. Add a per-projected-field sampled safety floor
        // once that metadata is propagated; fallback_bytes_per_row currently
        // covers the whole column group and would erase projection savings.
        row_group_sizes = std::move(projected_row_group_sizes);
        if (used_total_estimate) {
            size_estimate_source = SizeEstimateSource::TOTAL_FALLBACK;
            LOG_WARN(
                "[StorageV2] translator {} cannot use logical column size "
                "estimates ({}); using total chunk estimates for full "
                "projection (row_groups={})",
                key_,
                projected_estimate_error,
                row_group_sizes.size());
        } else {
            size_estimate_source = SizeEstimateSource::PROJECTED_COLUMNS;
            LOG_DEBUG(
                "[StorageV2] translator {} uses projected column size "
                "estimates (columns={}, row_groups={})",
                key_,
                projected_columns.size(),
                row_group_sizes.size());
        }
    } else if (fallback_bytes_per_row > 0) {
        const auto fallback_base =
            static_cast<uint64_t>(fallback_bytes_per_row);
        AssertInfo(fallback_base <= std::numeric_limits<uint64_t>::max() /
                                        max_materialized_field_count,
                   "fallback bytes per row {} overflows alias multiplier {}",
                   fallback_base,
                   max_materialized_field_count);
        const auto fallback = fallback_base * max_materialized_field_count;
        for (size_t i = 0; i < row_group_rows.size(); ++i) {
            if (row_group_rows[i] >
                std::numeric_limits<uint64_t>::max() / fallback) {
                ThrowInfo(
                    ErrorCode::UnexpectedError,
                    "{}",
                    std::string(fmt::format(
                        "fallback row group size exceeds the uint64_t range, "
                        "rows {}, bytes per row {}",
                        row_group_rows[i],
                        fallback)));
            }
            row_group_sizes[i] = row_group_rows[i] * fallback;
        }
        size_estimate_source = SizeEstimateSource::SAMPLED_FALLBACK;
        LOG_WARN(
            "[StorageV2] translator {} cannot use projected column size "
            "estimates ({}); using sampled fallback "
            "(bytes_per_row={}, row_groups={})",
            key_,
            projected_estimate_error,
            fallback,
            row_group_sizes.size());
    } else if (total_estimate_error.empty()) {
        auto total_size_result = chunk_reader_->get_chunk_estimated_size();
        if (total_size_result.ok() &&
            total_size_result.ValueOrDie().size() == row_group_sizes.size()) {
            row_group_sizes = total_size_result.ValueOrDie();
            if (scale_total_estimates_for_aliases(&row_group_sizes,
                                                  &total_estimate_error)) {
                size_estimate_source = SizeEstimateSource::TOTAL_FALLBACK;
                LOG_WARN(
                    "[StorageV2] translator {} cannot use projected column "
                    "size estimates ({}); using total chunk estimates "
                    "(row_groups={})",
                    key_,
                    projected_estimate_error,
                    row_group_sizes.size());
            } else {
                size_estimate_source = SizeEstimateSource::LAST_RESORT;
                row_group_sizes.assign(row_group_rows.size(), 0);
            }
        } else {
            total_estimate_error =
                total_size_result.ok()
                    ? fmt::format(
                          "row group count mismatched, expected {}, actual {}",
                          row_group_sizes.size(),
                          total_size_result.ValueOrDie().size())
                    : total_size_result.status().ToString();
            size_estimate_source = SizeEstimateSource::LAST_RESORT;
        }
    } else {
        size_estimate_source = SizeEstimateSource::LAST_RESORT;
    }

    // A zero estimate for live rows can mean that a projected field is absent
    // from an older fragment after schema evolution. Replace it before cell
    // grouping so a positive row group in the same cell cannot mask the
    // missing reservation or distort ComputeRowGroupsPerCell.
    // FIXME: Remove this sentinel once CellSpec distinguishes a known-empty
    // cell from an unavailable size estimate and accepts zero for the former.
    constexpr uint64_t kEmptyRowGroupReservationBytes = 1;
    constexpr uint64_t kLastResortBytesPerRow = 4096;
    const auto positive_fallback_bytes_per_row_base =
        fallback_bytes_per_row > 0
            ? static_cast<uint64_t>(fallback_bytes_per_row)
            : kLastResortBytesPerRow;
    AssertInfo(
        positive_fallback_bytes_per_row_base <=
            std::numeric_limits<uint64_t>::max() / max_materialized_field_count,
        "positive fallback bytes per row {} overflows alias "
        "multiplier {}",
        positive_fallback_bytes_per_row_base,
        max_materialized_field_count);
    const auto positive_fallback_bytes_per_row =
        positive_fallback_bytes_per_row_base * max_materialized_field_count;
    size_t live_row_groups = 0;
    size_t positive_fallback_row_groups = 0;
    for (size_t i = 0; i < row_group_sizes.size(); ++i) {
        if (row_group_rows[i] == 0) {
            // Empty logical row groups still participate in cell ranges. Keep
            // their loading reservation positive so the batch planner can
            // reach the reader and receive the valid empty batch.
            row_group_sizes[i] =
                std::max(row_group_sizes[i], kEmptyRowGroupReservationBytes);
            continue;
        }
        ++live_row_groups;
        if (row_group_sizes[i] != 0) {
            continue;
        }
        if (row_group_rows[i] > std::numeric_limits<uint64_t>::max() /
                                    positive_fallback_bytes_per_row) {
            ThrowInfo(
                ErrorCode::UnexpectedError,
                "{}",
                std::string(fmt::format(
                    "positive fallback row group size exceeds the uint64_t "
                    "range, rows {}, bytes per row {}",
                    row_group_rows[i],
                    positive_fallback_bytes_per_row)));
        }
        row_group_sizes[i] =
            row_group_rows[i] * positive_fallback_bytes_per_row;
        ++positive_fallback_row_groups;
    }
    if (positive_fallback_row_groups > 0) {
        if (size_estimate_source == SizeEstimateSource::LAST_RESORT) {
            LOG_WARN(
                "[StorageV2] translator {} cannot use projected column size "
                "estimates ({}) or total chunk estimates ({}), and has no "
                "sampled bytes_per_row; using 4KB/row last-resort estimate "
                "for {}/{} live row groups",
                key_,
                projected_estimate_error,
                total_estimate_error,
                positive_fallback_row_groups,
                live_row_groups);
        } else {
            LOG_WARN(
                "[StorageV2] translator {} replaces zero size estimates for "
                "{}/{} live row groups with a positive fallback "
                "(bytes_per_row={})",
                key_,
                positive_fallback_row_groups,
                live_row_groups,
                positive_fallback_bytes_per_row);
        }
    }

    // Merge row groups into group chunks(cache cells). Derive row-groups-
    // per-cell from the runtime-configurable target byte size so avg cell
    // byte size ≈ target.
    constexpr auto kMaxSignedSize =
        static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
    uint64_t total_estimated_size = 0;
    uint64_t total_rows = 0;
    for (size_t i = 0; i < row_group_sizes.size(); ++i) {
        if (row_group_sizes[i] > kMaxSignedSize - total_estimated_size) {
            ThrowInfo(ErrorCode::DataFormatBroken,
                      "row group size estimates exceed int64 range at row "
                      "group {}",
                      i);
        }
        total_estimated_size += row_group_sizes[i];
        if (row_group_rows[i] > kMaxSignedSize - total_rows) {
            ThrowInfo(ErrorCode::DataFormatBroken,
                      "row counts exceed int64 range at row group {}",
                      i);
        }
        total_rows += row_group_rows[i];
    }
    const int64_t cell_target_size_bytes = GetCellTargetSizeBytes();
    size_t total_row_groups = row_group_sizes.size();
    meta_.total_row_groups_ = total_row_groups;
    const size_t rgs_per_cell =
        ComputeRowGroupsPerCell(row_group_sizes, cell_target_size_bytes);
    size_t num_cells = (total_row_groups + rgs_per_cell - 1) / rgs_per_cell;

    // Populate cell_row_group_ranges_ (single data source, no multi-file)
    meta_.cell_row_group_ranges_.reserve(num_cells);
    for (size_t cid = 0; cid < num_cells; ++cid) {
        size_t start = cid * rgs_per_cell;
        size_t end = std::min(start + rgs_per_cell, total_row_groups);
        meta_.cell_row_group_ranges_.push_back({start, end});
    }

    // Build num_rows_until_chunk_ and chunk_memory_size_
    meta_.num_rows_until_chunk_.reserve(num_cells + 1);
    meta_.num_rows_until_chunk_.push_back(0);
    meta_.chunk_memory_size_.reserve(num_cells);

    int64_t cumulative_rows = 0;
    for (size_t cell_id = 0; cell_id < num_cells; ++cell_id) {
        auto [start, end] = meta_.get_row_group_range(cell_id);
        int64_t cell_size = 0;
        for (size_t i = start; i < end; ++i) {
            cumulative_rows += static_cast<int64_t>(row_group_rows[i]);
            cell_size += static_cast<int64_t>(row_group_sizes[i]);
        }
        meta_.num_rows_until_chunk_.push_back(cumulative_rows);
        meta_.chunk_memory_size_.push_back(cell_size);
    }

    LOG_INFO(
        "[StorageV2] translator {} merged {} row groups into {} cells "
        "(cell_target_size_bytes={})",
        key_,
        total_row_groups,
        num_cells,
        cell_target_size_bytes);

    // Bind loading overhead to the runtime limiter used by this translator.
    if (!meta_.chunk_memory_size_.empty()) {
        int64_t max_cell_sz = *std::max_element(
            meta_.chunk_memory_size_.begin(), meta_.chunk_memory_size_.end());
        // Async admission leases a whole window, including all of its cells.
        // Sync batches are instead split by their accumulated overhead bytes.
        const auto max_file_runtime_unit =
            std::max(enable_async_load_ ? async_read_window_bytes_
                                        : FieldDataLoadBatchTargetBytes(),
                     max_cell_sz);
        const auto max_memory_runtime_unit =
            enable_async_load_ ? loading_overhead_bytes(max_file_runtime_unit)
                               : std::max(FieldDataLoadBatchTargetBytes(),
                                          loading_overhead_bytes(max_cell_sz));
        auto memory_group =
            storage::LoadMemoryOverheadController::GetInstance().GetOrCreate();
        meta_.loading_overhead_config =
            milvus::cachinglayer::LoadingOverheadConfig{
                milvus::cachinglayer::LoadingOverheadGroupBinding{
                    std::move(memory_group), max_memory_runtime_unit},
                use_mmap_
                    ? std::make_optional(
                          milvus::cachinglayer::LoadingOverheadGroupBinding{
                              storage::LoadFileOverheadController::GetInstance()
                                  .GetOrCreate(),
                              max_file_runtime_unit})
                    : std::nullopt};
    }
}

size_t
ManifestGroupTranslator::num_cells() const {
    return meta_.chunk_memory_size_.size();
}

milvus::cachinglayer::cid_t
ManifestGroupTranslator::cell_id_of(milvus::cachinglayer::uid_t uid) const {
    return uid;
}

std::pair<milvus::cachinglayer::ResourceUsage,
          milvus::cachinglayer::ResourceUsage>
ManifestGroupTranslator::estimated_byte_size_of_cell(
    milvus::cachinglayer::cid_t cid) const {
    assert(cid < meta_.chunk_memory_size_.size());
    auto cell_sz = meta_.chunk_memory_size_[cid];
    auto overhead_sz = loading_overhead_bytes(cell_sz);

    if (use_mmap_) {
        return {{0, cell_sz}, {overhead_sz, cell_sz}};
    } else {
        return {{cell_sz, 0}, {overhead_sz, 0}};
    }
}

const std::string&
ManifestGroupTranslator::key() const {
    return key_;
}

std::vector<
    std::pair<milvus::cachinglayer::cid_t, std::unique_ptr<milvus::GroupChunk>>>
ManifestGroupTranslator::get_cells(
    milvus::OpContext* ctx,
    const std::vector<milvus::cachinglayer::cid_t>& cids) {
    // Check for cancellation before loading group chunks
    CheckCancellation(ctx, segment_id_, "ManifestGroupTranslator::get_cells()");

    if (cids.empty()) {
        return {};
    }

    const auto max_cid = *std::max_element(cids.begin(), cids.end());
    if (max_cid >= meta_.chunk_memory_size_.size()) {
        ThrowInfo(
            ErrorCode::UnexpectedError,
            "[StorageV2] translator {} cid {} is out of range. Total cells: {}",
            key_,
            max_cid,
            meta_.chunk_memory_size_.size());
    }

    // Build CellSpec for each requested cid
    std::vector<milvus::segcore::CellSpec> cell_specs;
    cell_specs.reserve(cids.size());
    for (const auto cid : cids) {
        const auto [start, end] = meta_.get_row_group_range(cid);
        cell_specs.push_back(
            {cid,
             /*file_idx=*/0,
             static_cast<int64_t>(start),
             static_cast<int64_t>(end - start),
             meta_.chunk_memory_size_[cid],
             loading_overhead_bytes(meta_.chunk_memory_size_[cid])});
    }

    if (enable_async_load_) {
        return get_cells_via_async_pipeline(ctx, std::move(cell_specs));
    }
    return get_cells_legacy(ctx, cids, std::move(cell_specs));
}

std::vector<ManifestGroupTranslator::CellResult>
ManifestGroupTranslator::get_cells_legacy(
    milvus::OpContext* ctx,
    const std::vector<milvus::cachinglayer::cid_t>& cids,
    std::vector<milvus::segcore::CellSpec> cell_specs) const {
    std::vector<CellResult> cells;
    cells.reserve(cids.size());

    // Create factory using ChunkReader — reads a batch of row groups at once
    auto factory = milvus::segcore::MakeChunkReaderFactory(chunk_reader_);

    // Submit cell-batch loading tasks
    auto load_futures = milvus::segcore::LoadCellBatchAsync(
        ctx,
        std::move(cell_specs),
        std::move(factory),
        FieldDataLoadBatchSplitTargetBytes(),
        load_priority_,
        [this](const std::vector<std::shared_ptr<arrow::Table>>& tables,
               const int64_t cid) {
            return load_group_chunk(
                tables, static_cast<milvus::cachinglayer::cid_t>(cid));
        });

    LOG_INFO(
        "[StorageV2] translator {} submits {} batch tasks for manifest "
        "column group {}",
        key_,
        load_futures.size(),
        column_group_index_);

    std::unordered_map<milvus::cachinglayer::cid_t,
                       std::unique_ptr<milvus::GroupChunk>>
        completed_cells;
    completed_cells.reserve(cids.size());

    std::exception_ptr first_error;
    for (auto& future : load_futures) {
        try {
            auto loaded_cells = future.get();
            if (first_error) {
                continue;
            }
            for (auto& loaded_cell : loaded_cells) {
                try {
                    CheckCancellation(ctx,
                                      segment_id_,
                                      "ManifestGroupTranslator::get_cells()");
                    AssertInfo(loaded_cell.chunk != nullptr,
                               "[StorageV2] translator {} cell {} is not "
                               "finalized by batch task",
                               key_,
                               loaded_cell.cid);
                    completed_cells[loaded_cell.cid] =
                        std::move(loaded_cell.chunk);
                } catch (...) {
                    first_error = std::current_exception();
                    break;
                }
            }
        } catch (...) {
            if (!first_error) {
                first_error = std::current_exception();
            }
        }
    }
    if (first_error) {
        std::rethrow_exception(first_error);
    }

    for (const auto cid : cids) {
        const auto it = completed_cells.find(cid);
        AssertInfo(
            it != completed_cells.end(),
            fmt::format(
                "[StorageV2] translator {} cell {} not loaded", key_, cid));
        cells.emplace_back(cid, std::move(it->second));
    }

    return cells;
}

std::vector<ManifestGroupTranslator::CellResult>
ManifestGroupTranslator::get_cells_via_async_pipeline(
    milvus::OpContext* ctx,
    std::vector<milvus::segcore::CellSpec> cell_specs) const {
    LOG_INFO(
        "[StorageV3] translator {} uses async load pipeline for {} cells in "
        "manifest column group {} of segment {}",
        key_,
        cell_specs.size(),
        column_group_index_,
        segment_id_);
    AsyncLoadPipelineOptions options{
        .read_window_bytes = static_cast<size_t>(async_read_window_bytes_),
        .load_priority = load_priority_};
    if (use_mmap_) {
        options.finalization_executor_provider = []() {
            return storage::LocalFileIOPool::GetInstance().GetExecutor();
        };
    }
    return folly::coro::blockingWait(LoadCellsAsync(
        ctx,
        segment_id_,
        std::move(cell_specs),
        chunk_reader_,
        [this](const std::vector<std::shared_ptr<arrow::Table>>& tables,
               const int64_t cid) {
            return load_group_chunk(
                tables, static_cast<milvus::cachinglayer::cid_t>(cid));
        },
        std::move(options)));
}

std::unique_ptr<milvus::GroupChunk>
ManifestGroupTranslator::load_group_chunk(
    const std::vector<std::shared_ptr<arrow::Table>>& tables,
    const milvus::cachinglayer::cid_t cid) const {
    assert(!tables.empty());
    // Use the first table's schema as reference for field iteration
    const auto& schema = tables[0]->schema();

    std::vector<FieldId> field_ids;
    field_ids.reserve(schema->num_fields());
    std::vector<FieldMeta> field_metas;
    field_metas.reserve(schema->num_fields());
    std::vector<arrow::ArrayVector> array_vecs;
    array_vecs.reserve(schema->num_fields());

    // Iterate through physical fields and expand each to the logical fields
    // that map to it. The reader projects a physical column once; aliases get
    // their own normalized Raw chunks below.
    for (int i = 0; i < schema->num_fields(); ++i) {
        const auto column_name = schema->field(i)->name();
        const auto mapping = physical_column_field_ids_.find(column_name);
        if (mapping == physical_column_field_ids_.end()) {
            ThrowInfo(ErrorCode::DataFormatBroken,
                      "[StorageV2] translator {} physical column {} has no "
                      "logical field mapping",
                      key_,
                      column_name);
        }
        const auto& logical_field_ids = mapping->second;
        if (logical_field_ids.size() == 1 &&
            logical_field_ids.front() == RowFieldID) {
            continue;
        }

        // Merge arrays from all tables for this field
        // All tables in a cell come from the same column group with consistent schema
        arrow::ArrayVector merged_array_vec;
        for (const auto& table : tables) {
            const auto& chunks = table->column(i)->chunks();
            merged_array_vec.insert(
                merged_array_vec.end(), chunks.begin(), chunks.end());
        }

        for (const auto fid : logical_field_ids) {
            if (fid == RowFieldID) {
                continue;
            }
            const auto it = field_metas_.find(fid);
            AssertInfo(it != field_metas_.end(),
                       "[StorageV2] translator {} field id {} not found in "
                       "field_metas",
                       key_,
                       fid.get());
            field_ids.push_back(fid);
            field_metas.push_back(it->second);
            array_vecs.push_back(merged_array_vec);
        }
    }

    // Normalize all arrow arrays for ChunkWriter compatibility.
    // Handles: vectors (nullable/non-nullable), strings, timestamps,
    // arrays, vector arrays, JSON, geometry.
    for (size_t idx = 0; idx < field_ids.size(); ++idx) {
        array_vecs[idx] = storage::NormalizeArrowForChunkWriter(
            array_vecs[idx], field_metas[idx]);
    }

    std::unordered_map<FieldId, std::shared_ptr<Chunk>> chunks;
    if (!use_mmap_) {
        // Memory mode
        chunks = create_group_chunk(
            field_ids, field_metas, array_vecs, mmap_populate_);
    } else {
        // Mmap mode — use unique generation suffix to avoid truncating files
        // that old MAP_SHARED mmaps still reference (see #48658).
        const auto gen = g_manifest_mmap_path_generation.fetch_add(
            1, std::memory_order_relaxed);
        std::filesystem::path filepath;
        switch (group_chunk_type_) {
            case GroupChunkType::DEFAULT:
                filepath = std::filesystem::path(mmap_dir_path_) /
                           fmt::format("seg_{}_cg_{}_{}_{}",
                                       segment_id_,
                                       column_group_index_,
                                       cid,
                                       gen);
                break;
            case GroupChunkType::JSON_KEY_STATS:
                filepath =
                    std::filesystem::path(mmap_dir_path_) /
                    fmt::format(
                        "seg_{}_jks_{}_cg_{}_{}_{}",
                        segment_id_,
                        // NOTE: here we assume the first field is the main field for json key stats group chunk
                        std::to_string(field_metas[0].get_main_field_id()),
                        column_group_index_,
                        cid,
                        gen);
                break;
            default:
                ThrowInfo(ErrorCode::UnexpectedError,
                          "unknown group chunk type: {}",
                          static_cast<uint8_t>(group_chunk_type_));
        }
        std::filesystem::create_directories(filepath.parent_path());
        chunks = create_group_chunk(field_ids,
                                    field_metas,
                                    array_vecs,
                                    mmap_populate_,
                                    filepath.string(),
                                    load_priority_,
                                    writeback_mode_);
    }

    return std::make_unique<milvus::GroupChunk>(std::move(chunks));
}

int64_t
ManifestGroupTranslator::loading_overhead_bytes(int64_t cell_size) const {
    if (!has_array_field_) {
        return cell_size;
    }
    if (cell_size > std::numeric_limits<int64_t>::max() / 2) {
        return std::numeric_limits<int64_t>::max();
    }
    return cell_size * 2;
}

}  // namespace milvus::segcore::storagev2translator
