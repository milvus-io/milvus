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
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include "NamedType/named_type_impl.hpp"
#include "arrow/api.h"
#include "cachinglayer/Utils.h"
#include "common/Chunk.h"
#include "common/ChunkWriter.h"
#include "common/Common.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/FieldMeta.h"
#include "common/GroupChunk.h"
#include "common/Types.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "log/Log.h"
#include "milvus-storage/reader.h"
#include "segcore/Utils.h"
#include "segcore/memory_planner.h"
#include "storage/ThreadPools.h"
#include "segcore/storagev2translator/GroupCTMeta.h"
#include "storage/Util.h"

namespace milvus::segcore::storagev2translator {

ManifestGroupTranslator::ManifestGroupTranslator(
    int64_t segment_id,
    GroupChunkType group_chunk_type,
    int64_t column_group_index,
    std::shared_ptr<milvus_storage::api::ChunkReader> chunk_reader,
    const std::unordered_map<FieldId, FieldMeta>& field_metas,
    bool use_mmap,
    bool mmap_populate,
    const std::string& mmap_dir_path,
    int64_t num_fields,
    milvus::proto::common::LoadPriority load_priority,
    bool eager_load,
    const std::string& warmup_policy)
    : segment_id_(segment_id),
      group_chunk_type_(group_chunk_type),
      column_group_index_(column_group_index),
      chunk_reader_(std::move(chunk_reader)),
      key_(fmt::format("seg_{}_cg_{}", segment_id, column_group_index)),
      field_metas_(field_metas),
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
            /* support_eviction */ true),
      use_mmap_(use_mmap),
      mmap_populate_(mmap_populate),
      load_priority_(load_priority) {
    auto chunk_size_result = chunk_reader_->get_chunk_size();
    if (!chunk_size_result.ok()) {
        throw std::runtime_error("get row group size failed");
    }
    const auto& row_group_sizes = chunk_size_result.ValueOrDie();

    auto rows_result = chunk_reader_->get_chunk_rows();
    if (!rows_result.ok()) {
        throw std::runtime_error("get row group rows failed");
    }
    const auto& row_group_rows = rows_result.ValueOrDie();

    // Merge row groups into group chunks(cache cells)
    size_t total_row_groups = row_group_sizes.size();
    meta_.total_row_groups_ = total_row_groups;
    size_t num_cells =
        (total_row_groups + kRowGroupsPerCell - 1) / kRowGroupsPerCell;

    // Populate cell_row_group_ranges_ (single data source, no multi-file)
    meta_.cell_row_group_ranges_.reserve(num_cells);
    for (size_t cid = 0; cid < num_cells; ++cid) {
        size_t start = cid * kRowGroupsPerCell;
        size_t end = std::min(start + kRowGroupsPerCell, total_row_groups);
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
        "[StorageV2] translator {} merged {} row groups into {} cells ({} "
        "row groups per cell)",
        key_,
        total_row_groups,
        num_cells,
        kRowGroupsPerCell);
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

    if (use_mmap_) {
        // why double the disk size for loading?
        // during file writing, the temporary size could be larger than the final size
        // so we need to reserve more space for the disk size.
        return {{0, cell_sz}, {2 * cell_sz, 2 * cell_sz}};
    } else {
        return {{cell_sz, 0}, {2 * cell_sz, 0}};
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

    std::vector<std::pair<milvus::cachinglayer::cid_t,
                          std::unique_ptr<milvus::GroupChunk>>>
        cells;
    cells.reserve(cids.size());

    auto max_cid = *std::max_element(cids.begin(), cids.end());
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
    for (auto cid : cids) {
        auto [start, end] = meta_.get_row_group_range(cid);
        cell_specs.push_back({cid,
                              /*file_idx=*/0,
                              static_cast<int64_t>(start),
                              static_cast<int64_t>(end - start)});
    }

    // Create factory using ChunkReader — reads a batch of row groups at once
    auto factory = milvus::segcore::MakeChunkReaderFactory(chunk_reader_);

    // Submit cell-batch loading tasks
    auto& pool = milvus::ThreadPools::GetThreadPool(
        milvus::PriorityForLoad(load_priority_));
    auto channel = std::make_shared<milvus::segcore::CellReaderChannel>(
        static_cast<size_t>(pool.GetMaxThreadNum() * 1.5));

    auto load_futures =
        milvus::segcore::LoadCellBatchAsync(ctx,
                                            std::move(cell_specs),
                                            std::move(factory),
                                            channel,
                                            DEFAULT_FIELD_MAX_MEMORY_LIMIT,
                                            load_priority_);

    LOG_INFO(
        "[StorageV2] translator {} submits {} batch tasks for manifest column "
        "group {}",
        key_,
        load_futures.size(),
        column_group_index_);

    // Pop loop — convert each cell immediately, no ArrowTable accumulation
    std::unordered_map<milvus::cachinglayer::cid_t,
                       std::unique_ptr<milvus::GroupChunk>>
        completed_cells;
    completed_cells.reserve(cids.size());

    try {
        std::shared_ptr<milvus::segcore::CellLoadResult> cell_data;
        while (channel->pop(cell_data)) {
            CheckCancellation(
                ctx, segment_id_, "ManifestGroupTranslator::get_cells()");
            completed_cells[cell_data->cid] =
                load_group_chunk(cell_data->tables, cell_data->cid);
        }
    } catch (...) {
        // Drain the channel to unblock producers that may be stuck on push()
        // to a full bounded channel. Without draining, producers block forever
        // and their task_guard (which calls channel->close()) never executes.
        std::shared_ptr<milvus::segcore::CellLoadResult> discard;
        try {
            while (channel->pop(discard)) {
            }
        } catch (...) {
            LOG_WARN("drain channel exception swallowed");
        }
        throw;
    }

    storage::WaitAllFutures(load_futures);

    for (auto cid : cids) {
        auto it = completed_cells.find(cid);
        AssertInfo(
            it != completed_cells.end(),
            fmt::format(
                "[StorageV2] translator {} cell {} not loaded", key_, cid));
        cells.emplace_back(cid, std::move(it->second));
    }

    return cells;
}

std::unique_ptr<milvus::GroupChunk>
ManifestGroupTranslator::load_group_chunk(
    const std::vector<std::shared_ptr<arrow::Table>>& tables,
    const milvus::cachinglayer::cid_t cid) {
    assert(!tables.empty());
    // Use the first table's schema as reference for field iteration
    const auto& schema = tables[0]->schema();

    std::vector<FieldId> field_ids;
    field_ids.reserve(schema->num_fields());
    std::vector<FieldMeta> field_metas;
    field_metas.reserve(schema->num_fields());
    std::vector<arrow::ArrayVector> array_vecs;
    array_vecs.reserve(schema->num_fields());

    // Iterate through fields to get field_id and create chunk
    for (int i = 0; i < schema->num_fields(); ++i) {
        // column name here is field id (ChunkReader stores field IDs as column names)
        auto column_name = schema->field(i)->name();
        auto field_id = std::stoll(column_name);

        auto fid = milvus::FieldId(field_id);
        if (fid == RowFieldID) {
            // ignore row id field
            continue;
        }
        auto it = field_metas_.find(fid);
        // Workaround for projection push-down missing for chunk reader
        // change back to assertion after supported
        if (it == field_metas_.end()) {
            continue;
        }
        const auto& field_meta = it->second;

        // Merge arrays from all tables for this field
        // All tables in a cell come from the same column group with consistent schema
        arrow::ArrayVector merged_array_vec;
        for (const auto& table : tables) {
            auto chunks = table->column(i)->chunks();
            merged_array_vec.insert(
                merged_array_vec.end(), chunks.begin(), chunks.end());
        }

        field_ids.push_back(fid);
        field_metas.push_back(field_meta);
        array_vecs.push_back(std::move(merged_array_vec));
    }

    std::unordered_map<FieldId, std::shared_ptr<Chunk>> chunks;
    if (!use_mmap_) {
        // Memory mode
        chunks = create_group_chunk(
            field_ids, field_metas, array_vecs, mmap_populate_);
    } else {
        // Mmap mode
        std::filesystem::path filepath;
        switch (group_chunk_type_) {
            case GroupChunkType::DEFAULT:
                filepath = std::filesystem::path(mmap_dir_path_) /
                           fmt::format("seg_{}_cg_{}_{}",
                                       segment_id_,
                                       column_group_index_,
                                       cid);
                break;
            case GroupChunkType::JSON_KEY_STATS:
                filepath =
                    std::filesystem::path(mmap_dir_path_) /
                    fmt::format(
                        "seg_{}_jks_{}_cg_{}_{}",
                        segment_id_,
                        // NOTE: here we assume the first field is the main field for json key stats group chunk
                        std::to_string(field_metas[0].get_main_field_id()),
                        column_group_index_,
                        cid);
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
                                    load_priority_);
    }

    return std::make_unique<milvus::GroupChunk>(chunks);
}

}  // namespace milvus::segcore::storagev2translator