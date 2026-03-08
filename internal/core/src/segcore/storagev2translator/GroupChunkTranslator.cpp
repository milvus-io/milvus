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
#include "segcore/storagev2translator/GroupChunkTranslator.h"

#include <assert.h>
#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
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
#include "milvus-storage/common/config.h"
#include "milvus-storage/common/constants.h"
#include "milvus-storage/common/metadata.h"
#include "milvus-storage/filesystem/fs.h"
#include "milvus-storage/format/parquet/file_reader.h"
#include "mmap/Types.h"
#include "segcore/InsertRecord.h"
#include "segcore/Utils.h"
#include "segcore/memory_planner.h"
#include "segcore/storagev2translator/GroupCTMeta.h"
#include "storage/KeyRetriever.h"
#include "storage/ThreadPools.h"
#include "storage/Util.h"

namespace milvus::segcore::storagev2translator {

GroupChunkTranslator::GroupChunkTranslator(
    int64_t segment_id,
    GroupChunkType group_chunk_type,
    const std::unordered_map<FieldId, FieldMeta>& field_metas,
    FieldDataInfo column_group_info,
    std::vector<std::string> insert_files,
    bool use_mmap,
    bool mmap_populate,
    int64_t num_fields,
    milvus::proto::common::LoadPriority load_priority,
    const std::string& warmup_policy)
    : segment_id_(segment_id),
      group_chunk_type_(group_chunk_type),
      key_([&]() {
          switch (group_chunk_type) {
              case GroupChunkType::DEFAULT:
                  return fmt::format(
                      "seg_{}_cg_{}", segment_id, column_group_info.field_id);
              case GroupChunkType::JSON_KEY_STATS:
                  AssertInfo(
                      column_group_info.main_field_id != INVALID_FIELD_ID,
                      "main field id is not set for json key stats group "
                      "chunk");
                  return fmt::format("seg_{}_jks_{}_cg_{}",
                                     segment_id,
                                     column_group_info.main_field_id,
                                     column_group_info.field_id);
          }
      }()),
      field_metas_(field_metas),
      column_group_info_(column_group_info),
      insert_files_(insert_files),
      use_mmap_(use_mmap),
      mmap_populate_(mmap_populate),
      load_priority_(load_priority),
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
                /* is_index */ false),
            /* support_eviction */ true) {
    auto fs = milvus_storage::ArrowFileSystemSingleton::GetInstance()
                  .GetArrowFileSystem();

    // Get row group metadata from files
    parquet_file_metadata_.reserve(insert_files_.size());
    row_group_meta_list_.reserve(insert_files_.size());
    for (const auto& file : insert_files_) {
        auto result = milvus_storage::FileRowGroupReader::Make(
            fs,
            file,
            milvus_storage::DEFAULT_READ_BUFFER_SIZE,
            storage::GetReaderProperties());
        AssertInfo(result.ok(),
                   "[StorageV2] Failed to create file row group reader: " +
                       result.status().ToString());
        auto reader = result.ValueOrDie();
        parquet_file_metadata_.push_back(
            reader->file_metadata()->GetParquetMetadata());

        field_id_mapping_ = reader->file_metadata()->GetFieldIDMapping();
        row_group_meta_list_.push_back(
            reader->file_metadata()->GetRowGroupMetadataVector());
        auto status = reader->Close();
        AssertInfo(status.ok(),
                   "[StorageV2] translator {} failed to close file reader when "
                   "get row group "
                   "metadata from file {} with error {}",
                   key_,
                   file + " with error: " + status.ToString());
    }

    // Build prefix sum for O(1) lookup in get_cid_from_file_and_row_group_index
    file_row_group_prefix_sum_.reserve(row_group_meta_list_.size() + 1);
    file_row_group_prefix_sum_.push_back(
        0);  // Base case: 0 row groups before first file
    size_t total_row_groups = 0;
    for (const auto& file_metas : row_group_meta_list_) {
        total_row_groups += file_metas.size();
        file_row_group_prefix_sum_.push_back(file_row_group_prefix_sum_.back() +
                                             file_metas.size());
    }

    // Collect row group sizes and row counts
    std::vector<int64_t> row_group_row_counts;
    std::vector<int64_t> row_group_sizes;
    row_group_sizes.reserve(total_row_groups);
    row_group_row_counts.reserve(total_row_groups);
    for (const auto& row_group_meta : row_group_meta_list_) {
        for (int i = 0; i < row_group_meta.size(); ++i) {
            row_group_sizes.push_back(row_group_meta.Get(i).memory_size());
            row_group_row_counts.push_back(row_group_meta.Get(i).row_num());
        }
    }

    // Build cell mapping: cells DO NOT span files — each cell's row groups
    // come entirely from one file.
    meta_.total_row_groups_ = total_row_groups;
    size_t global_rg_offset = 0;
    for (const auto& rg_meta : row_group_meta_list_) {
        size_t file_rg_count = rg_meta.size();
        for (size_t local_start = 0; local_start < file_rg_count;
             local_start += kRowGroupsPerCell) {
            size_t local_end =
                std::min(local_start + kRowGroupsPerCell, file_rg_count);
            meta_.cell_row_group_ranges_.push_back(
                {global_rg_offset + local_start, global_rg_offset + local_end});
        }
        global_rg_offset += file_rg_count;
    }

    size_t num_cells = meta_.cell_row_group_ranges_.size();

    // Merge row groups into group chunks(cache cells)
    meta_.num_rows_until_chunk_.reserve(num_cells + 1);
    meta_.num_rows_until_chunk_.push_back(0);
    meta_.chunk_memory_size_.reserve(num_cells);

    int64_t cumulative_rows = 0;
    for (size_t cell_id = 0; cell_id < num_cells; ++cell_id) {
        auto [start, end] = meta_.get_row_group_range(cell_id);
        int64_t cell_size = 0;
        for (size_t i = start; i < end; ++i) {
            cumulative_rows += row_group_row_counts[i];
            cell_size += row_group_sizes[i];
        }
        meta_.num_rows_until_chunk_.push_back(cumulative_rows);
        meta_.chunk_memory_size_.push_back(cell_size);
    }

    AssertInfo(
        meta_.num_rows_until_chunk_.back() == column_group_info_.row_count,
        fmt::format(
            "[StorageV2] data lost while loading column group {}: found "
            "num rows {} but expected {}",
            column_group_info_.field_id,
            meta_.num_rows_until_chunk_.back(),
            column_group_info_.row_count));

    LOG_INFO(
        "[StorageV2] translator {} merged {} row groups into {} cells ({} "
        "row groups per cell)",
        key_,
        total_row_groups,
        num_cells,
        kRowGroupsPerCell);
}

GroupChunkTranslator::~GroupChunkTranslator() {
}

size_t
GroupChunkTranslator::num_cells() const {
    return meta_.chunk_memory_size_.size();
}

milvus::cachinglayer::cid_t
GroupChunkTranslator::cell_id_of(milvus::cachinglayer::uid_t uid) const {
    return uid;
}

std::pair<milvus::cachinglayer::ResourceUsage,
          milvus::cachinglayer::ResourceUsage>
GroupChunkTranslator::estimated_byte_size_of_cell(
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
GroupChunkTranslator::key() const {
    return key_;
}

std::pair<size_t, size_t>
GroupChunkTranslator::get_file_and_row_group_offset(
    size_t global_row_group_idx) const {
    for (size_t file_idx = 0; file_idx < file_row_group_prefix_sum_.size() - 1;
         ++file_idx) {
        if (global_row_group_idx < file_row_group_prefix_sum_[file_idx + 1]) {
            return {
                file_idx,
                global_row_group_idx - file_row_group_prefix_sum_[file_idx]};
        }
    }

    AssertInfo(
        false,
        fmt::format("[StorageV2] translator {} global_row_group_idx {} is out "
                    "of range. Total row groups across all files: {}",
                    key_,
                    global_row_group_idx,
                    file_row_group_prefix_sum_.back()));
}

milvus::cachinglayer::cid_t
GroupChunkTranslator::get_global_row_group_idx(size_t file_idx,
                                               size_t row_group_idx) const {
    AssertInfo(file_idx < file_row_group_prefix_sum_.size() - 1,
               fmt::format("[StorageV2] translator {} file_idx {} is out of "
                           "range. Total files: {}",
                           key_,
                           file_idx,
                           file_row_group_prefix_sum_.size() - 1));

    size_t file_start = file_row_group_prefix_sum_[file_idx];
    size_t file_end = file_row_group_prefix_sum_[file_idx + 1];
    AssertInfo(row_group_idx < file_end - file_start,
               fmt::format("[StorageV2] translator {} row_group_idx {} is out "
                           "of range for file {}. "
                           "Total row groups in file: {}",
                           key_,
                           row_group_idx,
                           file_idx,
                           file_end - file_start));

    return file_start + row_group_idx;
}

std::vector<std::pair<cachinglayer::cid_t, std::unique_ptr<milvus::GroupChunk>>>
GroupChunkTranslator::get_cells(milvus::OpContext* ctx,
                                const std::vector<cachinglayer::cid_t>& cids) {
    // Check for cancellation before loading group chunks
    CheckCancellation(ctx, segment_id_, "GroupChunkTranslator::get_cells()");

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
        auto [rg_start, rg_end] = meta_.get_row_group_range(cid);
        auto [file_idx, local_off] = get_file_and_row_group_offset(rg_start);
        cell_specs.push_back({cid,
                              file_idx,
                              static_cast<int64_t>(local_off),
                              static_cast<int64_t>(rg_end - rg_start)});
    }

    // Submit cell-batch loading tasks
    auto& pool = milvus::ThreadPools::GetThreadPool(
        milvus::PriorityForLoad(load_priority_));
    auto channel = std::make_shared<milvus::segcore::CellReaderChannel>(
        static_cast<size_t>(pool.GetMaxThreadNum() * 1.5));
    auto fs = milvus_storage::ArrowFileSystemSingleton::GetInstance()
                  .GetArrowFileSystem();

    auto factory = milvus::segcore::MakeFileReaderFactory(insert_files_, fs);
    auto load_futures =
        milvus::segcore::LoadCellBatchAsync(ctx,
                                            std::move(cell_specs),
                                            std::move(factory),
                                            channel,
                                            DEFAULT_FIELD_MAX_MEMORY_LIMIT,
                                            load_priority_);

    LOG_INFO(
        "[StorageV2] translator {} submits {} batch tasks for column group {}",
        key_,
        load_futures.size(),
        column_group_info_.field_id);

    // Pop loop — convert each cell immediately, no ArrowTable accumulation
    std::unordered_map<cachinglayer::cid_t, std::unique_ptr<milvus::GroupChunk>>
        completed_cells;
    completed_cells.reserve(cids.size());

    try {
        std::shared_ptr<milvus::segcore::CellLoadResult> cell_data;
        while (channel->pop(cell_data)) {
            CheckCancellation(
                ctx, segment_id_, "GroupChunkTranslator::get_cells()");
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
GroupChunkTranslator::load_group_chunk(
    const std::vector<std::shared_ptr<arrow::Table>>& tables,
    const milvus::cachinglayer::cid_t cid) {
    assert(!tables.empty());
    // Use the first table's schema as reference for field iteration
    const auto& schema = tables[0]->schema();

    // Collect field info and merge array vectors from all tables
    std::vector<FieldId> field_ids;
    field_ids.reserve(schema->num_fields());
    std::vector<FieldMeta> field_metas;
    field_metas.reserve(schema->num_fields());
    std::vector<arrow::ArrayVector> array_vecs;
    array_vecs.reserve(schema->num_fields());

    for (int i = 0; i < schema->num_fields(); ++i) {
        AssertInfo(schema->field(i)->metadata()->Contains(
                       milvus_storage::ARROW_FIELD_ID_KEY),
                   "[StorageV2] translator {} field id not found in metadata "
                   "for field {}",
                   key_,
                   schema->field(i)->name());
        auto field_id = std::stoll(schema->field(i)
                                       ->metadata()
                                       ->Get(milvus_storage::ARROW_FIELD_ID_KEY)
                                       ->data());

        auto fid = milvus::FieldId(field_id);
        if (fid == RowFieldID) {
            // ignore row id field
            continue;
        }
        auto it = field_metas_.find(fid);
        AssertInfo(
            it != field_metas_.end(),
            "[StorageV2] translator {} field id {} not found in field_metas",
            key_,
            fid.get());
        const auto& field_meta = it->second;

        // Merge array vectors from all tables for this field
        // All tables in a cell come from the same column group with consistent schema
        arrow::ArrayVector merged_array_vec;
        for (const auto& table : tables) {
            const arrow::ArrayVector& array_vec = table->column(i)->chunks();
            merged_array_vec.insert(
                merged_array_vec.end(), array_vec.begin(), array_vec.end());
        }

        field_ids.push_back(fid);
        field_metas.push_back(field_meta);
        array_vecs.push_back(std::move(merged_array_vec));
    }

    std::unordered_map<FieldId, std::shared_ptr<Chunk>> chunks;
    if (!use_mmap_) {
        chunks = create_group_chunk(
            field_ids, field_metas, array_vecs, mmap_populate_);
    } else {
        std::filesystem::path filepath;
        switch (group_chunk_type_) {
            case GroupChunkType::DEFAULT:
                filepath =
                    std::filesystem::path(column_group_info_.mmap_dir_path) /
                    fmt::format("seg_{}_cg_{}_{}",
                                segment_id_,
                                column_group_info_.field_id,
                                cid);
                break;
            case GroupChunkType::JSON_KEY_STATS:
                filepath =
                    std::filesystem::path(column_group_info_.mmap_dir_path) /
                    fmt::format("seg_{}_jks_{}_cg_{}_{}",
                                segment_id_,
                                column_group_info_.main_field_id,
                                column_group_info_.field_id,
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
