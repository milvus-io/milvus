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
#include "common/type_c.h"
#include "segcore/storagev2translator/GroupCTMeta.h"
#include "common/GroupChunk.h"
#include "mmap/Types.h"
#include "common/Types.h"
#include "milvus-storage/common/metadata.h"
#include "milvus-storage/filesystem/fs.h"
#include "milvus-storage/common/constants.h"
#include "milvus-storage/format/parquet/file_reader.h"
#include "storage/ThreadPools.h"
#include "storage/KeyRetriever.h"
#include "segcore/memory_planner.h"

#include <memory>
#include <string>
#include <unordered_set>
#include <vector>
#include <unordered_map>
#include <set>
#include <algorithm>

#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "cachinglayer/Utils.h"
#include "common/ChunkWriter.h"
#include "segcore/Utils.h"

namespace milvus::segcore::storagev2translator {

GroupChunkTranslator::GroupChunkTranslator(
    int64_t segment_id,
    GroupChunkType group_chunk_type,
    const std::unordered_map<FieldId, FieldMeta>& field_metas,
    FieldDataInfo column_group_info,
    std::vector<std::string> insert_files,
    bool use_mmap,
    int64_t num_fields,
    milvus::proto::common::LoadPriority load_priority)
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
            milvus::segcore::getCacheWarmupPolicy(
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

    meta_.num_rows_until_chunk_.reserve(total_row_groups + 1);
    meta_.chunk_memory_size_.reserve(total_row_groups);

    meta_.num_rows_until_chunk_.push_back(0);
    for (const auto& row_group_meta : row_group_meta_list_) {
        for (int i = 0; i < row_group_meta.size(); ++i) {
            meta_.num_rows_until_chunk_.push_back(
                meta_.num_rows_until_chunk_.back() +
                row_group_meta.Get(i).row_num());
            meta_.chunk_memory_size_.push_back(
                row_group_meta.Get(i).memory_size());
        }
    }
    AssertInfo(
        meta_.num_rows_until_chunk_.back() == column_group_info_.row_count,
        fmt::format(
            "[StorageV2] data lost while loading column group {}: found "
            "num rows {} but expected {}",
            column_group_info_.field_id,
            meta_.num_rows_until_chunk_.back(),
            column_group_info_.row_count));
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
    auto [file_idx, row_group_idx] = get_file_and_row_group_index(cid);
    auto& row_group_meta = row_group_meta_list_[file_idx].Get(row_group_idx);

    auto cell_sz = static_cast<int64_t>(row_group_meta.memory_size());

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
GroupChunkTranslator::get_file_and_row_group_index(
    milvus::cachinglayer::cid_t cid) const {
    for (size_t file_idx = 0; file_idx < file_row_group_prefix_sum_.size() - 1;
         ++file_idx) {
        if (cid < file_row_group_prefix_sum_[file_idx + 1]) {
            return {file_idx, cid - file_row_group_prefix_sum_[file_idx]};
        }
    }

    AssertInfo(false,
               fmt::format("[StorageV2] translator {} cid {} is out of range. "
                           "Total row groups across all files: {}",
                           key_,
                           cid,
                           file_row_group_prefix_sum_.back()));
}

milvus::cachinglayer::cid_t
GroupChunkTranslator::get_cid_from_file_and_row_group_index(
    size_t file_idx, size_t row_group_idx) const {
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

// the returned cids are sorted. It may not follow the order of cids.
std::vector<std::pair<cachinglayer::cid_t, std::unique_ptr<milvus::GroupChunk>>>
GroupChunkTranslator::get_cells(const std::vector<cachinglayer::cid_t>& cids) {
    std::vector<std::pair<milvus::cachinglayer::cid_t,
                          std::unique_ptr<milvus::GroupChunk>>>
        cells;
    cells.reserve(cids.size());

    // Create row group lists for requested cids
    std::vector<std::vector<int64_t>> row_group_lists(insert_files_.size());

    for (auto cid : cids) {
        auto [file_idx, row_group_idx] = get_file_and_row_group_index(cid);
        row_group_lists[file_idx].push_back(row_group_idx);
    }

    auto parallel_degree =
        static_cast<uint64_t>(DEFAULT_FIELD_MAX_MEMORY_LIMIT / FILE_SLICE_SIZE);
    auto strategy =
        std::make_unique<ParallelDegreeSplitStrategy>(parallel_degree);

    auto& pool = ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::MIDDLE);
    auto channel = std::make_shared<ArrowReaderChannel>();
    auto fs = milvus_storage::ArrowFileSystemSingleton::GetInstance()
                  .GetArrowFileSystem();

    auto load_future = pool.Submit([&]() {
        return LoadWithStrategy(insert_files_,
                                channel,
                                DEFAULT_FIELD_MAX_MEMORY_LIMIT,
                                std::move(strategy),
                                row_group_lists,
                                fs,
                                nullptr,
                                load_priority_);
    });
    LOG_INFO(
        "[StorageV2] translator {} submits load column group {} task to thread "
        "pool",
        key_,
        column_group_info_.field_id);

    std::shared_ptr<milvus::ArrowDataWrapper> r;
    std::unordered_set<cachinglayer::cid_t> filled_cids;
    filled_cids.reserve(cids.size());
    while (channel->pop(r)) {
        for (const auto& table_info : r->arrow_tables) {
            // Convert file_index and row_group_index to global cid
            auto cid = get_cid_from_file_and_row_group_index(
                table_info.file_index, table_info.row_group_index);
            cells.emplace_back(cid, load_group_chunk(table_info.table, cid));
            filled_cids.insert(cid);
        }
    }

    // access underlying feature to get exception if any
    load_future.get();

    // Verify all requested cids have been filled
    for (auto cid : cids) {
        AssertInfo(filled_cids.find(cid) != filled_cids.end(),
                   "[StorageV2] translator {} cid {} was not filled, missing "
                   "row group id {}",
                   key_,
                   cid,
                   cid);
    }
    return cells;
}

std::unique_ptr<milvus::GroupChunk>
GroupChunkTranslator::load_group_chunk(
    const std::shared_ptr<arrow::Table>& table,
    const milvus::cachinglayer::cid_t cid) {
    AssertInfo(table != nullptr, "arrow table is nullptr");
    // Create chunks for each field in this batch
    std::unordered_map<FieldId, std::shared_ptr<Chunk>> chunks;
    // Iterate through field_id_list to get field_id and create chunk
    std::vector<FieldId> field_ids;
    std::vector<FieldMeta> field_metas;
    std::vector<arrow::ArrayVector> array_vecs;
    field_metas.reserve(table->schema()->num_fields());
    array_vecs.reserve(table->schema()->num_fields());

    for (int i = 0; i < table->schema()->num_fields(); ++i) {
        AssertInfo(table->schema()->field(i)->metadata()->Contains(
                       milvus_storage::ARROW_FIELD_ID_KEY),
                   "[StorageV2] translator {} field id not found in metadata "
                   "for field {}",
                   key_,
                   table->schema()->field(i)->name());
        auto field_id = std::stoll(table->schema()
                                       ->field(i)
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
        const arrow::ArrayVector& array_vec = table->column(i)->chunks();
        field_ids.push_back(fid);
        field_metas.push_back(field_meta);
        array_vecs.push_back(array_vec);
    }

    if (!use_mmap_) {
        chunks = create_group_chunk(field_ids, field_metas, array_vecs);
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
        chunks = create_group_chunk(
            field_ids, field_metas, array_vecs, filepath.string());
    }
    return std::make_unique<milvus::GroupChunk>(chunks);
}

}  // namespace milvus::segcore::storagev2translator
