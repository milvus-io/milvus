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
    const std::unordered_map<FieldId, FieldMeta>& field_metas,
    FieldDataInfo column_group_info,
    std::vector<std::string> insert_files,
    bool use_mmap,
    int64_t num_fields,
    milvus::proto::common::LoadPriority load_priority)
    : segment_id_(segment_id),
      key_(fmt::format("seg_{}_cg_{}", segment_id, column_group_info.field_id)),
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
    for (const auto& file : insert_files_) {
        auto reader = std::make_shared<milvus_storage::FileRowGroupReader>(
            fs,
            file,
            milvus_storage::DEFAULT_READ_BUFFER_SIZE,
            storage::GetReaderProperties());
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
    for (const auto& file_metas : row_group_meta_list_) {
        file_row_group_prefix_sum_.push_back(file_row_group_prefix_sum_.back() +
                                             file_metas.size());
    }

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
        return {{0, cell_sz}, {2 * cell_sz, cell_sz}};
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

    auto load_future = pool.Submit([&]() {
        return LoadWithStrategy(insert_files_,
                                channel,
                                DEFAULT_FIELD_MAX_MEMORY_LIMIT,
                                std::move(strategy),
                                row_group_lists,
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
        std::unique_ptr<Chunk> chunk;
        if (!use_mmap_) {
            // Memory mode
            chunk = create_chunk(field_meta, array_vec);
        } else {
            // Mmap mode
            std::filesystem::path filepath;
            if (field_meta.get_main_field_id() != INVALID_FIELD_ID) {
                // json shredding mode
                filepath =
                    std::filesystem::path(column_group_info_.mmap_dir_path) /
                    std::to_string(segment_id_) /
                    std::to_string(field_meta.get_main_field_id()) /
                    std::to_string(field_id) / std::to_string(cid);
            } else {
                filepath =
                    std::filesystem::path(column_group_info_.mmap_dir_path) /
                    std::to_string(segment_id_) / std::to_string(field_id) /
                    std::to_string(cid);
            }

            LOG_INFO(
                "[StorageV2] translator {} mmaping field {} chunk {} to path "
                "{}",
                key_,
                field_id,
                cid,
                filepath.string());

            std::filesystem::create_directories(filepath.parent_path());

            chunk = create_chunk(field_meta, array_vec, filepath.string());
        }

        chunks[fid] = std::move(chunk);
    }
    return std::make_unique<milvus::GroupChunk>(chunks);
}

}  // namespace milvus::segcore::storagev2translator
