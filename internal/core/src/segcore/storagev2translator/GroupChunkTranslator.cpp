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
#include "segcore/storagev2translator/GroupCTMeta.h"
#include "common/GroupChunk.h"
#include "mmap/Types.h"
#include "common/Types.h"
#include "milvus-storage/common/metadata.h"
#include "milvus-storage/filesystem/fs.h"
#include "storage/ThreadPools.h"
#include "segcore/memory_planner.h"

#include <string>
#include <vector>
#include <unordered_map>

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
    std::vector<milvus_storage::RowGroupMetadataVector>& row_group_meta_list,
    milvus_storage::FieldIDList field_id_list)
    : segment_id_(segment_id),
      key_(fmt::format("seg_{}_cg_{}", segment_id, column_group_info.field_id)),
      field_metas_(field_metas),
      column_group_info_(column_group_info),
      insert_files_(insert_files),
      use_mmap_(use_mmap),
      row_group_meta_list_(row_group_meta_list),
      field_id_list_(field_id_list),
      meta_(
          field_id_list.size(),
          use_mmap ? milvus::cachinglayer::StorageType::DISK
                   : milvus::cachinglayer::StorageType::MEMORY,
          // TODO(tiered storage 2): vector may be of small size and mixed with scalar, do we force it
          // to use the warm up policy of scalar field?
          milvus::segcore::getCacheWarmupPolicy(/* is_vector */ false,
                                                /* is_index */ false),
          /* support_eviction */ true) {
    AssertInfo(insert_files_.size() == row_group_meta_list_.size(),
               "Number of insert files must match number of row group metas");
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
        fmt::format("data lost while loading column group {}: found "
                    "num rows {} but expected {}",
                    column_group_info_.field_id,
                    meta_.num_rows_until_chunk_.back(),
                    column_group_info_.row_count));
}

GroupChunkTranslator::~GroupChunkTranslator() {
    for (auto chunk : group_chunks_) {
        if (chunk != nullptr) {
            // let the GroupChunk to be deleted by the unique_ptr
            auto chunk_ptr = std::unique_ptr<GroupChunk>(chunk);
        }
    }
}

size_t
GroupChunkTranslator::num_cells() const {
    return meta_.chunk_memory_size_.size();
}

milvus::cachinglayer::cid_t
GroupChunkTranslator::cell_id_of(milvus::cachinglayer::uid_t uid) const {
    return uid;
}

milvus::cachinglayer::ResourceUsage
GroupChunkTranslator::estimated_byte_size_of_cell(
    milvus::cachinglayer::cid_t cid) const {
    auto [file_idx, row_group_idx] = get_file_and_row_group_index(cid);
    auto& row_group_meta = row_group_meta_list_[file_idx].Get(row_group_idx);
    return {static_cast<int64_t>(row_group_meta.memory_size()), 0};
}

const std::string&
GroupChunkTranslator::key() const {
    return key_;
}

std::pair<size_t, size_t>
GroupChunkTranslator::get_file_and_row_group_index(
    milvus::cachinglayer::cid_t cid) const {
    size_t file_idx = 0;
    size_t remaining_cid = cid;

    for (; file_idx < row_group_meta_list_.size(); ++file_idx) {
        const auto& file_metas = row_group_meta_list_[file_idx];
        if (remaining_cid < file_metas.size()) {
            return {file_idx, remaining_cid};
        }
        remaining_cid -= file_metas.size();
    }

    return {0, 0};  // Default to first file and first row group if not found
}

std::vector<std::pair<cachinglayer::cid_t, std::unique_ptr<milvus::GroupChunk>>>
GroupChunkTranslator::get_cells(const std::vector<cachinglayer::cid_t>& cids) {
    std::vector<std::pair<milvus::cachinglayer::cid_t,
                          std::unique_ptr<milvus::GroupChunk>>>
        cells;
    cells.reserve(cids.size());

    // Create row group lists for requested cids
    std::vector<std::vector<int64_t>> row_group_lists;
    row_group_lists.reserve(insert_files_.size());
    for (size_t i = 0; i < insert_files_.size(); ++i) {
        row_group_lists.emplace_back();
    }

    for (auto cid : cids) {
        auto [file_idx, row_group_idx] = get_file_and_row_group_index(cid);
        row_group_lists[file_idx].push_back(row_group_idx);
    }

    auto parallel_degree =
        static_cast<uint64_t>(DEFAULT_FIELD_MAX_MEMORY_LIMIT / FILE_SLICE_SIZE);
    auto strategy =
        std::make_unique<ParallelDegreeSplitStrategy>(parallel_degree);

    auto& pool = ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::MIDDLE);
    auto fs = milvus_storage::ArrowFileSystemSingleton::GetInstance()
                  .GetArrowFileSystem();

    auto load_future = pool.Submit([&]() {
        return LoadWithStrategy(insert_files_,
                                column_group_info_.arrow_reader_channel,
                                DEFAULT_FIELD_MAX_MEMORY_LIMIT,
                                std::move(strategy),
                                row_group_lists);
    });
    LOG_INFO("segment {} submits load fields {} task to thread pool",
             segment_id_,
             field_id_list_.ToString());
    if (!use_mmap_) {
        load_column_group_in_memory();
    } else {
        load_column_group_in_mmap();
    }

    for (auto cid : cids) {
        AssertInfo(group_chunks_[cid] != nullptr,
                   "GroupChunkTranslator::get_cells failed to load cell {} of "
                   "CacheSlot {}.",
                   cid,
                   key_);
        cells.emplace_back(
            cid, std::unique_ptr<milvus::GroupChunk>(group_chunks_[cid]));
        group_chunks_[cid] = nullptr;
    }
    return cells;
}

void
GroupChunkTranslator::load_column_group_in_memory() {
    std::vector<size_t> row_counts(field_id_list_.size(), 0);
    std::shared_ptr<milvus::ArrowDataWrapper> r;
    std::vector<std::string> files;
    std::vector<size_t> file_offsets;
    while (column_group_info_.arrow_reader_channel->pop(r)) {
        for (const auto& table : r->arrow_tables) {
            process_batch(table, files, file_offsets, row_counts);
        }
    }
}

void
GroupChunkTranslator::load_column_group_in_mmap() {
    std::vector<std::string> files;
    std::vector<size_t> file_offsets;
    std::vector<size_t> row_counts;

    // Initialize files and offsets
    for (size_t i = 0; i < field_id_list_.size(); ++i) {
        auto field_id = field_id_list_.Get(i);
        auto filepath =
            std::filesystem::path(column_group_info_.mmap_dir_path) /
            std::to_string(segment_id_) / std::to_string(field_id);
        auto dir = filepath.parent_path();
        std::filesystem::create_directories(dir);
        files.push_back(filepath.string());
        file_offsets.push_back(0);
        row_counts.push_back(0);
    }

    std::shared_ptr<milvus::ArrowDataWrapper> r;
    while (column_group_info_.arrow_reader_channel->pop(r)) {
        for (const auto& table : r->arrow_tables) {
            process_batch(table, files, file_offsets, row_counts);
        }
    }
    for (size_t i = 0; i < files.size(); ++i) {
        auto ok = unlink(files[i].c_str());
        AssertInfo(ok == 0,
                   fmt::format("failed to unlink mmap data file {}, err: {}",
                               files[i].c_str(),
                               strerror(errno)));
    }
}

void
GroupChunkTranslator::process_batch(const std::shared_ptr<arrow::Table>& table,
                                    const std::vector<std::string>& files,
                                    std::vector<size_t>& file_offsets,
                                    std::vector<size_t>& row_counts) {
    // Create chunks for each field in this batch
    std::unordered_map<FieldId, std::shared_ptr<Chunk>> chunks;
    // Iterate through field_id_list to get field_id and create chunk
    for (size_t i = 0; i < field_id_list_.size(); ++i) {
        auto field_id = field_id_list_.Get(i);
        auto fid = milvus::FieldId(field_id);
        if (fid == RowFieldID) {
            // ignore row id field
            continue;
        }
        auto it = field_metas_.find(fid);
        AssertInfo(it != field_metas_.end(),
                   "Field id not found in field_metas");
        const auto& field_meta = it->second;
        const arrow::ArrayVector& array_vec = table->column(i)->chunks();
        auto dim =
            IsVectorDataType(field_meta.get_data_type()) &&
                    !IsSparseFloatVectorDataType(field_meta.get_data_type())
                ? field_meta.get_dim()
                : 1;
        std::unique_ptr<Chunk> chunk;
        if (!use_mmap_) {
            // Memory mode
            chunk = create_chunk(field_meta, dim, array_vec);
        } else {
            // Mmap mode
            int flags = O_RDWR;
            if (file_offsets[i] == 0) {
                // First write to this file, create and truncate
                flags |= O_CREAT | O_TRUNC;
            }
            auto file = File::Open(files[i], flags);
            // should seek to the file offset before writing
            file.Seek(file_offsets[i], SEEK_SET);
            chunk =
                create_chunk(field_meta, dim, file, file_offsets[i], array_vec);
            file_offsets[i] += chunk->Size();
        }

        row_counts[i] += chunk->RowNums();
        chunks[fid] = std::move(chunk);
    }
    // Create GroupChunk from chunks and store in results
    auto group_chunk = std::make_unique<milvus::GroupChunk>(chunks);
    group_chunks_.emplace_back(group_chunk.release());
}

}  // namespace milvus::segcore::storagev2translator
