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
#include "milvus-storage/common/constants.h"
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
    const std::vector<milvus_storage::RowGroupMetadataVector>&
        row_group_meta_list,
    int64_t num_fields,
    milvus::proto::common::LoadPriority load_priority)
    : segment_id_(segment_id),
      key_(fmt::format("seg_{}_cg_{}", segment_id, column_group_info.field_id)),
      field_metas_(field_metas),
      column_group_info_(column_group_info),
      insert_files_(insert_files),
      use_mmap_(use_mmap),
      row_group_meta_list_(row_group_meta_list),
      load_priority_(load_priority),
      meta_(
          num_fields,
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
    // TODO(tiered storage 1): should take into consideration of mmap or not.
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

    auto load_future = pool.Submit([&]() {
        return LoadWithStrategy(insert_files_,
                                column_group_info_.arrow_reader_channel,
                                DEFAULT_FIELD_MAX_MEMORY_LIMIT,
                                std::move(strategy),
                                row_group_lists,
                                nullptr,
                                load_priority_);
    });
    LOG_INFO("segment {} submits load column group {} task to thread pool",
             segment_id_,
             column_group_info_.field_id);

    std::shared_ptr<milvus::ArrowDataWrapper> r;
    int64_t cid_idx = 0;
    int64_t total_tables = 0;
    while (column_group_info_.arrow_reader_channel->pop(r)) {
        for (const auto& table : r->arrow_tables) {
            AssertInfo(cid_idx < cids.size(),
                       "Number of tables exceed number of cids ({})",
                       cids.size());
            auto cid = cids[cid_idx++];
            cells.emplace_back(cid, load_group_chunk(table, cid));
            total_tables++;
        }
    }
    AssertInfo(total_tables == cids.size(),
               "Number of tables ({}) does not match number of cids ({})",
               total_tables,
               cids.size());
    return cells;
}

std::unique_ptr<milvus::GroupChunk>
GroupChunkTranslator::load_group_chunk(
    const std::shared_ptr<arrow::Table>& table,
    const milvus::cachinglayer::cid_t cid) {
    // Create chunks for each field in this batch
    std::unordered_map<FieldId, std::shared_ptr<Chunk>> chunks;
    // Iterate through field_id_list to get field_id and create chunk
    for (int i = 0; i < table->schema()->num_fields(); ++i) {
        AssertInfo(table->schema()->field(i)->metadata()->Contains(
                       milvus_storage::ARROW_FIELD_ID_KEY),
                   "field id not found in metadata for field {}",
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
        AssertInfo(it != field_metas_.end(),
                   "Field id not found in field_metas");
        const auto& field_meta = it->second;
        const arrow::ArrayVector& array_vec = table->column(i)->chunks();
        std::unique_ptr<Chunk> chunk;
        if (!use_mmap_) {
            // Memory mode
            chunk = create_chunk(field_meta, array_vec);
        } else {
            // Mmap mode
            auto filepath =
                std::filesystem::path(column_group_info_.mmap_dir_path) /
                std::to_string(segment_id_) / std::to_string(field_id) /
                std::to_string(cid);

            LOG_INFO(
                "storage v2 segment {} mmaping field {} chunk {} to path {}",
                segment_id_,
                field_id,
                cid,
                filepath.string());

            std::filesystem::create_directories(filepath.parent_path());

            chunk = create_chunk(field_meta, array_vec, filepath.string());
            auto ok = unlink(filepath.c_str());
            AssertInfo(
                ok == 0,
                fmt::format(
                    "storage v2 failed to unlink mmap data file {}, err: {}",
                    filepath.c_str(),
                    strerror(errno)));
        }

        chunks[fid] = std::move(chunk);
    }
    return std::make_unique<milvus::GroupChunk>(chunks);
}

}  // namespace milvus::segcore::storagev2translator
