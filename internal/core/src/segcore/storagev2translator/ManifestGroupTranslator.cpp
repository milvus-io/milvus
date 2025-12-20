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
#include "common/type_c.h"
#include "milvus-storage/reader.h"
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

#include <cstddef>
#include <cstdint>
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

ManifestGroupTranslator::ManifestGroupTranslator(
    int64_t segment_id,
    int64_t column_group_index,
    std::unique_ptr<milvus_storage::api::ChunkReader> chunk_reader,
    const std::unordered_map<FieldId, FieldMeta>& field_metas,
    bool use_mmap,
    const std::string& mmap_dir_path,
    int64_t num_fields,
    milvus::proto::common::LoadPriority load_priority)
    : segment_id_(segment_id),
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
    auto chunk_size_result = chunk_reader_->get_chunk_size();
    if (!chunk_size_result.ok()) {
        throw std::runtime_error("get chunk size failed");
    }
    chunk_size_ = chunk_size_result.ValueOrDie();

    auto rows_result = chunk_reader_->get_chunk_rows();
    if (!rows_result.ok()) {
        throw std::runtime_error("get chunk rows failed");
    }

    auto chunk_rows = rows_result.ValueOrDie();

    meta_.num_rows_until_chunk_.push_back(0);
    for (int i = 0; i < chunk_reader_->total_number_of_chunks(); ++i) {
        meta_.num_rows_until_chunk_.push_back(
            meta_.num_rows_until_chunk_.back() +
            static_cast<int64_t>(chunk_rows[i]));
        meta_.chunk_memory_size_.push_back(
            static_cast<int64_t>(chunk_size_[i]));
    }
}

size_t
ManifestGroupTranslator::num_cells() const {
    return chunk_reader_->total_number_of_chunks();
}

milvus::cachinglayer::cid_t
ManifestGroupTranslator::cell_id_of(milvus::cachinglayer::uid_t uid) const {
    return uid;
}

std::pair<milvus::cachinglayer::ResourceUsage,
          milvus::cachinglayer::ResourceUsage>
ManifestGroupTranslator::estimated_byte_size_of_cell(
    milvus::cachinglayer::cid_t cid) const {
    // return chunk_reader_->get_chunk_size()[cid];
    AssertInfo(cid < chunk_size_.size(), "invalid cid");
    auto cell_sz = static_cast<int64_t>(chunk_size_[cid]);

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
    const std::vector<milvus::cachinglayer::cid_t>& cids) {
    std::vector<std::pair<milvus::cachinglayer::cid_t,
                          std::unique_ptr<milvus::GroupChunk>>>
        cells;
    cells.reserve(cids.size());

    auto parallel_degree =
        static_cast<uint64_t>(DEFAULT_FIELD_MAX_MEMORY_LIMIT / FILE_SLICE_SIZE);

    auto read_result =
        chunk_reader_->get_chunks(cids, static_cast<int64_t>(parallel_degree));

    if (!read_result.ok()) {
        throw std::runtime_error("get chunk failed");
    }

    auto chunks = read_result.ValueOrDie();
    for (size_t i = 0; i < chunks.size(); ++i) {
        auto& chunk = chunks[i];
        AssertInfo(chunk != nullptr,
                   "chunk is null, idx = {}, group index = {}, segment id = "
                   "{}, parallel degree = {}",
                   i,
                   column_group_index_,
                   segment_id_,
                   parallel_degree);
        auto cid = cids[i];
        auto group_chunk = load_group_chunk(chunk, cid);
        cells.emplace_back(cid, std::move(group_chunk));
    }

    return cells;
}

std::unique_ptr<milvus::GroupChunk>
ManifestGroupTranslator::load_group_chunk(
    const std::shared_ptr<arrow::RecordBatch>& record_batch,
    const milvus::cachinglayer::cid_t cid) {
    std::unordered_map<FieldId, std::shared_ptr<Chunk>> chunks;
    // Iterate through field_id_list to get field_id and create chunk
    for (int i = 0; i < record_batch->num_columns(); ++i) {
        // column name here is field id
        auto column_name = record_batch->column_name(i);
        auto field_id = std::stoll(column_name);

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

        const arrow::ArrayVector array_vec = {record_batch->column(i)};
        std::unique_ptr<Chunk> chunk;
        if (!use_mmap_) {
            // Memory mode
            chunk = create_chunk(field_meta, array_vec);
        } else {
            // Mmap mode
            std::filesystem::path filepath;
            if (field_meta.get_main_field_id() != INVALID_FIELD_ID) {
                // json shredding mode
                filepath = std::filesystem::path(mmap_dir_path_) /
                           std::to_string(segment_id_) /
                           std::to_string(field_meta.get_main_field_id()) /
                           std::to_string(field_id) / std::to_string(cid);
            } else {
                filepath = std::filesystem::path(mmap_dir_path_) /
                           std::to_string(segment_id_) /
                           std::to_string(field_id) / std::to_string(cid);
            }

            LOG_INFO(
                "[StorageV2] translator {} mmaping field {} chunk {} to path "
                "{}",
                key_,
                field_id,
                cid,
                filepath.string());

            std::filesystem::create_directories(filepath.parent_path());

            chunk = create_chunk(
                field_meta, array_vec, filepath.string(), load_priority_);
        }

        chunks[fid] = std::move(chunk);
    }
    return std::make_unique<milvus::GroupChunk>(chunks);
}

}  // namespace milvus::segcore::storagev2translator