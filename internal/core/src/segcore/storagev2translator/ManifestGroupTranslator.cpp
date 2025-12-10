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
    int64_t num_fields,
    milvus::proto::common::LoadPriority load_priority)
    : segment_id_(segment_id),
      column_group_index_(column_group_index),
      chunk_reader_(std::move(chunk_reader)),
      key_(fmt::format("seg_{}_cg_{}", segment_id, column_group_index)),
      field_metas_(field_metas),
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
            /* support_eviction */ true),
      use_mmap_(use_mmap),
      load_priority_(load_priority) {
    auto chunk_size_result = chunk_reader_->get_chunk_size();
    if (!chunk_size_result.ok()) {
        throw std::runtime_error("get row group size failed");
    }
    row_group_sizes_ = chunk_size_result.ValueOrDie();

    auto rows_result = chunk_reader_->get_chunk_rows();
    if (!rows_result.ok()) {
        throw std::runtime_error("get row group rows failed");
    }
    row_group_rows_ = rows_result.ValueOrDie();

    // Merge row groups into group chunks(cache cells)
    size_t total_row_groups = row_group_sizes_.size();
    meta_.total_row_groups_ = total_row_groups;
    size_t num_cells =
        (total_row_groups + kRowGroupsPerCell - 1) / kRowGroupsPerCell;

    // Build num_rows_until_chunk_ and chunk_memory_size_
    meta_.num_rows_until_chunk_.reserve(num_cells + 1);
    meta_.num_rows_until_chunk_.push_back(0);
    meta_.chunk_memory_size_.reserve(num_cells);

    int64_t cumulative_rows = 0;
    for (size_t cell_id = 0; cell_id < num_cells; ++cell_id) {
        auto [start, end] = meta_.get_row_group_range(cell_id);
        int64_t cell_size = 0;
        for (size_t i = start; i < end; ++i) {
            cumulative_rows += static_cast<int64_t>(row_group_rows_[i]);
            cell_size += static_cast<int64_t>(row_group_sizes_[i]);
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
    AssertInfo(cid < meta_.chunk_memory_size_.size(),
               fmt::format("[StorageV2] translator {} cid {} is out of range. "
                           "Total cells: {}",
                           key_,
                           cid,
                           meta_.chunk_memory_size_.size()));

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
    const std::vector<milvus::cachinglayer::cid_t>& cids) {
    std::vector<std::pair<milvus::cachinglayer::cid_t,
                          std::unique_ptr<milvus::GroupChunk>>>
        cells;
    cells.reserve(cids.size());

    // Collect all row group indices needed for the requested cells
    std::vector<cachinglayer::cid_t> needed_row_group_indices;
    for (auto cid : cids) {
        AssertInfo(cid < meta_.chunk_memory_size_.size(),
                   fmt::format("[StorageV2] translator {} cid {} is out of "
                               "range. Total cells: {}",
                               key_,
                               cid,
                               meta_.chunk_memory_size_.size()));
        auto [start, end] = meta_.get_row_group_range(cid);
        for (size_t i = start; i < end; ++i) {
            needed_row_group_indices.push_back(
                static_cast<cachinglayer::cid_t>(i));
        }
    }

    auto parallel_degree =
        static_cast<uint64_t>(DEFAULT_FIELD_MAX_MEMORY_LIMIT / FILE_SLICE_SIZE);

    auto read_result = chunk_reader_->get_chunks(
        needed_row_group_indices, static_cast<int64_t>(parallel_degree));

    if (!read_result.ok()) {
        throw std::runtime_error("get row group failed");
    }

    auto loaded_row_groups = read_result.ValueOrDie();

    // Build a map from row group index to loaded record batch
    std::unordered_map<size_t, std::shared_ptr<arrow::RecordBatch>>
        row_group_map;
    for (size_t i = 0; i < needed_row_group_indices.size(); ++i) {
        row_group_map[needed_row_group_indices[i]] = loaded_row_groups[i];
    }

    // Build cells from collected row groups
    std::unordered_set<cachinglayer::cid_t> filled_cids;
    filled_cids.reserve(cids.size());
    for (auto cid : cids) {
        if (filled_cids.count(cid) > 0) {
            continue;  // Already processed this cell
        }

        auto [start, end] = meta_.get_row_group_range(cid);
        std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches;
        record_batches.reserve(end - start);

        for (size_t i = start; i < end; ++i) {
            auto it = row_group_map.find(i);
            AssertInfo(it != row_group_map.end(),
                       fmt::format("[StorageV2] translator {} row group {} "
                                   "for cell {} was not loaded",
                                   key_,
                                   i,
                                   cid));
            AssertInfo(it->second != nullptr,
                       "row group is null, idx = {}, column group index = {}, "
                       "segment id = {}, parallel degree = {}",
                       i,
                       column_group_index_,
                       segment_id_,
                       parallel_degree);
            record_batches.push_back(it->second);
        }

        cells.emplace_back(cid, load_group_chunk(record_batches, cid));
        filled_cids.insert(cid);
    }

    return cells;
}

std::unique_ptr<milvus::GroupChunk>
ManifestGroupTranslator::load_group_chunk(
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& record_batches,
    milvus::cachinglayer::cid_t cid) {
    AssertInfo(!record_batches.empty(), "record_batches vector is empty");
    for (const auto& batch : record_batches) {
        AssertInfo(batch != nullptr, "arrow record batch is nullptr");
    }

    // Use the first record batch's schema as reference for field iteration
    const auto& first_batch = record_batches[0];

    std::vector<FieldId> field_ids;
    std::vector<FieldMeta> field_metas;
    std::vector<arrow::ArrayVector> array_vecs;
    field_ids.reserve(first_batch->num_columns());
    field_metas.reserve(first_batch->num_columns());
    array_vecs.reserve(first_batch->num_columns());

    for (int i = 0; i < first_batch->num_columns(); ++i) {
        // column name here is field id
        auto column_name = first_batch->column_name(i);
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

        // Merge arrays from all record batches for this field
        // All record batches in a cell come from the same column group with consistent schema
        arrow::ArrayVector merged_array_vec;
        for (const auto& batch : record_batches) {
            merged_array_vec.push_back(batch->column(i));
        }

        field_ids.push_back(fid);
        field_metas.push_back(field_meta);
        array_vecs.push_back(std::move(merged_array_vec));
    }

    std::unordered_map<FieldId, std::shared_ptr<Chunk>> chunks;
    if (!use_mmap_) {
        chunks = create_group_chunk(field_ids, field_metas, array_vecs);
    } else {
        std::filesystem::path filepath =
            std::filesystem::path(mmap_dir_path_) /
            fmt::format(
                "seg_{}_cg_{}_{}", segment_id_, column_group_index_, cid);
        std::filesystem::create_directories(filepath.parent_path());
        chunks = create_group_chunk(
            field_ids, field_metas, array_vecs, filepath.string());
    }
    return std::make_unique<milvus::GroupChunk>(chunks);
}

}  // namespace milvus::segcore::storagev2translator