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
#pragma once

#include <assert.h>
#include <stdint.h>
#include <algorithm>
#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "NamedType/underlying_functionalities.hpp"
#include "arrow/record_batch.h"
#include "cachinglayer/Translator.h"
#include "cachinglayer/Utils.h"
#include "common/FieldMeta.h"
#include "common/GroupChunk.h"
#include "common/OpContext.h"
#include "common/Types.h"
#include "common/protobuf_utils.h"
#include "milvus-storage/reader.h"
#include "pb/common.pb.h"
#include "segcore/storagev2translator/GroupCTMeta.h"

namespace milvus::segcore::storagev2translator {

/**
 * @brief Translator for loading column groups from milvus storage manifest
 *
 * This class implements the Translator interface to load column group data
 * from milvus storage v2 format. It reads chunks from a ChunkReader and
 * translates them into GroupChunk objects for caching.
 */
class ManifestGroupTranslator
    : public milvus::cachinglayer::Translator<milvus::GroupChunk> {
 public:
    /**
     * @brief Construct a translator for a column group
     *
     * @param segment_id ID of the segment being loaded
     * @param group_chunk_type Type of the group chunk
     * @param column_group_index Index of the column group within the segment
     * @param chunk_reader Reader for accessing chunks from storage
     * @param field_metas Metadata for all fields in this column group
     * @param use_mmap Whether to use memory mapping for data loading
     * @param mmap_populate Whether to populate data into memory mapping
     * @param mmap_dir_path Directory path for memory mapping
     * @param num_fields Total number of fields in the column group
     * @param load_priority Priority level for loading operations
     */
    ManifestGroupTranslator(
        int64_t segment_id,
        GroupChunkType group_chunk_type,
        int64_t column_group_index,
        std::unique_ptr<milvus_storage::api::ChunkReader> chunk_reader,
        const std::unordered_map<FieldId, FieldMeta>& field_metas,
        bool use_mmap,
        bool mmap_populate,
        const std::string& mmap_dir_path,
        int64_t num_fields,
        milvus::proto::common::LoadPriority load_priority,
        bool eager_load,
        const std::string& warmup_policy);
    ~ManifestGroupTranslator() = default;

    /**
     * @brief Get the total number of cells (chunks) in this column group
     *
     * @return Number of chunks available in the chunk reader
     */
    size_t
    num_cells() const override;

    /**
     * @brief Map a unit ID to its corresponding cell ID
     *
     * For this translator, the mapping is identical (uid == cid).
     *
     * @param uid Unit ID to map
     * @return Corresponding cell ID
     */
    milvus::cachinglayer::cid_t
    cell_id_of(milvus::cachinglayer::uid_t uid) const override;

    /**
     * @brief Estimate memory and disk usage for a cell
     *
     * Returns resource usage estimates for loading a specific chunk.
     * For mmap mode, reserves double the disk space for temporary files.
     *
     * @param cid Cell ID to estimate
     * @return Pair of (memory_usage, disk_usage) for loading and storage
     */
    std::pair<milvus::cachinglayer::ResourceUsage,
              milvus::cachinglayer::ResourceUsage>
    estimated_byte_size_of_cell(milvus::cachinglayer::cid_t cid) const override;

    /**
     * @brief Get the cache key for this translator
     *
     * @return Cache key in format "seg_{segment_id}_cg_{column_group_index}"
     */
    const std::string&
    key() const override;

    /**
     * @brief Load specified cells (chunks) from storage
     *
     * Reads the requested chunks from the chunk reader and converts them
     * to GroupChunk objects containing field data.
     *
     * @param cids List of cell IDs to load
     * @return Vector of (cell_id, GroupChunk) pairs
     */
    std::vector<std::pair<milvus::cachinglayer::cid_t,
                          std::unique_ptr<milvus::GroupChunk>>>
    get_cells(milvus::OpContext* ctx,
              const std::vector<milvus::cachinglayer::cid_t>& cids) override;

    /**
     * @brief Get the metadata object for this translator
     *
     * @return Pointer to the GroupCTMeta metadata
     */
    milvus::cachinglayer::Meta*
    meta() override {
        return &meta_;
    }

    /**
     * @brief Calculate total storage bytes needed for loading cells
     *
     * Sums up the storage size for all requested cells, with a minimum
     * size of 1MB per cell.
     *
     * @param cids List of cell IDs
     * @return Total storage bytes required
     */
    int64_t
    cells_storage_bytes(
        const std::vector<milvus::cachinglayer::cid_t>& cids) const override {
        constexpr int64_t MIN_STORAGE_BYTES = 1 * 1024 * 1024;
        int64_t total_size = 0;

        for (auto cid : cids) {
            assert(cid < meta_.chunk_memory_size_.size());
            total_size +=
                std::max(meta_.chunk_memory_size_[cid], MIN_STORAGE_BYTES);
        }
        return total_size;
    }

 private:
    /**
     * @brief Load a cell from multiple Arrow RecordBatches
     *
     * Converts multiple Arrow RecordBatches (from row groups) into a single
     * GroupChunk containing merged field data for all columns.
     *
     * @param record_batches Arrow RecordBatches from row groups
     * @param cid Cell ID of the chunk being loaded
     * @return GroupChunk containing the loaded field data
     */
    std::unique_ptr<milvus::GroupChunk>
    load_group_chunk(
        const std::vector<std::shared_ptr<arrow::RecordBatch>>& record_batches,
        milvus::cachinglayer::cid_t cid);

    int64_t segment_id_;
    GroupChunkType group_chunk_type_;
    int64_t column_group_index_;
    std::string key_;
    std::unordered_map<FieldId, FieldMeta> field_metas_;
    std::unique_ptr<milvus_storage::api::ChunkReader> chunk_reader_;

    GroupCTMeta meta_;
    bool use_mmap_;
    bool mmap_populate_;
    std::string mmap_dir_path_;
    milvus::proto::common::LoadPriority load_priority_{
        milvus::proto::common::LoadPriority::HIGH};
};

}  // namespace milvus::segcore::storagev2translator