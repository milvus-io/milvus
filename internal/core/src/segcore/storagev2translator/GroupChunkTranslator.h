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

#include <string>
#include <vector>
#include <memory>
#include <unordered_map>

#include "cachinglayer/Translator.h"
#include "cachinglayer/Utils.h"
#include "milvus-storage/common/metadata.h"
#include "mmap/Types.h"
#include "common/Types.h"
#include "common/GroupChunk.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/InsertRecord.h"
#include "segcore/storagev2translator/GroupCTMeta.h"

namespace milvus::segcore::storagev2translator {

class GroupChunkTranslator
    : public milvus::cachinglayer::Translator<milvus::GroupChunk> {
 public:
    GroupChunkTranslator(
        int64_t segment_id,
        const std::unordered_map<FieldId, FieldMeta>& field_metas,
        FieldDataInfo column_group_info,
        std::vector<std::string> insert_files,
        bool use_mmap,
        int64_t num_fields,
        milvus::proto::common::LoadPriority load_priority);

    ~GroupChunkTranslator() override;

    size_t
    num_cells() const override;

    milvus::cachinglayer::cid_t
    cell_id_of(milvus::cachinglayer::uid_t uid) const override;

    std::pair<milvus::cachinglayer::ResourceUsage,
              milvus::cachinglayer::ResourceUsage>
    estimated_byte_size_of_cell(milvus::cachinglayer::cid_t cid) const override;

    const std::string&
    key() const override;

    std::vector<std::pair<milvus::cachinglayer::cid_t,
                          std::unique_ptr<milvus::GroupChunk>>>
    get_cells(const std::vector<milvus::cachinglayer::cid_t>& cids) override;

    std::pair<size_t, size_t>
    get_file_and_row_group_index(milvus::cachinglayer::cid_t cid) const;

    milvus::cachinglayer::cid_t
    get_cid_from_file_and_row_group_index(size_t file_idx,
                                          size_t row_group_idx) const;

    milvus::cachinglayer::Meta*
    meta() override {
        return &meta_;
    }

    int64_t
    cells_storage_bytes(
        const std::vector<milvus::cachinglayer::cid_t>& cids) const override {
        constexpr int64_t MIN_STORAGE_BYTES = 1 * 1024 * 1024;
        int64_t total_size = 0;
        for (auto cid : cids) {
            auto [file_idx, row_group_idx] = get_file_and_row_group_index(cid);
            auto& row_group_meta =
                row_group_meta_list_[file_idx].Get(row_group_idx);
            total_size +=
                std::max(static_cast<int64_t>(row_group_meta.memory_size()),
                         MIN_STORAGE_BYTES);
        }
        return total_size;
    }

 private:
    std::unique_ptr<milvus::GroupChunk>
    load_group_chunk(const std::shared_ptr<arrow::Table>& table,
                     const milvus::cachinglayer::cid_t cid);

    int64_t segment_id_;
    std::string key_;
    std::unordered_map<FieldId, FieldMeta> field_metas_;
    FieldDataInfo column_group_info_;
    std::vector<std::string> insert_files_;
    std::vector<milvus_storage::RowGroupMetadataVector> row_group_meta_list_;
    std::vector<size_t> file_row_group_prefix_sum_;
    SchemaPtr schema_;
    bool is_sorted_by_pk_;
    ChunkedSegmentSealedImpl* chunked_segment_;
    std::unique_ptr<milvus::segcore::InsertRecord<true>> ir_;
    GroupCTMeta meta_;
    int64_t timestamp_offet_;
    bool use_mmap_;
    milvus::proto::common::LoadPriority load_priority_{
        milvus::proto::common::LoadPriority::HIGH};
};

}  // namespace milvus::segcore::storagev2translator
