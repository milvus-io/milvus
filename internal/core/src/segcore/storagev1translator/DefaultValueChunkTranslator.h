// Copyright (C) 2019-2025 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include <string>
#include <vector>
#include <optional>

#include "cachinglayer/Translator.h"
#include "cachinglayer/Utils.h"
#include "common/Chunk.h"
#include "common/FieldMeta.h"
#include "common/ChunkWriter.h"
#include "common/Types.h"
#include "mmap/Types.h"
#include "segcore/storagev1translator/ChunkTranslator.h"

namespace milvus::segcore::storagev1translator {

// This is to support add field.
class DefaultValueChunkTranslator
    : public milvus::cachinglayer::Translator<milvus::Chunk> {
 public:
    DefaultValueChunkTranslator(int64_t segment_id,
                                FieldMeta field_meta,
                                FieldDataInfo field_data_info,
                                bool use_mmap,
                                bool mmap_populate,
                                const std::string& warmup_policy);
    ~DefaultValueChunkTranslator() override;
    size_t
    num_cells() const override;
    milvus::cachinglayer::cid_t
    cell_id_of(milvus::cachinglayer::uid_t uid) const override;
    std::pair<milvus::cachinglayer::ResourceUsage,
              milvus::cachinglayer::ResourceUsage>
    estimated_byte_size_of_cell(milvus::cachinglayer::cid_t cid) const override;
    const std::string&
    key() const override;
    std::vector<
        std::pair<milvus::cachinglayer::cid_t, std::unique_ptr<milvus::Chunk>>>
    get_cells(milvus::OpContext* ctx,
              const std::vector<milvus::cachinglayer::cid_t>& cids) override;

    milvus::cachinglayer::Meta*
    meta() override {
        return &meta_;
    }

    int64_t
    cells_storage_bytes(
        const std::vector<milvus::cachinglayer::cid_t>& cids) const override {
        return 0;
    }

    int64_t
    value_size() const;

    // preferred bytes per cell when splitting default-value column
    static constexpr int64_t kTargetCellBytes = 64 * 1024;  // 64KB

 private:
    // total rows of this field in the segment
    int64_t total_rows_{0};

    // Number of rows in the primary cell (all full-sized cells).
    // The last cell may contain fewer rows.
    int64_t primary_cell_rows_{0};

    // Shared chunk buffers for default-value cells. All cells with the same
    // row count will share the same underlying memory via these buffers.
    std::optional<milvus::ChunkBuffer> primary_buffer_;

    int64_t segment_id_;
    std::string key_;
    bool use_mmap_;
    bool mmap_populate_;
    std::string mmap_dir_path_;
    CTMeta meta_;
    milvus::FieldMeta field_meta_;
};

}  // namespace milvus::segcore::storagev1translator
