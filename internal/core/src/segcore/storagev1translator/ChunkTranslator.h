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

#include "cachinglayer/Translator.h"
#include "cachinglayer/Utils.h"
#include "common/Chunk.h"
#include "common/type_c.h"
#include "mmap/Types.h"

namespace milvus::segcore::storagev1translator {

struct CTMeta : public milvus::cachinglayer::Meta {
    std::vector<int64_t> num_rows_until_chunk_;
    // virtual chunk is used to speed up the offset->cid translation
    // all virtual chunks have the same number of rows
    std::vector<int64_t>
        vcid_to_cid_arr_;  // the first cid of each virtual chunk
    int64_t
        virt_chunk_order_;  // indicates the size of each virtual chunk, i.e. 2^virt_chunk_order_
    CTMeta(milvus::cachinglayer::StorageType storage_type,
           milvus::cachinglayer::CellIdMappingMode cell_id_mapping_mode,
           milvus::cachinglayer::CellDataType cell_data_type,
           CacheWarmupPolicy cache_warmup_policy,
           bool support_eviction)
        : milvus::cachinglayer::Meta(storage_type,
                                     cell_id_mapping_mode,
                                     cell_data_type,
                                     cache_warmup_policy,
                                     support_eviction) {
    }
};

void
virtual_chunk_config(int64_t total_row_count,
                     int64_t nr_chunks,
                     const std::vector<int64_t>& num_rows_until_chunk,
                     int64_t& virt_chunk_order,
                     std::vector<int64_t>& vcid_to_cid_arr);

// For this translator each Chunk is a CacheCell, cid_t == uid_t.
class ChunkTranslator : public milvus::cachinglayer::Translator<milvus::Chunk> {
 public:
    struct FileInfo {
        std::string file_path;
        int64_t row_count;
        int64_t memory_size;
    };

    ChunkTranslator(int64_t segment_id,
                    FieldMeta field_meta,
                    FieldDataInfo field_data_info,
                    std::vector<FileInfo>&& file_infos,
                    bool use_mmap,
                    milvus::proto::common::LoadPriority load_priority);

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
    get_cells(const std::vector<milvus::cachinglayer::cid_t>& cids) override;

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
            total_size +=
                std::max(file_infos_[cid].memory_size, MIN_STORAGE_BYTES);
        }
        return total_size;
    }

 private:
    std::vector<FileInfo> file_infos_;
    int64_t segment_id_;
    int64_t field_id_;
    std::string key_;
    bool use_mmap_;
    CTMeta meta_;
    FieldMeta field_meta_;
    std::string mmap_dir_path_;
    milvus::proto::common::LoadPriority load_priority_{
        milvus::proto::common::LoadPriority::HIGH};
};

}  // namespace milvus::segcore::storagev1translator
