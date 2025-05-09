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
#include "mmap/Types.h"

namespace milvus::segcore::storagev1translator {

struct CTMeta : public milvus::cachinglayer::Meta {
    std::vector<int64_t> num_rows_until_chunk_;
    CTMeta(milvus::cachinglayer::StorageType storage_type)
        : milvus::cachinglayer::Meta(storage_type) {
    }
};

// This class will load all cells(Chunks) in ctor, and move them out during get_cells.
// This should be used only in storagev1(no eviction allowed), thus trying to get a
// same cell a second time will result in exception.
// For this translator each Chunk is a CacheCell, cid_t and uid_ is the same.
class ChunkTranslator : public milvus::cachinglayer::Translator<milvus::Chunk> {
 public:
    ChunkTranslator(int64_t segment_id,
                    FieldMeta field_meta,
                    FieldDataInfo field_data_info,
                    std::vector<std::string> insert_files,
                    bool use_mmap);

    ~ChunkTranslator() override;

    size_t
    num_cells() const override;
    milvus::cachinglayer::cid_t
    cell_id_of(milvus::cachinglayer::uid_t uid) const override;
    milvus::cachinglayer::ResourceUsage
    estimated_byte_size_of_cell(milvus::cachinglayer::cid_t cid) const override;
    const std::string&
    key() const override;
    std::vector<
        std::pair<milvus::cachinglayer::cid_t, std::unique_ptr<milvus::Chunk>>>
    get_cells(const std::vector<milvus::cachinglayer::cid_t>& cids) override;

    // TODO: info other than get_cels() should all be in meta()
    milvus::cachinglayer::Meta*
    meta() override {
        return &meta_;
    }

 private:
    int64_t segment_id_;
    std::string key_;
    bool use_mmap_;
    std::vector<milvus::Chunk*> chunks_;
    CTMeta meta_;
};

}  // namespace milvus::segcore::storagev1translator
