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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "cachinglayer/Translator.h"
#include "common/Types.h"
#include "index/json_stats/JsonKeyStats.h"
#include "milvus-storage/filesystem/fs.h"
#include "pb/index_cgo_msg.pb.h"
#include "storage/ChunkManager.h"

namespace milvus::segcore::storagev2translator {

struct JsonStatsLoadInfo {
    uint64_t segment_instance_uid;
    int64_t segment_id;
    std::string shard;
};

// The outer JSON-stats slot owns initialization metadata only. Shredding data
// and the BSON path index keep their own independently evictable cache slots.
class JsonStatsTranslator
    : public milvus::cachinglayer::Translator<milvus::index::JsonKeyStats> {
 public:
    JsonStatsTranslator(
        JsonStatsLoadInfo load_info,
        std::shared_ptr<const milvus::proto::indexcgo::LoadJsonKeyIndexInfo>
            info_proto,
        milvus::storage::ChunkManagerPtr chunk_manager,
        milvus_storage::ArrowFileSystemPtr fs);

    ~JsonStatsTranslator() override = default;

    size_t
    num_cells() const override;

    milvus::cachinglayer::cid_t
    cell_id_of(milvus::cachinglayer::uid_t uid) const override;

    std::pair<milvus::cachinglayer::ResourceUsage,
              milvus::cachinglayer::ResourceUsage>
    estimated_byte_size_of_cell(milvus::cachinglayer::cid_t cid) const override;

    int64_t
    cells_storage_bytes(
        const std::vector<milvus::cachinglayer::cid_t>& cids) const override;

    const std::string&
    key() const override;

    std::vector<std::pair<milvus::cachinglayer::cid_t,
                          std::unique_ptr<milvus::index::JsonKeyStats>>>
    get_cells(milvus::OpContext* ctx,
              const std::vector<milvus::cachinglayer::cid_t>& cids) override;

    milvus::cachinglayer::Meta*
    meta() override;

 private:
    JsonStatsLoadInfo load_info_;
    // Keep the source descriptor for retries, including failed cache publication.
    std::shared_ptr<const milvus::proto::indexcgo::LoadJsonKeyIndexInfo>
        info_proto_;
    milvus::storage::ChunkManagerPtr chunk_manager_;
    milvus_storage::ArrowFileSystemPtr fs_;
    std::string key_;
    milvus::cachinglayer::Meta meta_;
};

}  // namespace milvus::segcore::storagev2translator
