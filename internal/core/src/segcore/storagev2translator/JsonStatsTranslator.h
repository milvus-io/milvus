// Copyright (C) 2019-2020 Zilliz. All rights reserved.
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

#include <memory>
#include <string>
#include <vector>

#include "cachinglayer/Translator.h"
#include "common/Types.h"
#include "common/LoadInfo.h"
#include "index/json_stats/JsonKeyStats.h"
#include "storage/FileManager.h"

namespace milvus::segcore::storagev2translator {

struct JsonStatsLoadInfo {
    bool enable_mmap;
    std::string mmap_dir_path;
    int64_t segment_id;
    int64_t field_id;
    int64_t stats_size;
};

// Translator for JSON Key Stats (non-knowhere index). It loads a single-cell
// JsonKeyStats instance for a sealed segment field and exposes it to the cache
// layer with a stable key and resource usage.
class JsonStatsTranslator
    : public milvus::cachinglayer::Translator<milvus::index::JsonKeyStats> {
 public:
    JsonStatsTranslator(
        JsonStatsLoadInfo load_info,
        milvus::tracer::TraceContext ctx,
        milvus::storage::FileManagerContext file_manager_context,
        milvus::Config config);

    ~JsonStatsTranslator() override = default;

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
                          std::unique_ptr<milvus::index::JsonKeyStats>>>
    get_cells(const std::vector<milvus::cachinglayer::cid_t>& cids) override;

    Meta*
    meta() override;

    int64_t
    cells_storage_bytes(
        const std::vector<milvus::cachinglayer::cid_t>& cids) const override {
        constexpr int64_t MIN_STORAGE_BYTES = 1 * 1024 * 1024;
        return std::max(load_info_.stats_size, MIN_STORAGE_BYTES);
    }

 private:
    milvus::tracer::TraceContext ctx_;
    milvus::storage::FileManagerContext file_manager_context_;
    milvus::Config config_;
    std::string key_;
    JsonStatsLoadInfo load_info_{};
    milvus::cachinglayer::Meta meta_;
};

}  // namespace milvus::segcore::storagev2translator
