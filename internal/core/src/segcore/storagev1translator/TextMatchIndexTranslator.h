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
#include "index/TextMatchIndex.h"
#include "storage/FileManager.h"

namespace milvus::segcore::storagev1translator {

struct TextMatchIndexLoadInfo {
    bool enable_mmap;
    int64_t segment_id;
    int64_t field_id;
    std::string analyzer_params;
    int64_t index_size;
};

// Translator for TextMatchIndex (non-knowhere index). It loads a single-cell
// TextMatchIndex instance for a sealed segment field and exposes it to the cache
// layer with a stable key and resource usage.
class TextMatchIndexTranslator
    : public milvus::cachinglayer::Translator<milvus::index::TextMatchIndex> {
 public:
    TextMatchIndexTranslator(
        TextMatchIndexLoadInfo load_info,
        milvus::storage::FileManagerContext file_manager_context,
        milvus::Config config);

    ~TextMatchIndexTranslator() override = default;

    size_t
    num_cells() const override;

    milvus::cachinglayer::cid_t
    cell_id_of(milvus::cachinglayer::uid_t uid) const override;

    std::pair<milvus::cachinglayer::ResourceUsage,
              milvus::cachinglayer::ResourceUsage>
    estimated_byte_size_of_cell(milvus::cachinglayer::cid_t cid) const override;

    int64_t
    cells_storage_bytes(
        const std::vector<milvus::cachinglayer::cid_t>&) const override;

    const std::string&
    key() const override;

    std::vector<std::pair<milvus::cachinglayer::cid_t,
                          std::unique_ptr<milvus::index::TextMatchIndex>>>
    get_cells(const std::vector<milvus::cachinglayer::cid_t>& cids) override;

    milvus::cachinglayer::Meta*
    meta() override;

 private:
    TextMatchIndexLoadInfo load_info_;
    milvus::storage::FileManagerContext file_manager_context_;
    milvus::Config config_;
    std::string key_;
    milvus::cachinglayer::Meta meta_;
};

}  // namespace milvus::segcore::storagev1translator
