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

#include "segcore/storagev1translator/TextMatchIndexTranslator.h"

#include <utility>

#include "cachinglayer/CacheSlot.h"
#include "segcore/Utils.h"
#include "monitor/Monitor.h"
#include "common/ScopedTimer.h"

namespace milvus::segcore::storagev1translator {

TextMatchIndexTranslator::TextMatchIndexTranslator(
    TextMatchIndexLoadInfo load_info,
    milvus::storage::FileManagerContext file_manager_context,
    milvus::Config config)
    : load_info_(std::move(load_info)),
      file_manager_context_(std::move(file_manager_context)),
      config_(std::move(config)),
      key_(fmt::format(
          "seg_{}_textindex_{}", load_info_.segment_id, load_info_.field_id)),
      meta_(load_info_.enable_mmap ? milvus::cachinglayer::StorageType::DISK
                                   : milvus::cachinglayer::StorageType::MEMORY,
            milvus::cachinglayer::CellIdMappingMode::ALWAYS_ZERO,
            milvus::segcore::getCellDataType(/* is_vector */ false,
                                             /* is_index */ true),
            milvus::segcore::getCacheWarmupPolicy(/* is_vector */ false,
                                                  /* is_index */ true),
            /* support_eviction */ true) {
}

size_t
TextMatchIndexTranslator::num_cells() const {
    return 1;
}

milvus::cachinglayer::cid_t
TextMatchIndexTranslator::cell_id_of(milvus::cachinglayer::uid_t) const {
    return 0;
}

std::pair<milvus::cachinglayer::ResourceUsage,
          milvus::cachinglayer::ResourceUsage>
TextMatchIndexTranslator::estimated_byte_size_of_cell(
    milvus::cachinglayer::cid_t) const {
    // ignore the cid checking, because there is only one cell
    if (load_info_.enable_mmap) {
        return {{0, load_info_.index_size},
                {load_info_.index_size, load_info_.index_size}};
    } else {
        // The reason the maximum disk usage is not zero is that the text match index
        // is first written to the disk, then loaded into memory. Only after that are
        // the disk files deleted.
        return {{load_info_.index_size, 0},
                {load_info_.index_size, load_info_.index_size}};
    }
}

int64_t
TextMatchIndexTranslator::cells_storage_bytes(
    const std::vector<milvus::cachinglayer::cid_t>&) const {
    // ignore the cids checking, because there is only one cell
    constexpr int64_t MIN_STORAGE_BYTES = 1 * 1024 * 1024;
    return std::max(load_info_.index_size, MIN_STORAGE_BYTES);
}

const std::string&
TextMatchIndexTranslator::key() const {
    return key_;
}

std::vector<std::pair<milvus::cachinglayer::cid_t,
                      std::unique_ptr<milvus::index::TextMatchIndex>>>
TextMatchIndexTranslator::get_cells(
    const std::vector<milvus::cachinglayer::cid_t>&) {
    auto index =
        std::make_unique<milvus::index::TextMatchIndex>(file_manager_context_);

    {
        milvus::ScopedTimer timer(
            "text_match_index_load",
            [](double /*ms*/) {
                // no specific metric defined for text match index load yet
            },
            milvus::ScopedTimer::LogLevel::Info);
        index->Load(config_);
        index->RegisterTokenizer("milvus_tokenizer",
                                 load_info_.analyzer_params.c_str());
    }

    LOG_INFO("load text match index success for field:{} of segment:{}",
             load_info_.field_id,
             load_info_.segment_id);

    if (load_info_.enable_mmap) {
        index->SetCellSize({0, index->ByteSize()});
    } else {
        index->SetCellSize({index->ByteSize(), 0});
    }

    std::vector<std::pair<milvus::cachinglayer::cid_t,
                          std::unique_ptr<milvus::index::TextMatchIndex>>>
        result;
    result.emplace_back(std::make_pair(0, std::move(index)));
    return result;
}

milvus::cachinglayer::Meta*
TextMatchIndexTranslator::meta() {
    return &meta_;
}

}  // namespace milvus::segcore::storagev1translator
