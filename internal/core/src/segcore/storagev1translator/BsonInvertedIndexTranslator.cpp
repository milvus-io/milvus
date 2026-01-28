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

#include "segcore/storagev1translator/BsonInvertedIndexTranslator.h"

#include <utility>

#include "cachinglayer/CacheSlot.h"
#include "segcore/Utils.h"
#include "segcore/Utils.h"
#include "monitor/Monitor.h"
#include "common/ScopedTimer.h"
#include "log/Log.h"
#include "fmt/format.h"

namespace milvus::segcore::storagev1translator {

BsonInvertedIndexTranslator::BsonInvertedIndexTranslator(
    BsonInvertedIndexLoadInfo load_info,
    std::shared_ptr<milvus::storage::DiskFileManagerImpl> disk_file_manager)
    : load_info_(std::move(load_info)),
      disk_file_manager_(disk_file_manager),
      key_(fmt::format("seg_{}_json_stats_shared_field_{}",
                       load_info_.segment_id,
                       load_info_.field_id)),
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
BsonInvertedIndexTranslator::num_cells() const {
    return 1;
}

milvus::cachinglayer::cid_t
BsonInvertedIndexTranslator::cell_id_of(milvus::cachinglayer::uid_t) const {
    return 0;
}

std::pair<milvus::cachinglayer::ResourceUsage,
          milvus::cachinglayer::ResourceUsage>
BsonInvertedIndexTranslator::estimated_byte_size_of_cell(
    milvus::cachinglayer::cid_t) const {
    // ignore the cid checking, because there is only one cell
    if (load_info_.enable_mmap) {
        return {{0, load_info_.index_size},
                {load_info_.index_size, load_info_.index_size}};
    } else {
        return {{load_info_.index_size, 0},
                {load_info_.index_size, load_info_.index_size}};
    }
}

int64_t
BsonInvertedIndexTranslator::cells_storage_bytes(
    const std::vector<milvus::cachinglayer::cid_t>&) const {
    // ignore the cids checking, because there is only one cell
    constexpr int64_t MIN_STORAGE_BYTES = 1 * 1024 * 1024;
    return std::max(load_info_.index_size, MIN_STORAGE_BYTES);
}

const std::string&
BsonInvertedIndexTranslator::key() const {
    return key_;
}

std::vector<std::pair<milvus::cachinglayer::cid_t,
                      std::unique_ptr<milvus::index::BsonInvertedIndex>>>
BsonInvertedIndexTranslator::get_cells(
    milvus::OpContext* ctx,
    const std::vector<milvus::cachinglayer::cid_t>& cids) {
    // Check for cancellation before loading BSON inverted index
    CheckCancellation(
        ctx, load_info_.segment_id, "BsonInvertedIndexTranslator::get_cells()");

    auto index =
        std::make_unique<milvus::index::BsonInvertedIndex>(disk_file_manager_);

    {
        milvus::ScopedTimer timer(
            "bson_inverted_index_load",
            [](double /*ms*/) {
                // no specific metric defined for bson inverted index load yet
            },
            milvus::ScopedTimer::LogLevel::Info);

        // Load the index using the files from load_info_
        // Cast uint32_t to LoadPriority enum
        index->LoadIndex(load_info_.index_files,
                         static_cast<milvus::proto::common::LoadPriority>(
                             load_info_.load_priority),
                         load_info_.enable_mmap);
    }

    LOG_INFO("load bson inverted index success for field:{} of segment:{}",
             load_info_.field_id,
             load_info_.segment_id);

    std::vector<std::pair<milvus::cachinglayer::cid_t,
                          std::unique_ptr<milvus::index::BsonInvertedIndex>>>
        result;
    result.emplace_back(std::make_pair(0, std::move(index)));
    return result;
}

milvus::cachinglayer::Meta*
BsonInvertedIndexTranslator::meta() {
    return &meta_;
}

}  // namespace milvus::segcore::storagev1translator
