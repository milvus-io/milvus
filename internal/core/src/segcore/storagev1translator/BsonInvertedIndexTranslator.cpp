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

#include <algorithm>
#include <functional>
#include <limits>
#include <optional>
#include <string_view>
#include <utility>

#include "common/ScopedTimer.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "index/json_stats/bson_inverted.h"
#include "log/Log.h"
#include "pb/common.pb.h"
#include "segcore/CacheMetricAttribution.h"
#include "segcore/Utils.h"
#include "storage/DiskFileManagerImpl.h"
#include "common/Utils.h"
#include "folly/coro/BlockingWait.h"
#include "segcore/storagev2translator/StorageV2Config.h"
#include "storage/AsyncLoadExecutor.h"
#include "storage/LegacyIndexLoader.h"
#include "storage/FileWriter.h"

namespace milvus::segcore::storagev1translator {

BsonInvertedIndexTranslator::BsonInvertedIndexTranslator(
    BsonInvertedIndexLoadInfo load_info,
    milvus::storage::FileManagerContext file_manager_context)
    : load_info_(std::move(load_info)),
      file_manager_context_(std::move(file_manager_context)),
      key_(fmt::format("seg_{}_json_stats_shared_field_{}",
                       load_info_.segment_id,
                       load_info_.field_id)),
      meta_(load_info_.enable_mmap ? milvus::cachinglayer::StorageType::DISK
                                   : milvus::cachinglayer::StorageType::MEMORY,
            milvus::cachinglayer::CellIdMappingMode::ALWAYS_ZERO,
            milvus::segcore::getCellDataType(/* is_vector */ false,
                                             /* is_index */ true),
            milvus::segcore::getCacheWarmupPolicy(load_info_.warmup_policy,
                                                  /* is_vector */ false,
                                                  /* is_index */ true),
            /* support_eviction */ true,
            std::nullopt,
            milvus::segcore::MetricAttributionFromShard(load_info_.shard)) {
    if (load_info_.index_files.empty()) {
        return;
    }
    // Inspect both rollout modes: the switch can change before a cache reload.
    // Keep the existing final-size estimate (which can include other JSON
    // stats), but never reserve less than the decoded shared-key files.
    auto inspect = [&]() -> folly::coro::Task<void> {
        size_t payload_bytes = 0;
        size_t scratch = 0;
        const auto priority =
            static_cast<proto::common::LoadPriority>(load_info_.load_priority);
        for (const auto& file : load_info_.index_files) {
            auto input = storage::OpenLegacyIndexInput(
                file_manager_context_.chunkManagerPtr,
                file_manager_context_.fs,
                file);
            const auto info =
                co_await storage::InspectLegacyIndexFileAsync(*input, priority);
            payload_bytes = SaturatingAdd(payload_bytes, info.payload_bytes);
            scratch = std::max(scratch, info.max_transient_bytes);
        }
        constexpr auto max_size = std::numeric_limits<int64_t>::max();
        load_info_.index_size = std::max(
            load_info_.index_size,
            static_cast<int64_t>(std::min<size_t>(payload_bytes, max_size)));
        stream_memory_overhead_ = static_cast<int64_t>(std::min<size_t>(
            SaturatingAdd(storage::LegacyIndexMaxTransientBytes(scratch),
                          storage::FileWriter::MAX_BUFFER_SIZE),
            max_size));
    };
    if (storagev2translator::StorageV2AsyncLoadEnabled()) {
        folly::coro::blockingWait(
            inspect().scheduleOn(storage::ResolveAsyncLoadExecutor(
                {},
                static_cast<proto::common::LoadPriority>(
                    load_info_.load_priority))));
    } else {
        folly::coro::blockingWait(inspect());
    }
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
        // loaded: on disk; overhead: temp memory for download buffer
        return {{0, load_info_.index_size},
                {std::max(load_info_.index_size, stream_memory_overhead_), 0}};
    } else {
        // loaded: in memory; overhead: temp disk for local file before loading
        return {{load_info_.index_size, 0},
                {stream_memory_overhead_, load_info_.index_size}};
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

    auto disk_file_manager =
        std::make_shared<milvus::storage::DiskFileManagerImpl>(
            file_manager_context_);
    auto index =
        std::make_unique<milvus::index::BsonInvertedIndex>(disk_file_manager);

    {
        milvus::ScopedTimer timer(
            "bson_inverted_index_load",
            [](double /*us*/) {
                // no specific metric defined for bson inverted index load yet
            },
            milvus::ScopedTimer::LogLevel::Info);

        // Load the index using the files from load_info_
        // Files are absolute remote paths (basePath already prepended by caller)
        index->LoadIndex(load_info_.index_files,
                         static_cast<milvus::proto::common::LoadPriority>(
                             load_info_.load_priority),
                         load_info_.enable_mmap,
                         ctx);
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
