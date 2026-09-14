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

#include <algorithm>
#include <cstdint>
#include <functional>
#include <optional>
#include <string_view>
#include <type_traits>
#include <utility>

#include "common/ScopedTimer.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "index/TextMatchIndex.h"
#include "index/IndexFactory.h"
#include "log/Log.h"
#include "segcore/CacheMetricAttribution.h"
#include "segcore/Utils.h"
#include "segcore/storagev2translator/StorageV2Config.h"
#include "storage/LegacyIndexLoader.h"
#include "storage/AsyncLoadExecutor.h"
#include <folly/coro/BlockingWait.h>

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
            milvus::segcore::getCacheWarmupPolicy(load_info_.warmup_policy,
                                                  /* is_vector */ false,
                                                  /* is_index */ true),
            /* support_eviction */ true,
            std::nullopt,
            milvus::segcore::MetricAttributionFromShard(load_info_.shard)) {
    auto files = config_.at(index::INDEX_FILES).get<std::vector<std::string>>();
    const bool packed = files.size() == 1 && files.front().ends_with(".v3");
    if (!packed) {
        const auto base_path =
            index::GetValueFromConfig<std::string>(config_, STATS_BASE_PATH_KEY)
                .value_or("");
        AssertInfo(!base_path.empty(),
                   "stats_base_path is required for loading text index");
        for (auto& file : files) {
            file = base_path + "/" + file;
        }
        auto inspect = [&] {
            return storage::InspectLegacyIndexFilesAsync(
                files,
                file_manager_context_.chunkManagerPtr,
                file_manager_context_.fs,
                proto::common::LoadPriority::HIGH);
        };
        file_manager_context_.legacy_index_files =
            storagev2translator::StorageV2AsyncLoadEnabled()
                ? folly::coro::blockingWait(
                      inspect().scheduleOn(storage::ResolveAsyncLoadExecutor(
                          {}, proto::common::LoadPriority::HIGH)))
                : folly::coro::blockingWait(inspect());
    }
    // TextMatch persists Tantivy files and the same null-offset sidecar.
    // Keep one estimate for both routes so cached reloads can switch modes.
    auto resources =
        index::IndexFactory::GetInstance().ScalarIndexFileLoadResource(
            DataType::VARCHAR,
            load_info_.index_size,
            {{index::INDEX_TYPE, index::INVERTED_INDEX_TYPE},
             {index::SCALAR_INDEX_ENGINE_VERSION, packed ? "3" : "2"}},
            load_info_.enable_mmap,
            load_info_.num_rows,
            files,
            file_manager_context_,
            /*is_index_file=*/false);
    load_resource_request_ = resources.request;
    meta_.loading_overhead_config = std::move(resources.overhead);
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
    return {{load_resource_request_.final_memory_cost,
             load_resource_request_.final_disk_cost},
            {load_resource_request_.max_memory_cost -
                 load_resource_request_.final_memory_cost,
             load_resource_request_.max_disk_cost -
                 load_resource_request_.final_disk_cost}};
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
    milvus::OpContext* ctx,
    const std::vector<milvus::cachinglayer::cid_t>& cids) {
    // Check for cancellation before loading text match index
    CheckCancellation(
        ctx, load_info_.segment_id, "TextMatchIndexTranslator::get_cells()");

    auto index =
        std::make_unique<milvus::index::TextMatchIndex>(file_manager_context_);

    {
        milvus::ScopedTimer timer(
            "text_match_index_load",
            [](double /*us*/) {
                // no specific metric defined for text match index load yet
            },
            milvus::ScopedTimer::LogLevel::Info);
        index->Load(config_, ctx);
        index->RegisterAnalyzer("milvus_tokenizer",
                                load_info_.analyzer_params.c_str());
    }

    CheckCancellation(
        ctx, load_info_.segment_id, "TextMatchIndexTranslator::get_cells()");

    LOG_INFO("load text match index success for field:{} of segment:{}",
             load_info_.field_id,
             load_info_.segment_id);

    if (load_info_.enable_mmap) {
        auto bitmap_bytes = index->ValidityBitmapByteSize();
        index->SetCellSize({bitmap_bytes, index->ByteSize() - bitmap_bytes});
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
