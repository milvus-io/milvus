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
#include <optional>
#include <string_view>
#include <utility>
#include "folly/ScopeGuard.h"
#include "folly/coro/BlockingWait.h"
#include "folly/coro/WithCancellation.h"

#include "common/ScopedTimer.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "index/LoadResource.h"
#include "index/Meta.h"
#include "segcore/json_stats/bson_inverted.h"
#include "log/Log.h"
#include "pb/common.pb.h"
#include "segcore/CacheMetricAttribution.h"
#include "segcore/Utils.h"
#include "segcore/storagev1translator/IndexReaderTranslator.h"
#include "storage/DiskFileManagerImpl.h"
#include "storage/AsyncLoadExecutor.h"
#include "storage/LocalFileIOPool.h"
#include "storage/EntryStreamUtils.h"
#include "segcore/storagev2translator/StorageV2Config.h"

namespace milvus::segcore::storagev1translator {

BsonInvertedIndexTranslator::BsonInvertedIndexTranslator(
    BsonInvertedIndexLoadInfo load_info,
    milvus::storage::FileManagerContext file_manager_context,
    milvus::OpContext* op_ctx)
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
    file_manager_context_.use_async_load =
        file_manager_context_.use_async_load.value_or(
            storagev2translator::StorageV2AsyncLoadEnabled());
    if (*file_manager_context_.use_async_load) {
        IndexLoadContext load_ctx(op_ctx, load_info_.load_priority);
        storage::LoadOptions options;
        options.op_ctx = &load_ctx.ctx;
        const std::map<std::string, std::string> params{
            {index::INDEX_TYPE, index::INVERTED_INDEX_TYPE}};
        auto inspect = [&]() -> folly::coro::Task<void> {
            // BSON files already carry complete keys; Index preserves them.
            prepared_source_ = co_await storage::V1RemoteSource::OpenAsync(
                file_manager_context_,
                load_info_.index_files,
                options,
                storage::ArtifactStorageNamespace::Index,
                storage::V1SourceLayout::DiskFiles);
            auto detach = folly::makeGuard([&] {
                prepared_source_->SetLoadContext(
                    proto::common::LoadPriority::HIGH, {});
            });
            async_load_resource_ =
                co_await index::LegacyScalarIndexLoadResourceAsync(
                    DataType::VARCHAR,
                    load_info_.index_size,
                    params,
                    load_info_.enable_mmap,
                    0,
                    *prepared_source_);
        };
        folly::coro::blockingWait(folly::coro::co_withExecutor(
            storage::ResolveAsyncLoadExecutor(
                {},
                load_ctx.ctx.runtime_load_priority.value_or(0) != 0
                    ? proto::common::LoadPriority::LOW
                    : proto::common::LoadPriority::HIGH),
            inspect()));
        if (meta_.cache_warmup_policy ==
            CacheWarmupPolicy::CacheWarmupPolicy_Disable) {
            prepared_source_.reset();
        }
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
    if (async_load_resource_) {
        const auto& request = *async_load_resource_;
        return {{request.final_memory_cost, request.final_disk_cost},
                {request.max_memory_cost - request.final_memory_cost,
                 request.max_disk_cost - request.final_disk_cost}};
    }
    // ignore the cid checking, because there is only one cell
    if (load_info_.enable_mmap) {
        // loaded: on disk; overhead: temp memory for download buffer
        return {{static_cast<int64_t>(index::kScalarIndexFixedResidentBytes),
                 load_info_.index_size},
                {load_info_.index_size, 0}};
    } else {
        // loaded: in memory; overhead: temp disk for local file before loading
        return {
            {load_info_.index_size +
                 static_cast<int64_t>(index::kScalarIndexFixedResidentBytes),
             0},
            {0, load_info_.index_size}};
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
    // Consume preparation even on cancellation; retries always bind a new source.
    auto source = std::move(prepared_source_);
    // Check for cancellation before loading BSON inverted index
    CheckCancellation(
        ctx, load_info_.segment_id, "BsonInvertedIndexTranslator::get_cells()");

    IndexLoadContext load_ctx(ctx, load_info_.load_priority);
    const auto priority = load_ctx.ctx.runtime_load_priority.value_or(0) != 0
                              ? proto::common::LoadPriority::LOW
                              : proto::common::LoadPriority::HIGH;
    const bool use_async = *file_manager_context_.use_async_load;
    storage::LoadOptions options;
    options.enable_mmap = load_info_.enable_mmap;
    options.op_ctx = &load_ctx.ctx;
    auto load =
        [&]() -> folly::coro::Task<std::unique_ptr<index::BsonInvertedIndex>> {
        if (!source) {
            if (use_async) {
                source = co_await storage::V1RemoteSource::OpenAsync(
                    file_manager_context_,
                    load_info_.index_files,
                    options,
                    storage::ArtifactStorageNamespace::Index,
                    storage::V1SourceLayout::DiskFiles);
            } else {
                source = std::make_unique<storage::V1RemoteSource>(
                    file_manager_context_,
                    load_info_.index_files,
                    options,
                    storage::ArtifactStorageNamespace::Index,
                    storage::V1SourceLayout::DiskFiles);
            }
        }
        source->SetLoadContext(priority, load_ctx.ctx.cancellation_token);
        std::unique_ptr<index::BsonInvertedIndex> index;
        auto create = [&] {
            index = std::make_unique<index::BsonInvertedIndex>(
                std::make_shared<storage::DiskFileManagerImpl>(
                    file_manager_context_));
        };
        if (use_async) {
            co_await storage::RunLocalFileIOAsync(create, priority);
        } else {
            create();
        }
        auto result = co_await folly::coro::co_awaitTry(
            index->LoadIndex(*source, options, use_async));
        if (result.hasException() && use_async) {
            co_await storage::RunLocalFileIOAsync([&] { index.reset(); },
                                                  priority);
        }
        result.value();
        co_return index;
    };
    std::unique_ptr<index::BsonInvertedIndex> index;

    {
        milvus::ScopedTimer timer(
            "bson_inverted_index_load",
            [](double /*us*/) {
                // no specific metric defined for bson inverted index load yet
            },
            milvus::ScopedTimer::LogLevel::Info);

        // Load the index using the files from load_info_
        // Files are absolute remote paths (basePath already prepended by caller)
        if (use_async) {
            index = folly::coro::blockingWait(folly::coro::co_withExecutor(
                storage::ResolveAsyncLoadExecutor({}, priority), load()));
        } else {
            index = folly::coro::blockingWait(load());
        }
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
