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

#include "segcore/storagev2translator/JsonStatsTranslator.h"

#include <utility>

#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/ScopedTimer.h"
#include "fmt/core.h"
#include "index/Meta.h"
#include "log/Log.h"
#include "monitor/Monitor.h"
#include "segcore/CacheMetricAttribution.h"
#include "segcore/Utils.h"
#include "storage/FileManager.h"

namespace milvus::segcore::storagev2translator {

namespace {

CacheWarmupPolicy
AggregateWarmupPolicy(CacheWarmupPolicy columns, CacheWarmupPolicy bson_index) {
    if (columns == CacheWarmupPolicy::CacheWarmupPolicy_Sync ||
        bson_index == CacheWarmupPolicy::CacheWarmupPolicy_Sync) {
        return CacheWarmupPolicy::CacheWarmupPolicy_Sync;
    }
    if (columns == CacheWarmupPolicy::CacheWarmupPolicy_Async ||
        bson_index == CacheWarmupPolicy::CacheWarmupPolicy_Async) {
        return CacheWarmupPolicy::CacheWarmupPolicy_Async;
    }
    return CacheWarmupPolicy::CacheWarmupPolicy_Disable;
}

}  // namespace

JsonStatsTranslator::JsonStatsTranslator(
    JsonStatsLoadInfo load_info,
    std::shared_ptr<const milvus::proto::indexcgo::LoadJsonKeyIndexInfo>
        info_proto,
    milvus::storage::ChunkManagerPtr chunk_manager,
    milvus_storage::ArrowFileSystemPtr fs)
    : load_info_(std::move(load_info)),
      info_proto_(std::move(info_proto)),
      chunk_manager_(std::move(chunk_manager)),
      fs_(std::move(fs)),
      key_(fmt::format("seginst_{}_seg_{}_jsonstats_{}_build_{}_version_{}",
                       load_info_.segment_instance_uid,
                       load_info_.segment_id,
                       info_proto_->fieldid(),
                       info_proto_->buildid(),
                       info_proto_->version())),
      meta_(milvus::cachinglayer::StorageType::MEMORY,
            milvus::cachinglayer::CellIdMappingMode::ALWAYS_ZERO,
            milvus::segcore::getCellDataType(/* is_vector */ false,
                                             /* is_index */ true),
            AggregateWarmupPolicy(milvus::segcore::getCacheWarmupPolicy(
                                      info_proto_->warmup_policy(),
                                      /* is_vector */ false,
                                      /* is_index */ false),
                                  milvus::segcore::getCacheWarmupPolicy(
                                      info_proto_->warmup_policy(),
                                      /* is_vector */ false,
                                      /* is_index */ true)),
            /* support_eviction */ false,
            std::nullopt,
            milvus::segcore::MetricAttributionFromShard(load_info_.shard)) {
}

size_t
JsonStatsTranslator::num_cells() const {
    return 1;
}

milvus::cachinglayer::cid_t
JsonStatsTranslator::cell_id_of(milvus::cachinglayer::uid_t) const {
    return 0;
}

std::pair<milvus::cachinglayer::ResourceUsage,
          milvus::cachinglayer::ResourceUsage>
JsonStatsTranslator::estimated_byte_size_of_cell(
    milvus::cachinglayer::cid_t) const {
    // This coordination cell does not own another copy of the JSON payload.
    // JsonKeyStats metadata remains segment-management overhead; the inner
    // GroupChunk and BSON slots account for their own payloads.
    return {{0, 0}, {0, 0}};
}

int64_t
JsonStatsTranslator::cells_storage_bytes(
    const std::vector<milvus::cachinglayer::cid_t>&) const {
    return 0;
}

const std::string&
JsonStatsTranslator::key() const {
    return key_;
}

std::vector<std::pair<milvus::cachinglayer::cid_t,
                      std::unique_ptr<milvus::index::JsonKeyStats>>>
JsonStatsTranslator::get_cells(
    milvus::OpContext* ctx,
    const std::vector<milvus::cachinglayer::cid_t>& cids) {
    AssertInfo(cids.size() == 1 && cids.front() == 0,
               "JSON stats translator expects only cell 0, got {} cells",
               cids.size());
    CheckCancellation(
        ctx, load_info_.segment_id, "JsonStatsTranslator::get_cells()");

    milvus::storage::FieldDataMeta field_data_meta{info_proto_->collectionid(),
                                                   info_proto_->partitionid(),
                                                   load_info_.segment_id,
                                                   info_proto_->fieldid(),
                                                   info_proto_->schema()};
    milvus::storage::IndexMeta index_meta{load_info_.segment_id,
                                          info_proto_->fieldid(),
                                          info_proto_->buildid(),
                                          info_proto_->version()};
    milvus::storage::FileManagerContext file_ctx(
        field_data_meta, index_meta, chunk_manager_, fs_);

    milvus::Config config;
    std::vector<std::string> files;
    files.reserve(info_proto_->files_size());
    for (const auto& f : info_proto_->files()) {
        files.push_back(f);
    }
    config[milvus::index::INDEX_FILES] = files;
    config[milvus::LOAD_PRIORITY] = info_proto_->load_priority();
    config[milvus::index::ENABLE_MMAP] = info_proto_->enable_mmap();
    if (info_proto_->enable_mmap()) {
        config[milvus::index::MMAP_FILE_PATH] = info_proto_->mmap_dir_path();
    }
    if (!info_proto_->warmup_policy().empty()) {
        config[milvus::index::WARMUP] = info_proto_->warmup_policy();
    }
    config[milvus::index::INDEX_SIZE] = info_proto_->stats_size();
    if (!info_proto_->base_path().empty()) {
        config[STATS_BASE_PATH_KEY] = info_proto_->base_path();
    }
    config[JSON_STATS_CACHE_SHARD_KEY] = load_info_.shard;

    auto stats = std::make_unique<milvus::index::JsonKeyStats>(
        file_ctx, /* is_load */ true);
    {
        milvus::ScopedTimer timer(
            "json_stats_load",
            [](double us) {
                milvus::monitor::internal_json_stats_latency_load.Observe(
                    us / 1000.0);
            },
            milvus::ScopedTimer::LogLevel::Info);
        stats->Load(milvus::tracer::TraceContext{}, config);
    }

    CheckCancellation(
        ctx, load_info_.segment_id, "JsonStatsTranslator::get_cells()");
    LOG_INFO("initialize json stats success for field:{} of segment:{}",
             info_proto_->fieldid(),
             load_info_.segment_id);

    std::vector<std::pair<cid_t, std::unique_ptr<milvus::index::JsonKeyStats>>>
        result;
    result.emplace_back(0, std::move(stats));
    return result;
}

milvus::cachinglayer::Meta*
JsonStatsTranslator::meta() {
    return &meta_;
}

}  // namespace milvus::segcore::storagev2translator
