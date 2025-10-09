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

#include "cachinglayer/CacheSlot.h"
#include "segcore/Utils.h"
#include "monitor/Monitor.h"
#include "common/ScopedTimer.h"

namespace milvus::segcore::storagev2translator {

JsonStatsTranslator::JsonStatsTranslator(
    JsonStatsLoadInfo load_info,
    milvus::tracer::TraceContext ctx,
    milvus::storage::FileManagerContext file_manager_context,
    milvus::Config config)
    : load_info_(load_info),
      ctx_(ctx),
      file_manager_context_(std::move(file_manager_context)),
      config_(std::move(config)),
      key_(fmt::format(
          "seg_{}_jsonstats_{}", load_info_.segment_id, load_info_.field_id)),
      meta_(load_info_.enable_mmap ? milvus::cachinglayer::StorageType::DISK
                                   : milvus::cachinglayer::StorageType::MEMORY,
            milvus::cachinglayer::CellIdMappingMode::ALWAYS_ZERO,
            milvus::segcore::getCellDataType(/* is_vector */ false,
                                             /* is_index */ true),
            milvus::segcore::getCacheWarmupPolicy(/* is_vector */ false,
                                                  /* is_index */ true),
            /* support_eviction */ false) {
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
    if (load_info_.enable_mmap) {
        return {{0, load_info_.stats_size},
                {load_info_.stats_size, load_info_.stats_size}};
    } else {
        return {{load_info_.stats_size, 0}, {load_info_.stats_size, 0}};
    }
}

const std::string&
JsonStatsTranslator::key() const {
    return key_;
}

std::vector<std::pair<milvus::cachinglayer::cid_t,
                      std::unique_ptr<milvus::index::JsonKeyStats>>>
JsonStatsTranslator::get_cells(
    const std::vector<milvus::cachinglayer::cid_t>&) {
    auto stats = std::make_unique<milvus::index::JsonKeyStats>(
        file_manager_context_, /* is_load */ true);

    {
        milvus::ScopedTimer timer(
            "json_stats_load",
            [](double ms) {
                milvus::monitor::internal_json_stats_latency_load.Observe(ms);
            },
            milvus::ScopedTimer::LogLevel::Info);

        stats->Load(ctx_, config_);

        if (load_info_.enable_mmap) {
            stats->SetCellSize({0, load_info_.stats_size});
        } else {
            stats->SetCellSize({load_info_.stats_size, 0});
        }
    }

    LOG_INFO("load json stats success for field:{} of segment:{}",
             load_info_.field_id,
             load_info_.segment_id);

    std::vector<std::pair<cid_t, std::unique_ptr<milvus::index::JsonKeyStats>>>
        result;
    result.emplace_back(std::make_pair(0, std::move(stats)));
    return result;
}

Meta*
JsonStatsTranslator::meta() {
    return &meta_;
}

}  // namespace milvus::segcore::storagev2translator
