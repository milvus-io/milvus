// Copyright (C) 2019-2023 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include <stdlib.h>
#include <string.h>
#include <string>

#include "cachinglayer/Metrics.h"
#include "common/FastMem.h"
#include "common/init_c.h"
#include "common/PrometheusClient.h"
#include "monitor_c.h"

namespace {

const char*
CacheShardDiskUsageDataTypeLabel(milvus::cachinglayer::CellDataType data_type) {
    switch (data_type) {
        case milvus::cachinglayer::CellDataType::VECTOR_FIELD:
            return "vector_field";
        case milvus::cachinglayer::CellDataType::VECTOR_INDEX:
            return "vector_index";
        case milvus::cachinglayer::CellDataType::SCALAR_FIELD:
            return "scalar_field";
        case milvus::cachinglayer::CellDataType::SCALAR_INDEX:
            return "scalar_index";
        case milvus::cachinglayer::CellDataType::OTHER:
            return "other";
    }
    return "unknown";
}

char*
CopyCString(const char* value) {
    auto len = strlen(value);
    char* res = static_cast<char*>(malloc(len + 1));
    if (res == nullptr) {
        return nullptr;
    }
    milvus::fastmem::FastMemcpy(res, value, len);
    res[len] = '\0';
    return res;
}

char*
CopyCString(const std::string& value) {
    char* res = static_cast<char*>(malloc(value.length() + 1));
    if (res == nullptr) {
        return nullptr;
    }
    milvus::fastmem::FastMemcpy(res, value.data(), value.length());
    res[value.length()] = '\0';
    return res;
}

}  // namespace

char*
GetCoreMetrics() {
    UpdateArrowIOThreadPoolMetrics();
    static_cast<void>(
        milvus::cachinglayer::monitor::collect_cache_shard_disk_usage_stats());
    auto str = milvus::monitor::getPrometheusClient().GetMetrics();
    auto len = str.length();
    char* res = static_cast<char*>(malloc(len + 1));
    milvus::fastmem::FastMemcpy(res, str.data(), len);
    res[len] = '\0';
    return res;
}

CCacheShardDiskUsageStatsArray
GetCacheShardDiskUsageStats() {
    auto stats =
        milvus::cachinglayer::monitor::collect_cache_shard_disk_usage_stats();

    CCacheShardDiskUsageStatsArray result{nullptr, 0};
    if (stats.empty()) {
        return result;
    }

    result.stats = static_cast<CCacheShardDiskUsageStats*>(
        calloc(stats.size(), sizeof(CCacheShardDiskUsageStats)));
    if (result.stats == nullptr) {
        return result;
    }
    result.len = static_cast<int64_t>(stats.size());

    for (size_t i = 0; i < stats.size(); ++i) {
        result.stats[i].data_type = CopyCString(
            CacheShardDiskUsageDataTypeLabel(stats[i].cell_data_type));
        result.stats[i].shard = CopyCString(stats[i].shard);
        result.stats[i].disk_bytes = stats[i].disk_bytes;
        if (result.stats[i].data_type == nullptr ||
            result.stats[i].shard == nullptr) {
            DeleteCacheShardDiskUsageStats(result);
            return CCacheShardDiskUsageStatsArray{nullptr, 0};
        }
    }
    return result;
}

void
DeleteCacheShardDiskUsageStats(CCacheShardDiskUsageStatsArray stats) {
    if (stats.stats == nullptr) {
        return;
    }
    for (int64_t i = 0; i < stats.len; ++i) {
        free(stats.stats[i].data_type);
        free(stats.stats[i].shard);
    }
    free(stats.stats);
}
