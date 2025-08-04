// Copyright (C) 2019-2025 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License
#include "cachinglayer/Manager.h"

#include <memory>

#include "cachinglayer/Utils.h"
#include "log/Log.h"

namespace milvus::cachinglayer {

Manager&
Manager::GetInstance() {
    static Manager instance;
    return instance;
}

void
Manager::ConfigureTieredStorage(CacheWarmupPolicies warmup_policies,
                                CacheLimit cache_limit,
                                bool evictionEnabled,
                                EvictionConfig eviction_config) {
    static std::once_flag once;
    std::call_once(once, [&]() {
        Manager& manager = GetInstance();
        manager.warmup_policies_ = warmup_policies;
        manager.evictionEnabled_ = evictionEnabled;

        auto policy_str = warmup_policies.ToString();

        if (!evictionEnabled) {
            LOG_INFO(
                "[MCL] Tiered Storage manager is configured "
                "with disabled eviction and warmup policies: {}",
                policy_str);
            return;
        }

        ResourceUsage max{cache_limit.memory_max_bytes,
                          cache_limit.disk_max_bytes};
        ResourceUsage low_watermark{cache_limit.memory_low_watermark_bytes,
                                    cache_limit.disk_low_watermark_bytes};
        ResourceUsage high_watermark{cache_limit.memory_high_watermark_bytes,
                                     cache_limit.disk_high_watermark_bytes};

        manager.dlist_ = std::make_unique<internal::DList>(
            max, low_watermark, high_watermark, eviction_config);

        LOG_INFO(
            "[MCL] Configured Tiered Storage manager with "
            "memory watermark: low {}, high {}, max {}, "
            "disk watermark: low {}, high {}, max {}, "
            "cache touch window: {} ms, eviction interval: {} ms, "
            "physical memory max ratio: {}, max disk usage percentage: {}, "
            "loading memory factor: {}, cache cell unaccessed survival time: "
            "{} ms, warmup policies: {}",
            FormatBytes(low_watermark.memory_bytes),
            FormatBytes(high_watermark.memory_bytes),
            FormatBytes(max.memory_bytes),
            FormatBytes(low_watermark.file_bytes),
            FormatBytes(high_watermark.file_bytes),
            FormatBytes(max.file_bytes),
            eviction_config.cache_touch_window.count(),
            eviction_config.eviction_interval.count(),
            eviction_config.overloaded_memory_threshold_percentage,
            eviction_config.max_disk_usage_percentage,
            eviction_config.loading_memory_factor,
            eviction_config.cache_cell_unaccessed_survival_time.count(),
            policy_str);
    });
}

size_t
Manager::memory_overhead() const {
    // TODO(tiered storage 2): calculate memory overhead
    return 0;
}

}  // namespace milvus::cachinglayer
