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

#pragma once

#include <atomic>
#include <cstdint>

#include "common/EasyAssert.h"
#include "folly/executors/InlineExecutor.h"
#include <folly/futures/Future.h>
#include <prometheus/counter.h>
#include <prometheus/gauge.h>
#include <prometheus/histogram.h>

#include "monitor/prometheus_client.h"

namespace milvus::cachinglayer {

using uid_t = int64_t;
using cid_t = int64_t;

enum class StorageType {
    MEMORY,
    DISK,
    MIXED,
};

// TODO(tiered storage 4): this is a temporary function to get the result of a future
// by running it on the inline executor. We don't need this once we are fully async.
template <typename T>
T
SemiInlineGet(folly::SemiFuture<T>&& future) {
    return std::move(future).via(&folly::InlineExecutor::instance()).get();
}

inline std::string
FormatBytes(int64_t bytes) {
    if (bytes < 1024) {
        return fmt::format("{} B", bytes);
    } else if (bytes < 1024 * 1024) {
        return fmt::format("{:.2f} KB ({} B)", bytes / 1024.0, bytes);
    } else if (bytes < 1024 * 1024 * 1024) {
        return fmt::format(
            "{:.2f} MB ({} B)", bytes / (1024.0 * 1024.0), bytes);
    } else {
        return fmt::format(
            "{:.2f} GB ({} B)", bytes / (1024.0 * 1024.0 * 1024.0), bytes);
    }
}

struct ResourceUsage {
    int64_t memory_bytes{0};
    int64_t file_bytes{0};

    ResourceUsage() noexcept : memory_bytes(0), file_bytes(0) {
    }
    ResourceUsage(int64_t mem, int64_t file) noexcept
        : memory_bytes(mem), file_bytes(file) {
    }

    ResourceUsage
    operator+(const ResourceUsage& rhs) const {
        return ResourceUsage(memory_bytes + rhs.memory_bytes,
                             file_bytes + rhs.file_bytes);
    }

    void
    operator+=(const ResourceUsage& rhs) {
        memory_bytes += rhs.memory_bytes;
        file_bytes += rhs.file_bytes;
    }

    ResourceUsage
    operator-(const ResourceUsage& rhs) const {
        return ResourceUsage(memory_bytes - rhs.memory_bytes,
                             file_bytes - rhs.file_bytes);
    }

    ResourceUsage
    operator*(double factor) const {
        return ResourceUsage(
            static_cast<int64_t>(std::round(memory_bytes * factor)),
            static_cast<int64_t>(std::round(file_bytes * factor)));
    }

    friend ResourceUsage
    operator*(double factor, const ResourceUsage& usage) {
        return usage * factor;
    }

    void
    operator-=(const ResourceUsage& rhs) {
        memory_bytes -= rhs.memory_bytes;
        file_bytes -= rhs.file_bytes;
    }

    bool
    operator==(const ResourceUsage& rhs) const {
        return memory_bytes == rhs.memory_bytes && file_bytes == rhs.file_bytes;
    }

    bool
    operator!=(const ResourceUsage& rhs) const {
        return !(*this == rhs);
    }

    bool
    AnyGTZero() const {
        return memory_bytes > 0 || file_bytes > 0;
    }

    bool
    AllGEZero() const {
        return memory_bytes >= 0 && file_bytes >= 0;
    }

    bool
    CanHold(const ResourceUsage& rhs) const {
        return memory_bytes >= rhs.memory_bytes && file_bytes >= rhs.file_bytes;
    }

    StorageType
    storage_type() const {
        if (memory_bytes > 0 && file_bytes > 0) {
            return StorageType::MIXED;
        }
        return memory_bytes > 0 ? StorageType::MEMORY : StorageType::DISK;
    }

    std::string
    ToString() const {
        if (memory_bytes == 0 && file_bytes == 0) {
            return "EMPTY";
        }

        std::string result;
        if (memory_bytes > 0) {
            result += fmt::format("memory {}", FormatBytes(memory_bytes));
        }
        if (file_bytes > 0) {
            if (!result.empty()) {
                result += ", ";
            }
            result += fmt::format("disk {}", FormatBytes(file_bytes));
        }
        return result;
    }
};

inline std::ostream&
operator<<(std::ostream& os, const ResourceUsage& usage) {
    os << usage.ToString();
    return os;
}

inline void
operator+=(std::atomic<ResourceUsage>& atomic_lhs, const ResourceUsage& rhs) {
    ResourceUsage current = atomic_lhs.load();
    ResourceUsage new_value;
    do {
        new_value = current;
        new_value += rhs;
    } while (!atomic_lhs.compare_exchange_weak(current, new_value));
}

inline void
operator-=(std::atomic<ResourceUsage>& atomic_lhs, const ResourceUsage& rhs) {
    ResourceUsage current = atomic_lhs.load();
    ResourceUsage new_value;
    do {
        new_value = current;
        new_value -= rhs;
    } while (!atomic_lhs.compare_exchange_weak(current, new_value));
}

// helper struct for ConfigureTieredStorage, so the list of arguments is not too long.
struct CacheWarmupPolicies {
    CacheWarmupPolicy scalarFieldCacheWarmupPolicy;
    CacheWarmupPolicy vectorFieldCacheWarmupPolicy;
    CacheWarmupPolicy scalarIndexCacheWarmupPolicy;
    CacheWarmupPolicy vectorIndexCacheWarmupPolicy;

    CacheWarmupPolicies()
        : scalarFieldCacheWarmupPolicy(
              CacheWarmupPolicy::CacheWarmupPolicy_Sync),
          vectorFieldCacheWarmupPolicy(
              CacheWarmupPolicy::CacheWarmupPolicy_Disable),
          scalarIndexCacheWarmupPolicy(
              CacheWarmupPolicy::CacheWarmupPolicy_Sync),
          vectorIndexCacheWarmupPolicy(
              CacheWarmupPolicy::CacheWarmupPolicy_Sync) {
    }

    CacheWarmupPolicies(CacheWarmupPolicy scalarFieldCacheWarmupPolicy,
                        CacheWarmupPolicy vectorFieldCacheWarmupPolicy,
                        CacheWarmupPolicy scalarIndexCacheWarmupPolicy,
                        CacheWarmupPolicy vectorIndexCacheWarmupPolicy)
        : scalarFieldCacheWarmupPolicy(scalarFieldCacheWarmupPolicy),
          vectorFieldCacheWarmupPolicy(vectorFieldCacheWarmupPolicy),
          scalarIndexCacheWarmupPolicy(scalarIndexCacheWarmupPolicy),
          vectorIndexCacheWarmupPolicy(vectorIndexCacheWarmupPolicy) {
    }
};

struct CacheLimit {
    int64_t memory_low_watermark_bytes;
    int64_t memory_high_watermark_bytes;
    int64_t memory_max_bytes;
    int64_t disk_low_watermark_bytes;
    int64_t disk_high_watermark_bytes;
    int64_t disk_max_bytes;
    CacheLimit()
        : memory_low_watermark_bytes(0),
          memory_high_watermark_bytes(0),
          memory_max_bytes(0),
          disk_low_watermark_bytes(0),
          disk_high_watermark_bytes(0),
          disk_max_bytes(0) {
    }

    CacheLimit(int64_t memory_low_watermark_bytes,
               int64_t memory_high_watermark_bytes,
               int64_t memory_max_bytes,
               int64_t disk_low_watermark_bytes,
               int64_t disk_high_watermark_bytes,
               int64_t disk_max_bytes)
        : memory_low_watermark_bytes(memory_low_watermark_bytes),
          memory_high_watermark_bytes(memory_high_watermark_bytes),
          memory_max_bytes(memory_max_bytes),
          disk_low_watermark_bytes(disk_low_watermark_bytes),
          disk_high_watermark_bytes(disk_high_watermark_bytes),
          disk_max_bytes(disk_max_bytes) {
    }
};

struct EvictionConfig {
    // Touch a node means to move it to the head of the list, which requires locking the entire list.
    // Use cache_touch_window_ms to reduce the frequency of touching and reduce contention.
    std::chrono::milliseconds cache_touch_window;
    std::chrono::milliseconds eviction_interval;
    // Overloaded memory threshold percentage - limits cache memory usage to this percentage of total physical memory
    float overloaded_memory_threshold_percentage;
    // Max disk usage percentage - limits disk cache usage to this percentage of total disk space (not used yet)
    float max_disk_usage_percentage;
    // Loading memory factor for estimating memory during loading
    float loading_memory_factor;

    EvictionConfig()
        : cache_touch_window(std::chrono::milliseconds(0)),
          eviction_interval(std::chrono::milliseconds(0)),
          overloaded_memory_threshold_percentage(0.9),
          max_disk_usage_percentage(0.95),
          loading_memory_factor(2.5f) {
    }

    EvictionConfig(int64_t cache_touch_window_ms, int64_t eviction_interval_ms)
        : cache_touch_window(std::chrono::milliseconds(cache_touch_window_ms)),
          eviction_interval(std::chrono::milliseconds(eviction_interval_ms)),
          overloaded_memory_threshold_percentage(0.9),
          max_disk_usage_percentage(0.95),
          loading_memory_factor(2.5f) {
    }

    EvictionConfig(int64_t cache_touch_window_ms,
                   int64_t eviction_interval_ms,
                   float overloaded_memory_threshold_percentage,
                   float max_disk_usage_percentage,
                   float loading_memory_factor = 2.5f)
        : cache_touch_window(std::chrono::milliseconds(cache_touch_window_ms)),
          eviction_interval(std::chrono::milliseconds(eviction_interval_ms)),
          overloaded_memory_threshold_percentage(
              overloaded_memory_threshold_percentage),
          max_disk_usage_percentage(max_disk_usage_percentage),
          loading_memory_factor(loading_memory_factor) {
    }
};

namespace internal {

inline prometheus::Gauge&
cache_slot_count(StorageType storage_type) {
    switch (storage_type) {
        case StorageType::MEMORY:
            return monitor::internal_cache_slot_count_memory;
        case StorageType::DISK:
            return monitor::internal_cache_slot_count_disk;
        case StorageType::MIXED:
            return monitor::internal_cache_slot_count_mixed;
        default:
            ThrowInfo(ErrorCode::UnexpectedError, "Unknown StorageType");
    }
}

inline prometheus::Gauge&
cache_cell_count(StorageType storage_type) {
    switch (storage_type) {
        case StorageType::MEMORY:
            return monitor::internal_cache_cell_count_memory;
        case StorageType::DISK:
            return monitor::internal_cache_cell_count_disk;
        case StorageType::MIXED:
            return monitor::internal_cache_cell_count_mixed;
        default:
            ThrowInfo(ErrorCode::UnexpectedError, "Unknown StorageType");
    }
}

inline prometheus::Gauge&
cache_cell_loaded_count(StorageType storage_type) {
    switch (storage_type) {
        case StorageType::MEMORY:
            return monitor::internal_cache_cell_loaded_count_memory;
        case StorageType::DISK:
            return monitor::internal_cache_cell_loaded_count_disk;
        case StorageType::MIXED:
            return monitor::internal_cache_cell_loaded_count_mixed;
        default:
            ThrowInfo(ErrorCode::UnexpectedError, "Unknown StorageType");
    }
}

inline prometheus::Histogram&
cache_load_latency(StorageType storage_type) {
    switch (storage_type) {
        case StorageType::MEMORY:
            return monitor::internal_cache_load_latency_memory;
        case StorageType::DISK:
            return monitor::internal_cache_load_latency_disk;
        case StorageType::MIXED:
            return monitor::internal_cache_load_latency_mixed;
        default:
            ThrowInfo(ErrorCode::UnexpectedError, "Unknown StorageType");
    }
}

inline prometheus::Counter&
cache_op_result_count_hit(StorageType storage_type) {
    switch (storage_type) {
        case StorageType::MEMORY:
            return monitor::internal_cache_op_result_count_hit_memory;
        case StorageType::DISK:
            return monitor::internal_cache_op_result_count_hit_disk;
        case StorageType::MIXED:
            return monitor::internal_cache_op_result_count_hit_mixed;
        default:
            ThrowInfo(ErrorCode::UnexpectedError, "Unknown StorageType");
    }
}

inline prometheus::Counter&
cache_op_result_count_miss(StorageType storage_type) {
    switch (storage_type) {
        case StorageType::MEMORY:
            return monitor::internal_cache_op_result_count_miss_memory;
        case StorageType::DISK:
            return monitor::internal_cache_op_result_count_miss_disk;
        case StorageType::MIXED:
            return monitor::internal_cache_op_result_count_miss_mixed;
        default:
            ThrowInfo(ErrorCode::UnexpectedError, "Unknown StorageType");
    }
}

inline prometheus::Counter&
cache_cell_eviction_count(StorageType storage_type) {
    switch (storage_type) {
        case StorageType::MEMORY:
            return monitor::internal_cache_cell_eviction_count_memory;
        case StorageType::DISK:
            return monitor::internal_cache_cell_eviction_count_disk;
        case StorageType::MIXED:
            return monitor::internal_cache_cell_eviction_count_mixed;
        default:
            ThrowInfo(ErrorCode::UnexpectedError, "Unknown StorageType");
    }
}

inline prometheus::Counter&
cache_eviction_event_count() {
    return monitor::internal_cache_eviction_event_count_all;
}

inline prometheus::Histogram&
cache_item_lifetime_seconds(StorageType storage_type) {
    switch (storage_type) {
        case StorageType::MEMORY:
            return monitor::internal_cache_item_lifetime_seconds_memory;
        case StorageType::DISK:
            return monitor::internal_cache_item_lifetime_seconds_disk;
        case StorageType::MIXED:
            return monitor::internal_cache_item_lifetime_seconds_mixed;
        default:
            ThrowInfo(ErrorCode::UnexpectedError, "Unknown StorageType");
    }
}

inline prometheus::Counter&
cache_load_count_success(StorageType storage_type) {
    switch (storage_type) {
        case StorageType::MEMORY:
            return monitor::internal_cache_load_count_success_memory;
        case StorageType::DISK:
            return monitor::internal_cache_load_count_success_disk;
        case StorageType::MIXED:
            return monitor::internal_cache_load_count_success_mixed;
        default:
            ThrowInfo(ErrorCode::UnexpectedError, "Unknown StorageType");
    }
}

inline prometheus::Counter&
cache_load_count_fail(StorageType storage_type) {
    switch (storage_type) {
        case StorageType::MEMORY:
            return monitor::internal_cache_load_count_fail_memory;
        case StorageType::DISK:
            return monitor::internal_cache_load_count_fail_disk;
        case StorageType::MIXED:
            return monitor::internal_cache_load_count_fail_mixed;
        default:
            ThrowInfo(ErrorCode::UnexpectedError, "Unknown StorageType");
    }
}

inline prometheus::Gauge&
cache_memory_overhead_bytes(StorageType storage_type) {
    switch (storage_type) {
        case StorageType::MEMORY:
            return monitor::internal_cache_memory_overhead_bytes_memory;
        case StorageType::DISK:
            return monitor::internal_cache_memory_overhead_bytes_disk;
        case StorageType::MIXED:
            return monitor::internal_cache_memory_overhead_bytes_mixed;
        default:
            ThrowInfo(ErrorCode::UnexpectedError, "Unknown StorageType");
    }
}

struct SystemMemoryInfo {
    int64_t total_memory_bytes{0};
    int64_t available_memory_bytes{0};
    int64_t used_memory_bytes{0};
};

int64_t
getHostTotalMemory();
int64_t
getContainerMemLimit();

SystemMemoryInfo
getSystemMemoryInfo();
int64_t
getCurrentProcessMemoryUsage();

}  // namespace internal

}  // namespace milvus::cachinglayer
