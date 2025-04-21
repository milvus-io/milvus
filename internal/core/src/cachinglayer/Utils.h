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
    GEZero() const {
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
        return fmt::format("ResourceUsage{memory_bytes={}, file_bytes={}}",
                           memory_bytes,
                           file_bytes);
    }
};

inline std::ostream&
operator<<(std::ostream& os, const ResourceUsage& usage) {
    os << "ResourceUsage{memory_bytes=" << usage.memory_bytes
       << ", file_bytes=" << usage.file_bytes << "}";
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
            PanicInfo(ErrorCode::UnexpectedError, "Unknown StorageType");
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
            PanicInfo(ErrorCode::UnexpectedError, "Unknown StorageType");
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
            PanicInfo(ErrorCode::UnexpectedError, "Unknown StorageType");
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
            PanicInfo(ErrorCode::UnexpectedError, "Unknown StorageType");
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
            PanicInfo(ErrorCode::UnexpectedError, "Unknown StorageType");
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
            PanicInfo(ErrorCode::UnexpectedError, "Unknown StorageType");
    }
}

inline prometheus::Counter&
cache_eviction_count(StorageType storage_type) {
    switch (storage_type) {
        case StorageType::MEMORY:
            return monitor::internal_cache_eviction_count_memory;
        case StorageType::DISK:
            return monitor::internal_cache_eviction_count_disk;
        case StorageType::MIXED:
            return monitor::internal_cache_eviction_count_mixed;
        default:
            PanicInfo(ErrorCode::UnexpectedError, "Unknown StorageType");
    }
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
            PanicInfo(ErrorCode::UnexpectedError, "Unknown StorageType");
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
            PanicInfo(ErrorCode::UnexpectedError, "Unknown StorageType");
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
            PanicInfo(ErrorCode::UnexpectedError, "Unknown StorageType");
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
            PanicInfo(ErrorCode::UnexpectedError, "Unknown StorageType");
    }
}

}  // namespace internal

}  // namespace milvus::cachinglayer
