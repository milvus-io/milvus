// Copyright 2023 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <shared_mutex>

#include "common/type_c.h"
#include "milvus-storage/properties.h"
#include "storage/Types.h"

namespace milvus::storage {

// Arrow parquet prebuffer range-coalescing limits. Published and consumed as
// one unit: the parquet reader validates range_size_limit > hole_size_limit,
// so a mix of values from two generations is rejected downstream.
struct ArrowReaderLimits {
    int64_t hole_size_limit_bytes = 0;
    int64_t range_size_limit_bytes = 0;
};

// Process-local policy applied only when an External Table filesystem is
// materialized. These defaults must stay aligned with milvus-storage.
struct ExternalIopsConfig {
    uint32_t initial_rate = 2000;
    uint32_t max_rate = 5000;
};

struct PropertiesOptions {
    // Requested Talon stage bits; zero leaves the shared properties unchanged.
    uint32_t talon_stage = 0;
};

class LoonFFIPropertiesSingleton {
 private:
    friend class StorageTalonConfig_PropertiesFollowStorageConfig_Test;

    LoonFFIPropertiesSingleton();

 public:
    ~LoonFFIPropertiesSingleton();

    static LoonFFIPropertiesSingleton&
    GetInstance();

    void
    Init(CStorageConfig c_storage_config);

    void
    SetArrowReaderConfig(int64_t hole_size_limit_bytes,
                         int64_t range_size_limit_bytes);

    // Ordinary readers share the immutable configuration snapshot (nullptr
    // before Init). Callers that need to modify it must make a private copy.
    std::shared_ptr<const milvus_storage::api::Properties>
    GetProperties() const;

    // Explicit options require Init. Equivalent effective settings share an
    // immutable snapshot from a 128-entry LRU; unchanged settings reuse the base.
    // Base-config updates invalidate the cache; existing readers retain their
    // snapshots. Copy before injecting caller-specific values.
    // LoadOpen selects the open-only file policy; Manifest selects Talon for the
    // whole manifest. The stage mask is consumed here, not by storage.
    std::shared_ptr<const milvus_storage::api::Properties>
    GetProperties(const PropertiesOptions& options) const;

    void
    SetS3CrtQueueSizes(uint32_t crt_queue_sizes);

    // Job messages do not carry node-local CRT resource tuning.
    uint32_t
    GetS3CrtQueueSizes() const;

    void
    SetExternalIopsConfig(uint32_t initial_rate, uint32_t max_rate);

    ExternalIopsConfig
    GetExternalIopsConfig() const;

    // Sets the per-round read window (loon's reader.record_batch_max_size)
    // applied to index-build manifest reads. 0 keeps loon's built-in default
    // (32MB, which admits only one 64MB-class row group per prefetch round
    // and therefore serializes the raw-data download to one S3 GET at a
    // time). Applied per task via ApplyIndexBuildReadWindow — deliberately
    // NOT part of ApplyArrowReaderConfig so query-node load paths keep
    // their small default window.
    void
    SetIndexBuildReadWindow(int64_t window_bytes);

    int64_t
    GetIndexBuildReadWindow() const;

    // Stamps the index-build read window into `properties` when configured
    // (> 0). Lock-free; no-op when unset so loon's default applies.
    void
    ApplyIndexBuildReadWindow(
        milvus_storage::api::Properties& properties) const;

    // Returns the currently published limits as one coherent snapshot.
    ArrowReaderLimits
    GetArrowReaderLimits() const;

    // Applies the configured arrow reader prebuffer limits to `properties`.
    // Reads one coherent snapshot, so the two values always come from the
    // same generation. Guarded by config_mutex_ rather than mutex_, so it
    // stays callable from MakeInternalPropertiesFromStorageConfig while
    // Init holds mutex_ (lock order is always mutex_ -> config_mutex_;
    // SetArrowReaderConfig releases config_mutex_ before taking mutex_).
    // A value of 0 means "keep the arrow default" downstream.
    void
    ApplyArrowReaderConfig(milvus_storage::api::Properties& properties) const;

 private:
    mutable std::shared_mutex mutex_;
    std::shared_ptr<const milvus_storage::api::Properties> properties_ =
        nullptr;
    struct PropertiesCache;
    std::unique_ptr<PropertiesCache> properties_cache_;
    TalonReadConfig talon_config_{};
    // Guards configuration snapshots that do not belong to the internal
    // filesystem properties. Separate from mutex_ so the limits can be read
    // while mutex_ is held; see ApplyArrowReaderConfig.
    mutable std::shared_mutex config_mutex_;
    ArrowReaderLimits arrow_reader_limits_{};
    uint32_t s3_crt_queue_sizes_ = 125;
    ExternalIopsConfig external_iops_config_{};
    std::atomic<int64_t> index_build_read_window_bytes_{0};
};

}  // namespace milvus::storage
