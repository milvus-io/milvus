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

#include "storage/loon_ffi/property_singleton.h"

#include <compare>
#include <mutex>
#include <optional>
#include <string>
#include <utility>

#include "milvus-storage/common/lrucache.h"
#include "storage/loon_ffi/util.h"

namespace milvus::storage {

struct LoonFFIPropertiesSingleton::PropertiesCache {
    // The key describes the values written to properties, not the request mask.
    // Extend it with the effective values of any future PropertiesOptions fields.
    struct ResolvedOptions {
        uint32_t talon_mode;
        std::string external_talon_mode;
        bool talon_open_only;

        auto
        operator<=>(const ResolvedOptions&) const = default;
    };

    static std::optional<ResolvedOptions>
    Resolve(const milvus_storage::api::Properties& base,
            const TalonReadConfig& talon,
            const PropertiesOptions& options) {
        if (talon.mode == 0 ||
            !TalonStageEnabled(talon.enabled_stages, options.talon_stage)) {
            return std::nullopt;
        }
        // These types and keys are established by MakeInternalPropertiesFromStorageConfig.
        // Keep the Milvus-only external mode in its stored string representation.
        const ResolvedOptions base_options{
            std::get<uint32_t>(base.at(PROPERTY_FS_TALON_MODE)),
            std::get<std::string>(base.at("milvus.talon.external_mode")),
            std::get<bool>(base.at(PROPERTY_FS_TALON_OPEN_ONLY))};
        auto resolved = base_options;
        const auto storage_type = base.find(PROPERTY_FS_STORAGE_TYPE);
        const bool is_local =
            storage_type != base.end() &&
            storage_type->second ==
                milvus_storage::api::PropertyVariant{std::string("local")};
        if (!is_local) {
            resolved.talon_mode = talon.mode;
        }
        if (talon.enable_for_external_table) {
            resolved.external_talon_mode = std::to_string(talon.mode);
        }
        if (talon.enabled_stages != 0 &&
            talon.enabled_stages != kTalonStagesAll &&
            TalonStageEnabled(talon.enabled_stages,
                              options.talon_stage & kTalonStageLoadOpen)) {
            resolved.talon_open_only = true;
        }
        if (resolved == base_options) {
            return std::nullopt;
        }
        return resolved;
    }

    milvus_storage::LRUCache<
        ResolvedOptions,
        std::shared_ptr<const milvus_storage::api::Properties>>
        entries{128};
};

LoonFFIPropertiesSingleton::LoonFFIPropertiesSingleton()
    : properties_cache_(std::make_unique<PropertiesCache>()) {
}

LoonFFIPropertiesSingleton::~LoonFFIPropertiesSingleton() = default;

LoonFFIPropertiesSingleton&
LoonFFIPropertiesSingleton::GetInstance() {
    static LoonFFIPropertiesSingleton instance;
    return instance;
}

void
LoonFFIPropertiesSingleton::Init(CStorageConfig c_storage_config) {
    std::unique_lock lck(mutex_);

    if (properties_ == nullptr) {
        if (c_storage_config.crt_queue_sizes > 0) {
            std::unique_lock cfg_lck(config_mutex_);
            s3_crt_queue_sizes_ = c_storage_config.crt_queue_sizes;
        }
        auto properties =
            MakeInternalPropertiesFromStorageConfig(c_storage_config);
        talon_config_ = {c_storage_config.talon_mode,
                         c_storage_config.talon_enabled_stages,
                         c_storage_config.talon_enable_for_external_table};
        ApplyArrowReaderConfig(*properties);
        properties_ = std::move(properties);
    }
}

void
LoonFFIPropertiesSingleton::SetArrowReaderConfig(
    int64_t hole_size_limit_bytes, int64_t range_size_limit_bytes) {
    // Publish both limits as one snapshot. They are validated against
    // each other downstream (the parquet reader rejects
    // range_size_limit <= hole_size_limit), so a reader must never
    // observe one value from the new generation and the other from the
    // old one — e.g. updating 1MiB/4MiB to 8MiB/16MiB could otherwise
    // transiently expose 8MiB/4MiB and fail the build task.
    {
        std::unique_lock cfg_lck(config_mutex_);
        arrow_reader_limits_ = {hole_size_limit_bytes, range_size_limit_bytes};
    }
    std::unique_lock lck(mutex_);
    if (properties_ != nullptr) {
        auto properties =
            std::make_shared<milvus_storage::api::Properties>(*properties_);
        ApplyArrowReaderConfig(*properties);
        properties_ = std::move(properties);
        properties_cache_->entries.clean();
    }
}

std::shared_ptr<const milvus_storage::api::Properties>
LoonFFIPropertiesSingleton::GetProperties() const {
    std::shared_lock lck(mutex_);
    return properties_;
}

// The cache is scoped to the current base snapshot. Resolve request options
// before lookup so equivalent stages share the same effective configuration.
std::shared_ptr<const milvus_storage::api::Properties>
LoonFFIPropertiesSingleton::GetProperties(
    const PropertiesOptions& options) const {
    {
        std::shared_lock lck(mutex_);
        const auto key =
            PropertiesCache::Resolve(*properties_, talon_config_, options);
        if (!key) {
            return properties_;
        }
        if (const auto cached = properties_cache_->entries.get(*key)) {
            return *cached;
        }
    }
    // Re-resolve after acquiring the write lock: a base-config update may have
    // invalidated the cache while the lock was released. Serialize misses so
    // concurrent equivalent requests publish one snapshot.
    std::unique_lock lck(mutex_);
    const auto key =
        PropertiesCache::Resolve(*properties_, talon_config_, options);
    if (!key) {
        return properties_;
    }
    if (const auto cached = properties_cache_->entries.get(*key)) {
        return *cached;
    }
    auto properties =
        std::make_shared<milvus_storage::api::Properties>(*properties_);
    (*properties)[PROPERTY_FS_TALON_MODE] = key->talon_mode;
    (*properties)["milvus.talon.external_mode"] = key->external_talon_mode;
    (*properties)[PROPERTY_FS_TALON_OPEN_ONLY] = key->talon_open_only;
    properties_cache_->entries.put(*key, properties);
    return properties;
}

void
LoonFFIPropertiesSingleton::SetS3CrtQueueSizes(uint32_t crt_queue_sizes) {
    std::unique_lock cfg_lck(config_mutex_);
    s3_crt_queue_sizes_ = crt_queue_sizes;
}

uint32_t
LoonFFIPropertiesSingleton::GetS3CrtQueueSizes() const {
    std::shared_lock cfg_lck(config_mutex_);
    return s3_crt_queue_sizes_;
}

void
LoonFFIPropertiesSingleton::SetExternalIopsConfig(uint32_t initial_rate,
                                                  uint32_t max_rate) {
    std::unique_lock cfg_lck(config_mutex_);
    external_iops_config_ = {initial_rate, max_rate};
}

ExternalIopsConfig
LoonFFIPropertiesSingleton::GetExternalIopsConfig() const {
    std::shared_lock cfg_lck(config_mutex_);
    return external_iops_config_;
}

void
LoonFFIPropertiesSingleton::SetIndexBuildReadWindow(int64_t window_bytes) {
    index_build_read_window_bytes_.store(window_bytes,
                                         std::memory_order_relaxed);
}

int64_t
LoonFFIPropertiesSingleton::GetIndexBuildReadWindow() const {
    return index_build_read_window_bytes_.load(std::memory_order_relaxed);
}

void
LoonFFIPropertiesSingleton::ApplyIndexBuildReadWindow(
    milvus_storage::api::Properties& properties) const {
    auto window =
        index_build_read_window_bytes_.load(std::memory_order_relaxed);
    if (window <= 0) {
        return;
    }
    milvus_storage::api::SetValue(properties,
                                  PROPERTY_READER_RECORD_BATCH_MAX_SIZE,
                                  std::to_string(window).c_str());
}

ArrowReaderLimits
LoonFFIPropertiesSingleton::GetArrowReaderLimits() const {
    std::shared_lock cfg_lck(config_mutex_);
    return arrow_reader_limits_;
}

void
LoonFFIPropertiesSingleton::ApplyArrowReaderConfig(
    milvus_storage::api::Properties& properties) const {
    auto limits = GetArrowReaderLimits();
    milvus_storage::api::SetValue(
        properties,
        PROPERTY_READER_PARQUET_PREBUFFER_HOLE_SIZE_LIMIT,
        std::to_string(limits.hole_size_limit_bytes).c_str());
    milvus_storage::api::SetValue(
        properties,
        PROPERTY_READER_PARQUET_PREBUFFER_RANGE_SIZE_LIMIT,
        std::to_string(limits.range_size_limit_bytes).c_str());
}

}  // namespace milvus::storage
