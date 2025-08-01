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

#include <memory>

#include "cachinglayer/CacheSlot.h"
#include "cachinglayer/lrucache/DList.h"
#include "cachinglayer/Translator.h"
#include "cachinglayer/Utils.h"
#include "common/type_c.h"

namespace milvus::cachinglayer {

class Manager {
 public:
    static Manager&
    GetInstance();

    // This function is not thread safe, must be called exactly once before any CacheSlot is created,
    // and before any Manager instance method is called.
    // TODO(tiered storage 4): support dynamic update.
    static void
    ConfigureTieredStorage(CacheWarmupPolicies warmup_policies,
                           CacheLimit cache_limit,
                           bool evictionEnabled,
                           EvictionConfig eviction_config);

    Manager(const Manager&) = delete;
    Manager&
    operator=(const Manager&) = delete;
    Manager(Manager&&) = delete;
    Manager&
    operator=(Manager&&) = delete;

    template <typename CellT>
    std::shared_ptr<CacheSlot<CellT>>
    CreateCacheSlot(std::unique_ptr<Translator<CellT>> translator) {
        auto evictable =
            translator->meta()->support_eviction && evictionEnabled_;
        // NOTE: when evictionEnabled_ is false, dlist_ is nullptr.
        auto cache_slot = std::make_shared<CacheSlot<CellT>>(
            std::move(translator), dlist_.get(), evictable);
        cache_slot->Warmup();
        return cache_slot;
    }

    // memory overhead for managing all cache slots/cells/translators/policies.
    size_t
    memory_overhead() const;

    CacheWarmupPolicy
    getScalarFieldCacheWarmupPolicy() const {
        return warmup_policies_.scalarFieldCacheWarmupPolicy;
    }

    CacheWarmupPolicy
    getVectorFieldCacheWarmupPolicy() const {
        return warmup_policies_.vectorFieldCacheWarmupPolicy;
    }

    CacheWarmupPolicy
    getScalarIndexCacheWarmupPolicy() const {
        return warmup_policies_.scalarIndexCacheWarmupPolicy;
    }

    CacheWarmupPolicy
    getVectorIndexCacheWarmupPolicy() const {
        return warmup_policies_.vectorIndexCacheWarmupPolicy;
    }

    bool
    isEvictionEnabled() const {
        return evictionEnabled_;
    }

 private:
    friend void
    ConfigureTieredStorage(CacheWarmupPolicies warmup_policies,
                           CacheLimit cache_limit,
                           bool evictionEnabled,
                           EvictionConfig eviction_config);

    Manager() = default;

    std::unique_ptr<internal::DList> dlist_{nullptr};
    CacheWarmupPolicies warmup_policies_{};
    bool evictionEnabled_{false};
};  // class Manager

}  // namespace milvus::cachinglayer
