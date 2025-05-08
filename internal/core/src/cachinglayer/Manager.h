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
#include "cachinglayer/Translator.h"
#include "cachinglayer/lrucache/DList.h"

namespace milvus::cachinglayer {

class Manager {
 public:
    static Manager&
    GetInstance();

    // This function is not thread safe, must be called before any CacheSlot is created.
    // TODO(tiered storage 4): support dynamic update.
    static bool
    ConfigureTieredStorage(bool enabled_globally,
                           int64_t memory_limit_bytes,
                           int64_t disk_limit_bytes);

    Manager(const Manager&) = delete;
    Manager&
    operator=(const Manager&) = delete;
    Manager(Manager&&) = delete;
    Manager&
    operator=(Manager&&) = delete;

    ~Manager() = default;

    template <typename CellT>
    std::shared_ptr<CacheSlot<CellT>>
    CreateCacheSlot(std::unique_ptr<Translator<CellT>> translator) {
        return std::make_shared<CacheSlot<CellT>>(std::move(translator),
                                                  dlist_.get());
    }

    // memory overhead for managing all cache slots/cells/translators/policies.
    size_t
    memory_overhead() const;

 private:
    friend void
    ConfigureTieredStorage(bool enabled_globally,
                           int64_t memory_limit_bytes,
                           int64_t disk_limit_bytes);

    Manager() = default;  // Private constructor

    std::unique_ptr<internal::DList> dlist_{nullptr};
    bool enable_global_tiered_storage_{false};
};  // class Manager

}  // namespace milvus::cachinglayer
