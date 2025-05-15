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
#include <vector>
#include <utility>

#include "cachinglayer/Utils.h"
#include "common/type_c.h"

namespace milvus::cachinglayer {

struct Meta {
    // This storage type is currently used only by metrics to distinguish the slot type.
    // In actual resource reservation, we use the actual size of the cell to determine the type.
    StorageType storage_type;
    CacheWarmupPolicy cache_warmup_policy;
    bool support_eviction;
    explicit Meta(StorageType storage_type,
                  CacheWarmupPolicy cache_warmup_policy,
                  bool support_eviction)
        : storage_type(storage_type),
          cache_warmup_policy(cache_warmup_policy),
          support_eviction(support_eviction) {
    }
};

template <typename CellT>
class Translator {
 public:
    using value_type = CellT;

    virtual size_t
    num_cells() const = 0;
    virtual cid_t
    cell_id_of(uid_t uid) const = 0;
    // For resource reservation when a cell is about to be loaded.
    // If a cell is about to be pinned and loaded, and there are not enough resource for it, EvictionManager
    // will try to evict some other cells to make space. Thus this estimation should generally be greater
    // than or equal to the actual size. If the estimation is smaller than the actual size, with insufficient
    // resource reserved, the load may fail.
    virtual ResourceUsage
    estimated_byte_size_of_cell(cid_t cid) const = 0;
    // must be unique to identify a CacheSlot.
    virtual const std::string&
    key() const = 0;

    virtual Meta*
    meta() = 0;

    // Translator may choose to fetch more than requested cells.
    // TODO(tiered storage 2): This has a problem: when loading, the resource manager will only reserve the size of the
    // requested cells, How can translator be sure the extra cells can fit? Currently if bonus cells are returned,
    // used memory in cache may exceed the limit. Maybe try to reserve memory for bonus cells, and drop cell if failed.
    virtual std::vector<std::pair<cid_t, std::unique_ptr<CellT>>>
    get_cells(const std::vector<cid_t>& cids) = 0;
    virtual ~Translator() = default;
};

}  // namespace milvus::cachinglayer
