// Copyright (C) 2019-2020 Zilliz. All rights reserved.
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

#include <map>
#include <unordered_map>
#include <memory>
#include <shared_mutex>
#include <utility>
#include <tbb/concurrent_hash_map.h>

#include "common/Types.h"
#include "exceptions/EasyAssert.h"
#include "knowhere/index/VecIndex.h"

namespace milvus::segcore {

struct SealedIndexingEntry {
    knowhere::MetricType metric_type_;
    knowhere::VecIndexPtr indexing_;
};

using SealedIndexingEntryPtr = std::unique_ptr<SealedIndexingEntry>;

struct SealedIndexingRecord {
    void
    append_field_indexing(FieldId field_id, const knowhere::MetricType& metric_type, knowhere::VecIndexPtr indexing) {
        auto ptr = std::make_unique<SealedIndexingEntry>();
        ptr->indexing_ = indexing;
        ptr->metric_type_ = metric_type;
        std::unique_lock lck(mutex_);
        field_indexings_[field_id] = std::move(ptr);
    }

    const SealedIndexingEntry*
    get_field_indexing(FieldId field_id) const {
        std::shared_lock lck(mutex_);
        AssertInfo(field_indexings_.count(field_id), "field_id not found");
        return field_indexings_.at(field_id).get();
    }

    void
    drop_field_indexing(FieldId field_id) {
        std::unique_lock lck(mutex_);
        field_indexings_.erase(field_id);
    }

    bool
    is_ready(FieldId field_id) const {
        std::shared_lock lck(mutex_);
        return field_indexings_.count(field_id);
    }

 private:
    // field_offset -> SealedIndexingEntry
    std::unordered_map<FieldId, SealedIndexingEntryPtr> field_indexings_;
    mutable std::shared_mutex mutex_;
};

}  // namespace milvus::segcore
