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
#include <mutex>
#include <map>
#include <shared_mutex>
#include <utility>
#include <memory>
#include <tbb/concurrent_hash_map.h>
#include "utils/EasyAssert.h"
#include "knowhere/index/vector_index/VecIndex.h"
#include "common/Types.h"

namespace milvus::segcore {

struct SealedIndexingEntry {
    MetricType metric_type_;
    knowhere::VecIndexPtr indexing_;
};

using SealedIndexingEntryPtr = std::unique_ptr<SealedIndexingEntry>;

struct SealedIndexingRecord {
    void
    add_entry(int64_t field_offset, SealedIndexingEntryPtr&& ptr) {
        std::unique_lock lck(mutex_);
        entries_[field_offset] = std::move(ptr);
    }

    const SealedIndexingEntry*
    get_entry(int64_t field_offset) const {
        std::shared_lock lck(mutex_);
        AssertInfo(entries_.count(field_offset), "field_offset not found");
        return entries_.at(field_offset).get();
    }

    bool
    test_readiness(int64_t field_offset) const {
        std::shared_lock lck(mutex_);
        return entries_.count(field_offset);
    }

 private:
    // field_offset -> SealedIndexingEntry
    std::map<int64_t, SealedIndexingEntryPtr> entries_;
    mutable std::shared_mutex mutex_;
};
}  // namespace milvus::segcore
