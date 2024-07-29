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

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <tuple>
#include <utility>
#include <vector>
#include <folly/ConcurrentSkipList.h>

#include "AckResponder.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "segcore/Record.h"
#include "segcore/InsertRecord.h"
#include "ConcurrentVector.h"

namespace milvus::segcore {

struct Comparator {
    bool
    operator()(const std::pair<Timestamp, std::set<int64_t>>& left,
               const std::pair<Timestamp, std::set<int64_t>>& right) const {
        return left.first < right.first;
    }
};

using TSkipList =
    folly::ConcurrentSkipList<std::pair<Timestamp, std::set<int64_t>>,
                              Comparator>;

template <bool is_sealed = false>
class DeletedRecord {
 public:
    DeletedRecord(InsertRecord<is_sealed>* insert_record)
        : insert_record_(insert_record),
          deleted_pairs_(TSkipList::createInstance()) {
    }

    DeletedRecord(DeletedRecord<is_sealed>&& delete_record) = delete;
    DeletedRecord<is_sealed>&
    operator=(DeletedRecord<is_sealed>&& delete_record) = delete;

    void
    Push(const std::vector<PkType>& pks, const Timestamp* timestamps) {
        std::unique_lock<std::shared_mutex> lck(mutex_);
        int64_t removed_num = 0;
        int64_t mem_add = 0;
        for (size_t i = 0; i < pks.size(); ++i) {
            auto offsets = insert_record_->search_pk(pks[i], timestamps[i]);
            for (auto offset : offsets) {
                int64_t insert_row_offset = offset.get();
                // Assert(insert_record->timestamps_.size() >= insert_row_offset);
                if (insert_record_->timestamps_[insert_row_offset] <
                    timestamps[i]) {
                    InsertIntoInnerPairs(timestamps[i], {insert_row_offset});
                    removed_num++;
                    mem_add += sizeof(Timestamp) + sizeof(int64_t);
                }
            }
        }
        n_.fetch_add(removed_num);
        mem_size_.fetch_add(mem_add);
    }

    void
    Query(BitsetType& bitset, int64_t insert_barrier, Timestamp timestamp) {
        Assert(bitset.size() == insert_barrier);
        // TODO: add cache to bitset
        if (deleted_pairs_.size() == 0) {
            return;
        }
        auto end = deleted_pairs_.lower_bound(
            std::make_pair(timestamp, std::set<int64_t>{}));
        for (auto it = deleted_pairs_.begin(); it != end; it++) {
            for (auto& v : it->second) {
                bitset.set(v);
            }
        }

        // handle the case where end points to an element with the same timestamp
        if (end != deleted_pairs_.end() && end->first == timestamp) {
            for (auto& v : end->second) {
                bitset.set(v);
            }
        }
    }

    int64_t
    size() const {
        return n_.load();
    }

    size_t
    mem_size() const {
        return mem_size_.load();
    }

 private:
    void
    InsertIntoInnerPairs(Timestamp ts, std::set<int64_t> offsets) {
        auto it = deleted_pairs_.find(std::make_pair(ts, std::set<int64_t>{}));
        if (it == deleted_pairs_.end()) {
            deleted_pairs_.insert(std::make_pair(ts, offsets));
        } else {
            for (auto& val : offsets) {
                it->second.insert(val);
            }
        }
    }

 private:
    std::shared_mutex mutex_;
    std::atomic<int64_t> n_ = 0;
    std::atomic<int64_t> mem_size_ = 0;
    InsertRecord<is_sealed>* insert_record_;
    TSkipList::Accessor deleted_pairs_;
};

}  // namespace milvus::segcore
