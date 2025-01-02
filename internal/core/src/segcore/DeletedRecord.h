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
#include "segcore/SegmentInterface.h"
#include "ConcurrentVector.h"

namespace milvus::segcore {

using Offset = int32_t;

struct Comparator {
    bool
    operator()(const std::pair<Timestamp, Offset>& left,
               const std::pair<Timestamp, Offset>& right) const {
        if (left.first == right.first) {
            return left.second < right.second;
        }
        return left.first < right.first;
    }
};

// a lock-free list for multi-thread insert && read
using SortedDeleteList =
    folly::ConcurrentSkipList<std::pair<Timestamp, Offset>, Comparator>;

static int32_t DUMP_BATCH_SIZE = 100000;
static int32_t DELETE_PAIR_SIZE = sizeof(std::pair<Timestamp, Offset>);

template <bool is_sealed = false>
class DeletedRecord {
 public:
    DeletedRecord(InsertRecord<is_sealed>* insert_record,
                  SegmentInternalInterface* segment)
        : insert_record_(insert_record),
          segment_(segment),
          deleted_lists_(SortedDeleteList::createInstance()) {
    }

    // not binding segment, only for testing purposes
    DeletedRecord(InsertRecord<is_sealed>* insert_record)
        : insert_record_(insert_record),
          segment_(nullptr),
          deleted_lists_(SortedDeleteList::createInstance()) {
    }

    ~DeletedRecord() {
    }

    DeletedRecord(DeletedRecord<is_sealed>&& delete_record) = delete;

    DeletedRecord<is_sealed>&
    operator=(DeletedRecord<is_sealed>&& delete_record) = delete;

    void
    LoadPush(const std::vector<PkType>& pks, const Timestamp* timestamps) {
        if (pks.empty()) {
            return;
        }

        auto max_deleted_ts = InternalPush(pks, timestamps);

        if (max_deleted_ts > max_load_timestamp_) {
            max_load_timestamp_ = max_deleted_ts;
        }

        //TODO: add support for dump snapshot when load finished
    }

    // stream push delete timestamps should be sorted outside of the interface
    // considering concurrent query and push
    void
    StreamPush(const std::vector<PkType>& pks, const Timestamp* timestamps) {
        if (pks.empty()) {
            return;
        }

        InternalPush(pks, timestamps);

        bool can_dump = timestamps[0] >= max_load_timestamp_;
        if (can_dump) {
            DumpSnapshot();
        }
    }

    Timestamp
    InternalPush(const std::vector<PkType>& pks, const Timestamp* timestamps) {
        int64_t removed_num = 0;
        int64_t mem_add = 0;
        Timestamp max_timestamp = 0;

        SortedDeleteList::Accessor accessor(deleted_lists_);
        for (size_t i = 0; i < pks.size(); ++i) {
            auto deleted_pk = pks[i];
            auto deleted_ts = timestamps[i];
            if (deleted_ts > max_timestamp) {
                max_timestamp = deleted_ts;
            }
            std::vector<SegOffset> offsets;
            if (segment_) {
                offsets = segment_->search_pk(deleted_pk, deleted_ts);
            } else {
                // only for testing
                offsets = std::move(
                    insert_record_->search_pk(deleted_pk, deleted_ts));
            }
            for (auto& offset : offsets) {
                auto row_id = offset.get();
                // if already deleted, no need to add new record
                if (deleted_mask_.size() > row_id && deleted_mask_[row_id]) {
                    continue;
                }
                // if insert record and delete record is same timestamp,
                // delete not take effect on this record.
                if (deleted_ts == insert_record_->timestamps_[row_id]) {
                    continue;
                }
                accessor.insert(std::make_pair(deleted_ts, row_id));
                if constexpr (is_sealed) {
                    Assert(deleted_mask_.size() > 0);
                    deleted_mask_.set(row_id);
                } else {
                    // need to add mask size firstly for growing segment
                    deleted_mask_.resize(insert_record_->size());
                    deleted_mask_.set(row_id);
                }
                removed_num++;
                mem_add += DELETE_PAIR_SIZE;
            }
        }

        n_.fetch_add(removed_num);
        mem_size_.fetch_add(mem_add);
        return max_timestamp;
    }

    void
    Query(BitsetTypeView& bitset,
          int64_t insert_barrier,
          Timestamp query_timestamp) {
        Assert(bitset.size() == insert_barrier);

        SortedDeleteList::Accessor accessor(deleted_lists_);
        if (accessor.size() == 0) {
            return;
        }

        // try use snapshot to skip iterations
        bool hit_snapshot = false;
        SortedDeleteList::iterator next_iter;
        if (!snapshots_.empty()) {
            int loc = snapshots_.size() - 1;
            // find last meeted snapshot
            {
                std::shared_lock<std::shared_mutex> lock(snap_lock_);
                while (snapshots_[loc].first > query_timestamp && loc >= 0) {
                    loc--;
                }
                if (loc >= 0) {
                    next_iter = snap_next_iter_[loc];
                    auto or_size =
                        std::min(snapshots_[loc].second.size(), bitset.size());
                    bitset.inplace_or_with_count(snapshots_[loc].second,
                                                 or_size);
                    hit_snapshot = true;
                }
            }
        }

        auto start_iter = hit_snapshot ? next_iter : accessor.begin();
        auto end_iter =
            accessor.lower_bound(std::make_pair(query_timestamp, 0));

        auto it = start_iter;
        while (it != accessor.end() && it != end_iter) {
            // shall check query_timestamp boundary
            // since `lower_bound` could return end while other thread insert new records
            if (it->first >= query_timestamp) {
                break;
            }
            if (it->second < insert_barrier) {
                bitset.set(it->second);
            }
            it++;
        }
        // no need to continue if it.first == query_timestamp, since only timestamps before mvcc_ts are needed
    }

    size_t
    GetSnapshotBitsSize() const {
        auto all_dump_bits = 0;
        auto next_dump_ts = 0;
        std::shared_lock<std::shared_mutex> lock(snap_lock_);
        int loc = snapshots_.size() - 1;
        while (loc >= 0) {
            if (next_dump_ts != snapshots_[loc].first) {
                all_dump_bits += snapshots_[loc].second.size();
            }
            loc--;
        }
        return all_dump_bits;
    }

    void
    DumpSnapshot() {
        SortedDeleteList::Accessor accessor(deleted_lists_);
        int total_size = accessor.size();
        int dumped_size = snapshots_.empty() ? 0 : GetSnapshotBitsSize();

        while (total_size - dumped_size > DUMP_BATCH_SIZE) {
            int32_t bitsize = 0;
            if constexpr (is_sealed) {
                bitsize = sealed_row_count_;
            } else {
                bitsize = insert_record_->size();
            }
            BitsetType bitmap(bitsize, false);

            auto it = accessor.begin();
            Timestamp last_dump_ts = 0;
            if (!snapshots_.empty()) {
                it = snap_next_iter_.back();
                last_dump_ts = snapshots_.back().first;
                bitmap.inplace_or_with_count(snapshots_.back().second,
                                             snapshots_.back().second.size());
            }

            while (total_size - dumped_size > DUMP_BATCH_SIZE &&
                   it != accessor.end()) {
                Timestamp dump_ts = 0;

                for (auto size = 0; size < DUMP_BATCH_SIZE; ++it, ++size) {
                    bitmap.set(it->second);
                    if (size == DUMP_BATCH_SIZE - 1) {
                        dump_ts = it->first;
                    }
                }

                {
                    std::unique_lock<std::shared_mutex> lock(snap_lock_);
                    if (dump_ts == last_dump_ts) {
                        // only update
                        snapshots_.back().second = std::move(bitmap.clone());
                        snap_next_iter_.back() = it;
                    } else {
                        // add new snapshot
                        snapshots_.push_back(
                            std::make_pair(dump_ts, bitmap.clone()));
                        Assert(it != accessor.end() && it.good());
                        snap_next_iter_.push_back(it);
                    }

                    LOG_INFO(
                        "dump delete record snapshot at ts: {}, cursor: {}, "
                        "total size:{} "
                        "current snapshot size: {} for segment: {}",
                        dump_ts,
                        dumped_size + DUMP_BATCH_SIZE,
                        total_size,
                        snapshots_.size(),
                        segment_ ? segment_->get_segment_id() : 0);
                    last_dump_ts = dump_ts;
                }

                dumped_size += DUMP_BATCH_SIZE;
            }
        }
    }

    int64_t
    size() const {
        SortedDeleteList::Accessor accessor(deleted_lists_);
        return accessor.size();
    }

    size_t
    mem_size() const {
        return mem_size_.load();
    }

    void
    set_sealed_row_count(size_t row_count) {
        sealed_row_count_ = row_count;
        deleted_mask_.resize(row_count);
    }

    std::vector<std::pair<Timestamp, BitsetType>>
    get_snapshots() const {
        std::shared_lock<std::shared_mutex> lock(snap_lock_);
        std::vector<std::pair<Timestamp, BitsetType>> snapshots;
        for (const auto& snap : snapshots_) {
            snapshots.emplace_back(snap.first, snap.second.clone());
        }
        return snapshots;
    }

 public:
    std::atomic<int64_t> n_ = 0;
    std::atomic<int64_t> mem_size_ = 0;
    InsertRecord<is_sealed>* insert_record_;
    SegmentInternalInterface* segment_;
    std::shared_ptr<SortedDeleteList> deleted_lists_;
    // max timestamp of deleted records which replayed in load process
    Timestamp max_load_timestamp_{0};
    int32_t sealed_row_count_;
    // used to remove duplicated deleted records for fast access
    BitsetType deleted_mask_;

    // dump snapshot low frequency
    mutable std::shared_mutex snap_lock_;
    std::vector<std::pair<Timestamp, BitsetType>> snapshots_;
    // next delete record iterator that follows every snapshot
    std::vector<SortedDeleteList::iterator> snap_next_iter_;
};

}  // namespace milvus::segcore
