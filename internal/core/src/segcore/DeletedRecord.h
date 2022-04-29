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
#include <utility>

#include "AckResponder.h"
#include "common/Schema.h"
#include "segcore/Record.h"
#include "ConcurrentVector.h"

namespace milvus::segcore {

struct DeletedRecord {
    struct TmpBitmap {
        // Just for query
        int64_t del_barrier = 0;
        BitsetTypePtr bitmap_ptr;

        std::shared_ptr<TmpBitmap>
        clone(int64_t capacity);
    };
    static constexpr int64_t deprecated_size_per_chunk = 32 * 1024;
    DeletedRecord()
        : lru_(std::make_shared<TmpBitmap>()), timestamps_(deprecated_size_per_chunk), pks_(deprecated_size_per_chunk) {
        lru_->bitmap_ptr = std::make_shared<BitsetType>();
    }

    auto
    get_lru_entry() {
        std::shared_lock lck(shared_mutex_);
        return lru_;
    }

    void
    insert_lru_entry(std::shared_ptr<TmpBitmap> new_entry, bool force = false) {
        std::lock_guard lck(shared_mutex_);
        if (new_entry->del_barrier <= lru_->del_barrier) {
            if (!force || new_entry->bitmap_ptr->size() <= lru_->bitmap_ptr->size()) {
                // DO NOTHING
                return;
            }
        }
        lru_ = std::move(new_entry);
    }

 public:
    std::atomic<int64_t> reserved = 0;
    AckResponder ack_responder_;
    ConcurrentVector<Timestamp> timestamps_;
    ConcurrentVector<PkType> pks_;
    int64_t record_size_ = 0;

 private:
    std::shared_ptr<TmpBitmap> lru_;
    std::shared_mutex shared_mutex_;
};

inline auto
DeletedRecord::TmpBitmap::clone(int64_t capacity) -> std::shared_ptr<TmpBitmap> {
    auto res = std::make_shared<TmpBitmap>();
    res->del_barrier = this->del_barrier;
    res->bitmap_ptr = std::make_shared<BitsetType>();
    *(res->bitmap_ptr) = *(this->bitmap_ptr);
    res->bitmap_ptr->resize(capacity, false);
    return res;
}

}  // namespace milvus::segcore
