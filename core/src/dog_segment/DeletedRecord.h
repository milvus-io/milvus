#pragma once

#include "AckResponder.h"
#include "SegmentDefs.h"

namespace milvus::dog_segment {

struct DeletedRecord {
    std::atomic<int64_t> reserved = 0;
    AckResponder ack_responder_;
    ConcurrentVector<Timestamp, true> timestamps_;
    ConcurrentVector<idx_t, true> uids_;
    struct TmpBitmap {
        // Just for query
        int64_t del_barrier = 0;
        std::vector<bool> bitmap;

    };
    std::shared_ptr<TmpBitmap> lru_;
    std::shared_mutex shared_mutex_;

    DeletedRecord(): lru_(std::make_shared<TmpBitmap>()) {}
    auto get_lru_entry() {
        std::shared_lock lck(shared_mutex_);
        return lru_;
    }
    void insert_lru_entry(std::shared_ptr<TmpBitmap> new_entry) {
        std::lock_guard lck(shared_mutex_);
        if(new_entry->del_barrier <= lru_->del_barrier) {
            // DO NOTHING
            return;
        }
        lru_ = std::move(new_entry);
    }
};

}
