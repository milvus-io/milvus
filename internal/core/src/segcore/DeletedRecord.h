#pragma once

#include "AckResponder.h"
#include "common/Schema.h"
#include "knowhere/index/vector_index/IndexIVF.h"
#include <memory>

namespace milvus::segcore {

struct DeletedRecord {
    struct TmpBitmap {
        // Just for query
        int64_t del_barrier = 0;
        faiss::ConcurrentBitsetPtr bitmap_ptr;

        std::shared_ptr<TmpBitmap>
        clone(int64_t capacity);
    };

    DeletedRecord() : lru_(std::make_shared<TmpBitmap>()) {
        lru_->bitmap_ptr = std::make_shared<faiss::ConcurrentBitset>(0);
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
            if (!force || new_entry->bitmap_ptr->count() <= lru_->bitmap_ptr->count()) {
                // DO NOTHING
                return;
            }
        }
        lru_ = std::move(new_entry);
    }

 public:
    std::atomic<int64_t> reserved = 0;
    AckResponder ack_responder_;
    ConcurrentVector<Timestamp, true> timestamps_;
    ConcurrentVector<idx_t, true> uids_;

 private:
    std::shared_ptr<TmpBitmap> lru_;
    std::shared_mutex shared_mutex_;
};

inline auto
DeletedRecord::TmpBitmap::clone(int64_t capacity) -> std::shared_ptr<TmpBitmap> {
    auto res = std::make_shared<TmpBitmap>();
    res->del_barrier = this->del_barrier;
    res->bitmap_ptr = std::make_shared<faiss::ConcurrentBitset>(capacity);
    auto u8size = this->bitmap_ptr->u8size();
    memcpy(res->bitmap_ptr->mutable_data(), res->bitmap_ptr->data(), u8size);
    return res;
}

}  // namespace milvus::segcore
