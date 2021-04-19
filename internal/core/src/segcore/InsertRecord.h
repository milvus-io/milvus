#pragma once
#include "SegmentDefs.h"
#include "ConcurrentVector.h"
#include "AckResponder.h"

namespace milvus::segcore {
struct InsertRecord {
    std::atomic<int64_t> reserved = 0;
    AckResponder ack_responder_;
    ConcurrentVector<Timestamp, true> timestamps_;
    ConcurrentVector<idx_t, true> uids_;
    std::vector<std::shared_ptr<VectorBase>> entity_vec_;

    InsertRecord(const Schema& schema);

    template <typename Type>
    auto
    get_vec_entity(int offset) {
        return std::static_pointer_cast<ConcurrentVector<Type>>(entity_vec_[offset]);
    }
};
}  // namespace milvus::segcore
