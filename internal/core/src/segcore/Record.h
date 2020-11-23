#pragma once
#include "common/Schema.h"

namespace milvus::segcore {
template <typename RecordType>
inline int64_t
get_barrier(const RecordType& record, Timestamp timestamp) {
    auto& vec = record.timestamps_;
    int64_t beg = 0;
    int64_t end = record.ack_responder_.GetAck();
    while (beg < end) {
        auto mid = (beg + end) / 2;
        if (vec[mid] < timestamp) {
            beg = mid + 1;
        } else {
            end = mid;
        }
    }
    return beg;
}
}  // namespace milvus::segcore
