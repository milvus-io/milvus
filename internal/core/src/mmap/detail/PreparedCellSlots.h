// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. Licensed under the Apache License, Version 2.0.
#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

#include "common/EasyAssert.h"

namespace milvus::detail {

// Exact-key lookup for one prepared prefix, not a source-ID cache or domain
// restriction. At most64 distinct cells occupy128 buckets; collisions probe
// and compare the complete int64 key, so IDs sharing low bits never alias.
// Only slot markers are initialized; keys are read after a nonempty marker.
class PreparedCellSlots final {
 public:
    static constexpr size_t kMaximumCells = 64;
    static constexpr size_t kBuckets = 2 * kMaximumCells;
    static constexpr uint8_t kEmpty = 255;

    struct Match {
        size_t bucket;
        size_t cell_index;
    };

    PreparedCellSlots() {
        slots_.fill(kEmpty);
    }

    PreparedCellSlots(const PreparedCellSlots&) = delete;
    PreparedCellSlots&
    operator=(const PreparedCellSlots&) = delete;
    PreparedCellSlots(PreparedCellSlots&&) = delete;
    PreparedCellSlots&
    operator=(PreparedCellSlots&&) = delete;

    Match
    Find(int64_t id) const {
        size_t bucket = static_cast<uint64_t>(id) & (kBuckets - 1);
        for (size_t probe = 0; probe < kBuckets; ++probe) {
            const auto slot = slots_[bucket];
            if (slot == kEmpty || keys_[bucket] == id) {
                return {bucket, slot};
            }
            bucket = (bucket + 1) & (kBuckets - 1);
        }
        ThrowInfo(UnexpectedError, "prepared cell lookup is unexpectedly full");
    }

    // The next compact ordinal must match the caller's unique-cell prefix.
    // Validation happens before mutation, including the65th-insertion error.
    void
    Insert(Match match, int64_t id, size_t cell_index) {
        AssertInfo(match.bucket < kBuckets && match.cell_index == kEmpty &&
                       slots_[match.bucket] == kEmpty && cell_index == size_ &&
                       size_ < kMaximumCells,
                   "invalid prepared cell lookup insertion");
        keys_[match.bucket] = id;
        slots_[match.bucket] = static_cast<uint8_t>(cell_index);
        ++size_;
    }

 private:
    std::array<int64_t, kBuckets> keys_;
    std::array<uint8_t, kBuckets> slots_;
    size_t size_{0};
};

}  // namespace milvus::detail
