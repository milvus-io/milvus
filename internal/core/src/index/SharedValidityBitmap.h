// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <span>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "common/Types.h"

namespace milvus::index {

// Append-only validity protected by its growing owner's writer lock. Preparing
// storage may allocate; recording an accepted batch and freezing it do not.
//
// Once a complete word is inside an accepted prefix, it is NEVER written again.
// Readers share only those complete words. The incomplete last word is copied
// into each Snapshot: later appends may clear other bits in that same physical
// word, so even reading it with a mask would race. No word needs to be atomic.
class SharedValidityBitmap {
    using Words = std::vector<uint64_t>;
    static constexpr size_t kWordBits = 64;
    static constexpr uint64_t kAllValid = ~uint64_t{0};

 public:
    class Snapshot {
     public:
        size_t
        Count() const noexcept {
            return count_;
        }

        bool
        AllValid() const noexcept {
            return words_ == nullptr;
        }

        // Only these complete words may be read from shared storage. In
        // particular, do not expose the writer's mutable last partial word.
        std::span<const uint64_t>
        FullWords() const noexcept {
            return words_ ? std::span<const uint64_t>(words_->data(),
                                                      count_ / kWordBits)
                          : std::span<const uint64_t>{};
        }

        size_t
        BufferBytes() const noexcept {
            return words_ ? words_->capacity() * sizeof(uint64_t) : 0;
        }

        TargetBitmap
        ToBitmap() const {
            // The constructor sets padding bits to 1 as well. Copy complete
            // words only, then restore 1s above count in our private tail word.
            TargetBitmap result(count_, true);
            if (!words_) {
                return result;
            }
            const auto full = FullWords();
            if (!full.empty()) {
                std::memcpy(result.data(), full.data(), full.size_bytes());
            }
            const auto remaining = count_ % kWordBits;
            if (remaining != 0) {
                result.data()[full.size()] = tail_ | (kAllValid << remaining);
            }
            return result;
        }

     private:
        friend class SharedValidityBitmap;

        Snapshot(std::shared_ptr<const Words> words,
                 size_t count,
                 uint64_t tail)
            : words_(std::move(words)), count_(count), tail_(tail) {
        }

        std::shared_ptr<const Words> words_;
        size_t count_{0};
        uint64_t tail_{kAllValid};
    };

    // The first NULL allocates at least 16 KiB by default, or enough for the
    // complete batch. Later growth doubles capacity unless the batch needs more.
    explicit SharedValidityBitmap(size_t initial_capacity_bits = size_t{1}
                                                                 << 17)
        : initial_words_(CheckedWords(initial_capacity_bits)) {
        AssertInfo(initial_words_ != 0,
                   "validity initial capacity must be positive");
    }

    SharedValidityBitmap(const SharedValidityBitmap&) = delete;
    SharedValidityBitmap&
    operator=(const SharedValidityBitmap&) = delete;

    // Call before the engine accepts input. A failed engine write may leave
    // spare capacity, but neither NULL bits nor the accepted count have changed.
    void
    PrepareAppend(size_t row_end, bool has_null) {
        AssertInfo(row_end >= count_,
                   "validity append must not shrink its prefix");
        const auto required = CheckedWords(row_end);
        if (!words_ && !has_null) {
            return;
        }
        if (words_ && required <= words_->size()) {
            return;
        }
        const auto max_words = Words{}.max_size();
        AssertInfo(required <= max_words && initial_words_ <= max_words,
                   "validity buffer capacity exceeds vector domain");
        auto capacity = initial_words_;
        if (words_) {
            capacity = words_->size() <= max_words / 2 ? words_->size() * 2
                                                       : max_words;
        }
        capacity = std::max(capacity, required);
        auto next = std::make_shared<Words>(capacity, kAllValid);
        if (words_) {
            std::copy(words_->begin(), words_->end(), next->begin());
        }
        words_ = std::move(next);
    }

    // After engine acceptance, callers record only NULLs in the new prefix and
    // then advance it. All storage has been prepared; these operations cannot
    // allocate or throw. Previously accepted rows must never be changed.
    void
    AppendNull(size_t row) noexcept {
        assert(words_ && row >= count_ && row / kWordBits < words_->size());
        (*words_)[row / kWordBits] &= ~(uint64_t{1} << (row % kWordBits));
    }

    void
    FinishAppend(size_t row_end) noexcept {
        assert(row_end >= count_ &&
               (!words_ || (row_end / kWordBits + (row_end % kWordBits != 0)) <=
                               words_->size()));
        count_ = row_end;
    }

    Snapshot
    Freeze() const noexcept {
        const auto tail = words_ && count_ % kWordBits != 0
                              ? (*words_)[count_ / kWordBits]
                              : kAllValid;
        return Snapshot(words_, count_, tail);
    }

    // Sealed readers validate ordering, uniqueness and bounds at their existing
    // boundary. Build just once with row-sized storage, then discard offsets.
    static Snapshot
    FromValidatedNullOffsets(size_t count,
                             std::span<const size_t> null_offsets) {
        SharedValidityBitmap builder(std::max<size_t>(count, 1));
        builder.PrepareAppend(count, !null_offsets.empty());
        for (const auto row : null_offsets) {
            builder.AppendNull(row);
        }
        builder.FinishAppend(count);
        return builder.Freeze();
    }

 private:
    static size_t
    CheckedWords(size_t bits) {
        // TargetBitmap uses (bits + 63) / 64 too. Check before either rounding
        // or allocating, including an all-valid prefix with no buffer.
        AssertInfo(bits <= std::numeric_limits<size_t>::max() - (kWordBits - 1),
                   "validity bit count overflows word rounding");
        return (bits + kWordBits - 1) / kWordBits;
    }

    const size_t initial_words_;
    std::shared_ptr<Words> words_;
    size_t count_{0};
};

}  // namespace milvus::index
