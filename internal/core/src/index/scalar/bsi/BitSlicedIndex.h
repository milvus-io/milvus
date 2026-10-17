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

#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <type_traits>
#include <vector>

#include "common/Types.h"
#include "index/contracts/build/ScalarBuildInput.h"
#include "index/contracts/query/INullReader.h"
#include "index/contracts/query/IScalarPredicateReader.h"

// In-memory bit-sliced index (BSI) for signed integers: exact evaluation of
// equality, membership and range predicates using only bitmap operations,
// exposed through the IScalarPredicateReader<T> and INullReader query mixins.
//
// Encoding. For every valid row the kernel stores the offset
//     u = (int64_t)v - (int64_t)min        (0 <= u <= max - min < 2^32)
// as SliceCount() = bit_width(max - min) bit slices; slice i holds bit i of u
// for every row. Subtracting the minimum maps signed order onto unsigned
// order, so the type minimum and negative values need no special casing, and
// it trims the slices that would be constant for narrow value ranges. An
// existence bitmap (EBM) marks valid rows; NULL rows are 0 in the EBM and in
// all slices.
//
// Semantics match the scalar predicate contract (index/contracts/README.md):
// every returned bitmap has Count() bits, 1 = hit, and NULL rows are never a
// hit, including for NotIn and NotEqual.
//
// Thread safety: immutable after construction; const methods may be called
// concurrently.

namespace milvus::index {

// Value types the kernel is written for: signed integers of at most 32 bits.
// For these, the offset from min fits in uint32_t and inclusive/exclusive
// bound conversion (value +/- 1) fits in int64_t. 64-bit values would need a
// wider offset and bound type. Only int32_t is instantiated and tested
// (BitSlicedIndex.cpp); another width is added by instantiating it there and
// testing it.
template <typename T>
concept BitSlicedIndexValue = std::is_integral_v<T> && std::is_signed_v<T> &&
                              sizeof(T) <= sizeof(int32_t);

template <BitSlicedIndexValue T>
class BitSlicedIndex final : public IScalarPredicateReader<T>,
                             public INullReader {
 public:
    using ValueType = T;

    // One slice per value bit at most.
    static constexpr size_t kMaxSlices = sizeof(T) * 8;

    // Builds from the complete input. The input is borrowed only for the
    // duration of the constructor and is not retained.
    // Precondition (ScalarBuildInput contract): a non-empty validity view
    // covers batch.values.size() rows. ValidityView is a non-owning view with
    // no length, so this cannot be checked here; the batch producer owns it,
    // as for the other scalar builders.
    explicit BitSlicedIndex(const ScalarBuildInput<T>& input);

    BitSlicedIndex(const BitSlicedIndex&) = delete;
    BitSlicedIndex&
    operator=(const BitSlicedIndex&) = delete;
    BitSlicedIndex(BitSlicedIndex&&) = default;
    BitSlicedIndex&
    operator=(BitSlicedIndex&&) = default;
    ~BitSlicedIndex() override = default;

    // In() strategy switch. Slice walks cost about one bitmap pass per run per
    // slice (O(runs * k)); decoding the row offsets costs a fixed transpose
    // per 64 rows plus one key probe per valid row, independent of the key
    // count. In() walks while runs * k <= kInWalkSliceBudget and decodes
    // otherwise. 512 lies between the crossovers measured with
    // benchmark/BitSlicedIndexBenchmark.cpp on Linux/aarch64 (runs * k of
    // roughly 470 at k = 10 and 780 at k = 32); they are higher on Apple M2.
    // See the design doc for the numbers.
    static constexpr size_t kInWalkSliceBudget = 512;

    // Largest number of key runs In() evaluates with slice walks for an index
    // with `slice_count` slices.
    static constexpr size_t
    MaxInRunWalks(size_t slice_count) {
        return slice_count == 0 ? std::numeric_limits<size_t>::max()
                                : kInWalkSliceBudget / slice_count;
    }

    // The decode strategy probes keys through a dense bitmap over offsets
    // (at most kMaxDenseKeySpan / 8 bytes) when max - min < kMaxDenseKeySpan,
    // and through a hash set of the keys for wider spans.
    static constexpr uint64_t kMaxDenseKeySpan = uint64_t{1} << 20;

    // Logical rows, including NULL rows.
    size_t
    Count() const {
        return num_rows_;
    }

    // Non-NULL rows.
    size_t
    ValidCount() const {
        return valid_count_;
    }

    // Number of stored bit slices, in [0, kMaxSlices]. Zero when there are no
    // valid rows or when every valid row holds the same value.
    size_t
    SliceCount() const {
        return slices_.size();
    }

    // Minimum / maximum over valid rows; nullopt when there are none.
    std::optional<T>
    Min() const;

    std::optional<T>
    Max() const;

    // Heap bytes held by the slices and the existence bitmap.
    int64_t
    MemoryUsage() const;

    // IScalarPredicateReader<T>
    TargetBitmap
    In(size_t n, const T* values) const override;

    TargetBitmap
    NotIn(size_t n, const T* values) const override;

    TargetBitmap
    Range(const T& value, CompareOp op) const override;

    TargetBitmap
    Range(const T& lo, bool lo_inc, const T& hi, bool hi_inc) const override;

    // INullReader
    TargetBitmap
    IsNull() const override;

    TargetBitmap
    IsNotNull() const override;

 private:
    // Bounds are int64_t so that exclusive bounds can be converted to
    // inclusive ones (value +/- 1) without overflow.
    TargetBitmap
    EqualTo(int64_t value) const;

    TargetBitmap
    LessEqual(int64_t hi) const;

    TargetBitmap
    GreaterEqual(int64_t lo) const;

    TargetBitmap
    Between(int64_t lo, int64_t hi) const;

    // O'Neil-Quass slice walks over an offset that is known to lie in
    // [0, max - min]. Each returns the subset of `candidates` (a subset of the
    // EBM, consumed by the call) that satisfies the predicate.
    TargetBitmap
    EqualOffset(uint32_t offset, TargetBitmap candidates) const;

    TargetBitmap
    LessEqualOffset(uint32_t offset, TargetBitmap candidates) const;

    TargetBitmap
    GreaterEqualOffset(uint32_t offset, TargetBitmap candidates) const;

    // In() for many disjoint key runs: decodes each row's offset once and
    // probes `sorted_keys` (sorted, unique offsets in [0, max - min]).
    TargetBitmap
    InByDecoding(const std::vector<uint32_t>& sorted_keys) const;

    TargetBitmap
    Empty() const {
        return TargetBitmap(num_rows_, false);
    }

    uint32_t
    ToOffset(int64_t value) const {
        return static_cast<uint32_t>(value - static_cast<int64_t>(min_));
    }

    size_t num_rows_{0};
    size_t valid_count_{0};
    T min_{0};
    T max_{0};
    TargetBitmap existence_;
    // slices_[i] holds bit i of (value - min_) for every valid row.
    std::vector<TargetBitmap> slices_;
};

// Instantiated in BitSlicedIndex.cpp.
extern template class BitSlicedIndex<int32_t>;

using BitSlicedIndexInt32 = BitSlicedIndex<int32_t>;

}  // namespace milvus::index
