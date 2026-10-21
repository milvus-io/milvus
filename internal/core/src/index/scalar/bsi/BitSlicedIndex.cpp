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

#include "index/scalar/bsi/BitSlicedIndex.h"

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

#include "ankerl/unordered_dense.h"
#include "common/EasyAssert.h"

namespace milvus::index {

namespace {

// TargetBitmap stores bits LSB-first in 64-bit words; the build and the
// decode path below read and write those words directly.
using Word = uint64_t;
static_assert(
    std::is_same_v<std::remove_cv_t<std::remove_pointer_t<
                       decltype(std::declval<const TargetBitmap&>().data())>>,
                   Word>,
    "BitSlicedIndex assumes TargetBitmap uses 64-bit words");
constexpr size_t kWordBits = 64;

// In-place transpose of a 64x64 bit matrix, LSB-first: afterwards bit s of
// a[r] is what bit r of a[s] was. Branchless (6 rounds of masked swaps), so a
// block of 64 rows is decoded in a fixed number of word operations instead of
// one operation per set slice bit.
void
Transpose64(Word* a) {
    Word mask = 0x00000000FFFFFFFFULL;
    for (size_t j = 32; j != 0; j >>= 1, mask ^= (mask << j)) {
        for (size_t k = 0; k < 64; k = ((k | j) + 1) & ~j) {
            const Word t = ((a[k] >> j) ^ a[k | j]) & mask;
            a[k] ^= t << j;
            a[k | j] ^= t;
        }
    }
}

// Mask of the low (i + 1) bits; i is in [0, 31].
constexpr uint64_t
LowMask(size_t i) {
    return (uint64_t{1} << (i + 1)) - 1;
}

}  // namespace

template <BitSlicedIndexValue T>
BitSlicedIndex<T>::BitSlicedIndex(const ScalarBuildInput<T>& input) {
    // Pass 1: row count and value bounds over valid rows.
    size_t rows = 0;
    size_t valid = 0;
    T lo = 0;
    T hi = 0;
    for (const auto& batch : input.batches) {
        const auto n = batch.values.size();
        for (size_t i = 0; i < n; ++i) {
            if (batch.validity && !batch.validity[static_cast<int64_t>(i)]) {
                continue;
            }
            const T v = batch.values[i];
            if (valid == 0) {
                lo = v;
                hi = v;
            } else {
                lo = std::min(lo, v);
                hi = std::max(hi, v);
            }
            ++valid;
        }
        rows += n;
    }

    num_rows_ = rows;
    valid_count_ = valid;
    existence_ = TargetBitmap(rows, false);
    if (valid == 0) {
        return;
    }
    min_ = lo;
    max_ = hi;

    // max - min fits in uint32_t, so at most kMaxSlices slices are needed.
    const size_t slice_count =
        static_cast<size_t>(std::bit_width(ToOffset(max_)));
    slices_.reserve(slice_count);
    for (size_t i = 0; i < slice_count; ++i) {
        slices_.emplace_back(rows, false);
    }

    // Pass 2: assemble each 64-row word of the EBM and of every slice locally,
    // then store whole words. This avoids per-bit set() calls scattered over
    // k + 1 bitmaps. Rows map LSB-first: row r is bit r % 64 of word r / 64;
    // bits past `rows` in the last word stay 0.
    std::array<Word, kMaxSlices> slice_words{};
    Word existence_word = 0;
    const auto store_word = [&](size_t word_index) {
        existence_.data()[word_index] = existence_word;
        existence_word = 0;
        for (size_t s = 0; s < slice_count; ++s) {
            slices_[s].data()[word_index] = slice_words[s];
            slice_words[s] = 0;
        }
    };
    size_t row = 0;
    for (const auto& batch : input.batches) {
        const auto n = batch.values.size();
        for (size_t i = 0; i < n; ++i) {
            if (!batch.validity || batch.validity[static_cast<int64_t>(i)]) {
                const Word bit = Word{1} << (row % kWordBits);
                existence_word |= bit;
                uint32_t offset = ToOffset(batch.values[i]);
                while (offset != 0) {
                    slice_words[static_cast<size_t>(
                        std::countr_zero(offset))] |= bit;
                    offset &= offset - 1;
                }
            }
            ++row;
            if (row % kWordBits == 0) {
                store_word(row / kWordBits - 1);
            }
        }
    }
    if (row % kWordBits != 0) {
        store_word(row / kWordBits);
    }
}

template <BitSlicedIndexValue T>
BitSlicedIndex<T>
BitSlicedIndex<T>::FromState(State state) {
    const size_t rows = state.num_rows;
    if (state.existence.size() != rows) {
        ThrowInfo(DataFormatBroken,
                  "bit-sliced index existence bitmap has {} bits, expected {}",
                  state.existence.size(),
                  rows);
    }
    for (size_t i = 0; i < state.slices.size(); ++i) {
        if (state.slices[i].size() != rows) {
            ThrowInfo(DataFormatBroken,
                      "bit-sliced index slice {} has {} bits, expected {}",
                      i,
                      state.slices[i].size(),
                      rows);
        }
    }

    BitSlicedIndex index;
    index.num_rows_ = rows;
    index.valid_count_ = state.existence.count();
    index.existence_ = std::move(state.existence);
    index.slices_ = std::move(state.slices);
    if (index.valid_count_ == 0) {
        // The constructor leaves min_ = max_ = 0 and no slices for this input;
        // accept exactly that state, so the encoding stays unique.
        if (state.min != 0 || state.max != 0) {
            ThrowInfo(DataFormatBroken,
                      "bit-sliced index has bounds [{}, {}] but no valid rows",
                      state.min,
                      state.max);
        }
        if (!index.slices_.empty()) {
            ThrowInfo(DataFormatBroken,
                      "bit-sliced index has {} slices but no valid rows",
                      index.slices_.size());
        }
        return index;
    }

    if (state.min > state.max) {
        ThrowInfo(DataFormatBroken,
                  "bit-sliced index min {} is above max {}",
                  state.min,
                  state.max);
    }
    index.min_ = state.min;
    index.max_ = state.max;
    const auto span = index.ToOffset(index.max_);
    const auto expected_slices = static_cast<size_t>(std::bit_width(span));
    if (index.slices_.size() != expected_slices) {
        ThrowInfo(DataFormatBroken,
                  "bit-sliced index has {} slices for values in [{}, {}], "
                  "expected {}",
                  index.slices_.size(),
                  index.min_,
                  index.max_,
                  expected_slices);
    }
    // NULL rows are 0 in every slice: the union of the slices is a subset of
    // the EBM. One accumulator, so the check allocates one bitmap.
    auto stray = index.Empty();
    for (const auto& slice : index.slices_) {
        stray |= slice;
    }
    stray -= index.existence_;
    if (stray.any()) {
        ThrowInfo(DataFormatBroken,
                  "bit-sliced index slices have bits on NULL rows");
    }
    // The bounds must be exact: every valid row encodes an offset in
    // [0, max - min] (the <= walk with the span as its constant selects
    // exactly those rows), and both ends are reached by some row.
    if (index.LessEqualOffset(span, index.existence_.clone()).count() !=
        index.valid_count_) {
        ThrowInfo(DataFormatBroken,
                  "bit-sliced index slices encode a value above max {}",
                  index.max_);
    }
    if (!index.EqualOffset(0, index.existence_.clone()).any()) {
        ThrowInfo(DataFormatBroken,
                  "bit-sliced index has no row with value min {}",
                  index.min_);
    }
    if (!index.EqualOffset(span, index.existence_.clone()).any()) {
        ThrowInfo(DataFormatBroken,
                  "bit-sliced index has no row with value max {}",
                  index.max_);
    }
    return index;
}

template <BitSlicedIndexValue T>
std::optional<T>
BitSlicedIndex<T>::Min() const {
    if (valid_count_ == 0) {
        return std::nullopt;
    }
    return min_;
}

template <BitSlicedIndexValue T>
std::optional<T>
BitSlicedIndex<T>::Max() const {
    if (valid_count_ == 0) {
        return std::nullopt;
    }
    return max_;
}

template <BitSlicedIndexValue T>
int64_t
BitSlicedIndex<T>::MemoryUsage() const {
    auto total = static_cast<int64_t>(existence_.size_in_bytes());
    for (const auto& slice : slices_) {
        total += static_cast<int64_t>(slice.size_in_bytes());
    }
    return total;
}

// ---------------------------------------------------------------------------
// Slice walks (O'Neil & Quass, "Improved Query Performance with Variant
// Indexes", SIGMOD 1997). Each walk goes from the most significant slice down
// and maintains `eq`, the candidate rows whose already-visited high bits equal
// the constant's. A walk returns the subset of `candidates` that satisfies its
// predicate, so walks compose: Between() feeds the >= result into the <= walk.
// `candidates` must be a subset of the EBM. Callers guarantee
// 0 <= offset <= max - min.
// ---------------------------------------------------------------------------

template <BitSlicedIndexValue T>
TargetBitmap
BitSlicedIndex<T>::EqualOffset(uint32_t offset, TargetBitmap candidates) const {
    auto eq = std::move(candidates);
    for (size_t i = slices_.size(); i-- > 0;) {
        if ((offset >> i) & 1U) {
            eq &= slices_[i];
        } else {
            eq -= slices_[i];
        }
    }
    return eq;
}

template <BitSlicedIndexValue T>
TargetBitmap
BitSlicedIndex<T>::LessEqualOffset(uint32_t offset,
                                   TargetBitmap candidates) const {
    // lt and eq stay disjoint, which lets the update
    //     lt |= eq & ~slice;  eq &= slice;
    // be written without a temporary as
    //     lt |= eq;  eq &= slice;  lt -= eq;
    auto lt = Empty();
    auto eq = std::move(candidates);
    for (size_t i = slices_.size(); i-- > 0;) {
        // When every remaining low bit of the constant is 1, every row still
        // in eq is <= the constant.
        if ((offset & LowMask(i)) == LowMask(i)) {
            break;
        }
        if ((offset >> i) & 1U) {
            lt |= eq;
            eq &= slices_[i];
            lt -= eq;
        } else {
            eq -= slices_[i];
        }
    }
    lt |= eq;
    return lt;
}

template <BitSlicedIndexValue T>
TargetBitmap
BitSlicedIndex<T>::GreaterEqualOffset(uint32_t offset,
                                      TargetBitmap candidates) const {
    // Mirror image of LessEqualOffset:
    //     gt |= eq & slice;  eq &= ~slice;
    // becomes
    //     gt |= eq;  eq -= slice;  gt -= eq;
    auto gt = Empty();
    auto eq = std::move(candidates);
    for (size_t i = slices_.size(); i-- > 0;) {
        // When every remaining low bit of the constant is 0, every row still
        // in eq is >= the constant.
        if ((offset & LowMask(i)) == 0) {
            break;
        }
        if ((offset >> i) & 1U) {
            eq &= slices_[i];
        } else {
            gt |= eq;
            eq -= slices_[i];
            gt -= eq;
        }
    }
    gt |= eq;
    return gt;
}

// ---------------------------------------------------------------------------
// Bound handling in the signed value domain. int64_t bounds absorb the +/- 1
// used to turn exclusive bounds into inclusive ones.
// ---------------------------------------------------------------------------

template <BitSlicedIndexValue T>
TargetBitmap
BitSlicedIndex<T>::EqualTo(int64_t value) const {
    if (valid_count_ == 0 || value < min_ || value > max_) {
        return Empty();
    }
    return EqualOffset(ToOffset(value), existence_.clone());
}

template <BitSlicedIndexValue T>
TargetBitmap
BitSlicedIndex<T>::LessEqual(int64_t hi) const {
    if (valid_count_ == 0 || hi < min_) {
        return Empty();
    }
    if (hi >= max_) {
        return existence_.clone();
    }
    return LessEqualOffset(ToOffset(hi), existence_.clone());
}

template <BitSlicedIndexValue T>
TargetBitmap
BitSlicedIndex<T>::GreaterEqual(int64_t lo) const {
    if (valid_count_ == 0 || lo > max_) {
        return Empty();
    }
    if (lo <= min_) {
        return existence_.clone();
    }
    return GreaterEqualOffset(ToOffset(lo), existence_.clone());
}

template <BitSlicedIndexValue T>
TargetBitmap
BitSlicedIndex<T>::Between(int64_t lo, int64_t hi) const {
    if (valid_count_ == 0 || lo > hi) {
        return Empty();
    }
    lo = std::max<int64_t>(lo, min_);
    hi = std::min<int64_t>(hi, max_);
    if (lo > hi) {
        return Empty();
    }
    if (lo == hi) {
        return EqualOffset(ToOffset(lo), existence_.clone());
    }
    if (lo == min_) {
        return LessEqual(hi);
    }
    if (hi == max_) {
        return GreaterEqual(lo);
    }
    // Chain the walks: the <= walk starts from the >= result instead of from
    // the EBM, so a two-sided range needs one EBM copy and no final AND.
    return LessEqualOffset(
        ToOffset(hi), GreaterEqualOffset(ToOffset(lo), existence_.clone()));
}

// ---------------------------------------------------------------------------
// IScalarPredicateReader<T>
// ---------------------------------------------------------------------------

template <BitSlicedIndexValue T>
TargetBitmap
BitSlicedIndex<T>::In(size_t n, const T* values) const {
    AssertInfo(n == 0 || values != nullptr,
               "bit-sliced index In received null values with non-zero count");
    auto result = Empty();
    if (n == 0 || valid_count_ == 0) {
        return result;
    }

    // Work in the offset domain: keys outside [min, max] can never match, and
    // offset order equals value order.
    std::vector<uint32_t> keys;
    keys.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        if (values[i] >= min_ && values[i] <= max_) {
            keys.push_back(ToOffset(values[i]));
        }
    }
    std::sort(keys.begin(), keys.end());
    keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
    if (keys.empty()) {
        return result;
    }

    // Group consecutive offsets into inclusive runs [first, last].
    std::vector<std::pair<uint32_t, uint32_t>> runs;
    for (const uint32_t key : keys) {
        if (!runs.empty() &&
            static_cast<uint64_t>(key) ==
                static_cast<uint64_t>(runs.back().second) + 1) {
            runs.back().second = key;
        } else {
            runs.emplace_back(key, key);
        }
    }

    if (runs.size() > MaxInRunWalks(slices_.size())) {
        return InByDecoding(keys);
    }
    // Few runs: one walk per run. Between() routes a single-key run to the
    // equality walk and a longer run to the chained range walk.
    for (const auto& [first, last] : runs) {
        result |= Between(static_cast<int64_t>(min_) + first,
                          static_cast<int64_t>(min_) + last);
    }
    return result;
}

template <BitSlicedIndexValue T>
TargetBitmap
BitSlicedIndex<T>::InByDecoding(
    const std::vector<uint32_t>& sorted_keys) const {
    // Key set over offsets: a dense bitmap when the span is small, otherwise
    // a hash set (the same container TermExpr uses for raw-data IN).
    const uint64_t span = static_cast<uint64_t>(ToOffset(max_)) + 1;
    std::vector<Word> dense_keys;
    ankerl::unordered_dense::set<uint32_t> hashed_keys;
    if (span <= kMaxDenseKeySpan) {
        dense_keys.assign((span + kWordBits - 1) / kWordBits, 0);
        for (const uint32_t key : sorted_keys) {
            dense_keys[key / kWordBits] |= Word{1} << (key % kWordBits);
        }
    } else {
        hashed_keys.insert(sorted_keys.begin(), sorted_keys.end());
    }

    // Decode one 64-row word at a time: load the k slice words for word w
    // into a 64x64 bit block and transpose it, so block[r] becomes row r's
    // offset; then probe each valid row. Extra memory stays O(1) in the row
    // count, and every slice word is read once.
    auto result = Empty();
    Word* out = result.data();
    const Word* ebm = existence_.data();
    const size_t words = existence_.size_in_elements();
    std::array<Word, kWordBits> block{};
    for (size_t w = 0; w < words; ++w) {
        if (ebm[w] == 0) {
            continue;  // result word is already 0
        }
        block.fill(0);
        for (size_t s = 0; s < slices_.size(); ++s) {
            block[s] = slices_[s].data()[w];
        }
        Transpose64(block.data());
        Word hits = 0;
        if (!dense_keys.empty()) {
            for (Word m = ebm[w]; m != 0; m &= m - 1) {
                const auto b = static_cast<size_t>(std::countr_zero(m));
                const auto offset = static_cast<uint32_t>(block[b]);
                hits |=
                    ((dense_keys[offset / kWordBits] >> (offset % kWordBits)) &
                     1)
                    << b;
            }
        } else {
            for (Word m = ebm[w]; m != 0; m &= m - 1) {
                const auto b = static_cast<size_t>(std::countr_zero(m));
                if (hashed_keys.contains(static_cast<uint32_t>(block[b]))) {
                    hits |= Word{1} << b;
                }
            }
        }
        out[w] = hits;
    }
    return result;
}

template <BitSlicedIndexValue T>
TargetBitmap
BitSlicedIndex<T>::NotIn(size_t n, const T* values) const {
    auto result = existence_.clone();
    result -= In(n, values);
    return result;
}

template <BitSlicedIndexValue T>
TargetBitmap
BitSlicedIndex<T>::Range(const T& value, CompareOp op) const {
    const auto v = static_cast<int64_t>(value);
    switch (op) {
        case CompareOp::Equal:
            return EqualTo(v);
        case CompareOp::NotEqual: {
            auto result = existence_.clone();
            result -= EqualTo(v);
            return result;
        }
        case CompareOp::LessThan:
            return LessEqual(v - 1);
        case CompareOp::LessEqual:
            return LessEqual(v);
        case CompareOp::GreaterThan:
            return GreaterEqual(v + 1);
        case CompareOp::GreaterEqual:
            return GreaterEqual(v);
    }
    ThrowInfo(OpTypeInvalid,
              "unsupported bit-sliced index compare operator {}",
              static_cast<int>(op));
}

template <BitSlicedIndexValue T>
TargetBitmap
BitSlicedIndex<T>::Range(const T& lo,
                         bool lo_inc,
                         const T& hi,
                         bool hi_inc) const {
    const auto lower = static_cast<int64_t>(lo) + (lo_inc ? 0 : 1);
    const auto upper = static_cast<int64_t>(hi) - (hi_inc ? 0 : 1);
    return Between(lower, upper);
}

// ---------------------------------------------------------------------------
// INullReader
// ---------------------------------------------------------------------------

template <BitSlicedIndexValue T>
TargetBitmap
BitSlicedIndex<T>::IsNull() const {
    auto result = existence_.clone();
    result.flip();
    return result;
}

template <BitSlicedIndexValue T>
TargetBitmap
BitSlicedIndex<T>::IsNotNull() const {
    return existence_.clone();
}

// Only int32_t is instantiated (and tested) in this stage; see the
// BitSlicedIndexValue concept in the header for the supported widths.
template class BitSlicedIndex<int32_t>;

}  // namespace milvus::index
