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

#include <algorithm>
#include <numeric>
#include <cstddef>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "TimestampData.h"
#include "TimestampIndex.h"
#include "common/ArrayOffsets.h"
#include "common/EasyAssert.h"
#include "common/Schema.h"
#include "common/TrackingStdAllocator.h"
#include "common/Types.h"
#include "mmap/ChunkedColumn.h"
#include "segcore/AckResponder.h"
#include "segcore/ConcurrentVector.h"
#include <tuple>
#include <type_traits>

namespace milvus::segcore {

constexpr int64_t Unlimited = -1;
constexpr int64_t NoLimit = 0;
// If `bitset_count * 100` > `total_count * BruteForceSelectivity`, we use pk index.
// Otherwise, we use bruteforce to retrieve all the pks and then sort them.
constexpr int64_t BruteForceSelectivity = 10;

using Condition = std::function<bool(int64_t)>;

// Compressed storage for offset -> int64 PK reverse lookup.
// Uses global base + per-block base + bitpacked deltas.
// Block size = 128, O(1) random access within each block.
// Each block stores deltas with a uniform bit width (max bits needed in that block).
class CompressedInt64PkArray {
 public:
    static constexpr int64_t kBlockSize = 128;

    CompressedInt64PkArray() = default;

    void
    build(const int64_t* pks, int64_t num_rows) {
        if (num_rows == 0) {
            return;
        }
        num_rows_ = num_rows;

        // Find global minimum as base
        base_ = pks[0];
        for (int64_t i = 1; i < num_rows; ++i) {
            if (pks[i] < base_) {
                base_ = pks[i];
            }
        }

        // Calculate number of blocks
        int64_t num_blocks = (num_rows + kBlockSize - 1) / kBlockSize;
        block_bit_widths_.reserve(num_blocks);
        block_offsets_.reserve(num_blocks);
        block_bases_.reserve(num_blocks);

        for (int64_t block_id = 0; block_id < num_blocks; ++block_id) {
            int64_t block_start = block_id * kBlockSize;
            int64_t block_end = std::min(block_start + kBlockSize, num_rows);
            int64_t block_count = block_end - block_start;

            // Find block minimum
            int64_t block_min = pks[block_start];
            for (int64_t i = block_start + 1; i < block_end; ++i) {
                if (pks[i] < block_min) {
                    block_min = pks[i];
                }
            }

            // Find max delta to determine bit width
            uint64_t max_delta = 0;
            for (int64_t i = block_start; i < block_end; ++i) {
                uint64_t delta = static_cast<uint64_t>(pks[i] - block_min);
                if (delta > max_delta) {
                    max_delta = delta;
                }
            }

            uint8_t bits = bit_width(max_delta);
            block_bases_.push_back(static_cast<uint64_t>(block_min - base_));
            block_bit_widths_.push_back(bits);
            block_offsets_.push_back(static_cast<uint32_t>(data_.size()));

            // Pack deltas with uniform bit width
            if (bits == 0) {
                // All values identical in this block, no data needed
            } else {
                // Number of bytes needed: ceil(block_count * bits / 8)
                size_t num_bytes =
                    (static_cast<size_t>(block_count) * bits + 7) / 8;
                size_t data_start = data_.size();
                data_.resize(data_start + num_bytes, 0);

                for (int64_t i = 0; i < block_count; ++i) {
                    uint64_t delta =
                        static_cast<uint64_t>(pks[block_start + i] - block_min);
                    pack_value(data_start, i, bits, delta);
                }
            }
        }

        // Shrink to fit
        data_.shrink_to_fit();
        block_offsets_.shrink_to_fit();
        block_bases_.shrink_to_fit();
        block_bit_widths_.shrink_to_fit();
    }

    int64_t
    at(int64_t offset) const {
        AssertInfo(offset >= 0 && offset < num_rows_,
                   "offset out of range: {} not in [0, {})",
                   offset,
                   num_rows_);

        int64_t block_id = offset / kBlockSize;
        int64_t idx_in_block = offset % kBlockSize;
        uint8_t bits = block_bit_widths_[block_id];

        uint64_t delta = 0;
        if (bits > 0) {
            delta = unpack_value(block_offsets_[block_id], idx_in_block, bits);
        }

        return base_ + static_cast<int64_t>(block_bases_[block_id]) +
               static_cast<int64_t>(delta);
    }

    void
    bulk_at(const int64_t* offsets, int64_t count, int64_t* output) const {
        for (int64_t i = 0; i < count; ++i) {
            output[i] = at(offsets[i]);
        }
    }

    size_t
    memory_size() const {
        return sizeof(*this) + data_.capacity() +
               block_offsets_.capacity() * sizeof(uint32_t) +
               block_bases_.capacity() * sizeof(uint64_t) +
               block_bit_widths_.capacity() * sizeof(uint8_t);
    }

    int64_t
    num_rows() const {
        return num_rows_;
    }

    bool
    empty() const {
        return num_rows_ == 0;
    }

 private:
    static uint8_t
    bit_width(uint64_t max_val) {
        if (max_val == 0) {
            return 0;
        }
        return static_cast<uint8_t>(64 - __builtin_clzll(max_val));
    }

    void
    pack_value(size_t data_start, int64_t idx, uint8_t bits, uint64_t value) {
        uint64_t bit_offset = static_cast<uint64_t>(idx) * bits;
        size_t byte_offset = data_start + bit_offset / 8;
        unsigned bit_shift = bit_offset % 8;

        // Low 64 bits of (value << bit_shift)
        uint64_t lo = value << bit_shift;
        for (unsigned b = 0; b < 8 && byte_offset + b < data_.size(); ++b) {
            data_[byte_offset + b] |= static_cast<uint8_t>(lo >> (b * 8));
        }
        // High bits that overflowed past 64-bit boundary
        if (bit_shift > 0 && bit_shift + bits > 64) {
            uint64_t hi = value >> (64 - bit_shift);
            for (unsigned b = 0; byte_offset + 8 + b < data_.size() && b < 8;
                 ++b) {
                data_[byte_offset + 8 + b] |=
                    static_cast<uint8_t>(hi >> (b * 8));
            }
        }
    }

    uint64_t
    unpack_value(uint32_t block_data_offset, int64_t idx, uint8_t bits) const {
        uint64_t bit_offset = static_cast<uint64_t>(idx) * bits;
        size_t byte_offset = block_data_offset + bit_offset / 8;
        unsigned bit_shift = bit_offset % 8;

        // Read low 8 bytes
        uint64_t lo = 0;
        for (unsigned b = 0; b < 8 && byte_offset + b < data_.size(); ++b) {
            lo |= static_cast<uint64_t>(data_[byte_offset + b]) << (b * 8);
        }
        uint64_t result = lo >> bit_shift;

        // Read high bytes if crossing 64-bit boundary
        if (bit_shift > 0 && bit_shift + bits > 64) {
            uint64_t hi = 0;
            for (unsigned b = 0; byte_offset + 8 + b < data_.size() && b < 8;
                 ++b) {
                hi |= static_cast<uint64_t>(data_[byte_offset + 8 + b])
                      << (b * 8);
            }
            result |= hi << (64 - bit_shift);
        }

        uint64_t mask = (bits == 64) ? ~uint64_t(0) : (uint64_t(1) << bits) - 1;
        return result & mask;
    }

 private:
    int64_t base_ = 0;  // Global minimum PK
    int64_t num_rows_ = 0;
    std::vector<uint32_t> block_offsets_;  // Byte offset of each block in data_
    std::vector<uint64_t> block_bases_;    // Block base relative to global base
    std::vector<uint8_t> block_bit_widths_;  // Bit width per block
    std::vector<uint8_t> data_;              // Bitpacked deltas
};

class OffsetMap {
 public:
    virtual ~OffsetMap() = default;

    virtual bool
    contain(const PkType& pk) const = 0;

    virtual std::vector<int64_t>
    find(const PkType& pk) const = 0;

    virtual void
    find_range(const PkType& pk,
               proto::plan::OpType op,
               BitsetTypeView& bitset,
               Condition condition) const = 0;

    virtual void
    insert(const PkType& pk, int64_t offset) = 0;

    virtual void
    seal() = 0;

    virtual bool
    empty() const = 0;

    using OffsetType = int64_t;
    // TODO: in fact, we can retrieve the pk here. Not sure which way is more efficient.
    virtual std::pair<std::vector<OffsetMap::OffsetType>, bool>
    find_first_n(int64_t limit, const BitsetTypeView& bitset) const = 0;

    // Element-level version of find_first_n.
    // Find first N elements that pass filter, ordered by PK then element_index.
    // Returns:
    //   - vector of unique doc_offsets (no duplicates)
    //   - vector of element_indices per doc (element_indices[i] for doc_offsets[i])
    //   - has_more flag indicating if there are more results
    virtual std::
        tuple<std::vector<int64_t>, std::vector<std::vector<int32_t>>, bool>
        find_first_n_element(int64_t limit,
                             const BitsetTypeView& element_bitset,
                             const IArrayOffsets* array_offsets) const = 0;

    virtual void
    clear() = 0;

    virtual size_t
    memory_size() const = 0;
};

template <typename T>
class OffsetOrderedMap : public OffsetMap {
 public:
    using OrderedMap = std::map<
        T,
        std::vector<int64_t>,
        std::less<>,
        TrackingStdAllocator<std::pair<const T, std::vector<int64_t>>>>;

    bool
    contain(const PkType& pk) const override {
        std::shared_lock<std::shared_mutex> lck(mtx_);

        return map_.find(std::get<T>(pk)) != map_.end();
    }

    std::vector<int64_t>
    find(const PkType& pk) const override {
        std::shared_lock<std::shared_mutex> lck(mtx_);

        auto offset_vector = map_.find(std::get<T>(pk));
        return offset_vector != map_.end() ? offset_vector->second
                                           : std::vector<int64_t>();
    }

    void
    find_range(const PkType& pk,
               proto::plan::OpType op,
               BitsetTypeView& bitset,
               Condition condition) const override {
        std::shared_lock<std::shared_mutex> lck(mtx_);
        const T& target = std::get<T>(pk);

        if (op == proto::plan::OpType::Equal) {
            auto it = map_.find(target);
            if (it != map_.end()) {
                for (auto offset : it->second) {
                    if (condition(offset) && offset < bitset.size()) {
                        bitset[offset] = true;
                    }
                }
            }
        } else if (op == proto::plan::OpType::GreaterEqual) {
            auto it = map_.lower_bound(target);
            for (; it != map_.end(); ++it) {
                for (auto offset : it->second) {
                    if (condition(offset) && offset < bitset.size()) {
                        bitset[offset] = true;
                    }
                }
            }
        } else if (op == proto::plan::OpType::GreaterThan) {
            auto it = map_.upper_bound(target);
            for (; it != map_.end(); ++it) {
                for (auto offset : it->second) {
                    if (condition(offset) && offset < bitset.size()) {
                        bitset[offset] = true;
                    }
                }
            }
        } else if (op == proto::plan::OpType::LessEqual) {
            auto it = map_.upper_bound(target);
            for (auto ptr = map_.begin(); ptr != it; ++ptr) {
                for (auto offset : ptr->second) {
                    if (condition(offset) && offset < bitset.size()) {
                        bitset[offset] = true;
                    }
                }
            }
        } else if (op == proto::plan::OpType::LessThan) {
            auto it = map_.lower_bound(target);
            for (auto ptr = map_.begin(); ptr != it; ++ptr) {
                for (auto offset : ptr->second) {
                    if (condition(offset) && offset < bitset.size()) {
                        bitset[offset] = true;
                    }
                }
            }
        } else {
            ThrowInfo(ErrorCode::Unsupported,
                      fmt::format("unsupported op type {}", op));
        }
    }

    void
    insert(const PkType& pk, int64_t offset) override {
        std::unique_lock<std::shared_mutex> lck(mtx_);

        map_[std::get<T>(pk)].emplace_back(offset);
    }

    void
    seal() override {
        ThrowInfo(
            NotImplemented,
            "OffsetOrderedMap used for growing segment could not be sealed.");
    }

    bool
    empty() const override {
        std::shared_lock<std::shared_mutex> lck(mtx_);

        return map_.empty();
    }

    std::pair<std::vector<OffsetMap::OffsetType>, bool>
    find_first_n(int64_t limit, const BitsetTypeView& bitset) const override {
        std::shared_lock<std::shared_mutex> lck(mtx_);

        if (limit == Unlimited || limit == NoLimit) {
            limit = map_.size();
        }

        // TODO: we can't retrieve pk by offset very conveniently.
        //      Selectivity should be done outside.
        return find_first_n_by_index(limit, bitset);
    }

    std::tuple<std::vector<int64_t>, std::vector<std::vector<int32_t>>, bool>
    find_first_n_element(int64_t limit,
                         const BitsetTypeView& element_bitset,
                         const IArrayOffsets* array_offsets) const override {
        std::shared_lock<std::shared_mutex> lck(mtx_);

        if (limit == Unlimited || limit == NoLimit) {
            limit = static_cast<int64_t>(element_bitset.size());
        }

        return find_first_n_element_by_index(
            limit, element_bitset, array_offsets);
    }

    void
    clear() override {
        std::unique_lock<std::shared_mutex> lck(mtx_);
        map_.clear();
    }

    size_t
    memory_size() const override {
        std::shared_lock<std::shared_mutex> lck(mtx_);
        return map_.get_allocator().total_allocated();
    }

 private:
    std::pair<std::vector<OffsetMap::OffsetType>, bool>
    find_first_n_by_index(int64_t limit, const BitsetTypeView& bitset) const {
        int64_t hit_num = 0;  // avoid counting the number everytime.
        auto size = bitset.size();
        int64_t cnt = size - bitset.count();
        auto more_hit_than_limit = cnt > limit;
        limit = std::min(limit, cnt);
        std::vector<int64_t> seg_offsets;
        seg_offsets.reserve(limit);
        auto it = map_.begin();
        for (; hit_num < limit && it != map_.end(); it++) {
            // Offsets in the growing segment are ordered by timestamp,
            // so traverse from back to front to obtain the latest offset.
            for (int i = it->second.size() - 1; i >= 0; --i) {
                auto seg_offset = it->second[i];
                if (seg_offset >= size) {
                    // Frequently concurrent insert/query will cause this case.
                    continue;
                }

                if (!bitset[seg_offset]) {
                    seg_offsets.push_back(seg_offset);
                    hit_num++;
                    // PK hit, no need to continue traversing offsets with the same PK.
                    break;
                }
            }
        }
        return {seg_offsets, more_hit_than_limit && it != map_.end()};
    }

    std::tuple<std::vector<int64_t>, std::vector<std::vector<int32_t>>, bool>
    find_first_n_element_by_index(int64_t limit,
                                  const BitsetTypeView& element_bitset,
                                  const IArrayOffsets* array_offsets) const {
        std::vector<int64_t> doc_offsets;
        std::vector<std::vector<int32_t>> element_indices;

        int64_t hit_num = 0;
        auto element_size = static_cast<int64_t>(element_bitset.size());
        // Clamp limit to the actual number of matching elements,
        // same as find_first_n_by_index does for doc-level queries.
        int64_t cnt = element_size - element_bitset.count();
        auto more_hit_than_limit = cnt > limit;
        limit = std::min(limit, cnt);

        // Traverse map_ in PK order
        std::vector<int32_t> matching_indices;
        auto it = map_.begin();
        for (; hit_num < limit && it != map_.end(); ++it) {
            // For each PK, traverse from back to front to obtain the latest offset.
            // Same as find_first_n_by_index: only use the first (newest) offset
            // that has matching elements, then break to avoid returning stale versions.
            for (int i = it->second.size() - 1; i >= 0 && hit_num < limit;
                 --i) {
                auto doc_offset = it->second[i];

                // Get element range for this doc
                auto [first_elem, last_elem] =
                    array_offsets->ElementIDRangeOfRow(doc_offset);

                // Collect all matching element indices for this doc
                matching_indices.clear();
                for (int64_t elem_id = first_elem;
                     elem_id < last_elem && hit_num < limit;
                     ++elem_id) {
                    if (elem_id >= element_size) {
                        continue;
                    }
                    if (!element_bitset[elem_id]) {  // 0 means pass filter
                        matching_indices.push_back(
                            static_cast<int32_t>(elem_id - first_elem));
                        hit_num++;
                    }
                }

                // Only add doc if it has matching elements
                if (!matching_indices.empty()) {
                    doc_offsets.push_back(doc_offset);
                    element_indices.push_back(std::move(matching_indices));
                    // PK hit, no need to continue traversing older offsets with the same PK.
                    break;
                }
            }
        }

        bool has_more = more_hit_than_limit && (it != map_.end());
        return {std::move(doc_offsets), std::move(element_indices), has_more};
    }

 private:
    OrderedMap map_;
    mutable std::shared_mutex mtx_;
};

template <typename T>
class OffsetOrderedArray : public OffsetMap {
 public:
    bool
    contain(const PkType& pk) const override {
        const T& target = std::get<T>(pk);
        auto it =
            std::lower_bound(array_.begin(),
                             array_.end(),
                             target,
                             [](const std::pair<T, int64_t>& elem,
                                const T& value) { return elem.first < value; });

        return it != array_.end() && it->first == target;
    }

    std::vector<int64_t>
    find(const PkType& pk) const override {
        check_search();

        const T& target = std::get<T>(pk);
        auto it =
            std::lower_bound(array_.begin(),
                             array_.end(),
                             target,
                             [](const std::pair<T, int64_t>& elem,
                                const T& value) { return elem.first < value; });

        std::vector<int64_t> offset_vector;
        for (; it != array_.end() && it->first == target; ++it) {
            offset_vector.push_back(it->second);
        }

        return offset_vector;
    }

    void
    find_range(const PkType& pk,
               proto::plan::OpType op,
               BitsetTypeView& bitset,
               Condition condition) const override {
        check_search();
        auto lower_bound_comp = [](const std::pair<T, int64_t>& elem,
                                   const T& value) {
            return elem.first < value;
        };
        auto upper_bound_comp = [](const T& value,
                                   const std::pair<T, int64_t>& elem) {
            return value < elem.first;
        };

        const T& target = std::get<T>(pk);
        if (op == proto::plan::OpType::Equal) {
            auto it = std::lower_bound(
                array_.begin(), array_.end(), target, lower_bound_comp);
            for (; it != array_.end() && it->first == target; ++it) {
                if (condition(it->second)) {
                    bitset[it->second] = true;
                }
            }
        } else if (op == proto::plan::OpType::GreaterEqual) {
            auto it = std::lower_bound(
                array_.begin(), array_.end(), target, lower_bound_comp);
            for (; it < array_.end(); ++it) {
                if (condition(it->second)) {
                    bitset[it->second] = true;
                }
            }
        } else if (op == proto::plan::OpType::GreaterThan) {
            auto it = std::upper_bound(
                array_.begin(), array_.end(), target, upper_bound_comp);
            for (; it < array_.end(); ++it) {
                if (condition(it->second)) {
                    bitset[it->second] = true;
                }
            }
        } else if (op == proto::plan::OpType::LessEqual) {
            auto it = std::upper_bound(
                array_.begin(), array_.end(), target, upper_bound_comp);
            for (auto ptr = array_.begin(); ptr < it; ++ptr) {
                if (condition(ptr->second)) {
                    bitset[ptr->second] = true;
                }
            }
        } else if (op == proto::plan::OpType::LessThan) {
            auto it = std::lower_bound(
                array_.begin(), array_.end(), target, lower_bound_comp);
            for (auto ptr = array_.begin(); ptr < it; ++ptr) {
                if (condition(ptr->second)) {
                    bitset[ptr->second] = true;
                }
            }
        } else {
            ThrowInfo(ErrorCode::Unsupported,
                      fmt::format("unsupported op type {}", op));
        }
    }

    void
    insert(const PkType& pk, int64_t offset) override {
        if (is_sealed) {
            ThrowInfo(Unsupported,
                      "OffsetOrderedArray could not insert after seal");
        }
        array_.push_back(
            std::make_pair(std::get<T>(pk), static_cast<int32_t>(offset)));
    }

    void
    seal() override {
        sort(array_.begin(), array_.end());
        is_sealed = true;
    }

    bool
    empty() const override {
        return array_.empty();
    }

    std::pair<std::vector<OffsetMap::OffsetType>, bool>
    find_first_n(int64_t limit, const BitsetTypeView& bitset) const override {
        check_search();

        if (limit == Unlimited || limit == NoLimit) {
            limit = array_.size();
        }

        // TODO: we can't retrieve pk by offset very conveniently.
        //      Selectivity should be done outside.
        return find_first_n_by_index(limit, bitset);
    }

    std::tuple<std::vector<int64_t>, std::vector<std::vector<int32_t>>, bool>
    find_first_n_element(int64_t limit,
                         const BitsetTypeView& element_bitset,
                         const IArrayOffsets* array_offsets) const override {
        check_search();

        if (limit == Unlimited || limit == NoLimit) {
            limit = static_cast<int64_t>(element_bitset.size());
        }

        return find_first_n_element_by_index(
            limit, element_bitset, array_offsets);
    }

    void
    clear() override {
        array_.clear();
        is_sealed = false;
    }

    size_t
    memory_size() const override {
        return sizeof(std::pair<T, int32_t>) * array_.capacity();
    }

 private:
    std::pair<std::vector<OffsetMap::OffsetType>, bool>
    find_first_n_by_index(int64_t limit, const BitsetTypeView& bitset) const {
        int64_t hit_num = 0;  // avoid counting the number everytime.
        auto size = bitset.size();
        int64_t cnt = size - bitset.count();
        auto more_hit_than_limit = cnt > limit;
        limit = std::min(limit, cnt);
        std::vector<int64_t> seg_offsets;
        seg_offsets.reserve(limit);
        auto it = array_.begin();
        for (; hit_num < limit && it != array_.end(); it++) {
            auto seg_offset = it->second;
            if (seg_offset >= size) {
                // In fact, this case won't happen on sealed segments.
                continue;
            }

            if (!bitset[seg_offset]) {
                seg_offsets.push_back(seg_offset);
                hit_num++;
            }
        }
        return {seg_offsets, more_hit_than_limit && it != array_.end()};
    }

    std::tuple<std::vector<int64_t>, std::vector<std::vector<int32_t>>, bool>
    find_first_n_element_by_index(int64_t limit,
                                  const BitsetTypeView& element_bitset,
                                  const IArrayOffsets* array_offsets) const {
        std::vector<int64_t> doc_offsets;
        std::vector<std::vector<int32_t>> element_indices;

        int64_t hit_num = 0;
        auto element_size = static_cast<int64_t>(element_bitset.size());
        // Clamp limit to the actual number of matching elements,
        // same as find_first_n_by_index does for doc-level queries.
        int64_t cnt = element_size - element_bitset.count();
        auto more_hit_than_limit = cnt > limit;
        limit = std::min(limit, cnt);

        // Traverse array_ in PK order (already sorted)
        std::vector<int32_t> matching_indices;
        auto it = array_.begin();
        for (; hit_num < limit && it != array_.end(); ++it) {
            auto doc_offset = it->second;

            // Get element range for this doc
            auto [first_elem, last_elem] =
                array_offsets->ElementIDRangeOfRow(doc_offset);

            // Collect all matching element indices for this doc
            matching_indices.clear();
            for (int64_t elem_id = first_elem;
                 elem_id < last_elem && hit_num < limit;
                 ++elem_id) {
                if (elem_id >= element_size) {
                    continue;
                }
                if (!element_bitset[elem_id]) {  // 0 means pass filter
                    matching_indices.push_back(
                        static_cast<int32_t>(elem_id - first_elem));
                    hit_num++;
                }
            }

            // Only add doc if it has matching elements
            if (!matching_indices.empty()) {
                doc_offsets.push_back(doc_offset);
                element_indices.push_back(std::move(matching_indices));
            }
        }

        bool has_more = more_hit_than_limit && (it != array_.end());
        return {std::move(doc_offsets), std::move(element_indices), has_more};
    }

    void
    check_search() const {
        AssertInfo(is_sealed,
                   "OffsetOrderedArray could not search before seal");
    }

 private:
    bool is_sealed = false;
    std::vector<std::pair<T, int32_t>> array_;
};

class InsertRecordSealed {
 public:
    InsertRecordSealed(
        const Schema& schema,
        const int64_t size_per_chunk,
        const storage::MmapChunkDescriptorPtr mmap_descriptor = nullptr) {
        std::optional<FieldId> pk_field_id = schema.get_primary_field_id();
        // for sealed segment, only pk field is added.
        for (auto& field : schema) {
            auto field_id = field.first;
            if (pk_field_id.has_value() && pk_field_id.value() == field_id) {
                auto& field_meta = field.second;
                AssertInfo(!field_meta.is_nullable(),
                           "Primary key should not be nullable");
                switch (field_meta.get_data_type()) {
                    case DataType::INT64: {
                        pk2offset_ =
                            std::make_unique<OffsetOrderedArray<int64_t>>();
                        is_int64_pk_ = true;
                        break;
                    }
                    case DataType::VARCHAR: {
                        pk2offset_ =
                            std::make_unique<OffsetOrderedArray<std::string>>();
                        is_int64_pk_ = false;
                        break;
                    }
                    default: {
                        ThrowInfo(DataTypeInvalid,
                                  fmt::format("unsupported pk type",
                                              field_meta.get_data_type()));
                    }
                }
            }
        }
    }

    ~InsertRecordSealed() {
        if (estimated_memory_size_ > 0) {
            cachinglayer::Manager::GetInstance().RefundLoadedResource(
                {static_cast<int64_t>(estimated_memory_size_), 0});
            estimated_memory_size_ = 0;
        }
    }

    bool
    contain(const PkType& pk) const {
        return pk2offset_->contain(pk);
    }

    std::vector<SegOffset>
    search_pk(const PkType& pk,
              Timestamp timestamp,
              bool include_same_ts = true) const {
        std::shared_lock lck(shared_mutex_);
        std::vector<SegOffset> res_offsets;
        auto offset_iter = pk2offset_->find(pk);
        auto timestamp_hit =
            include_same_ts ? [](const Timestamp& ts1,
                                 const Timestamp& ts2) { return ts1 <= ts2; }
                            : [](const Timestamp& ts1, const Timestamp& ts2) {
                                  return ts1 < ts2;
                              };
        for (auto offset : offset_iter) {
            if (timestamp_hit(timestamps_[offset], timestamp)) {
                res_offsets.emplace_back(offset);
            }
        }
        return res_offsets;
    }

    void
    search_pk_range(const PkType& pk,
                    proto::plan::OpType op,
                    BitsetTypeView& bitset) const {
        pk2offset_->find_range(
            pk, op, bitset, [](int64_t offset) { return true; });
    }

    void
    search_pk_range(const PkType& pk,
                    Timestamp timestamp,
                    proto::plan::OpType op,
                    BitsetTypeView& bitset) const {
        auto condition = [this, timestamp](int64_t offset) {
            return timestamps_[offset] <= timestamp;
        };
        pk2offset_->find_range(pk, op, bitset, condition);
    }

    void
    search_pk_binary_range(const PkType& lower_pk,
                           bool lower_inclusive,
                           const PkType& upper_pk,
                           bool upper_inclusive,
                           BitsetTypeView& bitset) const {
        auto lower_op = lower_inclusive ? proto::plan::OpType::GreaterEqual
                                        : proto::plan::OpType::GreaterThan;
        auto upper_op = upper_inclusive ? proto::plan::OpType::LessEqual
                                        : proto::plan::OpType::LessThan;

        BitsetType upper_result(bitset.size());
        auto upper_view = upper_result.view();

        // values >= lower_pk (or > lower_pk if not inclusive)
        pk2offset_->find_range(
            lower_pk, lower_op, bitset, [](int64_t offset) { return true; });

        // values <= upper_pk (or < upper_pk if not inclusive)
        pk2offset_->find_range(
            upper_pk, upper_op, upper_view, [](int64_t offset) {
                return true;
            });

        bitset &= upper_result;
    }

    void
    insert_pks(milvus::DataType data_type, ChunkedColumnInterface* data) {
        std::lock_guard lck(shared_mutex_);
        int64_t offset = 0;
        switch (data_type) {
            case DataType::INT64: {
                auto num_chunk = data->num_chunks();
                std::vector<int64_t> chunk_ids(num_chunk);
                std::iota(chunk_ids.begin(), chunk_ids.end(), 0);
                data->PrefetchChunks(nullptr, chunk_ids);
                for (int i = 0; i < num_chunk; ++i) {
                    auto pw = data->DataOfChunk(nullptr, i);
                    auto pks = reinterpret_cast<const int64_t*>(pw.get());
                    auto chunk_num_rows = data->chunk_row_nums(i);
                    for (int j = 0; j < chunk_num_rows; ++j) {
                        pk2offset_->insert(pks[j], offset++);
                    }
                }
                break;
            }
            case DataType::VARCHAR: {
                auto num_chunk = data->num_chunks();
                for (int i = 0; i < num_chunk; ++i) {
                    auto column =
                        reinterpret_cast<ChunkedVariableColumn<std::string>*>(
                            data);
                    auto pw = column->StringViews(nullptr, i);
                    auto pks = pw.get().first;
                    for (auto& pk : pks) {
                        pk2offset_->insert(std::string(pk), offset++);
                    }
                }
                break;
            }
            default: {
                ThrowInfo(DataTypeInvalid,
                          fmt::format("unsupported primary key data type",
                                      data_type));
            }
        }
    }

    // Build only the compressed offset->pk mapping (no pk2offset_ index).
    // Used by sorted segments where pk2offset_ is not needed.
    void
    build_offset2pk(milvus::DataType data_type, ChunkedColumnInterface* data) {
        if (data_type != DataType::INT64 || !is_int64_pk_) {
            return;
        }
        std::lock_guard lck(shared_mutex_);
        int64_t total_rows = data->NumRows();
        std::vector<int64_t> all_pks;
        all_pks.reserve(total_rows);

        auto num_chunk = data->num_chunks();
        // Prefetch all chunks so that subsequent sequential
        // DataOfChunk() calls hit the cache instead of blocking
        // on HIGH pool downloads one at a time.
        std::vector<int64_t> chunk_ids(num_chunk);
        std::iota(chunk_ids.begin(), chunk_ids.end(), 0);
        data->PrefetchChunks(nullptr, chunk_ids);

        for (int i = 0; i < num_chunk; ++i) {
            auto pw = data->DataOfChunk(nullptr, i);
            auto pks = reinterpret_cast<const int64_t*>(pw.get());
            auto chunk_num_rows = data->chunk_row_nums(i);
            for (int j = 0; j < chunk_num_rows; ++j) {
                all_pks.push_back(pks[j]);
            }
        }

        offset2pk_ = std::make_unique<CompressedInt64PkArray>();
        offset2pk_->build(all_pks.data(), all_pks.size());

        size_t mem = offset2pk_->memory_size();
        cachinglayer::Manager::GetInstance().ChargeLoadedResource(
            {static_cast<int64_t>(mem), 0});
        estimated_memory_size_ += mem;
    }

    bool
    empty_pks() const {
        std::shared_lock lck(shared_mutex_);
        return pk2offset_->empty();
    }

    void
    seal_pks() {
        std::lock_guard lck(shared_mutex_);
        pk2offset_->seal();
        // update estimated memory size to caching layer
        // offset2pk_ memory is charged separately in build_offset2pk()
        size_t total_size = pk2offset_->memory_size();
        cachinglayer::Manager::GetInstance().ChargeLoadedResource(
            {static_cast<int64_t>(total_size), 0});
        estimated_memory_size_ += total_size;
    }

    // Pin mode: zero-copy from column chunks (StorageV2, single or multi-chunk)
    void
    init_timestamps_from_column(
        std::shared_ptr<ChunkedColumnInterface> column,
        std::vector<cachinglayer::PinWrapper<Chunk*>> pins,
        TimestampIndex timestamp_index) {
        std::lock_guard lck(shared_mutex_);
        timestamps_.InitFromPinnedChunks(std::move(column), std::move(pins));
        timestamp_index_ = std::move(timestamp_index);
        // Pin mode: timestamp data is managed by the column group,
        // only charge the index metadata memory.
        size_t ts_index_size = timestamp_index_.memory_size();
        cachinglayer::Manager::GetInstance().ChargeLoadedResource(
            {static_cast<int64_t>(ts_index_size), 0});
        estimated_memory_size_ += ts_index_size;
    }

    // Own mode: takes ownership of timestamp data (StorageV1 / multi-chunk)
    void
    init_timestamps_from_owned(std::vector<Timestamp> data,
                               TimestampIndex timestamp_index) {
        std::lock_guard lck(shared_mutex_);
        size_t count = data.size();
        timestamps_.InitFromOwnedData(std::move(data));
        timestamp_index_ = std::move(timestamp_index);
        size_t ts_data_size = count * sizeof(Timestamp);
        size_t ts_index_size = timestamp_index_.memory_size();
        cachinglayer::Manager::GetInstance().ChargeLoadedResource(
            {static_cast<int64_t>(ts_data_size + ts_index_size), 0});
        estimated_memory_size_ += ts_data_size + ts_index_size;
    }

    const TimestampData&
    timestamps() const {
        return timestamps_;
    }

    void
    clear() {
        timestamps_.clear();
        timestamp_index_ = TimestampIndex();
        if (pk2offset_) {
            pk2offset_->clear();
        }
        offset2pk_.reset();

        reserved = 0;
        if (estimated_memory_size_ > 0) {
            cachinglayer::Manager::GetInstance().RefundLoadedResource(
                {static_cast<int64_t>(estimated_memory_size_), 0});
            estimated_memory_size_ = 0;
        }
    }

    // Returns true if PK type is int64 and offset2pk is available
    bool
    has_int64_pk_index() const {
        return is_int64_pk_ && offset2pk_ != nullptr;
    }

    // Get PK by offset. Only valid when has_int64_pk_index() returns true.
    int64_t
    get_int64_pk_by_offset(int64_t offset) const {
        AssertInfo(
            has_int64_pk_index(),
            "get_int64_pk_by_offset requires int64 PK with offset2pk index");
        return offset2pk_->at(offset);
    }

    // Bulk get PKs by offsets. Only valid when has_int64_pk_index() returns true.
    void
    bulk_get_int64_pks_by_offsets(const int64_t* offsets,
                                  int64_t count,
                                  int64_t* output) const {
        AssertInfo(has_int64_pk_index(),
                   "bulk_get_int64_pks_by_offsets requires int64 PK with "
                   "offset2pk index");
        offset2pk_->bulk_at(offsets, count, output);
    }

 public:
    TimestampData timestamps_;
    std::atomic<int64_t> reserved = 0;
    // used for timestamps index of sealed segment
    TimestampIndex timestamp_index_;
    // pks to row offset
    std::unique_ptr<OffsetMap> pk2offset_;
    // offset to pk (compressed), only available for int64 PK
    std::unique_ptr<CompressedInt64PkArray> offset2pk_;
    // whether PK type is int64
    bool is_int64_pk_ = false;
    // estimated memory size of InsertRecord, only used for sealed segment
    int64_t estimated_memory_size_{0};

 protected:
    mutable std::shared_mutex shared_mutex_{};
};

class InsertRecordGrowing {
 public:
    InsertRecordGrowing(
        const Schema& schema,
        const int64_t size_per_chunk,
        const storage::MmapChunkDescriptorPtr mmap_descriptor = nullptr)
        : timestamps_(size_per_chunk) {
        std::optional<FieldId> pk_field_id = schema.get_primary_field_id();
        for (auto& field : schema) {
            auto field_id = field.first;
            auto& field_meta = field.second;
            if (pk_field_id.has_value() && pk_field_id.value() == field_id) {
                AssertInfo(!field_meta.is_nullable(),
                           "Primary key should not be nullable");
                switch (field_meta.get_data_type()) {
                    case DataType::INT64: {
                        pk2offset_ =
                            std::make_unique<OffsetOrderedMap<int64_t>>();
                        break;
                    }
                    case DataType::VARCHAR: {
                        pk2offset_ =
                            std::make_unique<OffsetOrderedMap<std::string>>();
                        break;
                    }
                    default: {
                        ThrowInfo(DataTypeInvalid,
                                  fmt::format("unsupported pk type: {}",
                                              field_meta.get_data_type()));
                    }
                }
            }
            append_field_meta(
                field_id, field_meta, size_per_chunk, mmap_descriptor);
        }
    }

    bool
    contain(const PkType& pk) const {
        return pk2offset_->contain(pk);
    }

    std::vector<SegOffset>
    search_pk(const PkType& pk,
              Timestamp timestamp,
              bool include_same_ts = true) const {
        std::shared_lock<std::shared_mutex> lck(shared_mutex_);
        std::vector<SegOffset> res_offsets;
        auto offset_iter = pk2offset_->find(pk);
        auto timestamp_hit =
            include_same_ts ? [](const Timestamp& ts1,
                                 const Timestamp& ts2) { return ts1 <= ts2; }
                            : [](const Timestamp& ts1, const Timestamp& ts2) {
                                  return ts1 < ts2;
                              };
        for (auto offset : offset_iter) {
            if (timestamp_hit(timestamps_[offset], timestamp)) {
                res_offsets.emplace_back(offset);
            }
        }
        return res_offsets;
    }

    void
    search_pk_range(const PkType& pk,
                    proto::plan::OpType op,
                    BitsetTypeView& bitset) const {
        pk2offset_->find_range(
            pk, op, bitset, [](int64_t offset) { return true; });
    }

    void
    search_pk_range(const PkType& pk,
                    Timestamp timestamp,
                    proto::plan::OpType op,
                    BitsetTypeView& bitset) const {
        auto condition = [this, timestamp](int64_t offset) {
            return timestamps_[offset] <= timestamp;
        };
        pk2offset_->find_range(pk, op, bitset, condition);
    }

    void
    insert_pks(const std::vector<FieldDataPtr>& field_datas) {
        std::lock_guard lck(shared_mutex_);
        int64_t offset = 0;
        for (auto& data : field_datas) {
            int64_t row_count = data->get_num_rows();
            auto data_type = data->get_data_type();
            switch (data_type) {
                case DataType::INT64: {
                    for (int i = 0; i < row_count; ++i) {
                        pk2offset_->insert(
                            *static_cast<const int64_t*>(data->RawValue(i)),
                            offset++);
                    }
                    break;
                }
                case DataType::VARCHAR: {
                    for (int i = 0; i < row_count; ++i) {
                        pk2offset_->insert(
                            *static_cast<const std::string*>(data->RawValue(i)),
                            offset++);
                    }
                    break;
                }
                default: {
                    ThrowInfo(DataTypeInvalid,
                              fmt::format("unsupported primary key data type",
                                          data_type));
                }
            }
        }
    }

    bool
    empty_pks() const {
        std::shared_lock lck(shared_mutex_);
        return pk2offset_->empty();
    }

    void
    seal_pks() {
        std::lock_guard lck(shared_mutex_);
        pk2offset_
            ->seal();  // will throw for growing map, consistent with previous behavior
    }

    const ConcurrentVector<Timestamp>&
    timestamps() const {
        return timestamps_;
    }

    void
    clear() {
        timestamps_.clear();
        timestamp_index_ = TimestampIndex();
        if (pk2offset_) {
            pk2offset_->clear();
        }
        reserved = 0;
        data_.clear();
        ack_responder_.clear();
    }

    void
    append_field_meta(
        FieldId field_id,
        const FieldMeta& field_meta,
        int64_t size_per_chunk,
        const storage::MmapChunkDescriptorPtr mmap_descriptor = nullptr) {
        if (field_meta.is_nullable()) {
            this->append_valid_data(field_id);
        }
        const milvus::storage::MmapConfig mmap_config =
            storage::MmapManager::GetInstance().GetMmapConfig();
        // todo: @cqy123456, scalar and vector mmap should be depend on GetScalarFieldEnableMmap()/ GetVectorFieldEnableMmap()
        storage::MmapChunkDescriptorPtr scalar_mmap_descriptor =
            mmap_config.GetEnableGrowingMmap() ? mmap_descriptor : nullptr;
        storage::MmapChunkDescriptorPtr vec_mmap_descriptor =
            mmap_config.GetEnableGrowingMmap() ? mmap_descriptor : nullptr;
        storage::MmapChunkDescriptorPtr dense_vec_mmap_descriptor = nullptr;
        // todo: @cqy123456, remove all condition and select of dense_vec_mmap_descriptor later
        {
            const auto& segcore_config =
                milvus::segcore::SegcoreConfig::default_config();
            auto enable_intermin_index =
                segcore_config.get_enable_interim_segment_index();
            auto dense_vec_intermin_index_type =
                segcore_config.get_dense_vector_intermin_index_type();
            auto enable_mmap_field_mmap =
                mmap_config.GetVectorFieldEnableMmap();
            bool enable_dense_vec_mmap =
                enable_intermin_index && enable_mmap_field_mmap &&
                (dense_vec_intermin_index_type ==
                 knowhere::IndexEnum::INDEX_FAISS_SCANN_DVR);
            dense_vec_mmap_descriptor =
                enable_dense_vec_mmap || mmap_config.GetEnableGrowingMmap()
                    ? mmap_descriptor
                    : nullptr;
        }
        if (field_meta.is_vector()) {
            if (field_meta.get_data_type() == DataType::VECTOR_FLOAT) {
                this->append_data<FloatVector>(field_id,
                                               field_meta.get_dim(),
                                               size_per_chunk,
                                               dense_vec_mmap_descriptor);
                return;
            } else if (field_meta.get_data_type() == DataType::VECTOR_BINARY) {
                this->append_data<BinaryVector>(field_id,
                                                field_meta.get_dim(),
                                                size_per_chunk,
                                                dense_vec_mmap_descriptor);
                return;
            } else if (field_meta.get_data_type() == DataType::VECTOR_FLOAT16) {
                this->append_data<Float16Vector>(field_id,
                                                 field_meta.get_dim(),
                                                 size_per_chunk,
                                                 dense_vec_mmap_descriptor);
                return;
            } else if (field_meta.get_data_type() ==
                       DataType::VECTOR_BFLOAT16) {
                this->append_data<BFloat16Vector>(field_id,
                                                  field_meta.get_dim(),
                                                  size_per_chunk,
                                                  dense_vec_mmap_descriptor);
                return;
            } else if (field_meta.get_data_type() ==
                       DataType::VECTOR_SPARSE_U32_F32) {
                this->append_data<SparseFloatVector>(
                    field_id, size_per_chunk, vec_mmap_descriptor);
                return;
            } else if (field_meta.get_data_type() == DataType::VECTOR_INT8) {
                this->append_data<Int8Vector>(field_id,
                                              field_meta.get_dim(),
                                              size_per_chunk,
                                              dense_vec_mmap_descriptor);
                return;
            } else if (field_meta.get_data_type() == DataType::VECTOR_ARRAY) {
                this->append_data<VectorArray>(field_id,
                                               field_meta.get_dim(),
                                               size_per_chunk,
                                               dense_vec_mmap_descriptor);
                return;
            } else {
                ThrowInfo(DataTypeInvalid,
                          fmt::format("unsupported vector type",
                                      field_meta.get_data_type()));
            }
        }
        switch (field_meta.get_data_type()) {
            case DataType::BOOL: {
                this->append_data<bool>(
                    field_id, size_per_chunk, scalar_mmap_descriptor);
                return;
            }
            case DataType::INT8: {
                this->append_data<int8_t>(
                    field_id, size_per_chunk, scalar_mmap_descriptor);
                return;
            }
            case DataType::INT16: {
                this->append_data<int16_t>(
                    field_id, size_per_chunk, scalar_mmap_descriptor);
                return;
            }
            case DataType::INT32: {
                this->append_data<int32_t>(
                    field_id, size_per_chunk, scalar_mmap_descriptor);
                return;
            }
            case DataType::INT64: {
                this->append_data<int64_t>(
                    field_id, size_per_chunk, scalar_mmap_descriptor);
                return;
            }
            case DataType::FLOAT: {
                this->append_data<float>(
                    field_id, size_per_chunk, scalar_mmap_descriptor);
                return;
            }
            case DataType::DOUBLE: {
                this->append_data<double>(
                    field_id, size_per_chunk, scalar_mmap_descriptor);
                return;
            }
            case DataType::TIMESTAMPTZ: {
                this->append_data<int64_t>(
                    field_id, size_per_chunk, scalar_mmap_descriptor);
                return;
            }
            case DataType::VARCHAR:
            case DataType::TEXT: {
                this->append_data<std::string>(
                    field_id, size_per_chunk, scalar_mmap_descriptor);
                return;
            }
            case DataType::JSON: {
                this->append_data<Json>(
                    field_id, size_per_chunk, scalar_mmap_descriptor);
                return;
            }
            case DataType::ARRAY: {
                this->append_data<Array>(
                    field_id, size_per_chunk, scalar_mmap_descriptor);
                return;
            }
            case DataType::GEOMETRY: {
                this->append_data<std::string>(
                    field_id, size_per_chunk, scalar_mmap_descriptor);
                return;
            }
            default: {
                ThrowInfo(DataTypeInvalid,
                          fmt::format("unsupported scalar type",
                                      field_meta.get_data_type()));
            }
        }
    }

    void
    insert_pk(const PkType& pk, int64_t offset) {
        std::lock_guard lck(shared_mutex_);
        pk2offset_->insert(pk, offset);
    }

    // get data without knowing the type
    VectorBase*
    get_data_base(FieldId field_id) const {
        AssertInfo(data_.find(field_id) != data_.end(),
                   "Cannot find field_data with field_id: " +
                       std::to_string(field_id.get()));
        AssertInfo(data_.at(field_id) != nullptr,
                   "data_ at i is null" + std::to_string(field_id.get()));
        return data_.at(field_id).get();
    }

    // get field data in given type, const version
    template <typename Type>
    const ConcurrentVector<Type>*
    get_data(FieldId field_id) const {
        auto base_ptr = get_data_base(field_id);
        auto ptr = dynamic_cast<const ConcurrentVector<Type>*>(base_ptr);
        Assert(ptr);
        return ptr;
    }

    // get field data in given type, non-const version
    template <typename Type>
    ConcurrentVector<Type>*
    get_data(FieldId field_id) {
        auto base_ptr = get_data_base(field_id);
        auto ptr = dynamic_cast<ConcurrentVector<Type>*>(base_ptr);
        Assert(ptr);
        return ptr;
    }

    ThreadSafeValidDataPtr
    get_valid_data(FieldId field_id) const {
        AssertInfo(valid_data_.find(field_id) != valid_data_.end(),
                   "Cannot find valid_data with field_id: " +
                       std::to_string(field_id.get()));
        AssertInfo(valid_data_.at(field_id) != nullptr,
                   "valid_data_ at i is null" + std::to_string(field_id.get()));
        return valid_data_.at(field_id);
    }

    bool
    is_data_exist(FieldId field_id) const {
        return data_.find(field_id) != data_.end();
    }

    bool
    is_valid_data_exist(FieldId field_id) const {
        return valid_data_.find(field_id) != valid_data_.end();
    }

    SpanBase
    get_span_base(FieldId field_id, int64_t chunk_id) const {
        auto data = get_data_base(field_id);
        if (is_valid_data_exist(field_id)) {
            auto size = data->get_chunk_size(chunk_id);
            auto element_offset = data->get_element_offset(chunk_id);
            return SpanBase(
                data->get_chunk_data(chunk_id),
                get_valid_data(field_id)->get_chunk_data(element_offset),
                size,
                data->get_element_size());
        }
        return data->get_span_base(chunk_id);
    }

    // append a column of scalar type
    void
    append_valid_data(FieldId field_id) {
        valid_data_.emplace(field_id, std::make_shared<ThreadSafeValidData>());
    }

    // append a column of vector type
    template <typename VectorType>
    void
    append_data(FieldId field_id,
                int64_t dim,
                int64_t size_per_chunk,
                const storage::MmapChunkDescriptorPtr mmap_descriptor) {
        static_assert(std::is_base_of_v<VectorTrait, VectorType>);
        bool use_mapping_storage = is_valid_data_exist(field_id);
        data_.emplace(
            field_id,
            std::make_unique<ConcurrentVector<VectorType>>(
                dim,
                size_per_chunk,
                mmap_descriptor,
                use_mapping_storage ? get_valid_data(field_id) : nullptr,
                use_mapping_storage));
    }

    // append a column of scalar or sparse float vector type
    template <typename Type>
    void
    append_data(FieldId field_id,
                int64_t size_per_chunk,
                const storage::MmapChunkDescriptorPtr mmap_descriptor) {
        static_assert(IsScalar<Type> || IsSparse<Type>);
        bool use_mapping_storage = is_valid_data_exist(field_id);
        if constexpr (IsSparse<Type>) {
            data_.emplace(
                field_id,
                std::make_unique<ConcurrentVector<Type>>(
                    size_per_chunk,
                    mmap_descriptor,
                    use_mapping_storage ? get_valid_data(field_id) : nullptr,
                    use_mapping_storage));
        } else {
            data_.emplace(
                field_id,
                std::make_unique<ConcurrentVector<Type>>(
                    size_per_chunk,
                    mmap_descriptor,
                    use_mapping_storage ? get_valid_data(field_id) : nullptr));
        }
    }

    void
    drop_field_data(FieldId field_id) {
        data_.erase(field_id);
        valid_data_.erase(field_id);
    }

    int64_t
    row_count() const {
        return ack_responder_.GetAck();
    }

    bool
    empty() const {
        return pk2offset_->empty();
    }

 public:
    ConcurrentVector<Timestamp> timestamps_;
    std::atomic<int64_t> reserved = 0;
    TimestampIndex timestamp_index_;
    std::unique_ptr<OffsetMap> pk2offset_;

    // used for preInsert of growing segment
    AckResponder ack_responder_;

 private:
    std::unordered_map<FieldId, std::unique_ptr<VectorBase>> data_{};
    std::unordered_map<FieldId, ThreadSafeValidDataPtr> valid_data_{};
    mutable std::shared_mutex shared_mutex_{};
};

// Keep the original template API via alias
template <bool is_sealed>
using InsertRecord =
    std::conditional_t<is_sealed, InsertRecordSealed, InsertRecordGrowing>;

}  // namespace milvus::segcore
