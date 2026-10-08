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

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <limits>
#include <type_traits>

#include "common.h"
#include "detail/maybe_vector.h"

namespace milvus {
namespace bitset {

namespace {

// A supporting facility for checking out of range.
// It is needed to add a capability to verify that we won't go out of
//   range even for the Release build.
template <bool RangeCheck>
struct RangeChecker {};

// disabled.
template <>
struct RangeChecker<false> {
    // Check if a < max
    template <typename SizeT>
    static inline void
    lt(const SizeT a, const SizeT max) {
    }

    // Check if a <= max
    template <typename SizeT>
    static inline void
    le(const SizeT a, const SizeT max) {
    }

    // Check if a == b
    template <typename SizeT>
    static inline void
    eq(const SizeT a, const SizeT b) {
    }
};

// enabled.
template <>
struct RangeChecker<true> {
    // Check if a < max
    template <typename SizeT>
    static inline void
    lt(const SizeT a, const SizeT max) {
        // todo: replace
        assert(a < max);
    }

    // Check if a <= max
    template <typename SizeT>
    static inline void
    le(const SizeT a, const SizeT max) {
        // todo: replace
        assert(a <= max);
    }

    // Check if a == b
    template <typename SizeT>
    static inline void
    eq(const SizeT a, const SizeT b) {
        // todo: replace
        assert(a == b);
    }
};

}  // namespace

namespace detail {
// Owned together with the bitmap storage, with a stable address across moves.
// Views only borrow this state; cache storage and invalidation stay here.
// Atomic statistics support concurrent readers. Bitmap writes still require
// external synchronization with readers and other writers.
class BitsetState {
 public:
    explicit BitsetState(size_t size) : Size(size) {
    }

    void
    Modified() {
        if (WriteDepth == 0 &&
            CachedPredicates.load(std::memory_order_relaxed) != 0) {
            ClearStatistics();
        }
    }
    void
    Escape() {
        Escaped = true;
        ClearStatistics();
    }
    void
    Resize(size_t size) {
        Modified();
        Size = size;
    }
    void
    BeginWrite() {
        Modified();
        ++WriteDepth;
    }
    void
    EndWrite() {
        assert(WriteDepth != 0);
        if (--WriteDepth == 0)
            Modified();
    }

    template <typename Policy>
    size_t
    Count(const typename Policy::data_type* data,
          size_t offset,
          size_t size) const {
        if (size == 0)
            return 0;
        if (!CanCache(offset, size))
            return Policy::op_count(data, offset, size);
        auto count = CachedCount.load(std::memory_order_relaxed);
        if (count == kUnknown) {
            count = Policy::op_count(data, offset, size);
            CachedCount.store(count, std::memory_order_relaxed);
            CachedPredicates.fetch_or(kCountKnown, std::memory_order_relaxed);
        }
        return count;
    }
    template <typename Policy>
    bool
    All(const typename Policy::data_type* data,
        size_t offset,
        size_t size) const {
        return Predicate<Policy>(data, offset, size, true);
    }
    template <typename Policy>
    bool
    None(const typename Policy::data_type* data,
         size_t offset,
         size_t size) const {
        return Predicate<Policy>(data, offset, size, false);
    }

 private:
    static constexpr size_t kUnknown = std::numeric_limits<size_t>::max();
    static constexpr uint8_t kAllKnown = 1, kAllTrue = 2, kNoneKnown = 4,
                             kNoneTrue = 8, kCountKnown = 16;
    size_t Size;
    size_t WriteDepth = 0;
    bool Escaped = false;
    mutable std::atomic<size_t> CachedCount{kUnknown};
    mutable std::atomic<uint8_t> CachedPredicates{0};

    void
    ClearStatistics() {
        CachedCount.store(kUnknown, std::memory_order_relaxed);
        CachedPredicates.store(0, std::memory_order_relaxed);
    }
    bool
    CanCache(size_t offset, size_t size) const {
        return offset == 0 && size == Size && WriteDepth == 0 && !Escaped;
    }
    template <typename Policy>
    bool
    Predicate(const typename Policy::data_type* data,
              size_t offset,
              size_t size,
              bool all) const {
        if (size == 0)
            return true;
        if (!CanCache(offset, size))
            return all ? Policy::op_all(data, offset, size)
                       : Policy::op_none(data, offset, size);
        const auto count = CachedCount.load(std::memory_order_relaxed);
        if (count != kUnknown)
            return all ? count == size : count == 0;
        const auto known = all ? kAllKnown : kNoneKnown;
        const auto value = all ? kAllTrue : kNoneTrue;
        const auto flags = CachedPredicates.load(std::memory_order_relaxed);
        if (flags & known)
            return flags & value;
        const bool result = all ? Policy::op_all(data, offset, size)
                                : Policy::op_none(data, offset, size);
        CachedPredicates.fetch_or(known | (result ? value : 0),
                                  std::memory_order_relaxed);
        if (result)
            CachedCount.store(all ? size : 0, std::memory_order_relaxed);
        return result;
    }
};

// Clear statistics on batch entry and keep reads uncached until it closes.
// Nested scopes on the same owner are supported.
class BitsetWriteScope {
    BitsetState* state_;
    void* data_;

 public:
    BitsetWriteScope(BitsetState* state, void* data)
        : state_(state), data_(data) {
        if (state_)
            state_->BeginWrite();
    }
    void*
    data() const {
        return data_;
    }
    BitsetWriteScope(const BitsetWriteScope&) = delete;
    BitsetWriteScope&
    operator=(const BitsetWriteScope&) = delete;
    ~BitsetWriteScope() {
        if (state_)
            state_->EndWrite();
    }
};

template <typename PolicyT>
class TrackedBitProxy {
    typename PolicyT::proxy_type proxy_;
    BitsetState* state_;

 public:
    TrackedBitProxy(typename PolicyT::proxy_type proxy, BitsetState* state)
        : proxy_(proxy), state_(state) {
    }
    operator bool() const {
        return bool(proxy_);
    }
    bool
    operator~() const {
        return ~proxy_;
    }
    TrackedBitProxy&
    operator=(bool value) {
        if (state_)
            state_->Modified();
        proxy_ = value;
        return *this;
    }
    TrackedBitProxy&
    operator=(const TrackedBitProxy& other) {
        return *this = bool(other);
    }
    TrackedBitProxy&
    operator|=(bool value) {
        if (value)
            set();
        return *this;
    }
    TrackedBitProxy&
    operator&=(bool value) {
        if (!value)
            reset();
        return *this;
    }
    TrackedBitProxy&
    operator^=(bool value) {
        if (value)
            flip();
        return *this;
    }
    void
    set() {
        *this = true;
    }
    void
    reset() {
        *this = false;
    }
    void
    flip() {
        *this = !bool(proxy_);
    }
};
}  // namespace detail

// CRTP

// Bitset view, which does not own the data.
template <typename PolicyT, bool IsRangeCheckEnabled>
class BitsetView;

template <typename PolicyT, bool IsRangeCheckEnabled>
using BitsetReadView = BitsetView<PolicyT, IsRangeCheckEnabled>;

template <typename PolicyT, bool IsRangeCheckEnabled>
class BitsetWriteView;

// Bitset, which owns the data.
template <typename PolicyT, typename ContainerT, bool IsRangeCheckEnabled>
class Bitset;

// Shared read operations for owners and both borrowed view types.
template <typename PolicyT, typename ImplT, bool IsRangeCheckEnabled>
class BitsetBase {
    template <typename, bool>
    friend class BitsetView;

    template <typename, typename, bool>
    friend class Bitset;

 public:
    using policy_type = PolicyT;
    using data_type = typename policy_type::data_type;
    using const_proxy_type = typename policy_type::const_proxy_type;

    using range_checker = RangeChecker<IsRangeCheckEnabled>;

    inline const data_type*
    data() const {
        return as_derived().data_impl();
    }

    // Return the number of bits we're working with.
    inline size_t
    size() const {
        return as_derived().size_impl();
    }

    // Return the number of bytes which is needed to
    //   contain all our bits.
    inline size_t
    size_in_bytes() const {
        return policy_type::get_required_size_in_bytes(this->size());
    }

    // Return the number of elements which is needed to
    //   contain all our bits.
    inline size_t
    size_in_elements() const {
        return policy_type::get_required_size_in_elements(this->size());
    }

    //
    inline bool
    empty() const {
        return (this->size() == 0);
    }

    //
    inline bool
    operator[](const size_t bit_idx) const {
        range_checker::lt(bit_idx, this->size());

        const size_t idx_v = bit_idx + this->offset();
        const auto proxy = policy_type::get_proxy(this->data(), idx_v);
        return proxy.operator bool();
    }

    // Return whether all bits are set to true.
    inline bool
    all() const {
        if (const auto* state = as_derived().mutation_state_impl())
            return state->template All<policy_type>(
                this->data(), this->offset(), this->size());
        return policy_type::op_all(this->data(), this->offset(), this->size());
    }

    // Return whether any of the bits is set to true.
    inline bool
    any() const {
        return (!this->none());
    }

    // Return whether all bits are set to false.
    inline bool
    none() const {
        if (const auto* state = as_derived().mutation_state_impl())
            return state->template None<policy_type>(
                this->data(), this->offset(), this->size());
        return policy_type::op_none(this->data(), this->offset(), this->size());
    }

    //
    inline BitsetView<PolicyT, IsRangeCheckEnabled>
    operator+(const size_t offset) const {
        return this->view(offset);
    }

    // Create a const view of a given size from the given position.
    inline BitsetView<PolicyT, IsRangeCheckEnabled>
    view(const size_t offset, const size_t size) const {
        range_checker::le(offset, this->size());
        range_checker::le(size, this->size() - offset);

        return BitsetView<PolicyT, IsRangeCheckEnabled>(
            this->data(),
            this->offset() + offset,
            size,
            as_derived().mutation_state_impl());
    }

    // Create a const view from the given position, which uses all available size.
    inline BitsetView<PolicyT, IsRangeCheckEnabled>
    view(const size_t offset) const {
        range_checker::le(offset, this->size());

        return BitsetView<PolicyT, IsRangeCheckEnabled>(
            this->data(),
            this->offset() + offset,
            this->size() - offset,
            as_derived().mutation_state_impl());
    }

    // Create a const view.
    inline BitsetView<PolicyT, IsRangeCheckEnabled>
    view() const {
        return this->view(0);
    }

    inline BitsetReadView<PolicyT, IsRangeCheckEnabled>
    read_view(const size_t begin, const size_t length) const {
        return this->view(begin, length);
    }

    inline BitsetReadView<PolicyT, IsRangeCheckEnabled>
    read_view(const size_t begin = 0) const {
        return this->view(begin);
    }

    // Return the number of bits which are set to true.
    inline size_t
    count() const {
        if (const auto* state = as_derived().mutation_state_impl())
            return state->template Count<policy_type>(
                this->data(), this->offset(), this->size());
        return policy_type::op_count(
            this->data(), this->offset(), this->size());
    }

    // Compare the current bitset with another bitset / bitset view.
    template <typename I, bool R>
    inline bool
    operator==(const BitsetBase<PolicyT, I, R>& other) const {
        if (this->size() != other.size()) {
            return false;
        }

        return policy_type::op_eq(this->data(),
                                  other.data(),
                                  this->offset(),
                                  other.offset(),
                                  this->size());
    }

    // Compare the current bitset with another bitset / bitset view.
    template <typename I, bool R>
    inline bool
    operator!=(const BitsetBase<PolicyT, I, R>& other) const {
        return (!(*this == other));
    }

    // Find the index of the first bit set to either true (default), or false.
    inline std::optional<size_t>
    find_first(const bool is_set = true) const {
        return policy_type::op_find(
            this->data(), this->offset(), this->size(), 0, is_set);
    }

    // Find the index of the first bit set to either true (default), or false, starting from a given bit index.
    inline std::optional<size_t>
    find_next(const size_t starting_bit_idx, const bool is_set = true) const {
        const size_t size_v = this->size();
        if (starting_bit_idx + 1 >= size_v) {
            return std::nullopt;
        }

        return policy_type::op_find(this->data(),
                                    this->offset(),
                                    this->size(),
                                    starting_bit_idx + 1,
                                    is_set);
    }

    // Read multiple bits starting from a given bit index.
    inline data_type
    read(const size_t starting_bit_idx, const size_t nbits) const {
        range_checker::le(nbits, 8 * sizeof(data_type));
        // Check the end bound without computing starting_bit_idx + nbits, which
        // can wrap around size_t and slip past the check; both operands are
        // unsigned so size() - starting_bit_idx is safe once the first holds.
        range_checker::le(starting_bit_idx, this->size());
        range_checker::le(nbits, this->size() - starting_bit_idx);

        return policy_type::op_read(
            this->data(), this->offset() + starting_bit_idx, nbits);
    }

    // Return the starting bit offset in our container.
    inline size_t
    offset() const {
        return as_derived().offset_impl();
    }

 protected:
    const detail::BitsetState*
    mutation_state_impl() const {
        return as_derived().mutation_state_impl();
    }

 private:
    inline const ImplT&
    as_derived() const {
        return static_cast<const ImplT&>(*this);
    }
};

namespace detail {
// Internal CRTP implementation for owners and fixed-size write views.
// Kernel offsets are local to the destination. ReadView selects source windows;
// WriteView selects destination windows without allocating another bitmap.
template <typename PolicyT, typename ImplT, bool IsRangeCheckEnabled>
class BitsetMutatingBase
    : public BitsetBase<PolicyT, ImplT, IsRangeCheckEnabled> {
 public:
    using read_base = BitsetBase<PolicyT, ImplT, IsRangeCheckEnabled>;
    using policy_type = PolicyT;
    using data_type = typename policy_type::data_type;
    using proxy_type = detail::TrackedBitProxy<PolicyT>;
    using range_checker = RangeChecker<IsRangeCheckEnabled>;
    using read_base::data;
    using read_base::operator[];

    inline BitsetWriteView<PolicyT, IsRangeCheckEnabled>
    write_view(const size_t begin, const size_t length) {
        check_range(begin, length);
        return BitsetWriteView<PolicyT, IsRangeCheckEnabled>(
            as_derived().data_impl(),
            this->offset() + begin,
            length,
            as_derived().mutation_state_impl());
    }

    inline BitsetWriteView<PolicyT, IsRangeCheckEnabled>
    write_view(const size_t begin = 0) {
        range_checker::le(begin, this->size());
        return this->write_view(begin, this->size() - begin);
    }

    // The owner and storage must outlive the scope. Do not resize or replace
    // them while writing; concurrent readers require external synchronization.
    detail::BitsetWriteScope
    scoped_write() {
        return detail::BitsetWriteScope(as_derived().mutation_state_impl(),
                                        as_derived().data_impl());
    }
    inline data_type*
    data() {
        as_derived().escape_data_impl();
        return as_derived().data_impl();
    }

    //
    inline proxy_type
    operator[](const size_t bit_idx) {
        range_checker::lt(bit_idx, this->size());

        const size_t idx_v = bit_idx + this->offset();
        return proxy_type(
            policy_type::get_proxy(as_derived().data_impl(), idx_v),
            as_derived().mutation_state_impl());
    }

    // Set all bits to true.
    inline void
    set() {
        policy_type::op_set(write_data(), this->offset(), this->size());
    }

    // Set a given bit to a given value.
    inline void
    set(const size_t bit_idx, const bool value = true) {
        this->operator[](bit_idx) = value;
    }

    // Set a given range of [a, b) bits to a given value.
    inline void
    set(const size_t bit_idx_start,
        const size_t size,
        const bool value = true) {
        check_range(bit_idx_start, size);

        policy_type::op_fill(
            write_data(), this->offset() + bit_idx_start, size, value);
    }

    // Set all bits to false.
    inline void
    reset() {
        policy_type::op_reset(write_data(), this->offset(), this->size());
    }

    // Set a given bit to false.
    inline void
    reset(const size_t bit_idx) {
        this->operator[](bit_idx) = false;
    }

    // Set a given range of [a, b) bits to false.
    inline void
    reset(const size_t bit_idx_start, const size_t size) {
        this->set(bit_idx_start, size, false);
    }

    // Inplace and.
    template <typename I, bool R>
    inline void
    inplace_and(const BitsetBase<PolicyT, I, R>& other,
                const size_t size,
                const size_t dst_offset = 0) {
        check_range(dst_offset, size);
        range_checker::le(size, other.size());

        policy_type::op_and(write_data(),
                            other.data(),
                            this->offset() + dst_offset,
                            other.offset(),
                            size);
    }

    // AND with other, then complement [dst_offset, dst_offset + size).
    // Adjacent bits are unchanged. This is NAND, not AND-NOT (inplace_sub).
    template <typename I, bool R>
    inline void
    inplace_and_flip(const BitsetBase<PolicyT, I, R>& other,
                     const size_t size,
                     const size_t dst_offset = 0) {
        check_range(dst_offset, size);
        range_checker::le(size, other.size());

        policy_type::op_and_flip(write_data(),
                                 other.data(),
                                 this->offset() + dst_offset,
                                 other.offset(),
                                 size);
    }

    template <bool R>
    inline void
    inplace_and(const BitsetView<PolicyT, R>* const others,
                const size_t n_others,
                const size_t size,
                const size_t dst_offset = 0) {
        check_range(dst_offset, size);
        for (size_t i = 0; i < n_others; i++) {
            range_checker::le(size, others[i].size());
        }

        // pick buffers
        detail::MaybeVector<const data_type*> tmp_data(n_others);
        detail::MaybeVector<size_t> tmp_offset(n_others);

        for (size_t i = 0; i < n_others; i++) {
            tmp_data[i] = others[i].data();
            tmp_offset[i] = others[i].offset();
        }

        policy_type::op_and_multiple(write_data(),
                                     tmp_data.data(),
                                     this->offset() + dst_offset,
                                     tmp_offset.data(),
                                     n_others,
                                     size);
    }

    template <bool R>
    inline void
    inplace_and(const BitsetView<PolicyT, R>* const others,
                const size_t n_others) {
        this->inplace_and(others, n_others, this->size());
    }

    template <typename ContainerT, bool R>
    inline void
    inplace_and(const Bitset<PolicyT, ContainerT, R>* const others,
                const size_t n_others,
                const size_t size,
                const size_t dst_offset = 0) {
        check_range(dst_offset, size);
        for (size_t i = 0; i < n_others; i++) {
            range_checker::le(size, others[i].size());
        }

        // pick buffers
        detail::MaybeVector<const data_type*> tmp_data(n_others);
        detail::MaybeVector<size_t> tmp_offset(n_others);

        for (size_t i = 0; i < n_others; i++) {
            tmp_data[i] = others[i].data();
            tmp_offset[i] = others[i].offset();
        }

        policy_type::op_and_multiple(write_data(),
                                     tmp_data.data(),
                                     this->offset() + dst_offset,
                                     tmp_offset.data(),
                                     n_others,
                                     size);
    }

    template <typename ContainerT, bool R>
    inline void
    inplace_and(const Bitset<PolicyT, ContainerT, R>* const others,
                const size_t n_others) {
        this->inplace_and(others, n_others, this->size());
    }

    // Inplace and. A given bitset / bitset view is expected to have the same size.
    template <typename I, bool R>
    inline ImplT&
    operator&=(const BitsetBase<PolicyT, I, R>& other) {
        range_checker::eq(other.size(), this->size());

        this->inplace_and(other, this->size());
        return as_derived();
    }

    // Inplace or.
    template <typename I, bool R>
    inline void
    inplace_or(const BitsetBase<PolicyT, I, R>& other,
               const size_t size,
               const size_t dst_offset = 0) {
        check_range(dst_offset, size);
        range_checker::le(size, other.size());

        policy_type::op_or(write_data(),
                           other.data(),
                           this->offset() + dst_offset,
                           other.offset(),
                           size);
    }

    template <bool R>
    inline void
    inplace_or(const BitsetView<PolicyT, R>* const others,
               const size_t n_others,
               const size_t size,
               const size_t dst_offset = 0) {
        check_range(dst_offset, size);
        for (size_t i = 0; i < n_others; i++) {
            range_checker::le(size, others[i].size());
        }

        // pick buffers
        detail::MaybeVector<const data_type*> tmp_data(n_others);
        detail::MaybeVector<size_t> tmp_offset(n_others);

        for (size_t i = 0; i < n_others; i++) {
            tmp_data[i] = others[i].data();
            tmp_offset[i] = others[i].offset();
        }

        policy_type::op_or_multiple(write_data(),
                                    tmp_data.data(),
                                    this->offset() + dst_offset,
                                    tmp_offset.data(),
                                    n_others,
                                    size);
    }

    template <bool R>
    inline void
    inplace_or(const BitsetView<PolicyT, R>* const others,
               const size_t n_others) {
        this->inplace_or(others, n_others, this->size());
    }

    template <typename ContainerT, bool R>
    inline void
    inplace_or(const Bitset<PolicyT, ContainerT, R>* const others,
               const size_t n_others,
               const size_t size,
               const size_t dst_offset = 0) {
        check_range(dst_offset, size);
        for (size_t i = 0; i < n_others; i++) {
            range_checker::le(size, others[i].size());
        }

        // pick buffers
        detail::MaybeVector<const data_type*> tmp_data(n_others);
        detail::MaybeVector<size_t> tmp_offset(n_others);

        for (size_t i = 0; i < n_others; i++) {
            tmp_data[i] = others[i].data();
            tmp_offset[i] = others[i].offset();
        }

        policy_type::op_or_multiple(write_data(),
                                    tmp_data.data(),
                                    this->offset() + dst_offset,
                                    tmp_offset.data(),
                                    n_others,
                                    size);
    }

    template <typename ContainerT, bool R>
    inline void
    inplace_or(const Bitset<PolicyT, ContainerT, R>* const others,
               const size_t n_others) {
        this->inplace_or(others, n_others, this->size());
    }

    // Inplace or. A given bitset / bitset view is expected to have the same size.
    template <typename I, bool R>
    inline ImplT&
    operator|=(const BitsetBase<PolicyT, I, R>& other) {
        range_checker::eq(other.size(), this->size());

        this->inplace_or(other, this->size());
        return as_derived();
    }

    // Revert all bits.
    inline void
    flip() {
        this->flip(0, this->size());
    }

    // Revert only [begin, begin + size); adjacent bits are unchanged.
    inline void
    flip(const size_t begin, const size_t size) {
        check_range(begin, size);
        policy_type::op_flip(write_data(), this->offset() + begin, size);
    }

    // Inplace xor.
    template <typename I, bool R>
    inline void
    inplace_xor(const BitsetBase<PolicyT, I, R>& other,
                const size_t size,
                const size_t dst_offset = 0) {
        check_range(dst_offset, size);
        range_checker::le(size, other.size());

        policy_type::op_xor(write_data(),
                            other.data(),
                            this->offset() + dst_offset,
                            other.offset(),
                            size);
    }

    // Inplace xor. A given bitset / bitset view is expected to have the same size.
    template <typename I, bool R>
    inline ImplT&
    operator^=(const BitsetBase<PolicyT, I, R>& other) {
        range_checker::eq(other.size(), this->size());

        this->inplace_xor(other, this->size());
        return as_derived();
    }

    // Inplace sub.
    template <typename I, bool R>
    inline void
    inplace_sub(const BitsetBase<PolicyT, I, R>& other,
                const size_t size,
                const size_t dst_offset = 0) {
        check_range(dst_offset, size);
        range_checker::le(size, other.size());

        policy_type::op_sub(write_data(),
                            other.data(),
                            this->offset() + dst_offset,
                            other.offset(),
                            size);
    }

    // Inplace sub. A given bitset / bitset view is expected to have the same size.
    template <typename I, bool R>
    inline ImplT&
    operator-=(const BitsetBase<PolicyT, I, R>& other) {
        range_checker::eq(other.size(), this->size());

        this->inplace_sub(other, this->size());
        return as_derived();
    }

    // Compare two arrays element-wise
    template <typename T, typename U>
    void
    inplace_compare_column(const T* const __restrict t,
                           const U* const __restrict u,
                           const size_t size,
                           CompareOpType op,
                           const size_t dst_offset = 0) {
        if (op == CompareOpType::EQ) {
            this->inplace_compare_column<T, U, CompareOpType::EQ>(
                t, u, size, dst_offset);
        } else if (op == CompareOpType::GE) {
            this->inplace_compare_column<T, U, CompareOpType::GE>(
                t, u, size, dst_offset);
        } else if (op == CompareOpType::GT) {
            this->inplace_compare_column<T, U, CompareOpType::GT>(
                t, u, size, dst_offset);
        } else if (op == CompareOpType::LE) {
            this->inplace_compare_column<T, U, CompareOpType::LE>(
                t, u, size, dst_offset);
        } else if (op == CompareOpType::LT) {
            this->inplace_compare_column<T, U, CompareOpType::LT>(
                t, u, size, dst_offset);
        } else if (op == CompareOpType::NE) {
            this->inplace_compare_column<T, U, CompareOpType::NE>(
                t, u, size, dst_offset);
        } else {
            // unimplemented
        }
    }

    template <typename T, typename U, CompareOpType Op>
    void
    inplace_compare_column(const T* const __restrict t,
                           const U* const __restrict u,
                           const size_t size,
                           const size_t dst_offset = 0) {
        check_range(dst_offset, size);

        policy_type::template op_compare_column<T, U, Op>(
            write_data(), this->offset() + dst_offset, t, u, size);
    }

    // Compare elements of an given array with a given value
    template <typename T>
    void
    inplace_compare_val(const T* const __restrict t,
                        const size_t size,
                        const T& value,
                        CompareOpType op,
                        const size_t dst_offset = 0) {
        if (op == CompareOpType::EQ) {
            this->inplace_compare_val<T, CompareOpType::EQ>(
                t, size, value, dst_offset);
        } else if (op == CompareOpType::GE) {
            this->inplace_compare_val<T, CompareOpType::GE>(
                t, size, value, dst_offset);
        } else if (op == CompareOpType::GT) {
            this->inplace_compare_val<T, CompareOpType::GT>(
                t, size, value, dst_offset);
        } else if (op == CompareOpType::LE) {
            this->inplace_compare_val<T, CompareOpType::LE>(
                t, size, value, dst_offset);
        } else if (op == CompareOpType::LT) {
            this->inplace_compare_val<T, CompareOpType::LT>(
                t, size, value, dst_offset);
        } else if (op == CompareOpType::NE) {
            this->inplace_compare_val<T, CompareOpType::NE>(
                t, size, value, dst_offset);
        } else {
            // unimplemented
        }
    }

    template <typename T, CompareOpType Op>
    void
    inplace_compare_val(const T* const __restrict t,
                        const size_t size,
                        const T& value,
                        const size_t dst_offset = 0) {
        check_range(dst_offset, size);

        policy_type::template op_compare_val<T, Op>(
            write_data(), this->offset() + dst_offset, t, size, value);
    }

    //
    template <typename T>
    void
    inplace_within_range_column(const T* const __restrict lower,
                                const T* const __restrict upper,
                                const T* const __restrict values,
                                const size_t size,
                                const RangeType op,
                                const size_t dst_offset = 0) {
        if (op == RangeType::IncInc) {
            this->inplace_within_range_column<T, RangeType::IncInc>(
                lower, upper, values, size, dst_offset);
        } else if (op == RangeType::IncExc) {
            this->inplace_within_range_column<T, RangeType::IncExc>(
                lower, upper, values, size, dst_offset);
        } else if (op == RangeType::ExcInc) {
            this->inplace_within_range_column<T, RangeType::ExcInc>(
                lower, upper, values, size, dst_offset);
        } else if (op == RangeType::ExcExc) {
            this->inplace_within_range_column<T, RangeType::ExcExc>(
                lower, upper, values, size, dst_offset);
        } else {
            // unimplemented
        }
    }

    template <typename T, RangeType Op>
    void
    inplace_within_range_column(const T* const __restrict lower,
                                const T* const __restrict upper,
                                const T* const __restrict values,
                                const size_t size,
                                const size_t dst_offset = 0) {
        check_range(dst_offset, size);

        policy_type::template op_within_range_column<T, Op>(
            write_data(),
            this->offset() + dst_offset,
            lower,
            upper,
            values,
            size);
    }

    //
    template <typename T>
    void
    inplace_within_range_val(const T& lower,
                             const T& upper,
                             const T* const __restrict values,
                             const size_t size,
                             const RangeType op,
                             const size_t dst_offset = 0) {
        if (op == RangeType::IncInc) {
            this->inplace_within_range_val<T, RangeType::IncInc>(
                lower, upper, values, size, dst_offset);
        } else if (op == RangeType::IncExc) {
            this->inplace_within_range_val<T, RangeType::IncExc>(
                lower, upper, values, size, dst_offset);
        } else if (op == RangeType::ExcInc) {
            this->inplace_within_range_val<T, RangeType::ExcInc>(
                lower, upper, values, size, dst_offset);
        } else if (op == RangeType::ExcExc) {
            this->inplace_within_range_val<T, RangeType::ExcExc>(
                lower, upper, values, size, dst_offset);
        } else {
            // unimplemented
        }
    }

    template <typename T, RangeType Op>
    void
    inplace_within_range_val(const T& lower,
                             const T& upper,
                             const T* const __restrict values,
                             const size_t size,
                             const size_t dst_offset = 0) {
        check_range(dst_offset, size);

        policy_type::template op_within_range_val<T, Op>(
            write_data(),
            this->offset() + dst_offset,
            lower,
            upper,
            values,
            size);
    }

    //
    template <typename T>
    void
    inplace_arith_compare(const T* const __restrict src,
                          const ArithHighPrecisionType<T>& right_operand,
                          const ArithHighPrecisionType<T>& value,
                          const size_t size,
                          const ArithOpType a_op,
                          const CompareOpType cmp_op,
                          const size_t dst_offset = 0) {
        if (a_op == ArithOpType::Add) {
            if (cmp_op == CompareOpType::EQ) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Add,
                                            CompareOpType::EQ>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::GE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Add,
                                            CompareOpType::GE>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::GT) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Add,
                                            CompareOpType::GT>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::LE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Add,
                                            CompareOpType::LE>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::LT) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Add,
                                            CompareOpType::LT>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::NE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Add,
                                            CompareOpType::NE>(
                    src, right_operand, value, size, dst_offset);
            } else {
                // unimplemented
            }
        } else if (a_op == ArithOpType::Sub) {
            if (cmp_op == CompareOpType::EQ) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Sub,
                                            CompareOpType::EQ>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::GE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Sub,
                                            CompareOpType::GE>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::GT) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Sub,
                                            CompareOpType::GT>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::LE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Sub,
                                            CompareOpType::LE>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::LT) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Sub,
                                            CompareOpType::LT>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::NE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Sub,
                                            CompareOpType::NE>(
                    src, right_operand, value, size, dst_offset);
            } else {
                // unimplemented
            }
        } else if (a_op == ArithOpType::Mul) {
            if (cmp_op == CompareOpType::EQ) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Mul,
                                            CompareOpType::EQ>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::GE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Mul,
                                            CompareOpType::GE>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::GT) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Mul,
                                            CompareOpType::GT>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::LE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Mul,
                                            CompareOpType::LE>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::LT) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Mul,
                                            CompareOpType::LT>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::NE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Mul,
                                            CompareOpType::NE>(
                    src, right_operand, value, size, dst_offset);
            } else {
                // unimplemented
            }
        } else if (a_op == ArithOpType::Div) {
            if (cmp_op == CompareOpType::EQ) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Div,
                                            CompareOpType::EQ>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::GE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Div,
                                            CompareOpType::GE>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::GT) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Div,
                                            CompareOpType::GT>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::LE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Div,
                                            CompareOpType::LE>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::LT) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Div,
                                            CompareOpType::LT>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::NE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Div,
                                            CompareOpType::NE>(
                    src, right_operand, value, size, dst_offset);
            } else {
                // unimplemented
            }
        } else if (a_op == ArithOpType::Mod) {
            if (cmp_op == CompareOpType::EQ) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Mod,
                                            CompareOpType::EQ>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::GE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Mod,
                                            CompareOpType::GE>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::GT) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Mod,
                                            CompareOpType::GT>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::LE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Mod,
                                            CompareOpType::LE>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::LT) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Mod,
                                            CompareOpType::LT>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::NE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Mod,
                                            CompareOpType::NE>(
                    src, right_operand, value, size, dst_offset);
            } else {
                // unimplemented
            }
        } else if (a_op == ArithOpType::BitAnd) {
            if (cmp_op == CompareOpType::EQ) {
                this->inplace_arith_compare<T,
                                            ArithOpType::BitAnd,
                                            CompareOpType::EQ>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::GE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::BitAnd,
                                            CompareOpType::GE>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::GT) {
                this->inplace_arith_compare<T,
                                            ArithOpType::BitAnd,
                                            CompareOpType::GT>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::LE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::BitAnd,
                                            CompareOpType::LE>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::LT) {
                this->inplace_arith_compare<T,
                                            ArithOpType::BitAnd,
                                            CompareOpType::LT>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::NE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::BitAnd,
                                            CompareOpType::NE>(
                    src, right_operand, value, size, dst_offset);
            } else {
                // unimplemented
            }
        } else if (a_op == ArithOpType::BitOr) {
            if (cmp_op == CompareOpType::EQ) {
                this->inplace_arith_compare<T,
                                            ArithOpType::BitOr,
                                            CompareOpType::EQ>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::GE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::BitOr,
                                            CompareOpType::GE>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::GT) {
                this->inplace_arith_compare<T,
                                            ArithOpType::BitOr,
                                            CompareOpType::GT>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::LE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::BitOr,
                                            CompareOpType::LE>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::LT) {
                this->inplace_arith_compare<T,
                                            ArithOpType::BitOr,
                                            CompareOpType::LT>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::NE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::BitOr,
                                            CompareOpType::NE>(
                    src, right_operand, value, size, dst_offset);
            } else {
                // unimplemented
            }
        } else if (a_op == ArithOpType::BitXor) {
            if (cmp_op == CompareOpType::EQ) {
                this->inplace_arith_compare<T,
                                            ArithOpType::BitXor,
                                            CompareOpType::EQ>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::GE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::BitXor,
                                            CompareOpType::GE>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::GT) {
                this->inplace_arith_compare<T,
                                            ArithOpType::BitXor,
                                            CompareOpType::GT>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::LE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::BitXor,
                                            CompareOpType::LE>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::LT) {
                this->inplace_arith_compare<T,
                                            ArithOpType::BitXor,
                                            CompareOpType::LT>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::NE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::BitXor,
                                            CompareOpType::NE>(
                    src, right_operand, value, size, dst_offset);
            } else {
                // unimplemented
            }
        } else if (a_op == ArithOpType::Shl) {
            if (cmp_op == CompareOpType::EQ) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Shl,
                                            CompareOpType::EQ>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::GE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Shl,
                                            CompareOpType::GE>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::GT) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Shl,
                                            CompareOpType::GT>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::LE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Shl,
                                            CompareOpType::LE>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::LT) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Shl,
                                            CompareOpType::LT>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::NE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Shl,
                                            CompareOpType::NE>(
                    src, right_operand, value, size, dst_offset);
            } else {
                // unimplemented
            }
        } else if (a_op == ArithOpType::Shr) {
            if (cmp_op == CompareOpType::EQ) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Shr,
                                            CompareOpType::EQ>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::GE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Shr,
                                            CompareOpType::GE>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::GT) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Shr,
                                            CompareOpType::GT>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::LE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Shr,
                                            CompareOpType::LE>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::LT) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Shr,
                                            CompareOpType::LT>(
                    src, right_operand, value, size, dst_offset);
            } else if (cmp_op == CompareOpType::NE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Shr,
                                            CompareOpType::NE>(
                    src, right_operand, value, size, dst_offset);
            } else {
                // unimplemented
            }
        } else {
            // unimplemented
        }
    }

    template <typename T, ArithOpType AOp, CompareOpType CmpOp>
    void
    inplace_arith_compare(const T* const __restrict src,
                          const ArithHighPrecisionType<T>& right_operand,
                          const ArithHighPrecisionType<T>& value,
                          const size_t size,
                          const size_t dst_offset = 0) {
        check_range(dst_offset, size);

        policy_type::template op_arith_compare<T, AOp, CmpOp>(
            write_data(),
            this->offset() + dst_offset,
            src,
            right_operand,
            value,
            size);
    }

    //
    // Inplace and. Also, counts the number of active bits.
    template <typename I, bool R>
    inline size_t
    inplace_and_with_count(const BitsetBase<PolicyT, I, R>& other,
                           const size_t size,
                           const size_t dst_offset = 0) {
        check_range(dst_offset, size);
        range_checker::le(size, other.size());

        return policy_type::op_and_with_count(write_data(),
                                              other.data(),
                                              this->offset() + dst_offset,
                                              other.offset(),
                                              size);
    }

    // Inplace or. Also, counts the number of inactive bits.
    template <typename I, bool R>
    inline size_t
    inplace_or_with_count(const BitsetBase<PolicyT, I, R>& other,
                          const size_t size,
                          const size_t dst_offset = 0) {
        check_range(dst_offset, size);
        range_checker::le(size, other.size());

        return policy_type::op_or_with_count(write_data(),
                                             other.data(),
                                             this->offset() + dst_offset,
                                             other.offset(),
                                             size);
    }

 private:
    inline data_type*
    write_data() {
        as_derived().modified_impl();
        return as_derived().data_impl();
    }

    inline void
    check_range(size_t begin, size_t length) const {
        range_checker::le(begin, this->size());
        range_checker::le(length, this->size() - begin);
    }

    inline ImplT&
    as_derived() {
        return static_cast<ImplT&>(*this);
    }
};
}  // namespace detail

// A non-owning writable interval. Its offset is applied by the shared kernels
// and tracked bit proxies. It cannot resize, reserve, append or replace storage.
// Writes are reported to the stable owner state; this view owns no statistics.
template <typename PolicyT, bool IsRangeCheckEnabled>
class BitsetWriteView : public detail::BitsetMutatingBase<
                            PolicyT,
                            BitsetWriteView<PolicyT, IsRangeCheckEnabled>,
                            IsRangeCheckEnabled> {
    template <typename, typename, bool>
    friend class BitsetBase;
    template <typename, typename, bool>
    friend class detail::BitsetMutatingBase;

 public:
    using policy_type = PolicyT;
    using data_type = typename policy_type::data_type;
    using proxy_type = detail::TrackedBitProxy<PolicyT>;
    using const_proxy_type = typename policy_type::const_proxy_type;
    using read_base = BitsetBase<PolicyT,
                                 BitsetWriteView<PolicyT, IsRangeCheckEnabled>,
                                 IsRangeCheckEnabled>;
    using read_base::operator+;

    BitsetWriteView() = default;

    BitsetWriteView
    operator+(const size_t begin) {
        return this->write_view(begin);
    }

    template <typename ContainerT, bool R>
    BitsetWriteView(Bitset<PolicyT, ContainerT, R>& owner)
        : Data(owner.data_impl()),
          Size(owner.size()),
          State(owner.mutation_state_impl()) {
    }

 private:
    BitsetWriteView(data_type* data,
                    size_t offset,
                    size_t size,
                    detail::BitsetState* state)
        : Data(data), Size(size), Offset(offset), State(state) {
    }

    data_type* Data = nullptr;
    size_t Size = 0;
    size_t Offset = 0;
    detail::BitsetState* State = nullptr;

    data_type*
    data_impl() {
        return Data;
    }
    const data_type*
    data_impl() const {
        return Data;
    }
    size_t
    size_impl() const {
        return Size;
    }
    size_t
    offset_impl() const {
        return Offset;
    }
    detail::BitsetState*
    mutation_state_impl() {
        return State;
    }
    const detail::BitsetState*
    mutation_state_impl() const {
        return State;
    }
    void
    modified_impl() {
        if (State)
            State->Modified();
    }
    void
    escape_data_impl() {
        if (State)
            State->Escape();
    }
};

// Non-owning read-only view. Owner writes remain observable. The backing
// storage and the referenced range must remain alive and valid; operations
// such as resize, reserve, clear, append, move assignment, and destruction can
// invalidate views. Statistics are delegated to the owner; this descriptor
// stores no cache and performs no invalidation. Raw borrowed buffers and partial
// intervals are scanned. Owner writes require external synchronization.
template <typename PolicyT, bool IsRangeCheckEnabled>
class BitsetView : public BitsetBase<PolicyT,
                                     BitsetView<PolicyT, IsRangeCheckEnabled>,
                                     IsRangeCheckEnabled> {
    template <typename, typename, bool>
    friend class BitsetBase;

 public:
    using policy_type = PolicyT;
    using data_type = typename policy_type::data_type;
    using const_proxy_type = typename policy_type::const_proxy_type;

    using range_checker = RangeChecker<IsRangeCheckEnabled>;

    BitsetView() = default;

    template <typename ImplT, bool R>
    BitsetView(const BitsetBase<PolicyT, ImplT, R>& bitset)
        : Data{bitset.data()},
          Size{bitset.size()},
          Offset{bitset.offset()},
          State{bitset.mutation_state_impl()} {
    }

    BitsetView(const void* data, const size_t size)
        : Data{reinterpret_cast<const data_type*>(data)}, Size{size} {
    }

    BitsetView(const void* data, const size_t offset, const size_t size)
        : Data{reinterpret_cast<const data_type*>(data)},
          Size{size},
          Offset{offset} {
    }

 private:
    BitsetView(const void* data,
               size_t offset,
               size_t size,
               const detail::BitsetState* state)
        : Data{reinterpret_cast<const data_type*>(data)},
          Size{size},
          Offset{offset},
          State{state} {
    }

    const detail::BitsetState* State = nullptr;

    const detail::BitsetState*
    mutation_state_impl() const {
        return State;
    }

    // the referenced bits are [Offset, Offset + Size)
    const data_type* Data = nullptr;
    // measured in bits
    size_t Size = 0;
    // measured in bits
    size_t Offset = 0;

    inline const data_type*
    data_impl() const {
        return Data;
    }
    inline size_t
    size_impl() const {
        return Size;
    }
    inline size_t
    offset_impl() const {
        return Offset;
    }
};

// Bitset
template <typename PolicyT, typename ContainerT, bool IsRangeCheckEnabled>
class Bitset : public detail::BitsetMutatingBase<
                   PolicyT,
                   Bitset<PolicyT, ContainerT, IsRangeCheckEnabled>,
                   IsRangeCheckEnabled> {
    friend class BitsetBase<PolicyT,
                            Bitset<PolicyT, ContainerT, IsRangeCheckEnabled>,
                            IsRangeCheckEnabled>;
    friend class detail::BitsetMutatingBase<
        PolicyT,
        Bitset<PolicyT, ContainerT, IsRangeCheckEnabled>,
        IsRangeCheckEnabled>;
    template <typename, bool>
    friend class BitsetWriteView;

 public:
    using policy_type = PolicyT;
    using data_type = typename policy_type::data_type;
    using proxy_type = detail::TrackedBitProxy<PolicyT>;
    using const_proxy_type = typename policy_type::const_proxy_type;

    using view_type = BitsetView<PolicyT, IsRangeCheckEnabled>;
    using read_view_type = BitsetReadView<PolicyT, IsRangeCheckEnabled>;
    using write_view_type = BitsetWriteView<PolicyT, IsRangeCheckEnabled>;

    // This is the container type.
    using container_type = ContainerT;
    // This is how the data is stored. For example, we may operate using
    //   uint64_t values, but store the data in std::vector<uint8_t> container.
    //   This is useful if we need to convert a bitset into a container
    //   using move operator.
    using container_data_type = typename container_type::value_type;

    using range_checker = RangeChecker<IsRangeCheckEnabled>;

    // Allocate an empty one.
    Bitset() = default;
    // Allocate the given number of bits.
    explicit Bitset(const size_t size)
        : Data(get_required_size_in_container_elements(size)),
          Size{size},
          State(size ? std::make_unique<detail::BitsetState>(size) : nullptr) {
    }
    // Allocate the given number of bits, initialize with a given value.
    Bitset(const size_t size, const bool init)
        : Data(get_required_size_in_container_elements(size),
               init ? static_cast<container_data_type>(data_type(-1))
                    : container_data_type(0)),
          Size{size},
          State(size ? std::make_unique<detail::BitsetState>(size) : nullptr) {
    }
    // Do not allow implicit copies (Rust style).
    Bitset(const Bitset&) = delete;
    // Allow default move.
    Bitset(Bitset&& other) noexcept(
        std::is_nothrow_move_constructible_v<container_type>)
        : Data(std::move(other.Data)),
          Size(other.Size),
          State(std::move(other.State)) {
        other.Size = 0;
    }
    // Do not allow implicit copies (Rust style).
    Bitset&
    operator=(const Bitset&) = delete;
    // Allow default move.
    Bitset&
    operator=(Bitset&& other) noexcept(
        std::is_nothrow_move_assignable_v<container_type>) {
        if (this != &other) {
            Data = std::move(other.Data);
            Size = other.Size;
            State = std::move(other.State);
            other.Size = 0;
        }
        return *this;
    }

    template <typename C, bool R>
    explicit Bitset(const BitsetBase<PolicyT, C, R>& other) {
        Data = container_type(
            get_required_size_in_container_elements(other.size()));
        Size = other.size();
        if (Size)
            State = std::make_unique<detail::BitsetState>(Size);

        policy_type::op_copy(other.data(),
                             other.offset(),
                             this->data_impl(),
                             this->offset(),
                             other.size());
    }

    // Clone a current bitset (Rust style).
    Bitset
    clone() const {
        Bitset cloned;
        cloned.Data = Data;
        cloned.Size = Size;
        if (Size)
            cloned.State = std::make_unique<detail::BitsetState>(Size);
        return cloned;
    }

    // Rust style.
    inline container_type
    into() && {
        escape_data_impl();
        return std::move(this->Data);
    }

    // Resize.
    void
    resize(const size_t new_size) {
        const size_t new_size_in_container_elements =
            get_required_size_in_container_elements(new_size);
        modified_impl();
        if (new_size && !State)
            State = std::make_unique<detail::BitsetState>(new_size);
        Data.resize(new_size_in_container_elements);
        Size = new_size;
        if (State)
            State->Resize(Size);
    }

    // Resize and initialize new bits with a given value if grown.
    void
    resize(const size_t new_size, const bool init) {
        const size_t old_size = this->size();
        this->resize(new_size);

        if (new_size > old_size) {
            policy_type::op_fill(
                this->data_impl(), old_size, new_size - old_size, init);
        }
    }

    // Append data from another bitset / bitset view in
    //   [starting_bit_idx, starting_bit_idx + count) range
    //   to the end of this bitset.
    template <typename I, bool R>
    void
    append(const BitsetBase<PolicyT, I, R>& other,
           const size_t starting_bit_idx,
           const size_t count) {
        range_checker::le(starting_bit_idx, other.size());
        range_checker::le(count, other.size() - starting_bit_idx);
        if (count == 0)
            return;
        // A source view can borrow our buffer, which resize may relocate.
        const auto source = reinterpret_cast<uintptr_t>(other.data());
        const auto begin = reinterpret_cast<uintptr_t>(this->data_impl());
        const auto bytes = Data.size() * sizeof(container_data_type);
        if (source >= begin && source - begin < bytes) {
            const Bitset packed(other.view(starting_bit_idx, count));
            append(packed, 0, count);
            return;
        }

        const size_t old_size = this->size();
        this->resize(this->size() + count);

        policy_type::op_copy(other.data(),
                             other.offset() + starting_bit_idx,
                             this->data_impl(),
                             this->offset() + old_size,
                             count);
    }

    // Append data from another bitset / bitset view
    //   to the end of this bitset.
    template <typename I, bool R>
    void
    append(const BitsetBase<PolicyT, I, R>& other) {
        this->append(other, 0, other.size());
    }

    // Make bitset empty.
    inline void
    clear() {
        modified_impl();
        Data.clear();
        Size = 0;
        if (State)
            State->Resize(0);
    }

    // Reserve
    inline void
    reserve(const size_t capacity) {
        const size_t capacity_in_container_elements =
            get_required_size_in_container_elements(capacity);
        modified_impl();
        Data.reserve(capacity_in_container_elements);
    }

    // Return a new bitset, equal to a | b
    template <typename I1, bool R1, typename I2, bool R2>
    friend Bitset
    operator|(const BitsetBase<PolicyT, I1, R1>& a,
              const BitsetBase<PolicyT, I2, R2>& b) {
        Bitset clone(a);
        return std::move(clone |= b);
    }

    // Return a new bitset, equal to a - b
    template <typename I1, bool R1, typename I2, bool R2>
    friend Bitset
    operator-(const BitsetBase<PolicyT, I1, R1>& a,
              const BitsetBase<PolicyT, I2, R2>& b) {
        Bitset clone(a);
        return std::move(clone -= b);
    }

 protected:
    // the container
    container_type Data;
    // the actual number of bits
    size_t Size = 0;
    std::unique_ptr<detail::BitsetState> State;

    detail::BitsetState*
    mutation_state_impl() {
        return State.get();
    }
    const detail::BitsetState*
    mutation_state_impl() const {
        return State.get();
    }
    void
    modified_impl() {
        if (State)
            State->Modified();
    }
    void
    escape_data_impl() {
        if (State)
            State->Escape();
    }

    inline data_type*
    data_impl() {
        return reinterpret_cast<data_type*>(Data.data());
    }
    inline const data_type*
    data_impl() const {
        return reinterpret_cast<const data_type*>(Data.data());
    }
    inline size_t
    size_impl() const {
        return Size;
    }
    inline size_t
    offset_impl() const {
        return 0;
    }

    //
    static inline size_t
    get_required_size_in_container_elements(const size_t size) {
        const size_t size_in_bytes =
            policy_type::get_required_size_in_bytes(size);
        return (size_in_bytes + sizeof(container_data_type) - 1) /
               sizeof(container_data_type);
    }
};

}  // namespace bitset
}  // namespace milvus
