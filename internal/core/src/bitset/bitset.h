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

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <optional>
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

// CRTP

// Bitset view, which does not own the data.
template <typename PolicyT, bool IsRangeCheckEnabled>
class BitsetView;

// Bitset, which owns the data.
template <typename PolicyT, typename ContainerT, bool IsRangeCheckEnabled>
class Bitset;

// This is the base CRTP class.
template <typename PolicyT, typename ImplT, bool IsRangeCheckEnabled>
class BitsetBase {
    template <typename, bool>
    friend class BitsetView;

    template <typename, typename, bool>
    friend class Bitset;

 public:
    using policy_type = PolicyT;
    using data_type = typename policy_type::data_type;
    using proxy_type = typename policy_type::proxy_type;
    using const_proxy_type = typename policy_type::const_proxy_type;

    using range_checker = RangeChecker<IsRangeCheckEnabled>;

    //
    inline data_type*
    data() {
        return as_derived().data_impl();
    }

    //
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
    inline proxy_type
    operator[](const size_t bit_idx) {
        range_checker::lt(bit_idx, this->size());

        const size_t idx_v = bit_idx + this->offset();
        return policy_type::get_proxy(this->data(), idx_v);
    }

    //
    inline bool
    operator[](const size_t bit_idx) const {
        range_checker::lt(bit_idx, this->size());

        const size_t idx_v = bit_idx + this->offset();
        const auto proxy = policy_type::get_proxy(this->data(), idx_v);
        return proxy.operator bool();
    }

    // Set all bits to true.
    inline void
    set() {
        policy_type::op_set(this->data(), this->offset(), this->size());
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
        range_checker::le(bit_idx_start + size, this->size());

        policy_type::op_fill(
            this->data(), this->offset() + bit_idx_start, size, value);
    }

    // Set all bits to false.
    inline void
    reset() {
        policy_type::op_reset(this->data(), this->offset(), this->size());
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

    // Return whether all bits are set to true.
    inline bool
    all() const {
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
        return policy_type::op_none(this->data(), this->offset(), this->size());
    }

    // Inplace and.
    template <typename I, bool R>
    inline void
    inplace_and(const BitsetBase<PolicyT, I, R>& other, const size_t size) {
        range_checker::le(size, this->size());
        range_checker::le(size, other.size());

        policy_type::op_and(
            this->data(), other.data(), this->offset(), other.offset(), size);
    }

    template <bool R>
    inline void
    inplace_and(const BitsetView<PolicyT, R>* const others,
                const size_t n_others,
                const size_t size) {
        range_checker::le(size, this->size());
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

        policy_type::op_and_multiple(this->data(),
                                     tmp_data.data(),
                                     this->offset(),
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
                const size_t size) {
        range_checker::le(size, this->size());
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

        policy_type::op_and_multiple(this->data(),
                                     tmp_data.data(),
                                     this->offset(),
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
    inplace_or(const BitsetBase<PolicyT, I, R>& other, const size_t size) {
        range_checker::le(size, this->size());
        range_checker::le(size, other.size());

        policy_type::op_or(
            this->data(), other.data(), this->offset(), other.offset(), size);
    }

    template <bool R>
    inline void
    inplace_or(const BitsetView<PolicyT, R>* const others,
               const size_t n_others,
               const size_t size) {
        range_checker::le(size, this->size());
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

        policy_type::op_or_multiple(this->data(),
                                    tmp_data.data(),
                                    this->offset(),
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
               const size_t size) {
        range_checker::le(size, this->size());
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

        policy_type::op_or_multiple(this->data(),
                                    tmp_data.data(),
                                    this->offset(),
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
        policy_type::op_flip(this->data(), this->offset(), this->size());
    }

    //
    inline BitsetView<PolicyT, IsRangeCheckEnabled>
    operator+(const size_t offset) {
        return this->view(offset);
    }

    // Create a view of a given size from the given position.
    inline BitsetView<PolicyT, IsRangeCheckEnabled>
    view(const size_t offset, const size_t size) {
        range_checker::le(offset, this->size());
        range_checker::le(offset + size, this->size());

        return BitsetView<PolicyT, IsRangeCheckEnabled>(
            this->data(), this->offset() + offset, size);
    }

    // Create a const view of a given size from the given position.
    inline BitsetView<PolicyT, IsRangeCheckEnabled>
    view(const size_t offset, const size_t size) const {
        range_checker::le(offset, this->size());
        range_checker::le(offset + size, this->size());

        return BitsetView<PolicyT, IsRangeCheckEnabled>(
            const_cast<data_type*>(this->data()),
            this->offset() + offset,
            size);
    }

    // Create a view from the given position, which uses all available size.
    inline BitsetView<PolicyT, IsRangeCheckEnabled>
    view(const size_t offset) {
        range_checker::le(offset, this->size());

        return BitsetView<PolicyT, IsRangeCheckEnabled>(
            this->data(), this->offset() + offset, this->size() - offset);
    }

    // Create a const view from the given position, which uses all available size.
    inline const BitsetView<PolicyT, IsRangeCheckEnabled>
    view(const size_t offset) const {
        range_checker::le(offset, this->size());

        return BitsetView<PolicyT, IsRangeCheckEnabled>(
            const_cast<data_type*>(this->data()),
            this->offset() + offset,
            this->size() - offset);
    }

    // Create a view.
    inline BitsetView<PolicyT, IsRangeCheckEnabled>
    view() {
        return this->view(0);
    }

    // Create a const view.
    inline const BitsetView<PolicyT, IsRangeCheckEnabled>
    view() const {
        return this->view(0);
    }

    // Return the number of bits which are set to true.
    inline size_t
    count() const {
        return policy_type::op_count(
            this->data(), this->offset(), this->size());
    }

    // Compare the current bitset with another bitset / bitset view.
    template <typename I, bool R>
    inline bool
    operator==(const BitsetBase<PolicyT, I, R>& other) {
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
    operator!=(const BitsetBase<PolicyT, I, R>& other) {
        return (!(*this == other));
    }

    // Inplace xor.
    template <typename I, bool R>
    inline void
    inplace_xor(const BitsetBase<PolicyT, I, R>& other, const size_t size) {
        range_checker::le(size, this->size());
        range_checker::le(size, other.size());

        policy_type::op_xor(
            this->data(), other.data(), this->offset(), other.offset(), size);
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
    inplace_sub(const BitsetBase<PolicyT, I, R>& other, const size_t size) {
        range_checker::le(size, this->size());
        range_checker::le(size, other.size());

        policy_type::op_sub(
            this->data(), other.data(), this->offset(), other.offset(), size);
    }

    // Inplace sub. A given bitset / bitset view is expected to have the same size.
    template <typename I, bool R>
    inline ImplT&
    operator-=(const BitsetBase<PolicyT, I, R>& other) {
        range_checker::eq(other.size(), this->size());

        this->inplace_sub(other, this->size());
        return as_derived();
    }

    // Find the index of the first bit set to true.
    inline std::optional<size_t>
    find_first() const {
        return policy_type::op_find(
            this->data(), this->offset(), this->size(), 0);
    }

    // Find the index of the first bit set to true, starting from a given bit index.
    inline std::optional<size_t>
    find_next(const size_t starting_bit_idx) const {
        const size_t size_v = this->size();
        if (starting_bit_idx + 1 >= size_v) {
            return std::nullopt;
        }

        return policy_type::op_find(
            this->data(), this->offset(), this->size(), starting_bit_idx + 1);
    }

    // Read multiple bits starting from a given bit index.
    inline data_type
    read(const size_t starting_bit_idx, const size_t nbits) {
        range_checker::le(nbits, sizeof(data_type));

        return policy_type::op_read(
            this->data(), this->offset() + starting_bit_idx, nbits);
    }

    // Write multiple bits starting from a given bit index.
    inline void
    write(const size_t starting_bit_idx,
          const data_type value,
          const size_t nbits) {
        range_checker::le(nbits, sizeof(data_type));

        policy_type::op_write(
            this->data(), this->offset() + starting_bit_idx, nbits, value);
    }

    // Compare two arrays element-wise
    template <typename T, typename U>
    void
    inplace_compare_column(const T* const __restrict t,
                           const U* const __restrict u,
                           const size_t size,
                           CompareOpType op) {
        if (op == CompareOpType::EQ) {
            this->inplace_compare_column<T, U, CompareOpType::EQ>(t, u, size);
        } else if (op == CompareOpType::GE) {
            this->inplace_compare_column<T, U, CompareOpType::GE>(t, u, size);
        } else if (op == CompareOpType::GT) {
            this->inplace_compare_column<T, U, CompareOpType::GT>(t, u, size);
        } else if (op == CompareOpType::LE) {
            this->inplace_compare_column<T, U, CompareOpType::LE>(t, u, size);
        } else if (op == CompareOpType::LT) {
            this->inplace_compare_column<T, U, CompareOpType::LT>(t, u, size);
        } else if (op == CompareOpType::NE) {
            this->inplace_compare_column<T, U, CompareOpType::NE>(t, u, size);
        } else {
            // unimplemented
        }
    }

    template <typename T, typename U, CompareOpType Op>
    void
    inplace_compare_column(const T* const __restrict t,
                           const U* const __restrict u,
                           const size_t size) {
        range_checker::le(size, this->size());

        policy_type::template op_compare_column<T, U, Op>(
            this->data(), this->offset(), t, u, size);
    }

    // Compare elements of an given array with a given value
    template <typename T>
    void
    inplace_compare_val(const T* const __restrict t,
                        const size_t size,
                        const T& value,
                        CompareOpType op) {
        if (op == CompareOpType::EQ) {
            this->inplace_compare_val<T, CompareOpType::EQ>(t, size, value);
        } else if (op == CompareOpType::GE) {
            this->inplace_compare_val<T, CompareOpType::GE>(t, size, value);
        } else if (op == CompareOpType::GT) {
            this->inplace_compare_val<T, CompareOpType::GT>(t, size, value);
        } else if (op == CompareOpType::LE) {
            this->inplace_compare_val<T, CompareOpType::LE>(t, size, value);
        } else if (op == CompareOpType::LT) {
            this->inplace_compare_val<T, CompareOpType::LT>(t, size, value);
        } else if (op == CompareOpType::NE) {
            this->inplace_compare_val<T, CompareOpType::NE>(t, size, value);
        } else {
            // unimplemented
        }
    }

    template <typename T, CompareOpType Op>
    void
    inplace_compare_val(const T* const __restrict t,
                        const size_t size,
                        const T& value) {
        range_checker::le(size, this->size());

        policy_type::template op_compare_val<T, Op>(
            this->data(), this->offset(), t, size, value);
    }

    //
    template <typename T>
    void
    inplace_within_range_column(const T* const __restrict lower,
                                const T* const __restrict upper,
                                const T* const __restrict values,
                                const size_t size,
                                const RangeType op) {
        if (op == RangeType::IncInc) {
            this->inplace_within_range_column<T, RangeType::IncInc>(
                lower, upper, values, size);
        } else if (op == RangeType::IncExc) {
            this->inplace_within_range_column<T, RangeType::IncExc>(
                lower, upper, values, size);
        } else if (op == RangeType::ExcInc) {
            this->inplace_within_range_column<T, RangeType::ExcInc>(
                lower, upper, values, size);
        } else if (op == RangeType::ExcExc) {
            this->inplace_within_range_column<T, RangeType::ExcExc>(
                lower, upper, values, size);
        } else {
            // unimplemented
        }
    }

    template <typename T, RangeType Op>
    void
    inplace_within_range_column(const T* const __restrict lower,
                                const T* const __restrict upper,
                                const T* const __restrict values,
                                const size_t size) {
        range_checker::le(size, this->size());

        policy_type::template op_within_range_column<T, Op>(
            this->data(), this->offset(), lower, upper, values, size);
    }

    //
    template <typename T>
    void
    inplace_within_range_val(const T& lower,
                             const T& upper,
                             const T* const __restrict values,
                             const size_t size,
                             const RangeType op) {
        if (op == RangeType::IncInc) {
            this->inplace_within_range_val<T, RangeType::IncInc>(
                lower, upper, values, size);
        } else if (op == RangeType::IncExc) {
            this->inplace_within_range_val<T, RangeType::IncExc>(
                lower, upper, values, size);
        } else if (op == RangeType::ExcInc) {
            this->inplace_within_range_val<T, RangeType::ExcInc>(
                lower, upper, values, size);
        } else if (op == RangeType::ExcExc) {
            this->inplace_within_range_val<T, RangeType::ExcExc>(
                lower, upper, values, size);
        } else {
            // unimplemented
        }
    }

    template <typename T, RangeType Op>
    void
    inplace_within_range_val(const T& lower,
                             const T& upper,
                             const T* const __restrict values,
                             const size_t size) {
        range_checker::le(size, this->size());

        policy_type::template op_within_range_val<T, Op>(
            this->data(), this->offset(), lower, upper, values, size);
    }

    //
    template <typename T>
    void
    inplace_arith_compare(const T* const __restrict src,
                          const ArithHighPrecisionType<T>& right_operand,
                          const ArithHighPrecisionType<T>& value,
                          const size_t size,
                          const ArithOpType a_op,
                          const CompareOpType cmp_op) {
        if (a_op == ArithOpType::Add) {
            if (cmp_op == CompareOpType::EQ) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Add,
                                            CompareOpType::EQ>(
                    src, right_operand, value, size);
            } else if (cmp_op == CompareOpType::GE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Add,
                                            CompareOpType::GE>(
                    src, right_operand, value, size);
            } else if (cmp_op == CompareOpType::GT) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Add,
                                            CompareOpType::GT>(
                    src, right_operand, value, size);
            } else if (cmp_op == CompareOpType::LE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Add,
                                            CompareOpType::LE>(
                    src, right_operand, value, size);
            } else if (cmp_op == CompareOpType::LT) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Add,
                                            CompareOpType::LT>(
                    src, right_operand, value, size);
            } else if (cmp_op == CompareOpType::NE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Add,
                                            CompareOpType::NE>(
                    src, right_operand, value, size);
            } else {
                // unimplemented
            }
        } else if (a_op == ArithOpType::Sub) {
            if (cmp_op == CompareOpType::EQ) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Sub,
                                            CompareOpType::EQ>(
                    src, right_operand, value, size);
            } else if (cmp_op == CompareOpType::GE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Sub,
                                            CompareOpType::GE>(
                    src, right_operand, value, size);
            } else if (cmp_op == CompareOpType::GT) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Sub,
                                            CompareOpType::GT>(
                    src, right_operand, value, size);
            } else if (cmp_op == CompareOpType::LE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Sub,
                                            CompareOpType::LE>(
                    src, right_operand, value, size);
            } else if (cmp_op == CompareOpType::LT) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Sub,
                                            CompareOpType::LT>(
                    src, right_operand, value, size);
            } else if (cmp_op == CompareOpType::NE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Sub,
                                            CompareOpType::NE>(
                    src, right_operand, value, size);
            } else {
                // unimplemented
            }
        } else if (a_op == ArithOpType::Mul) {
            if (cmp_op == CompareOpType::EQ) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Mul,
                                            CompareOpType::EQ>(
                    src, right_operand, value, size);
            } else if (cmp_op == CompareOpType::GE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Mul,
                                            CompareOpType::GE>(
                    src, right_operand, value, size);
            } else if (cmp_op == CompareOpType::GT) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Mul,
                                            CompareOpType::GT>(
                    src, right_operand, value, size);
            } else if (cmp_op == CompareOpType::LE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Mul,
                                            CompareOpType::LE>(
                    src, right_operand, value, size);
            } else if (cmp_op == CompareOpType::LT) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Mul,
                                            CompareOpType::LT>(
                    src, right_operand, value, size);
            } else if (cmp_op == CompareOpType::NE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Mul,
                                            CompareOpType::NE>(
                    src, right_operand, value, size);
            } else {
                // unimplemented
            }
        } else if (a_op == ArithOpType::Div) {
            if (cmp_op == CompareOpType::EQ) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Div,
                                            CompareOpType::EQ>(
                    src, right_operand, value, size);
            } else if (cmp_op == CompareOpType::GE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Div,
                                            CompareOpType::GE>(
                    src, right_operand, value, size);
            } else if (cmp_op == CompareOpType::GT) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Div,
                                            CompareOpType::GT>(
                    src, right_operand, value, size);
            } else if (cmp_op == CompareOpType::LE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Div,
                                            CompareOpType::LE>(
                    src, right_operand, value, size);
            } else if (cmp_op == CompareOpType::LT) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Div,
                                            CompareOpType::LT>(
                    src, right_operand, value, size);
            } else if (cmp_op == CompareOpType::NE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Div,
                                            CompareOpType::NE>(
                    src, right_operand, value, size);
            } else {
                // unimplemented
            }
        } else if (a_op == ArithOpType::Mod) {
            if (cmp_op == CompareOpType::EQ) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Mod,
                                            CompareOpType::EQ>(
                    src, right_operand, value, size);
            } else if (cmp_op == CompareOpType::GE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Mod,
                                            CompareOpType::GE>(
                    src, right_operand, value, size);
            } else if (cmp_op == CompareOpType::GT) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Mod,
                                            CompareOpType::GT>(
                    src, right_operand, value, size);
            } else if (cmp_op == CompareOpType::LE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Mod,
                                            CompareOpType::LE>(
                    src, right_operand, value, size);
            } else if (cmp_op == CompareOpType::LT) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Mod,
                                            CompareOpType::LT>(
                    src, right_operand, value, size);
            } else if (cmp_op == CompareOpType::NE) {
                this->inplace_arith_compare<T,
                                            ArithOpType::Mod,
                                            CompareOpType::NE>(
                    src, right_operand, value, size);
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
                          const size_t size) {
        range_checker::le(size, this->size());

        policy_type::template op_arith_compare<T, AOp, CmpOp>(
            this->data(), this->offset(), src, right_operand, value, size);
    }

    //
    // Inplace and. Also, counts the number of active bits.
    template <typename I, bool R>
    inline size_t
    inplace_and_with_count(const BitsetBase<PolicyT, I, R>& other,
                           const size_t size) {
        range_checker::le(size, this->size());
        range_checker::le(size, other.size());

        return policy_type::op_and_with_count(
            this->data(), other.data(), this->offset(), other.offset(), size);
    }

    // Inplace or. Also, counts the number of inactive bits.
    template <typename I, bool R>
    inline size_t
    inplace_or_with_count(const BitsetBase<PolicyT, I, R>& other,
                          const size_t size) {
        range_checker::le(size, this->size());
        range_checker::le(size, other.size());

        return policy_type::op_or_with_count(
            this->data(), other.data(), this->offset(), other.offset(), size);
    }

    // Return the starting bit offset in our container.
    inline size_t
    offset() const {
        return as_derived().offset_impl();
    }

 private:
    // CRTP
    inline ImplT&
    as_derived() {
        return static_cast<ImplT&>(*this);
    }

    // CRTP
    inline const ImplT&
    as_derived() const {
        return static_cast<const ImplT&>(*this);
    }
};

// Bitset view
template <typename PolicyT, bool IsRangeCheckEnabled>
class BitsetView : public BitsetBase<PolicyT,
                                     BitsetView<PolicyT, IsRangeCheckEnabled>,
                                     IsRangeCheckEnabled> {
    friend class BitsetBase<PolicyT,
                            BitsetView<PolicyT, IsRangeCheckEnabled>,
                            IsRangeCheckEnabled>;

 public:
    using policy_type = PolicyT;
    using data_type = typename policy_type::data_type;
    using proxy_type = typename policy_type::proxy_type;
    using const_proxy_type = typename policy_type::const_proxy_type;

    using range_checker = RangeChecker<IsRangeCheckEnabled>;

    BitsetView() = default;
    BitsetView(const BitsetView&) = default;
    BitsetView(BitsetView&&) = default;
    BitsetView&
    operator=(const BitsetView&) = default;
    BitsetView&
    operator=(BitsetView&&) = default;

    template <typename ImplT, bool R>
    explicit BitsetView(BitsetBase<PolicyT, ImplT, R>& bitset)
        : Data{bitset.data()}, Size{bitset.size()}, Offset{bitset.offset()} {
    }

    BitsetView(void* data, const size_t size)
        : Data{reinterpret_cast<data_type*>(data)}, Size{size} {
    }

    BitsetView(void* data, const size_t offset, const size_t size)
        : Data{reinterpret_cast<data_type*>(data)}, Size{size}, Offset{offset} {
    }

 private:
    // the referenced bits are [Offset, Offset + Size)
    data_type* Data = nullptr;
    // measured in bits
    size_t Size = 0;
    // measured in bits
    size_t Offset = 0;

    inline data_type*
    data_impl() {
        return Data;
    }
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
class Bitset
    : public BitsetBase<PolicyT,
                        Bitset<PolicyT, ContainerT, IsRangeCheckEnabled>,
                        IsRangeCheckEnabled> {
    friend class BitsetBase<PolicyT,
                            Bitset<PolicyT, ContainerT, IsRangeCheckEnabled>,
                            IsRangeCheckEnabled>;

 public:
    using policy_type = PolicyT;
    using data_type = typename policy_type::data_type;
    using proxy_type = typename policy_type::proxy_type;
    using const_proxy_type = typename policy_type::const_proxy_type;

    using view_type = BitsetView<PolicyT, IsRangeCheckEnabled>;

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
        : Data(get_required_size_in_container_elements(size)), Size{size} {
    }
    // Allocate the given number of bits, initialize with a given value.
    Bitset(const size_t size, const bool init)
        : Data(get_required_size_in_container_elements(size),
               init ? data_type(-1) : 0),
          Size{size} {
    }
    // Do not allow implicit copies (Rust style).
    Bitset(const Bitset&) = delete;
    // Allow default move.
    Bitset(Bitset&&) = default;
    // Do not allow implicit copies (Rust style).
    Bitset&
    operator=(const Bitset&) = delete;
    // Allow default move.
    Bitset&
    operator=(Bitset&&) = default;

    template <typename C, bool R>
    explicit Bitset(const BitsetBase<PolicyT, C, R>& other) {
        Data = container_type(
            get_required_size_in_container_elements(other.size()));
        Size = other.size();

        policy_type::op_copy(other.data(),
                             other.offset(),
                             this->data(),
                             this->offset(),
                             other.size());
    }

    // Clone a current bitset (Rust style).
    Bitset
    clone() const {
        Bitset cloned;
        cloned.Data = Data;
        cloned.Size = Size;
        return cloned;
    }

    // Rust style.
    inline container_type
    into() && {
        return std::move(this->Data);
    }

    // Resize.
    void
    resize(const size_t new_size) {
        const size_t new_size_in_container_elements =
            get_required_size_in_container_elements(new_size);
        Data.resize(new_size_in_container_elements);
        Size = new_size;
    }

    // Resize and initialize new bits with a given value if grown.
    void
    resize(const size_t new_size, const bool init) {
        const size_t old_size = this->size();
        this->resize(new_size);

        if (new_size > old_size) {
            policy_type::op_fill(
                this->data(), old_size, new_size - old_size, init);
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

        const size_t old_size = this->size();
        this->resize(this->size() + count);

        policy_type::op_copy(other.data(),
                             other.offset() + starting_bit_idx,
                             this->data(),
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
        Data.clear();
        Size = 0;
    }

    // Reserve
    inline void
    reserve(const size_t capacity) {
        const size_t capacity_in_container_elements =
            get_required_size_in_container_elements(capacity);
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
