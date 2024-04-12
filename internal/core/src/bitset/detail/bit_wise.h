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
#include <optional>

#include "proxy.h"

namespace milvus {
namespace bitset {
namespace detail {

// This is a naive reference policy that operates on bit level.
// No optimizations are applied.
// This is little-endian based.
template <typename ElementT>
struct BitWiseBitsetPolicy {
    using data_type = ElementT;
    constexpr static auto data_bits = sizeof(data_type) * 8;

    using size_type = size_t;

    using self_type = BitWiseBitsetPolicy<ElementT>;

    using proxy_type = Proxy<self_type>;
    using const_proxy_type = ConstProxy<self_type>;

    static inline size_type
    get_element(const size_t idx) {
        return idx / data_bits;
    }

    static inline size_type
    get_shift(const size_t idx) {
        return idx % data_bits;
    }

    static inline size_type
    get_required_size_in_elements(const size_t size) {
        return (size + data_bits - 1) / data_bits;
    }

    static inline size_type
    get_required_size_in_bytes(const size_t size) {
        return get_required_size_in_elements(size) * sizeof(data_type);
    }

    static inline proxy_type
    get_proxy(data_type* const __restrict data, const size_type idx) {
        data_type& element = data[get_element(idx)];
        const size_type shift = get_shift(idx);
        return proxy_type{element, shift};
    }

    static inline const_proxy_type
    get_proxy(const data_type* const __restrict data, const size_type idx) {
        const data_type& element = data[get_element(idx)];
        const size_type shift = get_shift(idx);
        return const_proxy_type{element, shift};
    }

    static inline data_type
    op_read(const data_type* const data,
            const size_type start,
            const size_type nbits) {
        data_type value = 0;
        for (size_type i = 0; i < nbits; i++) {
            const auto proxy = get_proxy(data, start + i);
            value += proxy ? (data_type(1) << i) : 0;
        }

        return value;
    }

    static void
    op_write(data_type* const data,
             const size_type start,
             const size_type nbits,
             const data_type value) {
        for (size_type i = 0; i < nbits; i++) {
            auto proxy = get_proxy(data, start + i);
            data_type mask = data_type(1) << i;
            if ((value & mask) == mask) {
                proxy = true;
            } else {
                proxy = false;
            }
        }
    }

    static inline void
    op_flip(data_type* const data,
            const size_type start,
            const size_type size) {
        for (size_type i = 0; i < size; i++) {
            auto proxy = get_proxy(data, start + i);
            proxy.flip();
        }
    }

    static inline void
    op_and(data_type* const left,
           const data_type* const right,
           const size_t start_left,
           const size_t start_right,
           const size_t size) {
        // todo: check if intersect

        for (size_type i = 0; i < size; i++) {
            auto proxy_left = get_proxy(left, start_left + i);
            auto proxy_right = get_proxy(right, start_right + i);

            proxy_left &= proxy_right;
        }
    }

    static inline void
    op_or(data_type* const left,
          const data_type* const right,
          const size_t start_left,
          const size_t start_right,
          const size_t size) {
        // todo: check if intersect

        for (size_type i = 0; i < size; i++) {
            auto proxy_left = get_proxy(left, start_left + i);
            auto proxy_right = get_proxy(right, start_right + i);

            proxy_left |= proxy_right;
        }
    }

    static inline void
    op_set(data_type* const data, const size_type start, const size_type size) {
        for (size_type i = 0; i < size; i++) {
            get_proxy(data, start + i) = true;
        }
    }

    static inline void
    op_reset(data_type* const data,
             const size_type start,
             const size_type size) {
        for (size_type i = 0; i < size; i++) {
            get_proxy(data, start + i) = false;
        }
    }

    static inline bool
    op_all(const data_type* const data,
           const size_type start,
           const size_type size) {
        for (size_type i = 0; i < size; i++) {
            if (!get_proxy(data, start + i)) {
                return false;
            }
        }

        return true;
    }

    static inline bool
    op_none(const data_type* const data,
            const size_type start,
            const size_type size) {
        for (size_type i = 0; i < size; i++) {
            if (get_proxy(data, start + i)) {
                return false;
            }
        }

        return true;
    }

    static void
    op_copy(const data_type* const src,
            const size_type start_src,
            data_type* const dst,
            const size_type start_dst,
            const size_type size) {
        for (size_type i = 0; i < size; i++) {
            const auto src_p = get_proxy(src, start_src + i);
            auto dst_p = get_proxy(dst, start_dst + i);
            dst_p = src_p.operator bool();
        }
    }

    static void
    op_fill(data_type* const dst,
            const size_type start_dst,
            const size_type size,
            const bool value) {
        for (size_type i = 0; i < size; i++) {
            auto dst_p = get_proxy(dst, start_dst + i);
            dst_p = value;
        }
    }

    static inline size_type
    op_count(const data_type* const data,
             const size_type start,
             const size_type size) {
        size_type count = 0;

        for (size_type i = 0; i < size; i++) {
            auto proxy = get_proxy(data, start + i);
            count += (proxy) ? 1 : 0;
        }

        return count;
    }

    static inline bool
    op_eq(const data_type* const left,
          const data_type* const right,
          const size_t start_left,
          const size_t start_right,
          const size_t size) {
        for (size_type i = 0; i < size; i++) {
            const auto proxy_left = get_proxy(left, start_left + i);
            const auto proxy_right = get_proxy(right, start_right + i);

            if (proxy_left != proxy_right) {
                return false;
            }
        }

        return true;
    }

    static inline void
    op_xor(data_type* const left,
           const data_type* const right,
           const size_t start_left,
           const size_t start_right,
           const size_t size) {
        // todo: check if intersect

        for (size_type i = 0; i < size; i++) {
            auto proxy_left = get_proxy(left, start_left + i);
            const auto proxy_right = get_proxy(right, start_right + i);

            proxy_left ^= proxy_right;
        }
    }

    static inline void
    op_sub(data_type* const left,
           const data_type* const right,
           const size_t start_left,
           const size_t start_right,
           const size_t size) {
        // todo: check if intersect

        for (size_type i = 0; i < size; i++) {
            auto proxy_left = get_proxy(left, start_left + i);
            const auto proxy_right = get_proxy(right, start_right + i);

            proxy_left &= ~proxy_right;
        }
    }

    //
    static inline std::optional<size_type>
    op_find(const data_type* const data,
            const size_type start,
            const size_type size,
            const size_type starting_idx) {
        for (size_type i = starting_idx; i < size; i++) {
            const auto proxy = get_proxy(data, start + i);
            if (proxy) {
                return i;
            }
        }

        return std::nullopt;
    }

    //
    template <typename T, typename U, CompareOpType Op>
    static inline void
    op_compare_column(data_type* const __restrict data,
                      const size_type start,
                      const T* const __restrict t,
                      const U* const __restrict u,
                      const size_type size) {
        for (size_type i = 0; i < size; i++) {
            get_proxy(data, start + i) =
                CompareOperator<Op>::compare(t[i], u[i]);
        }
    }

    //
    template <typename T, CompareOpType Op>
    static inline void
    op_compare_val(data_type* const __restrict data,
                   const size_type start,
                   const T* const __restrict t,
                   const size_type size,
                   const T& value) {
        for (size_type i = 0; i < size; i++) {
            get_proxy(data, start + i) =
                CompareOperator<Op>::compare(t[i], value);
        }
    }

    template <typename T, RangeType Op>
    static inline void
    op_within_range_column(data_type* const __restrict data,
                           const size_type start,
                           const T* const __restrict lower,
                           const T* const __restrict upper,
                           const T* const __restrict values,
                           const size_type size) {
        for (size_type i = 0; i < size; i++) {
            get_proxy(data, start + i) =
                RangeOperator<Op>::within_range(lower[i], upper[i], values[i]);
        }
    }

    //
    template <typename T, RangeType Op>
    static inline void
    op_within_range_val(data_type* const __restrict data,
                        const size_type start,
                        const T& lower,
                        const T& upper,
                        const T* const __restrict values,
                        const size_type size) {
        for (size_type i = 0; i < size; i++) {
            get_proxy(data, start + i) =
                RangeOperator<Op>::within_range(lower, upper, values[i]);
        }
    }

    //
    template <typename T, ArithOpType AOp, CompareOpType CmpOp>
    static inline void
    op_arith_compare(data_type* const __restrict data,
                     const size_type start,
                     const T* const __restrict src,
                     const ArithHighPrecisionType<T>& right_operand,
                     const ArithHighPrecisionType<T>& value,
                     const size_type size) {
        for (size_type i = 0; i < size; i++) {
            get_proxy(data, start + i) =
                ArithCompareOperator<AOp, CmpOp>::compare(
                    src[i], right_operand, value);
        }
    }

    //
    static inline size_t
    op_and_with_count(data_type* const left,
                      const data_type* const right,
                      const size_t start_left,
                      const size_t start_right,
                      const size_t size) {
        // todo: check if intersect

        size_t active = 0;
        for (size_type i = 0; i < size; i++) {
            auto proxy_left = get_proxy(left, start_left + i);
            auto proxy_right = get_proxy(right, start_right + i);

            const bool b = proxy_left & proxy_right;
            proxy_left = b;

            active += b ? 1 : 0;
        }

        return active;
    }

    static inline size_t
    op_or_with_count(data_type* const left,
                     const data_type* const right,
                     const size_t start_left,
                     const size_t start_right,
                     const size_t size) {
        // todo: check if intersect

        size_t inactive = 0;
        for (size_type i = 0; i < size; i++) {
            auto proxy_left = get_proxy(left, start_left + i);
            auto proxy_right = get_proxy(right, start_right + i);

            const bool b = proxy_left | proxy_right;
            proxy_left = b;

            inactive += b ? 0 : 1;
        }

        return inactive;
    }
};

}  // namespace detail
}  // namespace bitset
}  // namespace milvus
