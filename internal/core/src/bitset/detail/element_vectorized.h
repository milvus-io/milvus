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
#include <cstring>
#include <optional>
#include <type_traits>

#include "proxy.h"
#include "element_wise.h"

#if defined(__aarch64__) && defined(__ARM_NEON)
#include <arm_neon.h>
#endif
#if defined(__aarch64__) && defined(__linux__) && __has_include(<arm_sve.h>)
#include <arm_sve.h>
#include <sys/auxv.h>
#define BITSET_HAS_SVE_COUNT 1
#endif

namespace milvus {
namespace bitset {
namespace detail {

// Byte-addressed: supports both uint8_t and uint64_t policies, with no
// extra alignment/padding requirement or type-punning scalar loads.
// Builtin copies avoid fortified libc wrappers inheriting a caller TU's ISA.
#if defined(__x86_64__) && (defined(__GNUC__) || defined(__clang__))
__attribute__((target("arch=x86-64,no-avx,no-avx2,no-avx512f,no-popcnt")))
#elif defined(__aarch64__) && defined(__clang__)
__attribute__((target("no-sve,no-sve2")))
#elif defined(__aarch64__) && defined(__GNUC__)
__attribute__((target("arch=armv8-a")))
#endif
inline size_t
CountBytesScalar(const uint8_t* data, size_t bytes) {
    size_t count = 0;
    size_t i = 0;
    for (; i + sizeof(uint64_t) <= bytes; i += sizeof(uint64_t)) {
        uint64_t word;
        __builtin_memcpy(&word, data + i, sizeof(word));
        count += __builtin_popcountll(word);
    }
    for (; i < bytes; ++i) {
        count += __builtin_popcount(data[i]);
    }
    return count;
}

#if defined(__x86_64__) && (defined(__GNUC__) || defined(__clang__))
__attribute__((target("arch=x86-64,popcnt,no-avx,no-avx2,no-avx512f"),
               noinline)) inline size_t
CountBytesPopcnt(const uint8_t* data, size_t bytes) {
    size_t a = 0, b = 0, c = 0, d = 0;
    size_t i = 0;
    for (; i + 32 <= bytes; i += 32) {
        uint64_t words[4];
        __builtin_memcpy(words, data + i, sizeof(words));
        a += __builtin_popcountll(words[0]);
        b += __builtin_popcountll(words[1]);
        c += __builtin_popcountll(words[2]);
        d += __builtin_popcountll(words[3]);
    }
    for (; i + 8 <= bytes; i += 8) {
        uint64_t word;
        __builtin_memcpy(&word, data + i, sizeof(word));
        a += __builtin_popcountll(word);
    }
    for (; i < bytes; ++i) a += __builtin_popcount(data[i]);
    return a + b + c + d;
}

__attribute__((target("arch=x86-64,avx512f,avx512vl,avx512vpopcntdq,no-"
                      "avx512bw,no-avx512dq,no-avx512cd"),
               noinline)) inline size_t
CountBytesVpopcnt256(const uint8_t* data, size_t bytes) {
    size_t prefix = (32 - (reinterpret_cast<uintptr_t>(data) & 31)) & 31;
    if (prefix > bytes)
        prefix = bytes;
    const size_t leading = CountBytesScalar(data, prefix);
    data += prefix;
    bytes -= prefix;
    // Compiler builtins avoid intrinsic wrappers inheriting another TU's ISA.
    using Vector = unsigned long long __attribute__((vector_size(32)));
    using SignedVector = long long __attribute__((vector_size(32)));
    Vector sums[4] = {};
    size_t i = 0;
    for (; i + 128 <= bytes; i += 128) {
        for (size_t lane = 0; lane < 4; ++lane) {
            Vector words;
            __builtin_memcpy(&words, data + i + lane * 32, sizeof(words));
#if defined(__clang__)
            sums[lane] +=
                Vector(__builtin_ia32_vpopcntq_256(SignedVector(words)));
#else
            sums[lane] +=
                Vector(__builtin_ia32_vpopcountq_v4di(SignedVector(words)));
#endif
        }
    }
    Vector total = sums[0] + sums[1] + sums[2] + sums[3];
    for (; i + 32 <= bytes; i += 32) {
        Vector words;
        __builtin_memcpy(&words, data + i, sizeof(words));
#if defined(__clang__)
        total += Vector(__builtin_ia32_vpopcntq_256(SignedVector(words)));
#else
        total += Vector(__builtin_ia32_vpopcountq_v4di(SignedVector(words)));
#endif
    }
    return leading + total[0] + total[1] + total[2] + total[3] +
           CountBytesScalar(data + i, bytes - i);
}
#endif

#if defined(BITSET_HAS_SVE_COUNT)
// Function-level targeting keeps SVE instructions out of the baseline path.
#if defined(__clang__)
__attribute__((target("sve"), noinline))
#else
__attribute__((target("arch=armv8-a+sve"), noinline))
#endif
inline size_t
CountBytesSve(const uint8_t* data, size_t bytes) {
    const size_t width = svcntb();
    const auto pg8 = svptrue_b8();
    const auto pg64 = svptrue_b64();
    auto a = svdup_u64(0);
    auto b = a;
    auto c = a;
    auto d = a;
    size_t i = 0;
    for (; bytes - i >= 4 * width; i += 4 * width) {
        a = svadd_u64_x(
            pg64,
            a,
            svcnt_u64_x(pg64, svreinterpret_u64_u8(svld1_u8(pg8, data + i))));
        b = svadd_u64_x(
            pg64,
            b,
            svcnt_u64_x(pg64,
                        svreinterpret_u64_u8(svld1_u8(pg8, data + i + width))));
        c = svadd_u64_x(pg64,
                        c,
                        svcnt_u64_x(pg64,
                                    svreinterpret_u64_u8(
                                        svld1_u8(pg8, data + i + 2 * width))));
        d = svadd_u64_x(pg64,
                        d,
                        svcnt_u64_x(pg64,
                                    svreinterpret_u64_u8(
                                        svld1_u8(pg8, data + i + 3 * width))));
    }
    a = svadd_u64_x(pg64, svadd_u64_x(pg64, a, b), svadd_u64_x(pg64, c, d));
    for (; i < bytes;) {
        // Inactive bytes are zero-filled, including a partial uint64 lane.
        const auto pg = svwhilelt_b8(uint64_t(i), uint64_t(bytes));
        const auto words = svreinterpret_u64_u8(svld1_u8(pg, data + i));
        a = svadd_u64_x(pg64, a, svcnt_u64_x(pg64, words));
        if (bytes - i <= width) {
            break;
        }
        i += width;
    }
    return svaddv_u64(pg64, a);
}

#endif

// Isolate SIMD prologues and CPU dispatch from short range_count calls.
#if defined(__x86_64__) && (defined(__GNUC__) || defined(__clang__))
__attribute__((target("arch=x86-64,no-avx,no-avx2,no-avx512f,no-popcnt"),
               noinline))
#elif defined(__aarch64__) && defined(__clang__)
__attribute__((target("no-sve,no-sve2"), noinline))
#elif defined(__aarch64__) && defined(__GNUC__)
__attribute__((target("arch=armv8-a"), noinline))
#else
__attribute__((noinline))
#endif
inline size_t
CountBytesBulk(const uint8_t* data, size_t bytes) {
#if defined(__aarch64__) && defined(__ARM_NEON)
#if defined(BITSET_HAS_SVE_COUNT) && defined(HWCAP_SVE)
    static const bool use_sve = (getauxval(AT_HWCAP) & HWCAP_SVE) != 0;
    if (use_sve) {
        return CountBytesSve(data, bytes);
    }
#endif
    size_t prefix = (16 - (reinterpret_cast<uintptr_t>(data) & 15)) & 15;
    if (prefix > bytes)
        prefix = bytes;
    size_t total = CountBytesScalar(data, prefix);
    data += prefix;
    bytes -= prefix;
    size_t i = 0;
    for (; i + 1024 <= bytes; i += 1024) {
        auto a = vdupq_n_u16(0);
        auto b = a;
        auto c = a;
        auto d = a;
        for (size_t j = 0; j < 1024; j += 64) {
            a = vpadalq_u8(a, vcntq_u8(vld1q_u8(data + i + j)));
            b = vpadalq_u8(b, vcntq_u8(vld1q_u8(data + i + j + 16)));
            c = vpadalq_u8(c, vcntq_u8(vld1q_u8(data + i + j + 32)));
            d = vpadalq_u8(d, vcntq_u8(vld1q_u8(data + i + j + 48)));
        }
        // At most 1024 in any combined uint16 lane per 1 KiB chunk.
        total += vaddlvq_u16(vaddq_u16(vaddq_u16(a, b), vaddq_u16(c, d)));
    }
    auto tail = vdupq_n_u16(0);
    auto b = tail;
    auto c = tail;
    auto d = tail;
    for (; i + 64 <= bytes; i += 64) {
        tail = vpadalq_u8(tail, vcntq_u8(vld1q_u8(data + i)));
        b = vpadalq_u8(b, vcntq_u8(vld1q_u8(data + i + 16)));
        c = vpadalq_u8(c, vcntq_u8(vld1q_u8(data + i + 32)));
        d = vpadalq_u8(d, vcntq_u8(vld1q_u8(data + i + 48)));
    }
    tail = vaddq_u16(vaddq_u16(tail, b), vaddq_u16(c, d));
    for (; i + 16 <= bytes; i += 16) {
        tail = vpadalq_u8(tail, vcntq_u8(vld1q_u8(data + i)));
    }
    return total + vaddlvq_u16(tail) + CountBytesScalar(data + i, bytes - i);
#elif defined(__x86_64__) && (defined(__GNUC__) || defined(__clang__))
    static const bool use_vpopcnt = [] {
        __builtin_cpu_init();
        return __builtin_cpu_supports("avx512f") &&
               __builtin_cpu_supports("avx512vl") &&
               __builtin_cpu_supports("avx512vpopcntdq");
    }();
    if (use_vpopcnt) {
        return CountBytesVpopcnt256(data, bytes);
    }
    if (__builtin_cpu_supports("popcnt")) {
        return CountBytesPopcnt(data, bytes);
    }
    return CountBytesScalar(data, bytes);
#else
    return CountBytesScalar(data, bytes);
#endif
}

// SIMD applied on top of ElementWiseBitsetPolicy
template <typename ElementT, typename VectorizedT>
struct VectorizedElementWiseBitsetPolicy {
    using data_type = ElementT;
    constexpr static size_t data_bits = sizeof(data_type) * 8;

    using self_type = VectorizedElementWiseBitsetPolicy<ElementT, VectorizedT>;

    using proxy_type = Proxy<self_type>;
    using const_proxy_type = ConstProxy<self_type>;

    static inline size_t
    get_element(const size_t idx) {
        return idx / data_bits;
    }

    static inline size_t
    get_shift(const size_t idx) {
        return idx % data_bits;
    }

    static inline size_t
    get_required_size_in_elements(const size_t size) {
        return (size + data_bits - 1) / data_bits;
    }

    static inline size_t
    get_required_size_in_bytes(const size_t size) {
        return get_required_size_in_elements(size) * sizeof(data_type);
    }

    static inline proxy_type
    get_proxy(data_type* const __restrict data, const size_t idx) {
        data_type& element = data[get_element(idx)];
        const size_t shift = get_shift(idx);
        return proxy_type{element, shift};
    }

    static inline const_proxy_type
    get_proxy(const data_type* const __restrict data, const size_t idx) {
        const data_type& element = data[get_element(idx)];
        const size_t shift = get_shift(idx);
        return const_proxy_type{element, shift};
    }

    static inline void
    op_flip(data_type* const data, const size_t start, const size_t size) {
        ElementWiseBitsetPolicy<ElementT>::op_flip(data, start, size);
    }

    static inline void
    op_and(data_type* const left,
           const data_type* const right,
           const size_t start_left,
           const size_t start_right,
           const size_t size) {
        if (!VectorizedT::template forward_op_and<ElementT>(
                left, right, start_left, start_right, size)) {
            ElementWiseBitsetPolicy<ElementT>::op_and(
                left, right, start_left, start_right, size);
        }
    }

    static inline void
    op_and_flip(data_type* const left,
                const data_type* const right,
                const size_t start_left,
                const size_t start_right,
                const size_t size) {
        using Scalar = ElementWiseBitsetPolicy<ElementT>;
        if (size == 0) {
            return;
        }
        if (Scalar::ranges_overlap(
                left, start_left, right, start_right, size)) {
            // Use this policy's AND so even overlapping views retain the
            // traversal of the selected platform implementation.
            op_and(left, right, start_left, start_right, size);
            op_flip(left, start_left, size);
            return;
        }
        Scalar::op_and_flip(left, right, start_left, start_right, size);
    }

    static inline void
    op_and_multiple(data_type* const left,
                    const data_type* const* const rights,
                    const size_t start_left,
                    const size_t* const __restrict start_rights,
                    const size_t n_rights,
                    const size_t size) {
        if (!VectorizedT::template forward_op_and_multiple<ElementT>(
                left, rights, start_left, start_rights, n_rights, size)) {
            ElementWiseBitsetPolicy<ElementT>::op_and_multiple(
                left, rights, start_left, start_rights, n_rights, size);
        }
    }

    static inline void
    op_or(data_type* const left,
          const data_type* const right,
          const size_t start_left,
          const size_t start_right,
          const size_t size) {
        if (!VectorizedT::template forward_op_or<ElementT>(
                left, right, start_left, start_right, size)) {
            ElementWiseBitsetPolicy<ElementT>::op_or(
                left, right, start_left, start_right, size);
        }
    }

    static inline void
    op_or_multiple(data_type* const left,
                   const data_type* const* const rights,
                   const size_t start_left,
                   const size_t* const __restrict start_rights,
                   const size_t n_rights,
                   const size_t size) {
        if (!VectorizedT::template forward_op_or_multiple<ElementT>(
                left, rights, start_left, start_rights, n_rights, size)) {
            ElementWiseBitsetPolicy<ElementT>::op_or_multiple(
                left, rights, start_left, start_rights, n_rights, size);
        }
    }

    static inline void
    op_set(data_type* const data, const size_t start, const size_t size) {
        ElementWiseBitsetPolicy<ElementT>::op_set(data, start, size);
    }

    static inline void
    op_reset(data_type* const data, const size_t start, const size_t size) {
        ElementWiseBitsetPolicy<ElementT>::op_reset(data, start, size);
    }

    static inline bool
    op_all(const data_type* const data, const size_t start, const size_t size) {
        return ElementWiseBitsetPolicy<ElementT>::op_all(data, start, size);
    }

    static inline bool
    op_none(const data_type* const data,
            const size_t start,
            const size_t size) {
        return ElementWiseBitsetPolicy<ElementT>::op_none(data, start, size);
    }

    static void
    op_copy(const data_type* const src,
            const size_t start_src,
            data_type* const dst,
            const size_t start_dst,
            const size_t size) {
        ElementWiseBitsetPolicy<ElementT>::op_copy(
            src, start_src, dst, start_dst, size);
    }

    static inline size_t
    op_count(const data_type* const data,
             const size_t start,
             const size_t size) {
        if (size >= 16384) {
            return op_count_large(data, start, size);
        }
        return ElementWiseBitsetPolicy<ElementT>::op_count(data, start, size);
    }

    static __attribute__((noinline)) size_t
    op_count_large(const data_type* data, size_t start, size_t size) {
        using Scalar = ElementWiseBitsetPolicy<ElementT>;
        size_t count = 0;
        const size_t shift = Scalar::get_shift(start);
        if (shift != 0) {
            const size_t prefix = data_bits - shift;
            count = Scalar::op_count(data, start, prefix);
            start += prefix;
            size -= prefix;
        }
        const size_t middle = (size / data_bits) * data_bits;
        count += CountBytesBulk(
            reinterpret_cast<const uint8_t*>(data + Scalar::get_element(start)),
            middle / 8);
        return count + Scalar::op_count(data, start + middle, size - middle);
    }

    static inline bool
    op_eq(const data_type* const left,
          const data_type* const right,
          const size_t start_left,
          const size_t start_right,
          const size_t size) {
        return ElementWiseBitsetPolicy<ElementT>::op_eq(
            left, right, start_left, start_right, size);
    }

    static inline void
    op_xor(data_type* const left,
           const data_type* const right,
           const size_t start_left,
           const size_t start_right,
           const size_t size) {
        if (!VectorizedT::template forward_op_xor<ElementT>(
                left, right, start_left, start_right, size)) {
            ElementWiseBitsetPolicy<ElementT>::op_xor(
                left, right, start_left, start_right, size);
        }
    }

    static inline void
    op_sub(data_type* const left,
           const data_type* const right,
           const size_t start_left,
           const size_t start_right,
           const size_t size) {
        if (!VectorizedT::template forward_op_sub<ElementT>(
                left, right, start_left, start_right, size)) {
            ElementWiseBitsetPolicy<ElementT>::op_sub(
                left, right, start_left, start_right, size);
        }
    }

    static void
    op_fill(data_type* const data,
            const size_t start,
            const size_t size,
            const bool value) {
        ElementWiseBitsetPolicy<ElementT>::op_fill(data, start, size, value);
    }

    //
    static inline std::optional<size_t>
    op_find(const data_type* const data,
            const size_t start,
            const size_t size,
            const size_t starting_idx,
            const bool is_set) {
        return ElementWiseBitsetPolicy<ElementT>::op_find(
            data, start, size, starting_idx, is_set);
    }

    //
    static inline data_type
    op_read(const data_type* const data,
            const size_t start,
            const size_t nbits) {
        return ElementWiseBitsetPolicy<ElementT>::op_read(data, start, nbits);
    }

    //
    template <typename T, typename U, CompareOpType Op>
    static inline void
    op_compare_column(data_type* const __restrict data,
                      const size_t start,
                      const T* const __restrict t,
                      const U* const __restrict u,
                      const size_t size) {
        op_func(
            start,
            size,
            [data, t, u](const size_t starting_bit,
                         const size_t ptr_offset,
                         const size_t nbits) {
                ElementWiseBitsetPolicy<ElementT>::
                    template op_compare_column<T, U, Op>(data,
                                                         starting_bit,
                                                         t + ptr_offset,
                                                         u + ptr_offset,
                                                         nbits);
            },
            [data, t, u](const size_t starting_element,
                         const size_t ptr_offset,
                         const size_t nbits) {
                return VectorizedT::template op_compare_column<T, U, Op>(
                    reinterpret_cast<uint8_t*>(data + starting_element),
                    t + ptr_offset,
                    u + ptr_offset,
                    nbits);
            });
    }

    //
    template <typename T, CompareOpType Op>
    static inline void
    op_compare_val(data_type* const __restrict data,
                   const size_t start,
                   const T* const __restrict t,
                   const size_t size,
                   const T& value) {
        op_func(
            start,
            size,
            [data, t, value](const size_t starting_bit,
                             const size_t ptr_offset,
                             const size_t nbits) {
                ElementWiseBitsetPolicy<ElementT>::template op_compare_val<T,
                                                                           Op>(
                    data, starting_bit, t + ptr_offset, nbits, value);
            },
            [data, t, value](const size_t starting_element,
                             const size_t ptr_offset,
                             const size_t nbits) {
                return VectorizedT::template op_compare_val<T, Op>(
                    reinterpret_cast<uint8_t*>(data + starting_element),
                    t + ptr_offset,
                    nbits,
                    value);
            });
    }

    //
    template <typename T, RangeType Op>
    static inline void
    op_within_range_column(data_type* const __restrict data,
                           const size_t start,
                           const T* const __restrict lower,
                           const T* const __restrict upper,
                           const T* const __restrict values,
                           const size_t size) {
        op_func(
            start,
            size,
            [data, lower, upper, values](const size_t starting_bit,
                                         const size_t ptr_offset,
                                         const size_t nbits) {
                ElementWiseBitsetPolicy<ElementT>::
                    template op_within_range_column<T, Op>(data,
                                                           starting_bit,
                                                           lower + ptr_offset,
                                                           upper + ptr_offset,
                                                           values + ptr_offset,
                                                           nbits);
            },
            [data, lower, upper, values](const size_t starting_element,
                                         const size_t ptr_offset,
                                         const size_t nbits) {
                return VectorizedT::template op_within_range_column<T, Op>(
                    reinterpret_cast<uint8_t*>(data + starting_element),
                    lower + ptr_offset,
                    upper + ptr_offset,
                    values + ptr_offset,
                    nbits);
            });
    }

    //
    template <typename T, RangeType Op>
    static inline void
    op_within_range_val(data_type* const __restrict data,
                        const size_t start,
                        const T& lower,
                        const T& upper,
                        const T* const __restrict values,
                        const size_t size) {
        op_func(
            start,
            size,
            [data, lower, upper, values](const size_t starting_bit,
                                         const size_t ptr_offset,
                                         const size_t nbits) {
                ElementWiseBitsetPolicy<ElementT>::
                    template op_within_range_val<T, Op>(data,
                                                        starting_bit,
                                                        lower,
                                                        upper,
                                                        values + ptr_offset,
                                                        nbits);
            },
            [data, lower, upper, values](const size_t starting_element,
                                         const size_t ptr_offset,
                                         const size_t nbits) {
                return VectorizedT::template op_within_range_val<T, Op>(
                    reinterpret_cast<uint8_t*>(data + starting_element),
                    lower,
                    upper,
                    values + ptr_offset,
                    nbits);
            });
    }

    //
    template <typename T, ArithOpType AOp, CompareOpType CmpOp>
    static inline void
    op_arith_compare(data_type* const __restrict data,
                     const size_t start,
                     const T* const __restrict src,
                     const ArithHighPrecisionType<T>& right_operand,
                     const ArithHighPrecisionType<T>& value,
                     const size_t size) {
        op_func(
            start,
            size,
            [data, src, right_operand, value](const size_t starting_bit,
                                              const size_t ptr_offset,
                                              const size_t nbits) {
                ElementWiseBitsetPolicy<ElementT>::
                    template op_arith_compare<T, AOp, CmpOp>(data,
                                                             starting_bit,
                                                             src + ptr_offset,
                                                             right_operand,
                                                             value,
                                                             nbits);
            },
            [data, src, right_operand, value](const size_t starting_element,
                                              const size_t ptr_offset,
                                              const size_t nbits) {
                return VectorizedT::template op_arith_compare<T, AOp, CmpOp>(
                    reinterpret_cast<uint8_t*>(data + starting_element),
                    src + ptr_offset,
                    right_operand,
                    value,
                    nbits);
            });
    }

    //
    static inline size_t
    op_and_with_count(data_type* const left,
                      const data_type* const right,
                      const size_t start_left,
                      const size_t start_right,
                      const size_t size) {
        return ElementWiseBitsetPolicy<ElementT>::op_and_with_count(
            left, right, start_left, start_right, size);
    }

    static inline size_t
    op_or_with_count(data_type* const left,
                     const data_type* const right,
                     const size_t start_left,
                     const size_t start_right,
                     const size_t size) {
        return ElementWiseBitsetPolicy<ElementT>::op_or_with_count(
            left, right, start_left, start_right, size);
    }

    // void FuncBaseline(const size_t starting_bit, const size_t ptr_offset, const size_t nbits)
    // bool FuncVectorized(const size_t starting_element, const size_t ptr_offset, const size_t nbits)
    template <typename FuncBaseline, typename FuncVectorized>
    static inline void
    op_func(const size_t start,
            const size_t size,
            FuncBaseline func_baseline,
            FuncVectorized func_vectorized) {
        if (size == 0) {
            return;
        }

        auto start_element = get_element(start);
        const auto end_element = get_element(start + size);

        const auto start_shift = get_shift(start);
        const auto end_shift = get_shift(start + size);

        // same element?
        if (start_element == end_element) {
            func_baseline(start, 0, size);
            return;
        }

        //
        uintptr_t ptr_offset = 0;

        // process the first element
        if (start_shift != 0) {
            // it is possible to do vectorized masking here, but it is not worth it
            func_baseline(start, 0, data_bits - start_shift);

            // start from the next element
            start_element += 1;
            ptr_offset += data_bits - start_shift;
        }

        // process the middle
        {
            const size_t starting_bit_idx = start_element * data_bits;
            const size_t nbits = (end_element - start_element) * data_bits;

            // check if vectorized implementation is available
            if (!func_vectorized(start_element, ptr_offset, nbits)) {
                // vectorized implementation is not available, invoke the default one
                func_baseline(starting_bit_idx, ptr_offset, nbits);
            }

            //
            ptr_offset += nbits;
        }

        // process the last element
        if (end_shift != 0) {
            // it is possible to do vectorized masking here, but it is not worth it
            const size_t starting_bit_idx = end_element * data_bits;

            func_baseline(starting_bit_idx, ptr_offset, end_shift);
        }
    }
};

}  // namespace detail
}  // namespace bitset
}  // namespace milvus

#ifdef BITSET_HAS_SVE_COUNT
#undef BITSET_HAS_SVE_COUNT
#endif
