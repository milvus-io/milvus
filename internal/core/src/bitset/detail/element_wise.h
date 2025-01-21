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

#include "ctz.h"
#include "popcount.h"

#include "bitset/common.h"
#include "maybe_vector.h"

namespace milvus {
namespace bitset {
namespace detail {

// This one is similar to boost::dynamic_bitset
template <typename ElementT>
struct ElementWiseBitsetPolicy {
    using data_type = ElementT;
    constexpr static size_t data_bits = sizeof(data_type) * 8;

    using self_type = ElementWiseBitsetPolicy<ElementT>;

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

    static inline data_type
    op_read(const data_type* const data,
            const size_t start,
            const size_t nbits) {
        if (nbits == 0) {
            return 0;
        }

        const auto start_element = get_element(start);
        const auto end_element = get_element(start + nbits - 1);

        const auto start_shift = get_shift(start);
        const auto end_shift = get_shift(start + nbits - 1);

        if (start_element == end_element) {
            // read from 1 element only
            const data_type m1 = get_shift_mask_end(start_shift);
            const data_type m2 = get_shift_mask_begin(end_shift + 1);
            const data_type mask = get_shift_mask_end(start_shift) &
                                   get_shift_mask_begin(end_shift + 1);

            // read and shift
            const data_type element = data[start_element];
            const data_type value = (element & mask) >> start_shift;
            return value;
        } else {
            // read from 2 elements
            const data_type first_v = data[start_element];
            const data_type second_v = data[start_element + 1];

            const data_type first_mask = get_shift_mask_end(start_shift);
            const data_type second_mask = get_shift_mask_begin(end_shift + 1);

            const data_type value1 = (first_v & first_mask) >> start_shift;
            const data_type value2 = (second_v & second_mask);
            const data_type value =
                value1 | (value2 << (data_bits - start_shift));

            return value;
        }
    }

    static inline void
    op_write(data_type* const data,
             const size_t start,
             const size_t nbits,
             const data_type value) {
        if (nbits == 0) {
            return;
        }

        const auto start_element = get_element(start);
        const auto end_element = get_element(start + nbits - 1);

        const auto start_shift = get_shift(start);
        const auto end_shift = get_shift(start + nbits - 1);

        if (start_element == end_element) {
            // write into a single element

            const data_type m1 = get_shift_mask_end(start_shift);
            const data_type m2 = get_shift_mask_begin(end_shift + 1);
            const data_type mask = get_shift_mask_end(start_shift) &
                                   get_shift_mask_begin(end_shift + 1);

            // read an existing value
            const data_type element = data[start_element];
            // combine a new value
            const data_type new_value =
                (element & (~mask)) | ((value << start_shift) & mask);
            // write it back
            data[start_element] = new_value;
        } else {
            // write into two elements
            const data_type first_v = data[start_element];
            const data_type second_v = data[start_element + 1];

            const data_type first_mask = get_shift_mask_end(start_shift);
            const data_type second_mask = get_shift_mask_begin(end_shift + 1);

            const data_type value1 = (first_v & (~first_mask)) |
                                     ((value << start_shift) & first_mask);
            const data_type value2 =
                (second_v & (~second_mask)) |
                ((value >> (data_bits - start_shift)) & second_mask);

            data[start_element] = value1;
            data[start_element + 1] = value2;
        }
    }

    static inline void
    op_flip(data_type* const data, const size_t start, const size_t size) {
        if (size == 0) {
            return;
        }

        auto start_element = get_element(start);
        const auto end_element = get_element(start + size);

        const auto start_shift = get_shift(start);
        const auto end_shift = get_shift(start + size);

        // same element to modify?
        if (start_element == end_element) {
            const data_type existing_v = data[start_element];
            const data_type new_v = ~existing_v;

            const data_type existing_mask = get_shift_mask_begin(start_shift) |
                                            get_shift_mask_end(end_shift);
            const data_type new_mask = get_shift_mask_end(start_shift) &
                                       get_shift_mask_begin(end_shift);

            data[start_element] =
                (existing_v & existing_mask) | (new_v & new_mask);
            return;
        }

        // process the first element
        if (start_shift != 0) {
            const data_type existing_v = data[start_element];
            const data_type new_v = ~existing_v;

            const data_type existing_mask = get_shift_mask_begin(start_shift);
            const data_type new_mask = get_shift_mask_end(start_shift);

            data[start_element] =
                (existing_v & existing_mask) | (new_v & new_mask);
            start_element += 1;
        }

        // process the middle
        for (size_t i = start_element; i < end_element; i++) {
            data[i] = ~data[i];
        }

        // process the last element
        if (end_shift != 0) {
            const data_type existing_v = data[end_element];
            const data_type new_v = ~existing_v;

            const data_type existing_mask = get_shift_mask_end(end_shift);
            const data_type new_mask = get_shift_mask_begin(end_shift);

            data[end_element] =
                (existing_v & existing_mask) | (new_v & new_mask);
        }
    }

    static BITSET_ALWAYS_INLINE inline void
    op_and(data_type* const left,
           const data_type* const right,
           const size_t start_left,
           const size_t start_right,
           const size_t size) {
        op_func(left,
                right,
                start_left,
                start_right,
                size,
                [](const data_type left_v, const data_type right_v) {
                    return left_v & right_v;
                });
    }

    static BITSET_ALWAYS_INLINE inline void
    op_and_multiple(data_type* const left,
                    const data_type* const* const rights,
                    const size_t start_left,
                    const size_t* const __restrict start_rights,
                    const size_t n_rights,
                    const size_t size) {
        op_func(left,
                rights,
                start_left,
                start_rights,
                n_rights,
                size,
                [](const data_type left_v, const data_type right_v) {
                    return left_v & right_v;
                });
    }

    static BITSET_ALWAYS_INLINE inline void
    op_or(data_type* const left,
          const data_type* const right,
          const size_t start_left,
          const size_t start_right,
          const size_t size) {
        op_func(left,
                right,
                start_left,
                start_right,
                size,
                [](const data_type left_v, const data_type right_v) {
                    return left_v | right_v;
                });
    }

    static BITSET_ALWAYS_INLINE inline void
    op_or_multiple(data_type* const left,
                   const data_type* const* const rights,
                   const size_t start_left,
                   const size_t* const __restrict start_rights,
                   const size_t n_rights,
                   const size_t size) {
        op_func(left,
                rights,
                start_left,
                start_rights,
                n_rights,
                size,
                [](const data_type left_v, const data_type right_v) {
                    return left_v | right_v;
                });
    }

    static inline data_type
    get_shift_mask_begin(const size_t shift) {
        // 0 -> 0b00000000
        // 1 -> 0b00000001
        // 2 -> 0b00000011
        if (shift == data_bits) {
            return data_type(-1);
        }

        return (data_type(1) << shift) - data_type(1);
    }

    static inline data_type
    get_shift_mask_end(const size_t shift) {
        // 0 -> 0b11111111
        // 1 -> 0b11111110
        // 2 -> 0b11111100
        return ~(get_shift_mask_begin(shift));
    }

    static inline void
    op_set(data_type* const data, const size_t start, const size_t size) {
        op_fill(data, start, size, true);
    }

    static inline void
    op_reset(data_type* const data, const size_t start, const size_t size) {
        op_fill(data, start, size, false);
    }

    static inline bool
    op_all(const data_type* const data, const size_t start, const size_t size) {
        if (size == 0) {
            return true;
        }

        auto start_element = get_element(start);
        const auto end_element = get_element(start + size);

        const auto start_shift = get_shift(start);
        const auto end_shift = get_shift(start + size);

        // same element?
        if (start_element == end_element) {
            const data_type existing_v = data[start_element];

            const data_type existing_mask = get_shift_mask_end(start_shift) &
                                            get_shift_mask_begin(end_shift);

            return ((existing_v & existing_mask) == existing_mask);
        }

        // process the first element
        if (start_shift != 0) {
            const data_type existing_v = data[start_element];

            const data_type existing_mask = get_shift_mask_end(start_shift);
            if ((existing_v & existing_mask) != existing_mask) {
                return false;
            }

            start_element += 1;
        }

        // process the middle
        for (size_t i = start_element; i < end_element; i++) {
            if (data[i] != data_type(-1)) {
                return false;
            }
        }

        // process the last element
        if (end_shift != 0) {
            const data_type existing_v = data[end_element];

            const data_type existing_mask = get_shift_mask_begin(end_shift);

            if ((existing_v & existing_mask) != existing_mask) {
                return false;
            }
        }

        return true;
    }

    static inline bool
    op_none(const data_type* const data,
            const size_t start,
            const size_t size) {
        if (size == 0) {
            return true;
        }

        auto start_element = get_element(start);
        const auto end_element = get_element(start + size);

        const auto start_shift = get_shift(start);
        const auto end_shift = get_shift(start + size);

        // same element?
        if (start_element == end_element) {
            const data_type existing_v = data[start_element];

            const data_type existing_mask = get_shift_mask_end(start_shift) &
                                            get_shift_mask_begin(end_shift);

            return ((existing_v & existing_mask) == data_type(0));
        }

        // process the first element
        if (start_shift != 0) {
            const data_type existing_v = data[start_element];

            const data_type existing_mask = get_shift_mask_end(start_shift);
            if ((existing_v & existing_mask) != data_type(0)) {
                return false;
            }

            start_element += 1;
        }

        // process the middle
        for (size_t i = start_element; i < end_element; i++) {
            if (data[i] != data_type(0)) {
                return false;
            }
        }

        // process the last element
        if (end_shift != 0) {
            const data_type existing_v = data[end_element];

            const data_type existing_mask = get_shift_mask_begin(end_shift);

            if ((existing_v & existing_mask) != data_type(0)) {
                return false;
            }
        }

        return true;
    }

    static void
    op_copy(const data_type* const src,
            const size_t start_src,
            data_type* const dst,
            const size_t start_dst,
            const size_t size) {
        if (size == 0) {
            return;
        }

        // process big blocks
        const size_t size_b = (size / data_bits) * data_bits;

        if ((start_src % data_bits) == 0) {
            if ((start_dst % data_bits) == 0) {
                // plain memcpy
                for (size_t i = 0; i < size_b; i += data_bits) {
                    const data_type src_v = src[(start_src + i) / data_bits];
                    dst[(start_dst + i) / data_bits] = src_v;
                }
            } else {
                // easier read
                for (size_t i = 0; i < size_b; i += data_bits) {
                    const data_type src_v = src[(start_src + i) / data_bits];
                    op_write(dst, start_dst + i, data_bits, src_v);
                }
            }
        } else {
            if ((start_dst % data_bits) == 0) {
                // easier write
                for (size_t i = 0; i < size_b; i += data_bits) {
                    const data_type src_v =
                        op_read(src, start_src + i, data_bits);
                    dst[(start_dst + i) / data_bits] = src_v;
                }
            } else {
                // general case
                for (size_t i = 0; i < size_b; i += data_bits) {
                    const data_type src_v =
                        op_read(src, start_src + i, data_bits);
                    op_write(dst, start_dst + i, data_bits, src_v);
                }
            }
        }

        // process leftovers
        if (size_b != size) {
            const data_type src_v =
                op_read(src, start_src + size_b, size - size_b);
            op_write(dst, start_dst + size_b, size - size_b, src_v);
        }
    }

    static void
    op_fill(data_type* const data,
            const size_t start,
            const size_t size,
            const bool value) {
        if (size == 0) {
            return;
        }

        const data_type new_v = (value) ? data_type(-1) : data_type(0);

        //
        auto start_element = get_element(start);
        const auto end_element = get_element(start + size);

        const auto start_shift = get_shift(start);
        const auto end_shift = get_shift(start + size);

        // same element to modify?
        if (start_element == end_element) {
            const data_type existing_v = data[start_element];

            const data_type existing_mask = get_shift_mask_begin(start_shift) |
                                            get_shift_mask_end(end_shift);
            const data_type new_mask = get_shift_mask_end(start_shift) &
                                       get_shift_mask_begin(end_shift);

            data[start_element] =
                (existing_v & existing_mask) | (new_v & new_mask);
            return;
        }

        // process the first element
        if (start_shift != 0) {
            const data_type existing_v = data[start_element];

            const data_type existing_mask = get_shift_mask_begin(start_shift);
            const data_type new_mask = get_shift_mask_end(start_shift);

            data[start_element] =
                (existing_v & existing_mask) | (new_v & new_mask);
            start_element += 1;
        }

        // process the middle
        for (size_t i = start_element; i < end_element; i++) {
            data[i] = new_v;
        }

        // process the last element
        if (end_shift != 0) {
            const data_type existing_v = data[end_element];

            const data_type existing_mask = get_shift_mask_end(end_shift);
            const data_type new_mask = get_shift_mask_begin(end_shift);

            data[end_element] =
                (existing_v & existing_mask) | (new_v & new_mask);
        }
    }

    static inline size_t
    op_count(const data_type* const data,
             const size_t start,
             const size_t size) {
        if (size == 0) {
            return 0;
        }

        size_t count = 0;

        auto start_element = get_element(start);
        const auto end_element = get_element(start + size);

        const auto start_shift = get_shift(start);
        const auto end_shift = get_shift(start + size);

        // same element?
        if (start_element == end_element) {
            const data_type existing_v = data[start_element];

            const data_type existing_mask = get_shift_mask_end(start_shift) &
                                            get_shift_mask_begin(end_shift);

            return PopCountHelper<data_type>::count(existing_v & existing_mask);
        }

        // process the first element
        if (start_shift != 0) {
            const data_type existing_v = data[start_element];
            const data_type existing_mask = get_shift_mask_end(start_shift);

            count =
                PopCountHelper<data_type>::count(existing_v & existing_mask);

            start_element += 1;
        }

        // process the middle
        for (size_t i = start_element; i < end_element; i++) {
            count += PopCountHelper<data_type>::count(data[i]);
        }

        // process the last element
        if (end_shift != 0) {
            const data_type existing_v = data[end_element];
            const data_type existing_mask = get_shift_mask_begin(end_shift);

            count +=
                PopCountHelper<data_type>::count(existing_v & existing_mask);
        }

        return count;
    }

    static inline bool
    op_eq(const data_type* const left,
          const data_type* const right,
          const size_t start_left,
          const size_t start_right,
          const size_t size) {
        if (size == 0) {
            return true;
        }

        // process big chunks
        const size_t size_b = (size / data_bits) * data_bits;

        if ((start_left % data_bits) == 0) {
            if ((start_right % data_bits) == 0) {
                // plain "memcpy"
                size_t start_left_idx = start_left / data_bits;
                size_t start_right_idx = start_right / data_bits;

                for (size_t i = 0, j = 0; i < size_b; i += data_bits, j += 1) {
                    const data_type left_v = left[start_left_idx + j];
                    const data_type right_v = right[start_right_idx + j];
                    if (left_v != right_v) {
                        return false;
                    }
                }
            } else {
                // easier left
                size_t start_left_idx = start_left / data_bits;

                for (size_t i = 0, j = 0; i < size_b; i += data_bits, j += 1) {
                    const data_type left_v = left[start_left_idx + j];
                    const data_type right_v =
                        op_read(right, start_right + i, data_bits);
                    if (left_v != right_v) {
                        return false;
                    }
                }
            }
        } else {
            if ((start_right % data_bits) == 0) {
                // easier right
                size_t start_right_idx = start_right / data_bits;

                for (size_t i = 0, j = 0; i < size_b; i += data_bits, j += 1) {
                    const data_type left_v =
                        op_read(left, start_left + i, data_bits);
                    const data_type right_v = right[start_right_idx + j];
                    if (left_v != right_v) {
                        return false;
                    }
                }
            } else {
                // general case
                for (size_t i = 0; i < size_b; i += data_bits) {
                    const data_type left_v =
                        op_read(left, start_left + i, data_bits);
                    const data_type right_v =
                        op_read(right, start_right + i, data_bits);
                    if (left_v != right_v) {
                        return false;
                    }
                }
            }
        }

        // process leftovers
        if (size_b != size) {
            const data_type left_v =
                op_read(left, start_left + size_b, size - size_b);
            const data_type right_v =
                op_read(right, start_right + size_b, size - size_b);
            if (left_v != right_v) {
                return false;
            }
        }

        return true;
    }

    static BITSET_ALWAYS_INLINE inline void
    op_xor(data_type* const left,
           const data_type* const right,
           const size_t start_left,
           const size_t start_right,
           const size_t size) {
        op_func(left,
                right,
                start_left,
                start_right,
                size,
                [](const data_type left_v, const data_type right_v) {
                    return left_v ^ right_v;
                });
    }

    static BITSET_ALWAYS_INLINE inline void
    op_sub(data_type* const left,
           const data_type* const right,
           const size_t start_left,
           const size_t start_right,
           const size_t size) {
        op_func(left,
                right,
                start_left,
                start_right,
                size,
                [](const data_type left_v, const data_type right_v) {
                    return left_v & ~right_v;
                });
    }

    //
    static inline std::optional<size_t>
    op_find_1(const data_type* const data,
              const size_t start,
              const size_t size,
              const size_t starting_idx) {
        if (size == 0) {
            return std::nullopt;
        }

        //
        auto start_element = get_element(start + starting_idx);
        const auto end_element = get_element(start + size);

        const auto start_shift = get_shift(start + starting_idx);
        const auto end_shift = get_shift(start + size);

        // same element?
        if (start_element == end_element) {
            const data_type existing_v = data[start_element];

            const data_type existing_mask = get_shift_mask_end(start_shift) &
                                            get_shift_mask_begin(end_shift);

            const data_type value = existing_v & existing_mask;
            if (value != 0) {
                const auto ctz = CtzHelper<data_type>::ctz(value);
                return size_t(ctz) + start_element * data_bits - start;
            } else {
                return std::nullopt;
            }
        }

        // process the first element
        if (start_shift != 0) {
            const data_type existing_v = data[start_element];
            const data_type existing_mask = get_shift_mask_end(start_shift);

            const data_type value = existing_v & existing_mask;
            if (value != 0) {
                const auto ctz = CtzHelper<data_type>::ctz(value) +
                                 start_element * data_bits - start;
                return size_t(ctz);
            }

            start_element += 1;
        }

        // process the middle
        for (size_t i = start_element; i < end_element; i++) {
            const data_type value = data[i];
            if (value != 0) {
                const auto ctz = CtzHelper<data_type>::ctz(value);
                return size_t(ctz) + i * data_bits - start;
            }
        }

        // process the last element
        if (end_shift != 0) {
            const data_type existing_v = data[end_element];
            const data_type existing_mask = get_shift_mask_begin(end_shift);

            const data_type value = existing_v & existing_mask;
            if (value != 0) {
                const auto ctz = CtzHelper<data_type>::ctz(value);
                return size_t(ctz) + end_element * data_bits - start;
            }
        }

        return std::nullopt;
    }

    static inline std::optional<size_t>
    op_find_0(const data_type* const data,
              const size_t start,
              const size_t size,
              const size_t starting_idx) {
        if (size == 0) {
            return std::nullopt;
        }

        //
        auto start_element = get_element(start + starting_idx);
        const auto end_element = get_element(start + size);

        const auto start_shift = get_shift(start + starting_idx);
        const auto end_shift = get_shift(start + size);

        // same element?
        if (start_element == end_element) {
            const data_type existing_v = ~data[start_element];

            const data_type existing_mask = get_shift_mask_end(start_shift) &
                                            get_shift_mask_begin(end_shift);

            const data_type value = existing_v & existing_mask;
            if (value != 0) {
                const auto ctz = CtzHelper<data_type>::ctz(value);
                return size_t(ctz) + start_element * data_bits - start;
            } else {
                return std::nullopt;
            }
        }

        // process the first element
        if (start_shift != 0) {
            const data_type existing_v = ~data[start_element];
            const data_type existing_mask = get_shift_mask_end(start_shift);

            const data_type value = existing_v & existing_mask;
            if (value != 0) {
                const auto ctz = CtzHelper<data_type>::ctz(value) +
                                 start_element * data_bits - start;
                return size_t(ctz);
            }

            start_element += 1;
        }

        // process the middle
        for (size_t i = start_element; i < end_element; i++) {
            const data_type value = ~data[i];
            if (value != 0) {
                const auto ctz = CtzHelper<data_type>::ctz(value);
                return size_t(ctz) + i * data_bits - start;
            }
        }

        // process the last element
        if (end_shift != 0) {
            const data_type existing_v = ~data[end_element];
            const data_type existing_mask = get_shift_mask_begin(end_shift);

            const data_type value = existing_v & existing_mask;
            if (value != 0) {
                const auto ctz = CtzHelper<data_type>::ctz(value);
                return size_t(ctz) + end_element * data_bits - start;
            }
        }

        return std::nullopt;
    }

    //
    static inline std::optional<size_t>
    op_find(const data_type* const data,
            const size_t start,
            const size_t size,
            const size_t starting_idx,
            const bool is_set) {
        if (is_set) {
            return op_find_1(data, start, size, starting_idx);
        } else {
            return op_find_0(data, start, size, starting_idx);
        }
    }

    //
    template <typename T, typename U, CompareOpType Op>
    static inline void
    op_compare_column(data_type* const __restrict data,
                      const size_t start,
                      const T* const __restrict t,
                      const U* const __restrict u,
                      const size_t size) {
        op_func(data, start, size, [t, u](const size_t bit_idx) {
            return CompareOperator<Op>::compare(t[bit_idx], u[bit_idx]);
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
        op_func(data, start, size, [t, value](const size_t bit_idx) {
            return CompareOperator<Op>::compare(t[bit_idx], value);
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
            data, start, size, [lower, upper, values](const size_t bit_idx) {
                return RangeOperator<Op>::within_range(
                    lower[bit_idx], upper[bit_idx], values[bit_idx]);
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
            data, start, size, [lower, upper, values](const size_t bit_idx) {
                return RangeOperator<Op>::within_range(
                    lower, upper, values[bit_idx]);
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
        op_func(data,
                start,
                size,
                [src, right_operand, value](const size_t bit_idx) {
                    return ArithCompareOperator<AOp, CmpOp>::compare(
                        src[bit_idx], right_operand, value);
                });
    }

    //
    static inline size_t
    op_and_with_count(data_type* const left,
                      const data_type* const right,
                      const size_t start_left,
                      const size_t start_right,
                      const size_t size) {
        size_t active = 0;

        op_func(left,
                right,
                start_left,
                start_right,
                size,
                [&active](const data_type left_v, const data_type right_v) {
                    const data_type result = left_v & right_v;
                    active += PopCountHelper<data_type>::count(result);

                    return result;
                });

        return active;
    }

    static inline size_t
    op_or_with_count(data_type* const left,
                     const data_type* const right,
                     const size_t start_left,
                     const size_t start_right,
                     const size_t size) {
        size_t inactive = 0;

        const size_t size_b = (size / data_bits) * data_bits;

        // process bulk
        op_func(left,
                right,
                start_left,
                start_right,
                size_b,
                [&inactive](const data_type left_v, const data_type right_v) {
                    const data_type result = left_v | right_v;
                    inactive +=
                        (data_bits - PopCountHelper<data_type>::count(result));

                    return result;
                });

        // process leftovers
        if (size != size_b) {
            const data_type left_v =
                op_read(left, start_left + size_b, size - size_b);
            const data_type right_v =
                op_read(right, start_right + size_b, size - size_b);

            const data_type result_v = left_v | right_v;
            inactive +=
                (size - size_b - PopCountHelper<data_type>::count(result_v));
            op_write(left, start_left + size_b, size - size_b, result_v);
        }

        return inactive;
    }

    // data_type Func(const data_type left_v, const data_type right_v);
    template <typename Func>
    static BITSET_ALWAYS_INLINE inline void
    op_func(data_type* const left,
            const data_type* const right,
            const size_t start_left,
            const size_t start_right,
            const size_t size,
            Func func) {
        if (size == 0) {
            return;
        }

        // process big blocks
        const size_t size_b = (size / data_bits) * data_bits;
        if ((start_left % data_bits) == 0) {
            if ((start_right % data_bits) == 0) {
                // plain "memcpy".
                // A compiler auto-vectorization is expected.
                size_t start_left_idx = start_left / data_bits;
                size_t start_right_idx = start_right / data_bits;

                for (size_t i = 0, j = 0; i < size_b; i += data_bits, j += 1) {
                    data_type& left_v = left[start_left_idx + j];
                    const data_type right_v = right[start_right_idx + j];

                    const data_type result_v = func(left_v, right_v);
                    left_v = result_v;
                }
            } else {
                // easier read
                size_t start_left_idx = start_left / data_bits;

                for (size_t i = 0, j = 0; i < size_b; i += data_bits, j += 1) {
                    data_type& left_v = left[start_left_idx + j];
                    const data_type right_v =
                        op_read(right, start_right + i, data_bits);

                    const data_type result_v = func(left_v, right_v);
                    left_v = result_v;
                }
            }
        } else {
            if ((start_right % data_bits) == 0) {
                // easier write
                size_t start_right_idx = start_right / data_bits;

                for (size_t i = 0, j = 0; i < size_b; i += data_bits, j += 1) {
                    const data_type left_v =
                        op_read(left, start_left + i, data_bits);
                    const data_type right_v = right[start_right_idx + j];

                    const data_type result_v = func(left_v, right_v);
                    op_write(left, start_left + i, data_bits, result_v);
                }
            } else {
                // general case
                for (size_t i = 0; i < size_b; i += data_bits) {
                    const data_type left_v =
                        op_read(left, start_left + i, data_bits);
                    const data_type right_v =
                        op_read(right, start_right + i, data_bits);

                    const data_type result_v = func(left_v, right_v);
                    op_write(left, start_left + i, data_bits, result_v);
                }
            }
        }

        // process leftovers
        if (size_b != size) {
            const data_type left_v =
                op_read(left, start_left + size_b, size - size_b);
            const data_type right_v =
                op_read(right, start_right + size_b, size - size_b);

            const data_type result_v = func(left_v, right_v);
            op_write(left, start_left + size_b, size - size_b, result_v);
        }
    }

    // data_type Func(const data_type left_v, const data_type right_v);
    template <typename Func>
    static BITSET_ALWAYS_INLINE inline void
    op_func(data_type* const left,
            const data_type* const* const rights,
            const size_t start_left,
            const size_t* const __restrict start_rights,
            const size_t n_rights,
            const size_t size,
            Func func) {
        if (size == 0 || n_rights == 0) {
            return;
        }

        if (n_rights == 1) {
            op_func<Func>(
                left, rights[0], start_left, start_rights[0], size, func);
            return;
        }

        // process big blocks
        const size_t size_b = (size / data_bits) * data_bits;

        // check a specific case
        bool all_aligned = true;
        for (size_t i = 0; i < n_rights; i++) {
            if (start_rights[i] % data_bits != 0) {
                all_aligned = false;
                break;
            }
        }

        // all are aligned
        if (all_aligned) {
            MaybeVector<const data_type*> tmp(n_rights);
            for (size_t i = 0; i < n_rights; i++) {
                tmp[i] = rights[i] + (start_rights[i] / data_bits);
            }

            // plain "memcpy".
            // A compiler auto-vectorization is expected.
            const size_t start_left_idx = start_left / data_bits;
            data_type* left_ptr = left + start_left_idx;

            auto unrolled = [left_ptr, &tmp, func, size_b](const size_t count) {
                for (size_t i = 0, j = 0; i < size_b; i += data_bits, j += 1) {
                    data_type& left_v = left_ptr[j];
                    data_type value = left_v;

                    for (size_t k = 0; k < count; k++) {
                        const data_type right_v = tmp[k][j];

                        value = func(value, right_v);
                    }

                    left_v = value;
                }
            };

            switch (n_rights) {
                // case 1: unrolled(1); break;
                case 2:
                    unrolled(2);
                    break;
                case 3:
                    unrolled(3);
                    break;
                case 4:
                    unrolled(4);
                    break;
                case 5:
                    unrolled(5);
                    break;
                case 6:
                    unrolled(6);
                    break;
                case 7:
                    unrolled(7);
                    break;
                case 8:
                    unrolled(8);
                    break;
                default: {
                    for (size_t i = 0, j = 0; i < size_b;
                         i += data_bits, j += 1) {
                        data_type& left_v = left_ptr[j];
                        data_type value = left_v;

                        for (size_t k = 0; k < n_rights; k++) {
                            const data_type right_v = tmp[k][j];

                            value = func(value, right_v);
                        }

                        left_v = value;
                    }
                }
            }

        } else {
            // general case. Unoptimized.
            for (size_t i = 0; i < size_b; i += data_bits) {
                const data_type left_v =
                    op_read(left, start_left + i, data_bits);

                data_type value = left_v;
                for (size_t k = 0; k < n_rights; k++) {
                    const data_type right_v =
                        op_read(rights[k], start_rights[k] + i, data_bits);

                    value = func(value, right_v);
                }

                op_write(left, start_left + i, data_bits, value);
            }
        }

        // process leftovers
        if (size_b != size) {
            const data_type left_v =
                op_read(left, start_left + size_b, size - size_b);

            data_type value = left_v;
            for (size_t k = 0; k < n_rights; k++) {
                const data_type right_v =
                    op_read(rights[k], start_rights[k] + size_b, size - size_b);

                value = func(value, right_v);
            }

            op_write(left, start_left + size_b, size - size_b, value);
        }
    }

    // bool Func(const size_t bit_idx);
    template <typename Func>
    static BITSET_ALWAYS_INLINE inline void
    op_func(data_type* const __restrict data,
            const size_t start,
            const size_t size,
            Func func) {
        if (size == 0) {
            return;
        }

        auto start_element = get_element(start);
        const auto end_element = get_element(start + size);

        const auto start_shift = get_shift(start);
        const auto end_shift = get_shift(start + size);

        if (start_element == end_element) {
            data_type bits = 0;
            for (size_t j = 0; j < size; j++) {
                const bool bit = func(j);
                // // a curious example where the compiler does not optimize the code properly
                // bits |= (bit ? (data_type(1) << j) : 0);
                //
                // use the following code
                bits |= (data_type(bit ? 1 : 0) << j);
            }

            op_write(data, start, size, bits);
            return;
        }

        //
        uintptr_t ptr_offset = 0;

        // process the first element
        if (start_shift != 0) {
            const size_t n_bits = data_bits - start_shift;

            data_type bits = 0;
            for (size_t j = 0; j < n_bits; j++) {
                const bool bit = func(j);
                bits |= (data_type(bit ? 1 : 0) << j);
            }

            op_write(data, start, n_bits, bits);

            // start from the next element
            start_element += 1;
            ptr_offset += n_bits;
        }

        // process the middle
        {
            for (size_t i = start_element; i < end_element; i++) {
                data_type bits = 0;
                for (size_t j = 0; j < data_bits; j++) {
                    const bool bit = func(ptr_offset + j);
                    bits |= (data_type(bit ? 1 : 0) << j);
                }

                data[i] = bits;
                ptr_offset += data_bits;
            }
        }

        // process the last element
        if (end_shift != 0) {
            data_type bits = 0;
            for (size_t j = 0; j < end_shift; j++) {
                const bool bit = func(ptr_offset + j);
                bits |= (data_type(bit ? 1 : 0) << j);
            }

            const size_t starting_bit_idx = end_element * data_bits;
            op_write(data, starting_bit_idx, end_shift, bits);
        }
    }
};

}  // namespace detail
}  // namespace bitset
}  // namespace milvus
