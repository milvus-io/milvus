// Copyright (C) 2019-2024 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <gtest/gtest.h>
#include <stdio.h>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <limits>
#include <memory>
#include <random>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "bitset/bitset.h"
#include "bitset/common.h"
#include "bitset/detail/bit_wise.h"
#include "bitset/detail/element_vectorized.h"
#include "bitset/detail/element_wise.h"
#include "bitset/detail/platform/dynamic.h"
#include "bitset/detail/platform/vectorized_ref.h"
#include "gtest/gtest.h"

#if defined(__x86_64__)
#include "bitset/detail/platform/x86/avx2-decl.h"
#include "bitset/detail/platform/x86/avx2.h"
#include "bitset/detail/platform/x86/avx512.h"
#include "bitset/detail/platform/x86/instruction_set.h"
#endif

#if defined(__aarch64__)
#include "bitset/detail/platform/arm/neon.h"

#if defined(__ARM_FEATURE_SVE) && defined(BITSET_ENABLE_SVE_SUPPORT)
#include "bitset/detail/platform/arm/sve.h"
#endif

#endif

using namespace milvus::bitset;

//////////////////////////////////////////////////////////////////////////////////////////

// * The data is processed using ElementT,
// * A container stores the data using ContainerValueT elements,
// * VectorizerT defines the vectorization.

template <typename ElementT, typename ContainerValueT>
struct RefImplTraits {
    using policy_type = milvus::bitset::detail::BitWiseBitsetPolicy<ElementT>;
    using container_type = std::vector<ContainerValueT>;
    using bitset_type =
        milvus::bitset::Bitset<policy_type, container_type, false>;
    using bitset_view = milvus::bitset::BitsetView<policy_type, false>;
};

template <typename ElementT, typename ContainerValueT>
struct ElementImplTraits {
    using policy_type =
        milvus::bitset::detail::ElementWiseBitsetPolicy<ElementT>;
    using container_type = std::vector<ContainerValueT>;
    using bitset_type =
        milvus::bitset::Bitset<policy_type, container_type, false>;
    using bitset_view = milvus::bitset::BitsetView<policy_type, false>;
};

template <typename ElementT, typename ContainerValueT, typename VectorizerT>
struct VectorizedImplTraits {
    using policy_type =
        milvus::bitset::detail::VectorizedElementWiseBitsetPolicy<ElementT,
                                                                  VectorizerT>;
    using container_type = std::vector<ContainerValueT>;
    using bitset_type =
        milvus::bitset::Bitset<policy_type, container_type, false>;
    using bitset_view = milvus::bitset::BitsetView<policy_type, false>;
};

//////////////////////////////////////////////////////////////////////////////////////////

// set running mode to 1 to run a subset of tests
// set running mode to 2 to run benchmarks
// otherwise, all of the tests are run

#define RUNNING_MODE 1

#if RUNNING_MODE == 1
// short tests
static constexpr bool print_log = false;
static constexpr bool print_timing = false;

static constexpr size_t typical_sizes[] = {0, 1, 10, 100, 1000};
static constexpr size_t typical_offsets[] = {
    0, 1, 2, 3, 4, 5, 6, 7, 11, 21, 35, 55, 63, 127, 703};
static constexpr CompareOpType typical_compare_ops[] = {CompareOpType::EQ,
                                                        CompareOpType::GE,
                                                        CompareOpType::GT,
                                                        CompareOpType::LE,
                                                        CompareOpType::LT,
                                                        CompareOpType::NE};
static constexpr RangeType typical_range_types[] = {
    RangeType::IncInc, RangeType::IncExc, RangeType::ExcInc, RangeType::ExcExc};
static constexpr ArithOpType typical_arith_ops[] = {ArithOpType::Add,
                                                    ArithOpType::Sub,
                                                    ArithOpType::Mul,
                                                    ArithOpType::Div,
                                                    ArithOpType::Mod};

#elif RUNNING_MODE == 2

// benchmarks
static constexpr bool print_log = false;
static constexpr bool print_timing = true;

static constexpr size_t typical_sizes[] = {10000000};
static constexpr size_t typical_offsets[] = {1};
static constexpr CompareOpType typical_compare_ops[] = {CompareOpType::EQ,
                                                        CompareOpType::GE,
                                                        CompareOpType::GT,
                                                        CompareOpType::LE,
                                                        CompareOpType::LT,
                                                        CompareOpType::NE};
static constexpr RangeType typical_range_types[] = {
    RangeType::IncInc, RangeType::IncExc, RangeType::ExcInc, RangeType::ExcExc};
static constexpr ArithOpType typical_arith_ops[] = {ArithOpType::Add,
                                                    ArithOpType::Sub,
                                                    ArithOpType::Mul,
                                                    ArithOpType::Div,
                                                    ArithOpType::Mod};

#else

// full tests, mostly used for code coverage
static constexpr bool print_log = false;
static constexpr bool print_timing = false;

static constexpr size_t typical_sizes[] = {0,
                                           1,
                                           10,
                                           100,
                                           1000,
                                           10000,
                                           2048,
                                           2056,
                                           2064,
                                           2072,
                                           2080,
                                           2088,
                                           2096,
                                           2104,
                                           2112};
static constexpr size_t typical_offsets[] = {
    0,  1,   2,   3,   4,   5,   6,   7,   11,  21,  35,  45, 55,
    63, 127, 512, 520, 528, 536, 544, 556, 564, 572, 580, 703};
static constexpr CompareOpType typical_compare_ops[] = {CompareOpType::EQ,
                                                        CompareOpType::GE,
                                                        CompareOpType::GT,
                                                        CompareOpType::LE,
                                                        CompareOpType::LT,
                                                        CompareOpType::NE};
static constexpr RangeType typical_range_types[] = {
    RangeType::IncInc, RangeType::IncExc, RangeType::ExcInc, RangeType::ExcExc};
static constexpr ArithOpType typical_arith_ops[] = {ArithOpType::Add,
                                                    ArithOpType::Sub,
                                                    ArithOpType::Mul,
                                                    ArithOpType::Div,
                                                    ArithOpType::Mod};

#define FULL_TESTS 1
#endif

//////////////////////////////////////////////////////////////////////////////////////////

// combinations to run
using Ttypes2 = ::testing::Types<
#if FULL_TESTS == 1
    std::tuple<int8_t, int8_t, uint8_t, uint8_t>,
    std::tuple<int16_t, int16_t, uint8_t, uint8_t>,
    std::tuple<int32_t, int32_t, uint8_t, uint8_t>,
    std::tuple<int64_t, int64_t, uint8_t, uint8_t>,
    std::tuple<float, float, uint8_t, uint8_t>,
    std::tuple<double, double, uint8_t, uint8_t>,
    std::tuple<std::string, std::string, uint8_t, uint8_t>,
#endif

    std::tuple<int8_t, int8_t, uint64_t, uint8_t>,
    std::tuple<int16_t, int16_t, uint64_t, uint8_t>,
    std::tuple<int32_t, int32_t, uint64_t, uint8_t>,
    std::tuple<int64_t, int64_t, uint64_t, uint8_t>,
    std::tuple<float, float, uint64_t, uint8_t>,
    std::tuple<double, double, uint64_t, uint8_t>,
    std::tuple<std::string, std::string, uint64_t, uint8_t>

#if FULL_TESTS == 1
    ,
    std::tuple<int8_t, int8_t, uint8_t, uint64_t>,
    std::tuple<int16_t, int16_t, uint8_t, uint64_t>,
    std::tuple<int32_t, int32_t, uint8_t, uint64_t>,
    std::tuple<int64_t, int64_t, uint8_t, uint64_t>,
    std::tuple<float, float, uint8_t, uint64_t>,
    std::tuple<double, double, uint8_t, uint64_t>,
    std::tuple<std::string, std::string, uint8_t, uint64_t>,

    std::tuple<int8_t, int8_t, uint64_t, uint64_t>,
    std::tuple<int16_t, int16_t, uint64_t, uint64_t>,
    std::tuple<int32_t, int32_t, uint64_t, uint64_t>,
    std::tuple<int64_t, int64_t, uint64_t, uint64_t>,
    std::tuple<float, float, uint64_t, uint64_t>,
    std::tuple<double, double, uint64_t, uint64_t>,
    std::tuple<std::string, std::string, uint64_t, uint64_t>
#endif
    >;

// combinations to run
using Ttypes1 = ::testing::Types<
#if FULL_TESTS == 1
    std::tuple<int8_t, uint8_t, uint8_t>,
    std::tuple<int16_t, uint8_t, uint8_t>,
    std::tuple<int32_t, uint8_t, uint8_t>,
    std::tuple<int64_t, uint8_t, uint8_t>,
    std::tuple<float, uint8_t, uint8_t>,
    std::tuple<double, uint8_t, uint8_t>,
    std::tuple<std::string, uint8_t, uint8_t>,
#endif

    std::tuple<int8_t, uint64_t, uint8_t>,
    std::tuple<int16_t, uint64_t, uint8_t>,
    std::tuple<int32_t, uint64_t, uint8_t>,
    std::tuple<int64_t, uint64_t, uint8_t>,
    std::tuple<float, uint64_t, uint8_t>,
    std::tuple<double, uint64_t, uint8_t>,
    std::tuple<std::string, uint64_t, uint8_t>

#if FULL_TESTS == 1
    ,
    std::tuple<int8_t, uint8_t, uint64_t>,
    std::tuple<int16_t, uint8_t, uint64_t>,
    std::tuple<int32_t, uint8_t, uint64_t>,
    std::tuple<int64_t, uint8_t, uint64_t>,
    std::tuple<float, uint8_t, uint64_t>,
    std::tuple<double, uint8_t, uint64_t>,
    std::tuple<std::string, uint8_t, uint64_t>,

    std::tuple<int8_t, uint64_t, uint64_t>,
    std::tuple<int16_t, uint64_t, uint64_t>,
    std::tuple<int32_t, uint64_t, uint64_t>,
    std::tuple<int64_t, uint64_t, uint64_t>,
    std::tuple<float, uint64_t, uint64_t>,
    std::tuple<double, uint64_t, uint64_t>,
    std::tuple<std::string, uint64_t, uint64_t>
#endif
    >;

// combinations to run
using Ttypes0 = ::testing::Types<
#if FULL_TESTS == 1
    std::tuple<uint8_t, uint8_t>,
#endif

    std::tuple<uint64_t, uint8_t>

#if FULL_TESTS == 1
    ,
    std::tuple<uint8_t, uint64_t>,

    std::tuple<uint64_t, uint64_t>
#endif
    >;

//////////////////////////////////////////////////////////////////////////////////////////

struct StopWatch {
    using time_type =
        std::chrono::time_point<std::chrono::high_resolution_clock>;
    time_type start;

    StopWatch() {
        start = now();
    }

    inline double
    elapsed() {
        auto current = now();
        return std::chrono::duration<double>(current - start).count();
    }

    static inline time_type
    now() {
        return std::chrono::high_resolution_clock::now();
    }
};

//
template <typename T>
void
FillRandom(std::vector<T>& t,
           std::default_random_engine& rng,
           const size_t max_v) {
    std::uniform_int_distribution<uint8_t> tt(0, max_v);
    for (size_t i = 0; i < t.size(); i++) {
        t[i] = tt(rng);
    }
}

template <typename T>
void
FillRandomRange(std::vector<T>& t,
                std::default_random_engine& rng,
                const int32_t min_v,
                const int32_t max_v) {
    std::uniform_int_distribution<int32_t> tt(0, max_v);
    for (size_t i = 0; i < t.size(); i++) {
        t[i] = static_cast<T>(tt(rng));
    }
}

template <>
void
FillRandom<std::string>(std::vector<std::string>& t,
                        std::default_random_engine& rng,
                        const size_t max_v) {
    std::uniform_int_distribution<uint8_t> tt(0, max_v);
    for (size_t i = 0; i < t.size(); i++) {
        t[i] = std::to_string(tt(rng));
    }
}

template <typename BitsetT>
void
FillRandom(BitsetT& bitset,
           std::default_random_engine& rng,
           const size_t offset = 0) {
    std::uniform_int_distribution<uint8_t> tt(0, 1);
    for (size_t i = 0; i < bitset.size() - offset; i++) {
        bitset[offset + i] = (tt(rng) == 0);
    }
}

//
template <typename T>
T
from_i32(const int32_t i) {
    return T(i);
}

template <>
std::string
from_i32(const int32_t i) {
    return std::to_string(i);
}

//////////////////////////////////////////////////////////////////////////////////////////

//
template <typename BitsetT>
void
TestFindImpl(BitsetT& owner,
             const size_t max_v,
             const bool is_set,
             const size_t offset = 0) {
    auto bitset = owner.view(offset);
    const size_t n = bitset.size();

    std::default_random_engine rng(123);
    std::uniform_int_distribution<int8_t> u(0, max_v);

    std::vector<size_t> one_pos;
    for (size_t i = 0; i < n; i++) {
        bool enabled = (u(rng) == 0);
        if (enabled) {
            one_pos.push_back(i);
            owner[offset + i] = true;
        }
    }

    if (!is_set) {
        owner.flip(offset, n);
    }

    StopWatch sw;

    auto bit_idx = bitset.find_first(is_set);
    if (!bit_idx.has_value()) {
        ASSERT_EQ(one_pos.size(), 0);
        return;
    }

    for (size_t i = 0; i < one_pos.size(); i++) {
        ASSERT_TRUE(bit_idx.has_value()) << n << ", " << max_v;
        ASSERT_EQ(bit_idx.value(), one_pos[i]) << n << ", " << max_v;
        bit_idx = bitset.find_next(bit_idx.value(), is_set);
    }

    ASSERT_FALSE(bit_idx.has_value())
        << n << ", " << max_v << ", " << bit_idx.value();

    if (print_timing) {
        printf("elapsed %f\n", sw.elapsed());
    }
}

template <typename BitsetT>
void
TestFindImpl() {
    for (const size_t n : typical_sizes) {
        for (const bool is_set : {true, false}) {
            for (const size_t pr : {1, 100}) {
                BitsetT bitset(n);
                bitset.reset();

                if (print_log) {
                    printf("Testing bitset, n=%zd, is_set=%d, pr=%zd\n",
                           n,
                           (is_set) ? 1 : 0,
                           pr);
                }

                TestFindImpl(bitset, pr, is_set);

                for (const size_t offset : typical_offsets) {
                    if (offset >= n) {
                        continue;
                    }

                    bitset.reset();

                    if (print_log) {
                        printf(
                            "Testing bitset view, n=%zd, offset=%zd, "
                            "is_set=%d, pr=%zd\n",
                            n,
                            offset,
                            (is_set) ? 1 : 0,
                            pr);
                    }

                    TestFindImpl(bitset, pr, is_set, offset);
                }
            }
        }
    }
}

//
template <typename T>
class FindSuite : public ::testing::Test {};

TYPED_TEST_SUITE_P(FindSuite);

//
TYPED_TEST_P(FindSuite, BitWise) {
    using impl_traits = RefImplTraits<std::tuple_element_t<0, TypeParam>,
                                      std::tuple_element_t<1, TypeParam>>;
    TestFindImpl<typename impl_traits::bitset_type>();
}

TYPED_TEST_P(FindSuite, ElementWise) {
    using impl_traits = ElementImplTraits<std::tuple_element_t<0, TypeParam>,
                                          std::tuple_element_t<1, TypeParam>>;
    TestFindImpl<typename impl_traits::bitset_type>();
}

TYPED_TEST_P(FindSuite, Avx2) {
#if defined(__x86_64__)
    using namespace milvus::bitset::detail::x86;

    if (cpu_support_avx2()) {
        using impl_traits =
            VectorizedImplTraits<std::tuple_element_t<0, TypeParam>,
                                 std::tuple_element_t<1, TypeParam>,
                                 milvus::bitset::detail::x86::VectorizedAvx2>;
        TestFindImpl<typename impl_traits::bitset_type>();
    }
#endif
}

TYPED_TEST_P(FindSuite, Avx512) {
#if defined(__x86_64__)
    using namespace milvus::bitset::detail::x86;

    if (cpu_support_avx512()) {
        using impl_traits =
            VectorizedImplTraits<std::tuple_element_t<0, TypeParam>,
                                 std::tuple_element_t<1, TypeParam>,
                                 milvus::bitset::detail::x86::VectorizedAvx512>;
        TestFindImpl<typename impl_traits::bitset_type>();
    }
#endif
}

TYPED_TEST_P(FindSuite, Neon) {
#if defined(__aarch64__)
    using namespace milvus::bitset::detail::arm;

    using impl_traits =
        VectorizedImplTraits<std::tuple_element_t<0, TypeParam>,
                             std::tuple_element_t<1, TypeParam>,
                             milvus::bitset::detail::arm::VectorizedNeon>;
    TestFindImpl<typename impl_traits::bitset_type>();
#endif
}

TYPED_TEST_P(FindSuite, Sve) {
#if defined(__aarch64__) && defined(__ARM_FEATURE_SVE) && \
    defined(BITSET_ENABLE_SVE_SUPPORT)
    using namespace milvus::bitset::detail::arm;

    using impl_traits =
        VectorizedImplTraits<std::tuple_element_t<0, TypeParam>,
                             std::tuple_element_t<1, TypeParam>,
                             milvus::bitset::detail::arm::VectorizedSve>;
    TestFindImpl<typename impl_traits::bitset_type>();
#endif
}

TYPED_TEST_P(FindSuite, Dynamic) {
    using impl_traits =
        VectorizedImplTraits<std::tuple_element_t<0, TypeParam>,
                             std::tuple_element_t<1, TypeParam>,
                             milvus::bitset::detail::VectorizedDynamic>;
    TestFindImpl<typename impl_traits::bitset_type>();
}

TYPED_TEST_P(FindSuite, VecRef) {
    using impl_traits =
        VectorizedImplTraits<std::tuple_element_t<0, TypeParam>,
                             std::tuple_element_t<1, TypeParam>,
                             milvus::bitset::detail::VectorizedRef>;
    TestFindImpl<typename impl_traits::bitset_type>();
}

//
REGISTER_TYPED_TEST_SUITE_P(
    FindSuite, BitWise, ElementWise, Avx2, Avx512, Neon, Sve, Dynamic, VecRef);

INSTANTIATE_TYPED_TEST_SUITE_P(FindTest, FindSuite, Ttypes0);

//////////////////////////////////////////////////////////////////////////////////////////

//
template <typename BitsetT, typename T, typename U>
void
TestInplaceCompareColumnImpl(BitsetT& owner,
                             CompareOpType op,
                             const size_t offset = 0) {
    auto bitset = owner.view(offset);
    const size_t n = bitset.size();
    constexpr size_t max_v = 2;

    std::vector<T> t(n, from_i32<T>(0));
    std::vector<U> u(n, from_i32<T>(0));

    std::default_random_engine rng(123);
    FillRandom(t, rng, max_v);
    FillRandom(u, rng, max_v);

    StopWatch sw;
    owner.inplace_compare_column(t.data(), u.data(), n, op, offset);

    if (print_timing) {
        printf("elapsed %f\n", sw.elapsed());
    }

    for (size_t i = 0; i < n; i++) {
        if (op == CompareOpType::EQ) {
            ASSERT_EQ(t[i] == u[i], bitset[i]) << i;
        } else if (op == CompareOpType::GE) {
            ASSERT_EQ(t[i] >= u[i], bitset[i]) << i;
        } else if (op == CompareOpType::GT) {
            ASSERT_EQ(t[i] > u[i], bitset[i]) << i;
        } else if (op == CompareOpType::LE) {
            ASSERT_EQ(t[i] <= u[i], bitset[i]) << i;
        } else if (op == CompareOpType::LT) {
            ASSERT_EQ(t[i] < u[i], bitset[i]) << i;
        } else if (op == CompareOpType::NE) {
            ASSERT_EQ(t[i] != u[i], bitset[i]) << i;
        } else {
            ASSERT_TRUE(false) << "Not implemented";
        }
    }
}

template <typename BitsetT, typename T, typename U>
void
TestInplaceCompareColumnImpl() {
    for (const size_t n : typical_sizes) {
        for (const auto op : typical_compare_ops) {
            BitsetT bitset(n);
            bitset.reset();

            if (print_log) {
                printf("Testing bitset, n=%zd, op=%zd\n", n, (size_t)op);
            }

            TestInplaceCompareColumnImpl<BitsetT, T, U>(bitset, op);

            for (const size_t offset : typical_offsets) {
                if (offset >= n) {
                    continue;
                }

                bitset.reset();

                if (print_log) {
                    printf("Testing bitset view, n=%zd, offset=%zd, op=%zd\n",
                           n,
                           offset,
                           (size_t)op);
                }

                TestInplaceCompareColumnImpl<BitsetT, T, U>(bitset, op, offset);
            }
        }
    }
}

//
template <typename T>
class InplaceCompareColumnSuite : public ::testing::Test {};

TYPED_TEST_SUITE_P(InplaceCompareColumnSuite);

//
TYPED_TEST_P(InplaceCompareColumnSuite, BitWise) {
    using impl_traits = RefImplTraits<std::tuple_element_t<2, TypeParam>,
                                      std::tuple_element_t<3, TypeParam>>;
    TestInplaceCompareColumnImpl<typename impl_traits::bitset_type,
                                 std::tuple_element_t<0, TypeParam>,
                                 std::tuple_element_t<1, TypeParam>>();
}

//
TYPED_TEST_P(InplaceCompareColumnSuite, ElementWise) {
    using impl_traits = ElementImplTraits<std::tuple_element_t<2, TypeParam>,
                                          std::tuple_element_t<3, TypeParam>>;
    TestInplaceCompareColumnImpl<typename impl_traits::bitset_type,
                                 std::tuple_element_t<0, TypeParam>,
                                 std::tuple_element_t<1, TypeParam>>();
}

//
TYPED_TEST_P(InplaceCompareColumnSuite, Avx2) {
#if defined(__x86_64__)
    using namespace milvus::bitset::detail::x86;

    if (cpu_support_avx2()) {
        using impl_traits =
            VectorizedImplTraits<std::tuple_element_t<2, TypeParam>,
                                 std::tuple_element_t<3, TypeParam>,
                                 milvus::bitset::detail::x86::VectorizedAvx2>;
        TestInplaceCompareColumnImpl<typename impl_traits::bitset_type,
                                     std::tuple_element_t<0, TypeParam>,
                                     std::tuple_element_t<1, TypeParam>>();
    }
#endif
}

//
TYPED_TEST_P(InplaceCompareColumnSuite, Avx512) {
#if defined(__x86_64__)
    using namespace milvus::bitset::detail::x86;

    if (cpu_support_avx512()) {
        using impl_traits =
            VectorizedImplTraits<std::tuple_element_t<2, TypeParam>,
                                 std::tuple_element_t<3, TypeParam>,
                                 milvus::bitset::detail::x86::VectorizedAvx512>;
        TestInplaceCompareColumnImpl<typename impl_traits::bitset_type,
                                     std::tuple_element_t<0, TypeParam>,
                                     std::tuple_element_t<1, TypeParam>>();
    }
#endif
}

//
TYPED_TEST_P(InplaceCompareColumnSuite, Neon) {
#if defined(__aarch64__)
    using namespace milvus::bitset::detail::arm;

    using impl_traits =
        VectorizedImplTraits<std::tuple_element_t<2, TypeParam>,
                             std::tuple_element_t<3, TypeParam>,
                             milvus::bitset::detail::arm::VectorizedNeon>;
    TestInplaceCompareColumnImpl<typename impl_traits::bitset_type,
                                 std::tuple_element_t<0, TypeParam>,
                                 std::tuple_element_t<1, TypeParam>>();
#endif
}

//
TYPED_TEST_P(InplaceCompareColumnSuite, Sve) {
#if defined(__aarch64__) && defined(__ARM_FEATURE_SVE) && \
    defined(BITSET_ENABLE_SVE_SUPPORT)
    using namespace milvus::bitset::detail::arm;

    using impl_traits =
        VectorizedImplTraits<std::tuple_element_t<2, TypeParam>,
                             std::tuple_element_t<3, TypeParam>,
                             milvus::bitset::detail::arm::VectorizedSve>;
    TestInplaceCompareColumnImpl<typename impl_traits::bitset_type,
                                 std::tuple_element_t<0, TypeParam>,
                                 std::tuple_element_t<1, TypeParam>>();
#endif
}

//
TYPED_TEST_P(InplaceCompareColumnSuite, Dynamic) {
    using impl_traits =
        VectorizedImplTraits<std::tuple_element_t<2, TypeParam>,
                             std::tuple_element_t<3, TypeParam>,
                             milvus::bitset::detail::VectorizedDynamic>;
    TestInplaceCompareColumnImpl<typename impl_traits::bitset_type,
                                 std::tuple_element_t<0, TypeParam>,
                                 std::tuple_element_t<1, TypeParam>>();
}

//
TYPED_TEST_P(InplaceCompareColumnSuite, VecRef) {
    using impl_traits =
        VectorizedImplTraits<std::tuple_element_t<2, TypeParam>,
                             std::tuple_element_t<3, TypeParam>,
                             milvus::bitset::detail::VectorizedRef>;
    TestInplaceCompareColumnImpl<typename impl_traits::bitset_type,
                                 std::tuple_element_t<0, TypeParam>,
                                 std::tuple_element_t<1, TypeParam>>();
}

//
REGISTER_TYPED_TEST_SUITE_P(InplaceCompareColumnSuite,
                            BitWise,
                            ElementWise,
                            Avx2,
                            Avx512,
                            Neon,
                            Sve,
                            Dynamic,
                            VecRef);

INSTANTIATE_TYPED_TEST_SUITE_P(InplaceCompareColumnTest,
                               InplaceCompareColumnSuite,
                               Ttypes2);

//////////////////////////////////////////////////////////////////////////////////////////

//
template <typename BitsetT, typename T>
void
TestInplaceCompareValImpl(BitsetT& owner,
                          CompareOpType op,
                          const size_t offset = 0) {
    auto bitset = owner.view(offset);
    const size_t n = bitset.size();
    constexpr size_t max_v = 3;
    const T value = from_i32<T>(1);

    std::vector<T> t(n, from_i32<T>(0));

    std::default_random_engine rng(123);
    FillRandom(t, rng, max_v);

    StopWatch sw;
    owner.inplace_compare_val(t.data(), n, value, op, offset);

    if (print_timing) {
        printf("elapsed %f\n", sw.elapsed());
    }

    for (size_t i = 0; i < n; i++) {
        if (op == CompareOpType::EQ) {
            ASSERT_EQ(t[i] == value, bitset[i]) << i;
        } else if (op == CompareOpType::GE) {
            ASSERT_EQ(t[i] >= value, bitset[i]) << i;
        } else if (op == CompareOpType::GT) {
            ASSERT_EQ(t[i] > value, bitset[i]) << i;
        } else if (op == CompareOpType::LE) {
            ASSERT_EQ(t[i] <= value, bitset[i]) << i;
        } else if (op == CompareOpType::LT) {
            ASSERT_EQ(t[i] < value, bitset[i]) << i;
        } else if (op == CompareOpType::NE) {
            ASSERT_EQ(t[i] != value, bitset[i]) << i;
        } else {
            ASSERT_TRUE(false) << "Not implemented";
        }
    }
}

template <typename BitsetT, typename T>
void
TestInplaceCompareValImpl() {
    for (const size_t n : typical_sizes) {
        for (const auto op : typical_compare_ops) {
            BitsetT bitset(n);
            bitset.reset();

            if (print_log) {
                printf("Testing bitset, n=%zd, op=%zd\n", n, (size_t)op);
            }

            TestInplaceCompareValImpl<BitsetT, T>(bitset, op);

            for (const size_t offset : typical_offsets) {
                if (offset >= n) {
                    continue;
                }

                bitset.reset();

                if (print_log) {
                    printf("Testing bitset view, n=%zd, offset=%zd, op=%zd\n",
                           n,
                           offset,
                           (size_t)op);
                }

                TestInplaceCompareValImpl<BitsetT, T>(bitset, op, offset);
            }
        }
    }
}

//
template <typename T>
class InplaceCompareValSuite : public ::testing::Test {};

TYPED_TEST_SUITE_P(InplaceCompareValSuite);

TYPED_TEST_P(InplaceCompareValSuite, BitWise) {
    using impl_traits = RefImplTraits<std::tuple_element_t<1, TypeParam>,
                                      std::tuple_element_t<2, TypeParam>>;
    TestInplaceCompareValImpl<typename impl_traits::bitset_type,
                              std::tuple_element_t<0, TypeParam>>();
}

TYPED_TEST_P(InplaceCompareValSuite, ElementWise) {
    using impl_traits = ElementImplTraits<std::tuple_element_t<1, TypeParam>,
                                          std::tuple_element_t<2, TypeParam>>;
    TestInplaceCompareValImpl<typename impl_traits::bitset_type,
                              std::tuple_element_t<0, TypeParam>>();
}

TYPED_TEST_P(InplaceCompareValSuite, Avx2) {
#if defined(__x86_64__)
    using namespace milvus::bitset::detail::x86;

    if (cpu_support_avx2()) {
        using impl_traits =
            VectorizedImplTraits<std::tuple_element_t<1, TypeParam>,
                                 std::tuple_element_t<2, TypeParam>,
                                 milvus::bitset::detail::x86::VectorizedAvx2>;
        TestInplaceCompareValImpl<typename impl_traits::bitset_type,
                                  std::tuple_element_t<0, TypeParam>>();
    }
#endif
}

TYPED_TEST_P(InplaceCompareValSuite, Avx512) {
#if defined(__x86_64__)
    using namespace milvus::bitset::detail::x86;

    if (cpu_support_avx512()) {
        using impl_traits =
            VectorizedImplTraits<std::tuple_element_t<1, TypeParam>,
                                 std::tuple_element_t<2, TypeParam>,
                                 milvus::bitset::detail::x86::VectorizedAvx512>;
        TestInplaceCompareValImpl<typename impl_traits::bitset_type,
                                  std::tuple_element_t<0, TypeParam>>();
    }
#endif
}

TYPED_TEST_P(InplaceCompareValSuite, Neon) {
#if defined(__aarch64__)
    using namespace milvus::bitset::detail::arm;

    using impl_traits =
        VectorizedImplTraits<std::tuple_element_t<1, TypeParam>,
                             std::tuple_element_t<2, TypeParam>,
                             milvus::bitset::detail::arm::VectorizedNeon>;
    TestInplaceCompareValImpl<typename impl_traits::bitset_type,
                              std::tuple_element_t<0, TypeParam>>();
#endif
}

TYPED_TEST_P(InplaceCompareValSuite, Sve) {
#if defined(__aarch64__) && defined(__ARM_FEATURE_SVE) && \
    defined(BITSET_ENABLE_SVE_SUPPORT)
    using namespace milvus::bitset::detail::arm;

    using impl_traits =
        VectorizedImplTraits<std::tuple_element_t<1, TypeParam>,
                             std::tuple_element_t<2, TypeParam>,
                             milvus::bitset::detail::arm::VectorizedSve>;
    TestInplaceCompareValImpl<typename impl_traits::bitset_type,
                              std::tuple_element_t<0, TypeParam>>();
#endif
}

TYPED_TEST_P(InplaceCompareValSuite, Dynamic) {
    using impl_traits =
        VectorizedImplTraits<std::tuple_element_t<1, TypeParam>,
                             std::tuple_element_t<2, TypeParam>,
                             milvus::bitset::detail::VectorizedDynamic>;
    TestInplaceCompareValImpl<typename impl_traits::bitset_type,
                              std::tuple_element_t<0, TypeParam>>();
}

TYPED_TEST_P(InplaceCompareValSuite, VecRef) {
    using impl_traits =
        VectorizedImplTraits<std::tuple_element_t<1, TypeParam>,
                             std::tuple_element_t<2, TypeParam>,
                             milvus::bitset::detail::VectorizedRef>;
    TestInplaceCompareValImpl<typename impl_traits::bitset_type,
                              std::tuple_element_t<0, TypeParam>>();
}

//
REGISTER_TYPED_TEST_SUITE_P(InplaceCompareValSuite,
                            BitWise,
                            ElementWise,
                            Avx2,
                            Avx512,
                            Neon,
                            Sve,
                            Dynamic,
                            VecRef);

INSTANTIATE_TYPED_TEST_SUITE_P(InplaceCompareValTest,
                               InplaceCompareValSuite,
                               Ttypes1);

//////////////////////////////////////////////////////////////////////////////////////////

//
template <typename BitsetT, typename T>
void
TestInplaceWithinRangeColumnImpl(BitsetT& owner,
                                 RangeType op,
                                 const size_t offset = 0) {
    auto bitset = owner.view(offset);
    const size_t n = bitset.size();
    constexpr size_t max_v = 3;

    std::vector<T> range(n, from_i32<T>(0));
    std::vector<T> values(n, from_i32<T>(0));

    std::vector<T> lower(n, from_i32<T>(0));
    std::vector<T> upper(n, from_i32<T>(0));

    std::default_random_engine rng(123);
    FillRandom(lower, rng, max_v);
    FillRandom(range, rng, max_v);
    FillRandom(values, rng, 2 * max_v);

    for (size_t i = 0; i < n; i++) {
        upper[i] = lower[i] + range[i];
    }

    StopWatch sw;
    owner.inplace_within_range_column(
        lower.data(), upper.data(), values.data(), n, op, offset);

    if (print_timing) {
        printf("elapsed %f\n", sw.elapsed());
    }

    for (size_t i = 0; i < n; i++) {
        if (op == RangeType::IncInc) {
            ASSERT_EQ(lower[i] <= values[i] && values[i] <= upper[i], bitset[i])
                << i << " " << lower[i] << " " << values[i] << " " << upper[i];
        } else if (op == RangeType::IncExc) {
            ASSERT_EQ(lower[i] <= values[i] && values[i] < upper[i], bitset[i])
                << i << " " << lower[i] << " " << values[i] << " " << upper[i];
        } else if (op == RangeType::ExcInc) {
            ASSERT_EQ(lower[i] < values[i] && values[i] <= upper[i], bitset[i])
                << i << " " << lower[i] << " " << values[i] << " " << upper[i];
        } else if (op == RangeType::ExcExc) {
            ASSERT_EQ(lower[i] < values[i] && values[i] < upper[i], bitset[i])
                << i << " " << lower[i] << " " << values[i] << " " << upper[i];
        } else {
            ASSERT_TRUE(false) << "Not implemented";
        }
    }
}

template <typename BitsetT, typename T>
void
TestInplaceWithinRangeColumnImpl() {
    for (const size_t n : typical_sizes) {
        for (const auto op : typical_range_types) {
            BitsetT bitset(n);
            bitset.reset();

            if (print_log) {
                printf("Testing bitset, n=%zd, op=%zd\n", n, (size_t)op);
            }

            TestInplaceWithinRangeColumnImpl<BitsetT, T>(bitset, op);

            for (const size_t offset : typical_offsets) {
                if (offset >= n) {
                    continue;
                }

                bitset.reset();

                if (print_log) {
                    printf("Testing bitset view, n=%zd, offset=%zd, op=%zd\n",
                           n,
                           offset,
                           (size_t)op);
                }

                TestInplaceWithinRangeColumnImpl<BitsetT, T>(
                    bitset, op, offset);
            }
        }
    }
}

//
template <typename T>
class InplaceWithinRangeColumnSuite : public ::testing::Test {};

TYPED_TEST_SUITE_P(InplaceWithinRangeColumnSuite);

TYPED_TEST_P(InplaceWithinRangeColumnSuite, BitWise) {
    using impl_traits = RefImplTraits<std::tuple_element_t<1, TypeParam>,
                                      std::tuple_element_t<2, TypeParam>>;
    TestInplaceWithinRangeColumnImpl<typename impl_traits::bitset_type,
                                     std::tuple_element_t<0, TypeParam>>();
}

TYPED_TEST_P(InplaceWithinRangeColumnSuite, ElementWise) {
    using impl_traits = ElementImplTraits<std::tuple_element_t<1, TypeParam>,
                                          std::tuple_element_t<2, TypeParam>>;
    TestInplaceWithinRangeColumnImpl<typename impl_traits::bitset_type,
                                     std::tuple_element_t<0, TypeParam>>();
}

TYPED_TEST_P(InplaceWithinRangeColumnSuite, Avx2) {
#if defined(__x86_64__)
    using namespace milvus::bitset::detail::x86;

    if (cpu_support_avx2()) {
        using impl_traits =
            VectorizedImplTraits<std::tuple_element_t<1, TypeParam>,
                                 std::tuple_element_t<2, TypeParam>,
                                 milvus::bitset::detail::x86::VectorizedAvx2>;
        TestInplaceWithinRangeColumnImpl<typename impl_traits::bitset_type,
                                         std::tuple_element_t<0, TypeParam>>();
    }
#endif
}

TYPED_TEST_P(InplaceWithinRangeColumnSuite, Avx512) {
#if defined(__x86_64__)
    using namespace milvus::bitset::detail::x86;

    if (cpu_support_avx512()) {
        using impl_traits =
            VectorizedImplTraits<std::tuple_element_t<1, TypeParam>,
                                 std::tuple_element_t<2, TypeParam>,
                                 milvus::bitset::detail::x86::VectorizedAvx512>;
        TestInplaceWithinRangeColumnImpl<typename impl_traits::bitset_type,
                                         std::tuple_element_t<0, TypeParam>>();
    }
#endif
}

TYPED_TEST_P(InplaceWithinRangeColumnSuite, Neon) {
#if defined(__aarch64__)
    using namespace milvus::bitset::detail::arm;

    using impl_traits =
        VectorizedImplTraits<std::tuple_element_t<1, TypeParam>,
                             std::tuple_element_t<2, TypeParam>,
                             milvus::bitset::detail::arm::VectorizedNeon>;
    TestInplaceWithinRangeColumnImpl<typename impl_traits::bitset_type,
                                     std::tuple_element_t<0, TypeParam>>();
#endif
}

TYPED_TEST_P(InplaceWithinRangeColumnSuite, Sve) {
#if defined(__aarch64__) && defined(__ARM_FEATURE_SVE) && \
    defined(BITSET_ENABLE_SVE_SUPPORT)
    using namespace milvus::bitset::detail::arm;

    using impl_traits =
        VectorizedImplTraits<std::tuple_element_t<1, TypeParam>,
                             std::tuple_element_t<2, TypeParam>,
                             milvus::bitset::detail::arm::VectorizedSve>;
    TestInplaceWithinRangeColumnImpl<typename impl_traits::bitset_type,
                                     std::tuple_element_t<0, TypeParam>>();
#endif
}

TYPED_TEST_P(InplaceWithinRangeColumnSuite, Dynamic) {
    using impl_traits =
        VectorizedImplTraits<std::tuple_element_t<1, TypeParam>,
                             std::tuple_element_t<2, TypeParam>,
                             milvus::bitset::detail::VectorizedDynamic>;
    TestInplaceWithinRangeColumnImpl<typename impl_traits::bitset_type,
                                     std::tuple_element_t<0, TypeParam>>();
}

TYPED_TEST_P(InplaceWithinRangeColumnSuite, VecRef) {
    using impl_traits =
        VectorizedImplTraits<std::tuple_element_t<1, TypeParam>,
                             std::tuple_element_t<2, TypeParam>,
                             milvus::bitset::detail::VectorizedRef>;
    TestInplaceWithinRangeColumnImpl<typename impl_traits::bitset_type,
                                     std::tuple_element_t<0, TypeParam>>();
}

//
REGISTER_TYPED_TEST_SUITE_P(InplaceWithinRangeColumnSuite,
                            BitWise,
                            ElementWise,
                            Avx2,
                            Avx512,
                            Neon,
                            Sve,
                            Dynamic,
                            VecRef);

INSTANTIATE_TYPED_TEST_SUITE_P(InplaceWithinRangeColumnTest,
                               InplaceWithinRangeColumnSuite,
                               Ttypes1);

//////////////////////////////////////////////////////////////////////////////////////////

//
template <typename BitsetT, typename T>
void
TestInplaceWithinRangeValImpl(BitsetT& owner,
                              RangeType op,
                              const size_t offset = 0) {
    auto bitset = owner.view(offset);
    const size_t n = bitset.size();
    constexpr size_t max_v = 10;
    const T lower_v = from_i32<T>(3);
    const T upper_v = from_i32<T>(7);

    std::vector<T> values(n, from_i32<T>(0));

    std::default_random_engine rng(123);
    FillRandom(values, rng, max_v);

    StopWatch sw;
    owner.inplace_within_range_val(
        lower_v, upper_v, values.data(), n, op, offset);

    if (print_timing) {
        printf("elapsed %f\n", sw.elapsed());
    }

    for (size_t i = 0; i < n; i++) {
        if (op == RangeType::IncInc) {
            ASSERT_EQ(lower_v <= values[i] && values[i] <= upper_v, bitset[i])
                << i << " " << lower_v << " " << values[i] << " " << upper_v;
        } else if (op == RangeType::IncExc) {
            ASSERT_EQ(lower_v <= values[i] && values[i] < upper_v, bitset[i])
                << i << " " << lower_v << " " << values[i] << " " << upper_v;
        } else if (op == RangeType::ExcInc) {
            ASSERT_EQ(lower_v < values[i] && values[i] <= upper_v, bitset[i])
                << i << " " << lower_v << " " << values[i] << " " << upper_v;
        } else if (op == RangeType::ExcExc) {
            ASSERT_EQ(lower_v < values[i] && values[i] < upper_v, bitset[i])
                << i << " " << lower_v << " " << values[i] << " " << upper_v;
        } else {
            ASSERT_TRUE(false) << "Not implemented";
        }
    }
}

template <typename BitsetT, typename T>
void
TestInplaceWithinRangeValImpl() {
    for (const size_t n : typical_sizes) {
        for (const auto op : typical_range_types) {
            BitsetT bitset(n);
            bitset.reset();

            if (print_log) {
                printf("Testing bitset, n=%zd, op=%zd\n", n, (size_t)op);
            }

            TestInplaceWithinRangeValImpl<BitsetT, T>(bitset, op);

            for (const size_t offset : typical_offsets) {
                if (offset >= n) {
                    continue;
                }

                bitset.reset();

                if (print_log) {
                    printf("Testing bitset view, n=%zd, offset=%zd, op=%zd\n",
                           n,
                           offset,
                           (size_t)op);
                }

                TestInplaceWithinRangeValImpl<BitsetT, T>(bitset, op, offset);
            }
        }
    }
}

//
template <typename T>
class InplaceWithinRangeValSuite : public ::testing::Test {};

TYPED_TEST_SUITE_P(InplaceWithinRangeValSuite);

TYPED_TEST_P(InplaceWithinRangeValSuite, BitWise) {
    using impl_traits = RefImplTraits<std::tuple_element_t<1, TypeParam>,
                                      std::tuple_element_t<2, TypeParam>>;
    TestInplaceWithinRangeValImpl<typename impl_traits::bitset_type,
                                  std::tuple_element_t<0, TypeParam>>();
}

TYPED_TEST_P(InplaceWithinRangeValSuite, ElementWise) {
    using impl_traits = ElementImplTraits<std::tuple_element_t<1, TypeParam>,
                                          std::tuple_element_t<2, TypeParam>>;
    TestInplaceWithinRangeValImpl<typename impl_traits::bitset_type,
                                  std::tuple_element_t<0, TypeParam>>();
}

TYPED_TEST_P(InplaceWithinRangeValSuite, Avx2) {
#if defined(__x86_64__)
    using namespace milvus::bitset::detail::x86;

    if (cpu_support_avx2()) {
        using impl_traits =
            VectorizedImplTraits<std::tuple_element_t<1, TypeParam>,
                                 std::tuple_element_t<2, TypeParam>,
                                 milvus::bitset::detail::x86::VectorizedAvx2>;
        TestInplaceWithinRangeValImpl<typename impl_traits::bitset_type,
                                      std::tuple_element_t<0, TypeParam>>();
    }
#endif
}

TYPED_TEST_P(InplaceWithinRangeValSuite, Avx512) {
#if defined(__x86_64__)
    using namespace milvus::bitset::detail::x86;

    if (cpu_support_avx512()) {
        using impl_traits =
            VectorizedImplTraits<std::tuple_element_t<1, TypeParam>,
                                 std::tuple_element_t<2, TypeParam>,
                                 milvus::bitset::detail::x86::VectorizedAvx512>;
        TestInplaceWithinRangeValImpl<typename impl_traits::bitset_type,
                                      std::tuple_element_t<0, TypeParam>>();
    }
#endif
}

TYPED_TEST_P(InplaceWithinRangeValSuite, Neon) {
#if defined(__aarch64__)
    using namespace milvus::bitset::detail::arm;

    using impl_traits =
        VectorizedImplTraits<std::tuple_element_t<1, TypeParam>,
                             std::tuple_element_t<2, TypeParam>,
                             milvus::bitset::detail::arm::VectorizedNeon>;
    TestInplaceWithinRangeValImpl<typename impl_traits::bitset_type,
                                  std::tuple_element_t<0, TypeParam>>();
#endif
}

TYPED_TEST_P(InplaceWithinRangeValSuite, Sve) {
#if defined(__aarch64__) && defined(__ARM_FEATURE_SVE) && \
    defined(BITSET_ENABLE_SVE_SUPPORT)
    using namespace milvus::bitset::detail::arm;

    using impl_traits =
        VectorizedImplTraits<std::tuple_element_t<1, TypeParam>,
                             std::tuple_element_t<2, TypeParam>,
                             milvus::bitset::detail::arm::VectorizedSve>;
    TestInplaceWithinRangeValImpl<typename impl_traits::bitset_type,
                                  std::tuple_element_t<0, TypeParam>>();
#endif
}

TYPED_TEST_P(InplaceWithinRangeValSuite, Dynamic) {
    using impl_traits =
        VectorizedImplTraits<std::tuple_element_t<1, TypeParam>,
                             std::tuple_element_t<2, TypeParam>,
                             milvus::bitset::detail::VectorizedDynamic>;
    TestInplaceWithinRangeValImpl<typename impl_traits::bitset_type,
                                  std::tuple_element_t<0, TypeParam>>();
}

TYPED_TEST_P(InplaceWithinRangeValSuite, VecRef) {
    using impl_traits =
        VectorizedImplTraits<std::tuple_element_t<1, TypeParam>,
                             std::tuple_element_t<2, TypeParam>,
                             milvus::bitset::detail::VectorizedRef>;
    TestInplaceWithinRangeValImpl<typename impl_traits::bitset_type,
                                  std::tuple_element_t<0, TypeParam>>();
}

//
REGISTER_TYPED_TEST_SUITE_P(InplaceWithinRangeValSuite,
                            BitWise,
                            ElementWise,
                            Avx2,
                            Avx512,
                            Neon,
                            Sve,
                            Dynamic,
                            VecRef);

INSTANTIATE_TYPED_TEST_SUITE_P(InplaceWithinRangeValTest,
                               InplaceWithinRangeValSuite,
                               Ttypes1);

//////////////////////////////////////////////////////////////////////////////////////////

template <typename BitsetT, typename T>
struct TestInplaceArithCompareImplS {
    static void
    process(BitsetT& owner,
            ArithOpType a_op,
            CompareOpType cmp_op,
            const int32_t right_operand_in,
            const int32_t value_in,
            const size_t offset = 0) {
        using HT = ArithHighPrecisionType<T>;

        auto bitset = owner.view(offset);
        const size_t n = bitset.size();
        constexpr int32_t max_v = 10;

        std::vector<T> left(n, 0);
        const HT right_operand = from_i32<HT>(right_operand_in);
        const HT value = from_i32<HT>(value_in);

        std::default_random_engine rng(123);
        // Generating values in (-x, x) range.
        // This is fine, because we're operating with signed integers.
        FillRandomRange(left, rng, -max_v, max_v);

        StopWatch sw;
        owner.inplace_arith_compare(
            left.data(), right_operand, value, n, a_op, cmp_op, offset);

        if (print_timing) {
            printf("elapsed %f\n", sw.elapsed());
        }

        for (size_t i = 0; i < n; i++) {
            if (a_op == ArithOpType::Add) {
                if (cmp_op == CompareOpType::EQ) {
                    ASSERT_EQ((left[i] + right_operand) == value, bitset[i])
                        << i << " " << size_t(cmp_op) << " " << left[i] << " "
                        << right_operand << " " << value;
                } else if (cmp_op == CompareOpType::GE) {
                    ASSERT_EQ((left[i] + right_operand) >= value, bitset[i])
                        << i << " " << size_t(cmp_op) << " " << left[i] << " "
                        << right_operand << " " << value;
                } else if (cmp_op == CompareOpType::GT) {
                    ASSERT_EQ((left[i] + right_operand) > value, bitset[i])
                        << i << " " << size_t(cmp_op) << " " << left[i] << " "
                        << right_operand << " " << value;
                } else if (cmp_op == CompareOpType::LE) {
                    ASSERT_EQ((left[i] + right_operand) <= value, bitset[i])
                        << i << " " << size_t(cmp_op) << " " << left[i] << " "
                        << right_operand << " " << value;
                } else if (cmp_op == CompareOpType::LT) {
                    ASSERT_EQ((left[i] + right_operand) < value, bitset[i])
                        << i << " " << size_t(cmp_op) << " " << left[i] << " "
                        << right_operand << " " << value;
                } else if (cmp_op == CompareOpType::NE) {
                    ASSERT_EQ((left[i] + right_operand) != value, bitset[i])
                        << i << " " << size_t(cmp_op) << " " << left[i] << " "
                        << right_operand << " " << value;
                } else {
                    ASSERT_TRUE(false) << "Not implemented";
                }
            } else if (a_op == ArithOpType::Sub) {
                if (cmp_op == CompareOpType::EQ) {
                    ASSERT_EQ((left[i] - right_operand) == value, bitset[i])
                        << i << " " << size_t(cmp_op) << " " << left[i] << " "
                        << right_operand << " " << value;
                } else if (cmp_op == CompareOpType::GE) {
                    ASSERT_EQ((left[i] - right_operand) >= value, bitset[i])
                        << i << " " << size_t(cmp_op) << " " << left[i] << " "
                        << right_operand << " " << value;
                } else if (cmp_op == CompareOpType::GT) {
                    ASSERT_EQ((left[i] - right_operand) > value, bitset[i])
                        << i << " " << size_t(cmp_op) << " " << left[i] << " "
                        << right_operand << " " << value;
                } else if (cmp_op == CompareOpType::LE) {
                    ASSERT_EQ((left[i] - right_operand) <= value, bitset[i])
                        << i << " " << size_t(cmp_op) << " " << left[i] << " "
                        << right_operand << " " << value;
                } else if (cmp_op == CompareOpType::LT) {
                    ASSERT_EQ((left[i] - right_operand) < value, bitset[i])
                        << i << " " << size_t(cmp_op) << " " << left[i] << " "
                        << right_operand << " " << value;
                } else if (cmp_op == CompareOpType::NE) {
                    ASSERT_EQ((left[i] - right_operand) != value, bitset[i])
                        << i << " " << size_t(cmp_op) << " " << left[i] << " "
                        << right_operand << " " << value;
                } else {
                    ASSERT_TRUE(false) << "Not implemented";
                }
            } else if (a_op == ArithOpType::Mul) {
                if (cmp_op == CompareOpType::EQ) {
                    ASSERT_EQ((left[i] * right_operand) == value, bitset[i])
                        << i << " " << size_t(cmp_op) << " " << left[i] << " "
                        << right_operand << " " << value;
                } else if (cmp_op == CompareOpType::GE) {
                    ASSERT_EQ((left[i] * right_operand) >= value, bitset[i])
                        << i << " " << size_t(cmp_op) << " " << left[i] << " "
                        << right_operand << " " << value;
                } else if (cmp_op == CompareOpType::GT) {
                    ASSERT_EQ((left[i] * right_operand) > value, bitset[i])
                        << i << " " << size_t(cmp_op) << " " << left[i] << " "
                        << right_operand << " " << value;
                } else if (cmp_op == CompareOpType::LE) {
                    ASSERT_EQ((left[i] * right_operand) <= value, bitset[i])
                        << i << " " << size_t(cmp_op) << " " << left[i] << " "
                        << right_operand << " " << value;
                } else if (cmp_op == CompareOpType::LT) {
                    ASSERT_EQ((left[i] * right_operand) < value, bitset[i])
                        << i << " " << size_t(cmp_op) << " " << left[i] << " "
                        << right_operand << " " << value;
                } else if (cmp_op == CompareOpType::NE) {
                    ASSERT_EQ((left[i] * right_operand) != value, bitset[i])
                        << i << " " << size_t(cmp_op) << " " << left[i] << " "
                        << right_operand << " " << value;
                } else {
                    ASSERT_TRUE(false) << "Not implemented";
                }
            } else if (a_op == ArithOpType::Div) {
                if (cmp_op == CompareOpType::EQ) {
                    ASSERT_EQ((left[i] / right_operand) == value, bitset[i])
                        << i << " " << size_t(cmp_op) << " " << left[i] << " "
                        << right_operand << " " << value;
                } else if (cmp_op == CompareOpType::GE) {
                    ASSERT_EQ((left[i] / right_operand) >= value, bitset[i])
                        << i << " " << size_t(cmp_op) << " " << left[i] << " "
                        << right_operand << " " << value;
                } else if (cmp_op == CompareOpType::GT) {
                    ASSERT_EQ((left[i] / right_operand) > value, bitset[i])
                        << i << " " << size_t(cmp_op) << " " << left[i] << " "
                        << right_operand << " " << value;
                } else if (cmp_op == CompareOpType::LE) {
                    ASSERT_EQ((left[i] / right_operand) <= value, bitset[i])
                        << i << " " << size_t(cmp_op) << " " << left[i] << " "
                        << right_operand << " " << value;
                } else if (cmp_op == CompareOpType::LT) {
                    ASSERT_EQ((left[i] / right_operand) < value, bitset[i])
                        << i << " " << size_t(cmp_op) << " " << left[i] << " "
                        << right_operand << " " << value;
                } else if (cmp_op == CompareOpType::NE) {
                    ASSERT_EQ((left[i] / right_operand) != value, bitset[i])
                        << i << " " << size_t(cmp_op) << " " << left[i] << " "
                        << right_operand << " " << value;
                } else {
                    ASSERT_TRUE(false) << "Not implemented";
                }
            } else if (a_op == ArithOpType::Mod) {
                if (cmp_op == CompareOpType::EQ) {
                    ASSERT_EQ(fmod(left[i], right_operand) == value, bitset[i])
                        << i << " " << size_t(cmp_op) << " " << left[i] << " "
                        << right_operand << " " << value;
                } else if (cmp_op == CompareOpType::GE) {
                    ASSERT_EQ(fmod(left[i], right_operand) >= value, bitset[i])
                        << i << " " << size_t(cmp_op) << " " << left[i] << " "
                        << right_operand << " " << value;
                } else if (cmp_op == CompareOpType::GT) {
                    ASSERT_EQ(fmod(left[i], right_operand) > value, bitset[i])
                        << i << " " << size_t(cmp_op) << " " << left[i] << " "
                        << right_operand << " " << value;
                } else if (cmp_op == CompareOpType::LE) {
                    ASSERT_EQ(fmod(left[i], right_operand) <= value, bitset[i])
                        << i << " " << size_t(cmp_op) << " " << left[i] << " "
                        << right_operand << " " << value;
                } else if (cmp_op == CompareOpType::LT) {
                    ASSERT_EQ(fmod(left[i], right_operand) < value, bitset[i])
                        << i << " " << size_t(cmp_op) << " " << left[i] << " "
                        << right_operand << " " << value;
                } else if (cmp_op == CompareOpType::NE) {
                    ASSERT_EQ(fmod(left[i], right_operand) != value, bitset[i])
                        << i << " " << size_t(cmp_op) << " " << left[i] << " "
                        << right_operand << " " << value;
                } else {
                    ASSERT_TRUE(false) << "Not implemented";
                }
            } else {
                ASSERT_TRUE(false) << "Not implemented";
            }
        }
    }

    static void
    process_div_special(BitsetT& bitset,
                        CompareOpType cmp_op,
                        const T left_v,
                        const T right_v,
                        const T value_v) {
        // test a single special point for the division

        using HT = ArithHighPrecisionType<T>;

        const size_t n = bitset.size();

        std::vector<T> left(n, left_v);
        const HT right_operand = right_v;
        const HT value = value_v;

        bitset.inplace_arith_compare(
            left.data(), right_operand, value, n, ArithOpType::Div, cmp_op);

        for (size_t i = 0; i < n; i++) {
            if (cmp_op == CompareOpType::EQ) {
                ASSERT_EQ((left[i] / right_operand) == value, bitset[i])
                    << i << " " << size_t(cmp_op) << " " << left[i] << " "
                    << right_operand << " " << value;
            } else if (cmp_op == CompareOpType::GE) {
                ASSERT_EQ((left[i] / right_operand) >= value, bitset[i])
                    << i << " " << size_t(cmp_op) << " " << left[i] << " "
                    << right_operand << " " << value;
            } else if (cmp_op == CompareOpType::GT) {
                ASSERT_EQ((left[i] / right_operand) > value, bitset[i])
                    << i << " " << size_t(cmp_op) << " " << left[i] << " "
                    << right_operand << " " << value;
            } else if (cmp_op == CompareOpType::LE) {
                ASSERT_EQ((left[i] / right_operand) <= value, bitset[i])
                    << i << " " << size_t(cmp_op) << " " << left[i] << " "
                    << right_operand << " " << value;
            } else if (cmp_op == CompareOpType::LT) {
                ASSERT_EQ((left[i] / right_operand) < value, bitset[i])
                    << i << " " << size_t(cmp_op) << " " << left[i] << " "
                    << right_operand << " " << value;
            } else if (cmp_op == CompareOpType::NE) {
                ASSERT_EQ((left[i] / right_operand) != value, bitset[i])
                    << i << " " << size_t(cmp_op) << " " << left[i] << " "
                    << right_operand << " " << value;
            } else {
                ASSERT_TRUE(false) << "Not implemented";
            }
        }
    }
};

template <typename BitsetT>
struct TestInplaceArithCompareImplS<BitsetT, std::string> {
    static void
    process(
        BitsetT&, ArithOpType, CompareOpType, const int32_t, const int32_t) {
        // does nothing
    }
};

template <typename BitsetT, typename T>
void
TestInplaceArithCompareImpl() {
    if constexpr (std::is_floating_point_v<T>)
        for (const size_t n : typical_sizes) {
            for (const auto a_op : typical_arith_ops) {
                for (const auto cmp_op : typical_compare_ops) {
                    // test both positive, zero and negative
                    for (const int32_t right_operand : {2, 0, -2}) {
                        if ((!std::is_floating_point_v<T> ||
                             a_op == milvus::bitset::ArithOpType::Mod) &&
                            right_operand == 0) {
                            continue;
                        }

                        // test both positive, zero and negative
                        for (const int32_t value : {2, 0, -2}) {
                            BitsetT bitset(n);
                            bitset.reset();

                            if (print_log) {
                                printf(
                                    "Testing bitset, n=%zd, a_op=%zd, "
                                    "cmp_op=%zd, right_operand=%d\n",
                                    n,
                                    (size_t)a_op,
                                    (size_t)cmp_op,
                                    right_operand);
                            }

                            TestInplaceArithCompareImplS<BitsetT, T>::process(
                                bitset, a_op, cmp_op, right_operand, value);

                            for (const size_t offset : typical_offsets) {
                                if (offset >= n) {
                                    continue;
                                }

                                bitset.reset();

                                if (print_log) {
                                    printf(
                                        "Testing bitset view, n=%zd, "
                                        "offset=%zd, a_op=%zd, cmp_op=%zd, "
                                        "right_operand=%d\n",
                                        n,
                                        offset,
                                        (size_t)a_op,
                                        (size_t)cmp_op,
                                        right_operand);
                                }

                                TestInplaceArithCompareImplS<BitsetT, T>::
                                    process(bitset,
                                            a_op,
                                            cmp_op,
                                            right_operand,
                                            value,
                                            offset);
                            }
                        }
                    }
                }
            }
        }

    if constexpr (std::is_floating_point_v<T>) {
        // test various special use cases for IEEE-754 for the division operation.
        std::vector<T> variety = {0,
                                  1,
                                  -1,
                                  std::numeric_limits<T>::quiet_NaN(),
                                  -std::numeric_limits<T>::quiet_NaN(),
                                  std::numeric_limits<T>::infinity(),
                                  -std::numeric_limits<T>::infinity()};

        for (const auto cmp_op : typical_compare_ops) {
            for (const T left_v : variety) {
                for (const T right_v : variety) {
                    for (const T value_v : variety) {
                        // 40 should be sufficient to test avx512
                        BitsetT bitset(40);
                        bitset.reset();

                        if (print_log) {
                            printf(
                                "Testing bitset div special case, cmp_op=%zd, "
                                "left_v=%f, right_v=%f, value_v=%f\n",
                                (size_t)cmp_op,
                                left_v,
                                right_v,
                                value_v);
                        }

                        TestInplaceArithCompareImplS<BitsetT, T>::
                            process_div_special(
                                bitset, cmp_op, left_v, right_v, value_v);
                    }
                }
            }
        }
    }
}

//
template <typename T>
class InplaceArithCompareSuite : public ::testing::Test {};

TYPED_TEST_SUITE_P(InplaceArithCompareSuite);

TYPED_TEST_P(InplaceArithCompareSuite, BitWise) {
    using impl_traits = RefImplTraits<std::tuple_element_t<1, TypeParam>,
                                      std::tuple_element_t<2, TypeParam>>;
    TestInplaceArithCompareImpl<typename impl_traits::bitset_type,
                                std::tuple_element_t<0, TypeParam>>();
}

TYPED_TEST_P(InplaceArithCompareSuite, ElementWise) {
    using impl_traits = ElementImplTraits<std::tuple_element_t<1, TypeParam>,
                                          std::tuple_element_t<2, TypeParam>>;
    TestInplaceArithCompareImpl<typename impl_traits::bitset_type,
                                std::tuple_element_t<0, TypeParam>>();
}

TYPED_TEST_P(InplaceArithCompareSuite, Avx2) {
#if defined(__x86_64__)
    using namespace milvus::bitset::detail::x86;

    if (cpu_support_avx2()) {
        using impl_traits =
            VectorizedImplTraits<std::tuple_element_t<1, TypeParam>,
                                 std::tuple_element_t<2, TypeParam>,
                                 milvus::bitset::detail::x86::VectorizedAvx2>;
        TestInplaceArithCompareImpl<typename impl_traits::bitset_type,
                                    std::tuple_element_t<0, TypeParam>>();
    }
#endif
}

TYPED_TEST_P(InplaceArithCompareSuite, Avx512) {
#if defined(__x86_64__)
    using namespace milvus::bitset::detail::x86;

    if (cpu_support_avx512()) {
        using impl_traits =
            VectorizedImplTraits<std::tuple_element_t<1, TypeParam>,
                                 std::tuple_element_t<2, TypeParam>,
                                 milvus::bitset::detail::x86::VectorizedAvx512>;
        TestInplaceArithCompareImpl<typename impl_traits::bitset_type,
                                    std::tuple_element_t<0, TypeParam>>();
    }
#endif
}

TYPED_TEST_P(InplaceArithCompareSuite, Neon) {
#if defined(__aarch64__)
    using namespace milvus::bitset::detail::arm;

    using impl_traits =
        VectorizedImplTraits<std::tuple_element_t<1, TypeParam>,
                             std::tuple_element_t<2, TypeParam>,
                             milvus::bitset::detail::arm::VectorizedNeon>;
    TestInplaceArithCompareImpl<typename impl_traits::bitset_type,
                                std::tuple_element_t<0, TypeParam>>();
#endif
}

TYPED_TEST_P(InplaceArithCompareSuite, Sve) {
#if defined(__aarch64__) && defined(__ARM_FEATURE_SVE) && \
    defined(BITSET_ENABLE_SVE_SUPPORT)
    using namespace milvus::bitset::detail::arm;

    using impl_traits =
        VectorizedImplTraits<std::tuple_element_t<1, TypeParam>,
                             std::tuple_element_t<2, TypeParam>,
                             milvus::bitset::detail::arm::VectorizedSve>;
    TestInplaceArithCompareImpl<typename impl_traits::bitset_type,
                                std::tuple_element_t<0, TypeParam>>();
#endif
}

TYPED_TEST_P(InplaceArithCompareSuite, Dynamic) {
    using impl_traits =
        VectorizedImplTraits<std::tuple_element_t<1, TypeParam>,
                             std::tuple_element_t<2, TypeParam>,
                             milvus::bitset::detail::VectorizedDynamic>;
    TestInplaceArithCompareImpl<typename impl_traits::bitset_type,
                                std::tuple_element_t<0, TypeParam>>();
}

TYPED_TEST_P(InplaceArithCompareSuite, VecRef) {
    using impl_traits =
        VectorizedImplTraits<std::tuple_element_t<1, TypeParam>,
                             std::tuple_element_t<2, TypeParam>,
                             milvus::bitset::detail::VectorizedRef>;
    TestInplaceArithCompareImpl<typename impl_traits::bitset_type,
                                std::tuple_element_t<0, TypeParam>>();
}

//
REGISTER_TYPED_TEST_SUITE_P(InplaceArithCompareSuite,
                            BitWise,
                            ElementWise,
                            Avx2,
                            Avx512,
                            Neon,
                            Sve,
                            Dynamic,
                            VecRef);

INSTANTIATE_TYPED_TEST_SUITE_P(InplaceArithCompareTest,
                               InplaceArithCompareSuite,
                               Ttypes1);

//////////////////////////////////////////////////////////////////////////////////////////

template <typename BitsetT, typename BitsetU>
void
TestAppendImpl(BitsetT& bitset_dst, const BitsetU& bitset_src) {
    std::vector<bool> b_dst;
    b_dst.reserve(bitset_src.size() + bitset_dst.size());

    for (size_t i = 0; i < bitset_dst.size(); i++) {
        b_dst.push_back(bitset_dst[i]);
    }
    for (size_t i = 0; i < bitset_src.size(); i++) {
        b_dst.push_back(bitset_src[i]);
    }

    StopWatch sw;
    bitset_dst.append(bitset_src);

    if (print_timing) {
        printf("elapsed %f\n", sw.elapsed());
    }

    //
    ASSERT_EQ(b_dst.size(), bitset_dst.size());
    for (size_t i = 0; i < bitset_dst.size(); i++) {
        ASSERT_EQ(b_dst[i], bitset_dst[i]) << i;
    }
}

template <typename BitsetT>
void
TestAppendImpl() {
    std::default_random_engine rng(345);

    std::vector<BitsetT> bt0;
    for (const size_t n : typical_sizes) {
        BitsetT bitset(n);
        FillRandom(bitset, rng);
        bt0.push_back(std::move(bitset));
    }

    std::vector<BitsetT> bt1;
    for (const size_t n : typical_sizes) {
        BitsetT bitset(n);
        FillRandom(bitset, rng);
        bt1.push_back(std::move(bitset));
    }

    for (const auto& bt_a : bt0) {
        for (const auto& bt_b : bt1) {
            auto bt = bt_a.clone();

            if (print_log) {
                printf(
                    "Testing bitset, n=%zd, m=%zd\n", bt_a.size(), bt_b.size());
            }

            TestAppendImpl(bt, bt_b);

            for (const size_t offset : typical_offsets) {
                if (offset >= bt_b.size()) {
                    continue;
                }

                bt = bt_a.clone();
                auto view = bt_b.view(offset);

                if (print_log) {
                    printf("Testing bitset view, n=%zd, m=%zd, offset=%zd\n",
                           bt_a.size(),
                           bt_b.size(),
                           offset);
                }

                TestAppendImpl(bt, view);
            }
        }
    }
}

//
template <typename T>
class AppendSuite : public ::testing::Test {};

TYPED_TEST_SUITE_P(AppendSuite);

//
TYPED_TEST_P(AppendSuite, BitWise) {
    using impl_traits = RefImplTraits<std::tuple_element_t<0, TypeParam>,
                                      std::tuple_element_t<1, TypeParam>>;
    TestAppendImpl<typename impl_traits::bitset_type>();
}

TYPED_TEST_P(AppendSuite, ElementWise) {
    using impl_traits = ElementImplTraits<std::tuple_element_t<0, TypeParam>,
                                          std::tuple_element_t<1, TypeParam>>;
    TestAppendImpl<typename impl_traits::bitset_type>();
}

TYPED_TEST_P(AppendSuite, Avx2) {
#if defined(__x86_64__)
    using namespace milvus::bitset::detail::x86;

    if (cpu_support_avx2()) {
        using impl_traits =
            VectorizedImplTraits<std::tuple_element_t<0, TypeParam>,
                                 std::tuple_element_t<1, TypeParam>,
                                 milvus::bitset::detail::x86::VectorizedAvx2>;
        TestAppendImpl<typename impl_traits::bitset_type>();
    }
#endif
}

TYPED_TEST_P(AppendSuite, Avx512) {
#if defined(__x86_64__)
    using namespace milvus::bitset::detail::x86;

    if (cpu_support_avx512()) {
        using impl_traits =
            VectorizedImplTraits<std::tuple_element_t<0, TypeParam>,
                                 std::tuple_element_t<1, TypeParam>,
                                 milvus::bitset::detail::x86::VectorizedAvx512>;
        TestAppendImpl<typename impl_traits::bitset_type>();
    }
#endif
}

TYPED_TEST_P(AppendSuite, Neon) {
#if defined(__aarch64__)
    using namespace milvus::bitset::detail::arm;

    using impl_traits =
        VectorizedImplTraits<std::tuple_element_t<0, TypeParam>,
                             std::tuple_element_t<1, TypeParam>,
                             milvus::bitset::detail::arm::VectorizedNeon>;
    TestAppendImpl<typename impl_traits::bitset_type>();
#endif
}

TYPED_TEST_P(AppendSuite, Sve) {
#if defined(__aarch64__) && defined(__ARM_FEATURE_SVE) && \
    defined(BITSET_ENABLE_SVE_SUPPORT)
    using namespace milvus::bitset::detail::arm;

    using impl_traits =
        VectorizedImplTraits<std::tuple_element_t<0, TypeParam>,
                             std::tuple_element_t<1, TypeParam>,
                             milvus::bitset::detail::arm::VectorizedSve>;
    TestAppendImpl<typename impl_traits::bitset_type>();
#endif
}

TYPED_TEST_P(AppendSuite, Dynamic) {
    using impl_traits =
        VectorizedImplTraits<std::tuple_element_t<0, TypeParam>,
                             std::tuple_element_t<1, TypeParam>,
                             milvus::bitset::detail::VectorizedDynamic>;
    TestAppendImpl<typename impl_traits::bitset_type>();
}

TYPED_TEST_P(AppendSuite, VecRef) {
    using impl_traits =
        VectorizedImplTraits<std::tuple_element_t<0, TypeParam>,
                             std::tuple_element_t<1, TypeParam>,
                             milvus::bitset::detail::VectorizedRef>;
    TestAppendImpl<typename impl_traits::bitset_type>();
}

//
REGISTER_TYPED_TEST_SUITE_P(AppendSuite,
                            BitWise,
                            ElementWise,
                            Avx2,
                            Avx512,
                            Neon,
                            Sve,
                            Dynamic,
                            VecRef);

INSTANTIATE_TYPED_TEST_SUITE_P(AppendTest, AppendSuite, Ttypes0);

//////////////////////////////////////////////////////////////////////////////////////////

//
template <typename BitsetT>
void
TestCountImpl(BitsetT& owner, const size_t max_v, const size_t offset = 0) {
    auto bitset = owner.view(offset);
    const size_t n = bitset.size();

    std::default_random_engine rng(123);
    std::uniform_int_distribution<int8_t> u(0, max_v);

    std::vector<size_t> one_pos;
    for (size_t i = 0; i < n; i++) {
        bool enabled = (u(rng) == 0);
        if (enabled) {
            one_pos.push_back(i);
            owner[offset + i] = true;
        }
    }

    StopWatch sw;

    auto count = bitset.count();
    ASSERT_EQ(count, one_pos.size());

    if (print_timing) {
        printf("elapsed %f\n", sw.elapsed());
    }
}

template <typename BitsetT>
void
TestCountImpl() {
    for (const size_t n : typical_sizes) {
        for (const size_t pr : {1, 100}) {
            BitsetT bitset(n);
            bitset.reset();

            if (print_log) {
                printf("Testing bitset, n=%zd, pr=%zd\n", n, pr);
            }

            TestCountImpl(bitset, pr);

            for (const size_t offset : typical_offsets) {
                if (offset >= n) {
                    continue;
                }

                bitset.reset();

                if (print_log) {
                    printf("Testing bitset view, n=%zd, offset=%zd, pr=%zd\n",
                           n,
                           offset,
                           pr);
                }

                TestCountImpl(bitset, pr, offset);
            }
        }
    }
}

//
template <typename T>
class CountSuite : public ::testing::Test {};

TYPED_TEST_SUITE_P(CountSuite);

//
TYPED_TEST_P(CountSuite, BitWise) {
    using impl_traits = RefImplTraits<std::tuple_element_t<0, TypeParam>,
                                      std::tuple_element_t<1, TypeParam>>;
    TestCountImpl<typename impl_traits::bitset_type>();
}

TYPED_TEST_P(CountSuite, ElementWise) {
    using impl_traits = ElementImplTraits<std::tuple_element_t<0, TypeParam>,
                                          std::tuple_element_t<1, TypeParam>>;
    TestCountImpl<typename impl_traits::bitset_type>();
}

TYPED_TEST_P(CountSuite, Avx2) {
#if defined(__x86_64__)
    using namespace milvus::bitset::detail::x86;

    if (cpu_support_avx2()) {
        using impl_traits =
            VectorizedImplTraits<std::tuple_element_t<0, TypeParam>,
                                 std::tuple_element_t<1, TypeParam>,
                                 milvus::bitset::detail::x86::VectorizedAvx2>;
        TestCountImpl<typename impl_traits::bitset_type>();
    }
#endif
}

TYPED_TEST_P(CountSuite, Avx512) {
#if defined(__x86_64__)
    using namespace milvus::bitset::detail::x86;

    if (cpu_support_avx512()) {
        using impl_traits =
            VectorizedImplTraits<std::tuple_element_t<0, TypeParam>,
                                 std::tuple_element_t<1, TypeParam>,
                                 milvus::bitset::detail::x86::VectorizedAvx512>;
        TestCountImpl<typename impl_traits::bitset_type>();
    }
#endif
}

TYPED_TEST_P(CountSuite, Neon) {
#if defined(__aarch64__)
    using namespace milvus::bitset::detail::arm;

    using impl_traits =
        VectorizedImplTraits<std::tuple_element_t<0, TypeParam>,
                             std::tuple_element_t<1, TypeParam>,
                             milvus::bitset::detail::arm::VectorizedNeon>;
    TestCountImpl<typename impl_traits::bitset_type>();
#endif
}

TYPED_TEST_P(CountSuite, Sve) {
#if defined(__aarch64__) && defined(__ARM_FEATURE_SVE) && \
    defined(BITSET_ENABLE_SVE_SUPPORT)
    using namespace milvus::bitset::detail::arm;

    using impl_traits =
        VectorizedImplTraits<std::tuple_element_t<0, TypeParam>,
                             std::tuple_element_t<1, TypeParam>,
                             milvus::bitset::detail::arm::VectorizedSve>;
    TestCountImpl<typename impl_traits::bitset_type>();
#endif
}

TYPED_TEST_P(CountSuite, Dynamic) {
    using impl_traits =
        VectorizedImplTraits<std::tuple_element_t<0, TypeParam>,
                             std::tuple_element_t<1, TypeParam>,
                             milvus::bitset::detail::VectorizedDynamic>;
    TestCountImpl<typename impl_traits::bitset_type>();
}

TYPED_TEST_P(CountSuite, VecRef) {
    using impl_traits =
        VectorizedImplTraits<std::tuple_element_t<0, TypeParam>,
                             std::tuple_element_t<1, TypeParam>,
                             milvus::bitset::detail::VectorizedRef>;
    TestCountImpl<typename impl_traits::bitset_type>();
}

//
REGISTER_TYPED_TEST_SUITE_P(
    CountSuite, BitWise, ElementWise, Avx2, Avx512, Neon, Sve, Dynamic, VecRef);

INSTANTIATE_TYPED_TEST_SUITE_P(CountTest, CountSuite, Ttypes0);

//////////////////////////////////////////////////////////////////////////////////////////

enum class TestInplaceOp {
    AND,
    OR,
    XOR,
    SUB,
    AND_WITH_COUNT,
    OR_WITH_COUNT,
    FLIP,
    ALL,
    NONE
};

//
template <typename BitsetT>
void
TestInplaceOpImpl(BitsetT& owner,
                  BitsetT& owner_2,
                  const TestInplaceOp op,
                  const size_t offset = 0,
                  const size_t offset_2 = 0,
                  const size_t length = std::numeric_limits<size_t>::max()) {
    const size_t n = length == std::numeric_limits<size_t>::max()
                         ? owner.size() - offset
                         : length;
    auto bitset = owner.view(offset, n);
    auto bitset_2 = owner_2.view(offset_2, n);
    const size_t max_v = 3;

    std::default_random_engine rng(123);
    std::uniform_int_distribution<int8_t> u(0, max_v);

    // populate first bitset
    std::vector<bool> ref_bitset(n, false);
    for (size_t i = 0; i < n; i++) {
        bool enabled = (u(rng) == 0);

        ref_bitset[i] = enabled;
        owner[offset + i] = enabled;
    }

    // populate second bitset
    std::vector<bool> ref_bitset_2(n, false);
    for (size_t i = 0; i < n; i++) {
        bool enabled = (u(rng) == 0);

        ref_bitset_2[i] = enabled;
        owner_2[offset_2 + i] = enabled;
    }

    // for _WITH_COUNT ops
    size_t bits_count = 0;
    // for ALL and NONE ops
    bool bits_flag = false;
    bool bits_flag_2 = false;

    // evaluate
    StopWatch sw;

    if (op == TestInplaceOp::AND) {
        owner.inplace_and(bitset_2, n, offset);
    } else if (op == TestInplaceOp::OR) {
        owner.inplace_or(bitset_2, n, offset);
    } else if (op == TestInplaceOp::XOR) {
        owner.inplace_xor(bitset_2, n, offset);
    } else if (op == TestInplaceOp::SUB) {
        owner.inplace_sub(bitset_2, n, offset);
    } else if (op == TestInplaceOp::AND_WITH_COUNT) {
        // number of active bits
        bits_count = owner.inplace_and_with_count(bitset_2, n, offset);
    } else if (op == TestInplaceOp::OR_WITH_COUNT) {
        // number of inactive bits
        bits_count = owner.inplace_or_with_count(bitset_2, n, offset);
    } else if (op == TestInplaceOp::FLIP) {
        owner.flip(offset, n);
        owner_2.flip(offset_2, n);
    } else if (op == TestInplaceOp::ALL) {
        bits_flag = bitset.all();
        bits_flag_2 = bitset_2.all();
    } else if (op == TestInplaceOp::NONE) {
        bits_flag = bitset.none();
        bits_flag_2 = bitset_2.none();
    } else {
        ASSERT_TRUE(false) << "Not implemented";
    }

    if (print_timing) {
        printf("elapsed %f\n", sw.elapsed());
    }

    // ref for _WITH_COUNT ops
    size_t ref_bits_count = 0;

    // validate
    for (size_t i = 0; i < n; i++) {
        if (op == TestInplaceOp::AND) {
            ASSERT_EQ(bitset[i], ref_bitset[i] & ref_bitset_2[i]);
        } else if (op == TestInplaceOp::OR) {
            ASSERT_EQ(bitset[i], ref_bitset[i] | ref_bitset_2[i]);
        } else if (op == TestInplaceOp::XOR) {
            ASSERT_EQ(bitset[i], ref_bitset[i] ^ ref_bitset_2[i]);
        } else if (op == TestInplaceOp::SUB) {
            ASSERT_EQ(bitset[i], ref_bitset[i] & (~ref_bitset_2[i]));
        } else if (op == TestInplaceOp::AND ||
                   op == TestInplaceOp::AND_WITH_COUNT) {
            const bool ref_value = ref_bitset[i] & ref_bitset_2[i];
            ASSERT_EQ(bitset[i], ref_value);
            ref_bits_count += ref_value ? 1 : 0;
        } else if (op == TestInplaceOp::OR ||
                   op == TestInplaceOp::OR_WITH_COUNT) {
            const bool ref_value = ref_bitset[i] | ref_bitset_2[i];
            ASSERT_EQ(bitset[i], ref_value);
            ref_bits_count += ref_value ? 0 : 1;
        } else if (op == TestInplaceOp::FLIP) {
            ASSERT_EQ(bitset[i], !ref_bitset[i]);
            ASSERT_EQ(bitset_2[i], !ref_bitset_2[i]);
        } else if (op == TestInplaceOp::ALL) {
            if (bits_flag) {
                ASSERT_TRUE(ref_bitset[i]);
            }
            if (bits_flag_2) {
                ASSERT_TRUE(ref_bitset_2[i]);
            }
        } else if (op == TestInplaceOp::NONE) {
            if (bits_flag) {
                ASSERT_FALSE(ref_bitset[i]);
            }
            if (bits_flag_2) {
                ASSERT_FALSE(ref_bitset_2[i]);
            }
        } else {
            ASSERT_TRUE(false) << "Not implemented";
        }
    }

    // additional validation for _WITH_COUNT ops
    if (op == TestInplaceOp::AND_WITH_COUNT ||
        op == TestInplaceOp::OR_WITH_COUNT) {
        ASSERT_EQ(bits_count, ref_bits_count)
            << ((op == TestInplaceOp::AND_WITH_COUNT) ? "and" : "or");
    }

    // additional validation for ALL and NONE
    if (op == TestInplaceOp::ALL) {
        bool ref_bits_flag = true;
        bool ref_bits_flag_2 = true;
        for (size_t i = 0; i < n; i++) {
            ref_bits_flag &= ref_bitset[i];
            ref_bits_flag_2 &= ref_bitset_2[i];
        }

        ASSERT_EQ(ref_bits_flag, bits_flag);
        ASSERT_EQ(ref_bits_flag_2, bits_flag_2);
    } else if (op == TestInplaceOp::NONE) {
        bool ref_bits_flag = true;
        bool ref_bits_flag_2 = true;
        for (size_t i = 0; i < n; i++) {
            ref_bits_flag &= !ref_bitset[i];
            ref_bits_flag_2 &= !ref_bitset_2[i];
        }

        ASSERT_EQ(ref_bits_flag, bits_flag);
        ASSERT_EQ(ref_bits_flag_2, bits_flag_2);
    }
}

template <typename BitsetT>
void
TestInplaceOpImpl() {
    const auto inplace_ops = {TestInplaceOp::AND,
                              TestInplaceOp::OR,
                              TestInplaceOp::XOR,
                              TestInplaceOp::SUB,
                              TestInplaceOp::AND_WITH_COUNT,
                              TestInplaceOp::OR_WITH_COUNT,
                              TestInplaceOp::FLIP,
                              TestInplaceOp::ALL,
                              TestInplaceOp::NONE};

    for (const size_t n : typical_sizes) {
        for (const auto op : inplace_ops) {
            BitsetT bitset(n);
            bitset.reset();
            BitsetT bitset_2(n);
            bitset_2.reset();

            if (print_log) {
                printf("Testing bitset, n=%zd, op=%zd\n", n, (size_t)op);
            }

            TestInplaceOpImpl<BitsetT>(bitset, bitset_2, op);

            // same offsets
            for (const size_t offset : typical_offsets) {
                if (offset >= n) {
                    continue;
                }

                bitset.reset();

                bitset_2.reset();

                if (print_log) {
                    printf("Testing bitset view, n=%zd, offset=%zd, op=%zd\n",
                           n,
                           offset,
                           (size_t)op);
                }

                TestInplaceOpImpl<BitsetT>(
                    bitset, bitset_2, op, offset, offset, n - offset);
            }

            // fixed left offset
            for (const size_t offset : typical_offsets) {
                if (offset >= n) {
                    continue;
                }

                bitset.reset();

                bitset_2.reset();

                if (print_log) {
                    printf(
                        "Testing left-fixed bitset view, n=%zd, offset=%zd, "
                        "op=%zd\n",
                        n,
                        offset,
                        (size_t)op);
                }

                TestInplaceOpImpl<BitsetT>(
                    bitset, bitset_2, op, 0, offset, n - offset);
            }

            // fixed right offset
            for (const size_t offset : typical_offsets) {
                if (offset >= n) {
                    continue;
                }

                bitset.reset();

                bitset_2.reset();

                if (print_log) {
                    printf(
                        "Testing right-fixed bitset view, n=%zd, offset=%zd, "
                        "op=%zd\n",
                        n,
                        offset,
                        (size_t)op);
                }

                TestInplaceOpImpl<BitsetT>(
                    bitset, bitset_2, op, offset, 0, n - offset);
            }
        }
    }
}

//
template <typename T>
class InplaceOpSuite : public ::testing::Test {};

TYPED_TEST_SUITE_P(InplaceOpSuite);

TYPED_TEST_P(InplaceOpSuite, BitWise) {
    using impl_traits = RefImplTraits<std::tuple_element_t<0, TypeParam>,
                                      std::tuple_element_t<1, TypeParam>>;
    TestInplaceOpImpl<typename impl_traits::bitset_type>();
}

TYPED_TEST_P(InplaceOpSuite, ElementWise) {
    using impl_traits = ElementImplTraits<std::tuple_element_t<0, TypeParam>,
                                          std::tuple_element_t<1, TypeParam>>;
    TestInplaceOpImpl<typename impl_traits::bitset_type>();
}

TYPED_TEST_P(InplaceOpSuite, Avx2) {
#if defined(__x86_64__)
    using namespace milvus::bitset::detail::x86;

    if (cpu_support_avx2()) {
        using impl_traits =
            VectorizedImplTraits<std::tuple_element_t<0, TypeParam>,
                                 std::tuple_element_t<1, TypeParam>,
                                 milvus::bitset::detail::x86::VectorizedAvx2>;
        TestInplaceOpImpl<typename impl_traits::bitset_type>();
    }
#endif
}

TYPED_TEST_P(InplaceOpSuite, Avx512) {
#if defined(__x86_64__)
    using namespace milvus::bitset::detail::x86;

    if (cpu_support_avx512()) {
        using impl_traits =
            VectorizedImplTraits<std::tuple_element_t<0, TypeParam>,
                                 std::tuple_element_t<1, TypeParam>,
                                 milvus::bitset::detail::x86::VectorizedAvx512>;
        TestInplaceOpImpl<typename impl_traits::bitset_type>();
    }
#endif
}

TYPED_TEST_P(InplaceOpSuite, Neon) {
#if defined(__aarch64__)
    using namespace milvus::bitset::detail::arm;

    using impl_traits =
        VectorizedImplTraits<std::tuple_element_t<0, TypeParam>,
                             std::tuple_element_t<1, TypeParam>,
                             milvus::bitset::detail::arm::VectorizedNeon>;
    TestInplaceOpImpl<typename impl_traits::bitset_type>();
#endif
}

TYPED_TEST_P(InplaceOpSuite, Sve) {
#if defined(__aarch64__) && defined(__ARM_FEATURE_SVE) && \
    defined(BITSET_ENABLE_SVE_SUPPORT)
    using namespace milvus::bitset::detail::arm;

    using impl_traits =
        VectorizedImplTraits<std::tuple_element_t<0, TypeParam>,
                             std::tuple_element_t<1, TypeParam>,
                             milvus::bitset::detail::arm::VectorizedSve>;
    TestInplaceOpImpl<typename impl_traits::bitset_type>();
#endif
}

TYPED_TEST_P(InplaceOpSuite, Dynamic) {
    using impl_traits =
        VectorizedImplTraits<std::tuple_element_t<0, TypeParam>,
                             std::tuple_element_t<1, TypeParam>,
                             milvus::bitset::detail::VectorizedDynamic>;
    TestInplaceOpImpl<typename impl_traits::bitset_type>();
}

TYPED_TEST_P(InplaceOpSuite, VecRef) {
    using impl_traits =
        VectorizedImplTraits<std::tuple_element_t<0, TypeParam>,
                             std::tuple_element_t<1, TypeParam>,
                             milvus::bitset::detail::VectorizedRef>;
    TestInplaceOpImpl<typename impl_traits::bitset_type>();
}

//
REGISTER_TYPED_TEST_SUITE_P(InplaceOpSuite,
                            BitWise,
                            ElementWise,
                            Avx2,
                            Avx512,
                            Neon,
                            Sve,
                            Dynamic,
                            VecRef);

INSTANTIATE_TYPED_TEST_SUITE_P(InplaceOpTest, InplaceOpSuite, Ttypes0);

//////////////////////////////////////////////////////////////////////////////////////////

//
template <typename BitsetT>
void
TestInplaceOpMultipleImpl(BitsetT& owner,
                          std::vector<BitsetT>& bitset_others,
                          const TestInplaceOp op,
                          const size_t offset = 0) {
    auto bitset = owner.view(offset);
    const size_t n = bitset.size();
    std::vector<typename BitsetT::view_type> views;
    for (const auto& other : bitset_others) views.push_back(other.view(offset));
    const size_t n_others = bitset_others.size();
    const size_t max_v = 3;

    std::default_random_engine rng(123);
    std::uniform_int_distribution<int8_t> u(0, max_v);

    // populate first bitset
    std::vector<bool> ref_bitset(n, false);
    for (size_t i = 0; i < n; i++) {
        bool enabled = (u(rng) == 0);

        ref_bitset[i] = enabled;
        owner[offset + i] = enabled;
    }

    // populate others
    std::vector<std::vector<bool>> ref_others;
    for (size_t j = 0; j < n_others; j++) {
        std::vector<bool> ref_other(n, false);
        for (size_t i = 0; i < n; i++) {
            bool enabled = (u(rng) == 0);

            ref_other[i] = enabled;
            bitset_others[j][offset + i] = enabled;
        }

        ref_others.push_back(std::move(ref_other));
    }

    // evaluate
    StopWatch sw;
    if (op == TestInplaceOp::AND) {
        if (offset == 0)
            owner.inplace_and(bitset_others.data(), n_others, n);
        else
            owner.inplace_and(views.data(), n_others, n, offset);
    } else if (op == TestInplaceOp::OR) {
        if (offset == 0)
            owner.inplace_or(bitset_others.data(), n_others, n);
        else
            owner.inplace_or(views.data(), n_others, n, offset);
    } else {
        ASSERT_TRUE(false) << "Not implemented";
    }

    if (print_timing) {
        printf("elapsed %f\n", sw.elapsed());
    }

    // validate
    for (size_t i = 0; i < n; i++) {
        if (op == TestInplaceOp::AND) {
            bool b = ref_bitset[i];
            for (size_t j = 0; j < n_others; j++) {
                b &= ref_others[j][i];
            }
            ASSERT_EQ(bitset[i], b);
        } else if (op == TestInplaceOp::OR) {
            bool b = ref_bitset[i];
            for (size_t j = 0; j < n_others; j++) {
                b |= ref_others[j][i];
            }
            ASSERT_EQ(bitset[i], b);
        } else {
            ASSERT_TRUE(false) << "Not implemented";
        }
    }
}

template <typename BitsetT>
void
TestInplaceOpMultipleImpl() {
    const auto inplace_ops = {TestInplaceOp::AND, TestInplaceOp::OR};

    for (const size_t n : typical_sizes) {
        for (const size_t n_ngb : {1, 2, 3, 4, 5, 6, 7, 8, 9}) {
            for (const auto op : inplace_ops) {
                BitsetT bitset(n);
                bitset.reset();

                std::vector<BitsetT> bitset_others;
                for (size_t i = 0; i < n_ngb; i++) {
                    BitsetT bitset_other(n);
                    bitset_other.reset();

                    bitset_others.push_back(std::move(bitset_other));
                }

                if (print_log) {
                    printf("Testing bitset, n=%zd, op=%zd\n", n, (size_t)op);
                }

                TestInplaceOpMultipleImpl<BitsetT>(bitset, bitset_others, op);

                for (const size_t offset : typical_offsets) {
                    if (offset >= n) {
                        continue;
                    }

                    bitset.reset();

                    for (auto& other : bitset_others) other.reset();

                    if (print_log) {
                        printf(
                            "Testing bitset view, n=%zd, offset=%zd, op=%zd\n",
                            n,
                            offset,
                            (size_t)op);
                    }

                    TestInplaceOpMultipleImpl<BitsetT>(
                        bitset, bitset_others, op, offset);
                }
            }
        }
    }
}

//
template <typename T>
class InplaceOpMultipleSuite : public ::testing::Test {};

TYPED_TEST_SUITE_P(InplaceOpMultipleSuite);

TYPED_TEST_P(InplaceOpMultipleSuite, BitWise) {
    using impl_traits = RefImplTraits<std::tuple_element_t<0, TypeParam>,
                                      std::tuple_element_t<1, TypeParam>>;
    TestInplaceOpMultipleImpl<typename impl_traits::bitset_type>();
}

TYPED_TEST_P(InplaceOpMultipleSuite, ElementWise) {
    using impl_traits = ElementImplTraits<std::tuple_element_t<0, TypeParam>,
                                          std::tuple_element_t<1, TypeParam>>;
    TestInplaceOpMultipleImpl<typename impl_traits::bitset_type>();
}

TYPED_TEST_P(InplaceOpMultipleSuite, Avx2) {
#if defined(__x86_64__)
    using namespace milvus::bitset::detail::x86;

    if (cpu_support_avx2()) {
        using impl_traits =
            VectorizedImplTraits<std::tuple_element_t<0, TypeParam>,
                                 std::tuple_element_t<1, TypeParam>,
                                 milvus::bitset::detail::x86::VectorizedAvx2>;
        TestInplaceOpMultipleImpl<typename impl_traits::bitset_type>();
    }
#endif
}

TYPED_TEST_P(InplaceOpMultipleSuite, Avx512) {
#if defined(__x86_64__)
    using namespace milvus::bitset::detail::x86;

    if (cpu_support_avx512()) {
        using impl_traits =
            VectorizedImplTraits<std::tuple_element_t<0, TypeParam>,
                                 std::tuple_element_t<1, TypeParam>,
                                 milvus::bitset::detail::x86::VectorizedAvx512>;
        TestInplaceOpMultipleImpl<typename impl_traits::bitset_type>();
    }
#endif
}

TYPED_TEST_P(InplaceOpMultipleSuite, Neon) {
#if defined(__aarch64__)
    using namespace milvus::bitset::detail::arm;

    using impl_traits =
        VectorizedImplTraits<std::tuple_element_t<0, TypeParam>,
                             std::tuple_element_t<1, TypeParam>,
                             milvus::bitset::detail::arm::VectorizedNeon>;
    TestInplaceOpMultipleImpl<typename impl_traits::bitset_type>();
#endif
}

TYPED_TEST_P(InplaceOpMultipleSuite, Sve) {
#if defined(__aarch64__) && defined(__ARM_FEATURE_SVE) && \
    defined(BITSET_ENABLE_SVE_SUPPORT)
    using namespace milvus::bitset::detail::arm;

    using impl_traits =
        VectorizedImplTraits<std::tuple_element_t<0, TypeParam>,
                             std::tuple_element_t<1, TypeParam>,
                             milvus::bitset::detail::arm::VectorizedSve>;
    TestInplaceOpMultipleImpl<typename impl_traits::bitset_type>();
#endif
}

TYPED_TEST_P(InplaceOpMultipleSuite, Dynamic) {
    using impl_traits =
        VectorizedImplTraits<std::tuple_element_t<0, TypeParam>,
                             std::tuple_element_t<1, TypeParam>,
                             milvus::bitset::detail::VectorizedDynamic>;
    TestInplaceOpMultipleImpl<typename impl_traits::bitset_type>();
}

TYPED_TEST_P(InplaceOpMultipleSuite, VecRef) {
    using impl_traits =
        VectorizedImplTraits<std::tuple_element_t<0, TypeParam>,
                             std::tuple_element_t<1, TypeParam>,
                             milvus::bitset::detail::VectorizedRef>;
    TestInplaceOpMultipleImpl<typename impl_traits::bitset_type>();
}

//
REGISTER_TYPED_TEST_SUITE_P(InplaceOpMultipleSuite,
                            BitWise,
                            ElementWise,
                            Avx2,
                            Avx512,
                            Neon,
                            Sve,
                            Dynamic,
                            VecRef);

INSTANTIATE_TYPED_TEST_SUITE_P(InplaceOpMultipleTest,
                               InplaceOpMultipleSuite,
                               Ttypes0);

//////////////////////////////////////////////////////////////////////////////////////////

//
template <typename BitsetT>
void
TestFillImpl(BitsetT& owner, const bool flag, const size_t offset = 0) {
    auto bitset = owner.view(offset);
    const size_t n = bitset.size();

    // rng
    std::default_random_engine rng(123);

    // test everything
    {
        FillRandom(owner, rng, offset);

        //
        StopWatch sw;

        if (flag) {
            owner.set(offset, n);
        } else {
            owner.reset(offset, n);
        }

        if (print_timing) {
            printf("elapsed %f\n", sw.elapsed());
        }

        for (size_t i = 0; i < n; i++) {
            ASSERT_EQ(bitset[i], flag);
        }
    }

    // test a first half
    {
        FillRandom(owner, rng, offset);

        //
        StopWatch sw;

        if (flag) {
            owner.set(offset, n / 2);
        } else {
            owner.reset(offset, n / 2);
        }

        if (print_timing) {
            printf("elapsed %f\n", sw.elapsed());
        }

        for (size_t i = 0; i < n / 2; i++) {
            ASSERT_EQ(bitset[i], flag);
        }
    }

    // test a second half
    {
        FillRandom(owner, rng, offset);

        //
        StopWatch sw;

        if (flag) {
            owner.set(offset + n / 2, n - n / 2);
        } else {
            owner.reset(offset + n / 2, n - n / 2);
        }

        if (print_timing) {
            printf("elapsed %f\n", sw.elapsed());
        }

        for (size_t i = n / 2; i < n; i++) {
            ASSERT_EQ(bitset[i], flag);
        }
    }
}

template <typename BitsetT>
void
TestFillImpl() {
    for (const size_t n : typical_sizes) {
        for (const bool flag : {true, false}) {
            BitsetT bitset(n);
            bitset.reset();

            if (print_log) {
                printf("Testing bitset, n=%zd, flag=%zd\n", n, size_t(flag));
            }

            TestFillImpl(bitset, flag);

            for (const size_t offset : typical_offsets) {
                if (offset >= n) {
                    continue;
                }

                bitset.reset();

                if (print_log) {
                    printf("Testing bitset view, n=%zd, offset=%zd, flag=%zd\n",
                           n,
                           offset,
                           size_t(flag));
                }

                TestFillImpl(bitset, flag, offset);
            }
        }
    }
}

//
template <typename T>
class FillSuite : public ::testing::Test {};

TYPED_TEST_SUITE_P(FillSuite);

//
TYPED_TEST_P(FillSuite, BitWise) {
    using impl_traits = RefImplTraits<std::tuple_element_t<0, TypeParam>,
                                      std::tuple_element_t<1, TypeParam>>;
    TestFillImpl<typename impl_traits::bitset_type>();
}

TYPED_TEST_P(FillSuite, ElementWise) {
    using impl_traits = ElementImplTraits<std::tuple_element_t<0, TypeParam>,
                                          std::tuple_element_t<1, TypeParam>>;
    TestFillImpl<typename impl_traits::bitset_type>();
}

TYPED_TEST_P(FillSuite, Avx2) {
#if defined(__x86_64__)
    using namespace milvus::bitset::detail::x86;

    if (cpu_support_avx2()) {
        using impl_traits =
            VectorizedImplTraits<std::tuple_element_t<0, TypeParam>,
                                 std::tuple_element_t<1, TypeParam>,
                                 milvus::bitset::detail::x86::VectorizedAvx2>;
        TestFillImpl<typename impl_traits::bitset_type>();
    }
#endif
}

TYPED_TEST_P(FillSuite, Avx512) {
#if defined(__x86_64__)
    using namespace milvus::bitset::detail::x86;

    if (cpu_support_avx512()) {
        using impl_traits =
            VectorizedImplTraits<std::tuple_element_t<0, TypeParam>,
                                 std::tuple_element_t<1, TypeParam>,
                                 milvus::bitset::detail::x86::VectorizedAvx512>;
        TestFillImpl<typename impl_traits::bitset_type>();
    }
#endif
}

TYPED_TEST_P(FillSuite, Neon) {
#if defined(__aarch64__)
    using namespace milvus::bitset::detail::arm;

    using impl_traits =
        VectorizedImplTraits<std::tuple_element_t<0, TypeParam>,
                             std::tuple_element_t<1, TypeParam>,
                             milvus::bitset::detail::arm::VectorizedNeon>;
    TestFillImpl<typename impl_traits::bitset_type>();
#endif
}

TYPED_TEST_P(FillSuite, Sve) {
#if defined(__aarch64__) && defined(__ARM_FEATURE_SVE) && \
    defined(BITSET_ENABLE_SVE_SUPPORT)
    using namespace milvus::bitset::detail::arm;

    using impl_traits =
        VectorizedImplTraits<std::tuple_element_t<0, TypeParam>,
                             std::tuple_element_t<1, TypeParam>,
                             milvus::bitset::detail::arm::VectorizedSve>;
    TestFillImpl<typename impl_traits::bitset_type>();
#endif
}

TYPED_TEST_P(FillSuite, Dynamic) {
    using impl_traits =
        VectorizedImplTraits<std::tuple_element_t<0, TypeParam>,
                             std::tuple_element_t<1, TypeParam>,
                             milvus::bitset::detail::VectorizedDynamic>;
    TestFillImpl<typename impl_traits::bitset_type>();
}

TYPED_TEST_P(FillSuite, VecRef) {
    using impl_traits =
        VectorizedImplTraits<std::tuple_element_t<0, TypeParam>,
                             std::tuple_element_t<1, TypeParam>,
                             milvus::bitset::detail::VectorizedRef>;
    TestFillImpl<typename impl_traits::bitset_type>();
}

//
REGISTER_TYPED_TEST_SUITE_P(
    FillSuite, BitWise, ElementWise, Avx2, Avx512, Neon, Sve, Dynamic, VecRef);

INSTANTIATE_TYPED_TEST_SUITE_P(FillTest, FillSuite, Ttypes0);

//////////////////////////////////////////////////////////////////////////////////////////

// read() must accept widths up to 8 * sizeof(data_type) bits: the old range
// check compared nbits against sizeof(data_type), silently capping reads at
// 8 bits for a 64-bit word and rejecting every wider (legal) read.
template <typename BitsetT>
void
TestReadImpl() {
    constexpr size_t n = 150;
    BitsetT bitset(n);
    // pattern: every 3rd bit set
    for (size_t i = 0; i < n; i += 3) {
        bitset[i] = true;
    }

    for (const size_t offset : {0, 1, 7, 63, 64, 86}) {
        for (const size_t nbits : {1, 8, 9, 33, 64}) {
            if (offset + nbits > n) {
                continue;
            }
            const auto value = bitset.read(offset, nbits);
            for (size_t j = 0; j < nbits; ++j) {
                const bool expected = ((offset + j) % 3 == 0);
                ASSERT_EQ(((value >> j) & 1) != 0, expected)
                    << "offset=" << offset << " nbits=" << nbits << " j=" << j;
            }
        }
    }
}

TEST(ReadTest, BitWise) {
    TestReadImpl<typename RefImplTraits<uint64_t, uint8_t>::bitset_type>();
}

TEST(ReadTest, ElementWise) {
    TestReadImpl<typename ElementImplTraits<uint64_t, uint8_t>::bitset_type>();
}

TEST(ReadTest, Dynamic) {
    TestReadImpl<typename VectorizedImplTraits<
        uint64_t,
        uint8_t,
        milvus::bitset::detail::VectorizedDynamic>::bitset_type>();
}

TEST(ReadTest, VecRef) {
    TestReadImpl<typename VectorizedImplTraits<
        uint64_t,
        uint8_t,
        milvus::bitset::detail::VectorizedRef>::bitset_type>();
}

//////////////////////////////////////////////////////////////////////////////////////////

template <typename T>
void
TestBulkRangeBoundaries() {
    using P = milvus::bitset::detail::ElementWiseBitsetPolicy<T>;
    constexpr size_t width = sizeof(T) * 8;
    std::mt19937_64 rng(20260924);
    for (size_t offset = 0; offset < width; ++offset) {
        for (size_t size : {size_t(0),
                            size_t(1),
                            width - 1,
                            width,
                            width + 1,
                            size_t(8191),
                            size_t(8192),
                            size_t(8193),
                            size_t(16383),
                            size_t(16384),
                            size_t(16385),
                            size_t(65539)}) {
            for (int shape = 0; shape < 3; ++shape) {
                std::vector<T> data((offset + size + width - 1) / width);
                for (auto& word : data)
                    word = shape == 0 ? T(0) : shape == 1 ? T(-1) : T(rng());
                size_t expected = 0;
                for (size_t i = 0; i < size; ++i)
                    expected +=
                        (data[(offset + i) / width] >> ((offset + i) % width)) &
                        1;
                ASSERT_EQ(P::op_count(data.data(), offset, size), expected);
                ASSERT_EQ(P::op_all(data.data(), offset, size),
                          expected == size);
                ASSERT_EQ(P::op_none(data.data(), offset, size), expected == 0);
            }
        }
    }
    ASSERT_EQ(P::op_count(nullptr, 0, 0), 0);
    ASSERT_TRUE(P::op_all(nullptr, 0, 0));
    ASSERT_TRUE(P::op_none(nullptr, 0, 0));
}

TEST(BulkRangeBoundaryTest, ByteAndWordPolicies) {
    TestBulkRangeBoundaries<uint8_t>();
    TestBulkRangeBoundaries<uint64_t>();
}

TEST(BulkRangeBoundaryTest, ExactByteBuffers) {
    using namespace milvus::bitset::detail;
    std::mt19937_64 rng(9024);
    for (size_t offset = 0; offset < 64; ++offset) {
        for (size_t bytes : {size_t(1),
                             size_t(7),
                             size_t(8),
                             size_t(15),
                             size_t(16),
                             size_t(31),
                             size_t(32),
                             size_t(1023),
                             size_t(1024),
                             size_t(1025),
                             size_t(4099)}) {
            std::vector<uint8_t> data(offset + bytes);
            for (auto& value : data) value = uint8_t(rng());
            size_t expected = 0;
            for (size_t i = offset; i < data.size(); ++i)
                expected += __builtin_popcount(data[i]);
            ASSERT_EQ(CountBytesBulk(data.data() + offset, bytes), expected);
            ASSERT_EQ(CountBytesScalar(data.data() + offset, bytes), expected);
        }
    }
}

template <typename T>
void
TestUnalignedOutputBoundaries() {
    using P = milvus::bitset::detail::ElementWiseBitsetPolicy<T>;
    constexpr size_t width = sizeof(T) * 8;
    std::mt19937_64 rng(831);
    for (size_t left = 0; left < width; ++left) {
        for (size_t right = 0; right < width; ++right) {
            for (size_t size :
                 {4 * width - 1, 4 * width, 4 * width + 1, 9 * width + 3}) {
                std::vector<T> a((left + size + width - 1) / width),
                    b((right + size + width - 1) / width);
                for (auto& value : a) value = T(rng());
                for (auto& value : b) value = T(rng());
                for (int op = 0; op < 7; ++op) {
                    auto expected = a;
                    auto actual = a;
                    size_t ones = 0;
                    for (size_t i = 0; i < size; ++i) {
                        const bool x =
                            (a[(left + i) / width] >> ((left + i) % width)) & 1;
                        const bool y =
                            (b[(right + i) / width] >> ((right + i) % width)) &
                            1;
                        const bool z = op == 0                ? y
                                       : (op == 1 || op == 5) ? (x && y)
                                       : (op == 2 || op == 6) ? (x || y)
                                       : op == 3              ? (x != y)
                                                              : (x && !y);
                        auto m = T(T(1) << ((left + i) % width));
                        expected[(left + i) / width] =
                            T((expected[(left + i) / width] & ~m) |
                              ((T(0) - T(z)) & m));
                        ones += z;
                    }
                    switch (op) {
                        case 0:
                            P::op_copy(
                                b.data(), right, actual.data(), left, size);
                            break;
                        case 1:
                            P::op_and(
                                actual.data(), b.data(), left, right, size);
                            break;
                        case 2:
                            P::op_or(
                                actual.data(), b.data(), left, right, size);
                            break;
                        case 3:
                            P::op_xor(
                                actual.data(), b.data(), left, right, size);
                            break;
                        case 4:
                            P::op_sub(
                                actual.data(), b.data(), left, right, size);
                            break;
                        case 5:
                            ASSERT_EQ(
                                P::op_and_with_count(
                                    actual.data(), b.data(), left, right, size),
                                ones);
                            break;
                        case 6:
                            ASSERT_EQ(
                                P::op_or_with_count(
                                    actual.data(), b.data(), left, right, size),
                                size - ones);
                            break;
                    }
                    ASSERT_EQ(actual, expected)
                        << "offsets=" << left << ',' << right << " op=" << op;
                }
            }
        }
    }
}

TEST(BulkRangeBoundaryTest, UnalignedOutputAndCountCallbacks) {
    TestUnalignedOutputBoundaries<uint8_t>();
    TestUnalignedOutputBoundaries<uint64_t>();
}

template <typename Policy>
void
TestAndFlipBoundaries() {
    using T = typename Policy::data_type;
    using View = BitsetView<Policy, true>;
    using Owner = Bitset<Policy, std::vector<T>, true>;
    constexpr size_t width = sizeof(T) * 8;
    std::mt19937_64 rng(94025);
    for (size_t left = 0; left < width; ++left) {
        for (size_t right = 0; right < width; ++right) {
            for (size_t size : {size_t(0),
                                size_t(1),
                                width - 1,
                                width,
                                width + 1,
                                4 * width - 1,
                                4 * width,
                                4 * width + 1,
                                9 * width + 3}) {
                // Exact allocations: no SIMD padding may be assumed.
                std::vector<T> a((left + size + width - 1) / width);
                std::vector<T> b((right + size + width - 1) / width);
                for (auto& value : a) value = T(rng());
                for (auto& value : b) value = T(rng());
                const auto original_b = b;
                auto expected = a;
                for (size_t i = 0; i < size; ++i) {
                    const bool x =
                        (a[(left + i) / width] >> ((left + i) % width)) & 1;
                    const bool y =
                        (b[(right + i) / width] >> ((right + i) % width)) & 1;
                    const T mask = T(T(1) << ((left + i) % width));
                    expected[(left + i) / width] =
                        T((expected[(left + i) / width] & ~mask) |
                          ((T(0) - T(!(x && y))) & mask));
                }
                Owner dst(left + size);
                std::copy(a.begin(), a.end(), dst.data());
                const View src(b.data(), right, size);
                dst.inplace_and_flip(src, size, left);
                ASSERT_EQ(std::move(dst).into(), expected)
                    << left << ',' << right << ',' << size;
                ASSERT_EQ(b, original_b);
            }
        }
    }
    // The explicit size only changes a prefix of the destination view.
    std::vector<T> data(8, T(-1)), valid(8, T(-1));
    Owner dst(data.size() * width);
    std::copy(data.begin(), data.end(), dst.data());
    const View src(valid.data(), 3, 6 * width);
    dst.inplace_and_flip(src, width + 1, 1);
    data = std::move(dst).into();
    for (size_t i = 0; i < data.size() * width; ++i) {
        ASSERT_EQ(bool((data[i / width] >> (i % width)) & 1),
                  !(i >= 1 && i < width + 2));
    }
    Policy::op_and_flip(nullptr, nullptr, 0, 0, 0);
}

template <typename Policy>
void
TestAndFlipAliasing() {
    using T = typename Policy::data_type;
    constexpr size_t width = sizeof(T) * 8;
    std::mt19937_64 rng(921);
    for (size_t left = 0; left < width; ++left) {
        for (size_t right = 0; right < width; ++right) {
            for (size_t size :
                 {size_t(1), width + 1, 4 * width + 1, 9 * width + 3}) {
                for (size_t delta_left : {size_t(0), size_t(1)}) {
                    for (size_t delta_right : {size_t(0), size_t(1)}) {
                        std::vector<T> expected(13);
                        for (auto& value : expected) value = T(rng());
                        auto actual = expected;
                        Policy::op_and(expected.data() + delta_left,
                                       expected.data() + delta_right,
                                       left,
                                       right,
                                       size);
                        Policy::op_flip(
                            expected.data() + delta_left, left, size);
                        Policy::op_and_flip(actual.data() + delta_left,
                                            actual.data() + delta_right,
                                            left,
                                            right,
                                            size);
                        ASSERT_EQ(actual, expected)
                            << left << ',' << right << ',' << size;
                    }
                }
            }
        }
    }
}

TEST(AndFlipTest, BoundariesAndTruthTable) {
    using namespace milvus::bitset::detail;
    TestAndFlipBoundaries<ElementWiseBitsetPolicy<uint8_t>>();
    TestAndFlipBoundaries<ElementWiseBitsetPolicy<uint64_t>>();
    TestAndFlipBoundaries<BitWiseBitsetPolicy<uint8_t>>();
    TestAndFlipBoundaries<
        VectorizedElementWiseBitsetPolicy<uint64_t, VectorizedRef>>();
    TestAndFlipBoundaries<
        VectorizedElementWiseBitsetPolicy<uint64_t, VectorizedDynamic>>();
}

TEST(AndFlipTest, OverlappingViewsRetainTwoPassTraversal) {
    using namespace milvus::bitset::detail;
    TestAndFlipAliasing<ElementWiseBitsetPolicy<uint8_t>>();
    TestAndFlipAliasing<ElementWiseBitsetPolicy<uint64_t>>();
    TestAndFlipAliasing<BitWiseBitsetPolicy<uint8_t>>();
    TestAndFlipAliasing<
        VectorizedElementWiseBitsetPolicy<uint64_t, VectorizedRef>>();
    TestAndFlipAliasing<
        VectorizedElementWiseBitsetPolicy<uint64_t, VectorizedDynamic>>();
}

namespace {

using ReadOnlyPolicy =
    milvus::bitset::detail::ElementWiseBitsetPolicy<uint64_t>;
using ReadOnlyOwner = Bitset<ReadOnlyPolicy, std::vector<uint8_t>, true>;
using ReadOnlyView = ReadOnlyOwner::view_type;

// Descriptor constness must never grant writes to the referenced storage.
static_assert(std::is_same_v<decltype(std::declval<ReadOnlyView&>().data()),
                             const uint64_t*>);
static_assert(std::is_same_v<decltype(std::declval<ReadOnlyView&>()[0]), bool>);
static_assert(
    std::is_same_v<decltype(std::declval<ReadOnlyOwner&>().data()), uint64_t*>);
static_assert(std::is_constructible_v<ReadOnlyView, const ReadOnlyOwner&>);
static_assert(std::is_constructible_v<ReadOnlyView, const void*, size_t>);
static_assert(std::is_trivially_copyable_v<ReadOnlyView>);

#define BITSET_WRITE_TRAIT(Name, ...)                                       \
    template <typename T, typename = void>                                  \
    struct Name : std::false_type {};                                       \
    template <typename T>                                                   \
    struct Name<T, std::void_t<decltype(__VA_ARGS__)>> : std::true_type {}; \
    static_assert(Name<ReadOnlyOwner>::value);                              \
    static_assert(!Name<ReadOnlyView>::value)

BITSET_WRITE_TRAIT(HasSet, std::declval<T&>().set());
BITSET_WRITE_TRAIT(HasReset, std::declval<T&>().reset());
BITSET_WRITE_TRAIT(HasFlip, std::declval<T&>().flip());
BITSET_WRITE_TRAIT(HasBitWrite, std::declval<T&>()[0] = true);
BITSET_WRITE_TRAIT(HasAnd,
                   std::declval<T&>().inplace_and(std::declval<ReadOnlyView>(),
                                                  0));
BITSET_WRITE_TRAIT(HasOr,
                   std::declval<T&>().inplace_or(std::declval<ReadOnlyView>(),
                                                 0));
BITSET_WRITE_TRAIT(HasXor,
                   std::declval<T&>().inplace_xor(std::declval<ReadOnlyView>(),
                                                  0));
BITSET_WRITE_TRAIT(HasSub,
                   std::declval<T&>().inplace_sub(std::declval<ReadOnlyView>(),
                                                  0));
BITSET_WRITE_TRAIT(
    HasNand,
    std::declval<T&>().inplace_and_flip(std::declval<ReadOnlyView>(), 0));
BITSET_WRITE_TRAIT(
    HasAndCount,
    std::declval<T&>().inplace_and_with_count(std::declval<ReadOnlyView>(), 0));
BITSET_WRITE_TRAIT(
    HasOrCount,
    std::declval<T&>().inplace_or_with_count(std::declval<ReadOnlyView>(), 0));
BITSET_WRITE_TRAIT(HasAndAssign,
                   std::declval<T&>() &= std::declval<ReadOnlyView>());
BITSET_WRITE_TRAIT(HasOrAssign,
                   std::declval<T&>() |= std::declval<ReadOnlyView>());
BITSET_WRITE_TRAIT(HasXorAssign,
                   std::declval<T&>() ^= std::declval<ReadOnlyView>());
BITSET_WRITE_TRAIT(HasSubAssign,
                   std::declval<T&>() -= std::declval<ReadOnlyView>());
BITSET_WRITE_TRAIT(
    HasColumnWrite,
    std::declval<T&>().inplace_compare_column(static_cast<const int*>(nullptr),
                                              static_cast<const int*>(nullptr),
                                              0,
                                              CompareOpType::EQ));
BITSET_WRITE_TRAIT(
    HasValueWrite,
    std::declval<T&>().inplace_compare_val(
        static_cast<const int*>(nullptr), 0, 0, CompareOpType::EQ));
BITSET_WRITE_TRAIT(HasRangeColumnWrite,
                   std::declval<T&>().inplace_within_range_column(
                       static_cast<const int*>(nullptr),
                       static_cast<const int*>(nullptr),
                       static_cast<const int*>(nullptr),
                       0,
                       RangeType::IncInc));
BITSET_WRITE_TRAIT(
    HasRangeValueWrite,
    std::declval<T&>().inplace_within_range_val(
        0, 1, static_cast<const int*>(nullptr), 0, RangeType::IncInc));
BITSET_WRITE_TRAIT(
    HasArithWrite,
    std::declval<T&>().inplace_arith_compare(static_cast<const int*>(nullptr),
                                             int64_t(1),
                                             int64_t(0),
                                             0,
                                             ArithOpType::Add,
                                             CompareOpType::EQ));
#undef BITSET_WRITE_TRAIT

TEST(ReadOnlyBitsetViewTest, ConstBuffersAndLiveOwnerReads) {
    ReadOnlyOwner owner(193, false);
    const auto& const_owner = owner;
    ReadOnlyView view(const_owner);
    auto window = const_owner.view(7, 130);
    EXPECT_TRUE(view.none());
    EXPECT_EQ(window.count(), 0);
    owner.set(7);
    owner.set(136);
    EXPECT_TRUE(window[0]);
    EXPECT_TRUE(window[129]);
    EXPECT_EQ(window.count(), 2);
    EXPECT_EQ(window.find_first().value(), 0);
    EXPECT_EQ(window.find_next(0).value(), 129);
    owner.set(7, 130, true);
    EXPECT_TRUE(window.all());
    owner.reset(7, 130);
    EXPECT_TRUE(window.none());
    const ReadOnlyOwner copied(window);
    EXPECT_TRUE(copied.view() == window);
    EXPECT_TRUE(const_owner.view(7).view(0, 130) == window);
    EXPECT_TRUE((window + 130).empty());

    const uint64_t buffer[] = {0x81, 0};
    ReadOnlyView borrowed(buffer, 7, 65);
    EXPECT_TRUE(borrowed[0]);
    EXPECT_EQ(borrowed.count(), 1);
    EXPECT_EQ(borrowed.read(0, 64), 1);
    EXPECT_EQ(borrowed.read(64, 1), 0);
    ReadOnlyView empty;
    EXPECT_TRUE(empty.empty());
    EXPECT_EQ(empty.count(), 0);
    EXPECT_TRUE(empty.all());
    EXPECT_TRUE(empty.none());
}

template <typename Policy>
void
TestOwnerWindowWrites() {
    using Owner = Bitset<Policy, std::vector<typename Policy::data_type>, true>;
    const size_t offsets[] = {0, 1, 7, 8, 9, 63, 64, 65};
    const size_t lengths[] = {0, 1, 7, 8, 9, 63, 64, 65, 257, 1027};
    for (size_t begin : offsets) {
        for (size_t src_begin : offsets) {
            for (size_t length : lengths) {
                // Exact buffers with an untouched prefix and suffix.
                Owner original(begin + length + 11, false);
                Owner source(src_begin + length, false);
                for (size_t i = 0; i < original.size(); ++i)
                    original.set(i, (i % 3) == 0);
                for (size_t i = 0; i < source.size(); ++i)
                    source.set(i, (i % 5) < 2);
                const auto rhs = source.view(src_begin, length);
                for (int op = 0; op < 11; ++op) {
                    Owner actual = original.clone();
                    size_t counted = 0;
                    switch (op) {
                        case 0:
                            actual.inplace_and(rhs, length, begin);
                            break;
                        case 1:
                            actual.inplace_or(rhs, length, begin);
                            break;
                        case 2:
                            actual.inplace_xor(rhs, length, begin);
                            break;
                        case 3:
                            actual.inplace_sub(rhs, length, begin);
                            break;
                        case 4:
                            actual.inplace_and_flip(rhs, length, begin);
                            break;
                        case 5:
                            counted = actual.inplace_and_with_count(
                                rhs, length, begin);
                            break;
                        case 6:
                            counted = actual.inplace_or_with_count(
                                rhs, length, begin);
                            break;
                        case 7:
                            actual.flip(begin, length);
                            break;
                        case 8:
                            actual.set(begin, length, true);
                            break;
                        case 9:
                            actual.reset(begin, length);
                            break;
                        case 10:
                            actual.set(begin, length, false);
                            break;
                    }
                    size_t ones = 0;
                    for (size_t i = 0; i < actual.size(); ++i) {
                        bool expected = original[i];
                        if (i >= begin && i - begin < length) {
                            const bool y = rhs[i - begin];
                            switch (op) {
                                case 0:
                                case 5:
                                    expected = expected && y;
                                    break;
                                case 1:
                                case 6:
                                    expected = expected || y;
                                    break;
                                case 2:
                                    expected = expected != y;
                                    break;
                                case 3:
                                    expected = expected && !y;
                                    break;
                                case 4:
                                    expected = !(expected && y);
                                    break;
                                case 7:
                                    expected = !expected;
                                    break;
                                case 8:
                                    expected = true;
                                    break;
                                case 9:
                                case 10:
                                    expected = false;
                                    break;
                            }
                            ones += expected;
                        }
                        ASSERT_EQ(bool(actual[i]), expected)
                            << begin << ',' << src_begin << ',' << length << ','
                            << op << ',' << i;
                    }
                    if (op == 5)
                        ASSERT_EQ(counted, ones);
                    if (op == 6)
                        ASSERT_EQ(counted, length - ones);
                }
            }
        }
    }
    // An empty range at the end is valid; no input elements are accessed.
    Owner end(71, true);
    end.inplace_and(typename Owner::view_type{}, 0, end.size());
}

// Runtime and compile-time comparison dispatch both target a middle window.
TEST(ReadOnlyBitsetViewTest, ComparisonWritersPreserveAdjacentBits) {
    const int values[] = {-2, -1, 0, 1, 2, 3, 4, 5};
    ReadOnlyOwner owner(31, true);
    auto check = [&](auto write, auto predicate) {
        owner.set();
        write();
        for (size_t i = 0; i < owner.size(); ++i) {
            const bool expected =
                i >= 13 && i < 21 ? predicate(values[i - 13]) : true;
            ASSERT_EQ(bool(owner[i]), expected) << i;
        }
    };
    check(
        [&] { owner.inplace_compare_val(values, 8, 1, CompareOpType::GT, 13); },
        [](int v) { return v > 1; });
    check(
        [&] {
            owner.inplace_compare_val<int, CompareOpType::GT>(values, 8, 1, 13);
        },
        [](int v) { return v > 1; });
    check(
        [&] {
            owner.inplace_compare_column(
                values, values, 8, CompareOpType::NE, 13);
        },
        [](int) { return false; });
    check(
        [&] {
            owner.inplace_compare_column<int, int, CompareOpType::NE>(
                values, values, 8, 13);
        },
        [](int) { return false; });
    check(
        [&] {
            owner.inplace_within_range_val(
                0, 3, values, 8, RangeType::IncExc, 13);
        },
        [](int v) { return v >= 0 && v < 3; });
    check(
        [&] {
            owner.inplace_within_range_val<int, RangeType::IncExc>(
                0, 3, values, 8, 13);
        },
        [](int v) { return v >= 0 && v < 3; });
    check(
        [&] {
            owner.inplace_within_range_column(
                values, values, values, 8, RangeType::IncInc, 13);
        },
        [](int) { return true; });
    check(
        [&] {
            owner.inplace_within_range_column<int, RangeType::ExcExc>(
                values, values, values, 8, 13);
        },
        [](int) { return false; });
    check(
        [&] {
            owner.inplace_arith_compare(values,
                                        int64_t(2),
                                        int64_t(3),
                                        8,
                                        ArithOpType::Add,
                                        CompareOpType::GE,
                                        13);
        },
        [](int v) { return v + 2 >= 3; });
    check(
        [&] {
            owner.inplace_arith_compare<int,
                                        ArithOpType::Add,
                                        CompareOpType::GE>(
                values, int64_t(2), int64_t(3), 8, 13);
        },
        [](int v) { return v + 2 >= 3; });
}

TEST(ReadOnlyBitsetViewTest, OwnerRangeBoundaries) {
    using namespace milvus::bitset::detail;
    TestOwnerWindowWrites<ElementWiseBitsetPolicy<uint8_t>>();
    TestOwnerWindowWrites<ElementWiseBitsetPolicy<uint64_t>>();
    TestOwnerWindowWrites<BitWiseBitsetPolicy<uint8_t>>();
    TestOwnerWindowWrites<
        VectorizedElementWiseBitsetPolicy<uint64_t, VectorizedRef>>();
    TestOwnerWindowWrites<
        VectorizedElementWiseBitsetPolicy<uint64_t, VectorizedDynamic>>();
}

template <typename Policy>
void
TestOwnerMultipleWindows() {
    using Owner = Bitset<Policy, std::vector<typename Policy::data_type>, true>;
    using View = typename Owner::view_type;
    Owner dst(321, true);
    Owner a(257, false), b(257, false);
    for (size_t i = 0; i < 257; ++i) {
        a.set(i, i % 3 == 0);
        b.set(i, i % 5 == 0);
    }
    const View inputs[] = {a.view(7, 193), b.view(31, 193)};
    for (bool is_and : {true, false}) {
        dst.set(0, dst.size(), is_and);
        if (is_and) {
            dst.inplace_and(inputs, 2, 193, 65);
        } else {
            dst.inplace_or(inputs, 2, 193, 65);
        }
        for (size_t i = 0; i < dst.size(); ++i) {
            bool expected = is_and;
            if (i >= 65 && i < 258) {
                expected = is_and ? inputs[0][i - 65] && inputs[1][i - 65]
                                  : inputs[0][i - 65] || inputs[1][i - 65];
            }
            ASSERT_EQ(bool(dst[i]), expected) << i;
        }
    }
    auto before = dst.clone();
    dst.inplace_and(inputs, 0, 193, 65);
    dst.inplace_or(inputs, 0, 193, 65);
    dst.inplace_and(inputs, 2, 0, dst.size());
    dst.inplace_or(inputs, 2, 0, dst.size());
    EXPECT_TRUE(before == dst);

    // The owner-array overload also accepts a destination window.
    Owner owners[] = {a.clone(), b.clone()};
    dst.set();
    dst.inplace_and(owners, 2, 193, 65);
    EXPECT_TRUE(dst.view(0, 65).all());
    EXPECT_TRUE(dst.view(258).all());
    for (size_t i = 0; i < 193; ++i) {
        EXPECT_EQ(bool(dst[65 + i]), bool(a[i]) && bool(b[i]));
    }
    dst.reset();
    dst.inplace_or(owners, 2, 193, 65);
    EXPECT_TRUE(dst.view(0, 65).none());
    EXPECT_TRUE(dst.view(258).none());
    for (size_t i = 0; i < 193; ++i) {
        EXPECT_EQ(bool(dst[65 + i]), bool(a[i]) || bool(b[i]));
    }
}

TEST(ReadOnlyBitsetViewTest, MultipleSourcesUseIndependentWindows) {
    using namespace milvus::bitset::detail;
    TestOwnerMultipleWindows<ElementWiseBitsetPolicy<uint8_t>>();
    TestOwnerMultipleWindows<ElementWiseBitsetPolicy<uint64_t>>();
    TestOwnerMultipleWindows<BitWiseBitsetPolicy<uint8_t>>();
    TestOwnerMultipleWindows<
        VectorizedElementWiseBitsetPolicy<uint64_t, VectorizedRef>>();
    TestOwnerMultipleWindows<
        VectorizedElementWiseBitsetPolicy<uint64_t, VectorizedDynamic>>();
}

}  // namespace

int
main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
