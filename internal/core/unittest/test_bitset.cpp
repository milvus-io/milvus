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

#include <cassert>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <random>
#include <string>
#include <tuple>
#include <vector>

#include "bitset/bitset.h"
#include "bitset/detail/bit_wise.h"
#include "bitset/detail/element_wise.h"
#include "bitset/detail/element_vectorized.h"
#include "bitset/detail/platform/dynamic.h"
#include "bitset/detail/platform/vectorized_ref.h"

#if defined(__x86_64__)
#include "bitset/detail/platform/x86/avx2.h"
#include "bitset/detail/platform/x86/avx512.h"
#include "bitset/detail/platform/x86/instruction_set.h"
#endif

#if defined(__aarch64__)
#include "bitset/detail/platform/arm/neon.h"

#ifdef __ARM_FEATURE_SVE
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
FillRandom(BitsetT& bitset, std::default_random_engine& rng) {
    std::uniform_int_distribution<uint8_t> tt(0, 1);
    for (size_t i = 0; i < bitset.size(); i++) {
        bitset[i] = (tt(rng) == 0);
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
TestFindImpl(BitsetT& bitset, const size_t max_v) {
    const size_t n = bitset.size();

    std::default_random_engine rng(123);
    std::uniform_int_distribution<int8_t> u(0, max_v);

    std::vector<size_t> one_pos;
    for (size_t i = 0; i < n; i++) {
        bool enabled = (u(rng) == 0);
        if (enabled) {
            one_pos.push_back(i);
            bitset[i] = true;
        }
    }

    StopWatch sw;

    auto bit_idx = bitset.find_first();
    if (!bit_idx.has_value()) {
        ASSERT_EQ(one_pos.size(), 0);
        return;
    }

    for (size_t i = 0; i < one_pos.size(); i++) {
        ASSERT_TRUE(bit_idx.has_value()) << n << ", " << max_v;
        ASSERT_EQ(bit_idx.value(), one_pos[i]) << n << ", " << max_v;
        bit_idx = bitset.find_next(bit_idx.value());
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
        for (const size_t pr : {1, 100}) {
            BitsetT bitset(n);
            bitset.reset();

            if (print_log) {
                printf("Testing bitset, n=%zd, pr=%zd\n", n, pr);
            }

            TestFindImpl(bitset, pr);

            for (const size_t offset : typical_offsets) {
                if (offset >= n) {
                    continue;
                }

                bitset.reset();
                auto view = bitset.view(offset);

                if (print_log) {
                    printf("Testing bitset view, n=%zd, offset=%zd, pr=%zd\n",
                           n,
                           offset,
                           pr);
                }

                TestFindImpl(view, pr);
            }
        }
    }
}

//
TEST(FindRef, f) {
    using impl_traits = RefImplTraits<uint64_t, uint8_t>;
    TestFindImpl<typename impl_traits::bitset_type>();
}

//
TEST(FindElement, f) {
    using impl_traits = ElementImplTraits<uint64_t, uint8_t>;
    TestFindImpl<typename impl_traits::bitset_type>();
}

// //
// TEST(FindVectorizedAvx2, f) {
//     TestFindImpl<avx2_u64_u8::bitset_type>();
// }

//////////////////////////////////////////////////////////////////////////////////////////

//
template <typename BitsetT, typename T, typename U>
void
TestInplaceCompareColumnImpl(BitsetT& bitset, CompareOpType op) {
    const size_t n = bitset.size();
    constexpr size_t max_v = 2;

    std::vector<T> t(n, from_i32<T>(0));
    std::vector<U> u(n, from_i32<T>(0));

    std::default_random_engine rng(123);
    FillRandom(t, rng, max_v);
    FillRandom(u, rng, max_v);

    StopWatch sw;
    bitset.inplace_compare_column(t.data(), u.data(), n, op);

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
                auto view = bitset.view(offset);

                if (print_log) {
                    printf("Testing bitset view, n=%zd, offset=%zd, op=%zd\n",
                           n,
                           offset,
                           (size_t)op);
                }

                TestInplaceCompareColumnImpl<decltype(view), T, U>(view, op);
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
#if defined(__aarch64__) && defined(__ARM_FEATURE_SVE)
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
TestInplaceCompareValImpl(BitsetT& bitset, CompareOpType op) {
    const size_t n = bitset.size();
    constexpr size_t max_v = 3;
    const T value = from_i32<T>(1);

    std::vector<T> t(n, from_i32<T>(0));

    std::default_random_engine rng(123);
    FillRandom(t, rng, max_v);

    StopWatch sw;
    bitset.inplace_compare_val(t.data(), n, value, op);

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
                auto view = bitset.view(offset);

                if (print_log) {
                    printf("Testing bitset view, n=%zd, offset=%zd, op=%zd\n",
                           n,
                           offset,
                           (size_t)op);
                }

                TestInplaceCompareValImpl<decltype(view), T>(view, op);
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
#if defined(__aarch64__) && defined(__ARM_FEATURE_SVE)
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
TestInplaceWithinRangeColumnImpl(BitsetT& bitset, RangeType op) {
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
    bitset.inplace_within_range_column(
        lower.data(), upper.data(), values.data(), n, op);

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
                auto view = bitset.view(offset);

                if (print_log) {
                    printf("Testing bitset view, n=%zd, offset=%zd, op=%zd\n",
                           n,
                           offset,
                           (size_t)op);
                }

                TestInplaceWithinRangeColumnImpl<decltype(view), T>(view, op);
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
#if defined(__aarch64__) && defined(__ARM_FEATURE_SVE)
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
TestInplaceWithinRangeValImpl(BitsetT& bitset, RangeType op) {
    const size_t n = bitset.size();
    constexpr size_t max_v = 10;
    const T lower_v = from_i32<T>(3);
    const T upper_v = from_i32<T>(7);

    std::vector<T> values(n, from_i32<T>(0));

    std::default_random_engine rng(123);
    FillRandom(values, rng, max_v);

    StopWatch sw;
    bitset.inplace_within_range_val(lower_v, upper_v, values.data(), n, op);

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
                auto view = bitset.view(offset);

                if (print_log) {
                    printf("Testing bitset view, n=%zd, offset=%zd, op=%zd\n",
                           n,
                           offset,
                           (size_t)op);
                }

                TestInplaceWithinRangeValImpl<decltype(view), T>(view, op);
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
#if defined(__aarch64__) && defined(__ARM_FEATURE_SVE)
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
    process(BitsetT& bitset, ArithOpType a_op, CompareOpType cmp_op) {
        using HT = ArithHighPrecisionType<T>;

        const size_t n = bitset.size();
        constexpr size_t max_v = 10;

        std::vector<T> left(n, 0);
        const HT right_operand = from_i32<HT>(2);
        const HT value = from_i32<HT>(5);

        std::default_random_engine rng(123);
        FillRandom(left, rng, max_v);

        StopWatch sw;
        bitset.inplace_arith_compare(
            left.data(), right_operand, value, n, a_op, cmp_op);

        if (print_timing) {
            printf("elapsed %f\n", sw.elapsed());
        }

        for (size_t i = 0; i < n; i++) {
            if (a_op == ArithOpType::Add) {
                if (cmp_op == CompareOpType::EQ) {
                    ASSERT_EQ((left[i] + right_operand) == value, bitset[i])
                        << i;
                } else if (cmp_op == CompareOpType::GE) {
                    ASSERT_EQ((left[i] + right_operand) >= value, bitset[i])
                        << i;
                } else if (cmp_op == CompareOpType::GT) {
                    ASSERT_EQ((left[i] + right_operand) > value, bitset[i])
                        << i;
                } else if (cmp_op == CompareOpType::LE) {
                    ASSERT_EQ((left[i] + right_operand) <= value, bitset[i])
                        << i;
                } else if (cmp_op == CompareOpType::LT) {
                    ASSERT_EQ((left[i] + right_operand) < value, bitset[i])
                        << i;
                } else if (cmp_op == CompareOpType::NE) {
                    ASSERT_EQ((left[i] + right_operand) != value, bitset[i])
                        << i;
                } else {
                    ASSERT_TRUE(false) << "Not implemented";
                }
            } else if (a_op == ArithOpType::Sub) {
                if (cmp_op == CompareOpType::EQ) {
                    ASSERT_EQ((left[i] - right_operand) == value, bitset[i])
                        << i;
                } else if (cmp_op == CompareOpType::GE) {
                    ASSERT_EQ((left[i] - right_operand) >= value, bitset[i])
                        << i;
                } else if (cmp_op == CompareOpType::GT) {
                    ASSERT_EQ((left[i] - right_operand) > value, bitset[i])
                        << i;
                } else if (cmp_op == CompareOpType::LE) {
                    ASSERT_EQ((left[i] - right_operand) <= value, bitset[i])
                        << i;
                } else if (cmp_op == CompareOpType::LT) {
                    ASSERT_EQ((left[i] - right_operand) < value, bitset[i])
                        << i;
                } else if (cmp_op == CompareOpType::NE) {
                    ASSERT_EQ((left[i] - right_operand) != value, bitset[i])
                        << i;
                } else {
                    ASSERT_TRUE(false) << "Not implemented";
                }
            } else if (a_op == ArithOpType::Mul) {
                if (cmp_op == CompareOpType::EQ) {
                    ASSERT_EQ((left[i] * right_operand) == value, bitset[i])
                        << i;
                } else if (cmp_op == CompareOpType::GE) {
                    ASSERT_EQ((left[i] * right_operand) >= value, bitset[i])
                        << i;
                } else if (cmp_op == CompareOpType::GT) {
                    ASSERT_EQ((left[i] * right_operand) > value, bitset[i])
                        << i;
                } else if (cmp_op == CompareOpType::LE) {
                    ASSERT_EQ((left[i] * right_operand) <= value, bitset[i])
                        << i;
                } else if (cmp_op == CompareOpType::LT) {
                    ASSERT_EQ((left[i] * right_operand) < value, bitset[i])
                        << i;
                } else if (cmp_op == CompareOpType::NE) {
                    ASSERT_EQ((left[i] * right_operand) != value, bitset[i])
                        << i;
                } else {
                    ASSERT_TRUE(false) << "Not implemented";
                }
            } else if (a_op == ArithOpType::Div) {
                if (cmp_op == CompareOpType::EQ) {
                    ASSERT_EQ((left[i] / right_operand) == value, bitset[i])
                        << i;
                } else if (cmp_op == CompareOpType::GE) {
                    ASSERT_EQ((left[i] / right_operand) >= value, bitset[i])
                        << i;
                } else if (cmp_op == CompareOpType::GT) {
                    ASSERT_EQ((left[i] / right_operand) > value, bitset[i])
                        << i;
                } else if (cmp_op == CompareOpType::LE) {
                    ASSERT_EQ((left[i] / right_operand) <= value, bitset[i])
                        << i;
                } else if (cmp_op == CompareOpType::LT) {
                    ASSERT_EQ((left[i] / right_operand) < value, bitset[i])
                        << i;
                } else if (cmp_op == CompareOpType::NE) {
                    ASSERT_EQ((left[i] / right_operand) != value, bitset[i])
                        << i;
                } else {
                    ASSERT_TRUE(false) << "Not implemented";
                }
            } else if (a_op == ArithOpType::Mod) {
                if (cmp_op == CompareOpType::EQ) {
                    ASSERT_EQ(fmod(left[i], right_operand) == value, bitset[i])
                        << i;
                } else if (cmp_op == CompareOpType::GE) {
                    ASSERT_EQ(fmod(left[i], right_operand) >= value, bitset[i])
                        << i;
                } else if (cmp_op == CompareOpType::GT) {
                    ASSERT_EQ(fmod(left[i], right_operand) > value, bitset[i])
                        << i;
                } else if (cmp_op == CompareOpType::LE) {
                    ASSERT_EQ(fmod(left[i], right_operand) <= value, bitset[i])
                        << i;
                } else if (cmp_op == CompareOpType::LT) {
                    ASSERT_EQ(fmod(left[i], right_operand) < value, bitset[i])
                        << i;
                } else if (cmp_op == CompareOpType::NE) {
                    ASSERT_EQ(fmod(left[i], right_operand) != value, bitset[i])
                        << i;
                } else {
                    ASSERT_TRUE(false) << "Not implemented";
                }
            } else {
                ASSERT_TRUE(false) << "Not implemented";
            }
        }
    }
};

template <typename BitsetT>
struct TestInplaceArithCompareImplS<BitsetT, std::string> {
    static void
    process(BitsetT&, ArithOpType, CompareOpType) {
        // does nothing
    }
};

template <typename BitsetT, typename T>
void
TestInplaceArithCompareImpl() {
    for (const size_t n : typical_sizes) {
        for (const auto a_op : typical_arith_ops) {
            for (const auto cmp_op : typical_compare_ops) {
                BitsetT bitset(n);
                bitset.reset();

                if (print_log) {
                    printf(
                        "Testing bitset, n=%zd, a_op=%zd\n", n, (size_t)a_op);
                }

                TestInplaceArithCompareImplS<BitsetT, T>::process(
                    bitset, a_op, cmp_op);

                for (const size_t offset : typical_offsets) {
                    if (offset >= n) {
                        continue;
                    }

                    bitset.reset();
                    auto view = bitset.view(offset);

                    if (print_log) {
                        printf(
                            "Testing bitset view, n=%zd, offset=%zd, a_op=%zd, "
                            "cmp_op=%zd\n",
                            n,
                            offset,
                            (size_t)a_op,
                            (size_t)cmp_op);
                    }

                    TestInplaceArithCompareImplS<decltype(view), T>::process(
                        view, a_op, cmp_op);
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
#if defined(__aarch64__) && defined(__ARM_FEATURE_SVE)
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

TEST(Append, BitWise) {
    using impl_traits = RefImplTraits<uint64_t, uint8_t>;
    TestAppendImpl<typename impl_traits::bitset_type>();
}

TEST(Append, ElementWise) {
    using impl_traits = ElementImplTraits<uint64_t, uint8_t>;
    TestAppendImpl<typename impl_traits::bitset_type>();
}

//////////////////////////////////////////////////////////////////////////////////////////

//
template <typename BitsetT>
void
TestCountImpl(BitsetT& bitset, const size_t max_v) {
    const size_t n = bitset.size();

    std::default_random_engine rng(123);
    std::uniform_int_distribution<int8_t> u(0, max_v);

    std::vector<size_t> one_pos;
    for (size_t i = 0; i < n; i++) {
        bool enabled = (u(rng) == 0);
        if (enabled) {
            one_pos.push_back(i);
            bitset[i] = true;
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
                auto view = bitset.view(offset);

                if (print_log) {
                    printf("Testing bitset view, n=%zd, offset=%zd, pr=%zd\n",
                           n,
                           offset,
                           pr);
                }

                TestCountImpl(view, pr);
            }
        }
    }
}

//
TEST(CountRef, f) {
    using impl_traits = RefImplTraits<uint64_t, uint8_t>;
    TestCountImpl<typename impl_traits::bitset_type>();
}

//
TEST(CountElement, f) {
    using impl_traits = ElementImplTraits<uint64_t, uint8_t>;
    TestCountImpl<typename impl_traits::bitset_type>();
}

//////////////////////////////////////////////////////////////////////////////////////////

int
main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
