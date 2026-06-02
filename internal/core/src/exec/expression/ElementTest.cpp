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

#include <gtest/gtest.h>
#include <algorithm>
#include <cstdint>
#include <numeric>
#include <random>
#include <string>
#include <vector>

#include "exec/expression/Element.h"
#include <xsimd/xsimd.hpp>
#include "common/SimdUtil.h"

using namespace milvus::exec;
using VT = MultiElement::ValueType;

// Construct ValueType with explicit type to avoid implicit integer promotion
// (e.g. int8_t -> int in std::variant). This matches how TermExpr.cpp calls
// In() using std::in_place_type<T>.
template <typename T>
VT
MakeVT(T v) {
    return VT(std::in_place_type<T>, v);
}

// ═══════════════════════════════════════════════════════════════════════════
// SimdBatchElement::In() tests — per-row lookup
// ═══════════════════════════════════════════════════════════════════════════

TEST(SimdBatchElementTest, Int8) {
    std::vector<int8_t> vals = {-128, -1, 0, 1, 127};
    SimdBatchElement<int8_t> sb(vals);
    EXPECT_EQ(sb.Size(), 5);
    for (auto v : vals) {
        EXPECT_TRUE(sb.In(MakeVT(v)));
    }
    EXPECT_FALSE(sb.In(MakeVT(int8_t(2))));
    EXPECT_FALSE(sb.In(MakeVT(int8_t(100))));
}

TEST(SimdBatchElementTest, Int16) {
    std::vector<int16_t> vals = {-32768, -1, 0, 1, 32767};
    SimdBatchElement<int16_t> sb(vals);
    for (auto v : vals) {
        EXPECT_TRUE(sb.In(MakeVT(v)));
    }
    EXPECT_FALSE(sb.In(MakeVT(int16_t(2))));
}

TEST(SimdBatchElementTest, Int32) {
    std::vector<int32_t> vals = {-100000, -1, 0, 1, 42, 100000};
    SimdBatchElement<int32_t> sb(vals);
    for (auto v : vals) {
        EXPECT_TRUE(sb.In(MakeVT(v)));
    }
    EXPECT_FALSE(sb.In(MakeVT(int32_t(2))));
}

TEST(SimdBatchElementTest, Int64) {
    std::vector<int64_t> vals = {-1000000000LL, -1, 0, 1, 1000000000LL};
    SimdBatchElement<int64_t> sb(vals);
    for (auto v : vals) {
        EXPECT_TRUE(sb.In(MakeVT(v)));
    }
    EXPECT_FALSE(sb.In(MakeVT(int64_t(2))));
}

TEST(SimdBatchElementTest, Float) {
    std::vector<float> vals = {-1.5f, 0.0f, 1.5f, 1e10f};
    SimdBatchElement<float> sb(vals);
    for (auto v : vals) {
        EXPECT_TRUE(sb.In(MakeVT(v)));
    }
    EXPECT_FALSE(sb.In(MakeVT(1.6f)));
    EXPECT_FALSE(sb.In(MakeVT(0.1f)));
}

TEST(SimdBatchElementTest, Double) {
    std::vector<double> vals = {-1.5, 0.0, 1.5, 1e100};
    SimdBatchElement<double> sb(vals);
    for (auto v : vals) {
        EXPECT_TRUE(sb.In(MakeVT(v)));
    }
    EXPECT_FALSE(sb.In(MakeVT(1.6)));
}

TEST(SimdBatchElementTest, Empty) {
    SimdBatchElement<int32_t> sb(std::vector<int32_t>{});
    EXPECT_TRUE(sb.Empty());
    EXPECT_FALSE(sb.In(MakeVT(int32_t(0))));
}

TEST(SimdBatchElementTest, SingleElement) {
    SimdBatchElement<int64_t> sb(std::vector<int64_t>{42});
    EXPECT_TRUE(sb.In(MakeVT(int64_t(42))));
    EXPECT_FALSE(sb.In(MakeVT(int64_t(0))));
    EXPECT_FALSE(sb.In(MakeVT(int64_t(43))));
}

TEST(SimdBatchElementTest, GetElements) {
    std::vector<int32_t> vals = {1, 2, 3};
    SimdBatchElement<int32_t> sb(vals);
    auto result = sb.GetElements();
    std::vector<int32_t> expected = {1, 2, 3};
    EXPECT_EQ(result, expected);
}

// ═══════════════════════════════════════════════════════════════════════════
// SimdBatchElement::FilterChunk() tests — batch SIMD path
//
// For each numeric type: fill a data buffer, run FilterChunk, verify that
// the result bitmap matches element-wise In() exactly.
// ═══════════════════════════════════════════════════════════════════════════

// Helper: run FilterChunk and verify against per-row In() as ground truth.
template <typename T>
void
VerifyFilterChunk(const std::vector<T>& in_vals,
                  const std::vector<T>& data,
                  const std::string& label = "") {
    // SimdBatchElement requires pre-sorted, deduplicated input
    auto sorted = in_vals;
    std::sort(sorted.begin(), sorted.end());
    sorted.erase(std::unique(sorted.begin(), sorted.end()), sorted.end());
    SimdBatchElement<T> sb(sorted);
    SetElement<T> se(in_vals);

    const int n = static_cast<int>(data.size());
    milvus::TargetBitmap bm_filter(n, false);
    milvus::TargetBitmap bm_in(n, false);

    milvus::TargetBitmapView view_filter(bm_filter.data(), n);
    milvus::TargetBitmapView view_in(bm_in.data(), n);

    sb.FilterChunk(data.data(), n, view_filter);

    // Ground truth: per-row In() via SetElement (hash-based, known correct)
    for (int i = 0; i < n; ++i) {
        view_in[i] = se.In(MakeVT(data[i]));
    }

    for (int i = 0; i < n; ++i) {
        EXPECT_EQ(view_filter[i], view_in[i])
            << label << " mismatch at index " << i << " data=" << data[i];
    }
}

TEST(FilterChunkTest, Int8Basic) {
    std::vector<int8_t> in_vals = {-128, -1, 0, 1, 127};
    // Data: sequential -128..127 covers all int8 values
    std::vector<int8_t> data(256);
    for (int i = 0; i < 256; ++i) {
        data[i] = static_cast<int8_t>(i - 128);
    }
    VerifyFilterChunk(in_vals, data, "Int8Basic");
}

TEST(FilterChunkTest, Int16Basic) {
    std::vector<int16_t> in_vals = {-32768, -1000, -1, 0, 1, 1000, 32767};
    std::vector<int16_t> data(512);
    for (int i = 0; i < 512; ++i) {
        data[i] = static_cast<int16_t>(i - 256);
    }
    VerifyFilterChunk(in_vals, data, "Int16Basic");
}

TEST(FilterChunkTest, Int32Basic) {
    std::vector<int32_t> in_vals = {-999999, -1, 0, 1, 42, 999999};
    std::vector<int32_t> data(1024);
    std::iota(data.begin(), data.end(), -512);
    VerifyFilterChunk(in_vals, data, "Int32Basic");
}

TEST(FilterChunkTest, Int64Basic) {
    std::vector<int64_t> in_vals = {-1000000000LL, -1, 0, 1, 1000000000LL};
    std::vector<int64_t> data(512);
    std::iota(data.begin(), data.end(), -256);
    VerifyFilterChunk(in_vals, data, "Int64Basic");
}

TEST(FilterChunkTest, FloatBasic) {
    std::vector<float> in_vals = {-1.5f, 0.0f, 1.5f, 100.0f};
    std::vector<float> data(256);
    for (int i = 0; i < 256; ++i) {
        data[i] = static_cast<float>(i) * 0.5f - 64.0f;
    }
    VerifyFilterChunk(in_vals, data, "FloatBasic");
}

TEST(FilterChunkTest, DoubleBasic) {
    std::vector<double> in_vals = {-1.5, 0.0, 1.5, 100.0};
    std::vector<double> data(256);
    for (int i = 0; i < 256; ++i) {
        data[i] = static_cast<double>(i) * 0.5 - 64.0;
    }
    VerifyFilterChunk(in_vals, data, "DoubleBasic");
}

// Empty IN list — FilterChunk should leave bitmap all-false.
TEST(FilterChunkTest, EmptyInList) {
    SimdBatchElement<int32_t> sb(std::vector<int32_t>{});
    std::vector<int32_t> data = {1, 2, 3, 4, 5};
    milvus::TargetBitmap bm(5, false);
    milvus::TargetBitmapView view(bm.data(), 5);
    sb.FilterChunk(data.data(), 5, view);
    for (int i = 0; i < 5; ++i) {
        EXPECT_FALSE(view[i]);
    }
}

// Single element — tests broadcast correctness.
TEST(FilterChunkTest, SingleInValue) {
    std::vector<int64_t> in_vals = {42};
    std::vector<int64_t> data = {41, 42, 43, 42, 0, 42};
    VerifyFilterChunk(in_vals, data, "SingleInValue");
}

// All data matches — every bit should be set.
TEST(FilterChunkTest, AllMatch) {
    std::vector<int32_t> in_vals = {7};
    std::vector<int32_t> data(128, 7);
    VerifyFilterChunk(in_vals, data, "AllMatch");
}

// No data matches — bitmap should stay all-false.
TEST(FilterChunkTest, NoneMatch) {
    std::vector<int32_t> in_vals = {999};
    std::vector<int32_t> data(128);
    std::iota(data.begin(), data.end(), 0);
    VerifyFilterChunk(in_vals, data, "NoneMatch");
}

// Scalar tail: data size not a multiple of SIMD lane width.
// Tests sizes 1..kLanes+1 to exercise all tail lengths.
TEST(FilterChunkTest, ScalarTailAllSizes) {
    using Batch = xsimd::batch<int32_t>;
    constexpr int kLanes = Batch::size;
    std::vector<int32_t> in_vals = {3, 7, 15};

    for (int sz = 1; sz <= kLanes + 1; ++sz) {
        std::vector<int32_t> data(sz);
        std::iota(data.begin(), data.end(), 0);
        VerifyFilterChunk(in_vals, data, "TailSize=" + std::to_string(sz));
    }
}

// Large data: multiple full SIMD chunks + tail.
TEST(FilterChunkTest, LargeData) {
    std::vector<int32_t> in_vals = {10, 100, 500, 999};
    std::vector<int32_t> data(8192);
    std::iota(data.begin(), data.end(), 0);
    VerifyFilterChunk(in_vals, data, "LargeData");
}

// IN list at threshold boundary (64 — the SIMD/hash cutover).
TEST(FilterChunkTest, MaxSimdSize) {
    constexpr size_t kMax = 64;
    std::vector<int32_t> in_vals(kMax);
    std::iota(in_vals.begin(), in_vals.end(), 0);

    // Data contains some IN values and some not.
    std::vector<int32_t> data(kMax * 2);
    std::iota(data.begin(), data.end(), -static_cast<int32_t>(kMax / 2));
    VerifyFilterChunk(in_vals, data, "MaxSimdSize");
}

// Randomized stress test: random data + random IN values.
TEST(FilterChunkTest, RandomizedInt32) {
    std::mt19937 rng(42);
    std::uniform_int_distribution<int32_t> dist(-1000, 1000);

    std::vector<int32_t> in_vals(20);
    for (auto& v : in_vals) v = dist(rng);

    std::vector<int32_t> data(4096);
    for (auto& v : data) v = dist(rng);

    VerifyFilterChunk(in_vals, data, "RandomizedInt32");
}

TEST(FilterChunkTest, RandomizedInt8) {
    std::mt19937 rng(123);
    std::uniform_int_distribution<int16_t> dist(-128, 127);

    std::vector<int8_t> in_vals(10);
    for (auto& v : in_vals) v = static_cast<int8_t>(dist(rng));

    std::vector<int8_t> data(1024);
    for (auto& v : data) v = static_cast<int8_t>(dist(rng));

    VerifyFilterChunk(in_vals, data, "RandomizedInt8");
}

TEST(FilterChunkTest, RandomizedFloat) {
    std::mt19937 rng(77);
    std::uniform_real_distribution<float> dist(-100.0f, 100.0f);

    std::vector<float> in_vals(15);
    for (auto& v : in_vals) v = dist(rng);

    // Embed some known matches into data
    std::vector<float> data(2048);
    for (auto& v : data) v = dist(rng);
    for (size_t i = 0; i < in_vals.size(); ++i) {
        data[i * 100] = in_vals[i];
    }

    VerifyFilterChunk(in_vals, data, "RandomizedFloat");
}

TEST(FilterChunkTest, RandomizedDouble) {
    std::mt19937 rng(99);
    std::uniform_real_distribution<double> dist(-1e6, 1e6);

    std::vector<double> in_vals(8);
    for (auto& v : in_vals) v = dist(rng);

    std::vector<double> data(1024);
    for (auto& v : data) v = dist(rng);
    for (size_t i = 0; i < in_vals.size(); ++i) {
        data[i * 50] = in_vals[i];
    }

    VerifyFilterChunk(in_vals, data, "RandomizedDouble");
}

// ═══════════════════════════════════════════════════════════════════════════
// Non-zero-offset BitsetView — verifies the scalar fallback path in
// FilterChunk when the bitmap has a non-zero bit offset.
// ═══════════════════════════════════════════════════════════════════════════

template <typename T>
void
VerifyFilterChunkWithOffset(const std::vector<T>& in_vals,
                            const std::vector<T>& data,
                            int offset,
                            const std::string& label = "") {
    // SimdBatchElement requires pre-sorted, deduplicated input
    auto sorted = in_vals;
    std::sort(sorted.begin(), sorted.end());
    sorted.erase(std::unique(sorted.begin(), sorted.end()), sorted.end());
    SimdBatchElement<T> sb(sorted);
    SetElement<T> se(in_vals);

    const int n = static_cast<int>(data.size());

    // Create a bitmap larger than needed, with a non-zero bit offset.
    // BitsetView(data, offset, size) — offset shifts bit position 0.
    milvus::TargetBitmap bm_backing(n + offset, false);
    milvus::TargetBitmapView view_with_offset(bm_backing.data(), offset, n);

    sb.FilterChunk(data.data(), n, view_with_offset);

    // Ground truth
    for (int i = 0; i < n; ++i) {
        bool expected = se.In(MakeVT(data[i]));
        EXPECT_EQ(bool(view_with_offset[i]), expected)
            << label << " offset=" << offset << " mismatch at index " << i
            << " data=" << data[i];
    }
}

TEST(FilterChunkTest, NonZeroOffset_Int32) {
    std::vector<int32_t> in_vals = {-1, 0, 1, 42, 999};
    std::vector<int32_t> data(512);
    std::iota(data.begin(), data.end(), -256);
    for (int off : {1, 3, 7, 8, 13, 31, 63}) {
        VerifyFilterChunkWithOffset(
            in_vals, data, off, "Int32_off" + std::to_string(off));
    }
}

TEST(FilterChunkTest, NonZeroOffset_Int8) {
    std::vector<int8_t> in_vals = {-128, -1, 0, 1, 127};
    std::vector<int8_t> data(256);
    for (int i = 0; i < 256; ++i) data[i] = static_cast<int8_t>(i - 128);
    for (int off : {1, 5, 7}) {
        VerifyFilterChunkWithOffset(
            in_vals, data, off, "Int8_off" + std::to_string(off));
    }
}

TEST(FilterChunkTest, NonZeroOffset_Double) {
    std::vector<double> in_vals = {-1.5, 0.0, 1.5, 100.0};
    std::vector<double> data(256);
    for (int i = 0; i < 256; ++i) data[i] = static_cast<double>(i) * 0.5 - 64.0;
    for (int off : {1, 4, 7}) {
        VerifyFilterChunkWithOffset(
            in_vals, data, off, "Double_off" + std::to_string(off));
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// toBitMask tests — verify the SimdUtil.h bitmask extraction
//
// Uses xsimd::batch_bool for the default arch to test whichever SIMD path
// the build machine supports (AVX512, AVX2, SSE2, NEON, or generic).
// ═══════════════════════════════════════════════════════════════════════════

template <typename T>
void
VerifyToBitMask() {
    using Batch = xsimd::batch<T>;
    using BatchBool = xsimd::batch_bool<T>;
    constexpr int kLanes = Batch::size;

    // All-false mask → 0
    {
        BatchBool mask(false);
        uint64_t result = milvus::toBitMask(mask);
        EXPECT_EQ(result, 0u) << "all-false should be 0";
    }

    // All-true mask → (1 << kLanes) - 1
    {
        BatchBool mask(true);
        uint64_t result = milvus::toBitMask(mask);
        uint64_t expected =
            (kLanes == 64) ? ~uint64_t(0) : (uint64_t(1) << kLanes) - 1;
        EXPECT_EQ(result, expected)
            << "all-true should set " << kLanes << " bits";
    }

    // Single lane set: broadcast a value, compare equal, check mask.
    // Put the target value at each position and verify the right bit is set.
    for (int pos = 0; pos < kLanes; ++pos) {
        alignas(64) T data[64] = {};  // zero-initialized
        data[pos] = T(42);

        auto d = Batch::load_unaligned(data);
        auto target = Batch::broadcast(T(42));
        auto mask = (d == target);
        uint64_t result = milvus::toBitMask(mask);

        EXPECT_EQ(result, uint64_t(1) << pos)
            << "only bit " << pos << " should be set";
    }

    // Multiple lanes set
    {
        alignas(64) T data[64] = {};
        // Set lanes 0 and kLanes-1
        data[0] = T(99);
        data[kLanes - 1] = T(99);

        auto d = Batch::load_unaligned(data);
        auto target = Batch::broadcast(T(99));
        auto mask = (d == target);
        uint64_t result = milvus::toBitMask(mask);

        uint64_t expected = uint64_t(1) | (uint64_t(1) << (kLanes - 1));
        EXPECT_EQ(result, expected)
            << "bits 0 and " << (kLanes - 1) << " should be set";
    }
}

TEST(ToBitMaskTest, Int8) {
    VerifyToBitMask<int8_t>();
}

TEST(ToBitMaskTest, Int16) {
    VerifyToBitMask<int16_t>();
}

TEST(ToBitMaskTest, Int32) {
    VerifyToBitMask<int32_t>();
}

TEST(ToBitMaskTest, Int64) {
    VerifyToBitMask<int64_t>();
}

TEST(ToBitMaskTest, Float) {
    VerifyToBitMask<float>();
}

TEST(ToBitMaskTest, Double) {
    VerifyToBitMask<double>();
}

// Verify kAllSet matches expected value for each type.
TEST(ToBitMaskTest, KAllSetConsistency) {
    auto check = [](auto kAllSet, int lanes, const char* name) {
        uint64_t expected =
            (lanes == 64) ? ~uint64_t(0) : (uint64_t(1) << lanes) - 1;
        EXPECT_EQ(kAllSet, expected) << name << " kAllSet mismatch";
    };

    using A = xsimd::default_arch;
    check(milvus::BitMask<int8_t, A>::kAllSet,
          xsimd::batch_bool<int8_t, A>::size,
          "int8");
    check(milvus::BitMask<int16_t, A>::kAllSet,
          xsimd::batch_bool<int16_t, A>::size,
          "int16");
    check(milvus::BitMask<int32_t, A>::kAllSet,
          xsimd::batch_bool<int32_t, A>::size,
          "int32");
    check(milvus::BitMask<int64_t, A>::kAllSet,
          xsimd::batch_bool<int64_t, A>::size,
          "int64");
    check(milvus::BitMask<float, A>::kAllSet,
          xsimd::batch_bool<float, A>::size,
          "float");
    check(milvus::BitMask<double, A>::kAllSet,
          xsimd::batch_bool<double, A>::size,
          "double");
}

// ═══════════════════════════════════════════════════════════════════════════
// SetElement<std::string> tests
// ═══════════════════════════════════════════════════════════════════════════

TEST(SetElementStringTest, Basic) {
    std::vector<std::string> vals = {"hello", "world", "test"};
    SetElement<std::string> fh(vals);
    EXPECT_EQ(fh.Size(), 3);
    EXPECT_TRUE(fh.In(MakeVT(std::string("hello"))));
    EXPECT_TRUE(fh.In(MakeVT(std::string("world"))));
    EXPECT_FALSE(fh.In(MakeVT(std::string("Hello"))));
    EXPECT_FALSE(fh.In(MakeVT(std::string(""))));
}

TEST(SetElementStringTest, StringView) {
    SetElement<std::string> fh(std::vector<std::string>{"alpha", "beta"});
    EXPECT_TRUE(fh.In(MakeVT(std::string_view("alpha"))));
    EXPECT_FALSE(fh.In(MakeVT(std::string_view("gamma"))));
}

TEST(SetElementStringTest, LongStrings) {
    std::string s1(1000, 'a');
    std::string s2(1000, 'b');
    std::string s3 = s1;
    s3[999] = 'z';  // differs at end (beyond 8-byte hash window)

    SetElement<std::string> fh(std::vector<std::string>{s1, s2});
    EXPECT_TRUE(fh.In(MakeVT(s1)));
    EXPECT_TRUE(fh.In(MakeVT(s2)));
    // s3 has same first 8 bytes as s1 but differs — hash may collide,
    // but full strcmp in ankerl ensures correctness
    EXPECT_FALSE(fh.In(MakeVT(s3)));
}

TEST(SetElementStringTest, Empty) {
    SetElement<std::string> fh(std::vector<std::string>{});
    EXPECT_TRUE(fh.Empty());
    EXPECT_FALSE(fh.In(MakeVT(std::string(""))));
}

TEST(SetElementStringTest, EmptyString) {
    SetElement<std::string> fh(std::vector<std::string>{""});
    EXPECT_TRUE(fh.In(MakeVT(std::string(""))));
    EXPECT_FALSE(fh.In(MakeVT(std::string("x"))));
}

// Strings that share first 8 bytes but differ later — stress CheapStringHash.
TEST(SetElementStringTest, SharedPrefixCollision) {
    std::string prefix(8, 'A');  // exactly 8 bytes shared
    std::vector<std::string> vals;
    for (int i = 0; i < 100; ++i) {
        vals.push_back(prefix + std::to_string(i));
    }
    SetElement<std::string> fh(vals);
    for (auto& v : vals) {
        EXPECT_TRUE(fh.In(MakeVT(v))) << "should find: " << v;
    }
    EXPECT_FALSE(fh.In(MakeVT(prefix + "999")));
    EXPECT_FALSE(fh.In(MakeVT(prefix)));
}

// Duplicates in input — Size() should deduplicate.
TEST(SetElementStringTest, Duplicates) {
    SetElement<std::string> fh(
        std::vector<std::string>{"dup", "dup", "dup", "other"});
    EXPECT_EQ(fh.Size(), 2);
    EXPECT_TRUE(fh.In(MakeVT(std::string("dup"))));
    EXPECT_TRUE(fh.In(MakeVT(std::string("other"))));
}

// ═══════════════════════════════════════════════════════════════════════════
// GetElementValues helper
// ═══════════════════════════════════════════════════════════════════════════

TEST(GetElementValuesTest, FromSimdBatch) {
    auto elem = std::make_shared<SimdBatchElement<int64_t>>(
        std::vector<int64_t>{10, 20, 30});
    auto result =
        GetElementValues<int64_t>(std::static_pointer_cast<MultiElement>(elem));
    EXPECT_EQ(result, (std::vector<int64_t>{10, 20, 30}));
}

TEST(GetElementValuesTest, FromSetElement) {
    auto elem = std::make_shared<SetElement<int32_t>>(
        std::vector<int32_t>{100, 200, 300});
    auto result =
        GetElementValues<int32_t>(std::static_pointer_cast<MultiElement>(elem));
    std::sort(result.begin(), result.end());
    EXPECT_EQ(result, (std::vector<int32_t>{100, 200, 300}));
}

TEST(GetElementValuesTest, FromFixedHashStr) {
    auto elem = std::make_shared<SetElement<std::string>>(
        std::vector<std::string>{"x", "y"});
    auto result = GetElementValues<std::string>(
        std::static_pointer_cast<MultiElement>(elem));
    std::sort(result.begin(), result.end());
    EXPECT_EQ(result, (std::vector<std::string>{"x", "y"}));
}

TEST(GetElementValuesTest, FromSortVectorElement) {
    auto elem = std::make_shared<SortVectorElement<int64_t>>(
        std::vector<int64_t>{30, 10, 20});
    auto result =
        GetElementValues<int64_t>(std::static_pointer_cast<MultiElement>(elem));
    // SortVectorElement sorts on construction
    EXPECT_EQ(result, (std::vector<int64_t>{10, 20, 30}));
}

TEST(GetElementValuesTest, UnknownTypeReturnsEmpty) {
    // FlatVectorElement is not handled by GetElementValues — should return {}
    auto elem = std::make_shared<FlatVectorElement<int32_t>>(
        std::vector<int32_t>{1, 2, 3});
    auto result =
        GetElementValues<int32_t>(std::static_pointer_cast<MultiElement>(elem));
    EXPECT_TRUE(result.empty());
}

// ═══════════════════════════════════════════════════════════════════════════
// Cross-validation: new element types must agree with SetElement
// ═══════════════════════════════════════════════════════════════════════════

TEST(CrossValidationTest, Int8SimdVsSet) {
    std::vector<int8_t> vals = {-128, -5, 0, 5, 10, 127};
    SimdBatchElement<int8_t> sb(vals);
    SetElement<int8_t> se(vals);
    for (int v = -128; v <= 127; ++v) {
        EXPECT_EQ(sb.In(MakeVT(int8_t(v))), se.In(MakeVT(int8_t(v))))
            << "Mismatch for " << v;
    }
}

TEST(CrossValidationTest, Int16SimdVsSet) {
    std::vector<int16_t> vals = {-32768, -100, 0, 100, 256, 32767};
    SimdBatchElement<int16_t> sb(vals);
    SetElement<int16_t> se(vals);
    for (int v = -500; v <= 500; ++v) {
        EXPECT_EQ(sb.In(MakeVT(int16_t(v))), se.In(MakeVT(int16_t(v))))
            << "Mismatch for " << v;
    }
}

TEST(CrossValidationTest, Int32SimdVsSet) {
    std::vector<int32_t> vals = {-100, -1, 0, 1, 42, 1000, 999999};
    SimdBatchElement<int32_t> sb(vals);
    SetElement<int32_t> se(vals);
    std::vector<int32_t> probes = {-101,
                                   -100,
                                   -99,
                                   -2,
                                   -1,
                                   0,
                                   1,
                                   2,
                                   41,
                                   42,
                                   43,
                                   999,
                                   1000,
                                   1001,
                                   999998,
                                   999999,
                                   1000000};
    for (auto v : probes) {
        EXPECT_EQ(sb.In(MakeVT(v)), se.In(MakeVT(v))) << "Mismatch for " << v;
    }
}

TEST(CrossValidationTest, Int64SimdVsSet) {
    std::vector<int64_t> vals = {-(1LL << 40), -1, 0, 1, 1LL << 40};
    SimdBatchElement<int64_t> sb(vals);
    SetElement<int64_t> se(vals);
    std::vector<int64_t> probes = {
        -2, -1, 0, 1, 2, 1LL << 40, (1LL << 40) + 1, -(1LL << 40)};
    for (auto v : probes) {
        EXPECT_EQ(sb.In(MakeVT(v)), se.In(MakeVT(v))) << "Mismatch for " << v;
    }
}

TEST(CrossValidationTest, FloatSimdVsSet) {
    std::vector<float> vals = {-1.5f, 0.0f, 1.5f, 1e10f};
    SimdBatchElement<float> sb(vals);
    SetElement<float> se(vals);
    std::vector<float> probes = {0.0f, -0.0f, 1.5f, 1.6f, -1.5f, 1e10f, 1e9f};
    for (auto v : probes) {
        EXPECT_EQ(sb.In(MakeVT(v)), se.In(MakeVT(v))) << "Mismatch for " << v;
    }
}

TEST(CrossValidationTest, DoubleSimdVsSet) {
    std::vector<double> vals = {-1.5, 0.0, 1e-100, 1.5, 1e100};
    SimdBatchElement<double> sb(vals);
    SetElement<double> se(vals);
    std::vector<double> probes = {
        0.0, -0.0, 1.5, 1.6, -1.5, 1e100, 1e99, 1e-100, 1e-99};
    for (auto v : probes) {
        EXPECT_EQ(sb.In(MakeVT(v)), se.In(MakeVT(v))) << "Mismatch for " << v;
    }
}

TEST(CrossValidationTest, StringFixedHashVsSet) {
    std::vector<std::string> vals = {"foo", "bar", std::string(500, 'x')};
    SetElement<std::string> fh(vals);
    SetElement<std::string> se(vals);
    std::vector<std::string> probes = {"foo",
                                       "bar",
                                       "baz",
                                       "",
                                       "fo",
                                       "fooo",
                                       std::string(500, 'x'),
                                       std::string(500, 'y'),
                                       std::string(499, 'x'),
                                       std::string(501, 'x')};
    for (auto& v : probes) {
        EXPECT_EQ(fh.In(MakeVT(v)), se.In(MakeVT(v)))
            << "Mismatch for '" << v.substr(0, 20) << "'";
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// FilterChunk vs In() cross-validation for all numeric types
//
// Both paths (SIMD batch and per-row linear scan) must produce identical
// results. This catches mask extraction bugs, broadcast errors, and
// scalar-tail mismatches.
// ═══════════════════════════════════════════════════════════════════════════

template <typename T>
void
FilterChunkVsIn(const std::vector<T>& in_vals,
                const std::vector<T>& data,
                const std::string& label) {
    auto sorted = in_vals;
    std::sort(sorted.begin(), sorted.end());
    sorted.erase(std::unique(sorted.begin(), sorted.end()), sorted.end());
    SimdBatchElement<T> sb(sorted);
    const int n = static_cast<int>(data.size());

    milvus::TargetBitmap bm_chunk(n, false);
    milvus::TargetBitmap bm_in(n, false);
    milvus::TargetBitmapView view_chunk(bm_chunk.data(), n);
    milvus::TargetBitmapView view_in(bm_in.data(), n);

    sb.FilterChunk(data.data(), n, view_chunk);
    for (int i = 0; i < n; ++i) {
        view_in[i] = sb.In(MakeVT(data[i]));
    }

    for (int i = 0; i < n; ++i) {
        EXPECT_EQ(view_chunk[i], view_in[i])
            << label << " FilterChunk vs In mismatch at " << i;
    }
}

TEST(FilterChunkVsInTest, Int8) {
    std::vector<int8_t> data(256);
    for (int i = 0; i < 256; ++i) data[i] = static_cast<int8_t>(i - 128);
    FilterChunkVsIn<int8_t>({-128, -1, 0, 1, 127}, data, "Int8");
}

TEST(FilterChunkVsInTest, Int16) {
    std::vector<int16_t> data(512);
    for (int i = 0; i < 512; ++i) data[i] = static_cast<int16_t>(i - 256);
    FilterChunkVsIn<int16_t>({-256, 0, 255, -32768, 32767}, data, "Int16");
}

TEST(FilterChunkVsInTest, Int32) {
    std::vector<int32_t> data(1024);
    std::iota(data.begin(), data.end(), -512);
    FilterChunkVsIn<int32_t>({-512, 0, 42, 511}, data, "Int32");
}

TEST(FilterChunkVsInTest, Int64) {
    std::vector<int64_t> data(256);
    std::iota(data.begin(), data.end(), -128);
    FilterChunkVsIn<int64_t>({-128, 0, 42, 127}, data, "Int64");
}

TEST(FilterChunkVsInTest, Float) {
    std::vector<float> data(256);
    for (int i = 0; i < 256; ++i) data[i] = i * 0.25f;
    FilterChunkVsIn<float>({0.0f, 1.0f, 10.25f, 63.75f}, data, "Float");
}

TEST(FilterChunkVsInTest, Double) {
    std::vector<double> data(128);
    for (int i = 0; i < 128; ++i) data[i] = i * 0.1;
    FilterChunkVsIn<double>({0.0, 1.0, 5.0, 12.7}, data, "Double");
}

// ═══════════════════════════════════════════════════════════════════════════
// SortVectorElement tests
// Production usage: JsonContainsExpr creates SortVectorElement for
// JSON_CONTAINS / JSON_CONTAINS_ALL / JSON_CONTAINS_ANY queries.
// Always constructed with full vector (constructor sorts automatically).
// ═══════════════════════════════════════════════════════════════════════════

TEST(SortVectorElementTest, Basic) {
    SortVectorElement<int32_t> sv(std::vector<int32_t>{5, 3, 1, 4, 2});
    EXPECT_EQ(sv.Size(), 5);
    for (int v : {1, 2, 3, 4, 5}) {
        EXPECT_TRUE(sv.In(MakeVT(int32_t(v))));
    }
    EXPECT_FALSE(sv.In(MakeVT(int32_t(0))));
    EXPECT_FALSE(sv.In(MakeVT(int32_t(6))));
}

TEST(SortVectorElementTest, StringWithStringView) {
    SortVectorElement<std::string> sv(
        std::vector<std::string>{"cherry", "apple", "banana"});
    EXPECT_TRUE(sv.In(MakeVT(std::string("apple"))));
    EXPECT_TRUE(sv.In(MakeVT(std::string_view("banana"))));
    EXPECT_FALSE(sv.In(MakeVT(std::string("grape"))));
}

// ═══════════════════════════════════════════════════════════════════════════
// FlatVectorElement tests
// Production usage: TermExpr index path creates FlatVectorElement with
// IndexInnerType or uint8_t (for bool). Only In() and direct values_ access
// are used. GetElements()/Size() are part of MultiElement interface but
// not called directly in production.
// ═══════════════════════════════════════════════════════════════════════════

TEST(FlatVectorElementTest, Basic) {
    FlatVectorElement<int64_t> fv(std::vector<int64_t>{10, 20, 30});
    EXPECT_TRUE(fv.In(MakeVT(int64_t(10))));
    EXPECT_TRUE(fv.In(MakeVT(int64_t(30))));
    EXPECT_FALSE(fv.In(MakeVT(int64_t(15))));
}

TEST(FlatVectorElementTest, DirectValuesAccess) {
    // Production code accesses values_ directly (TermExpr.cpp:938,971)
    FlatVectorElement<int32_t> fv(std::vector<int32_t>{10, 20, 30});
    EXPECT_EQ(fv.values_.size(), 3);
    EXPECT_EQ(fv.values_[0], 10);
}

// SetElement<bool> uses a specialized two-flag implementation (has_true_/
// has_false_) to avoid ankerl::unordered_dense::set<bool> whose wyhash
// reads 8 bytes from a 1-byte bool, causing ASAN stack-use-after-scope.

TEST(SetElementBoolTest, GenericHashSet) {
    SetElement<bool> se(std::vector<bool>{true, false});
    EXPECT_TRUE(se.In(MakeVT(true)));
    EXPECT_TRUE(se.In(MakeVT(false)));
    EXPECT_EQ(se.Size(), 2);
}

TEST(SetElementBoolTest, GenericHashSetSingleTrue) {
    SetElement<bool> se(std::vector<bool>{true});
    EXPECT_TRUE(se.In(MakeVT(true)));
    EXPECT_FALSE(se.In(MakeVT(false)));
}

TEST(SetElementBoolTest, GenericHashSetEmpty) {
    SetElement<bool> se(std::vector<bool>{});
    EXPECT_TRUE(se.Empty());
}

// Test the GenericValue constructor — this is the path used by
// JsonContainsExpr::ExecJsonContains<bool> (the e2e crash path).
TEST(SetElementBoolTest, FromGenericValue) {
    using GV = milvus::proto::plan::GenericValue;
    GV gv_true, gv_false;
    gv_true.set_bool_val(true);
    gv_false.set_bool_val(false);

    SetElement<bool> se(std::vector<GV>{gv_true, gv_false});
    EXPECT_TRUE(se.In(MakeVT(true)));
    EXPECT_TRUE(se.In(MakeVT(false)));
    EXPECT_EQ(se.Size(), 2);
}

TEST(SetElementBoolTest, FromGenericValueSingleTrue) {
    using GV = milvus::proto::plan::GenericValue;
    GV gv_true;
    gv_true.set_bool_val(true);

    SetElement<bool> se(std::vector<GV>{gv_true});
    EXPECT_TRUE(se.In(MakeVT(true)));
    EXPECT_FALSE(se.In(MakeVT(false)));
    EXPECT_EQ(se.Size(), 1);
}

// ═══════════════════════════════════════════════════════════════════════════
// SingleElement tests
// ═══════════════════════════════════════════════════════════════════════════

TEST(SingleElementTest, SetAndGetInt64) {
    SingleElement se;
    se.SetValue<int64_t>(int64_t(42));
    EXPECT_EQ(se.GetValue<int64_t>(), 42);
}

TEST(SingleElementTest, SetAndGetString) {
    SingleElement se;
    se.SetValue<std::string>(std::string("hello"));
    EXPECT_EQ(se.GetValue<std::string>(), "hello");
}

TEST(SingleElementTest, SetAndGetDouble) {
    SingleElement se;
    se.SetValue<double>(3.14);
    EXPECT_DOUBLE_EQ(se.GetValue<double>(), 3.14);
}

// SetElement<bool> specialization uses has_true_/has_false_ flags.
// GetElements() returns [true] and/or [false] based on flags.

// ═══════════════════════════════════════════════════════════════════════════
// P1: SimdBatchElement dedup — duplicates in IN list should be removed
// ═══════════════════════════════════════════════════════════════════════════

TEST(FilterChunkTest, DeduplicatedFilterChunk) {
    // Go rewriter pre-sorts and deduplicates; test with sorted unique vals
    std::vector<int32_t> vals = {7, 42};
    std::vector<int32_t> data(128);
    std::iota(data.begin(), data.end(), 0);
    VerifyFilterChunk(vals, data, "DeduplicatedFilterChunk");
}

// ═══════════════════════════════════════════════════════════════════════════
// P1: FilterChunk OR-only semantic — only sets bits, never clears
//
// FilterChunk assumes the bitmap is zero-initialized and only sets bits
// to true. If bits are already set, they must be preserved (OR semantic).
// ═══════════════════════════════════════════════════════════════════════════

TEST(FilterChunkTest, PreservesExistingTrueBits) {
    std::vector<int32_t> in_vals = {42};
    std::vector<int32_t> data = {1, 2, 3, 42, 5, 6, 7, 8};
    const int n = static_cast<int>(data.size());

    milvus::TargetBitmap bm(n, false);
    milvus::TargetBitmapView view(bm.data(), n);

    // Pre-set some bits that are NOT in the IN list
    view[0] = true;  // data[0]=1, not in IN list
    view[2] = true;  // data[2]=3, not in IN list

    SimdBatchElement<int32_t> sb(in_vals);
    sb.FilterChunk(data.data(), n, view);

    // Pre-set bits should be preserved (OR semantic)
    EXPECT_TRUE(view[0]) << "pre-set bit at index 0 should be preserved";
    EXPECT_TRUE(view[2]) << "pre-set bit at index 2 should be preserved";
    // Matched value should be set
    EXPECT_TRUE(view[3]) << "data[3]=42 should match";
    // Unmatched, non-pre-set should remain false
    EXPECT_FALSE(view[1]) << "data[1]=2 should not match";
    EXPECT_FALSE(view[4]) << "data[4]=5 should not match";
}

TEST(FilterChunkTest, PreservesExistingTrueBitsFloat) {
    std::vector<float> in_vals = {1.0f};
    std::vector<float> data = {0.0f, 1.0f, 2.0f, 3.0f};
    const int n = 4;

    milvus::TargetBitmap bm(n, false);
    milvus::TargetBitmapView view(bm.data(), n);
    view[2] = true;  // pre-set, not a match

    SimdBatchElement<float> sb(in_vals);
    sb.FilterChunk(data.data(), n, view);

    EXPECT_FALSE(view[0]);
    EXPECT_TRUE(view[1]);  // matched
    EXPECT_TRUE(view[2]);  // preserved
    EXPECT_FALSE(view[3]);
}

// ═══════════════════════════════════════════════════════════════════════════
// P1: Adversarial string prefix test — CheapStringHash correctness
//
// 1000 strings with identical first 8 bytes. After removing is_avalanching,
// ankerl's mixer produces distinct fingerprints, preventing O(N) probe.
// This test validates correctness (not performance) under collision.
// ═══════════════════════════════════════════════════════════════════════════

TEST(SetElementStringTest, AdversarialPrefixLarge) {
    // 1000 strings with identical first 8 bytes and same length
    std::string prefix(8, 'X');
    std::vector<std::string> vals;
    for (int i = 0; i < 1000; ++i) {
        // Pad to same length (20 chars) to make CheapStringHash
        // produce identical hash values for all (prefix + length match)
        std::string s = prefix + std::to_string(10000 + i);
        vals.push_back(s);
    }

    SetElement<std::string> fh(vals);
    EXPECT_EQ(fh.Size(), 1000);

    // All inserted values must be found
    for (auto& v : vals) {
        EXPECT_TRUE(fh.In(MakeVT(v))) << "should find: " << v;
    }

    // Values NOT in the set must not be found (even with same prefix)
    for (int i = 1000; i < 1100; ++i) {
        std::string s = prefix + std::to_string(10000 + i);
        EXPECT_FALSE(fh.In(MakeVT(s))) << "should not find: " << s;
    }
}

TEST(SetElementStringTest, AdversarialSameHashSameLength) {
    // All strings have identical first 8 bytes AND identical length.
    // CheapStringHash produces exactly the same hash for all of them.
    // Without the ankerl mixer (is_avalanching removed), the fingerprints
    // should still differentiate thanks to wyhash mixing.
    constexpr int kCount = 500;
    constexpr int kStrLen = 16;
    std::string base(kStrLen, 'Z');

    std::vector<std::string> vals;
    for (int i = 0; i < kCount; ++i) {
        std::string s = base;
        // Encode i into bytes 8-11 (beyond the 8-byte hash window)
        auto suffix = std::to_string(i);
        for (size_t j = 0; j < suffix.size() && 8 + j < s.size(); ++j) {
            s[8 + j] = suffix[j];
        }
        vals.push_back(s);
    }

    SetElement<std::string> fh(vals);
    EXPECT_EQ(fh.Size(), kCount);

    // Cross-validate against SetElement<string> (known-correct, uses wyhash)
    SetElement<std::string> se(vals);
    for (auto& v : vals) {
        EXPECT_TRUE(fh.In(MakeVT(v))) << "FixedHash should find: " << v;
        EXPECT_TRUE(se.In(MakeVT(v))) << "SetElement should find: " << v;
    }

    // Probe with non-existent same-prefix strings
    for (int i = kCount; i < kCount + 100; ++i) {
        std::string s = base;
        auto suffix = std::to_string(i);
        for (size_t j = 0; j < suffix.size() && 8 + j < s.size(); ++j) {
            s[8 + j] = suffix[j];
        }
        EXPECT_EQ(fh.In(MakeVT(s)), se.In(MakeVT(s)))
            << "Disagreement for: " << s;
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Type boundary value tests — INT_MIN, INT_MAX, FLT_MAX, denormals, -0.0
// ═══════════════════════════════════════════════════════════════════════════

TEST(SimdBatchElementTest, Int8Boundaries) {
    std::vector<int8_t> vals = {INT8_MIN, -1, 0, 1, INT8_MAX};
    SimdBatchElement<int8_t> sb(vals);
    for (auto v : vals) {
        EXPECT_TRUE(sb.In(MakeVT(v))) << "Missing: " << (int)v;
    }
    EXPECT_FALSE(sb.In(MakeVT(int8_t(2))));
}

TEST(SimdBatchElementTest, Int16Boundaries) {
    std::vector<int16_t> vals = {INT16_MIN, -1, 0, 1, INT16_MAX};
    SimdBatchElement<int16_t> sb(vals);
    for (auto v : vals) {
        EXPECT_TRUE(sb.In(MakeVT(v))) << "Missing: " << v;
    }
    EXPECT_FALSE(sb.In(MakeVT(int16_t(2))));
}

TEST(SimdBatchElementTest, Int32Boundaries) {
    std::vector<int32_t> vals = {INT32_MIN, -1, 0, 1, INT32_MAX};
    SimdBatchElement<int32_t> sb(vals);
    for (auto v : vals) {
        EXPECT_TRUE(sb.In(MakeVT(v))) << "Missing: " << v;
    }
    EXPECT_FALSE(sb.In(MakeVT(int32_t(2))));
}

TEST(SimdBatchElementTest, Int64Boundaries) {
    std::vector<int64_t> vals = {INT64_MIN, -1, 0, 1, INT64_MAX};
    SimdBatchElement<int64_t> sb(vals);
    for (auto v : vals) {
        EXPECT_TRUE(sb.In(MakeVT(v))) << "Missing: " << v;
    }
    EXPECT_FALSE(sb.In(MakeVT(int64_t(2))));
}

TEST(SimdBatchElementTest, FloatSpecialValues) {
    // Pre-sorted; -0.0 and 0.0 compare equal in IEEE 754
    std::vector<float> vals = {
        std::numeric_limits<float>::lowest(),
        0.0f,
        std::numeric_limits<float>::denorm_min(),  // smallest positive denormal
        std::numeric_limits<float>::min(),         // smallest positive normal
        std::numeric_limits<float>::max(),
    };
    SimdBatchElement<float> sb(vals);
    EXPECT_TRUE(sb.In(MakeVT(0.0f)));
    EXPECT_TRUE(sb.In(MakeVT(-0.0f)));  // -0.0 == 0.0
    EXPECT_TRUE(sb.In(MakeVT(std::numeric_limits<float>::max())));
    EXPECT_TRUE(sb.In(MakeVT(std::numeric_limits<float>::lowest())));
    EXPECT_TRUE(sb.In(MakeVT(std::numeric_limits<float>::min())));
    EXPECT_TRUE(sb.In(MakeVT(std::numeric_limits<float>::denorm_min())));
    EXPECT_FALSE(sb.In(MakeVT(1.0f)));
}

TEST(SimdBatchElementTest, DoubleSpecialValues) {
    // Pre-sorted; -0.0 and 0.0 compare equal in IEEE 754
    std::vector<double> vals = {std::numeric_limits<double>::lowest(),
                                0.0,
                                std::numeric_limits<double>::denorm_min(),
                                std::numeric_limits<double>::min(),
                                std::numeric_limits<double>::max()};
    SimdBatchElement<double> sb(vals);
    EXPECT_TRUE(sb.In(MakeVT(0.0)));
    EXPECT_TRUE(sb.In(MakeVT(-0.0)));
    EXPECT_TRUE(sb.In(MakeVT(std::numeric_limits<double>::max())));
    EXPECT_TRUE(sb.In(MakeVT(std::numeric_limits<double>::lowest())));
    EXPECT_FALSE(sb.In(MakeVT(1.0)));
}

// Boundary values through FilterChunk
TEST(FilterChunkTest, Int32BoundaryValues) {
    std::vector<int32_t> in_vals = {INT32_MIN, 0, INT32_MAX};
    std::vector<int32_t> data = {
        INT32_MIN, INT32_MIN + 1, -1, 0, 1, INT32_MAX - 1, INT32_MAX};
    VerifyFilterChunk(in_vals, data, "Int32Boundary");
}

TEST(FilterChunkTest, FloatBoundaryValues) {
    float fmax = std::numeric_limits<float>::max();
    float fmin = std::numeric_limits<float>::lowest();
    std::vector<float> in_vals = {fmin, -0.0f, 0.0f, fmax};
    std::vector<float> data = {fmax, fmin, 0.0f, -0.0f, 1.0f, -1.0f};
    VerifyFilterChunk(in_vals, data, "FloatBoundary");
}

// ═══════════════════════════════════════════════════════════════════════════
// FilterChunk edge cases
// ═══════════════════════════════════════════════════════════════════════════

TEST(FilterChunkTest, SizeZero) {
    std::vector<int32_t> in_vals = {1, 2, 3};
    SimdBatchElement<int32_t> sb(in_vals);
    milvus::TargetBitmap bm(0, false);
    milvus::TargetBitmapView view(bm.data(), 0);
    // Should not crash with size=0
    sb.FilterChunk(static_cast<const int32_t*>(nullptr), 0, view);
}

TEST(FilterChunkTest, SizeOne) {
    std::vector<int32_t> in_vals = {42};
    std::vector<int32_t> data = {42};
    VerifyFilterChunk(in_vals, data, "SizeOne");
}

TEST(FilterChunkTest, AllSameDataAllMatch) {
    // All data elements are the same value and it's in IN list
    std::vector<int32_t> in_vals = {7, 8, 9};
    std::vector<int32_t> data(256, 7);
    VerifyFilterChunk(in_vals, data, "AllSameDataAllMatch");
}

TEST(FilterChunkTest, AllSameDataNoMatch) {
    std::vector<int32_t> in_vals = {1, 2, 3};
    std::vector<int32_t> data(256, 99);
    VerifyFilterChunk(in_vals, data, "AllSameDataNoMatch");
}

TEST(FilterChunkTest, LargeInListNearThreshold) {
    // IN list size = 63 (just below kSimdThreshold=64), still uses SIMD path
    constexpr int kInSize = 63;
    std::vector<int32_t> in_vals(kInSize);
    for (int i = 0; i < kInSize; ++i) {
        in_vals[i] = i * 3;  // 0, 3, 6, 9, ...
    }
    std::vector<int32_t> data(2048);
    std::iota(data.begin(), data.end(), 0);
    VerifyFilterChunk(in_vals, data, "LargeInListNearThreshold");
}

TEST(FilterChunkTest, SingleINValueAllTypes) {
    // Regression: single-element IN list across all types
    {
        std::vector<int8_t> data = {0, 1, 2, 3, 4, 5};
        VerifyFilterChunk<int8_t>({int8_t(3)}, data, "SingleIN_int8");
    }
    {
        std::vector<int16_t> data = {0, 1, 2, 3, 4, 5};
        VerifyFilterChunk<int16_t>({int16_t(3)}, data, "SingleIN_int16");
    }
    {
        std::vector<double> data = {0.0, 1.0, 2.0, 3.0};
        VerifyFilterChunk<double>({3.0}, data, "SingleIN_double");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Wrong-type variant — In() should return false for type mismatch
// ═══════════════════════════════════════════════════════════════════════════

// SimdBatchElement does not check variant type — production code
// guarantees correct type via std::in_place_type<T>.

TEST(SetElementTest, WrongTypeVariant) {
    SetElement<int64_t> se(std::vector<int64_t>{42});
    EXPECT_TRUE(se.In(MakeVT(int64_t(42))));
    EXPECT_FALSE(se.In(MakeVT(int32_t(42))));
    EXPECT_FALSE(se.In(MakeVT(float(42.0f))));
    EXPECT_FALSE(se.In(MakeVT(std::monostate{})));
}

TEST(SortVectorElementTest, WrongTypeVariant) {
    SortVectorElement<int32_t> sv(std::vector<int32_t>{10});
    EXPECT_TRUE(sv.In(MakeVT(int32_t(10))));
    EXPECT_FALSE(sv.In(MakeVT(int64_t(10))));
    EXPECT_FALSE(sv.In(MakeVT(std::monostate{})));
}

TEST(FlatVectorElementTest, WrongTypeVariant) {
    FlatVectorElement<int64_t> fv(std::vector<int64_t>{5});
    EXPECT_TRUE(fv.In(MakeVT(int64_t(5))));
    EXPECT_FALSE(fv.In(MakeVT(int32_t(5))));
    EXPECT_FALSE(fv.In(MakeVT(std::monostate{})));
}

// Production uses FlatVectorElement<uint8_t> for bool index path
TEST(FlatVectorElementTest, Uint8ForBoolIndex) {
    FlatVectorElement<uint8_t> fv(std::vector<uint8_t>{1, 0});
    EXPECT_TRUE(fv.In(MakeVT(uint8_t(1))));
    EXPECT_TRUE(fv.In(MakeVT(uint8_t(0))));
    EXPECT_FALSE(fv.In(MakeVT(uint8_t(2))));
    EXPECT_EQ(fv.values_.size(), 2);
    EXPECT_EQ(fv.values_[0], 1);
    EXPECT_EQ(fv.values_[1], 0);
}

TEST(SetElementStringTest, WrongTypeVariant) {
    SetElement<std::string> fh(std::vector<std::string>{"hello"});
    EXPECT_TRUE(fh.In(MakeVT(std::string("hello"))));
    EXPECT_TRUE(fh.In(MakeVT(std::string_view("hello"))));
    EXPECT_FALSE(fh.In(MakeVT(int32_t(0))));
    EXPECT_FALSE(fh.In(MakeVT(bool(true))));
    EXPECT_FALSE(fh.In(MakeVT(std::monostate{})));
}

// ═══════════════════════════════════════════════════════════════════════════
// SetElement<string> — direct tests with string_view heterogeneous lookup
// ═══════════════════════════════════════════════════════════════════════════

TEST(SetElementTest, StringWithStringView) {
    SetElement<std::string> se(
        std::vector<std::string>{"alpha", "beta", "gamma"});
    EXPECT_TRUE(se.In(MakeVT(std::string("alpha"))));
    EXPECT_TRUE(se.In(MakeVT(std::string_view("beta"))));
    EXPECT_FALSE(se.In(MakeVT(std::string("delta"))));
    EXPECT_FALSE(se.In(MakeVT(std::string_view("ALPHA"))));
}

TEST(SetElementTest, DeduplicatesOnInsert) {
    SetElement<int32_t> se(std::vector<int32_t>{1, 2, 2, 3, 3, 3});
    EXPECT_EQ(se.Size(), 3);
}

TEST(SetElementTest, StringDeduplicates) {
    SetElement<std::string> se(std::vector<std::string>{"dup", "dup", "other"});
    EXPECT_EQ(se.Size(), 2);
}

TEST(SetElementTest, EmptyAndSize) {
    SetElement<int32_t> empty_se(std::vector<int32_t>{});
    EXPECT_TRUE(empty_se.Empty());
    EXPECT_EQ(empty_se.Size(), 0);

    SetElement<int32_t> non_empty(std::vector<int32_t>{1});
    EXPECT_FALSE(non_empty.Empty());
    EXPECT_EQ(non_empty.Size(), 1);
}

TEST(SetElementTest, GetElementsInt) {
    SetElement<int32_t> se(std::vector<int32_t>{3, 1, 2});
    auto elems = se.GetElements();
    std::sort(elems.begin(), elems.end());
    EXPECT_EQ(elems, (std::vector<int32_t>{1, 2, 3}));
}

TEST(SetElementTest, GetElementsString) {
    SetElement<std::string> se(std::vector<std::string>{"z", "a", "m"});
    auto elems = se.GetElements();
    std::sort(elems.begin(), elems.end());
    EXPECT_EQ(elems, (std::vector<std::string>{"a", "m", "z"}));
}

// Generic hash set deduplicates on insert.
TEST(SetElementBoolTest, GenericDeduplicates) {
    SetElement<bool> se(std::vector<bool>{true, true, true});
    EXPECT_EQ(se.Size(), 1);
    EXPECT_TRUE(se.In(MakeVT(true)));
    EXPECT_FALSE(se.In(MakeVT(false)));
}

// ═══════════════════════════════════════════════════════════════════════════
// SortVectorElement — via constructor (production path)
// Production: JsonContainsExpr always constructs with full vector
// ═══════════════════════════════════════════════════════════════════════════

TEST(SortVectorElementTest, EmptyAndSize) {
    SortVectorElement<int32_t> sv(std::vector<int32_t>{});
    EXPECT_TRUE(sv.Empty());
    EXPECT_EQ(sv.Size(), 0);
}

TEST(SortVectorElementTest, LargeList) {
    std::vector<int32_t> vals(1000);
    for (int i = 0; i < 1000; ++i) vals[i] = 1000 - i;  // reverse order
    SortVectorElement<int32_t> sv(vals);
    for (int v : {1, 500, 1000}) {
        EXPECT_TRUE(sv.In(MakeVT(int32_t(v))));
    }
    EXPECT_FALSE(sv.In(MakeVT(int32_t(0))));
    EXPECT_FALSE(sv.In(MakeVT(int32_t(1001))));
}

TEST(SortVectorElementTest, GetElements) {
    SortVectorElement<int32_t> sv(std::vector<int32_t>{30, 10, 20});
    auto elems = sv.GetElements();
    EXPECT_EQ(elems, (std::vector<int32_t>{10, 20, 30}));
}

// ═══════════════════════════════════════════════════════════════════════════
// SetElement<std::string> — GetElements, direct CheapStringHash boundary tests
// ═══════════════════════════════════════════════════════════════════════════

TEST(SetElementStringTest, GetElements) {
    SetElement<std::string> fh(std::vector<std::string>{"z", "a", "m"});
    auto elems = fh.GetElements();
    std::sort(elems.begin(), elems.end());
    EXPECT_EQ(elems, (std::vector<std::string>{"a", "m", "z"}));
}

TEST(SetElementStringTest, BoundaryStringLengths) {
    // Test strings of length 0, 1, 7, 8, 9 — around the 8-byte hash window
    std::vector<std::string> vals = {
        "",           // 0 bytes
        "a",          // 1 byte
        "1234567",    // 7 bytes
        "12345678",   // exactly 8 bytes
        "123456789",  // 9 bytes (1 byte beyond hash window)
    };
    SetElement<std::string> fh(vals);
    EXPECT_EQ(fh.Size(), 5);
    for (auto& v : vals) {
        EXPECT_TRUE(fh.In(MakeVT(v))) << "Missing: '" << v << "'";
    }
    // Near-misses
    EXPECT_FALSE(fh.In(MakeVT(std::string("b"))));
    EXPECT_FALSE(fh.In(MakeVT(std::string("1234567x"))));   // differ at byte 7
    EXPECT_FALSE(fh.In(MakeVT(std::string("12345679"))));   // differ at byte 8
    EXPECT_FALSE(fh.In(MakeVT(std::string("12345678x"))));  // differ beyond 8
}

TEST(SetElementStringTest, StringViewLookup) {
    SetElement<std::string> fh(std::vector<std::string>{"hello", "world"});
    EXPECT_TRUE(fh.In(MakeVT(std::string_view("hello"))));
    EXPECT_TRUE(fh.In(MakeVT(std::string_view("world"))));
    EXPECT_FALSE(fh.In(MakeVT(std::string_view("HELLO"))));
}

// ═══════════════════════════════════════════════════════════════════════════
// GetElementValues<bool> — through SetElement<bool>
// ═══════════════════════════════════════════════════════════════════════════

// GetElementValues<bool> works with the SetElement<bool> specialization.
TEST(GetElementValuesTest, FromSetElementBool) {
    auto elem =
        std::make_shared<SetElement<bool>>(std::vector<bool>{true, false});
    auto result =
        GetElementValues<bool>(std::static_pointer_cast<MultiElement>(elem));
    EXPECT_EQ(result.size(), 2);
}

TEST(GetElementValuesTest, NullptrReturnsEmpty) {
    auto result = GetElementValues<int32_t>(nullptr);
    EXPECT_TRUE(result.empty());
}

// ═══════════════════════════════════════════════════════════════════════════
// SetElement<T>::FilterChunk — batch hash lookup without variant overhead
// ═══════════════════════════════════════════════════════════════════════════

TEST(SetElementFilterChunkTest, Int32) {
    std::vector<int32_t> in_vals = {-1, 0, 1, 42, 999};
    SetElement<int32_t> se(in_vals);

    std::vector<int32_t> data = {-1, 2, 0, 42, 100, 999, -2, 1};
    const int n = static_cast<int>(data.size());
    milvus::TargetBitmap bm(n, false);
    milvus::TargetBitmapView view(bm.data(), n);
    se.FilterChunk(data.data(), n, view);

    // Expected: indices 0(-1), 2(0), 3(42), 5(999), 7(1) match
    EXPECT_TRUE(view[0]);
    EXPECT_FALSE(view[1]);
    EXPECT_TRUE(view[2]);
    EXPECT_TRUE(view[3]);
    EXPECT_FALSE(view[4]);
    EXPECT_TRUE(view[5]);
    EXPECT_FALSE(view[6]);
    EXPECT_TRUE(view[7]);
}

TEST(SetElementFilterChunkTest, Int64) {
    std::vector<int64_t> in_vals = {100, 200, 300};
    SetElement<int64_t> se(in_vals);

    std::vector<int64_t> data = {100, 101, 200, 201, 300};
    const int n = static_cast<int>(data.size());
    milvus::TargetBitmap bm(n, false);
    milvus::TargetBitmapView view(bm.data(), n);
    se.FilterChunk(data.data(), n, view);

    EXPECT_TRUE(view[0]);
    EXPECT_FALSE(view[1]);
    EXPECT_TRUE(view[2]);
    EXPECT_FALSE(view[3]);
    EXPECT_TRUE(view[4]);
}

TEST(SetElementFilterChunkTest, StringDirect) {
    std::vector<std::string> in_vals = {"hello", "world", "foo"};
    SetElement<std::string> se(in_vals);

    std::vector<std::string> data = {"hello", "bar", "world", "baz", "foo"};
    const int n = static_cast<int>(data.size());
    milvus::TargetBitmap bm(n, false);
    milvus::TargetBitmapView view(bm.data(), n);
    se.FilterChunk(data.data(), n, view);

    EXPECT_TRUE(view[0]);
    EXPECT_FALSE(view[1]);
    EXPECT_TRUE(view[2]);
    EXPECT_FALSE(view[3]);
    EXPECT_TRUE(view[4]);
}

TEST(SetElementFilterChunkTest, StringViewData) {
    std::vector<std::string> in_vals = {"alpha", "beta"};
    SetElement<std::string> se(in_vals);

    // Simulate sealed segment: data is string_view array
    std::string backing[] = {"alpha", "gamma", "beta", "delta"};
    std::vector<std::string_view> data;
    for (auto& s : backing) data.push_back(s);

    const int n = static_cast<int>(data.size());
    milvus::TargetBitmap bm(n, false);
    milvus::TargetBitmapView view(bm.data(), n);
    se.FilterChunk(data.data(), n, view);

    EXPECT_TRUE(view[0]);   // alpha
    EXPECT_FALSE(view[1]);  // gamma
    EXPECT_TRUE(view[2]);   // beta
    EXPECT_FALSE(view[3]);  // delta
}

// ═══════════════════════════════════════════════════════════════════════════
// FlatVectorElement<string> — small IN linear scan correctness
// ═══════════════════════════════════════════════════════════════════════════

TEST(FlatVectorElementTest, StringSmallIN) {
    std::vector<std::string> in_vals = {"a", "bb", "ccc"};
    FlatVectorElement<std::string> fv(in_vals);

    EXPECT_TRUE(fv.In(MakeVT(std::string("a"))));
    EXPECT_TRUE(fv.In(MakeVT(std::string("bb"))));
    EXPECT_TRUE(fv.In(MakeVT(std::string("ccc"))));
    EXPECT_FALSE(fv.In(MakeVT(std::string("d"))));
    EXPECT_FALSE(fv.In(MakeVT(std::string(""))));
}

TEST(FlatVectorElementTest, StringViewLookup) {
    std::vector<std::string> in_vals = {"hello", "world"};
    FlatVectorElement<std::string> fv(in_vals);

    // string_view lookup
    EXPECT_TRUE(fv.In(MakeVT(std::string_view("hello"))));
    EXPECT_FALSE(fv.In(MakeVT(std::string_view("other"))));
}

TEST(FlatVectorElementTest, Empty) {
    FlatVectorElement<std::string> fv(std::vector<std::string>{});
    EXPECT_TRUE(fv.Empty());
    EXPECT_FALSE(fv.In(MakeVT(std::string("x"))));
}

TEST(FlatVectorElementTest, SingleValue) {
    FlatVectorElement<std::string> fv(std::vector<std::string>{"only"});
    EXPECT_EQ(fv.Size(), 1);
    EXPECT_TRUE(fv.In(MakeVT(std::string("only"))));
    EXPECT_FALSE(fv.In(MakeVT(std::string("other"))));
}

// ═══════════════════════════════════════════════════════════════════════════
// SimdBatchElement — all-duplicates IN list
// ═══════════════════════════════════════════════════════════════════════════

// ═══════════════════════════════════════════════════════════════════════════
// SingleElement — additional type coverage
// ═══════════════════════════════════════════════════════════════════════════

TEST(SingleElementTest, SetAndGetInt8) {
    SingleElement se;
    se.SetValue<int8_t>(int8_t(-128));
    EXPECT_EQ(se.GetValue<int8_t>(), -128);
}

TEST(SingleElementTest, SetAndGetFloat) {
    SingleElement se;
    se.SetValue<float>(1.5f);
    EXPECT_FLOAT_EQ(se.GetValue<float>(), 1.5f);
}

TEST(SingleElementTest, SetAndGetBool) {
    SingleElement se;
    se.SetValue<bool>(true);
    EXPECT_EQ(se.GetValue<bool>(), true);
}

// ═══════════════════════════════════════════════════════════════════════════
// Remainder loop coverage — data size that is NOT a multiple of kStep
// but IS a multiple of kLanes + some extra.
//
// When kUnroll > 1, the code has three loops:
//   1. Main loop:      processes kStep elements per iteration
//   2. Remainder loop: processes kLanes elements per iteration
//   3. Scalar tail:    processes 1 element per iteration
//
// These tests ensure the remainder loop is exercised for every type.
// ═══════════════════════════════════════════════════════════════════════════

// Helper: compute kStep for a given type (same logic as FilterChunk)
template <typename T>
constexpr int
GetKStep() {
    constexpr int kLanes = xsimd::batch<T>::size;
    constexpr int kMaxUnroll = 8;
    constexpr int kIdealUnroll = (kLanes >= 64) ? 1 : 64 / kLanes;
    constexpr int kUnroll =
        (kIdealUnroll <= kMaxUnroll) ? kIdealUnroll : kMaxUnroll;
    return kLanes * kUnroll;
}

template <typename T>
void
VerifyRemainderLoop(const std::string& label) {
    constexpr int kLanes = xsimd::batch<T>::size;
    constexpr int kStep = GetKStep<T>();

    // Data size = kStep + kLanes + 3 (exercises main + remainder + scalar)
    const int data_size = kStep + kLanes + 3;
    std::mt19937 rng(42);

    std::vector<T> data(data_size);
    if constexpr (std::is_floating_point_v<T>) {
        for (int i = 0; i < data_size; ++i) data[i] = static_cast<T>(i);
    } else {
        for (int i = 0; i < data_size; ++i)
            data[i] = static_cast<T>(i % 256 - 128);
    }

    // IN values that hit across all three loop sections
    std::vector<T> in_vals;
    in_vals.push_back(data[0]);               // in main loop
    in_vals.push_back(data[kStep]);           // in remainder loop
    in_vals.push_back(data[kStep + kLanes]);  // in scalar tail
    in_vals.push_back(data[data_size - 1]);   // last element (scalar tail)

    VerifyFilterChunk(in_vals, data, label + " remainder");

    // Also test with larger IN list to stress remainder with more comparisons
    std::vector<T> big_in;
    for (int i = 0; i < 30; ++i) {
        big_in.push_back(data[i * data_size / 30]);
    }
    VerifyFilterChunk(big_in, data, label + " remainder_big_IN");

    // Test data size = kStep * 2 + kLanes * (kUnroll - 1) + kLanes/2
    // This maximizes remainder iterations: kUnroll - 1 remainder steps.
    constexpr int kUnroll = kStep / kLanes;
    if constexpr (kUnroll > 1) {
        const int max_remainder_size =
            kStep * 2 + kLanes * (kUnroll - 1) + kLanes / 2;
        std::vector<T> data2(max_remainder_size);
        for (int i = 0; i < max_remainder_size; ++i) {
            if constexpr (std::is_floating_point_v<T>)
                data2[i] = static_cast<T>(i);
            else
                data2[i] = static_cast<T>(i % 256 - 128);
        }
        std::vector<T> in2 = {data2[0],
                              data2[kStep * 2],
                              data2[kStep * 2 + kLanes],
                              data2[max_remainder_size - 1]};
        VerifyFilterChunk(in2, data2, label + " max_remainder");
    }
}

TEST(FilterChunkTest, RemainderLoop_Int8) {
    VerifyRemainderLoop<int8_t>("int8");
}

TEST(FilterChunkTest, RemainderLoop_Int16) {
    VerifyRemainderLoop<int16_t>("int16");
}

TEST(FilterChunkTest, RemainderLoop_Int32) {
    VerifyRemainderLoop<int32_t>("int32");
}

TEST(FilterChunkTest, RemainderLoop_Int64) {
    VerifyRemainderLoop<int64_t>("int64");
}

TEST(FilterChunkTest, RemainderLoop_Float) {
    VerifyRemainderLoop<float>("float");
}

TEST(FilterChunkTest, RemainderLoop_Double) {
    VerifyRemainderLoop<double>("double");
}

// ═══════════════════════════════════════════════════════════════════════════
// SIMD dispatch selection: verify SimdBatchElement vs SetElement choice
// based on IN list size relative to simdLaneCount<T>() * 8 threshold.
// ═══════════════════════════════════════════════════════════════════════════

template <typename T>
void
VerifyDispatchChoice(const std::string& label) {
    const size_t threshold =
        static_cast<size_t>(milvus::exec::simdLaneCount<T>()) * 8;

    // Below threshold → SimdBatchElement
    {
        std::vector<T> vals(threshold);
        std::iota(vals.begin(), vals.end(), T(0));
        auto ptr = std::make_shared<SimdBatchElement<T>>(vals);
        // Verify it's actually a SimdBatchElement
        ASSERT_NE(std::dynamic_pointer_cast<SimdBatchElement<T>>(ptr), nullptr)
            << label << " below threshold should be SimdBatchElement";
        // Verify FilterChunk correctness at threshold boundary
        std::vector<T> data(256);
        std::iota(data.begin(), data.end(), T(0));
        VerifyFilterChunk(vals, data, label + " at_threshold");
    }

    // Above threshold → SetElement (hash)
    {
        std::vector<T> vals(threshold + 1);
        std::iota(vals.begin(), vals.end(), T(0));
        auto ptr = std::make_shared<SetElement<T>>(vals);
        ASSERT_NE(std::dynamic_pointer_cast<SetElement<T>>(ptr), nullptr)
            << label << " above threshold should be SetElement";
    }
}

TEST(DispatchSelectionTest, Int8) {
    VerifyDispatchChoice<int8_t>("int8");
}

TEST(DispatchSelectionTest, Int16) {
    VerifyDispatchChoice<int16_t>("int16");
}

TEST(DispatchSelectionTest, Int32) {
    VerifyDispatchChoice<int32_t>("int32");
}

TEST(DispatchSelectionTest, Int64) {
    VerifyDispatchChoice<int64_t>("int64");
}

TEST(DispatchSelectionTest, Float) {
    VerifyDispatchChoice<float>("float");
}

TEST(DispatchSelectionTest, Double) {
    VerifyDispatchChoice<double>("double");
}

// ═══════════════════════════════════════════════════════════════════════════
// SetElement<string> FilterChunk with string_view — edge cases
// ═══════════════════════════════════════════════════════════════════════════

TEST(SetElementFilterChunkTest, StringViewEmptyStrings) {
    std::vector<std::string> in_vals = {""};
    SetElement<std::string> se(in_vals);

    std::string backing[] = {"", "notempty", ""};
    std::vector<std::string_view> data;
    for (auto& s : backing) data.push_back(s);

    const int n = static_cast<int>(data.size());
    milvus::TargetBitmap bm(n, false);
    milvus::TargetBitmapView view(bm.data(), n);
    se.FilterChunk(data.data(), n, view);

    EXPECT_TRUE(view[0]);
    EXPECT_FALSE(view[1]);
    EXPECT_TRUE(view[2]);
}

TEST(SetElementFilterChunkTest, StringViewAllMatch) {
    std::vector<std::string> in_vals = {"a", "b", "c"};
    SetElement<std::string> se(in_vals);

    std::string backing[] = {"a", "b", "c", "a", "b"};
    std::vector<std::string_view> data;
    for (auto& s : backing) data.push_back(s);

    const int n = static_cast<int>(data.size());
    milvus::TargetBitmap bm(n, false);
    milvus::TargetBitmapView view(bm.data(), n);
    se.FilterChunk(data.data(), n, view);

    for (int i = 0; i < n; ++i) {
        EXPECT_TRUE(view[i]) << "index " << i;
    }
}

TEST(SetElementFilterChunkTest, StringViewNoneMatch) {
    std::vector<std::string> in_vals = {"x", "y"};
    SetElement<std::string> se(in_vals);

    std::string backing[] = {"a", "b", "c"};
    std::vector<std::string_view> data;
    for (auto& s : backing) data.push_back(s);

    const int n = static_cast<int>(data.size());
    milvus::TargetBitmap bm(n, false);
    milvus::TargetBitmapView view(bm.data(), n);
    se.FilterChunk(data.data(), n, view);

    for (int i = 0; i < n; ++i) {
        EXPECT_FALSE(view[i]) << "index " << i;
    }
}

TEST(SetElementFilterChunkTest, StringViewLongStrings) {
    // Long strings to exercise hash quality beyond short-string optimization
    std::string long1(256, 'a');
    std::string long2(256, 'b');
    std::string long3(256, 'c');
    std::vector<std::string> in_vals = {long1, long3};
    SetElement<std::string> se(in_vals);

    std::string backing[] = {long1, long2, long3, long2};
    std::vector<std::string_view> data;
    for (auto& s : backing) data.push_back(s);

    const int n = static_cast<int>(data.size());
    milvus::TargetBitmap bm(n, false);
    milvus::TargetBitmapView view(bm.data(), n);
    se.FilterChunk(data.data(), n, view);

    EXPECT_TRUE(view[0]);   // long1
    EXPECT_FALSE(view[1]);  // long2
    EXPECT_TRUE(view[2]);   // long3
    EXPECT_FALSE(view[3]);  // long2
}

// ═══════════════════════════════════════════════════════════════════════════
// Numeric FilterChunk: varying IN list sizes × data sizes
// Cross-validates SimdBatchElement (SIMD) against SetElement (hash).
// ═══════════════════════════════════════════════════════════════════════════

template <typename T>
void
VerifyFilterChunkVaryingSizes(const std::string& label) {
    const std::vector<int> in_sizes = {1, 2, 5, 16, 32, 60};
    const std::vector<int> data_sizes = {1, 7, 64, 255, 1024, 4096};

    for (int in_sz : in_sizes) {
        std::vector<T> in_vals(in_sz);
        for (int j = 0; j < in_sz; ++j) {
            in_vals[j] = static_cast<T>(j * 7);  // spread out values
        }
        std::sort(in_vals.begin(), in_vals.end());

        for (int data_sz : data_sizes) {
            std::vector<T> data(data_sz);
            for (int i = 0; i < data_sz; ++i) {
                data[i] = static_cast<T>(i % 512);
            }
            VerifyFilterChunk(in_vals,
                              data,
                              label + " in=" + std::to_string(in_sz) +
                                  " data=" + std::to_string(data_sz));
        }
    }
}

TEST(FilterChunkCrossTest, Int32VaryingSizes) {
    VerifyFilterChunkVaryingSizes<int32_t>("int32");
}

TEST(FilterChunkCrossTest, Int64VaryingSizes) {
    VerifyFilterChunkVaryingSizes<int64_t>("int64");
}

TEST(FilterChunkCrossTest, FloatVaryingSizes) {
    VerifyFilterChunkVaryingSizes<float>("float");
}
