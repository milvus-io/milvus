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

#include "common/FastMem.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

#include <gtest/gtest.h>

namespace milvus::fastmem {
namespace {

constexpr uint8_t kGuardByte = 0xA5;
constexpr uint8_t kFillByte = 0x5A;
constexpr size_t kGuardSize = 8;

std::vector<uint8_t>
MakeSource(size_t size, size_t extra = 0) {
    std::vector<uint8_t> source(size + extra);
    for (size_t i = 0; i < source.size(); ++i) {
        source[i] = static_cast<uint8_t>((i * 131 + 17) & 0xFF);
    }
    return source;
}

void
AssertCopyMatchesStdMemcpy(size_t size, size_t src_offset, size_t dst_offset) {
    auto source = MakeSource(size, src_offset);
    std::vector<uint8_t> expected(size + dst_offset + kGuardSize, kGuardByte);
    std::vector<uint8_t> actual(size + dst_offset + kGuardSize, kGuardByte);

    if (size != 0) {
        std::memcpy(
            expected.data() + dst_offset, source.data() + src_offset, size);
    }
    FastMemcpy(actual.data() + dst_offset, source.data() + src_offset, size);

    ASSERT_EQ(actual, expected);
    for (size_t i = 0; i < dst_offset; ++i) {
        ASSERT_EQ(actual[i], kGuardByte);
    }
    for (size_t i = dst_offset + size; i < actual.size(); ++i) {
        ASSERT_EQ(actual[i], kGuardByte);
    }
}

}  // namespace

TEST(FastMemTest, FastMemcpyMatchesStdMemcpyForSmallSizes) {
    for (size_t size = 0; size <= kFastMemcpyMaxSize; ++size) {
        AssertCopyMatchesStdMemcpy(size, 0, 0);
    }
}

TEST(FastMemTest, FastMemcpyFallsBackForLargeSizes) {
    for (auto size :
         std::array<size_t, 5>{kFastMemcpyMaxSize + 1, 257, 512, 1024, 4093}) {
        AssertCopyMatchesStdMemcpy(size, 0, 0);
    }
}

TEST(FastMemTest, FastMemcpyHandlesUnalignedPointers) {
    for (auto size : std::array<size_t, 9>{
             0, 1, 7, 16, 31, 64, kFastMemcpyMaxSize, 255, 4093}) {
        AssertCopyMatchesStdMemcpy(size, 1, 3);
    }
}

TEST(FastMemTest, FastMemcpyEnabledReflectsMaxSize) {
#if MILVUS_FASTCPY_MAX_SIZE == 0
    ASSERT_FALSE(FastMemcpyEnabled());
#else
    ASSERT_TRUE(kFastMemcpyPlatformSupported);
    ASSERT_TRUE(FastMemcpyEnabled());
#endif
}

TEST(FastMemTest, FastMemcpyDispatchLimitMatchesMaxSizeLimit) {
    ASSERT_EQ(kFastMemcpyDispatchMaxSize, 1024);
    ASSERT_LE(kFastMemcpyMaxSize, kFastMemcpyDispatchMaxSize);
}

TEST(FastMemTest, FastMemcpyKeepsZeroSizeDestinationUntouched) {
    std::array<uint8_t, 16> source{};
    std::array<uint8_t, 16> destination{};
    destination.fill(kFillByte);

    FastMemcpy(destination.data(), source.data(), 0);

    for (auto value : destination) {
        ASSERT_EQ(value, kFillByte);
    }
}

TEST(FastMemTest, FastMemcpyMatchesStdMemcpyForComparisonCases) {
    for (auto size : std::array<size_t, 13>{
             0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 768, 1024}) {
        AssertCopyMatchesStdMemcpy(size, 1, 3);
    }
}

}  // namespace milvus::fastmem
