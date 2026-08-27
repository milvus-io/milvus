// Copyright (C) 2019-2025 Zilliz. All rights reserved.
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

#include <algorithm>
#include <cstdint>
#include <limits>
#include <utility>

#include "common/Consts.h"
#include "common/Utils.h"
#include "index/Meta.h"
#include "segcore/storagev1translator/TextMatchIndexTranslator.h"
#include "storage/ThreadPools.h"

namespace milvus::segcore::storagev1translator {
namespace {

TEST(TextMatchIndexTranslatorTest, NonMmapAccountsForRamDirectoryCopyPeak) {
    constexpr int64_t kIndexSize = 64 * 1024 * 1024;
    TextMatchIndexLoadInfo load_info{
        false, 1, 2, "{}", kIndexSize, 3, 0, "disable"};
    TextMatchIndexTranslator translator(
        std::move(load_info), storage::FileManagerContext{}, Config{});

    const auto [final_usage, peak_usage] =
        translator.estimated_loading_usage({0});

    EXPECT_EQ(final_usage.memory_bytes, kIndexSize);
    EXPECT_EQ(final_usage.file_bytes, 0);
    EXPECT_EQ(peak_usage.memory_bytes, 2 * kIndexSize);
    EXPECT_EQ(peak_usage.file_bytes, kIndexSize);
}

TEST(TextMatchIndexTranslatorTest,
     MmapV3UsesPoolBoundedStreamAndValidityBitmap) {
    constexpr int64_t kIndexSize = std::numeric_limits<int64_t>::max() / 4;
    constexpr int64_t kNumRows = 512 * 1024;
    constexpr int64_t kValidityBitmapBytes = kNumRows / 8;
    constexpr auto kLoadPriority = proto::common::LoadPriority::HIGH;

    TextMatchIndexLoadInfo load_info{
        true, 1, 2, "{}", kIndexSize, 3, kNumRows, "disable"};
    Config config;
    config[LOAD_PRIORITY] = kLoadPriority;
    TextMatchIndexTranslator translator(
        std::move(load_info), storage::FileManagerContext{}, config);

    const auto worker_count = std::max<size_t>(
        1,
        ThreadPools::GetThreadPool(PriorityForLoad(kLoadPriority))
            .GetMaxThreadNum());
    const auto stream_peak =
        std::min<int64_t>(kIndexSize,
                          SaturatingMultiply(static_cast<int64_t>(worker_count),
                                             DEFAULT_INDEX_FILE_SLICE_SIZE));
    const auto expected_peak = SaturatingAdd(stream_peak, kValidityBitmapBytes);

    const auto [final_usage, peak_usage] =
        translator.estimated_loading_usage({0});

    EXPECT_EQ(final_usage.memory_bytes, kValidityBitmapBytes);
    EXPECT_EQ(final_usage.file_bytes, kIndexSize);
    EXPECT_EQ(peak_usage.memory_bytes, expected_peak);
    EXPECT_EQ(peak_usage.file_bytes, kIndexSize);
}

}  // namespace
}  // namespace milvus::segcore::storagev1translator
