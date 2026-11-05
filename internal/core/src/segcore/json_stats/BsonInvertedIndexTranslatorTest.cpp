// Copyright(C) 2019 - 2020 Zilliz.All rights reserved.
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

#include "index/LoadResource.h"
#include "segcore/json_stats/BsonInvertedIndexTranslator.h"

namespace milvus::segcore {
namespace {

TEST(BsonInvertedIndexTranslatorTest, ReservesFixedScalarReaderMemoryOnce) {
    constexpr int64_t index_size = 4096;
    for (const bool mmap : {false, true}) {
        BsonInvertedIndexTranslator translator(
            {mmap, 3, 101, index_size, {}, 0, "disable", ""},
            storage::FileManagerContext{});
        const auto [loaded, private_overhead, shared_overhead] =
            translator.estimated_loading_usage({0});
        const auto overhead = private_overhead + shared_overhead;
        EXPECT_EQ(
            loaded.memory_bytes,
            (mmap ? 0 : index_size) + index::kScalarIndexFixedResidentBytes);
        EXPECT_EQ(loaded.file_bytes, mmap ? index_size : 0);
        EXPECT_EQ(overhead.memory_bytes, mmap ? index_size : 0);
        EXPECT_EQ(overhead.file_bytes, mmap ? 0 : index_size);
    }
}

}  // namespace
}  // namespace milvus::segcore
