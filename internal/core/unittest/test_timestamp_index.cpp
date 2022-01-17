// Copyright (C) 2019-2020 Zilliz. All rights reserved.
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
#include <vector>

#include "segcore/TimestampIndex.h"

using namespace milvus;
using namespace milvus::segcore;

TEST(TimestampIndex, Naive) {
    SUCCEED();
    std::vector<Timestamp> timestamps{
        1, 2, 14, 11, 13, 22, 21, 20,
    };
    std::vector<int64_t> lengths = {2, 3, 3};
    TimestampIndex index;
    index.set_length_meta(lengths);
    index.build_with(timestamps.data(), timestamps.size());

    auto guessed_slice = GenerateFakeSlices(timestamps.data(), timestamps.size(), 2);
    ASSERT_EQ(guessed_slice.size(), lengths.size());
    for (auto i = 0; i < lengths.size(); ++i) {
        ASSERT_EQ(guessed_slice[i], lengths[i]);
    }
}
