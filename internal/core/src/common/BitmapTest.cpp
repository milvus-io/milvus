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
#include "test_utils/DataGen.h"
#include "index/ScalarIndexSort.h"

TEST(Bitmap, Naive) {
    using namespace milvus;
    using namespace milvus::segcore;

    auto schema = std::make_shared<Schema>();
    auto field_id = schema->AddDebugField("height", DataType::FLOAT);
    int N = 10000;
    auto raw_data = DataGen(schema, N);
    auto vec = raw_data.get_col<float>(field_id);
    auto sort_index = std::make_shared<index::ScalarIndexSort<float>>();
    sort_index->Build(N, vec.data());
    {
        auto res = sort_index->Range(0, OpType::LessThan);
        double count = 0;
        for (size_t i = 0; i < res.size(); ++i) {
            if (res[i] == true)
                count++;
        }
        ASSERT_NEAR(count / N, 0.5, 0.01);
    }
    {
        auto res = sort_index->Range(-1, false, 1, true);
        double count = 0;
        for (size_t i = 0; i < res.size(); ++i) {
            if (res[i] == true)
                count++;
        }
        ASSERT_NEAR(count / N, 0.682, 0.01);
    }
}
