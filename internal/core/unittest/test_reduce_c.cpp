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

#include "common/Consts.h"
#include "segcore/ReduceStructure.h"

TEST(SearchResultPair, Greater) {
    auto pair1 = SearchResultPair(0, 1.0, nullptr, 0, 0, 10);
    auto pair2 = SearchResultPair(1, 2.0, nullptr, 1, 0, 10);
    ASSERT_EQ(pair1 > pair2, false);

    pair1.primary_key_ = INVALID_ID;
    pair2.primary_key_ = 1;
    ASSERT_EQ(pair1 > pair2, false);

    pair1.primary_key_ = 0;
    pair2.primary_key_ = INVALID_ID;
    ASSERT_EQ(pair1 > pair2, true);

    pair1.primary_key_ = INVALID_ID;
    pair2.primary_key_ = INVALID_ID;
    ASSERT_EQ(pair1 > pair2, false);
}
