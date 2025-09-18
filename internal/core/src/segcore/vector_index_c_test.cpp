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
#include "segcore/vector_index_c.h"

TEST(CApiTest, GetIndexListSizeAndFeatures) {
    int size = GetIndexListSize();
    ASSERT_GT(size, 0);

    std::vector<const char*> index_keys(size);
    std::vector<uint64_t> index_features(size);

    GetIndexFeatures(index_keys.data(), index_features.data());

    for (int i = 0; i < size; i++) {
        ASSERT_NE(index_keys[i], nullptr);
        ASSERT_GT(strlen(index_keys[i]), 0);
        ASSERT_GT(index_features[i], 0);
    }
}