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

#include "segcore/SingletonConfig.h"
#include "test_utils/SingletonConfigRestorer.h"
#include <gtest/gtest.h>

TEST(SingletonConfig, GetterAndSetter) {
    using namespace milvus::segcore;

    milvus::test::SingletonConfigRestorer restorer;

    auto& cfg = SingletonConfig::GetInstance();

    cfg.set_enable_parallel_reduce(true);
    cfg.set_nq_threshold_to_enable_parallel_reduce(1000);
    cfg.set_k_threshold_to_enable_parallel_reduce(10000);

    ASSERT_TRUE(cfg.is_enable_parallel_reduce());
    ASSERT_EQ(cfg.get_nq_threshold_to_enable_parallel_reduce(), 1000);
    ASSERT_EQ(cfg.get_k_threshold_to_enable_parallel_reduce(), 10000);
}