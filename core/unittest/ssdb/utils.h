// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once

#include <gtest/gtest.h>
#include <random>
#include <memory>

#include "db/SSDBImpl.h"

inline int
RandomInt(int start, int end) {
    std::random_device dev;
    std::mt19937 rng(dev());
    std::uniform_int_distribution<std::mt19937::result_type> dist(start, end);
    return dist(rng);
}

class BaseTest : public ::testing::Test {
 protected:
    void
    InitLog();

    void
    SetUp() override;
    void
    TearDown() override;
};

class SnapshotTest : public BaseTest {
 protected:
    void
    SetUp() override;
    void
    TearDown() override;
};

class SSDBTest : public BaseTest {
 protected:
    std::shared_ptr<milvus::engine::SSDBImpl> db_;

    void
    SetUp() override;
    void
    TearDown() override;
};
