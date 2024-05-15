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

#include <google/protobuf/text_format.h>
#include <gtest/gtest.h>

#include <array>
#include <boost/format.hpp>
#include <chrono>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <unordered_set>

#include "common/EasyAssert.h"
#include "common/LoadInfo.h"
#include "common/Types.h"
#include "common/type_c.h"
#include "plan/PlanNode.h"

using namespace milvus;
using namespace milvus::segcore;
using namespace milvus::tracer;
using namespace milvus::plan;

TEST(PlanIdTest, planid_generator) {
    PlanNodeIdGenerator generator;
    auto id = generator.Next();
    EXPECT_STREQ(id.c_str(), "0");
    id = generator.Next();
    EXPECT_STREQ(id.c_str(), "1");
    id = generator.Next();
    EXPECT_STREQ(id.c_str(), "2");

    generator.Set(std::numeric_limits<int>::max());
    id = generator.Next();
    EXPECT_STREQ(id.c_str(), "0");
    id = generator.Next();
    EXPECT_STREQ(id.c_str(), "1");

    generator.ReSet();
    id = generator.Next();
    EXPECT_STREQ(id.c_str(), "0");
}
