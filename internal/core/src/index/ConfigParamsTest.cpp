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

#include <string>

#include "common/Common.h"
#include "folly/ScopeGuard.h"
#include "index/Utils.h"

namespace milvus::index {
namespace {

TEST(IndexConfigParamsTest, CheckedValuesAndMissingValues) {
    const auto previous = CONFIG_PARAM_TYPE_CHECK_ENABLED.load();
    auto restore = folly::makeGuard(
        [previous] { SetDefaultConfigParamTypeCheck(previous); });
    SetDefaultConfigParamTypeCheck(true);
    const Config config{{"integer", 100},
                        {"boolean", true},
                        {"boolean_string", "true"},
                        {"real", 1.234},
                        {"null", nullptr}};
    EXPECT_EQ(GetValueFromConfig<int64_t>(config, "integer"), 100);
    EXPECT_EQ(GetValueFromConfig<bool>(config, "boolean"), true);
    EXPECT_EQ(GetValueFromConfig<bool>(config, "boolean_string"), true);
    ASSERT_TRUE(GetValueFromConfig<double>(config, "real").has_value());
    EXPECT_NEAR(*GetValueFromConfig<double>(config, "real"), 1.234, 0.001);
    EXPECT_FALSE(GetValueFromConfig<std::string>(config, "null").has_value());
    EXPECT_FALSE(GetValueFromConfig<std::string>(config, "absent").has_value());
    try {
        static_cast<void>(GetValueFromConfig<std::string>(config, "real"));
        FAIL() << "a checked type mismatch must throw";
    } catch (const SegcoreError& error) {
        EXPECT_NE(std::string(error.what()).find("config type error for key"),
                  std::string::npos);
    }
}

TEST(IndexConfigParamsTest, UncheckedTypeMismatchReturnsNoValue) {
    const auto previous = CONFIG_PARAM_TYPE_CHECK_ENABLED.load();
    auto restore = folly::makeGuard(
        [previous] { SetDefaultConfigParamTypeCheck(previous); });
    SetDefaultConfigParamTypeCheck(false);
    const Config config{{"integer", 100},
                        {"boolean", true},
                        {"boolean_string", "true"},
                        {"real", 1.234},
                        {"real_string", "1.234"},
                        {"null", nullptr}};
    EXPECT_EQ(GetValueFromConfig<int64_t>(config, "integer"), 100);
    EXPECT_EQ(GetValueFromConfig<bool>(config, "boolean"), true);
    EXPECT_EQ(GetValueFromConfig<bool>(config, "boolean_string"), true);
    ASSERT_TRUE(GetValueFromConfig<double>(config, "real").has_value());
    EXPECT_NEAR(*GetValueFromConfig<double>(config, "real"), 1.234, 0.001);
    EXPECT_FALSE(GetValueFromConfig<double>(config, "real_string").has_value());
    EXPECT_FALSE(GetValueFromConfig<bool>(config, "null").has_value());
}

}  // namespace
}  // namespace milvus::index
