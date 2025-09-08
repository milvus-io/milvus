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
#include <string_view>

#include "common/Utils.h"
#include "query/Utils.h"

TEST(Util_Query, StringMatch) {
    using namespace milvus;
    using namespace milvus::query;

    ASSERT_ANY_THROW(Match(1, 2, OpType::PrefixMatch));
    ASSERT_ANY_THROW(Match(std::string("not_match_operation"),
                           std::string("not_match"),
                           OpType::LessEqual));

    ASSERT_TRUE(PrefixMatch("prefix1", "prefix"));
    ASSERT_TRUE(PostfixMatch("1postfix", "postfix"));
    ASSERT_TRUE(InnerMatch("xxinner1xx", "inner"));
    ASSERT_TRUE(Match(
        std::string("prefix1"), std::string("prefix"), OpType::PrefixMatch));
    ASSERT_TRUE(Match(
        std::string("1postfix"), std::string("postfix"), OpType::PostfixMatch));
    ASSERT_TRUE(Match(std::string("xxpostfixxx"),
                      std::string("postfix"),
                      OpType::InnerMatch));

    ASSERT_FALSE(PrefixMatch("", "longer"));
    ASSERT_FALSE(PostfixMatch("", "longer"));
    ASSERT_FALSE(InnerMatch("", "longer"));

    ASSERT_FALSE(PrefixMatch("dontmatch", "prefix"));
    ASSERT_FALSE(InnerMatch("dontmatch", "postfix"));

    ASSERT_TRUE(Match(std::string_view("prefix1"),
                      std::string("prefix"),
                      OpType::PrefixMatch));
    ASSERT_TRUE(Match(std::string_view("1postfix"),
                      std::string("postfix"),
                      OpType::PostfixMatch));
    ASSERT_TRUE(Match(std::string_view("xxpostfixxx"),
                      std::string("postfix"),
                      OpType::InnerMatch));
    ASSERT_TRUE(
        Match(std::string_view("x"), std::string("x"), OpType::PrefixMatch));
    ASSERT_FALSE(
        Match(std::string_view(""), std::string("x"), OpType::InnerMatch));
    ASSERT_TRUE(
        Match(std::string_view("x"), std::string(""), OpType::InnerMatch));
}

TEST(Util_Query, OutOfRange) {
    using milvus::query::out_of_range;

    ASSERT_FALSE(out_of_range<int32_t>(
        static_cast<int64_t>(std::numeric_limits<int32_t>::max()) - 1));
    ASSERT_FALSE(out_of_range<int32_t>(
        static_cast<int64_t>(std::numeric_limits<int32_t>::min()) + 1));

    ASSERT_TRUE(out_of_range<int32_t>(
        static_cast<int64_t>(std::numeric_limits<int32_t>::max()) + 1));
    ASSERT_TRUE(out_of_range<int32_t>(
        static_cast<int64_t>(std::numeric_limits<int32_t>::min()) - 1));
}

TEST(Util_Query, DisCloser) {
    EXPECT_TRUE(milvus::query::dis_closer(0.1, 0.2, "L2"));
    EXPECT_FALSE(milvus::query::dis_closer(0.2, 0.1, "L2"));
    EXPECT_FALSE(milvus::query::dis_closer(0.1, 0.1, "L2"));

    EXPECT_TRUE(milvus::query::dis_closer(0.2, 0.1, "IP"));
    EXPECT_FALSE(milvus::query::dis_closer(0.1, 0.2, "IP"));
    EXPECT_FALSE(milvus::query::dis_closer(0.1, 0.1, "IP"));
}
