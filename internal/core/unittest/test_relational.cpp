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
#include "query/Relational.h"
#include "common/Utils.h"
#include <string>

TEST(Relational, Basic) {
    using namespace milvus::query;

    int64_t i64 = 4;
    int64_t another_i64 = 5;

    std::string s = "str4";
    std::string another_s = "str5";

    ASSERT_EQ(Relational<decltype(std::equal_to<>{})>()(i64, another_i64), i64 == another_i64);
    ASSERT_EQ(Relational<decltype(std::not_equal_to<>{})>()(i64, another_i64), i64 != another_i64);
    ASSERT_EQ(Relational<decltype(std::greater_equal<>{})>()(i64, another_i64), i64 >= another_i64);
    ASSERT_EQ(Relational<decltype(std::greater<>{})>()(i64, another_i64), i64 > another_i64);
    ASSERT_EQ(Relational<decltype(std::less_equal<>{})>()(i64, another_i64), i64 <= another_i64);
    ASSERT_EQ(Relational<decltype(std::less<>{})>()(i64, another_i64), i64 < another_i64);

    ASSERT_EQ(Relational<decltype(std::equal_to<>{})>()(s, another_s), s == another_s);
    ASSERT_EQ(Relational<decltype(std::not_equal_to<>{})>()(s, another_s), s != another_s);
    ASSERT_EQ(Relational<decltype(std::greater_equal<>{})>()(s, another_s), s >= another_s);
    ASSERT_EQ(Relational<decltype(std::greater<>{})>()(s, another_s), s > another_s);
    ASSERT_EQ(Relational<decltype(std::less_equal<>{})>()(s, another_s), s <= another_s);
    ASSERT_EQ(Relational<decltype(std::less<>{})>()(s, another_s), s < another_s);
    ASSERT_EQ(Relational<decltype(MatchOp<milvus::OpType::PrefixMatch>{})>()(s, another_s),
              milvus::PrefixMatch(s, another_s));
    ASSERT_EQ(Relational<decltype(MatchOp<milvus::OpType::PostfixMatch>{})>()(s, another_s),
              milvus::PostfixMatch(s, another_s));
}

TEST(Relational, DifferentFundamentalType) {
    using namespace milvus::query;

    int32_t i32 = 3;
    int64_t i64 = 4;

    ASSERT_EQ(Relational<decltype(std::equal_to<>{})>()(i64, i32), i64 == i32);
    ASSERT_EQ(Relational<decltype(std::not_equal_to<>{})>()(i64, i32), i64 != i32);
    ASSERT_EQ(Relational<decltype(std::greater_equal<>{})>()(i64, i32), i64 >= i32);
    ASSERT_EQ(Relational<decltype(std::greater<>{})>()(i64, i32), i64 > i32);
    ASSERT_EQ(Relational<decltype(std::less_equal<>{})>()(i64, i32), i64 <= i32);
    ASSERT_EQ(Relational<decltype(std::less<>{})>()(i64, i32), i64 < i32);
}

TEST(Relational, DifferentInCompatibleType) {
    using namespace milvus::query;

    int64_t i64 = 4;
    std::string s = "str4";

    ASSERT_ANY_THROW(Relational<decltype(std::equal_to<>{})>()(s, i64));
    ASSERT_ANY_THROW(Relational<decltype(std::not_equal_to<>{})>()(s, i64));
    ASSERT_ANY_THROW(Relational<decltype(std::greater_equal<>{})>()(s, i64));
    ASSERT_ANY_THROW(Relational<decltype(std::greater<>{})>()(s, i64));
    ASSERT_ANY_THROW(Relational<decltype(std::less_equal<>{})>()(s, i64));
    ASSERT_ANY_THROW(Relational<decltype(std::less<>{})>()(s, i64));
    ASSERT_ANY_THROW(Relational<decltype(MatchOp<milvus::OpType::PrefixMatch>{})>()(s, i64));
    ASSERT_ANY_THROW(Relational<decltype(MatchOp<milvus::OpType::PostfixMatch>{})>()(s, i64));

    ASSERT_ANY_THROW(Relational<decltype(std::equal_to<>{})>()(i64, s));
    ASSERT_ANY_THROW(Relational<decltype(std::not_equal_to<>{})>()(i64, s));
    ASSERT_ANY_THROW(Relational<decltype(std::greater_equal<>{})>()(i64, s));
    ASSERT_ANY_THROW(Relational<decltype(std::greater<>{})>()(i64, s));
    ASSERT_ANY_THROW(Relational<decltype(std::less_equal<>{})>()(i64, s));
    ASSERT_ANY_THROW(Relational<decltype(std::less<>{})>()(i64, s));
    ASSERT_ANY_THROW(Relational<decltype(MatchOp<milvus::OpType::PrefixMatch>{})>()(i64, s));
    ASSERT_ANY_THROW(Relational<decltype(MatchOp<milvus::OpType::PostfixMatch>{})>()(i64, s));
}
