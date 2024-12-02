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

#include "common/Types.h"
#include "common/Vector.h"
#include "exec/expression/function/impl/StringFunctions.h"

using namespace milvus;
using namespace milvus::exec::expression::function;

class FunctionTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
    }

    void
    TearDown() override {
    }
};

TEST_F(FunctionTest, Empty) {
    std::vector<milvus::VectorPtr> arg_vec;
    auto col1 =
        std::make_shared<milvus::ColumnVector>(milvus::DataType::STRING, 15);
    auto* col1_data = col1->RawAsValues<std::string>();
    for (int i = 0; i <= 10; ++i) {
        col1_data[i] = std::to_string(i);
    }
    for (int i = 11; i < 15; ++i) {
        col1_data[i] = "";
    }
    arg_vec.push_back(col1);
    milvus::RowVector args(std::move(arg_vec));
    VectorPtr result;
    EmptyVarchar(args, result);

    auto result_vec = std::dynamic_pointer_cast<milvus::ColumnVector>(result);
    ASSERT_NE(result_vec, nullptr);
    TargetBitmapView bitmap(result_vec->GetRawData(), result_vec->size());
    for (int i = 0; i < 15; ++i) {
        EXPECT_TRUE(result_vec->ValidAt(i)) << "i: " << i;
        EXPECT_EQ(bitmap[i], i >= 11) << "i: " << i;
    }
}

TEST_F(FunctionTest, EmptyNull) {
    std::vector<milvus::VectorPtr> arg_vec;
    auto col1 = std::make_shared<milvus::ColumnVector>(
        milvus::DataType::STRING, 15, 15);
    arg_vec.push_back(col1);
    milvus::RowVector args(std::move(arg_vec));
    VectorPtr result;
    EmptyVarchar(args, result);

    auto result_vec = std::dynamic_pointer_cast<milvus::ColumnVector>(result);
    ASSERT_NE(result_vec, nullptr);
    TargetBitmapView bitmap(result_vec->GetRawData(), result_vec->size());
    for (int i = 0; i < 15; ++i) {
        EXPECT_FALSE(result_vec->ValidAt(i)) << "i: " << i;
    }
}

TEST_F(FunctionTest, EmptyConstant) {
    std::vector<milvus::VectorPtr> arg_vec;
    auto col1 = std::make_shared<milvus::ConstantVector<std::string>>(
        milvus::DataType::STRING, 15, "xx");
    arg_vec.push_back(col1);
    milvus::RowVector args(std::move(arg_vec));
    VectorPtr result;
    EmptyVarchar(args, result);

    auto result_vec = std::dynamic_pointer_cast<milvus::ColumnVector>(result);
    ASSERT_NE(result_vec, nullptr);
    TargetBitmapView bitmap(result_vec->GetRawData(), result_vec->size());
    for (int i = 0; i < 15; ++i) {
        EXPECT_TRUE(result_vec->ValidAt(i)) << "i: " << i;
        EXPECT_FALSE(bitmap[i]) << "i: " << i;
    }
}

TEST_F(FunctionTest, EmptyIncorrectArgs) {
    VectorPtr result;

    std::vector<milvus::VectorPtr> arg_vec;

    // empty args
    milvus::RowVector empty_args(arg_vec);
    EXPECT_ANY_THROW(EmptyVarchar(empty_args, result));

    // incorrect type, expected string or varchar
    arg_vec.push_back(std::make_shared<milvus::ColumnVector>(
        milvus::DataType::INT32, 15, 15));
    milvus::RowVector int_args(arg_vec);
    EXPECT_ANY_THROW(EmptyVarchar(int_args, result));

    arg_vec.clear();

    // incorrect size, expected 1
    arg_vec.push_back(std::make_shared<milvus::ColumnVector>(
        milvus::DataType::STRING, 15, 15));
    arg_vec.push_back(std::make_shared<milvus::ColumnVector>(
        milvus::DataType::STRING, 15, 15));
    milvus::RowVector multi_args(arg_vec);
    EXPECT_ANY_THROW(EmptyVarchar(multi_args, result));
}

static constexpr int64_t STARTS_WITH_ROW_COUNT = 8;
static void
InitStrsForStartWith(std::shared_ptr<milvus::ColumnVector> col1) {
    auto* col1_data = col1->RawAsValues<std::string>();
    col1_data[0] = "123";
    col1_data[1] = "";
    col1_data[2] = "";
    TargetBitmapView valid_bitmap_col1(col1->GetValidRawData(), col1->size());
    valid_bitmap_col1[2] = false;
    col1_data[3] = "aaabbbaaa";
    col1_data[4] = "aaabbbaaa";
    col1_data[5] = "xx";
    col1_data[6] = "xx";
    col1_data[7] = "1";
}

static void
StartWithCheck(VectorPtr result, bool valid[], bool expected[]) {
    auto result_vec = std::dynamic_pointer_cast<milvus::ColumnVector>(result);
    ASSERT_NE(result_vec, nullptr);
    TargetBitmapView bitmap(result_vec->GetRawData(), result_vec->size());
    for (int i = 0; i < STARTS_WITH_ROW_COUNT; ++i) {
        EXPECT_EQ(result_vec->ValidAt(i), valid[i]) << "i: " << i;
        EXPECT_EQ(bitmap[i], expected[i]) << "i: " << i;
    }
}

TEST_F(FunctionTest, StartsWithColumnVector) {
    std::vector<milvus::VectorPtr> arg_vec;

    auto col1 = std::make_shared<milvus::ColumnVector>(milvus::DataType::STRING,
                                                       STARTS_WITH_ROW_COUNT);
    InitStrsForStartWith(col1);
    arg_vec.push_back(col1);

    auto col2 = std::make_shared<milvus::ColumnVector>(milvus::DataType::STRING,
                                                       STARTS_WITH_ROW_COUNT);
    auto* col2_data = col2->RawAsValues<std::string>();
    col2_data[0] = "12";
    col2_data[1] = "1";
    col2_data[3] = "aaabbbaaac";
    col2_data[4] = "aaabbbaax";
    col2_data[5] = "";
    col2_data[6] = "";
    TargetBitmapView valid_bitmap_col2(col2->GetValidRawData(), col2->size());
    valid_bitmap_col2[6] = false;
    col2_data[7] = "124";
    arg_vec.push_back(col2);

    milvus::RowVector args(std::move(arg_vec));

    bool valid[STARTS_WITH_ROW_COUNT] = {
        true, true, false, true, true, true, false, true};
    bool expected[STARTS_WITH_ROW_COUNT] = {
        true, false, false, false, false, true, false, false};

    VectorPtr result;
    StartsWithVarchar(args, result);
    StartWithCheck(result, valid, expected);
}

TEST_F(FunctionTest, StartsWithColumnAndConstantVector) {
    std::vector<milvus::VectorPtr> arg_vec;

    auto col1 = std::make_shared<milvus::ColumnVector>(milvus::DataType::STRING,
                                                       STARTS_WITH_ROW_COUNT);
    InitStrsForStartWith(col1);
    arg_vec.push_back(col1);

    const std::string constant_str = "1";
    auto col2 = std::make_shared<milvus::ConstantVector<std::string>>(
        milvus::DataType::STRING, STARTS_WITH_ROW_COUNT, constant_str);
    arg_vec.push_back(col2);

    milvus::RowVector args(std::move(arg_vec));

    bool valid[STARTS_WITH_ROW_COUNT] = {
        true, true, false, true, true, true, true, true};
    bool expected[STARTS_WITH_ROW_COUNT] = {
        true, false, false, false, false, false, false, true};

    VectorPtr result;
    StartsWithVarchar(args, result);
    StartWithCheck(result, valid, expected);
}

TEST_F(FunctionTest, StartsWithIncorrectArgs) {
    VectorPtr result;

    std::vector<milvus::VectorPtr> arg_vec;

    // empty args
    milvus::RowVector empty_args(arg_vec);
    EXPECT_ANY_THROW(StartsWithVarchar(empty_args, result));

    // incorrect type, expected string or varchar
    arg_vec.push_back(std::make_shared<milvus::ColumnVector>(
        milvus::DataType::STRING, 15, 15));
    arg_vec.push_back(std::make_shared<milvus::ColumnVector>(
        milvus::DataType::INT32, 15, 15));
    milvus::RowVector string_int_args(arg_vec);
    EXPECT_ANY_THROW(StartsWithVarchar(string_int_args, result));

    arg_vec.clear();

    // incorrect size, expected 2
    arg_vec.push_back(std::make_shared<milvus::ColumnVector>(
        milvus::DataType::STRING, 15, 15));
    milvus::RowVector single_args(arg_vec);
    EXPECT_ANY_THROW(StartsWithVarchar(single_args, result));

    arg_vec.clear();
    arg_vec.push_back(std::make_shared<milvus::ColumnVector>(
        milvus::DataType::STRING, 15, 15));
    arg_vec.push_back(std::make_shared<milvus::ColumnVector>(
        milvus::DataType::STRING, 15, 15));
    arg_vec.push_back(std::make_shared<milvus::ColumnVector>(
        milvus::DataType::STRING, 15, 15));
    milvus::RowVector three_args(arg_vec);
    EXPECT_ANY_THROW(StartsWithVarchar(three_args, result));
}
