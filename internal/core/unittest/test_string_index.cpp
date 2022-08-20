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
#include <knowhere/index/vector_index/helpers/IndexParameter.h>
#include <knowhere/index/vector_index/adapter/VectorAdapter.h>
#include <knowhere/archive/KnowhereConfig.h>

#include "index/Index.h"
#include "index/ScalarIndex.h"

#define private public
#include "index/StringIndex.h"
#include "index/StringIndexMarisa.h"

#include "index/IndexFactory.h"
#include "test_utils/indexbuilder_test_utils.h"
#include "test_utils/AssertUtils.h"

constexpr int64_t nb = 100;
namespace schemapb = milvus::proto::schema;

class StringIndexBaseTest : public ::testing::Test {
    void
    SetUp() override {
        size_t n = 10;
        strs = GenStrArr(n);
        *str_arr.mutable_data() = {strs.begin(), strs.end()};
        str_ds = GenDsFromPB(str_arr);
    }

    void
    TearDown() override {
        delete[](char*)(knowhere::GetDatasetTensor(str_ds));
    }

 protected:
    std::vector<std::string> strs;
    schemapb::StringArray str_arr;
    knowhere::DatasetPtr str_ds;
};

class StringIndexMarisaTest : public StringIndexBaseTest {};

TEST_F(StringIndexMarisaTest, Constructor) {
    auto index = milvus::scalar::CreateStringIndexMarisa();
}

TEST_F(StringIndexMarisaTest, Build) {
    auto index = milvus::scalar::CreateStringIndexMarisa();
    index->Build(strs.size(), strs.data());
}

TEST_F(StringIndexMarisaTest, BuildWithDataset) {
    auto index = milvus::scalar::CreateStringIndexMarisa();
    index->BuildWithDataset(str_ds);
}

TEST_F(StringIndexMarisaTest, Count) {
    auto index = milvus::scalar::CreateStringIndexMarisa();
    index->BuildWithDataset(str_ds);
    ASSERT_EQ(strs.size(), index->Count());
}

TEST_F(StringIndexMarisaTest, In) {
    auto index = milvus::scalar::CreateStringIndexMarisa();
    index->BuildWithDataset(str_ds);
    auto bitset = index->In(strs.size(), strs.data());
    ASSERT_EQ(bitset->size(), strs.size());
    ASSERT_TRUE(bitset->any());
}

TEST_F(StringIndexMarisaTest, NotIn) {
    auto index = milvus::scalar::CreateStringIndexMarisa();
    index->BuildWithDataset(str_ds);
    auto bitset = index->NotIn(strs.size(), strs.data());
    ASSERT_EQ(bitset->size(), strs.size());
    ASSERT_TRUE(bitset->none());
}

TEST_F(StringIndexMarisaTest, Range) {
    auto index = milvus::scalar::CreateStringIndexMarisa();
    std::vector<std::string> strings(nb);
    for (int i = 0; i < nb; ++i) {
        strings[i] = std::to_string(std::rand() % 10);
    }
    *str_arr.mutable_data() = {strings.begin(), strings.end()};
    str_ds = GenDsFromPB(str_arr);
    index->BuildWithDataset(str_ds);

    {
        auto bitset = index->Range("0", milvus::OpType::GreaterEqual);
        ASSERT_EQ(bitset->size(), nb);
        ASSERT_EQ(bitset->count(), nb);
    }

    {
        auto bitset = index->Range("90", milvus::OpType::LessThan);
        ASSERT_EQ(bitset->size(), nb);
        ASSERT_EQ(bitset->count(), nb);
    }

    {
        auto bitset = index->Range("9", milvus::OpType::LessEqual);
        ASSERT_EQ(bitset->size(), nb);
        ASSERT_EQ(bitset->count(), nb);
    }

    {
        auto bitset = index->Range("0", true, "9", true);
        ASSERT_EQ(bitset->size(), nb);
        ASSERT_EQ(bitset->count(), nb);
    }

    {
        auto bitset = index->Range("0", true, "90", false);
        ASSERT_EQ(bitset->size(), nb);
        ASSERT_EQ(bitset->count(), nb);
    }
}

TEST_F(StringIndexMarisaTest, Reverse) {
    auto index_types = GetIndexTypes<std::string>();
    for (const auto& index_type : index_types) {
        auto index = milvus::scalar::IndexFactory::GetInstance().CreateIndex<std::string>(index_type);
        index->BuildWithDataset(str_ds);
        assert_reverse<std::string>(index, strs);
    }
}

TEST_F(StringIndexMarisaTest, PrefixMatch) {
    auto index = milvus::scalar::CreateStringIndexMarisa();
    index->BuildWithDataset(str_ds);

    for (size_t i = 0; i < strs.size(); i++) {
        auto str = strs[i];
        auto bitset = index->PrefixMatch(str);
        ASSERT_EQ(bitset->size(), strs.size());
        ASSERT_TRUE(bitset->test(i));
    }
}

TEST_F(StringIndexMarisaTest, Query) {
    auto index = milvus::scalar::CreateStringIndexMarisa();
    index->BuildWithDataset(str_ds);

    {
        auto ds = knowhere::GenDataset(strs.size(), 8, strs.data());
        ds->Set<milvus::OpType>(milvus::scalar::OPERATOR_TYPE, milvus::OpType::In);
        auto bitset = index->Query(ds);
        ASSERT_TRUE(bitset->any());
    }

    {
        auto ds = knowhere::GenDataset(strs.size(), 8, strs.data());
        ds->Set<milvus::OpType>(milvus::scalar::OPERATOR_TYPE, milvus::OpType::NotIn);
        auto bitset = index->Query(ds);
        ASSERT_TRUE(bitset->none());
    }

    {
        auto ds = std::make_shared<knowhere::Dataset>();
        ds->Set<milvus::OpType>(milvus::scalar::OPERATOR_TYPE, milvus::OpType::GreaterEqual);
        ds->Set<std::string>(milvus::scalar::RANGE_VALUE, "0");
        auto bitset = index->Query(ds);
        ASSERT_EQ(bitset->size(), strs.size());
        ASSERT_EQ(bitset->count(), strs.size());
    }

    {
        auto ds = std::make_shared<knowhere::Dataset>();
        ds->Set<milvus::OpType>(milvus::scalar::OPERATOR_TYPE, milvus::OpType::Range);
        ds->Set<std::string>(milvus::scalar::LOWER_BOUND_VALUE, "0");
        ds->Set<std::string>(milvus::scalar::UPPER_BOUND_VALUE, "range");
        ds->Set<bool>(milvus::scalar::LOWER_BOUND_INCLUSIVE, true);
        ds->Set<bool>(milvus::scalar::UPPER_BOUND_INCLUSIVE, true);
        auto bitset = index->Query(ds);
        ASSERT_TRUE(bitset->any());
    }

    {
        for (size_t i = 0; i < strs.size(); i++) {
            auto ds = std::make_shared<knowhere::Dataset>();
            ds->Set<milvus::OpType>(milvus::scalar::OPERATOR_TYPE, milvus::OpType::PrefixMatch);
            ds->Set<std::string>(milvus::scalar::PREFIX_VALUE, std::move(strs[i]));
            auto bitset = index->Query(ds);
            ASSERT_EQ(bitset->size(), strs.size());
            ASSERT_TRUE(bitset->test(i));
        }
    }
}

TEST_F(StringIndexMarisaTest, Codec) {
    auto index = milvus::scalar::CreateStringIndexMarisa();
    std::vector<std::string> strings(nb);
    for (int i = 0; i < nb; ++i) {
        strings[i] = std::to_string(std::rand() % 10);
    }
    *str_arr.mutable_data() = {strings.begin(), strings.end()};
    str_ds = GenDsFromPB(str_arr);
    index->BuildWithDataset(str_ds);

    std::vector<std::string> invalid_strings = {std::to_string(nb)};
    auto copy_index = milvus::scalar::CreateStringIndexMarisa();

    {
        auto binary_set = index->Serialize(nullptr);
        copy_index->Load(binary_set);
    }

    {
        auto bitset = copy_index->In(nb, strings.data());
        ASSERT_EQ(bitset->size(), nb);
        ASSERT_TRUE(bitset->any());
    }

    {
        auto bitset = copy_index->In(1, invalid_strings.data());
        ASSERT_EQ(bitset->size(), nb);
        ASSERT_TRUE(bitset->none());
    }

    {
        auto bitset = copy_index->NotIn(nb, strings.data());
        ASSERT_EQ(bitset->size(), nb);
        ASSERT_TRUE(bitset->none());
    }

    {
        auto bitset = copy_index->Range("0", milvus::OpType::GreaterEqual);
        ASSERT_EQ(bitset->size(), nb);
        ASSERT_EQ(bitset->count(), nb);
    }

    {
        auto bitset = copy_index->Range("90", milvus::OpType::LessThan);
        ASSERT_EQ(bitset->size(), nb);
        ASSERT_EQ(bitset->count(), nb);
    }

    {
        auto bitset = copy_index->Range("9", milvus::OpType::LessEqual);
        ASSERT_EQ(bitset->size(), nb);
        ASSERT_EQ(bitset->count(), nb);
    }

    {
        auto bitset = copy_index->Range("0", true, "9", true);
        ASSERT_EQ(bitset->size(), nb);
        ASSERT_EQ(bitset->count(), nb);
    }

    {
        auto bitset = copy_index->Range("0", true, "90", false);
        ASSERT_EQ(bitset->size(), nb);
        ASSERT_EQ(bitset->count(), nb);
    }

    {
        for (size_t i = 0; i < nb; i++) {
            auto str = strings[i];
            auto bitset = copy_index->PrefixMatch(str);
            ASSERT_EQ(bitset->size(), nb);
            ASSERT_TRUE(bitset->test(i));
        }
    }
}
