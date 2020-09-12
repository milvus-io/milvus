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

#include <gtest/gtest.h>

#include "knowhere/common/Exception.h"
#include "knowhere/index/vector_index/IndexBinaryIDMAP.h"

#include "Helper.h"
#include "unittest/utils.h"

using ::testing::Combine;
using ::testing::TestWithParam;
using ::testing::Values;

class BinaryIDMAPTest : public DataGen, public TestWithParam<std::string> {
 protected:
    void
    SetUp() override {
        Init_with_default(true);
        index_ = std::make_shared<milvus::knowhere::BinaryIDMAP>();
    }

    void
    TearDown() override{};

 protected:
    milvus::knowhere::BinaryIDMAPPtr index_ = nullptr;
};

INSTANTIATE_TEST_CASE_P(METRICParameters, BinaryIDMAPTest,
                        Values(std::string("JACCARD"), std::string("TANIMOTO"), std::string("HAMMING")));

TEST_P(BinaryIDMAPTest, binaryidmap_basic) {
    ASSERT_TRUE(!xb_bin.empty());

    std::string MetricType = GetParam();
    milvus::knowhere::Config conf{
        {milvus::knowhere::meta::DIM, dim},
        {milvus::knowhere::meta::TOPK, k},
        {milvus::knowhere::Metric::TYPE, MetricType},
    };

    // null faiss index
    {
        ASSERT_ANY_THROW(index_->Serialize());
        ASSERT_ANY_THROW(index_->Query(query_dataset, conf));
        ASSERT_ANY_THROW(index_->Add(nullptr, conf));
        ASSERT_ANY_THROW(index_->AddWithoutIds(nullptr, conf));
    }

    index_->Train(base_dataset, conf);
    index_->Add(base_dataset, conf);
    EXPECT_EQ(index_->Count(), nb);
    EXPECT_EQ(index_->Dim(), dim);
    ASSERT_TRUE(index_->GetRawVectors() != nullptr);
    ASSERT_TRUE(index_->GetRawIds() != nullptr);
    auto result = index_->Query(query_dataset, conf);
    AssertAnns(result, nq, k);
    // PrintResult(result, nq, k);

    auto binaryset = index_->Serialize();
    auto new_index = std::make_shared<milvus::knowhere::BinaryIDMAP>();
    new_index->Load(binaryset);
    auto result2 = new_index->Query(query_dataset, conf);
    AssertAnns(result2, nq, k);
    // PrintResult(re_result, nq, k);

    faiss::ConcurrentBitsetPtr concurrent_bitset_ptr = std::make_shared<faiss::ConcurrentBitset>(nb);
    for (int64_t i = 0; i < nq; ++i) {
        concurrent_bitset_ptr->set(i);
    }
    index_->SetBlacklist(concurrent_bitset_ptr);

    auto result_bs_1 = index_->Query(query_dataset, conf);
    AssertAnns(result_bs_1, nq, k, CheckMode::CHECK_NOT_EQUAL);

    // auto result4 = index_->SearchById(id_dataset, conf);
    // AssertAneq(result4, nq, k);
}

TEST_P(BinaryIDMAPTest, binaryidmap_serialize) {
    auto serialize = [](const std::string& filename, milvus::knowhere::BinaryPtr& bin, uint8_t* ret) {
        FileIOWriter writer(filename);
        writer(static_cast<void*>(bin->data.get()), bin->size);

        FileIOReader reader(filename);
        reader(ret, bin->size);
    };

    std::string MetricType = GetParam();
    milvus::knowhere::Config conf{
        {milvus::knowhere::meta::DIM, dim},
        {milvus::knowhere::meta::TOPK, k},
        {milvus::knowhere::Metric::TYPE, MetricType},
    };

    {
        // serialize index
        index_->Train(base_dataset, conf);
        index_->AddWithoutIds(base_dataset, milvus::knowhere::Config());
        auto re_result = index_->Query(query_dataset, conf);
        AssertAnns(re_result, nq, k);
        //        PrintResult(re_result, nq, k);
        EXPECT_EQ(index_->Count(), nb);
        EXPECT_EQ(index_->Dim(), dim);
        auto binaryset = index_->Serialize();
        auto bin = binaryset.GetByName("BinaryIVF");

        std::string filename = "/tmp/bianryidmap_test_serialize.bin";
        auto load_data = new uint8_t[bin->size];
        serialize(filename, bin, load_data);

        binaryset.clear();
        std::shared_ptr<uint8_t[]> data(load_data);
        binaryset.Append("BinaryIVF", data, bin->size);

        index_->Load(binaryset);
        EXPECT_EQ(index_->Count(), nb);
        EXPECT_EQ(index_->Dim(), dim);
        auto result = index_->Query(query_dataset, conf);
        AssertAnns(result, nq, k);
        // PrintResult(result, nq, k);
    }
}
