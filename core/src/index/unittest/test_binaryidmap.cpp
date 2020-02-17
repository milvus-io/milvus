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

class BinaryIDMAPTest : public BinaryDataGen, public TestWithParam<knowhere::METRICTYPE> {
 protected:
    void
    SetUp() override {
        Init_with_binary_default();
        index_ = std::make_shared<knowhere::BinaryIDMAP>();
    }

    void
    TearDown() override{};

 protected:
    knowhere::BinaryIDMAPPtr index_ = nullptr;
};

INSTANTIATE_TEST_CASE_P(METRICParameters, BinaryIDMAPTest,
                        Values(knowhere::METRICTYPE::JACCARD, knowhere::METRICTYPE::TANIMOTO,
                               knowhere::METRICTYPE::HAMMING));

TEST_P(BinaryIDMAPTest, binaryidmap_basic) {
    ASSERT_TRUE(!xb.empty());

    knowhere::METRICTYPE MetricType = GetParam();
    auto conf = std::make_shared<knowhere::BinIDMAPCfg>();
    conf->d = dim;
    conf->k = k;
    conf->metric_type = MetricType;

    index_->Train(conf);
    index_->Add(base_dataset, conf);
    EXPECT_EQ(index_->Count(), nb);
    EXPECT_EQ(index_->Dimension(), dim);
    ASSERT_TRUE(index_->GetRawVectors() != nullptr);
    ASSERT_TRUE(index_->GetRawIds() != nullptr);
    auto result = index_->Search(query_dataset, conf);
    AssertAnns(result, nq, k);
    // PrintResult(result, nq, k);

    auto binaryset = index_->Serialize();
    auto new_index = std::make_shared<knowhere::BinaryIDMAP>();
    new_index->Load(binaryset);
    auto re_result = index_->Search(query_dataset, conf);
    AssertAnns(re_result, nq, k);
    // PrintResult(re_result, nq, k);
}

TEST_P(BinaryIDMAPTest, binaryidmap_serialize) {
    auto serialize = [](const std::string& filename, knowhere::BinaryPtr& bin, uint8_t* ret) {
        FileIOWriter writer(filename);
        writer(static_cast<void*>(bin->data.get()), bin->size);

        FileIOReader reader(filename);
        reader(ret, bin->size);
    };

    knowhere::METRICTYPE MetricType = GetParam();
    auto conf = std::make_shared<knowhere::BinIDMAPCfg>();
    conf->d = dim;
    conf->k = k;
    conf->metric_type = MetricType;

    {
        // serialize index
        index_->Train(conf);
        index_->Add(base_dataset, knowhere::Config());
        auto re_result = index_->Search(query_dataset, conf);
        AssertAnns(re_result, nq, k);
        //        PrintResult(re_result, nq, k);
        EXPECT_EQ(index_->Count(), nb);
        EXPECT_EQ(index_->Dimension(), dim);
        auto binaryset = index_->Serialize();
        auto bin = binaryset.GetByName("BinaryIVF");

        std::string filename = "/tmp/bianryidmap_test_serialize.bin";
        auto load_data = new uint8_t[bin->size];
        serialize(filename, bin, load_data);

        binaryset.clear();
        auto data = std::make_shared<uint8_t>();
        data.reset(load_data);
        binaryset.Append("BinaryIVF", data, bin->size);

        index_->Load(binaryset);
        EXPECT_EQ(index_->Count(), nb);
        EXPECT_EQ(index_->Dimension(), dim);
        auto result = index_->Search(query_dataset, conf);
        AssertAnns(result, nq, k);
        //        PrintResult(result, nq, k);
    }
}
