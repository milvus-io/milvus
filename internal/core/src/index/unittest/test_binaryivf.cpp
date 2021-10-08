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
#include <iostream>
#include <thread>

#include "knowhere/common/Exception.h"
#include "knowhere/common/Timer.h"
#include "knowhere/index/vector_index/IndexBinaryIVF.h"
#include "knowhere/index/vector_index/adapter/VectorAdapter.h"
#include "unittest/Helper.h"
#include "unittest/utils.h"

using ::testing::Combine;
using ::testing::TestWithParam;
using ::testing::Values;

class BinaryIVFTest : public DataGen, public TestWithParam<std::string> {
 protected:
    void
    SetUp() override {
        std::string MetricType = GetParam();
        Init_with_default(true);
        //        nb = 1000000;
        //        nq = 1000;
        //        k = 1000;
        //        Generate(DIM, NB, NQ);
        index_ = std::make_shared<milvus::knowhere::BinaryIVF>();

        milvus::knowhere::Config temp_conf{
            {milvus::knowhere::meta::DIM, dim},           {milvus::knowhere::meta::TOPK, k},
            {milvus::knowhere::IndexParams::nlist, 100},  {milvus::knowhere::IndexParams::nprobe, 10},
            {milvus::knowhere::Metric::TYPE, MetricType},
        };
        conf = temp_conf;
    }

    void
    TearDown() override {
    }

 protected:
    std::string index_type;
    milvus::knowhere::Config conf;
    milvus::knowhere::BinaryIVFIndexPtr index_ = nullptr;
};

INSTANTIATE_TEST_CASE_P(METRICParameters,
                        BinaryIVFTest,
                        Values(std::string("JACCARD"), std::string("TANIMOTO"), std::string("HAMMING")));

TEST_P(BinaryIVFTest, binaryivf_basic) {
    assert(!xb_bin.empty());

    // null faiss index
    {
        ASSERT_ANY_THROW(index_->Serialize(conf));
        ASSERT_ANY_THROW(index_->Query(query_dataset, conf, nullptr));
        ASSERT_ANY_THROW(index_->AddWithoutIds(nullptr, conf));
    }

    index_->BuildAll(base_dataset, conf);
    EXPECT_EQ(index_->Count(), nb);
    EXPECT_EQ(index_->Dim(), dim);

    auto result = index_->Query(query_dataset, conf, nullptr);
    AssertAnns(result, nq, conf[milvus::knowhere::meta::TOPK]);
    // PrintResult(result, nq, k);

    faiss::ConcurrentBitsetPtr concurrent_bitset_ptr = std::make_shared<faiss::ConcurrentBitset>(nb);
    for (int64_t i = 0; i < nq; ++i) {
        concurrent_bitset_ptr->set(i);
    }

    auto result2 = index_->Query(query_dataset, conf, concurrent_bitset_ptr);
    AssertAnns(result2, nq, k, CheckMode::CHECK_NOT_EQUAL);

#if 0
    auto result3 = index_->QueryById(id_dataset, conf, nullptr);
    AssertAnns(result3, nq, k, CheckMode::CHECK_NOT_EQUAL);

    auto result4 = index_->GetVectorById(xid_dataset, conf);
    AssertBinVeceq(result4, base_dataset, xid_dataset, nq, dim/8);
#endif
}

TEST_P(BinaryIVFTest, binaryivf_serialize) {
    auto serialize = [](const std::string& filename, milvus::knowhere::BinaryPtr& bin, uint8_t* ret) {
        FileIOWriter writer(filename);
        writer(static_cast<void*>(bin->data.get()), bin->size);

        FileIOReader reader(filename);
        reader(ret, bin->size);
    };

    // {
    //     // serialize index-model
    //     auto model = index_->Train(base_dataset, conf);
    //     auto binaryset = model->Serialize();
    //     auto bin = binaryset.GetByName("BinaryIVF");
    //
    //     std::string filename = "/tmp/binaryivf_test_model_serialize.bin";
    //     auto load_data = new uint8_t[bin->size];
    //     serialize(filename, bin, load_data);
    //
    //     binaryset.clear();
    //     auto data = std::make_shared<uint8_t>();
    //     data.reset(load_data);
    //     binaryset.Append("BinaryIVF", data, bin->size);
    //
    //     model->Load(binaryset);
    //
    //     index_->set_index_model(model);
    //     index_->Add(base_dataset, conf);
    //     auto result = index_->Query(query_dataset, conf);
    //     AssertAnns(result, nq, conf[milvus::knowhere::meta::TOPK]);
    // }

    {
        // serialize index
        index_->BuildAll(base_dataset, conf);
        //        index_->set_index_model(model);
        //        index_->Add(base_dataset, conf);
        auto binaryset = index_->Serialize(conf);
        auto bin = binaryset.GetByName("BinaryIVF");

        std::string filename = "/tmp/binaryivf_test_serialize.bin";
        auto load_data = new uint8_t[bin->size];
        serialize(filename, bin, load_data);

        binaryset.clear();
        std::shared_ptr<uint8_t[]> data(load_data);
        binaryset.Append("BinaryIVF", data, bin->size);

        index_->Load(binaryset);
        EXPECT_EQ(index_->Count(), nb);
        EXPECT_EQ(index_->Dim(), dim);
        auto result = index_->Query(query_dataset, conf, nullptr);
        AssertAnns(result, nq, conf[milvus::knowhere::meta::TOPK]);
        // PrintResult(result, nq, k);
    }
}

TEST_P(BinaryIVFTest, binaryivf_slice) {
    {
        // serialize index
        index_->BuildAll(base_dataset, conf);
        auto binaryset = index_->Serialize(conf);

        index_->Load(binaryset);
        EXPECT_EQ(index_->Count(), nb);
        EXPECT_EQ(index_->Dim(), dim);
        auto result = index_->Query(query_dataset, conf, nullptr);
        AssertAnns(result, nq, conf[milvus::knowhere::meta::TOPK]);
        // PrintResult(result, nq, k);
    }
}
