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
#include "knowhere/index/vector_index/helpers/IndexParameter.h"
#include <iostream>
#include <sstream>

#include "knowhere/common/Exception.h"
#include "knowhere/index/vector_index/IndexNGTPANNG.h"

#include "unittest/utils.h"

using ::testing::Combine;
using ::testing::TestWithParam;
using ::testing::Values;

class NGTPANNGTest : public DataGen, public TestWithParam<std::string> {
 protected:
    void
    SetUp() override {
        IndexType = GetParam();
        Generate(128, 10000, 10);
        index_ = std::make_shared<milvus::knowhere::IndexNGTPANNG>();
        conf = milvus::knowhere::Config{
            {milvus::knowhere::meta::DIM, dim},
            {milvus::knowhere::meta::TOPK, 10},
            {milvus::knowhere::Metric::TYPE, milvus::knowhere::Metric::L2},
            {milvus::knowhere::IndexParams::edge_size, 10},
            {milvus::knowhere::IndexParams::epsilon, 0.1},
            {milvus::knowhere::IndexParams::max_search_edges, 50},
            {milvus::knowhere::IndexParams::forcedly_pruned_edge_size, 60},
            {milvus::knowhere::IndexParams::selectively_pruned_edge_size, 30},
            {milvus::knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE, 4},
        };
    }

 protected:
    milvus::knowhere::Config conf;
    std::shared_ptr<milvus::knowhere::IndexNGTPANNG> index_ = nullptr;
    std::string IndexType;
};

INSTANTIATE_TEST_CASE_P(NGTPANNGParameters, NGTPANNGTest, Values("NGTPANNG"));

TEST_P(NGTPANNGTest, ngtpanng_basic) {
    assert(!xb.empty());

    // null index
    {
        ASSERT_ANY_THROW(index_->Train(base_dataset, conf));
        ASSERT_ANY_THROW(index_->Query(query_dataset, conf, nullptr));
        ASSERT_ANY_THROW(index_->Serialize(conf));
        ASSERT_ANY_THROW(index_->AddWithoutIds(base_dataset, conf));
        ASSERT_ANY_THROW(index_->Count());
        ASSERT_ANY_THROW(index_->Dim());
    }

    index_->BuildAll(base_dataset, conf);
    ASSERT_EQ(index_->Count(), nb);
    ASSERT_EQ(index_->Dim(), dim);

    auto result = index_->Query(query_dataset, conf, nullptr);
    AssertAnns(result, nq, k);
}

TEST_P(NGTPANNGTest, ngtpanng_delete) {
    assert(!xb.empty());

    index_->BuildAll(base_dataset, conf);
    ASSERT_EQ(index_->Count(), nb);
    ASSERT_EQ(index_->Dim(), dim);

    faiss::ConcurrentBitsetPtr bitset = std::make_shared<faiss::ConcurrentBitset>(nb);
    for (auto i = 0; i < nq; ++i) {
        bitset->set(i);
    }

    auto result1 = index_->Query(query_dataset, conf, nullptr);
    AssertAnns(result1, nq, k);

    auto result2 = index_->Query(query_dataset, conf, bitset);
    AssertAnns(result2, nq, k, CheckMode::CHECK_NOT_EQUAL);
}

TEST_P(NGTPANNGTest, ngtpanng_serialize) {
    auto serialize = [](const std::string& filename, milvus::knowhere::BinaryPtr& bin, uint8_t* ret) {
        {
            // write and flush
            FileIOWriter writer(filename);
            writer(static_cast<void*>(bin->data.get()), bin->size);
        }

        FileIOReader reader(filename);
        reader(ret, bin->size);
    };

    {
        // serialize index
        index_->BuildAll(base_dataset, conf);
        auto binaryset = index_->Serialize(milvus::knowhere::Config());

        auto bin_obj_data = binaryset.GetByName("ngt_obj_data");
        std::string filename1 = "/tmp/ngt_obj_data_serialize.bin";
        auto load_data1 = new uint8_t[bin_obj_data->size];
        serialize(filename1, bin_obj_data, load_data1);

        auto bin_grp_data = binaryset.GetByName("ngt_grp_data");
        std::string filename2 = "/tmp/ngt_grp_data_serialize.bin";
        auto load_data2 = new uint8_t[bin_grp_data->size];
        serialize(filename2, bin_grp_data, load_data2);

        auto bin_prf_data = binaryset.GetByName("ngt_prf_data");
        std::string filename3 = "/tmp/ngt_prf_data_serialize.bin";
        auto load_data3 = new uint8_t[bin_prf_data->size];
        serialize(filename3, bin_prf_data, load_data3);

        auto bin_tre_data = binaryset.GetByName("ngt_tre_data");
        std::string filename4 = "/tmp/ngt_tre_data_serialize.bin";
        auto load_data4 = new uint8_t[bin_tre_data->size];
        serialize(filename4, bin_tre_data, load_data4);

        binaryset.clear();
        std::shared_ptr<uint8_t[]> obj_data(load_data1);
        binaryset.Append("ngt_obj_data", obj_data, bin_obj_data->size);

        std::shared_ptr<uint8_t[]> grp_data(load_data2);
        binaryset.Append("ngt_grp_data", grp_data, bin_grp_data->size);

        std::shared_ptr<uint8_t[]> prf_data(load_data3);
        binaryset.Append("ngt_prf_data", prf_data, bin_prf_data->size);

        std::shared_ptr<uint8_t[]> tre_data(load_data4);
        binaryset.Append("ngt_tre_data", tre_data, bin_tre_data->size);

        index_->Load(binaryset);
        ASSERT_EQ(index_->Count(), nb);
        ASSERT_EQ(index_->Dim(), dim);
        auto result = index_->Query(query_dataset, conf, nullptr);
        AssertAnns(result, nq, conf[milvus::knowhere::meta::TOPK]);
    }
}

TEST_P(NGTPANNGTest, ngtpanng_slice) {
    {
        // serialize index
        index_->BuildAll(base_dataset, conf);
        auto binaryset = index_->Serialize(milvus::knowhere::Config());

        index_->Load(binaryset);
        ASSERT_EQ(index_->Count(), nb);
        ASSERT_EQ(index_->Dim(), dim);
        auto result = index_->Query(query_dataset, conf, nullptr);
        AssertAnns(result, nq, conf[milvus::knowhere::meta::TOPK]);
    }
}
