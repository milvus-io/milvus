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
#include <knowhere/index/vector_index/IndexRHNSWFlat.h>
#include "knowhere/index/vector_index/helpers/IndexParameter.h"
#include <iostream>
#include <random>
#include "knowhere/common/Exception.h"
#include "unittest/utils.h"

using ::testing::Combine;
using ::testing::TestWithParam;
using ::testing::Values;

class RHNSWFlatTest : public DataGen, public TestWithParam<std::string> {
 protected:
    void
    SetUp() override {
        IndexType = GetParam();
        std::cout << "IndexType from GetParam() is: " << IndexType << std::endl;
        Generate(64, 10000, 10);  // dim = 64, nb = 10000, nq = 10
        index_ = std::make_shared<milvus::knowhere::IndexRHNSWFlat>();
        conf = milvus::knowhere::Config{
            {milvus::knowhere::meta::DIM, 64},
            {milvus::knowhere::meta::TOPK, 10},
            {milvus::knowhere::IndexParams::M, 16},
            {milvus::knowhere::IndexParams::efConstruction, 200},
            {milvus::knowhere::IndexParams::ef, 200},
            {milvus::knowhere::Metric::TYPE, milvus::knowhere::Metric::L2},
            {milvus::knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE, 4},
        };
    }

 protected:
    milvus::knowhere::Config conf;
    std::shared_ptr<milvus::knowhere::IndexRHNSWFlat> index_ = nullptr;
    std::string IndexType;
};

INSTANTIATE_TEST_CASE_P(HNSWParameters, RHNSWFlatTest, Values("RHNSWFlat"));

TEST_P(RHNSWFlatTest, HNSW_basic) {
    assert(!xb.empty());

    index_->Train(base_dataset, conf);
    index_->AddWithoutIds(base_dataset, conf);
    EXPECT_EQ(index_->Count(), nb);
    EXPECT_EQ(index_->Dim(), dim);

    auto result1 = index_->Query(query_dataset, conf, nullptr);
    //    AssertAnns(result1, nq, k);

    // Serialize and Load before Query
    milvus::knowhere::BinarySet bs = index_->Serialize(conf);

    int64_t dim = base_dataset->Get<int64_t>(milvus::knowhere::meta::DIM);
    int64_t rows = base_dataset->Get<int64_t>(milvus::knowhere::meta::ROWS);
    auto raw_data = base_dataset->Get<const void*>(milvus::knowhere::meta::TENSOR);
    milvus::knowhere::BinaryPtr bptr = std::make_shared<milvus::knowhere::Binary>();
    bptr->data = std::shared_ptr<uint8_t[]>((uint8_t*)raw_data, [&](uint8_t*) {});
    bptr->size = dim * rows * sizeof(float);
    bs.Append(RAW_DATA, bptr);
    auto tmp_index = std::make_shared<milvus::knowhere::IndexRHNSWFlat>();

    tmp_index->Load(bs);

    auto result2 = tmp_index->Query(query_dataset, conf, nullptr);
    //    AssertAnns(result2, nq, k);
}

TEST_P(RHNSWFlatTest, HNSW_delete) {
    assert(!xb.empty());

    index_->Train(base_dataset, conf);
    index_->AddWithoutIds(base_dataset, conf);
    EXPECT_EQ(index_->Count(), nb);
    EXPECT_EQ(index_->Dim(), dim);

    faiss::ConcurrentBitsetPtr bitset = std::make_shared<faiss::ConcurrentBitset>(nb);
    for (auto i = 0; i < nq; ++i) {
        bitset->set(i);
    }

    auto result1 = index_->Query(query_dataset, conf, nullptr);
    //    AssertAnns(result1, nq, k);

    auto result2 = index_->Query(query_dataset, conf, bitset);
    //    AssertAnns(result2, nq, k, CheckMode::CHECK_NOT_EQUAL);

    /*
     * delete result checked by eyes
    auto ids1 = result1->Get<int64_t*>(milvus::knowhere::meta::IDS);
    auto ids2 = result2->Get<int64_t*>(milvus::knowhere::meta::IDS);
    std::cout << std::endl;
    for (int i = 0; i < nq; ++ i) {
        std::cout << "ids1: ";
        for (int j = 0; j < k; ++ j) {
            std::cout << *(ids1 + i * k + j) << " ";
        }
        std::cout << "ids2: ";
        for (int j = 0; j < k; ++ j) {
            std::cout << *(ids2 + i * k + j) << " ";
        }
        std::cout << std::endl;
        for (int j = 0; j < std::min(5, k>>1); ++ j) {
            ASSERT_EQ(*(ids1 + i * k + j + 1), *(ids2 + i * k + j));
        }
    }
    */
}

TEST_P(RHNSWFlatTest, HNSW_serialize) {
    auto serialize = [](const std::string& filename, milvus::knowhere::BinaryPtr& bin, uint8_t* ret) {
        {
            FileIOWriter writer(filename);
            writer(static_cast<void*>(bin->data.get()), bin->size);
        }

        FileIOReader reader(filename);
        reader(ret, bin->size);
    };

    {
        index_->Train(base_dataset, conf);
        index_->AddWithoutIds(base_dataset, conf);
        auto binaryset = index_->Serialize(conf);
        std::string index_type = index_->index_type();
        std::string idx_name = index_type + "_Index";
        std::string met_name = "META";
        if (binaryset.binary_map_.find(idx_name) == binaryset.binary_map_.end()) {
            std::cout << "no idx!" << std::endl;
        }
        if (binaryset.binary_map_.find(met_name) == binaryset.binary_map_.end()) {
            std::cout << "no met!" << std::endl;
        }
        auto bin_idx = binaryset.GetByName(idx_name);
        auto bin_met = binaryset.GetByName(met_name);

        std::string filename_idx = "/tmp/RHNSWFlat_test_serialize_idx.bin";
        std::string filename_met = "/tmp/RHNSWFlat_test_serialize_met.bin";
        auto load_idx = new uint8_t[bin_idx->size];
        auto load_met = new uint8_t[bin_met->size];
        serialize(filename_idx, bin_idx, load_idx);
        serialize(filename_met, bin_met, load_met);

        binaryset.clear();
        auto new_idx = std::make_shared<milvus::knowhere::IndexRHNSWFlat>();
        std::shared_ptr<uint8_t[]> met(load_met);
        std::shared_ptr<uint8_t[]> idx(load_idx);
        binaryset.Append(new_idx->index_type() + "_Index", idx, bin_idx->size);
        binaryset.Append("META", met, bin_met->size);

        int64_t dim = base_dataset->Get<int64_t>(milvus::knowhere::meta::DIM);
        int64_t rows = base_dataset->Get<int64_t>(milvus::knowhere::meta::ROWS);
        auto raw_data = base_dataset->Get<const void*>(milvus::knowhere::meta::TENSOR);
        milvus::knowhere::BinaryPtr bptr = std::make_shared<milvus::knowhere::Binary>();
        bptr->data = std::shared_ptr<uint8_t[]>((uint8_t*)raw_data, [&](uint8_t*) {});
        bptr->size = dim * rows * sizeof(float);
        binaryset.Append(RAW_DATA, bptr);
        new_idx->Load(binaryset);
        EXPECT_EQ(new_idx->Count(), nb);
        EXPECT_EQ(new_idx->Dim(), dim);
        auto result = new_idx->Query(query_dataset, conf, nullptr);
        //        AssertAnns(result, nq, conf[milvus::knowhere::meta::TOPK]);
    }
}

TEST_P(RHNSWFlatTest, HNSW_slice) {
    {
        index_->Train(base_dataset, conf);
        index_->AddWithoutIds(base_dataset, conf);
        auto binaryset = index_->Serialize(conf);
        auto new_idx = std::make_shared<milvus::knowhere::IndexRHNSWFlat>();
        int64_t dim = base_dataset->Get<int64_t>(milvus::knowhere::meta::DIM);
        int64_t rows = base_dataset->Get<int64_t>(milvus::knowhere::meta::ROWS);
        auto raw_data = base_dataset->Get<const void*>(milvus::knowhere::meta::TENSOR);
        milvus::knowhere::BinaryPtr bptr = std::make_shared<milvus::knowhere::Binary>();
        bptr->data = std::shared_ptr<uint8_t[]>((uint8_t*)raw_data, [&](uint8_t*) {});
        bptr->size = dim * rows * sizeof(float);
        binaryset.Append(RAW_DATA, bptr);
        new_idx->Load(binaryset);
        EXPECT_EQ(new_idx->Count(), nb);
        EXPECT_EQ(new_idx->Dim(), dim);
        auto result = new_idx->Query(query_dataset, conf, nullptr);
        //        AssertAnns(result, nq, conf[milvus::knowhere::meta::TOPK]);
    }
}
