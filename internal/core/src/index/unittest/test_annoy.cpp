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
#include "knowhere/index/vector_index/IndexAnnoy.h"

#include "unittest/utils.h"

using ::testing::Combine;
using ::testing::TestWithParam;
using ::testing::Values;

class AnnoyTest : public DataGen, public TestWithParam<std::string> {
 protected:
    void
    SetUp() override {
        IndexType = GetParam();
        Generate(128, 10000, 10);
        index_ = std::make_shared<milvus::knowhere::IndexAnnoy>();
        conf = milvus::knowhere::Config{
            {milvus::knowhere::meta::DIM, dim},
            {milvus::knowhere::meta::TOPK, 10},
            {milvus::knowhere::IndexParams::n_trees, 4},
            {milvus::knowhere::IndexParams::search_k, 100},
            {milvus::knowhere::Metric::TYPE, milvus::knowhere::Metric::L2},
            {milvus::knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE, 4},
        };
    }

 protected:
    milvus::knowhere::Config conf;
    std::shared_ptr<milvus::knowhere::IndexAnnoy> index_ = nullptr;
    std::string IndexType;
};

INSTANTIATE_TEST_CASE_P(AnnoyParameters, AnnoyTest, Values("Annoy"));

TEST_P(AnnoyTest, annoy_basic) {
    assert(!xb.empty());

    // null faiss index
    {
        ASSERT_ANY_THROW(index_->Train(base_dataset, conf));
        ASSERT_ANY_THROW(index_->Query(query_dataset, conf, nullptr));
        ASSERT_ANY_THROW(index_->Serialize(conf));
        ASSERT_ANY_THROW(index_->AddWithoutIds(base_dataset, conf));
        ASSERT_ANY_THROW(index_->Count());
        ASSERT_ANY_THROW(index_->Dim());
    }

    index_->BuildAll(base_dataset, conf);  // Train + AddWithoutIds
    ASSERT_EQ(index_->Count(), nb);
    ASSERT_EQ(index_->Dim(), dim);

    auto result = index_->Query(query_dataset, conf, nullptr);
    AssertAnns(result, nq, k);

    /*
     * output result to check by eyes
    {
        auto ids = result->Get<int64_t*>(milvus::knowhere::meta::IDS);
        auto dist = result->Get<float*>(milvus::knowhere::meta::DISTANCE);

        std::stringstream ss_id;
        std::stringstream ss_dist;
        for (auto i = 0; i < nq; i++) {
            for (auto j = 0; j < k; ++j) {
                // ss_id << *ids->data()->GetValues<int64_t>(1, i * k + j) << " ";
                // ss_dist << *dists->data()->GetValues<float>(1, i * k + j) << " ";
                ss_id << *((int64_t*)(ids) + i * k + j) << " ";
                ss_dist << *((float*)(dist) + i * k + j) << " ";
            }
            ss_id << std::endl;
            ss_dist << std::endl;
        }
        std::cout << "id\n" << ss_id.str() << std::endl;
        std::cout << "dist\n" << ss_dist.str() << std::endl;
    }
    */
}

TEST_P(AnnoyTest, annoy_delete) {
    assert(!xb.empty());

    index_->BuildAll(base_dataset, conf);  // Train + AddWithoutIds
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
        std::cout << " ids2: ";
        for (int j = 0; j < k; ++ j) {
            std::cout << *(ids2 + i * k + j) << " ";
        }
        std::cout << std::endl;
        for (int j = 0; j < std::min(5, k>>1); ++ j) {
            ASSERT_EQ(*(ids1 + i * k + j + 1), *(ids2 + i * k + j));
        }
    }
    */
    /*
     * output result to check by eyes
    {
        auto ids = result->Get<int64_t*>(milvus::knowhere::meta::IDS);
        auto dist = result->Get<float*>(milvus::knowhere::meta::DISTANCE);

        std::stringstream ss_id;
        std::stringstream ss_dist;
        for (auto i = 0; i < nq; i++) {
            for (auto j = 0; j < k; ++j) {
                // ss_id << *ids->data()->GetValues<int64_t>(1, i * k + j) << " ";
                // ss_dist << *dists->data()->GetValues<float>(1, i * k + j) << " ";
                ss_id << *((int64_t*)(ids) + i * k + j) << " ";
                ss_dist << *((float*)(dist) + i * k + j) << " ";
            }
            ss_id << std::endl;
            ss_dist << std::endl;
        }
        std::cout << "id\n" << ss_id.str() << std::endl;
        std::cout << "dist\n" << ss_dist.str() << std::endl;
    }
    */
}

TEST_P(AnnoyTest, annoy_serialize) {
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

        auto bin_data = binaryset.GetByName("annoy_index_data");
        std::string filename1 = "/tmp/annoy_test_data_serialize.bin";
        auto load_data1 = new uint8_t[bin_data->size];
        serialize(filename1, bin_data, load_data1);

        auto bin_metric_type = binaryset.GetByName("annoy_metric_type");
        std::string filename2 = "/tmp/annoy_test_metric_type_serialize.bin";
        auto load_data2 = new uint8_t[bin_metric_type->size];
        serialize(filename2, bin_metric_type, load_data2);

        auto bin_dim = binaryset.GetByName("annoy_dim");
        std::string filename3 = "/tmp/annoy_test_dim_serialize.bin";
        auto load_data3 = new uint8_t[bin_dim->size];
        serialize(filename3, bin_dim, load_data3);

        binaryset.clear();
        std::shared_ptr<uint8_t[]> index_data(load_data1);
        binaryset.Append("annoy_index_data", index_data, bin_data->size);

        std::shared_ptr<uint8_t[]> metric_data(load_data2);
        binaryset.Append("annoy_metric_type", metric_data, bin_metric_type->size);

        std::shared_ptr<uint8_t[]> dim_data(load_data3);
        binaryset.Append("annoy_dim", dim_data, bin_dim->size);

        index_->Load(binaryset);
        ASSERT_EQ(index_->Count(), nb);
        ASSERT_EQ(index_->Dim(), dim);
        auto result = index_->Query(query_dataset, conf, nullptr);
        AssertAnns(result, nq, conf[milvus::knowhere::meta::TOPK]);
    }
}

TEST_P(AnnoyTest, annoy_slice) {
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

/*
 * faiss style test
 * keep it
int
main() {
    int64_t d = 64;      // dimension
    int64_t nb = 10000;  // database size
    int64_t nq = 10;     // 10000;                        // nb of queries
    faiss::ConcurrentBitsetPtr bitset = std::make_shared<faiss::ConcurrentBitset>(nb);

    int64_t* ids = new int64_t[nb];
    float* xb = new float[d * nb];
    float* xq = new float[d * nq];

    for (int i = 0; i < nb; i++) {
        for (int j = 0; j < d; j++) xb[d * i + j] = (float)drand48();
        xb[d * i] += i / 1000.;
        ids[i] = i;
    }
    printf("gen xb and ids done! \n");

    //    srand((unsigned)time(nullptr));
    auto random_seed = (unsigned)time(nullptr);
    printf("delete ids: \n");
    for (int i = 0; i < nq; i++) {
        auto tmp = rand_r(&random_seed) % nb;
        printf("%d\n", tmp);
        //        std::cout << "before delete, test result: " << bitset->test(tmp) << std::endl;
        bitset->set(tmp);
        //        std::cout << "after delete, test result: " << bitset->test(tmp) << std::endl;
        for (int j = 0; j < d; j++) xq[d * i + j] = xb[d * tmp + j];
        //        xq[d * i] += i / 1000.;
    }
    printf("\n");

    int k = 4;
    int n_trees = 5;
    int search_k = 100;
    milvus::knowhere::IndexAnnoy index;
    milvus::knowhere::DatasetPtr base_dataset = generate_dataset(nb, d, (const void*)xb, ids);

    milvus::knowhere::Config base_conf{
        {milvus::knowhere::meta::DIM, d},
        {milvus::knowhere::meta::TOPK, k},
        {milvus::knowhere::IndexParams::n_trees, n_trees},
        {milvus::knowhere::Metric::TYPE, milvus::knowhere::Metric::L2},
    };
    milvus::knowhere::DatasetPtr query_dataset = generate_query_dataset(nq, d, (const void*)xq);
    milvus::knowhere::Config query_conf{
        {milvus::knowhere::meta::DIM, d},
        {milvus::knowhere::meta::TOPK, k},
        {milvus::knowhere::IndexParams::search_k, search_k},
    };

    index.BuildAll(base_dataset, base_conf);

    printf("------------sanity check----------------\n");
    {  // sanity check
        auto res = index.Query(query_dataset, query_conf);
        printf("Query done!\n");
        const int64_t* I = res->Get<int64_t*>(milvus::knowhere::meta::IDS);
        float* D = res->Get<float*>(milvus::knowhere::meta::DISTANCE);

        printf("I=\n");
        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < k; j++) printf("%5ld ", I[i * k + j]);
            printf("\n");
        }

        printf("D=\n");
        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < k; j++) printf("%7g ", D[i * k + j]);
            printf("\n");
        }
    }

    printf("---------------search xq-------------\n");
    {  // search xq
        auto res = index.Query(query_dataset, query_conf);
        const int64_t* I = res->Get<int64_t*>(milvus::knowhere::meta::IDS);

        printf("I=\n");
        for (int i = 0; i < nq; i++) {
            for (int j = 0; j < k; j++) printf("%5ld ", I[i * k + j]);
            printf("\n");
        }
    }

    printf("----------------search xq with delete------------\n");
    {  // search xq with delete
        auto res = index.Query(query_dataset, query_conf, bitset);
        auto I = res->Get<int64_t*>(milvus::knowhere::meta::IDS);

        printf("I=\n");
        for (int i = 0; i < nq; i++) {
            for (int j = 0; j < k; j++) printf("%5ld ", I[i * k + j]);
            printf("\n");
        }
    }

    delete[] xb;
    delete[] xq;
    delete[] ids;

    return 0;
}
*/
