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
#include <src/index/knowhere/knowhere/index/vector_index/helpers/IndexParameter.h>
#include <iostream>
#include <sstream>

#include "knowhere/common/Exception.h"
#include "knowhere/index/vector_index/IndexAnnoy.h"

#include "unittest/utils.h"

using ::testing::Combine;
using ::testing::TestWithParam;
using ::testing::Values;

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

    //    srand((unsigned)time(NULL));
    auto random_seed = (unsigned)time(NULL);
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
        index.SetBlacklist(bitset);
        auto res = index.Query(query_dataset, query_conf);
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

/*
class AnnoyTest : public DataGen, public TestWithParam<std::string> {
 protected:
    void
    SetUp() override {
        IndexType = GetParam();
        std::cout << "IndexType from GetParam() is: " << IndexType << std::endl;
        Generate(128, 1000, 5);
        index_ = std::make_shared<milvus::knowhere::IndexAnnoy>();
        conf = milvus::knowhere::Config{
            {milvus::knowhere::meta::DIM, dim},
            {milvus::knowhere::meta::TOPK, 1},
            {milvus::knowhere::IndexParams::n_trees, 4},
            {milvus::knowhere::IndexParams::search_k, 100},
            {milvus::knowhere::Metric::TYPE, milvus::knowhere::Metric::L2},
        };

//        Init_with_default();
    }

 protected:
    milvus::knowhere::Config conf;
    std::shared_ptr<milvus::knowhere::IndexAnnoy> index_ = nullptr;
    std::string IndexType;
};

INSTANTIATE_TEST_CASE_P(AnnoyParameters, AnnoyTest, Values(""));

TEST_P(AnnoyTest, annoy_basic) {
    assert(!xb.empty());

//    index_->Train(base_dataset, conf);
    index_->BuildAll(base_dataset, conf);
    auto result = index_->Query(query_dataset, conf);
    AssertAnns(result, nq, k);

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
}

TEST_P(AnnoyTest, annoy_delete) {
    assert(!xb.empty());

//    index_->Train(base_dataset, conf);
    index_->BuildAll(base_dataset, conf);
    // index_->Add(base_dataset, conf);
    faiss::ConcurrentBitsetPtr bitset = std::make_shared<faiss::ConcurrentBitset>(nb);
    for (auto i = 0; i < nq; ++ i) {
        bitset->set(i);

    auto result = index_->Query(query_dataset, conf);
    AssertAnns(result, nq, k);

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
    }   }
}
*/
