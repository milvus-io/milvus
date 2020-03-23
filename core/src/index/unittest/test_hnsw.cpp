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

#include <knowhere/index/vector_index/IndexHNSW.h>
#include <src/index/knowhere/knowhere/index/vector_index/helpers/IndexParameter.h>
#include <iostream>
#include <random>
#include "./utils.h"

int
main() {
    int64_t d = 64;      // dimension
    int64_t nb = 10000;  // database size
    int64_t nq = 10;     // 10000;                        // nb of queries
    faiss::ConcurrentBitsetPtr bitset = std::make_shared<faiss::ConcurrentBitset>(nb);

    int64_t* ids = new int64_t[nb];
    float* xb = new float[d * nb];
    float* xq = new float[d * nq];
    //    int64_t *ids = (int64_t*)malloc(nb * sizeof(int64_t));
    //    float* xb = (float*)malloc(d * nb * sizeof(float));
    //    float* xq = (float*)malloc(d * nq * sizeof(float));

    for (int i = 0; i < nb; i++) {
        for (int j = 0; j < d; j++) xb[d * i + j] = drand48();
        xb[d * i] += i / 1000.;
        ids[i] = i;
    }
    printf("gen xb and ids done! \n");

    //    srand((unsigned)time(NULL));
    auto random_seed = (unsigned)time(NULL);
    printf("delete ids: \n");
    for (int i = 0; i < nq; i++) {
        auto tmp = rand_r(&random_seed) % nb;
        printf("%ld\n", tmp);
        //        std::cout << "before delete, test result: " << bitset->test(tmp) << std::endl;
        bitset->set(tmp);
        //        std::cout << "after delete, test result: " << bitset->test(tmp) << std::endl;
        for (int j = 0; j < d; j++) xq[d * i + j] = xb[d * tmp + j];
        //        xq[d * i] += i / 1000.;
    }
    printf("\n");

    int k = 4;
    int m = 16;
    int ef = 200;
    milvus::knowhere::IndexHNSW index;
    milvus::knowhere::DatasetPtr base_dataset = generate_dataset(nb, d, (const void*)xb, ids);
    /*
    base_dataset->Set(milvus::knowhere::meta::ROWS, nb);
    base_dataset->Set(milvus::knowhere::meta::DIM, d);
    base_dataset->Set(milvus::knowhere::meta::TENSOR, (const void*)xb);
    base_dataset->Set(milvus::knowhere::meta::IDS, (const int64_t*)ids);
    */

    milvus::knowhere::Config base_conf{
        {milvus::knowhere::meta::DIM, d},
        {milvus::knowhere::meta::TOPK, k},
        {milvus::knowhere::IndexParams::M, m},
        {milvus::knowhere::IndexParams::efConstruction, ef},
        {milvus::knowhere::Metric::TYPE, milvus::knowhere::Metric::L2},
    };
    milvus::knowhere::DatasetPtr query_dataset = generate_query_dataset(nq, d, (const void*)xq);
    milvus::knowhere::Config query_conf{
        {milvus::knowhere::meta::DIM, d},
        {milvus::knowhere::meta::TOPK, k},
        {milvus::knowhere::IndexParams::M, m},
        {milvus::knowhere::IndexParams::ef, ef},
        {milvus::knowhere::Metric::TYPE, milvus::knowhere::Metric::L2},
    };

    index.Train(base_dataset, base_conf);
    index.Add(base_dataset, base_conf);

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
