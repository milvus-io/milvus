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

#include <faiss/AutoTune.h>
#include <faiss/Index.h>
#include <faiss/gpu/GpuAutoTune.h>
#include <faiss/gpu/GpuIndexIVFFlat.h>
#include <faiss/gpu/StandardGpuResources.h>
#include <faiss/index_io.h>

#include <chrono>
#include <iostream>
#include <thread>

class TestGpuRes {
 public:
    TestGpuRes() {
        res_ = new faiss::gpu::StandardGpuResources;
    }

    ~TestGpuRes() {
        delete res_;
        delete index_;
    }

    std::shared_ptr<faiss::Index>
    Do() {
        int d = 128;      // dimension
        int nb = 100000;  // database size
        int nq = 100;     // nb of queries
        int nlist = 1638;

        float* xb = new float[d * nb];
        float* xq = new float[d * nq];

        for (int i = 0; i < nb; i++) {
            for (int j = 0; j < d; j++) xb[d * i + j] = drand48();
            xb[d * i] += i / 1000.;
        }

        for (int i = 0; i < nq; i++) {
            for (int j = 0; j < d; j++) xq[d * i + j] = drand48();
            xq[d * i] += i / 1000.;
        }

        index_ = new faiss::gpu::GpuIndexIVFFlat(res_, d, nlist, faiss::METRIC_L2);
        index_->train(nb, xb);
        index_->add(nb, xb);

        std::shared_ptr<faiss::Index> host_index = nullptr;
        host_index.reset(faiss::gpu::index_gpu_to_cpu(index_));
        return host_index;
    }

 private:
    faiss::gpu::GpuResources* res_ = nullptr;
    faiss::Index* index_ = nullptr;
};

TEST(gpuresource, resource) {
    TestGpuRes t;
    t.Do();
}

TEST(test, resource_re) {
    int d = 128;       // dimension
    int nb = 1000000;  // database size
    int nq = 100;      // nb of queries
    int nlist = 16384;
    int k = 100;

    float* xb = new float[d * nb];
    float* xq = new float[d * nq];

    for (int i = 0; i < nb; i++) {
        for (int j = 0; j < d; j++) xb[d * i + j] = drand48();
        xb[d * i] += i / 1000.;
    }

    for (int i = 0; i < nq; i++) {
        for (int j = 0; j < d; j++) xq[d * i + j] = drand48();
        xq[d * i] += i / 1000.;
    }

    auto elems = nq * k;
    auto res_ids = (int64_t*)malloc(sizeof(int64_t) * elems);
    auto res_dis = (float*)malloc(sizeof(float) * elems);

    faiss::gpu::StandardGpuResources res;
    auto cpu_index = faiss::index_factory(d, "IVF16384, Flat");
    auto device_index = faiss::gpu::index_cpu_to_gpu(&res, 0, cpu_index);
    device_index->train(nb, xb);
    device_index->add(nb, xb);
    auto new_index = faiss::gpu::index_gpu_to_cpu(device_index);

    delete device_index;

    std::cout << "start clone" << std::endl;
    auto load = [&] {
        std::cout << "start" << std::endl;
        faiss::gpu::StandardGpuResources res;
        // res.noTempMemory();
        for (int l = 0; l < 100; ++l) {
            auto x = faiss::gpu::index_cpu_to_gpu(&res, 1, new_index);
            delete x;
        }
        std::cout << "load finish" << std::endl;
    };

    auto search = [&] {
        faiss::gpu::StandardGpuResources res;
        auto device_index = faiss::gpu::index_cpu_to_gpu(&res, 1, new_index);
        std::cout << "search start" << std::endl;
        for (int l = 0; l < 10000; ++l) {
            device_index->search(nq, xq, 10, res_dis, res_ids);
        }
        std::cout << "search finish" << std::endl;
        delete device_index;
        delete cpu_index;
    };

    load();
    search();
    std::thread t1(search);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    std::thread t2(load);
    t1.join();
    t2.join();
    std::cout << "finish clone" << std::endl;

    // std::this_thread::sleep_for(5s);
    //
    // auto device_index_2 = faiss::gpu::index_cpu_to_gpu(&res, 1, cpu_index);
    // device_index->train(nb, xb);
    // device_index->add(nb, xb);

    // std::cout << "finish clone" << std::endl;
    // std::this_thread::sleep_for(5s);

    // std::this_thread::sleep_for(2s);
    // std::cout << "start clone" << std::endl;
    // auto new_index = faiss::clone_index(device_index);
    // std::cout << "start search" << std::endl;
    // new_index->search(nq, xq, k, res_dis, res_ids);

    // std::cout << "start clone" << std::endl;
    //{
    //    faiss::gpu::StandardGpuResources res;
    //    auto cpu_index = faiss::index_factory(d, "IVF1638, Flat");
    //    auto device_index = faiss::gpu::index_cpu_to_gpu(&res, 1, cpu_index);
    //    device_index->train(nb, xb);
    //    device_index->add(nb, xb);
    //    std::cout << "finish clone" << std::endl;
    //    delete device_index;
    //    delete cpu_index;
    //    std::cout << "finish clone" << std::endl;
    //}
    //
    // std::cout << "finish clone" << std::endl;
}
