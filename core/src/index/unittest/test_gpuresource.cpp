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

#include <faiss/AutoTune.h>
#include <faiss/gpu/GpuAutoTune.h>
#include <faiss/gpu/GpuIndexIVFFlat.h>

#include "knowhere/common/Exception.h"
#include "knowhere/common/Timer.h"
#include "knowhere/index/vector_index/IndexGPUIVF.h"
#include "knowhere/index/vector_index/IndexGPUIVFPQ.h"
#include "knowhere/index/vector_index/IndexGPUIVFSQ.h"
#include "knowhere/index/vector_index/IndexIVF.h"
#include "knowhere/index/vector_index/IndexIVFPQ.h"
#include "knowhere/index/vector_index/IndexIVFSQ.h"
#include "knowhere/index/vector_index/IndexIVFSQHybrid.h"
#include "knowhere/index/vector_index/helpers/Cloner.h"

#include "unittest/Helper.h"
#include "unittest/utils.h"

class GPURESTEST : public DataGen, public TestGpuIndexBase {
 protected:
    void
    SetUp() override {
        TestGpuIndexBase::SetUp();
        Generate(DIM, NB, NQ);

        k = K;
        elems = nq * k;
        ids = (int64_t*)malloc(sizeof(int64_t) * elems);
        dis = (float*)malloc(sizeof(float) * elems);
    }

    void
    TearDown() override {
        delete ids;
        delete dis;
        TestGpuIndexBase::TearDown();
    }

 protected:
    std::string index_type;
    knowhere::IVFIndexPtr index_ = nullptr;

    int64_t* ids = nullptr;
    float* dis = nullptr;
    int64_t elems = 0;
};

TEST_F(GPURESTEST, copyandsearch) {
    // search and copy at the same time
    printf("==================\n");

    index_type = "GPUIVF";
    index_ = IndexFactory(index_type);

    auto conf = ParamGenerator::GetInstance().Gen(ParameterType::ivf);
    auto preprocessor = index_->BuildPreprocessor(base_dataset, conf);
    index_->set_preprocessor(preprocessor);
    auto model = index_->Train(base_dataset, conf);
    index_->set_index_model(model);
    index_->Add(base_dataset, conf);
    auto result = index_->Search(query_dataset, conf);
    AssertAnns(result, nq, k);

    auto cpu_idx = knowhere::cloner::CopyGpuToCpu(index_, knowhere::Config());
    cpu_idx->Seal();
    auto search_idx = knowhere::cloner::CopyCpuToGpu(cpu_idx, DEVICEID, knowhere::Config());

    constexpr int64_t search_count = 50;
    constexpr int64_t load_count = 15;
    auto search_func = [&] {
        // TimeRecorder tc("search&load");
        for (int i = 0; i < search_count; ++i) {
            search_idx->Search(query_dataset, conf);
            // if (i > search_count - 6 || i == 0)
            //    tc.RecordSection("search once");
        }
        // tc.ElapseFromBegin("search finish");
    };
    auto load_func = [&] {
        // TimeRecorder tc("search&load");
        for (int i = 0; i < load_count; ++i) {
            knowhere::cloner::CopyCpuToGpu(cpu_idx, DEVICEID, knowhere::Config());
            // if (i > load_count -5 || i < 5)
            // tc.RecordSection("Copy to gpu");
        }
        // tc.ElapseFromBegin("load finish");
    };

    knowhere::TimeRecorder tc("Basic");
    knowhere::cloner::CopyCpuToGpu(cpu_idx, DEVICEID, knowhere::Config());
    tc.RecordSection("Copy to gpu once");
    search_idx->Search(query_dataset, conf);
    tc.RecordSection("Search once");
    search_func();
    tc.RecordSection("Search total cost");
    load_func();
    tc.RecordSection("Copy total cost");

    std::thread search_thread(search_func);
    std::thread load_thread(load_func);
    search_thread.join();
    load_thread.join();
    tc.RecordSection("Copy&Search total");
}

TEST_F(GPURESTEST, trainandsearch) {
    index_type = "GPUIVF";
    index_ = IndexFactory(index_type);

    auto conf = ParamGenerator::GetInstance().Gen(ParameterType::ivf);
    auto preprocessor = index_->BuildPreprocessor(base_dataset, conf);
    index_->set_preprocessor(preprocessor);
    auto model = index_->Train(base_dataset, conf);
    auto new_index = IndexFactory(index_type);
    new_index->set_index_model(model);
    new_index->Add(base_dataset, conf);
    auto cpu_idx = knowhere::cloner::CopyGpuToCpu(new_index, knowhere::Config());
    cpu_idx->Seal();
    auto search_idx = knowhere::cloner::CopyCpuToGpu(cpu_idx, DEVICEID, knowhere::Config());

    constexpr int train_count = 5;
    constexpr int search_count = 200;
    auto train_stage = [&] {
        for (int i = 0; i < train_count; ++i) {
            auto model = index_->Train(base_dataset, conf);
            auto test_idx = IndexFactory(index_type);
            test_idx->set_index_model(model);
            test_idx->Add(base_dataset, conf);
        }
    };
    auto search_stage = [&](knowhere::VectorIndexPtr& search_idx) {
        for (int i = 0; i < search_count; ++i) {
            auto result = search_idx->Search(query_dataset, conf);
            AssertAnns(result, nq, k);
        }
    };

    // TimeRecorder tc("record");
    // train_stage();
    // tc.RecordSection("train cost");
    // search_stage(search_idx);
    // tc.RecordSection("search cost");

    {
        // search and build parallel
        std::thread search_thread(search_stage, std::ref(search_idx));
        std::thread train_thread(train_stage);
        train_thread.join();
        search_thread.join();
    }
    {
        // build parallel
        std::thread train_1(train_stage);
        std::thread train_2(train_stage);
        train_1.join();
        train_2.join();
    }
    {
        // search parallel
        auto search_idx_2 = knowhere::cloner::CopyCpuToGpu(cpu_idx, DEVICEID, knowhere::Config());
        std::thread search_1(search_stage, std::ref(search_idx));
        std::thread search_2(search_stage, std::ref(search_idx_2));
        search_1.join();
        search_2.join();
    }
}

#ifdef CompareToOriFaiss
TEST_F(GPURESTEST, gpu_ivf_resource_test) {
    assert(!xb.empty());

    {
        index_ = std::make_shared<knowhere::GPUIVF>(-1);
        ASSERT_EQ(std::dynamic_pointer_cast<knowhere::GPUIVF>(index_)->GetGpuDevice(), -1);
        std::dynamic_pointer_cast<knowhere::GPUIVF>(index_)->SetGpuDevice(DEVICEID);
        ASSERT_EQ(std::dynamic_pointer_cast<knowhere::GPUIVF>(index_)->GetGpuDevice(), DEVICEID);

        auto conf = ParamGenerator::GetInstance().Gen(ParameterType::ivfsq);
        auto preprocessor = index_->BuildPreprocessor(base_dataset, conf);
        index_->set_preprocessor(preprocessor);
        auto model = index_->Train(base_dataset, conf);
        index_->set_index_model(model);
        index_->Add(base_dataset, conf);
        EXPECT_EQ(index_->Count(), nb);
        EXPECT_EQ(index_->Dimension(), dim);

        //        knowhere::TimeRecorder tc("knowere GPUIVF");
        for (int i = 0; i < search_count; ++i) {
            index_->Search(query_dataset, conf);
            if (i > search_count - 6 || i < 5)
            //                tc.RecordSection("search once");
        }
        //        tc.ElapseFromBegin("search all");
    }
    knowhere::FaissGpuResourceMgr::GetInstance().Dump();

    //    {
    //        // ori faiss IVF-Search
    //        faiss::gpu::StandardGpuResources res;
    //        faiss::gpu::GpuIndexIVFFlatConfig idx_config;
    //        idx_config.device = DEVICEID;
    //        faiss::gpu::GpuIndexIVFFlat device_index(&res, dim, 1638, faiss::METRIC_L2, idx_config);
    //        device_index.train(nb, xb.data());
    //        device_index.add(nb, xb.data());
    //
    //        knowhere::TimeRecorder tc("ori IVF");
    //        for (int i = 0; i < search_count; ++i) {
    //            device_index.search(nq, xq.data(), k, dis, ids);
    //            if (i > search_count - 6 || i < 5)
    //                tc.RecordSection("search once");
    //        }
    //        tc.ElapseFromBegin("search all");
    //    }
}

TEST_F(GPURESTEST, gpuivfsq) {
    {
        // knowhere gpu ivfsq
        index_type = "GPUIVFSQ";
        index_ = IndexFactory(index_type);

        auto conf = std::make_shared<knowhere::IVFSQCfg>();
        conf->nlist = 1638;
        conf->d = dim;
        conf->gpu_id = DEVICEID;
        conf->metric_type = knowhere::METRICTYPE::L2;
        conf->k = k;
        conf->nbits = 8;
        conf->nprobe = 1;

        auto preprocessor = index_->BuildPreprocessor(base_dataset, conf);
        index_->set_preprocessor(preprocessor);
        auto model = index_->Train(base_dataset, conf);
        index_->set_index_model(model);
        index_->Add(base_dataset, conf);
        //        auto result = index_->Search(query_dataset, conf);
        //        AssertAnns(result, nq, k);

        auto cpu_idx = knowhere::cloner::CopyGpuToCpu(index_, knowhere::Config());
        cpu_idx->Seal();

        knowhere::TimeRecorder tc("knowhere GPUSQ8");
        auto search_idx = knowhere::cloner::CopyCpuToGpu(cpu_idx, DEVICEID, knowhere::Config());
        tc.RecordSection("Copy to gpu");
        for (int i = 0; i < search_count; ++i) {
            search_idx->Search(query_dataset, conf);
            if (i > search_count - 6 || i < 5)
                tc.RecordSection("search once");
        }
        tc.ElapseFromBegin("search all");
    }

    {
        // Ori gpuivfsq Test
        const char* index_description = "IVF1638,SQ8";
        faiss::Index* ori_index = faiss::index_factory(dim, index_description, faiss::METRIC_L2);

        faiss::gpu::StandardGpuResources res;
        auto device_index = faiss::gpu::index_cpu_to_gpu(&res, DEVICEID, ori_index);
        device_index->train(nb, xb.data());
        device_index->add(nb, xb.data());

        auto cpu_index = faiss::gpu::index_gpu_to_cpu(device_index);
        auto idx = dynamic_cast<faiss::IndexIVF*>(cpu_index);
        if (idx != nullptr) {
            idx->to_readonly();
        }
        delete device_index;
        delete ori_index;

        faiss::gpu::GpuClonerOptions option;
        option.allInGpu = true;

        knowhere::TimeRecorder tc("ori GPUSQ8");
        faiss::Index* search_idx = faiss::gpu::index_cpu_to_gpu(&res, DEVICEID, cpu_index, &option);
        tc.RecordSection("Copy to gpu");
        for (int i = 0; i < search_count; ++i) {
            search_idx->search(nq, xq.data(), k, dis, ids);
            if (i > search_count - 6 || i < 5)
                tc.RecordSection("search once");
        }
        tc.ElapseFromBegin("search all");
        delete cpu_index;
        delete search_idx;
    }
}
#endif
