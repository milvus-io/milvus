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
#include "knowhere/index/IndexType.h"
#include "knowhere/index/vector_index/IndexIVF.h"
#include "knowhere/index/vector_index/IndexIVFPQ.h"
#include "knowhere/index/vector_index/IndexIVFSQ.h"
#include "knowhere/index/vector_index/gpu/IndexGPUIVF.h"
#include "knowhere/index/vector_index/gpu/IndexGPUIVFPQ.h"
#include "knowhere/index/vector_index/gpu/IndexGPUIVFSQ.h"
#include "knowhere/index/vector_index/gpu/IndexIVFSQHybrid.h"
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
        ids = new int64_t[elems];
        dis = new float[elems];
    }

    void
    TearDown() override {
        delete[] ids;
        delete[] dis;
        TestGpuIndexBase::TearDown();
    }

 protected:
    milvus::knowhere::IndexType index_type_;
    milvus::knowhere::IndexMode index_mode_;
    milvus::knowhere::IVFPtr index_ = nullptr;

    int64_t* ids = nullptr;
    float* dis = nullptr;
    int64_t elems = 0;
};

TEST_F(GPURESTEST, copyandsearch) {
    // search and copy at the same time
    printf("==================\n");

    index_type_ = milvus::knowhere::IndexEnum::INDEX_FAISS_IVFFLAT;
    index_mode_ = milvus::knowhere::IndexMode::MODE_GPU;
    index_ = IndexFactory(index_type_, index_mode_);

    auto conf = ParamGenerator::GetInstance().Gen(index_type_);
    index_->Train(base_dataset, conf);
    index_->AddWithoutIds(base_dataset, conf);
    auto result = index_->Query(query_dataset, conf, nullptr);
    AssertAnns(result, nq, k);

    index_->SetIndexSize(nb * dim * sizeof(float));
    auto cpu_idx = milvus::knowhere::cloner::CopyGpuToCpu(index_, milvus::knowhere::Config());
    milvus::knowhere::IVFPtr ivf_idx = std::dynamic_pointer_cast<milvus::knowhere::IVF>(cpu_idx);
    ivf_idx->Seal();
    auto search_idx = milvus::knowhere::cloner::CopyCpuToGpu(cpu_idx, DEVICEID, milvus::knowhere::Config());

    constexpr int64_t search_count = 50;
    constexpr int64_t load_count = 15;
    auto search_func = [&] {
        // TimeRecorder tc("search&load");
        for (int i = 0; i < search_count; ++i) {
            auto result = search_idx->Query(query_dataset, conf, nullptr);
            // if (i > search_count - 6 || i == 0)
            //    tc.RecordSection("search once");
        }
        // tc.ElapseFromBegin("search finish");
    };
    auto load_func = [&] {
        // TimeRecorder tc("search&load");
        for (int i = 0; i < load_count; ++i) {
            milvus::knowhere::cloner::CopyCpuToGpu(cpu_idx, DEVICEID, milvus::knowhere::Config());
            // if (i > load_count -5 || i < 5)
            // tc.RecordSection("Copy to gpu");
        }
        // tc.ElapseFromBegin("load finish");
    };

    milvus::knowhere::TimeRecorder tc("Basic");
    milvus::knowhere::cloner::CopyCpuToGpu(cpu_idx, DEVICEID, milvus::knowhere::Config());
    tc.RecordSection("Copy to gpu once");
    auto result2 = search_idx->Query(query_dataset, conf, nullptr);
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
    index_type_ = milvus::knowhere::IndexEnum::INDEX_FAISS_IVFFLAT;
    index_mode_ = milvus::knowhere::IndexMode::MODE_GPU;
    index_ = IndexFactory(index_type_, index_mode_);

    auto conf = ParamGenerator::GetInstance().Gen(index_type_);
    index_->Train(base_dataset, conf);
    index_->AddWithoutIds(base_dataset, conf);
    index_->SetIndexSize(nb * dim * sizeof(float));
    auto cpu_idx = milvus::knowhere::cloner::CopyGpuToCpu(index_, milvus::knowhere::Config());
    milvus::knowhere::IVFPtr ivf_idx = std::dynamic_pointer_cast<milvus::knowhere::IVF>(cpu_idx);
    ivf_idx->Seal();
    auto search_idx = milvus::knowhere::cloner::CopyCpuToGpu(cpu_idx, DEVICEID, milvus::knowhere::Config());

    constexpr int train_count = 5;
    constexpr int search_count = 200;
    auto train_stage = [&] {
        for (int i = 0; i < train_count; ++i) {
            index_->Train(base_dataset, conf);
            index_->AddWithoutIds(base_dataset, conf);
        }
    };
    auto search_stage = [&](milvus::knowhere::VecIndexPtr& search_idx) {
        for (int i = 0; i < search_count; ++i) {
            auto result = search_idx->Query(query_dataset, conf, nullptr);
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
        auto search_idx_2 = milvus::knowhere::cloner::CopyCpuToGpu(cpu_idx, DEVICEID, milvus::knowhere::Config());
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
        index_ = std::make_shared<milvus::knowhere::GPUIVF>(-1);
        ASSERT_EQ(std::dynamic_pointer_cast<milvus::knowhere::GPUIVF>(index_)->GetGpuDevice(), -1);
        std::dynamic_pointer_cast<milvus::knowhere::GPUIVF>(index_)->SetGpuDevice(DEVICEID);
        ASSERT_EQ(std::dynamic_pointer_cast<milvus::knowhere::GPUIVF>(index_)->GetGpuDevice(), DEVICEID);

        auto conf = ParamGenerator::GetInstance().Gen(ParameterType::ivfsq);
        auto preprocessor = index_->BuildPreprocessor(base_dataset, conf);
        index_->set_preprocessor(preprocessor);
        auto model = index_->Train(base_dataset, conf);
        index_->set_index_model(model);
        index_->Add(base_dataset, conf);
        EXPECT_EQ(index_->Count(), nb);
        EXPECT_EQ(index_->Dimension(), dim);

        //        milvus::knowhere::TimeRecorder tc("knowere GPUIVF");
        for (int i = 0; i < search_count; ++i) {
            index_->Search(query_dataset, conf);
            if (i > search_count - 6 || i < 5)
            //                tc.RecordSection("search once");
        }
        //        tc.ElapseFromBegin("search all");
    }
    milvus::knowhere::FaissGpuResourceMgr::GetInstance().Dump();

    //    {
    //        // ori faiss IVF-Search
    //        faiss::gpu::StandardGpuResources res;
    //        faiss::gpu::GpuIndexIVFFlatConfig idx_config;
    //        idx_config.device = DEVICEID;
    //        faiss::gpu::GpuIndexIVFFlat device_index(&res, dim, 1638, faiss::METRIC_L2, idx_config);
    //        device_index.train(nb, xb.data());
    //        device_index.add(nb, xb.data());
    //
    //        milvus::knowhere::TimeRecorder tc("ori IVF");
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

        auto conf = std::make_shared<milvus::knowhere::IVFSQCfg>();
        conf->nlist = 1638;
        conf->d = dim;
        conf->gpu_id = DEVICEID;
        conf->metric_type = milvus::knowhere::METRICTYPE::L2;
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

        auto cpu_idx = milvus::knowhere::cloner::CopyGpuToCpu(index_, milvus::knowhere::Config());
        cpu_idx->Seal();

        milvus::knowhere::TimeRecorder tc("knowhere GPUSQ8");
        auto search_idx = milvus::knowhere::cloner::CopyCpuToGpu(cpu_idx, DEVICEID, milvus::knowhere::Config());
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

        milvus::knowhere::TimeRecorder tc("ori GPUSQ8");
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
