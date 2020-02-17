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

#include "knowhere/index/vector_index/helpers/FaissGpuResourceMgr.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"
#include "wrapper/VecIndex.h"
#include "wrapper/utils.h"

#include <gtest/gtest.h>
#include <fiu-local.h>
#include <fiu-control.h>
#include "knowhere/index/vector_index/IndexIVFSQHybrid.h"

using ::testing::Combine;
using ::testing::TestWithParam;
using ::testing::Values;

class KnowhereHybrid : public DataGenBase, public ::testing::Test {
 protected:
    void
    SetUp() override {
        knowhere::FaissGpuResourceMgr::GetInstance().InitDevice(DEVICEID, PINMEM, TEMPMEM, RESNUM);

        dim = 128;
        nb = 10000;
        nq = 100;
        k = 100;
        GenData(dim, nb, nq, xb, xq, ids, k, gt_ids, gt_dis);
    }

    void
    TearDown() override {
        knowhere::FaissGpuResourceMgr::GetInstance().Free();
    }

 protected:
    milvus::engine::IndexType index_type;
    milvus::engine::VecIndexPtr index_ = nullptr;
    knowhere::Config conf;
};

#ifdef CUSTOMIZATION
TEST_F(KnowhereHybrid, test_interface) {
    assert(!xb.empty());

    index_type = milvus::engine::IndexType::FAISS_IVFSQ8_HYBRID;
    index_ = GetVecIndexFactory(index_type);
    conf = ParamGenerator::GetInstance().Gen(index_type);

    auto elems = nq * k;
    std::vector<int64_t> res_ids(elems);
    std::vector<float> res_dis(elems);

    conf->gpu_id = DEVICEID;
    conf->d = dim;
    conf->k = k;
    index_->BuildAll(nb, xb.data(), ids.data(), conf);
    index_->Search(nq, xq.data(), res_dis.data(), res_ids.data(), conf);
    AssertResult(res_ids, res_dis);
    EXPECT_EQ(index_->Count(), nb);
    EXPECT_EQ(index_->Dimension(), dim);

    auto binaryset = index_->Serialize();
    {
        // cpu -> gpu
        auto cpu_idx = GetVecIndexFactory(index_type);
        cpu_idx->Load(binaryset);
        {
            for (int i = 0; i < 2; ++i) {
                auto gpu_idx = cpu_idx->CopyToGpu(DEVICEID, conf);
                gpu_idx->Search(nq, xq.data(), res_dis.data(), res_ids.data(), conf);
                AssertResult(res_ids, res_dis);
            }
        }
    }

    {
        // quantization already in gpu, only copy data
        auto cpu_idx = GetVecIndexFactory(index_type);
        cpu_idx->Load(binaryset);

        auto pair = cpu_idx->CopyToGpuWithQuantizer(DEVICEID, conf);
        auto gpu_idx = pair.first;
        auto quantization = pair.second;

        gpu_idx->Search(nq, xq.data(), res_dis.data(), res_ids.data(), conf);
        AssertResult(res_ids, res_dis);

        auto quantizer_conf = std::make_shared<knowhere::QuantizerCfg>();
        quantizer_conf->mode = 2;
        quantizer_conf->gpu_id = DEVICEID;
        for (int i = 0; i < 2; ++i) {
            auto hybrid_idx = GetVecIndexFactory(index_type);
            hybrid_idx->Load(binaryset);

            hybrid_idx->LoadData(quantization, quantizer_conf);
            hybrid_idx->Search(nq, xq.data(), res_dis.data(), res_ids.data(), conf);
            AssertResult(res_ids, res_dis);
        }
    }

    {
        // quantization already in gpu, only set quantization
        auto cpu_idx = GetVecIndexFactory(index_type);
        cpu_idx->Load(binaryset);

        auto pair = cpu_idx->CopyToGpuWithQuantizer(DEVICEID, conf);
        auto quantization = pair.second;

        for (int i = 0; i < 2; ++i) {
            auto hybrid_idx = GetVecIndexFactory(index_type);
            hybrid_idx->Load(binaryset);

            hybrid_idx->SetQuantizer(quantization);
            hybrid_idx->Search(nq, xq.data(), res_dis.data(), res_ids.data(), conf);
            AssertResult(res_ids, res_dis);
            hybrid_idx->UnsetQuantizer();
        }
    }
}

TEST_F(KnowhereHybrid, test_invalid_index) {
    assert(!xb.empty());
    fiu_init(0);

    index_type = milvus::engine::IndexType::FAISS_IVFSQ8_HYBRID;
    fiu_enable("GetVecIndexFactory.IVFSQHybrid.mock", 1, nullptr, 0);
    index_ = GetVecIndexFactory(index_type);
    fiu_disable("GetVecIndexFactory.IVFSQHybrid.mock");
    conf = ParamGenerator::GetInstance().Gen(index_type);

    auto elems = nq * k;
    std::vector<int64_t> res_ids(elems);
    std::vector<float> res_dis(elems);

    conf->gpu_id = DEVICEID;
    conf->d = dim;
    conf->k = k;

    auto status = index_->BuildAll(nb, xb.data(), ids.data(), conf);
    ASSERT_FALSE(status.ok());

    auto quantizer = std::make_shared<knowhere::Quantizer>();
    auto quantizer_cfg = std::make_shared<knowhere::QuantizerCfg>();
    index_->LoadQuantizer(quantizer_cfg);
    status = index_->SetQuantizer(quantizer);
    ASSERT_FALSE(status.ok());

    fiu_enable("IVFHybridIndex.SetQuantizer.throw_knowhere_exception", 1, nullptr, 0);
    status = index_->SetQuantizer(quantizer);
    ASSERT_FALSE(status.ok());
    fiu_disable("IVFHybridIndex.SetQuantizer.throw_knowhere_exception");

    fiu_enable("IVFHybridIndex.SetQuantizer.throw_std_exception", 1, nullptr, 0);
    status = index_->SetQuantizer(quantizer);
    ASSERT_FALSE(status.ok());
    fiu_disable("IVFHybridIndex.SetQuantizer.throw_std_exception");

    status = index_->UnsetQuantizer();
    ASSERT_FALSE(status.ok());
    fiu_enable("IVFHybridIndex.UnsetQuantizer.throw_knowhere_exception", 1, nullptr, 0);
    status = index_->UnsetQuantizer();
    ASSERT_FALSE(status.ok());
    fiu_disable("IVFHybridIndex.UnsetQuantizer.throw_knowhere_exception");

    fiu_enable("IVFHybridIndex.UnsetQuantizer.throw_std_exception", 1, nullptr, 0);
    status = index_->UnsetQuantizer();
    ASSERT_FALSE(status.ok());
    fiu_disable("IVFHybridIndex.UnsetQuantizer.throw_std_exception");

    auto vecindex = index_->LoadData(quantizer, quantizer_cfg);
    ASSERT_EQ(vecindex, nullptr);

    fiu_enable("IVFHybridIndex.LoadData.throw_std_exception", 1, nullptr, 0);
    vecindex = index_->LoadData(quantizer, quantizer_cfg);
    ASSERT_EQ(vecindex, nullptr);
    fiu_disable("IVFHybridIndex.LoadData.throw_std_exception");

    fiu_enable("IVFHybridIndex.LoadData.throw_knowhere_exception", 1, nullptr, 0);
    vecindex = index_->LoadData(quantizer, quantizer_cfg);
    ASSERT_EQ(vecindex, nullptr);
    fiu_disable("IVFHybridIndex.LoadData.throw_knowhere_exception");

    index_->CopyToGpuWithQuantizer(DEVICEID, conf);
    fiu_enable("IVFHybridIndex.LoadData.throw_knowhere_exception", 1, nullptr, 0);
    index_->CopyToGpuWithQuantizer(DEVICEID, conf);
    fiu_disable("IVFHybridIndex.LoadData.throw_knowhere_exception");
    fiu_enable("IVFHybridIndex.LoadData.throw_std_exception", 1, nullptr, 0);
    index_->CopyToGpuWithQuantizer(DEVICEID, conf);
    fiu_disable("IVFHybridIndex.LoadData.throw_std_exception");
}

#endif
