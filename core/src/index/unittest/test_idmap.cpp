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

#include <fiu-control.h>
#include <fiu-local.h>
#include <iostream>
#include <thread>

#include "knowhere/common/Exception.h"
#include "knowhere/index/vector_index/IndexIDMAP.h"
#include "knowhere/index/vector_index/IndexType.h"
#ifdef MILVUS_GPU_VERSION
#include <faiss/gpu/GpuCloner.h>
#include "knowhere/index/vector_index/gpu/IndexGPUIDMAP.h"
#include "knowhere/index/vector_index/helpers/Cloner.h"
#include "knowhere/index/vector_index/helpers/FaissGpuResourceMgr.h"
#endif
#include "Helper.h"
#include "unittest/utils.h"

using ::testing::Combine;
using ::testing::TestWithParam;
using ::testing::Values;

class IDMAPTest : public DataGen, public TestWithParam<milvus::knowhere::IndexMode> {
 protected:
    void
    SetUp() override {
#ifdef MILVUS_GPU_VERSION
        milvus::knowhere::FaissGpuResourceMgr::GetInstance().InitDevice(DEVICEID, PINMEM, TEMPMEM, RESNUM);
#endif
        index_mode_ = GetParam();
        Init_with_default();
        index_ = std::make_shared<milvus::knowhere::IDMAP>();
    }

    void
    TearDown() override {
#ifdef MILVUS_GPU_VERSION
        milvus::knowhere::FaissGpuResourceMgr::GetInstance().Free();
#endif
    }

 protected:
    milvus::knowhere::IDMAPPtr index_ = nullptr;
    milvus::knowhere::IndexMode index_mode_;
};

INSTANTIATE_TEST_CASE_P(IDMAPParameters, IDMAPTest,
                        Values(
#ifdef MILVUS_GPU_VERSION
                            milvus::knowhere::IndexMode::MODE_GPU,
#endif
                            milvus::knowhere::IndexMode::MODE_CPU));

TEST_P(IDMAPTest, idmap_basic) {
    ASSERT_TRUE(!xb.empty());

    milvus::knowhere::Config conf{{milvus::knowhere::meta::DIM, dim},
                                  {milvus::knowhere::meta::TOPK, k},
                                  {milvus::knowhere::Metric::TYPE, milvus::knowhere::Metric::L2}};

    // null faiss index
    {
        ASSERT_ANY_THROW(index_->Serialize());
        ASSERT_ANY_THROW(index_->Query(query_dataset, conf));
        ASSERT_ANY_THROW(index_->Add(nullptr, conf));
        ASSERT_ANY_THROW(index_->AddWithoutIds(nullptr, conf));
    }

    index_->Train(base_dataset, conf);
    index_->Add(base_dataset, conf);
    EXPECT_EQ(index_->Count(), nb);
    EXPECT_EQ(index_->Dim(), dim);
    ASSERT_TRUE(index_->GetRawVectors() != nullptr);
    ASSERT_TRUE(index_->GetRawIds() != nullptr);
    auto result = index_->Query(query_dataset, conf);
    AssertAnns(result, nq, k);
    //    PrintResult(result, nq, k);

    if (index_mode_ == milvus::knowhere::IndexMode::MODE_GPU) {
#ifdef MILVUS_GPU_VERSION
        // cpu to gpu
        index_ = std::dynamic_pointer_cast<milvus::knowhere::IDMAP>(index_->CopyCpuToGpu(DEVICEID, conf));
#endif
    }

    auto binaryset = index_->Serialize();
    auto new_index = std::make_shared<milvus::knowhere::IDMAP>();
    new_index->Load(binaryset);
    auto result2 = new_index->Query(query_dataset, conf);
    AssertAnns(result2, nq, k);
    //    PrintResult(re_result, nq, k);

#if 0
    auto result3 = new_index->QueryById(id_dataset, conf);
    AssertAnns(result3, nq, k);

    auto result4 = new_index->GetVectorById(xid_dataset, conf);
    AssertVec(result4, base_dataset, xid_dataset, 1, dim);
#endif

    faiss::ConcurrentBitsetPtr concurrent_bitset_ptr = std::make_shared<faiss::ConcurrentBitset>(nb);
    for (int64_t i = 0; i < nq; ++i) {
        concurrent_bitset_ptr->set(i);
    }
    index_->SetBlacklist(concurrent_bitset_ptr);

    auto result_bs_1 = index_->Query(query_dataset, conf);
    AssertAnns(result_bs_1, nq, k, CheckMode::CHECK_NOT_EQUAL);

#if 0
    auto result_bs_2 = index_->QueryById(id_dataset, conf);
    AssertAnns(result_bs_2, nq, k, CheckMode::CHECK_NOT_EQUAL);

    auto result_bs_3 = index_->GetVectorById(xid_dataset, conf);
    AssertVec(result_bs_3, base_dataset, xid_dataset, 1, dim, CheckMode::CHECK_NOT_EQUAL);
#endif
}

TEST_P(IDMAPTest, idmap_serialize) {
    auto serialize = [](const std::string& filename, milvus::knowhere::BinaryPtr& bin, uint8_t* ret) {
        FileIOWriter writer(filename);
        writer(static_cast<void*>(bin->data.get()), bin->size);

        FileIOReader reader(filename);
        reader(ret, bin->size);
    };

    milvus::knowhere::Config conf{{milvus::knowhere::meta::DIM, dim},
                                  {milvus::knowhere::meta::TOPK, k},
                                  {milvus::knowhere::Metric::TYPE, milvus::knowhere::Metric::L2}};

    {
        // serialize index
        index_->Train(base_dataset, conf);
        index_->Add(base_dataset, milvus::knowhere::Config());

        if (index_mode_ == milvus::knowhere::IndexMode::MODE_GPU) {
#ifdef MILVUS_GPU_VERSION
            // cpu to gpu
            index_ = std::dynamic_pointer_cast<milvus::knowhere::IDMAP>(index_->CopyCpuToGpu(DEVICEID, conf));
#endif
        }

        auto re_result = index_->Query(query_dataset, conf);
        AssertAnns(re_result, nq, k);
        //        PrintResult(re_result, nq, k);
        EXPECT_EQ(index_->Count(), nb);
        EXPECT_EQ(index_->Dim(), dim);
        auto binaryset = index_->Serialize();
        auto bin = binaryset.GetByName("IVF");

        std::string filename = "/tmp/idmap_test_serialize.bin";
        auto load_data = new uint8_t[bin->size];
        serialize(filename, bin, load_data);

        binaryset.clear();
        std::shared_ptr<uint8_t[]> data(load_data);
        binaryset.Append("IVF", data, bin->size);

        index_->Load(binaryset);
        EXPECT_EQ(index_->Count(), nb);
        EXPECT_EQ(index_->Dim(), dim);
        auto result = index_->Query(query_dataset, conf);
        AssertAnns(result, nq, k);
        //        PrintResult(result, nq, k);
    }
}

#ifdef MILVUS_GPU_VERSION
TEST_P(IDMAPTest, idmap_copy) {
    ASSERT_TRUE(!xb.empty());

    milvus::knowhere::Config conf{{milvus::knowhere::meta::DIM, dim},
                                  {milvus::knowhere::meta::TOPK, k},
                                  {milvus::knowhere::Metric::TYPE, milvus::knowhere::Metric::L2}};

    index_->Train(base_dataset, conf);
    index_->Add(base_dataset, conf);
    EXPECT_EQ(index_->Count(), nb);
    EXPECT_EQ(index_->Dim(), dim);
    ASSERT_TRUE(index_->GetRawVectors() != nullptr);
    ASSERT_TRUE(index_->GetRawIds() != nullptr);
    auto result = index_->Query(query_dataset, conf);
    AssertAnns(result, nq, k);
    // PrintResult(result, nq, k);

    {
        // clone
        //        auto clone_index = index_->Clone();
        //        auto clone_result = clone_index->Search(query_dataset, conf);
        //        AssertAnns(clone_result, nq, k);
    }

    {
        // cpu to gpu
        ASSERT_ANY_THROW(milvus::knowhere::cloner::CopyCpuToGpu(index_, -1, conf));
        auto clone_index = milvus::knowhere::cloner::CopyCpuToGpu(index_, DEVICEID, conf);
        auto clone_result = clone_index->Query(query_dataset, conf);
        AssertAnns(clone_result, nq, k);
        ASSERT_THROW({ std::static_pointer_cast<milvus::knowhere::GPUIDMAP>(clone_index)->GetRawVectors(); },
                     milvus::knowhere::KnowhereException);
        ASSERT_THROW({ std::static_pointer_cast<milvus::knowhere::GPUIDMAP>(clone_index)->GetRawIds(); },
                     milvus::knowhere::KnowhereException);

        fiu_init(0);
        fiu_enable("GPUIDMP.SerializeImpl.throw_exception", 1, nullptr, 0);
        ASSERT_ANY_THROW(clone_index->Serialize());
        fiu_disable("GPUIDMP.SerializeImpl.throw_exception");

        auto binary = clone_index->Serialize();
        clone_index->Load(binary);
        auto new_result = clone_index->Query(query_dataset, conf);
        AssertAnns(new_result, nq, k);

        //        auto clone_gpu_idx = clone_index->Clone();
        //        auto clone_gpu_res = clone_gpu_idx->Search(query_dataset, conf);
        //        AssertAnns(clone_gpu_res, nq, k);

        // gpu to cpu
        auto host_index = milvus::knowhere::cloner::CopyGpuToCpu(clone_index, conf);
        auto host_result = host_index->Query(query_dataset, conf);
        AssertAnns(host_result, nq, k);
        ASSERT_TRUE(std::static_pointer_cast<milvus::knowhere::IDMAP>(host_index)->GetRawVectors() != nullptr);
        ASSERT_TRUE(std::static_pointer_cast<milvus::knowhere::IDMAP>(host_index)->GetRawIds() != nullptr);

        // gpu to gpu
        auto device_index = milvus::knowhere::cloner::CopyCpuToGpu(index_, DEVICEID, conf);
        auto new_device_index =
            std::static_pointer_cast<milvus::knowhere::GPUIDMAP>(device_index)->CopyGpuToGpu(DEVICEID, conf);
        auto device_result = new_device_index->Query(query_dataset, conf);
        AssertAnns(device_result, nq, k);
    }
}
#endif
