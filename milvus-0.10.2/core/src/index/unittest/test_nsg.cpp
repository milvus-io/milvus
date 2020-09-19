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

#include <fiu-control.h>
#include <fiu-local.h>
#include <gtest/gtest.h>
#include <memory>

#include "knowhere/common/Exception.h"
#include "knowhere/index/vector_index/FaissBaseIndex.h"
#include "knowhere/index/vector_index/IndexNSG.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"
#ifdef MILVUS_GPU_VERSION
#include "knowhere/index/vector_index/gpu/IndexGPUIDMAP.h"
#include "knowhere/index/vector_index/helpers/Cloner.h"
#include "knowhere/index/vector_index/helpers/FaissGpuResourceMgr.h"
#endif

#include "knowhere/common/Timer.h"
#include "knowhere/index/vector_index/impl/nsg/NSGIO.h"

#include "unittest/utils.h"

using ::testing::Combine;
using ::testing::TestWithParam;
using ::testing::Values;

constexpr int64_t DEVICE_GPU0 = 0;

class NSGInterfaceTest : public DataGen, public ::testing::Test {
 protected:
    void
    SetUp() override {
#ifdef MILVUS_GPU_VERSION
        int64_t MB = 1024 * 1024;
        milvus::knowhere::FaissGpuResourceMgr::GetInstance().InitDevice(DEVICE_GPU0, MB * 200, MB * 600, 1);
#endif
        int nsg_dim = 256;
        Generate(nsg_dim, 20000, nq);
        index_ = std::make_shared<milvus::knowhere::NSG>();

        train_conf = milvus::knowhere::Config{{milvus::knowhere::meta::DIM, 256},
                                              {milvus::knowhere::IndexParams::nlist, 163},
                                              {milvus::knowhere::IndexParams::nprobe, 8},
                                              {milvus::knowhere::IndexParams::knng, 20},
                                              {milvus::knowhere::IndexParams::search_length, 40},
                                              {milvus::knowhere::IndexParams::out_degree, 30},
                                              {milvus::knowhere::IndexParams::candidate, 100},
                                              {milvus::knowhere::Metric::TYPE, milvus::knowhere::Metric::L2}};

        search_conf = milvus::knowhere::Config{
            {milvus::knowhere::meta::TOPK, k},
            {milvus::knowhere::IndexParams::search_length, 30},
        };
    }

    void
    TearDown() override {
#ifdef MILVUS_GPU_VERSION
        milvus::knowhere::FaissGpuResourceMgr::GetInstance().Free();
#endif
    }

 protected:
    std::shared_ptr<milvus::knowhere::NSG> index_;
    milvus::knowhere::Config train_conf;
    milvus::knowhere::Config search_conf;
};

TEST_F(NSGInterfaceTest, basic_test) {
    assert(!xb.empty());
    fiu_init(0);
    // untrained index
    {
        ASSERT_ANY_THROW(index_->Serialize());
        ASSERT_ANY_THROW(index_->Query(query_dataset, search_conf));
        ASSERT_ANY_THROW(index_->Add(base_dataset, search_conf));
        ASSERT_ANY_THROW(index_->AddWithoutIds(base_dataset, search_conf));
    }

    train_conf[milvus::knowhere::meta::DEVICEID] = -1;
    index_->BuildAll(base_dataset, train_conf);
    auto result = index_->Query(query_dataset, search_conf);
    AssertAnns(result, nq, k);

    auto binaryset = index_->Serialize();
    {
        fiu_enable("NSG.Serialize.throw_exception", 1, nullptr, 0);
        ASSERT_ANY_THROW(index_->Serialize());
        fiu_disable("NSG.Serialize.throw_exception");
    }

    /* test NSG GPU train */
    auto new_index_1 = std::make_shared<milvus::knowhere::NSG>(DEVICE_GPU0);
    train_conf[milvus::knowhere::meta::DEVICEID] = DEVICE_GPU0;
    new_index_1->BuildAll(base_dataset, train_conf);
    auto new_result_1 = new_index_1->Query(query_dataset, search_conf);
    AssertAnns(new_result_1, nq, k);

    /* test NSG index load */
    auto new_index_2 = std::make_shared<milvus::knowhere::NSG>();
    new_index_2->Load(binaryset);
    {
        fiu_enable("NSG.Load.throw_exception", 1, nullptr, 0);
        ASSERT_ANY_THROW(new_index_2->Load(binaryset));
        fiu_disable("NSG.Load.throw_exception");
    }

    auto new_result_2 = new_index_2->Query(query_dataset, search_conf);
    AssertAnns(new_result_2, nq, k);

    ASSERT_EQ(index_->Count(), nb);
    ASSERT_EQ(index_->Dim(), dim);
}

TEST_F(NSGInterfaceTest, compare_test) {
    milvus::knowhere::impl::DistanceL2 distanceL2;
    milvus::knowhere::impl::DistanceIP distanceIP;

    milvus::knowhere::TimeRecorder tc("Compare");
    for (int i = 0; i < 1000; ++i) {
        distanceL2.Compare(xb.data(), xq.data(), 256);
    }
    tc.RecordSection("L2");
    for (int i = 0; i < 1000; ++i) {
        distanceIP.Compare(xb.data(), xq.data(), 256);
    }
    tc.RecordSection("IP");
}

TEST_F(NSGInterfaceTest, delete_test) {
    assert(!xb.empty());

    train_conf[milvus::knowhere::meta::DEVICEID] = DEVICE_GPU0;
    index_->Train(base_dataset, train_conf);

    auto result = index_->Query(query_dataset, search_conf);
    AssertAnns(result, nq, k);

    ASSERT_EQ(index_->Count(), nb);
    ASSERT_EQ(index_->Dim(), dim);

    faiss::ConcurrentBitsetPtr bitset = std::make_shared<faiss::ConcurrentBitset>(nb);
    for (int i = 0; i < nq; i++) {
        bitset->set(i);
    }

    auto I_before = result->Get<int64_t*>(milvus::knowhere::meta::IDS);

    // search xq with delete
    index_->SetBlacklist(bitset);
    auto result_after = index_->Query(query_dataset, search_conf);
    AssertAnns(result_after, nq, k, CheckMode::CHECK_NOT_EQUAL);
    auto I_after = result_after->Get<int64_t*>(milvus::knowhere::meta::IDS);

    // First vector deleted
    for (int i = 0; i < nq; i++) {
        ASSERT_NE(I_before[i * k], I_after[i * k]);
    }
}
