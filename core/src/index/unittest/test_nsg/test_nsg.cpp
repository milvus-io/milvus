// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <gtest/gtest.h>
#include <memory>

#include "knowhere/common/Exception.h"
#include "knowhere/index/vector_index/FaissBaseIndex.h"
#include "knowhere/index/vector_index/IndexNSG.h"
#ifdef MILVUS_GPU_VERSION
#include "knowhere/index/vector_index/helpers/FaissGpuResourceMgr.h"
#endif

#include "knowhere/common/Timer.h"
#include "knowhere/index/vector_index/nsg/NSGIO.h"

#include "unittest/utils.h"

using ::testing::Combine;
using ::testing::TestWithParam;
using ::testing::Values;

constexpr int64_t DEVICEID = 0;

class NSGInterfaceTest : public DataGen, public ::testing::Test {
 protected:
    void
    SetUp() override {
        // Init_with_default();
#ifdef MILVUS_GPU_VERSION
        int64_t MB = 1024 * 1024;
        knowhere::FaissGpuResourceMgr::GetInstance().InitDevice(DEVICEID, MB * 200, MB * 600, 1);
#endif
        Generate(256, 1000000 / 100, 1);
        index_ = std::make_shared<knowhere::NSG>();

        auto tmp_conf = std::make_shared<knowhere::NSGCfg>();
        tmp_conf->gpu_id = DEVICEID;
        tmp_conf->knng = 20;
        tmp_conf->nprobe = 8;
        tmp_conf->nlist = 163;
        tmp_conf->search_length = 40;
        tmp_conf->out_degree = 30;
        tmp_conf->candidate_pool_size = 100;
        tmp_conf->metric_type = knowhere::METRICTYPE::L2;
        train_conf = tmp_conf;
        train_conf->Dump();

        auto tmp2_conf = std::make_shared<knowhere::NSGCfg>();
        tmp2_conf->k = k;
        tmp2_conf->search_length = 30;
        search_conf = tmp2_conf;
        search_conf->Dump();
    }

    void
    TearDown() override {
#ifdef MILVUS_GPU_VERSION
        knowhere::FaissGpuResourceMgr::GetInstance().Free();
#endif
    }

 protected:
    std::shared_ptr<knowhere::NSG> index_;
    knowhere::Config train_conf;
    knowhere::Config search_conf;
};

TEST_F(NSGInterfaceTest, basic_test) {
    assert(!xb.empty());

    auto model = index_->Train(base_dataset, train_conf);
    auto result = index_->Search(query_dataset, search_conf);
    AssertAnns(result, nq, k);

    auto binaryset = index_->Serialize();
    auto new_index = std::make_shared<knowhere::NSG>();
    new_index->Load(binaryset);
    auto new_result = new_index->Search(query_dataset, search_conf);
    AssertAnns(result, nq, k);

    ASSERT_EQ(index_->Count(), nb);
    ASSERT_EQ(index_->Dimension(), dim);
    //    ASSERT_THROW({ index_->Clone(); }, knowhere::KnowhereException);
    ASSERT_NO_THROW({
        index_->Add(base_dataset, knowhere::Config());
        index_->Seal();
    });
}

TEST_F(NSGInterfaceTest, comparetest) {
    knowhere::algo::DistanceL2 distanceL2;
    knowhere::algo::DistanceIP distanceIP;

    knowhere::TimeRecorder tc("Compare");
    for (int i = 0; i < 1000; ++i) {
        distanceL2.Compare(xb.data(), xq.data(), 256);
    }
    tc.RecordSection("L2");
    for (int i = 0; i < 1000; ++i) {
        distanceIP.Compare(xb.data(), xq.data(), 256);
    }
    tc.RecordSection("IP");
}
