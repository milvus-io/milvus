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
#include "knowhere/index/vector_index/helpers/FaissGpuResourceMgr.h"
#include "knowhere/index/vector_index/nsg/NSGIO.h"

#include "unittest/utils.h"

namespace {

namespace kn = knowhere;

}  // namespace

using ::testing::Combine;
using ::testing::TestWithParam;
using ::testing::Values;

constexpr int64_t DEVICE_ID = 1;

class NSGInterfaceTest : public DataGen, public ::testing::Test {
 protected:
    void
    SetUp() override {
        // Init_with_default();
        kn::FaissGpuResourceMgr::GetInstance().InitDevice(DEVICE_ID, 1024 * 1024 * 200, 1024 * 1024 * 600, 2);
        Generate(256, 1000000, 1);
        index_ = std::make_shared<kn::NSG>();

        auto tmp_conf = std::make_shared<kn::NSGCfg>();
        tmp_conf->gpu_id = DEVICE_ID;
        tmp_conf->knng = 100;
        tmp_conf->nprobe = 32;
        tmp_conf->nlist = 16384;
        tmp_conf->search_length = 60;
        tmp_conf->out_degree = 70;
        tmp_conf->candidate_pool_size = 500;
        tmp_conf->metric_type = kn::METRICTYPE::L2;
        train_conf = tmp_conf;

        auto tmp2_conf = std::make_shared<kn::NSGCfg>();
        tmp2_conf->k = k;
        tmp2_conf->search_length = 30;
        search_conf = tmp2_conf;
    }

    void
    TearDown() override {
        kn::FaissGpuResourceMgr::GetInstance().Free();
    }

 protected:
    std::shared_ptr<kn::NSG> index_;
    kn::Config train_conf;
    kn::Config search_conf;
};

void
AssertAnns(const kn::DatasetPtr& result, const int& nq, const int& k) {
    auto ids = result->array()[0];
    for (auto i = 0; i < nq; i++) {
        EXPECT_EQ(i, *(ids->data()->GetValues<int64_t>(1, i * k)));
    }
}

TEST_F(NSGInterfaceTest, basic_test) {
    assert(!xb.empty());

    auto model = index_->Train(base_dataset, train_conf);
    auto result = index_->Search(query_dataset, search_conf);
    AssertAnns(result, nq, k);

    auto binaryset = index_->Serialize();
    auto new_index = std::make_shared<kn::NSG>();
    new_index->Load(binaryset);
    auto new_result = new_index->Search(query_dataset, search_conf);
    AssertAnns(result, nq, k);

    ASSERT_EQ(index_->Count(), nb);
    ASSERT_EQ(index_->Dimension(), dim);
    ASSERT_THROW({ index_->Clone(); }, knowhere::KnowhereException);
    ASSERT_NO_THROW({
        index_->Add(base_dataset, kn::Config());
        index_->Seal();
    });

    {
        // std::cout << "k = 1" << std::endl;
        // new_index->Search(GenQuery(1), Config::object{{"k", 1}});
        // new_index->Search(GenQuery(10), Config::object{{"k", 1}});
        // new_index->Search(GenQuery(100), Config::object{{"k", 1}});
        // new_index->Search(GenQuery(1000), Config::object{{"k", 1}});
        // new_index->Search(GenQuery(10000), Config::object{{"k", 1}});

        // std::cout << "k = 5" << std::endl;
        // new_index->Search(GenQuery(1), Config::object{{"k", 5}});
        // new_index->Search(GenQuery(20), Config::object{{"k", 5}});
        // new_index->Search(GenQuery(100), Config::object{{"k", 5}});
        // new_index->Search(GenQuery(300), Config::object{{"k", 5}});
        // new_index->Search(GenQuery(500), Config::object{{"k", 5}});
    }
}
