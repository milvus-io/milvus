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

#include "knowhere/common/exception.h"
#include "knowhere/index/vector_index/gpu_ivf.h"
#include "knowhere/index/vector_index/nsg_index.h"
#include "knowhere/index/vector_index/nsg/nsg_io.h"

#include "../utils.h"


using namespace zilliz::knowhere;
using ::testing::TestWithParam;
using ::testing::Values;
using ::testing::Combine;

constexpr int64_t DEVICE_ID = 0;

class NSGInterfaceTest : public DataGen, public TestWithParam<::std::tuple<Config, Config>> {
 protected:
    void SetUp() override {
        //Init_with_default();
        FaissGpuResourceMgr::GetInstance().InitDevice(DEVICE_ID, 1024*1024*200, 1024*1024*600, 2);
        Generate(256, 10000, 1);
        index_ = std::make_shared<NSG>();
        std::tie(train_cfg, search_cfg) = GetParam();
    }

    void TearDown() override {
        FaissGpuResourceMgr::GetInstance().Free();
    }

 protected:
    std::shared_ptr<NSG> index_;
    Config train_cfg;
    Config search_cfg;
};

INSTANTIATE_TEST_CASE_P(NSGparameters, NSGInterfaceTest,
                        Values(std::make_tuple(
                            // search length > out_degree
                            Config::object{{"nlist", 128}, {"nprobe", 50}, {"knng", 100}, {"metric_type", "L2"},
                                           {"search_length", 60}, {"out_degree", 70}, {"candidate_pool_size", 500}},
                            Config::object{{"k", 20}, {"search_length", 30}}))
);

void AssertAnns(const DatasetPtr &result,
                const int &nq,
                const int &k) {
    auto ids = result->array()[0];
    for (auto i = 0; i < nq; i++) {
        EXPECT_EQ(i, *(ids->data()->GetValues<int64_t>(1, i * k)));
    }
}

TEST_P(NSGInterfaceTest, basic_test) {
    assert(!xb.empty());

    auto model = index_->Train(base_dataset, train_cfg);
    auto result = index_->Search(query_dataset, search_cfg);
    AssertAnns(result, nq, k);

    auto binaryset = index_->Serialize();
    auto new_index = std::make_shared<NSG>();
    new_index->Load(binaryset);
    auto new_result = new_index->Search(query_dataset, Config::object{{"k", k}});
    AssertAnns(result, nq, k);

    ASSERT_EQ(index_->Count(), nb);
    ASSERT_EQ(index_->Dimension(), dim);
    ASSERT_THROW({index_->Clone();}, zilliz::knowhere::KnowhereException);
    ASSERT_NO_THROW({
        index_->Add(base_dataset, Config());
        index_->Seal();
    });

    {
        //std::cout << "k = 1" << std::endl;
        //new_index->Search(GenQuery(1), Config::object{{"k", 1}});
        //new_index->Search(GenQuery(10), Config::object{{"k", 1}});
        //new_index->Search(GenQuery(100), Config::object{{"k", 1}});
        //new_index->Search(GenQuery(1000), Config::object{{"k", 1}});
        //new_index->Search(GenQuery(10000), Config::object{{"k", 1}});

        //std::cout << "k = 5" << std::endl;
        //new_index->Search(GenQuery(1), Config::object{{"k", 5}});
        //new_index->Search(GenQuery(20), Config::object{{"k", 5}});
        //new_index->Search(GenQuery(100), Config::object{{"k", 5}});
        //new_index->Search(GenQuery(300), Config::object{{"k", 5}});
        //new_index->Search(GenQuery(500), Config::object{{"k", 5}});
    }
}

