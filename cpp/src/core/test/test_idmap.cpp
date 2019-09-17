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

#include <iostream>
#include <sstream>

#include "knowhere/index/vector_index/idmap.h"
#include "knowhere/adapter/structure.h"
#include "knowhere/index/vector_index/cloner.h"
#include "knowhere/common/exception.h"

#include "utils.h"


using namespace zilliz::knowhere;

static int device_id = 0;
class IDMAPTest : public DataGen, public ::testing::Test {
 protected:
    void SetUp() override {
        FaissGpuResourceMgr::GetInstance().InitDevice(device_id, 1024*1024*200, 1024*1024*300, 2);
        Init_with_default();
        index_ = std::make_shared<IDMAP>();
    }

    void TearDown() override {
        FaissGpuResourceMgr::GetInstance().Free();
    }

 protected:
    IDMAPPtr index_ = nullptr;
};

void AssertAnns(const DatasetPtr &result,
                const int &nq,
                const int &k) {
    auto ids = result->array()[0];
    for (auto i = 0; i < nq; i++) {
        EXPECT_EQ(i, *(ids->data()->GetValues<int64_t>(1, i * k)));
    }
}

void PrintResult(const DatasetPtr &result,
                  const int &nq,
                  const int &k) {
    auto ids = result->array()[0];
    auto dists = result->array()[1];

    std::stringstream ss_id;
    std::stringstream ss_dist;
    for (auto i = 0; i < 10; i++) {
        for (auto j = 0; j < k; ++j) {
            ss_id << *(ids->data()->GetValues<int64_t>(1, i * k + j)) << " ";
            ss_dist << *(dists->data()->GetValues<float>(1, i * k + j)) << " ";
        }
        ss_id << std::endl;
        ss_dist << std::endl;
    }
    std::cout << "id\n" << ss_id.str() << std::endl;
    std::cout << "dist\n" << ss_dist.str() << std::endl;
}

TEST_F(IDMAPTest, idmap_basic) {
    ASSERT_TRUE(!xb.empty());
    Config Default_cfg;

    index_->Train(Config::object{{"dim", dim}, {"metric_type", "L2"}});
    index_->Add(base_dataset, Default_cfg);
    EXPECT_EQ(index_->Count(), nb);
    EXPECT_EQ(index_->Dimension(), dim);
    ASSERT_TRUE(index_->GetRawVectors() != nullptr);
    ASSERT_TRUE(index_->GetRawIds() != nullptr);
    auto result = index_->Search(query_dataset, Config::object{{"k", k}});
    AssertAnns(result, nq, k);
    PrintResult(result, nq, k);

    index_->Seal();
    auto binaryset = index_->Serialize();
    auto new_index = std::make_shared<IDMAP>();
    new_index->Load(binaryset);
    auto re_result = index_->Search(query_dataset, Config::object{{"k", k}});
    AssertAnns(re_result, nq, k);
    PrintResult(re_result, nq, k);
}

TEST_F(IDMAPTest, idmap_serialize) {
    auto serialize = [](const std::string &filename, BinaryPtr &bin, uint8_t *ret) {
        FileIOWriter writer(filename);
        writer(static_cast<void *>(bin->data.get()), bin->size);

        FileIOReader reader(filename);
        reader(ret, bin->size);
    };

    {
        // serialize index
        index_->Train(Config::object{{"dim", dim}, {"metric_type", "L2"}});
        index_->Add(base_dataset, Config());
        auto re_result = index_->Search(query_dataset, Config::object{{"k", k}});
        AssertAnns(re_result, nq, k);
        PrintResult(re_result, nq, k);
        EXPECT_EQ(index_->Count(), nb);
        EXPECT_EQ(index_->Dimension(), dim);
        auto binaryset = index_->Serialize();
        auto bin = binaryset.GetByName("IVF");

        std::string filename = "/tmp/idmap_test_serialize.bin";
        auto load_data = new uint8_t[bin->size];
        serialize(filename, bin, load_data);

        binaryset.clear();
        auto data = std::make_shared<uint8_t>();
        data.reset(load_data);
        binaryset.Append("IVF", data, bin->size);

        index_->Load(binaryset);
        EXPECT_EQ(index_->Count(), nb);
        EXPECT_EQ(index_->Dimension(), dim);
        auto result = index_->Search(query_dataset, Config::object{{"k", k}});
        AssertAnns(result, nq, k);
        PrintResult(result, nq, k);
    }
}

TEST_F(IDMAPTest, copy_test) {
    ASSERT_TRUE(!xb.empty());
    Config Default_cfg;

    index_->Train(Config::object{{"dim", dim}, {"metric_type", "L2"}});
    index_->Add(base_dataset, Default_cfg);
    EXPECT_EQ(index_->Count(), nb);
    EXPECT_EQ(index_->Dimension(), dim);
    ASSERT_TRUE(index_->GetRawVectors() != nullptr);
    ASSERT_TRUE(index_->GetRawIds() != nullptr);
    auto result = index_->Search(query_dataset, Config::object{{"k", k}});
    AssertAnns(result, nq, k);
    //PrintResult(result, nq, k);

    {
        // clone
        auto clone_index = index_->Clone();
        auto clone_result = clone_index->Search(query_dataset, Config::object{{"k", k}});
        AssertAnns(clone_result, nq, k);
    }

    {
        // cpu to gpu
        auto clone_index = CopyCpuToGpu(index_, device_id, Config());
        auto clone_result = clone_index->Search(query_dataset, Config::object{{"k", k}});
        AssertAnns(clone_result, nq, k);
        ASSERT_THROW({ std::static_pointer_cast<GPUIDMAP>(clone_index)->GetRawVectors(); },
                     zilliz::knowhere::KnowhereException);
        ASSERT_THROW({ std::static_pointer_cast<GPUIDMAP>(clone_index)->GetRawIds(); },
                     zilliz::knowhere::KnowhereException);

        auto binary = clone_index->Serialize();
        clone_index->Load(binary);
        auto new_result = clone_index->Search(query_dataset, Config::object{{"k", k}});
        AssertAnns(new_result, nq, k);

        auto clone_gpu_idx = clone_index->Clone();
        auto clone_gpu_res = clone_gpu_idx->Search(query_dataset, Config::object{{"k", k}});
        AssertAnns(clone_gpu_res, nq, k);

        // gpu to cpu
        auto host_index = CopyGpuToCpu(clone_index, Config());
        auto host_result = host_index->Search(query_dataset, Config::object{{"k", k}});
        AssertAnns(host_result, nq, k);
        ASSERT_TRUE(std::static_pointer_cast<IDMAP>(host_index)->GetRawVectors() != nullptr);
        ASSERT_TRUE(std::static_pointer_cast<IDMAP>(host_index)->GetRawIds() != nullptr);

        // gpu to gpu
        auto device_index = CopyCpuToGpu(index_, device_id, Config());
        auto new_device_index = std::static_pointer_cast<GPUIDMAP>(device_index)->CopyGpuToGpu(device_id, Config());
        auto device_result = new_device_index->Search(query_dataset, Config::object{{"k", k}});
        AssertAnns(device_result, nq, k);
    }
}
