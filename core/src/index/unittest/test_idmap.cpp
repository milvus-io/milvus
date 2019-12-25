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

#include "knowhere/common/Exception.h"
#include "knowhere/index/vector_index/IndexIDMAP.h"
#ifdef MILVUS_GPU_VERSION
#include "knowhere/index/vector_index/IndexGPUIDMAP.h"
#include "knowhere/index/vector_index/helpers/Cloner.h"
#endif
#include "Helper.h"
#include "unittest/utils.h"

class IDMAPTest : public DataGen, public TestGpuIndexBase {
 protected:
    void
    SetUp() override {
        TestGpuIndexBase::SetUp();

        Init_with_default();
        index_ = std::make_shared<knowhere::IDMAP>();
    }

    void
    TearDown() override {
        TestGpuIndexBase::TearDown();
    }

 protected:
    knowhere::IDMAPPtr index_ = nullptr;
};

TEST_F(IDMAPTest, idmap_basic) {
    ASSERT_TRUE(!xb.empty());

    auto conf = std::make_shared<knowhere::Cfg>();
    conf->d = dim;
    conf->k = k;
    conf->metric_type = knowhere::METRICTYPE::L2;

    index_->Train(conf);
    index_->Add(base_dataset, conf);
    EXPECT_EQ(index_->Count(), nb);
    EXPECT_EQ(index_->Dimension(), dim);
    ASSERT_TRUE(index_->GetRawVectors() != nullptr);
    ASSERT_TRUE(index_->GetRawIds() != nullptr);
    auto result = index_->Search(query_dataset, conf);
    AssertAnns(result, nq, k);
    //    PrintResult(result, nq, k);

    index_->Seal();
    auto binaryset = index_->Serialize();
    auto new_index = std::make_shared<knowhere::IDMAP>();
    new_index->Load(binaryset);
    auto re_result = index_->Search(query_dataset, conf);
    AssertAnns(re_result, nq, k);
    //    PrintResult(re_result, nq, k);
}

TEST_F(IDMAPTest, idmap_serialize) {
    auto serialize = [](const std::string& filename, knowhere::BinaryPtr& bin, uint8_t* ret) {
        FileIOWriter writer(filename);
        writer(static_cast<void*>(bin->data.get()), bin->size);

        FileIOReader reader(filename);
        reader(ret, bin->size);
    };

    auto conf = std::make_shared<knowhere::Cfg>();
    conf->d = dim;
    conf->k = k;
    conf->metric_type = knowhere::METRICTYPE::L2;

    {
        // serialize index
        index_->Train(conf);
        index_->Add(base_dataset, knowhere::Config());
        auto re_result = index_->Search(query_dataset, conf);
        AssertAnns(re_result, nq, k);
        //        PrintResult(re_result, nq, k);
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
        auto result = index_->Search(query_dataset, conf);
        AssertAnns(result, nq, k);
        //        PrintResult(result, nq, k);
    }
}

#ifdef MILVUS_GPU_VERSION
TEST_F(IDMAPTest, copy_test) {
    ASSERT_TRUE(!xb.empty());

    auto conf = std::make_shared<knowhere::Cfg>();
    conf->d = dim;
    conf->k = k;
    conf->metric_type = knowhere::METRICTYPE::L2;

    index_->Train(conf);
    index_->Add(base_dataset, conf);
    EXPECT_EQ(index_->Count(), nb);
    EXPECT_EQ(index_->Dimension(), dim);
    ASSERT_TRUE(index_->GetRawVectors() != nullptr);
    ASSERT_TRUE(index_->GetRawIds() != nullptr);
    auto result = index_->Search(query_dataset, conf);
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
        auto clone_index = knowhere::cloner::CopyCpuToGpu(index_, DEVICEID, conf);
        auto clone_result = clone_index->Search(query_dataset, conf);
        AssertAnns(clone_result, nq, k);
        ASSERT_THROW({ std::static_pointer_cast<knowhere::GPUIDMAP>(clone_index)->GetRawVectors(); },
                     knowhere::KnowhereException);
        ASSERT_THROW({ std::static_pointer_cast<knowhere::GPUIDMAP>(clone_index)->GetRawIds(); },
                     knowhere::KnowhereException);

        auto binary = clone_index->Serialize();
        clone_index->Load(binary);
        auto new_result = clone_index->Search(query_dataset, conf);
        AssertAnns(new_result, nq, k);

        //        auto clone_gpu_idx = clone_index->Clone();
        //        auto clone_gpu_res = clone_gpu_idx->Search(query_dataset, conf);
        //        AssertAnns(clone_gpu_res, nq, k);

        // gpu to cpu
        auto host_index = knowhere::cloner::CopyGpuToCpu(clone_index, conf);
        auto host_result = host_index->Search(query_dataset, conf);
        AssertAnns(host_result, nq, k);
        ASSERT_TRUE(std::static_pointer_cast<knowhere::IDMAP>(host_index)->GetRawVectors() != nullptr);
        ASSERT_TRUE(std::static_pointer_cast<knowhere::IDMAP>(host_index)->GetRawIds() != nullptr);

        // gpu to gpu
        auto device_index = knowhere::cloner::CopyCpuToGpu(index_, DEVICEID, conf);
        auto new_device_index =
            std::static_pointer_cast<knowhere::GPUIDMAP>(device_index)->CopyGpuToGpu(DEVICEID, conf);
        auto device_result = new_device_index->Search(query_dataset, conf);
        AssertAnns(device_result, nq, k);
    }
}
#endif
