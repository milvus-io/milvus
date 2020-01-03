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
#include <thread>

#ifdef MILVUS_GPU_VERSION
#include <faiss/gpu/GpuIndexIVFFlat.h>
#endif

#include "knowhere/adapter/VectorAdapter.h"
#include "knowhere/common/Exception.h"
#include "knowhere/common/Timer.h"

#include "knowhere/index/vector_index/IndexIVF.h"
#include "knowhere/index/vector_index/IndexIVFPQ.h"
#include "knowhere/index/vector_index/IndexIVFSQ.h"

#ifdef MILVUS_GPU_VERSION
#include "knowhere/index/vector_index/IndexGPUIVF.h"
#include "knowhere/index/vector_index/IndexGPUIVFPQ.h"
#include "knowhere/index/vector_index/IndexGPUIVFSQ.h"
#include "knowhere/index/vector_index/IndexIVFSQHybrid.h"
#include "knowhere/index/vector_index/helpers/Cloner.h"
#endif

#include "unittest/Helper.h"
#include "unittest/utils.h"

using ::testing::Combine;
using ::testing::TestWithParam;
using ::testing::Values;

class IVFTest : public DataGen, public TestWithParam<::std::tuple<std::string, ParameterType>> {
 protected:
    void
    SetUp() override {
#ifdef MILVUS_GPU_VERSION
        knowhere::FaissGpuResourceMgr::GetInstance().InitDevice(DEVICEID, PINMEM, TEMPMEM, RESNUM);
#endif
        ParameterType parameter_type;
        std::tie(index_type, parameter_type) = GetParam();
        // Init_with_default();
        //        nb = 1000000;
        //        nq = 1000;
        //        k = 1000;
        Generate(DIM, NB, NQ);
        index_ = IndexFactory(index_type);
        conf = ParamGenerator::GetInstance().Gen(parameter_type);
        conf->Dump();
    }

    void
    TearDown() override {
#ifdef MILVUS_GPU_VERSION
        knowhere::FaissGpuResourceMgr::GetInstance().Free();
#endif
    }

 protected:
    std::string index_type;
    knowhere::Config conf;
    knowhere::IVFIndexPtr index_ = nullptr;
};

INSTANTIATE_TEST_CASE_P(IVFParameters, IVFTest,
                        Values(
#ifdef MILVUS_GPU_VERSION
                            std::make_tuple("GPUIVF", ParameterType::ivf),
                            std::make_tuple("GPUIVFPQ", ParameterType::ivfpq),
                            std::make_tuple("GPUIVFSQ", ParameterType::ivfsq),
#ifdef CUSTOMIZATION
                            std::make_tuple("IVFSQHybrid", ParameterType::ivfsq),
#endif
#endif
                            std::make_tuple("IVF", ParameterType::ivf), std::make_tuple("IVFPQ", ParameterType::ivfpq),
                            std::make_tuple("IVFSQ", ParameterType::ivfsq)));

TEST_P(IVFTest, ivf_basic) {
    assert(!xb.empty());

    auto preprocessor = index_->BuildPreprocessor(base_dataset, conf);
    index_->set_preprocessor(preprocessor);

    auto model = index_->Train(base_dataset, conf);
    index_->set_index_model(model);
    index_->Add(base_dataset, conf);
    EXPECT_EQ(index_->Count(), nb);
    EXPECT_EQ(index_->Dimension(), dim);

    auto result = index_->Search(query_dataset, conf);
    AssertAnns(result, nq, conf->k);
    // PrintResult(result, nq, k);
}

TEST_P(IVFTest, ivf_serialize) {
    auto serialize = [](const std::string& filename, knowhere::BinaryPtr& bin, uint8_t* ret) {
        FileIOWriter writer(filename);
        writer(static_cast<void*>(bin->data.get()), bin->size);

        FileIOReader reader(filename);
        reader(ret, bin->size);
    };

    {
        // serialize index-model
        auto model = index_->Train(base_dataset, conf);
        auto binaryset = model->Serialize();
        auto bin = binaryset.GetByName("IVF");

        std::string filename = "/tmp/ivf_test_model_serialize.bin";
        auto load_data = new uint8_t[bin->size];
        serialize(filename, bin, load_data);

        binaryset.clear();
        auto data = std::make_shared<uint8_t>();
        data.reset(load_data);
        binaryset.Append("IVF", data, bin->size);

        model->Load(binaryset);

        index_->set_index_model(model);
        index_->Add(base_dataset, conf);
        auto result = index_->Search(query_dataset, conf);
        AssertAnns(result, nq, conf->k);
    }

    {
        // serialize index
        auto model = index_->Train(base_dataset, conf);
        index_->set_index_model(model);
        index_->Add(base_dataset, conf);
        auto binaryset = index_->Serialize();
        auto bin = binaryset.GetByName("IVF");

        std::string filename = "/tmp/ivf_test_serialize.bin";
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
        AssertAnns(result, nq, conf->k);
    }
}

// TODO(linxj): deprecated
#ifdef MILVUS_GPU_VERSION
TEST_P(IVFTest, clone_test) {
    assert(!xb.empty());

    auto preprocessor = index_->BuildPreprocessor(base_dataset, conf);
    index_->set_preprocessor(preprocessor);

    auto model = index_->Train(base_dataset, conf);
    index_->set_index_model(model);
    index_->Add(base_dataset, conf);
    EXPECT_EQ(index_->Count(), nb);
    EXPECT_EQ(index_->Dimension(), dim);
    auto result = index_->Search(query_dataset, conf);
    AssertAnns(result, nq, conf->k);
    // PrintResult(result, nq, k);

    auto AssertEqual = [&](knowhere::DatasetPtr p1, knowhere::DatasetPtr p2) {
        auto ids_p1 = p1->Get<int64_t*>(knowhere::meta::IDS);
        auto ids_p2 = p2->Get<int64_t*>(knowhere::meta::IDS);

        for (int i = 0; i < nq * k; ++i) {
            EXPECT_EQ(*((int64_t*)(ids_p2) + i), *((int64_t*)(ids_p1) + i));
            //            EXPECT_EQ(*(ids_p2->data()->GetValues<int64_t>(1, i)), *(ids_p1->data()->GetValues<int64_t>(1,
            //            i)));
        }
    };

    //    {
    //        // clone in place
    //        std::vector<std::string> support_idx_vec{"IVF", "GPUIVF", "IVFPQ", "IVFSQ", "GPUIVFSQ"};
    //        auto finder = std::find(support_idx_vec.cbegin(), support_idx_vec.cend(), index_type);
    //        if (finder != support_idx_vec.cend()) {
    //            EXPECT_NO_THROW({
    //                                auto clone_index = index_->Clone();
    //                                auto clone_result = clone_index->Search(query_dataset, conf);
    //                                //AssertAnns(result, nq, conf->k);
    //                                AssertEqual(result, clone_result);
    //                                std::cout << "inplace clone [" << index_type << "] success" << std::endl;
    //                            });
    //        } else {
    //            EXPECT_THROW({
    //                             std::cout << "inplace clone [" << index_type << "] failed" << std::endl;
    //                             auto clone_index = index_->Clone();
    //                         }, KnowhereException);
    //        }
    //    }

    {
        // copy from gpu to cpu
        std::vector<std::string> support_idx_vec{"GPUIVF", "GPUIVFSQ", "GPUIVFPQ", "IVFSQHybrid"};
        auto finder = std::find(support_idx_vec.cbegin(), support_idx_vec.cend(), index_type);
        if (finder != support_idx_vec.cend()) {
            EXPECT_NO_THROW({
                auto clone_index = knowhere::cloner::CopyGpuToCpu(index_, knowhere::Config());
                auto clone_result = clone_index->Search(query_dataset, conf);
                AssertEqual(result, clone_result);
                std::cout << "clone G <=> C [" << index_type << "] success" << std::endl;
            });
        } else {
            EXPECT_THROW(
                {
                    std::cout << "clone G <=> C [" << index_type << "] failed" << std::endl;
                    auto clone_index = knowhere::cloner::CopyGpuToCpu(index_, knowhere::Config());
                },
                knowhere::KnowhereException);
        }
    }

    if (index_type == "IVFSQHybrid") {
        return;
    }

    {
        // copy to gpu
        std::vector<std::string> support_idx_vec{"IVF", "GPUIVF", "IVFSQ", "GPUIVFSQ", "IVFPQ", "GPUIVFPQ"};
        auto finder = std::find(support_idx_vec.cbegin(), support_idx_vec.cend(), index_type);
        if (finder != support_idx_vec.cend()) {
            EXPECT_NO_THROW({
                auto clone_index = knowhere::cloner::CopyCpuToGpu(index_, DEVICEID, knowhere::Config());
                auto clone_result = clone_index->Search(query_dataset, conf);
                AssertEqual(result, clone_result);
                std::cout << "clone C <=> G [" << index_type << "] success" << std::endl;
            });
        } else {
            EXPECT_THROW(
                {
                    std::cout << "clone C <=> G [" << index_type << "] failed" << std::endl;
                    auto clone_index = knowhere::cloner::CopyCpuToGpu(index_, DEVICEID, knowhere::Config());
                },
                knowhere::KnowhereException);
        }
    }
}
#endif

#ifdef MILVUS_GPU_VERSION
#ifdef CUSTOMIZATION
TEST_P(IVFTest, gpu_seal_test) {
    std::vector<std::string> support_idx_vec{"GPUIVF", "GPUIVFSQ"};
    auto finder = std::find(support_idx_vec.cbegin(), support_idx_vec.cend(), index_type);
    if (finder == support_idx_vec.cend()) {
        return;
    }

    assert(!xb.empty());

    auto preprocessor = index_->BuildPreprocessor(base_dataset, conf);
    index_->set_preprocessor(preprocessor);

    auto model = index_->Train(base_dataset, conf);
    index_->set_index_model(model);
    index_->Add(base_dataset, conf);
    EXPECT_EQ(index_->Count(), nb);
    EXPECT_EQ(index_->Dimension(), dim);
    auto result = index_->Search(query_dataset, conf);
    AssertAnns(result, nq, conf->k);

    auto cpu_idx = knowhere::cloner::CopyGpuToCpu(index_, knowhere::Config());

    knowhere::TimeRecorder tc("CopyToGpu");
    knowhere::cloner::CopyCpuToGpu(cpu_idx, DEVICEID, knowhere::Config());
    auto without_seal = tc.RecordSection("Without seal");
    cpu_idx->Seal();
    tc.RecordSection("seal cost");
    knowhere::cloner::CopyCpuToGpu(cpu_idx, DEVICEID, knowhere::Config());
    auto with_seal = tc.RecordSection("With seal");
    ASSERT_GE(without_seal, with_seal);
}
#endif
#endif
