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

#include <faiss/AutoTune.h>
#include <faiss/gpu/GpuAutoTune.h>
#include <faiss/gpu/GpuIndexIVFFlat.h>

#include "knowhere/common/Exception.h"
#include "knowhere/common/Timer.h"
#include "knowhere/index/vector_index/IndexGPUIVF.h"
#include "knowhere/index/vector_index/IndexGPUIVFPQ.h"
#include "knowhere/index/vector_index/IndexGPUIVFSQ.h"
#include "knowhere/index/vector_index/IndexIVF.h"
#include "knowhere/index/vector_index/IndexIVFPQ.h"
#include "knowhere/index/vector_index/IndexIVFSQ.h"
#include "knowhere/index/vector_index/IndexIVFSQHybrid.h"
#include "knowhere/index/vector_index/helpers/Cloner.h"

#include "unittest/utils.h"

using ::testing::Combine;
using ::testing::TestWithParam;
using ::testing::Values;

constexpr int device_id = 0;
constexpr int64_t DIM = 128;
constexpr int64_t NB = 1000000 / 100;
constexpr int64_t NQ = 10;
constexpr int64_t K = 10;

knowhere::IVFIndexPtr
IndexFactory(const std::string& type) {
    if (type == "IVF") {
        return std::make_shared<knowhere::IVF>();
    } else if (type == "IVFPQ") {
        return std::make_shared<knowhere::IVFPQ>();
    } else if (type == "GPUIVF") {
        return std::make_shared<knowhere::GPUIVF>(device_id);
    } else if (type == "GPUIVFPQ") {
        return std::make_shared<knowhere::GPUIVFPQ>(device_id);
    } else if (type == "IVFSQ") {
        return std::make_shared<knowhere::IVFSQ>();
    } else if (type == "GPUIVFSQ") {
        return std::make_shared<knowhere::GPUIVFSQ>(device_id);
    } else if (type == "IVFSQHybrid") {
        return std::make_shared<knowhere::IVFSQHybrid>(device_id);
    }
}

enum class ParameterType {
    ivf,
    ivfpq,
    ivfsq,
    nsg,
};

class ParamGenerator {
 public:
    static ParamGenerator&
    GetInstance() {
        static ParamGenerator instance;
        return instance;
    }

    knowhere::Config
    Gen(const ParameterType& type) {
        if (type == ParameterType::ivf) {
            auto tempconf = std::make_shared<knowhere::IVFCfg>();
            tempconf->d = DIM;
            tempconf->gpu_id = device_id;
            tempconf->nlist = 100;
            tempconf->nprobe = 16;
            tempconf->k = K;
            tempconf->metric_type = knowhere::METRICTYPE::L2;
            return tempconf;
        } else if (type == ParameterType::ivfpq) {
            auto tempconf = std::make_shared<knowhere::IVFPQCfg>();
            tempconf->d = DIM;
            tempconf->gpu_id = device_id;
            tempconf->nlist = 25;
            tempconf->nprobe = 4;
            tempconf->k = K;
            tempconf->m = 4;
            tempconf->nbits = 8;
            tempconf->metric_type = knowhere::METRICTYPE::L2;
            return tempconf;
        } else if (type == ParameterType::ivfsq) {
            auto tempconf = std::make_shared<knowhere::IVFSQCfg>();
            tempconf->d = DIM;
            tempconf->gpu_id = device_id;
            tempconf->nlist = 100;
            tempconf->nprobe = 16;
            tempconf->k = K;
            tempconf->nbits = 8;
            tempconf->metric_type = knowhere::METRICTYPE::L2;
            return tempconf;
        }
    }
};

class IVFTest : public DataGen, public TestWithParam<::std::tuple<std::string, ParameterType>> {
 protected:
    void
    SetUp() override {
        ParameterType parameter_type;
        std::tie(index_type, parameter_type) = GetParam();
        // Init_with_default();
        Generate(DIM, NB, NQ);
        index_ = IndexFactory(index_type);
        conf = ParamGenerator::GetInstance().Gen(parameter_type);
        knowhere::FaissGpuResourceMgr::GetInstance().InitDevice(device_id, 1024 * 1024 * 200, 1024 * 1024 * 600, 2);
    }

    void
    TearDown() override {
        knowhere::FaissGpuResourceMgr::GetInstance().Free();
    }

    knowhere::VectorIndexPtr
    ChooseTodo() {
        std::vector<std::string> gpu_idx{"GPUIVFSQ"};
        auto finder = std::find(gpu_idx.cbegin(), gpu_idx.cend(), index_type);
        if (finder != gpu_idx.cend()) {
            return knowhere::cloner::CopyCpuToGpu(index_, device_id, knowhere::Config());
        }
        return index_;
    }

 protected:
    std::string index_type;
    knowhere::Config conf;
    knowhere::IVFIndexPtr index_ = nullptr;
};

INSTANTIATE_TEST_CASE_P(IVFParameters, IVFTest, Values(std::make_tuple("IVF", ParameterType::ivf),
                                                       std::make_tuple("GPUIVF", ParameterType::ivf),
                                                       std::make_tuple("IVFPQ", ParameterType::ivfpq),
                                                       std::make_tuple("GPUIVFPQ", ParameterType::ivfpq),
                                                       std::make_tuple("IVFSQ", ParameterType::ivfsq),
#ifdef CUSTOMIZATION
                                                       std::make_tuple("IVFSQHybrid", ParameterType::ivfsq),
#endif
                                                       std::make_tuple("GPUIVFSQ", ParameterType::ivfsq)));

void
AssertAnns(const knowhere::DatasetPtr& result, const int& nq, const int& k) {
    auto ids = result->array()[0];
    for (auto i = 0; i < nq; i++) {
        EXPECT_EQ(i, *(ids->data()->GetValues<int64_t>(1, i * k)));
    }
}

void
PrintResult(const knowhere::DatasetPtr& result, const int& nq, const int& k) {
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

TEST_P(IVFTest, ivf_basic) {
    assert(!xb.empty());

    auto preprocessor = index_->BuildPreprocessor(base_dataset, conf);
    index_->set_preprocessor(preprocessor);

    auto model = index_->Train(base_dataset, conf);
    index_->set_index_model(model);
    index_->Add(base_dataset, conf);
    EXPECT_EQ(index_->Count(), nb);
    EXPECT_EQ(index_->Dimension(), dim);

    auto new_idx = ChooseTodo();
    auto result = new_idx->Search(query_dataset, conf);
    AssertAnns(result, nq, conf->k);
    // PrintResult(result, nq, k);
}

TEST_P(IVFTest, hybrid) {
    if (index_type != "IVFSQHybrid") {
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

    //    auto new_idx = ChooseTodo();
    //    auto result = new_idx->Search(query_dataset, conf);
    //    AssertAnns(result, nq, conf->k);

    {
        auto hybrid_1_idx = std::make_shared<knowhere::IVFSQHybrid>(device_id);

        auto binaryset = index_->Serialize();
        hybrid_1_idx->Load(binaryset);

        auto quantizer_conf = std::make_shared<knowhere::QuantizerCfg>();
        quantizer_conf->mode = 1;
        quantizer_conf->gpu_id = device_id;
        auto q = hybrid_1_idx->LoadQuantizer(quantizer_conf);
        hybrid_1_idx->SetQuantizer(q);
        auto result = hybrid_1_idx->Search(query_dataset, conf);
        AssertAnns(result, nq, conf->k);
        PrintResult(result, nq, k);
        hybrid_1_idx->UnsetQuantizer();
    }

    {
        auto hybrid_2_idx = std::make_shared<knowhere::IVFSQHybrid>(device_id);

        auto binaryset = index_->Serialize();
        hybrid_2_idx->Load(binaryset);

        auto quantizer_conf = std::make_shared<knowhere::QuantizerCfg>();
        quantizer_conf->mode = 1;
        quantizer_conf->gpu_id = device_id;
        auto q = hybrid_2_idx->LoadQuantizer(quantizer_conf);
        quantizer_conf->mode = 2;
        auto gpu_idx = hybrid_2_idx->LoadData(q, quantizer_conf);

        auto result = gpu_idx->Search(query_dataset, conf);
        AssertAnns(result, nq, conf->k);
        PrintResult(result, nq, k);
    }
}

// TEST_P(IVFTest, gpu_to_cpu) {
//    if (index_type.find("GPU") == std::string::npos) { return; }
//
//    // else
//    assert(!xb.empty());
//
//    auto preprocessor = index_->BuildPreprocessor(base_dataset, conf);
//    index_->set_preprocessor(preprocessor);
//
//    auto model = index_->Train(base_dataset, conf);
//    index_->set_index_model(model);
//    index_->Add(base_dataset, conf);
//    EXPECT_EQ(index_->Count(), nb);
//    EXPECT_EQ(index_->Dimension(), dim);
//    auto result = index_->Search(query_dataset, conf);
//    AssertAnns(result, nq, k);
//
//    if (auto device_index = std::dynamic_pointer_cast<GPUIVF>(index_)) {
//        auto host_index = device_index->Copy_index_gpu_to_cpu();
//        auto result = host_index->Search(query_dataset, conf);
//        AssertAnns(result, nq, k);
//    }
//}

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
        auto new_idx = ChooseTodo();
        auto result = new_idx->Search(query_dataset, conf);
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
        auto new_idx = ChooseTodo();
        auto result = new_idx->Search(query_dataset, conf);
        AssertAnns(result, nq, conf->k);
    }
}

TEST_P(IVFTest, clone_test) {
    assert(!xb.empty());

    auto preprocessor = index_->BuildPreprocessor(base_dataset, conf);
    index_->set_preprocessor(preprocessor);

    auto model = index_->Train(base_dataset, conf);
    index_->set_index_model(model);
    index_->Add(base_dataset, conf);
    EXPECT_EQ(index_->Count(), nb);
    EXPECT_EQ(index_->Dimension(), dim);
    auto new_idx = ChooseTodo();
    auto result = new_idx->Search(query_dataset, conf);
    AssertAnns(result, nq, conf->k);
    // PrintResult(result, nq, k);

    auto AssertEqual = [&](knowhere::DatasetPtr p1, knowhere::DatasetPtr p2) {
        auto ids_p1 = p1->array()[0];
        auto ids_p2 = p2->array()[0];

        for (int i = 0; i < nq * k; ++i) {
            EXPECT_EQ(*(ids_p2->data()->GetValues<int64_t>(1, i)), *(ids_p1->data()->GetValues<int64_t>(1, i)));
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
        if (index_type == "IVFSQHybrid") {
            return;
        }
    }

    {
        // copy from gpu to cpu
        std::vector<std::string> support_idx_vec{"GPUIVF", "GPUIVFSQ", "IVFSQHybrid"};
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

    {
        // copy to gpu
        std::vector<std::string> support_idx_vec{"IVF", "GPUIVF", "IVFSQ", "GPUIVFSQ"};
        auto finder = std::find(support_idx_vec.cbegin(), support_idx_vec.cend(), index_type);
        if (finder != support_idx_vec.cend()) {
            EXPECT_NO_THROW({
                auto clone_index = knowhere::cloner::CopyCpuToGpu(index_, device_id, knowhere::Config());
                auto clone_result = clone_index->Search(query_dataset, conf);
                AssertEqual(result, clone_result);
                std::cout << "clone C <=> G [" << index_type << "] success" << std::endl;
            });
        } else {
            EXPECT_THROW(
                {
                    std::cout << "clone C <=> G [" << index_type << "] failed" << std::endl;
                    auto clone_index = knowhere::cloner::CopyCpuToGpu(index_, device_id, knowhere::Config());
                },
                knowhere::KnowhereException);
        }
    }
}

#ifdef CUSTOMIZATION
TEST_P(IVFTest, seal_test) {
    // FaissGpuResourceMgr::GetInstance().InitDevice(device_id);

    std::vector<std::string> support_idx_vec{"GPUIVF", "GPUIVFSQ", "IVFSQHybrid"};
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
    auto new_idx = ChooseTodo();
    auto result = new_idx->Search(query_dataset, conf);
    AssertAnns(result, nq, conf->k);

    auto cpu_idx = knowhere::cloner::CopyGpuToCpu(index_, knowhere::Config());

    knowhere::TimeRecorder tc("CopyToGpu");
    knowhere::cloner::CopyCpuToGpu(cpu_idx, device_id, knowhere::Config());
    auto without_seal = tc.RecordSection("Without seal");
    cpu_idx->Seal();
    tc.RecordSection("seal cost");
    knowhere::cloner::CopyCpuToGpu(cpu_idx, device_id, knowhere::Config());
    auto with_seal = tc.RecordSection("With seal");
    ASSERT_GE(without_seal, with_seal);
}
#endif

class GPURESTEST : public DataGen, public ::testing::Test {
 protected:
    void
    SetUp() override {
        Generate(128, 1000000, 1000);
        knowhere::FaissGpuResourceMgr::GetInstance().InitDevice(device_id, 1024 * 1024 * 200, 1024 * 1024 * 300, 2);

        k = 100;
        elems = nq * k;
        ids = (int64_t*)malloc(sizeof(int64_t) * elems);
        dis = (float*)malloc(sizeof(float) * elems);
    }

    void
    TearDown() override {
        delete ids;
        delete dis;
        knowhere::FaissGpuResourceMgr::GetInstance().Free();
    }

 protected:
    std::string index_type;
    knowhere::IVFIndexPtr index_ = nullptr;

    int64_t* ids = nullptr;
    float* dis = nullptr;
    int64_t elems = 0;
};

const int search_count = 18;
const int load_count = 3;

TEST_F(GPURESTEST, gpu_ivf_resource_test) {
    assert(!xb.empty());

    {
        index_ = std::make_shared<knowhere::GPUIVF>(-1);
        ASSERT_EQ(std::dynamic_pointer_cast<knowhere::GPUIVF>(index_)->GetGpuDevice(), -1);
        std::dynamic_pointer_cast<knowhere::GPUIVF>(index_)->SetGpuDevice(device_id);
        ASSERT_EQ(std::dynamic_pointer_cast<knowhere::GPUIVF>(index_)->GetGpuDevice(), device_id);

        auto conf = std::make_shared<knowhere::IVFCfg>();
        conf->nlist = 1638;
        conf->d = dim;
        conf->gpu_id = device_id;
        conf->metric_type = knowhere::METRICTYPE::L2;
        conf->k = k;
        conf->nprobe = 1;

        auto preprocessor = index_->BuildPreprocessor(base_dataset, conf);
        index_->set_preprocessor(preprocessor);
        auto model = index_->Train(base_dataset, conf);
        index_->set_index_model(model);
        index_->Add(base_dataset, conf);
        EXPECT_EQ(index_->Count(), nb);
        EXPECT_EQ(index_->Dimension(), dim);

        knowhere::TimeRecorder tc("knowere GPUIVF");
        for (int i = 0; i < search_count; ++i) {
            index_->Search(query_dataset, conf);
            if (i > search_count - 6 || i < 5)
                tc.RecordSection("search once");
        }
        tc.ElapseFromBegin("search all");
    }
    knowhere::FaissGpuResourceMgr::GetInstance().Dump();

    {
        // IVF-Search
        faiss::gpu::StandardGpuResources res;
        faiss::gpu::GpuIndexIVFFlatConfig idx_config;
        idx_config.device = device_id;
        faiss::gpu::GpuIndexIVFFlat device_index(&res, dim, 1638, faiss::METRIC_L2, idx_config);
        device_index.train(nb, xb.data());
        device_index.add(nb, xb.data());

        knowhere::TimeRecorder tc("ori IVF");
        for (int i = 0; i < search_count; ++i) {
            device_index.search(nq, xq.data(), k, dis, ids);
            if (i > search_count - 6 || i < 5)
                tc.RecordSection("search once");
        }
        tc.ElapseFromBegin("search all");
    }
}

#ifdef CUSTOMIZATION
TEST_F(GPURESTEST, gpuivfsq) {
    {
        // knowhere gpu ivfsq
        index_type = "GPUIVFSQ";
        index_ = IndexFactory(index_type);

        auto conf = std::make_shared<knowhere::IVFSQCfg>();
        conf->nlist = 1638;
        conf->d = dim;
        conf->gpu_id = device_id;
        conf->metric_type = knowhere::METRICTYPE::L2;
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

        auto cpu_idx = knowhere::cloner::CopyGpuToCpu(index_, knowhere::Config());
        cpu_idx->Seal();

        knowhere::TimeRecorder tc("knowhere GPUSQ8");
        auto search_idx = knowhere::cloner::CopyCpuToGpu(cpu_idx, device_id, knowhere::Config());
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
        auto device_index = faiss::gpu::index_cpu_to_gpu(&res, device_id, ori_index);
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

        knowhere::TimeRecorder tc("ori GPUSQ8");
        faiss::Index* search_idx = faiss::gpu::index_cpu_to_gpu(&res, device_id, cpu_index, &option);
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

TEST_F(GPURESTEST, copyandsearch) {
    // search and copy at the same time
    printf("==================\n");

    index_type = "GPUIVF";
    index_ = IndexFactory(index_type);

    auto conf = std::make_shared<knowhere::IVFSQCfg>();
    conf->nlist = 1638;
    conf->d = dim;
    conf->gpu_id = device_id;
    conf->metric_type = knowhere::METRICTYPE::L2;
    conf->k = k;
    conf->nbits = 8;
    conf->nprobe = 1;

    auto preprocessor = index_->BuildPreprocessor(base_dataset, conf);
    index_->set_preprocessor(preprocessor);
    auto model = index_->Train(base_dataset, conf);
    index_->set_index_model(model);
    index_->Add(base_dataset, conf);
    //    auto result = index_->Search(query_dataset, conf);
    //    AssertAnns(result, nq, k);

    auto cpu_idx = knowhere::cloner::CopyGpuToCpu(index_, knowhere::Config());
    cpu_idx->Seal();

    auto search_idx = knowhere::cloner::CopyCpuToGpu(cpu_idx, device_id, knowhere::Config());

    auto search_func = [&] {
        // TimeRecorder tc("search&load");
        for (int i = 0; i < search_count; ++i) {
            search_idx->Search(query_dataset, conf);
            // if (i > search_count - 6 || i == 0)
            //    tc.RecordSection("search once");
        }
        // tc.ElapseFromBegin("search finish");
    };
    auto load_func = [&] {
        // TimeRecorder tc("search&load");
        for (int i = 0; i < load_count; ++i) {
            knowhere::cloner::CopyCpuToGpu(cpu_idx, device_id, knowhere::Config());
            // if (i > load_count -5 || i < 5)
            // tc.RecordSection("Copy to gpu");
        }
        // tc.ElapseFromBegin("load finish");
    };

    knowhere::TimeRecorder tc("basic");
    knowhere::cloner::CopyCpuToGpu(cpu_idx, device_id, knowhere::Config());
    tc.RecordSection("Copy to gpu once");
    search_idx->Search(query_dataset, conf);
    tc.RecordSection("search once");
    search_func();
    tc.RecordSection("only search total");
    load_func();
    tc.RecordSection("only copy total");

    std::thread search_thread(search_func);
    std::thread load_thread(load_func);
    search_thread.join();
    load_thread.join();
    tc.RecordSection("Copy&search total");
}

TEST_F(GPURESTEST, TrainAndSearch) {
    index_type = "GPUIVF";
    index_ = IndexFactory(index_type);

    auto conf = std::make_shared<knowhere::IVFSQCfg>();
    conf->nlist = 1638;
    conf->d = dim;
    conf->gpu_id = device_id;
    conf->metric_type = knowhere::METRICTYPE::L2;
    conf->k = k;
    conf->nbits = 8;
    conf->nprobe = 1;

    auto preprocessor = index_->BuildPreprocessor(base_dataset, conf);
    index_->set_preprocessor(preprocessor);
    auto model = index_->Train(base_dataset, conf);
    auto new_index = IndexFactory(index_type);
    new_index->set_index_model(model);
    new_index->Add(base_dataset, conf);
    auto cpu_idx = knowhere::cloner::CopyGpuToCpu(new_index, knowhere::Config());
    cpu_idx->Seal();
    auto search_idx = knowhere::cloner::CopyCpuToGpu(cpu_idx, device_id, knowhere::Config());

    constexpr int train_count = 1;
    constexpr int search_count = 5000;
    auto train_stage = [&] {
        for (int i = 0; i < train_count; ++i) {
            auto model = index_->Train(base_dataset, conf);
            auto test_idx = IndexFactory(index_type);
            test_idx->set_index_model(model);
            test_idx->Add(base_dataset, conf);
        }
    };
    auto search_stage = [&](knowhere::VectorIndexPtr& search_idx) {
        for (int i = 0; i < search_count; ++i) {
            auto result = search_idx->Search(query_dataset, conf);
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
        auto search_idx_2 = knowhere::cloner::CopyCpuToGpu(cpu_idx, device_id, knowhere::Config());
        std::thread search_1(search_stage, std::ref(search_idx));
        std::thread search_2(search_stage, std::ref(search_idx_2));
        search_1.join();
        search_2.join();
    }
}

// TODO(lxj): Add exception test
