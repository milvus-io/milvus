////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include <gtest/gtest.h>

#include <iostream>
#include <thread>

#include <faiss/AutoTune.h>
#include <faiss/gpu/GpuAutoTune.h>
#include <faiss/gpu/GpuIndexIVFFlat.h>

#include "knowhere/index/vector_index/gpu_ivf.h"
#include "knowhere/index/vector_index/ivf.h"
#include "knowhere/adapter/structure.h"
#include "knowhere/index/vector_index/cloner.h"
#include "knowhere/common/exception.h"
#include "knowhere/common/timer.h"

#include "utils.h"


using namespace zilliz::knowhere;

using ::testing::TestWithParam;
using ::testing::Values;
using ::testing::Combine;

static int device_id = 0;
IVFIndexPtr IndexFactory(const std::string &type) {
    if (type == "IVF") {
        return std::make_shared<IVF>();
    } else if (type == "IVFPQ") {
        return std::make_shared<IVFPQ>();
    } else if (type == "GPUIVF") {
        return std::make_shared<GPUIVF>(device_id);
    } else if (type == "GPUIVFPQ") {
        return std::make_shared<GPUIVFPQ>(device_id);
    } else if (type == "IVFSQ") {
        return std::make_shared<IVFSQ>();
    } else if (type == "GPUIVFSQ") {
        return std::make_shared<GPUIVFSQ>(device_id);
    }
}

class IVFTest
    : public DataGen, public TestWithParam<::std::tuple<std::string, Config, Config, Config, Config>> {
 protected:
    void SetUp() override {
        std::tie(index_type, preprocess_cfg, train_cfg, add_cfg, search_cfg) = GetParam();
        //Init_with_default();
        Generate(128, 1000000/100, 10);
        index_ = IndexFactory(index_type);
        FaissGpuResourceMgr::GetInstance().InitDevice(device_id, 1024*1024*200, 1024*1024*600, 2);
    }
    void TearDown() override {
        FaissGpuResourceMgr::GetInstance().Free();
    }

 protected:
    std::string index_type;
    Config preprocess_cfg;
    Config train_cfg;
    Config add_cfg;
    Config search_cfg;
    IVFIndexPtr index_ = nullptr;
};


INSTANTIATE_TEST_CASE_P(IVFParameters, IVFTest,
                        Values(
                            std::make_tuple("IVF",
                                            Config(),
                                            Config::object{{"nlist", 100}, {"metric_type", "L2"}},
                                            Config(),
                                            Config::object{{"k", 10}}),
                            //std::make_tuple("IVFPQ",
                            //                Config(),
                            //                Config::object{{"nlist", 100}, {"M", 8}, {"nbits", 8}, {"metric_type", "L2"}},
                            //                Config(),
                            //                Config::object{{"k", 10}}),
                            std::make_tuple("GPUIVF",
                                            Config(),
                                            Config::object{{"nlist", 100}, {"gpu_id", device_id}, {"metric_type", "L2"}},
                                            Config(),
                                            Config::object{{"k", 10}}),
                            //std::make_tuple("GPUIVFPQ",
                            //                Config(),
                            //                Config::object{{"gpu_id", device_id}, {"nlist", 100}, {"M", 8}, {"nbits", 8}, {"metric_type", "L2"}},
                            //                Config(),
                            //                Config::object{{"k", 10}}),
                            std::make_tuple("IVFSQ",
                                            Config(),
                                            Config::object{{"nlist", 100}, {"nbits", 8}, {"metric_type", "L2"}},
                                            Config(),
                                            Config::object{{"k", 10}}),
                            std::make_tuple("GPUIVFSQ",
                                            Config(),
                                            Config::object{{"gpu_id", device_id}, {"nlist", 100}, {"nbits", 8}, {"metric_type", "L2"}},
                                            Config(),
                                            Config::object{{"k", 10}})
                        )
);

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

TEST_P(IVFTest, ivf_basic) {
    assert(!xb.empty());

    auto preprocessor = index_->BuildPreprocessor(base_dataset, preprocess_cfg);
    index_->set_preprocessor(preprocessor);

    auto model = index_->Train(base_dataset, train_cfg);
    index_->set_index_model(model);
    index_->Add(base_dataset, add_cfg);
    EXPECT_EQ(index_->Count(), nb);
    EXPECT_EQ(index_->Dimension(), dim);
    auto result = index_->Search(query_dataset, search_cfg);
    AssertAnns(result, nq, k);
    //PrintResult(result, nq, k);
}

//TEST_P(IVFTest, gpu_to_cpu) {
//    if (index_type.find("GPU") == std::string::npos) { return; }
//
//    // else
//    assert(!xb.empty());
//
//    auto preprocessor = index_->BuildPreprocessor(base_dataset, preprocess_cfg);
//    index_->set_preprocessor(preprocessor);
//
//    auto model = index_->Train(base_dataset, train_cfg);
//    index_->set_index_model(model);
//    index_->Add(base_dataset, add_cfg);
//    EXPECT_EQ(index_->Count(), nb);
//    EXPECT_EQ(index_->Dimension(), dim);
//    auto result = index_->Search(query_dataset, search_cfg);
//    AssertAnns(result, nq, k);
//
//    if (auto device_index = std::dynamic_pointer_cast<GPUIVF>(index_)) {
//        auto host_index = device_index->Copy_index_gpu_to_cpu();
//        auto result = host_index->Search(query_dataset, search_cfg);
//        AssertAnns(result, nq, k);
//    }
//}

TEST_P(IVFTest, ivf_serialize) {
    auto serialize = [](const std::string &filename, BinaryPtr &bin, uint8_t *ret) {
        FileIOWriter writer(filename);
        writer(static_cast<void *>(bin->data.get()), bin->size);

        FileIOReader reader(filename);
        reader(ret, bin->size);
    };

    {
        // serialize index-model
        auto model = index_->Train(base_dataset, train_cfg);
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
        index_->Add(base_dataset, add_cfg);
        auto result = index_->Search(query_dataset, search_cfg);
        AssertAnns(result, nq, k);
    }

    {
        // serialize index
        auto model = index_->Train(base_dataset, train_cfg);
        index_->set_index_model(model);
        index_->Add(base_dataset, add_cfg);
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
        auto result = index_->Search(query_dataset, search_cfg);
        AssertAnns(result, nq, k);
    }
}

TEST_P(IVFTest, clone_test) {
    assert(!xb.empty());

    auto preprocessor = index_->BuildPreprocessor(base_dataset, preprocess_cfg);
    index_->set_preprocessor(preprocessor);

    auto model = index_->Train(base_dataset, train_cfg);
    index_->set_index_model(model);
    index_->Add(base_dataset, add_cfg);
    EXPECT_EQ(index_->Count(), nb);
    EXPECT_EQ(index_->Dimension(), dim);
    auto result = index_->Search(query_dataset, search_cfg);
    AssertAnns(result, nq, k);
    //PrintResult(result, nq, k);

    auto AssertEqual = [&] (DatasetPtr p1, DatasetPtr p2) {
        auto ids_p1 = p1->array()[0];
        auto ids_p2 = p2->array()[0];

        for (int i = 0; i < nq * k; ++i) {
            EXPECT_EQ(*(ids_p2->data()->GetValues<int64_t>(1, i)),
                      *(ids_p1->data()->GetValues<int64_t>(1, i)));
        }
    };

    {
        // clone in place
        std::vector<std::string> support_idx_vec{"IVF", "GPUIVF", "IVFPQ", "IVFSQ", "GPUIVFSQ"};
        auto finder = std::find(support_idx_vec.cbegin(), support_idx_vec.cend(), index_type);
        if (finder != support_idx_vec.cend()) {
            EXPECT_NO_THROW({
                                auto clone_index = index_->Clone();
                                auto clone_result = clone_index->Search(query_dataset, search_cfg);
                                //AssertAnns(result, nq, k);
                                AssertEqual(result, clone_result);
                                std::cout << "inplace clone [" << index_type << "] success" << std::endl;
                            });
        } else {
            EXPECT_THROW({
                             std::cout << "inplace clone [" << index_type << "] failed" << std::endl;
                             auto clone_index = index_->Clone();
                         }, KnowhereException);
        }
    }

    {
        // copy from gpu to cpu
        std::vector<std::string> support_idx_vec{"GPUIVF", "GPUIVFSQ"};
        auto finder = std::find(support_idx_vec.cbegin(), support_idx_vec.cend(), index_type);
        if (finder != support_idx_vec.cend()) {
            EXPECT_NO_THROW({
                                auto clone_index = CopyGpuToCpu(index_, Config());
                                auto clone_result = clone_index->Search(query_dataset, search_cfg);
                                AssertEqual(result, clone_result);
                                std::cout << "clone G <=> C [" << index_type << "] success" << std::endl;
                            });
        } else {
            EXPECT_THROW({
                             std::cout << "clone G <=> C [" << index_type << "] failed" << std::endl;
                             auto clone_index = CopyGpuToCpu(index_, Config());
                         }, KnowhereException);
        }
    }

    {
        // copy to gpu
        std::vector<std::string> support_idx_vec{"IVF", "GPUIVF", "IVFSQ", "GPUIVFSQ"};
        auto finder = std::find(support_idx_vec.cbegin(), support_idx_vec.cend(), index_type);
        if (finder != support_idx_vec.cend()) {
            EXPECT_NO_THROW({
                                auto clone_index = CopyCpuToGpu(index_, device_id, Config());
                                auto clone_result = clone_index->Search(query_dataset, search_cfg);
                                AssertEqual(result, clone_result);
                                std::cout << "clone C <=> G [" << index_type << "] success" << std::endl;
                            });
        } else {
            EXPECT_THROW({
                             std::cout << "clone C <=> G [" << index_type << "] failed" << std::endl;
                             auto clone_index = CopyCpuToGpu(index_, device_id, Config());
                         }, KnowhereException);
        }
    }
}

TEST_P(IVFTest, seal_test) {
    //FaissGpuResourceMgr::GetInstance().InitDevice(device_id);

    std::vector<std::string> support_idx_vec{"GPUIVF", "GPUIVFSQ"};
    auto finder = std::find(support_idx_vec.cbegin(), support_idx_vec.cend(), index_type);
    if (finder == support_idx_vec.cend()) {
        return;
    }

    assert(!xb.empty());

    //index_ = std::make_shared<GPUIVF>(0);
    auto preprocessor = index_->BuildPreprocessor(base_dataset, preprocess_cfg);
    index_->set_preprocessor(preprocessor);

    auto model = index_->Train(base_dataset, train_cfg);
    index_->set_index_model(model);
    index_->Add(base_dataset, add_cfg);
    EXPECT_EQ(index_->Count(), nb);
    EXPECT_EQ(index_->Dimension(), dim);
    auto result = index_->Search(query_dataset, search_cfg);
    AssertAnns(result, nq, k);

    auto cpu_idx = CopyGpuToCpu(index_, Config());

    TimeRecorder tc("CopyToGpu");
    CopyCpuToGpu(cpu_idx, device_id, Config());
    auto without_seal = tc.RecordSection("Without seal");
    cpu_idx->Seal();
    tc.RecordSection("seal cost");
    CopyCpuToGpu(cpu_idx, device_id, Config());
    auto with_seal = tc.RecordSection("With seal");
    ASSERT_GE(without_seal, with_seal);
}


class GPURESTEST
    : public DataGen, public ::testing::Test {
 protected:
    void SetUp() override {
        //std::tie(index_type, preprocess_cfg, train_cfg, add_cfg, search_cfg) = GetParam();
        //Init_with_default();
        Generate(128, 1000000, 1000);
        k = 100;
        //index_ = IndexFactory(index_type);
        FaissGpuResourceMgr::GetInstance().InitDevice(device_id, 1024*1024*200, 1024*1024*300, 2);

        elems = nq * k;
        ids = (int64_t *) malloc(sizeof(int64_t) * elems);
        dis = (float *) malloc(sizeof(float) * elems);
    }

    void TearDown() override {
        delete ids;
        delete dis;
        FaissGpuResourceMgr::GetInstance().Free();
    }

 protected:
    std::string index_type;
    Config preprocess_cfg;
    Config train_cfg;
    Config add_cfg;
    Config search_cfg;
    IVFIndexPtr index_ = nullptr;

    int64_t *ids = nullptr;
    float *dis = nullptr;
    int64_t elems = 0;
};

const int search_count = 18;
const int load_count = 3;

TEST_F(GPURESTEST, gpu_ivf_resource_test) {
    assert(!xb.empty());


    {
        index_ =  std::make_shared<GPUIVF>(-1);
        ASSERT_EQ(std::dynamic_pointer_cast<GPUIVF>(index_)->GetGpuDevice(), -1);
        std::dynamic_pointer_cast<GPUIVF>(index_)->SetGpuDevice(device_id);
        ASSERT_EQ(std::dynamic_pointer_cast<GPUIVF>(index_)->GetGpuDevice(), device_id);

        auto preprocessor = index_->BuildPreprocessor(base_dataset, preprocess_cfg);
        index_->set_preprocessor(preprocessor);
        train_cfg = Config::object{{"nlist", 1638}, {"gpu_id", device_id}, {"metric_type", "L2"}};
        auto model = index_->Train(base_dataset, train_cfg);
        index_->set_index_model(model);
        index_->Add(base_dataset, add_cfg);
        EXPECT_EQ(index_->Count(), nb);
        EXPECT_EQ(index_->Dimension(), dim);

        search_cfg  = Config::object{{"k", k}};
        TimeRecorder tc("knowere GPUIVF");
        for (int i = 0; i < search_count; ++i) {
            index_->Search(query_dataset, search_cfg);
            if (i > search_count - 6 || i < 5)
                tc.RecordSection("search once");
        }
        tc.ElapseFromBegin("search all");
    }
    FaissGpuResourceMgr::GetInstance().Dump();

    {
        // IVF-Search
        faiss::gpu::StandardGpuResources res;
        faiss::gpu::GpuIndexIVFFlatConfig idx_config;
        idx_config.device = device_id;
        faiss::gpu::GpuIndexIVFFlat device_index(&res, dim, 1638, faiss::METRIC_L2, idx_config);
        device_index.train(nb, xb.data());
        device_index.add(nb, xb.data());

        TimeRecorder tc("ori IVF");
        for (int i = 0; i < search_count; ++i) {
            device_index.search(nq, xq.data(), k, dis, ids);
            if (i > search_count - 6 || i < 5)
                tc.RecordSection("search once");
        }
        tc.ElapseFromBegin("search all");
    }

}

TEST_F(GPURESTEST, gpuivfsq) {
    {
        // knowhere gpu ivfsq
        index_type = "GPUIVFSQ";
        index_ = IndexFactory(index_type);
        auto preprocessor = index_->BuildPreprocessor(base_dataset, preprocess_cfg);
        index_->set_preprocessor(preprocessor);
        train_cfg = Config::object{{"gpu_id", device_id}, {"nlist", 1638}, {"nbits", 8}, {"metric_type", "L2"}};
        auto model = index_->Train(base_dataset, train_cfg);
        index_->set_index_model(model);
        index_->Add(base_dataset, add_cfg);
        search_cfg  = Config::object{{"k", k}};
        auto result = index_->Search(query_dataset, search_cfg);
        AssertAnns(result, nq, k);

        auto cpu_idx = CopyGpuToCpu(index_, Config());
        cpu_idx->Seal();

        TimeRecorder tc("knowhere GPUSQ8");
        auto search_idx = CopyCpuToGpu(cpu_idx, device_id, Config());
        tc.RecordSection("Copy to gpu");
        for (int i = 0; i < search_count; ++i) {
            search_idx->Search(query_dataset, search_cfg);
            if (i > search_count - 6 || i < 5)
                tc.RecordSection("search once");
        }
        tc.ElapseFromBegin("search all");
    }

    {
        // Ori gpuivfsq Test
        const char *index_description = "IVF1638,SQ8";
        faiss::Index *ori_index = faiss::index_factory(dim, index_description, faiss::METRIC_L2);

        faiss::gpu::StandardGpuResources res;
        auto device_index = faiss::gpu::index_cpu_to_gpu(&res, device_id, ori_index);
        device_index->train(nb, xb.data());
        device_index->add(nb, xb.data());

        auto cpu_index = faiss::gpu::index_gpu_to_cpu(device_index);
        auto idx = dynamic_cast<faiss::IndexIVF *>(cpu_index);
        if (idx != nullptr) {
            idx->to_readonly();
        }
        delete device_index;
        delete ori_index;

        faiss::gpu::GpuClonerOptions option;
        option.allInGpu = true;

        TimeRecorder tc("ori GPUSQ8");
        faiss::Index *search_idx = faiss::gpu::index_cpu_to_gpu(&res, device_id, cpu_index, &option);
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

TEST_F(GPURESTEST, copyandsearch) {
    printf("==================\n");

    // search and copy at the same time
    index_type = "GPUIVFSQ";
    //index_type = "GPUIVF";
    index_ = IndexFactory(index_type);
    auto preprocessor = index_->BuildPreprocessor(base_dataset, preprocess_cfg);
    index_->set_preprocessor(preprocessor);
    train_cfg = Config::object{{"gpu_id", device_id}, {"nlist", 1638}, {"nbits", 8}, {"metric_type", "L2"}};
    auto model = index_->Train(base_dataset, train_cfg);
    index_->set_index_model(model);
    index_->Add(base_dataset, add_cfg);
    search_cfg = Config::object{{"k", k}};
    auto result = index_->Search(query_dataset, search_cfg);
    AssertAnns(result, nq, k);

    auto cpu_idx = CopyGpuToCpu(index_, Config());
    cpu_idx->Seal();

    auto search_idx = CopyCpuToGpu(cpu_idx, device_id, Config());

    auto search_func = [&] {
        //TimeRecorder tc("search&load");
        for (int i = 0; i < search_count; ++i) {
            search_idx->Search(query_dataset, search_cfg);
            //if (i > search_count - 6 || i == 0)
            //    tc.RecordSection("search once");
        }
        //tc.ElapseFromBegin("search finish");
    };
    auto load_func = [&] {
        //TimeRecorder tc("search&load");
        for (int i = 0; i < load_count; ++i) {
            CopyCpuToGpu(cpu_idx, device_id, Config());
            //if (i > load_count -5 || i < 5)
                //tc.RecordSection("Copy to gpu");
        }
        //tc.ElapseFromBegin("load finish");
    };

    TimeRecorder tc("basic");
    CopyCpuToGpu(cpu_idx, device_id, Config());
    tc.RecordSection("Copy to gpu once");
    search_idx->Search(query_dataset, search_cfg);
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
    index_type = "GPUIVFSQ";
    //index_type = "GPUIVF";
    const int train_count = 1;
    const int search_count = 5000;

    index_ = IndexFactory(index_type);
    auto preprocessor = index_->BuildPreprocessor(base_dataset, preprocess_cfg);
    index_->set_preprocessor(preprocessor);
    train_cfg = Config::object{{"gpu_id", device_id}, {"nlist", 1638}, {"nbits", 8}, {"metric_type", "L2"}};
    auto model = index_->Train(base_dataset, train_cfg);
    auto new_index = IndexFactory(index_type);
    new_index->set_index_model(model);
    new_index->Add(base_dataset, add_cfg);
    auto cpu_idx = CopyGpuToCpu(new_index, Config());
    cpu_idx->Seal();
    auto search_idx = CopyCpuToGpu(cpu_idx, device_id, Config());

    auto train_stage = [&] {
        train_cfg = Config::object{{"gpu_id", device_id}, {"nlist", 1638}, {"nbits", 8}, {"metric_type", "L2"}};
        for (int i = 0; i < train_count; ++i) {
            auto model = index_->Train(base_dataset, train_cfg);
            auto test_idx = IndexFactory(index_type);
            test_idx->set_index_model(model);
            test_idx->Add(base_dataset, add_cfg);
        }
    };
    auto search_stage = [&](VectorIndexPtr& search_idx) {
        search_cfg = Config::object{{"k", k}};
        for (int i = 0; i < search_count; ++i) {
            auto result = search_idx->Search(query_dataset, search_cfg);
            AssertAnns(result, nq, k);
        }
    };

    //TimeRecorder tc("record");
    //train_stage();
    //tc.RecordSection("train cost");
    //search_stage(search_idx);
    //tc.RecordSection("search cost");

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
        auto search_idx_2 = CopyCpuToGpu(cpu_idx, device_id, Config());
        std::thread search_1(search_stage, std::ref(search_idx));
        std::thread search_2(search_stage, std::ref(search_idx_2));
        search_1.join();
        search_2.join();
    }
}



// TODO(linxj): Add exception test
