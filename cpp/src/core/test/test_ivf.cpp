////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include <gtest/gtest.h>

#include <iostream>
#include <sstream>

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

static int device_id = 1;
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
        Generate(128, 1000000/5, 10);
        index_ = IndexFactory(index_type);
        FaissGpuResourceMgr::GetInstance().InitDevice(device_id);
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
                                            Config::object{{"nlist", 1638}, {"gpu_id", device_id}, {"metric_type", "L2"}},
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
                                            Config::object{{"gpu_id", device_id}, {"nlist", 1638}, {"nbits", 8}, {"metric_type", "L2"}},
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

// TODO(linxj): Add exception test
