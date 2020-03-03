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

#include <gtest/gtest.h>
#include <memory>

#include "knowhere/common/Exception.h"
#include "knowhere/index/vector_index/FaissBaseIndex.h"
#include "knowhere/index/vector_index/IndexNSG.h"
#ifdef MILVUS_GPU_VERSION
#include "knowhere/index/vector_index/IndexGPUIDMAP.h"
#include "knowhere/index/vector_index/helpers/Cloner.h"
#include "knowhere/index/vector_index/helpers/FaissGpuResourceMgr.h"
#endif

#include "knowhere/common/Timer.h"
#include "knowhere/index/vector_index/nsg/NSGIO.h"

#include <fiu-control.h>
#include <fiu-local.h>
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
        tmp_conf->d = 256;
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
    fiu_init(0);
    // untrained index
    {
        ASSERT_ANY_THROW(index_->Search(query_dataset, search_conf));
        ASSERT_ANY_THROW(index_->Serialize());
    }
    train_conf->gpu_id = knowhere::INVALID_VALUE;
    auto model_invalid_gpu = index_->Train(base_dataset, train_conf);
    train_conf->gpu_id = DEVICEID;
    auto model = index_->Train(base_dataset, train_conf);
    auto result = index_->Search(query_dataset, search_conf);
    AssertAnns(result, nq, k);

    auto binaryset = index_->Serialize();
    {
        fiu_enable("NSG.Serialize.throw_exception", 1, nullptr, 0);
        ASSERT_ANY_THROW(index_->Serialize());
        fiu_disable("NSG.Serialize.throw_exception");
    }

    auto new_index = std::make_shared<knowhere::NSG>();
    new_index->Load(binaryset);
    {
        fiu_enable("NSG.Load.throw_exception", 1, nullptr, 0);
        ASSERT_ANY_THROW(new_index->Load(binaryset));
        fiu_disable("NSG.Load.throw_exception");
    }

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

//#include <src/index/knowhere/knowhere/index/vector_index/nsg/OriNSG.h>
// TEST(test, ori_nsg) {
//    //    float* p_data = nullptr;
//    size_t rows, dim;
//    char* filename = "/mnt/112d53a6-5592-4360-a33b-7fd789456fce/workspace/Data/sift/sift_base.fvecs";
//    //    loads_data(filename, p_data, rows, dim);
//    float* p_data = fvecs_read(filename, &dim, &rows);
//
//    std::string knng_filename =
//        "/mnt/112d53a6-5592-4360-a33b-7fd789456fce/workspace/Cellar/anns/efanna_graph/tests/sift.1M.50NN.graph";
//    std::vector<std::vector<int64_t>> knng;
//    Load_nns_graph(knng, knng_filename.c_str());
//
//    //    float* search_data = nullptr;
//    size_t nq, search_dim;
//    char* searchfile = "/mnt/112d53a6-5592-4360-a33b-7fd789456fce/workspace/Data/sift/sift_query.fvecs";
//    //    loads_data(searchfile, search_data, nq, search_dim);
//    float* search_data = fvecs_read(searchfile, &search_dim, &nq);
//    assert(search_dim == dim);
//
//    size_t k, nq2;
//    char* gtfile = "/mnt/112d53a6-5592-4360-a33b-7fd789456fce/workspace/Data/sift/sift_groundtruth.ivecs";
//    int* gt_int = ivecs_read(gtfile, &k, &nq2);
//    int64_t* gt = new int64_t[k * nq2];
//    for (int i = 0; i < k * nq2; i++) {
//        gt[i] = gt_int[i];
//    }
//    delete[] gt_int;
//
//    std::vector<int64_t> store_ids(rows);
//    for (int i = 0; i < rows; ++i) {
//        store_ids[i] = i;
//    }
//
//    int64_t* I = new int64_t[nq * k];
//    float* D = new float[nq * k];
//#if 0
//        efanna2e::Parameters params;
//        params.Set<int64_t>("L", 50);
//        params.Set<int64_t>("R", 55);
//        params.Set<int64_t>("C", 300);
//        auto orinsg = std::make_shared<efanna2e::IndexNSG>(dim, rows, efanna2e::Metric::L2, nullptr);
//        orinsg->Load_nn_graph(knng);
//        orinsg->Build(rows, (float*)p_data, params);
//
//        efanna2e::Parameters paras;
//        paras.Set<unsigned>("L_search", 45);
//        paras.Set<unsigned>("P_search",100);
//        k = 10;
//        std::vector<std::vector<int64_t> > res;
//        for (unsigned i = 0; i < nq; i++) {
//            std::vector<int64_t> tmp(k);
//            orinsg->Search(search_data + i * dim, p_data, k, paras, tmp.data());
//            res.push_back(tmp);
//        }
//    }
//#else
//    knowhere::algo::BuildParams params;
//    params.search_length = 50;
//    params.out_degree = 55;
//    params.candidate_pool_size = 300;
//    auto nsg = std::make_shared<knowhere::algo::NsgIndex>(dim, rows);
//#if 1
//    knowhere::FaissGpuResourceMgr::GetInstance().InitDevice(DEVICEID, 1024 * 1024 * 200, 1024 * 1024 * 600, 2);
//    auto dataset = generate_dataset(int64_t(rows), int64_t(dim), p_data, store_ids.data());
//    auto config = std::make_shared<knowhere::IVFCfg>();
//    config->d = dim;
//    config->gpu_id = 0;
//    config->metric_type = knowhere::METRICTYPE::L2;
//    auto preprocess_index = std::make_shared<knowhere::IDMAP>();
//    preprocess_index->Train(config);
//    preprocess_index->AddWithoutId(dataset, config);
//    auto xx = knowhere::cloner::CopyCpuToGpu(preprocess_index, 0, config);
//    auto ss = std::dynamic_pointer_cast<knowhere::GPUIDMAP>(xx);
//
//    std::vector<std::vector<int64_t>> kng;
//    ss->GenGraph(p_data, 50, kng, config);
//    nsg->SetKnnGraph(kng);
//    knowhere::FaissGpuResourceMgr::GetInstance().Free();
//#else
//    nsg->SetKnnGraph(knng);
//#endif
//    nsg->Build_with_ids(rows, (float*)p_data, store_ids.data(), params);
//    knowhere::algo::SearchParams s_params;
//    s_params.search_length = 45;
//    nsg->Search(search_data, nq, dim, k, D, I, s_params);
//#endif
//
//    int n_1 = 0, n_10 = 0, n_100 = 0;
//    for (int i = 0; i < nq; i++) {
//        int gt_nn = gt[i * k];
//        for (int j = 0; j < k; j++) {
//            if (I[i * k + j] == gt_nn) {
//                if (j < 1)
//                    n_1++;
//                if (j < 10)
//                    n_10++;
//                if (j < 100)
//                    n_100++;
//            }
//        }
//    }
//    printf("R@1 = %.4f\n", n_1 / float(nq));
//    printf("R@10 = %.4f\n", n_10 / float(nq));
//    printf("R@100 = %.4f\n", n_100 / float(nq));
//}
//
// TEST(testxx, test_idmap){
//    int k = 50;
//    std::string knng_filename =
//        "/mnt/112d53a6-5592-4360-a33b-7fd789456fce/workspace/Cellar/anns/efanna_graph/tests/sift.50NN.graph";
//    std::vector<std::vector<int64_t>> gt_knng;
//    Load_nns_graph(gt_knng, knng_filename.c_str());
//
//    size_t rows, dim;
//    char* filename =
//    "/mnt/112d53a6-5592-4360-a33b-7fd789456fce/workspace/Cellar/anns/efanna_graph/tests/siftsmall/siftsmall_base.fvecs";
//    float* p_data = fvecs_read(filename, &dim, &rows);
//
//    std::vector<int64_t> store_ids(rows);
//    for (int i = 0; i < rows; ++i) {
//        store_ids[i] = i;
//    }
//
//    knowhere::FaissGpuResourceMgr::GetInstance().InitDevice(DEVICEID, 1024 * 1024 * 200, 1024 * 1024 * 600, 2);
//    auto dataset = generate_dataset(int64_t(rows), int64_t(dim), p_data, store_ids.data());
//    auto config = std::make_shared<knowhere::IVFCfg>();
//    config->d = dim;
//    config->gpu_id = 0;
//    config->metric_type = knowhere::METRICTYPE::L2;
//    auto preprocess_index = std::make_shared<knowhere::IDMAP>();
//    preprocess_index->Train(config);
//    preprocess_index->AddWithoutId(dataset, config);
//    auto xx = knowhere::cloner::CopyCpuToGpu(preprocess_index, 0, config);
//    auto ss = std::dynamic_pointer_cast<knowhere::GPUIDMAP>(xx);
//    std::vector<std::vector<int64_t>> idmap_knng;
//    ss->GenGraph(p_data, k, idmap_knng,config);
//    knowhere::FaissGpuResourceMgr::GetInstance().Free();
//
//    int n_1 = 0, n_10 = 0, n_100 = 0;
//    for (int i = 0; i < rows; i++) {
//        int gt_nn = gt_knng[i][0];
//        int l_n_1 = 0;
//        int l_n_10 = 0;
//        int l_n_100 = 0;
//        for (int j = 0; j < k; j++) {
//            if (idmap_knng[i][j] == gt_nn) {
//                if (j < 1){
//                    n_1++;
//                    l_n_1++;
//                }
//                if (j < 10){
//                    n_10++;
//                    l_n_10++;
//                }
//                if (j < 100){
//                    n_100++;
//                    l_n_100++;
//                }
//
//            }
//            if ((j == k-1) && (l_n_100 == 0)){
//                std::cout << "error id: " << i << std::endl;
//            }
//        }
//    }
//    printf("R@1 = %.4f\n", n_1 / float(rows));
//    printf("R@10 = %.4f\n", n_10 / float(rows));
//    printf("R@100 = %.4f\n", n_100 / float(rows));
//}
