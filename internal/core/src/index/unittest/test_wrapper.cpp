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

#include "easyloggingpp/easylogging++.h"

#ifdef MILVUS_GPU_VERSION

#include "knowhere/index/vector_index/helpers/FaissGpuResourceMgr.h"
#include "wrapper/WrapperException.h"

#endif

#include <fiu-control.h>
#include <fiu/fiu-local.h>
#include <gtest/gtest.h>

#include "knowhere/index/vector_index/helpers/IndexParameter.h"
#include "wrapper/VecIndex.h"
#include "wrapper/utils.h"

INITIALIZE_EASYLOGGINGPP

using ::testing::Combine;
using ::testing::TestWithParam;
using ::testing::Values;

class KnowhereWrapperTest
    : public DataGenBase,
      public TestWithParam<::std::tuple<milvus::engine::IndexType, std::string, int, int, int, int>> {
 protected:
    void
    SetUp() override {
#ifdef MILVUS_GPU_VERSION
        knowhere::FaissGpuResourceMgr::GetInstance().InitDevice(DEVICEID, PINMEM, TEMPMEM, RESNUM);
#endif
        std::string generator_type;
        std::tie(index_type, generator_type, dim, nb, nq, k) = GetParam();
        GenData(dim, nb, nq, xb, xq, ids, k, gt_ids, gt_dis);

        knowhere::Config tempconf{{knowhere::Metric::TYPE, knowhere::Metric::L2},
                                  {knowhere::meta::ROWS, nb},
                                  {knowhere::meta::DIM, dim},
                                  {knowhere::meta::TOPK, k},
                                  {knowhere::meta::DEVICEID, DEVICEID}};

        index_ = GetVecIndexFactory(index_type);
        conf = ParamGenerator::GetInstance().GenBuild(index_type, tempconf);
        searchconf = ParamGenerator::GetInstance().GenSearchConf(index_type, tempconf);
    }

    void
    TearDown() override {
#ifdef MILVUS_GPU_VERSION
        knowhere::FaissGpuResourceMgr::GetInstance().Free();
#endif
    }

 protected:
    milvus::engine::IndexType index_type;
    milvus::engine::VecIndexPtr index_ = nullptr;
    knowhere::Config conf;
    knowhere::Config searchconf;
};

INSTANTIATE_TEST_CASE_P(
    WrapperParam,
    KnowhereWrapperTest,
    Values(
//["Index type", "Generator type", "dim", "nb", "nq", "k", "build config", "search config"]
#ifdef MILVUS_GPU_VERSION
        std::make_tuple(milvus::engine::IndexType::FAISS_IVFFLAT_GPU, "Default", DIM, NB, 10, 10),
        std::make_tuple(milvus::engine::IndexType::FAISS_IVFFLAT_MIX, "Default", 64, 1000, 10, 10),
        std::make_tuple(milvus::engine::IndexType::FAISS_IVFSQ8_GPU, "Default", DIM, NB, 10, 10),
        std::make_tuple(milvus::engine::IndexType::FAISS_IVFSQ8_MIX, "Default", DIM, NB, 10, 10),
        std::make_tuple(milvus::engine::IndexType::FAISS_IVFPQ_MIX, "Default", 64, 1000, 10, 10),
// std::make_tuple(milvus::engine::IndexType::NSG_MIX, "Default", 128, 250000, 10, 10),
#endif
        // std::make_tuple(milvus::engine::IndexType::SPTAG_KDT_RNT_CPU, "Default", 128, 100, 10, 10),
        // std::make_tuple(milvus::engine::IndexType::SPTAG_BKT_RNT_CPU, "Default", 126, 100, 10, 10),
        std::make_tuple(milvus::engine::IndexType::HNSW, "Default", 64, 10000, 5, 10),
        std::make_tuple(milvus::engine::IndexType::FAISS_IDMAP, "Default", 64, 1000, 10, 10),
        std::make_tuple(milvus::engine::IndexType::FAISS_IVFFLAT_CPU, "Default", 64, 1000, 10, 10),
        std::make_tuple(milvus::engine::IndexType::FAISS_IVFSQ8_CPU, "Default", DIM, NB, 10, 10)));

#ifdef MILVUS_GPU_VERSION
TEST_P(KnowhereWrapperTest, WRAPPER_EXCEPTION_TEST) {
    std::string err_msg = "failed";
    milvus::engine::WrapperException ex(err_msg);

    std::string msg = ex.what();
    EXPECT_EQ(msg, err_msg);
}

#endif

TEST_P(KnowhereWrapperTest, BASE_TEST) {
    EXPECT_EQ(index_->GetType(), index_type);

    auto elems = nq * k;
    std::vector<int64_t> res_ids(elems);
    std::vector<float> res_dis(elems);

    index_->BuildAll(nb, xb.data(), ids.data(), conf);
    index_->Search(nq, xq.data(), res_dis.data(), res_ids.data(), searchconf);
    AssertResult(res_ids, res_dis);

    {
        index_->GetDeviceId();

        fiu_init(0);
        fiu_enable("VecIndexImpl.BuildAll.throw_knowhere_exception", 1, nullptr, 0);
        fiu_enable("BFIndex.BuildAll.throw_knowhere_exception", 1, nullptr, 0);
        fiu_enable("IVFMixIndex.BuildAll.throw_knowhere_exception", 1, nullptr, 0);
        index_->BuildAll(nb, xb.data(), ids.data(), conf);
        fiu_disable("IVFMixIndex.BuildAll.throw_knowhere_exception");
        fiu_disable("BFIndex.BuildAll.throw_knowhere_exception");
        fiu_disable("VecIndexImpl.BuildAll.throw_knowhere_exception");

        fiu_enable("VecIndexImpl.BuildAll.throw_std_exception", 1, nullptr, 0);
        fiu_enable("BFIndex.BuildAll.throw_std_exception", 1, nullptr, 0);
        fiu_enable("IVFMixIndex.BuildAll.throw_std_exception", 1, nullptr, 0);
        index_->BuildAll(nb, xb.data(), ids.data(), conf);
        fiu_disable("IVFMixIndex.BuildAll.throw_std_exception");
        fiu_disable("BFIndex.BuildAll.throw_std_exception");
        fiu_disable("VecIndexImpl.BuildAll.throw_std_exception");

        fiu_enable("VecIndexImpl.Add.throw_knowhere_exception", 1, nullptr, 0);
        index_->Add(nb, xb.data(), ids.data());
        fiu_disable("VecIndexImpl.Add.throw_knowhere_exception");

        fiu_enable("VecIndexImpl.Add.throw_std_exception", 1, nullptr, 0);
        index_->Add(nb, xb.data(), ids.data());
        fiu_disable("VecIndexImpl.Add.throw_std_exception");

        fiu_enable("VecIndexImpl.Search.throw_knowhere_exception", 1, nullptr, 0);
        index_->Search(nq, xq.data(), res_dis.data(), res_ids.data(), searchconf);
        fiu_disable("VecIndexImpl.Search.throw_knowhere_exception");

        fiu_enable("VecIndexImpl.Search.throw_std_exception", 1, nullptr, 0);
        index_->Search(nq, xq.data(), res_dis.data(), res_ids.data(), searchconf);
        fiu_disable("VecIndexImpl.Search.throw_std_exception");
    }
}

#ifdef MILVUS_GPU_VERSION
TEST_P(KnowhereWrapperTest, TO_GPU_TEST) {
    if (index_type == milvus::engine::IndexType::HNSW) {
        return;
    }
    EXPECT_EQ(index_->GetType(), index_type);

    auto elems = nq * k;
    std::vector<int64_t> res_ids(elems);
    std::vector<float> res_dis(elems);

    index_->BuildAll(nb, xb.data(), ids.data(), conf);
    index_->Search(nq, xq.data(), res_dis.data(), res_ids.data(), searchconf);
    AssertResult(res_ids, res_dis);

    {
        auto dev_idx = index_->CopyToGpu(DEVICEID);
        for (int i = 0; i < 10; ++i) {
            dev_idx->Search(nq, xq.data(), res_dis.data(), res_ids.data(), searchconf);
        }
        AssertResult(res_ids, res_dis);
    }

    {
        std::string file_location = "/tmp/knowhere_gpu_file";
        write_index(index_, file_location);
        auto new_index = milvus::engine::read_index(file_location);

        auto dev_idx = new_index->CopyToGpu(DEVICEID);
        for (int i = 0; i < 10; ++i) {
            dev_idx->Search(nq, xq.data(), res_dis.data(), res_ids.data(), searchconf);
        }
        AssertResult(res_ids, res_dis);
    }
}

#endif

TEST_P(KnowhereWrapperTest, SERIALIZE_TEST) {
    std::cout << "type: " << static_cast<int>(index_type) << std::endl;
    EXPECT_EQ(index_->GetType(), index_type);

    auto elems = nq * k;
    std::vector<int64_t> res_ids(elems);
    std::vector<float> res_dis(elems);
    index_->BuildAll(nb, xb.data(), ids.data(), conf);
    index_->Search(nq, xq.data(), res_dis.data(), res_ids.data(), searchconf);
    AssertResult(res_ids, res_dis);

    {
        auto binary = index_->Serialize();
        auto type = index_->GetType();
        auto new_index = GetVecIndexFactory(type);
        new_index->Load(binary);
        EXPECT_EQ(new_index->Dimension(), index_->Dimension());
        EXPECT_EQ(new_index->Count(), index_->Count());

        std::vector<int64_t> res_ids(elems);
        std::vector<float> res_dis(elems);
        new_index->Search(nq, xq.data(), res_dis.data(), res_ids.data(), searchconf);
        AssertResult(res_ids, res_dis);
    }

    {
        std::string file_location = "/tmp/knowhere";
        write_index(index_, file_location);
        auto new_index = milvus::engine::read_index(file_location);
        EXPECT_EQ(new_index->GetType(), ConvertToCpuIndexType(index_type));
        EXPECT_EQ(new_index->Dimension(), index_->Dimension());
        EXPECT_EQ(new_index->Count(), index_->Count());

        std::vector<int64_t> res_ids(elems);
        std::vector<float> res_dis(elems);
        new_index->Search(nq, xq.data(), res_dis.data(), res_ids.data(), searchconf);
        AssertResult(res_ids, res_dis);
    }

    {
        std::string file_location = "/tmp/knowhere_gpu_file";
        fiu_init(0);
        fiu_enable("VecIndex.write_index.throw_knowhere_exception", 1, nullptr, 0);
        auto s = write_index(index_, file_location);
        ASSERT_FALSE(s.ok());
        fiu_disable("VecIndex.write_index.throw_knowhere_exception");

        fiu_enable("VecIndex.write_index.throw_std_exception", 1, nullptr, 0);
        s = write_index(index_, file_location);
        ASSERT_FALSE(s.ok());
        fiu_disable("VecIndex.write_index.throw_std_exception");

        fiu_enable("VecIndex.write_index.throw_no_space_exception", 1, nullptr, 0);
        s = write_index(index_, file_location);
        ASSERT_FALSE(s.ok());
        fiu_disable("VecIndex.write_index.throw_no_space_exception");
    }
}

// #include "wrapper/ConfAdapter.h"

// TEST(whatever, test_config) {
//     milvus::engine::TempMetaConf conf;
//     conf.nprobe = 16;
//     conf.dim = 128;
//     auto nsg_conf = std::make_shared<milvus::engine::NSGConfAdapter>();
//     nsg_conf->Match(conf);
//     nsg_conf->MatchSearch(conf, milvus::engine::IndexType::NSG_MIX);

//     auto pq_conf = std::make_shared<milvus::engine::IVFPQConfAdapter>();
//     pq_conf->Match(conf);
//     pq_conf->MatchSearch(conf, milvus::engine::IndexType::FAISS_IVFPQ_MIX);

//     auto kdt_conf = std::make_shared<milvus::engine::SPTAGKDTConfAdapter>();
//     kdt_conf->Match(conf);
//     kdt_conf->MatchSearch(conf, milvus::engine::IndexType::SPTAG_KDT_RNT_CPU);

//     auto bkt_conf = std::make_shared<milvus::engine::SPTAGBKTConfAdapter>();
//     bkt_conf->Match(conf);
//     bkt_conf->MatchSearch(conf, milvus::engine::IndexType::SPTAG_BKT_RNT_CPU);

//     auto config_mgr = milvus::engine::AdapterMgr::GetInstance();
//     try {
//         config_mgr.GetAdapter(milvus::engine::IndexType::INVALID);
//     } catch (std::exception& e) {
//         std::cout << "catch an expected exception" << std::endl;
//     }

//     conf.size = 1000000.0;
//     conf.nlist = 10;
//     auto ivf_conf = std::make_shared<milvus::engine::IVFConfAdapter>();
//     ivf_conf->Match(conf);
//     conf.nprobe = -1;
//     ivf_conf->MatchSearch(conf, milvus::engine::IndexType::FAISS_IVFFLAT_GPU);
//     conf.nprobe = 4096;
//     ivf_conf->MatchSearch(conf, milvus::engine::IndexType::FAISS_IVFPQ_GPU);

//     auto ivf_pq_conf = std::make_shared<milvus::engine::IVFPQConfAdapter>();
//     conf.metric_type = knowhere::METRICTYPE::IP;
//     try {
//         ivf_pq_conf->Match(conf);
//     } catch (std::exception& e) {
//         std::cout << "catch an expected exception" << std::endl;
//     }

//     conf.metric_type = knowhere::METRICTYPE::L2;
//     fiu_init(0);
//     fiu_enable("IVFPQConfAdapter.Match.empty_resset", 1, nullptr, 0);
//     try {
//         ivf_pq_conf->Match(conf);
//     } catch (std::exception& e) {
//         std::cout << "catch an expected exception" << std::endl;
//     }
//     fiu_disable("IVFPQConfAdapter.Match.empty_resset");

//     conf.nprobe = -1;
//     try {
//         ivf_pq_conf->MatchSearch(conf, milvus::engine::IndexType::FAISS_IVFPQ_GPU);
//     } catch (std::exception& e) {
//         std::cout << "catch an expected exception" << std::endl;
//     }
// }

#include "wrapper/VecImpl.h"

TEST(BFIndex, test_bf_index_fail) {
    auto bf_ptr = std::make_shared<milvus::engine::BFIndex>(nullptr);
    auto float_vec = bf_ptr->GetRawVectors();
    ASSERT_EQ(float_vec, nullptr);
    milvus::engine::Config config;

    fiu_init(0);
    fiu_enable("BFIndex.Build.throw_knowhere_exception", 1, nullptr, 0);
    auto err_code = bf_ptr->Build(config);
    ASSERT_EQ(err_code, milvus::KNOWHERE_UNEXPECTED_ERROR);
    fiu_disable("BFIndex.Build.throw_knowhere_exception");

    fiu_enable("BFIndex.Build.throw_std_exception", 1, nullptr, 0);
    err_code = bf_ptr->Build(config);
    ASSERT_EQ(err_code, milvus::KNOWHERE_ERROR);
    fiu_disable("BFIndex.Build.throw_std_exception");
}

// #include "knowhere/index/vector_index/IndexIDMAP.h"
// #include "src/wrapper/VecImpl.h"
// #include "src/index/unittest/utils.h"
// The two case below prove NSG is concern with data distribution
// Further work: 1. Use right basedata and pass it by milvus
//                  a. batch size is 100000 [Pass]
//                  b. transfer all at once [Pass]
//               2. Use SIFT1M in test and check time cost []
// TEST_P(KnowhereWrapperTest, nsgwithidmap) {
//     auto idmap = GetVecIndexFactory(milvus::engine::IndexType::FAISS_IDMAP);
//     auto ori_xb = xb;
//     auto ori_ids = ids;
//     std::vector<float> temp_xb;
//     std::vector<int64_t> temp_ids;
//     nb = 50000;
//     for (int i = 0; i < 20; ++i) {
//         GenData(dim, nb, nq, xb, xq, ids, k, gt_ids, gt_dis);
//         assert(xb.size() == nb*dim);
// //#define IDMAP
// #ifdef IDMAP
//         temp_xb.insert(temp_xb.end(), xb.data(), xb.data() + nb*dim);
//         temp_ids.insert(temp_ids.end(), ori_ids.data()+nb*i, ori_ids.data() + nb*(i+1));
//         if (i == 0) {
//             idmap->BuildAll(nb, temp_xb.data(), temp_ids.data(), conf);
//         } else {
//             idmap->Add(nb, temp_xb.data(), temp_ids.data());
//         }
//         temp_xb.clear();
//         temp_ids.clear();
// #else
//         temp_xb.insert(temp_xb.end(), xb.data(), xb.data() + nb*dim);
//         temp_ids.insert(temp_ids.end(), ori_ids.data()+nb*i, ori_ids.data() + nb*(i+1));
// #endif
//     }

// #ifdef IDMAP
//     auto idmap_idx = std::dynamic_pointer_cast<milvus::engine::BFIndex>(idmap);
//     auto x = idmap_idx->Count();
//     index_->BuildAll(idmap_idx->Count(), idmap_idx->GetRawVectors(), idmap_idx->GetRawIds(), conf);
// #else
//     assert(temp_xb.size() == 1000000*128);
//     index_->BuildAll(1000000, temp_xb.data(), ori_ids.data(), conf);
// #endif
// }

// TEST_P(KnowhereWrapperTest, nsgwithsidmap) {
//     auto idmap = GetVecIndexFactory(milvus::engine::IndexType::FAISS_IDMAP);
//     auto ori_xb = xb;
//     std::vector<float> temp_xb;
//     std::vector<int64_t> temp_ids;
//     nb = 50000;
//     for (int i = 0; i < 20; ++i) {
// #define IDMAP
// #ifdef IDMAP
//         temp_xb.insert(temp_xb.end(), ori_xb.data()+nb*dim*i, ori_xb.data() + nb*dim*(i+1));
//         temp_ids.insert(temp_ids.end(), ids.data()+nb*i, ids.data() + nb*(i+1));
//         if (i == 0) {
//             idmap->BuildAll(nb, temp_xb.data(), temp_ids.data(), conf);
//         } else {
//             idmap->Add(nb, temp_xb.data(), temp_ids.data());
//         }
//         temp_xb.clear();
//         temp_ids.clear();
// #else
//         temp_xb.insert(temp_xb.end(), ori_xb.data()+nb*dim*i, ori_xb.data() + nb*dim*(i+1));
//         temp_ids.insert(temp_ids.end(), ids.data()+nb*i, ids.data() + nb*(i+1));
// #endif
//     }

// #ifdef IDMAP
//     auto idmap_idx = std::dynamic_pointer_cast<milvus::engine::BFIndex>(idmap);
//     auto x = idmap_idx->Count();
//     index_->BuildAll(idmap_idx->Count(), idmap_idx->GetRawVectors(), idmap_idx->GetRawIds(), conf);
// #else
//     index_->BuildAll(1000000, temp_xb.data(), temp_ids.data(), conf);
// #endif

//     // The code use to store raw base data
//     FileIOWriter writer("/tmp/newraw");
//     ori_xb.shrink_to_fit();
//     std::cout << "size" << ori_xb.size();
//     writer(static_cast<void*>(ori_xb.data()), ori_xb.size()* sizeof(float));
//     std::cout << "Finish!" << std::endl;
// }

// void load_data(char* filename, float*& data, unsigned& num,
//                unsigned& dim) {  // load data with sift10K pattern
//     std::ifstream in(filename, std::ios::binary);
//     if (!in.is_open()) {
//         std::cout << "open file error" << std::endl;
//         exit(-1);
//     }
//     in.read((char*)&dim, 4);
//     in.seekg(0, std::ios::end);
//     std::ios::pos_type ss = in.tellg();
//     size_t fsize = (size_t)ss;
//     num = (unsigned)(fsize / (dim + 1) / 4);
//     data = new float[(size_t)num * (size_t)dim];

//     in.seekg(0, std::ios::beg);
//     for (size_t i = 0; i < num; i++) {
//         in.seekg(4, std::ios::cur);
//         in.read((char*)(data + i * dim), dim * 4);
//     }
//     in.close();
// }

// TEST_P(KnowhereWrapperTest, Sift1M) {
//     float* data = nullptr;
//     unsigned points_num, dim;
//     load_data("/mnt/112d53a6-5592-4360-a33b-7fd789456fce/workspace/Data/sift/sift_base.fvecs", data, points_num,
//     dim); std::cout << points_num << " " << dim << std::endl;

//     index_->BuildAll(points_num, data, ids.data(), conf);
// }
