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
#include "utils/easylogging++.h"

#include "src/wrapper/vec_index.h"
#include "knowhere/index/vector_index/IndexGPUIVF.h"
#include "knowhere/index/vector_index/utils/FaissGpuResourceMgr.h"

#include "utils.h"

INITIALIZE_EASYLOGGINGPP

using namespace zilliz::milvus::engine;
//using namespace zilliz::knowhere;

using ::testing::TestWithParam;
using ::testing::Values;
using ::testing::Combine;

constexpr int64_t DIM = 128;
constexpr int64_t NB = 100000;
constexpr int64_t DEVICE_ID = 0;

class KnowhereWrapperTest
    : public TestWithParam<::std::tuple<IndexType, std::string, int, int, int, int, Config, Config>> {
 protected:
    void SetUp() override {
        zilliz::knowhere::FaissGpuResourceMgr::GetInstance().InitDevice(DEVICE_ID, 1024*1024*200, 1024*1024*300, 2);

        std::string generator_type;
        std::tie(index_type, generator_type, dim, nb, nq, k, train_cfg, search_cfg) = GetParam();

        auto generator = std::make_shared<DataGenBase>();
        generator->GenData(dim, nb, nq, xb, xq, ids, k, gt_ids, gt_dis);

        index_ = GetVecIndexFactory(index_type);
    }
    void TearDown() override {
        zilliz::knowhere::FaissGpuResourceMgr::GetInstance().Free();
    }

    void AssertResult(const std::vector<long> &ids, const std::vector<float> &dis) {
        EXPECT_EQ(ids.size(), nq * k);
        EXPECT_EQ(dis.size(), nq * k);

        for (auto i = 0; i < nq; i++) {
            EXPECT_EQ(ids[i * k], gt_ids[i * k]);
            //EXPECT_EQ(dis[i * k], gt_dis[i * k]);
        }

        int match = 0;
        for (int i = 0; i < nq; ++i) {
            for (int j = 0; j < k; ++j) {
                for (int l = 0; l < k; ++l) {
                    if (ids[i * nq + j] == gt_ids[i * nq + l]) match++;
                }
            }
        }

        auto precision = float(match) / (nq * k);
        EXPECT_GT(precision, 0.5);
        std::cout << std::endl << "Precision: " << precision
                  << ", match: " << match
                  << ", total: " << nq * k
                  << std::endl;
    }

 protected:
    IndexType index_type;
    Config train_cfg;
    Config search_cfg;

    int dim = DIM;
    int nb = NB;
    int nq = 10;
    int k = 10;
    std::vector<float> xb;
    std::vector<float> xq;
    std::vector<long> ids;

    VecIndexPtr index_ = nullptr;

    // Ground Truth
    std::vector<long> gt_ids;
    std::vector<float> gt_dis;
};

INSTANTIATE_TEST_CASE_P(WrapperParam, KnowhereWrapperTest,
                        Values(
                            //["Index type", "Generator type", "dim", "nb", "nq", "k", "build config", "search config"]
                            std::make_tuple(IndexType::FAISS_IVFFLAT_CPU, "Default",
                                            64, 100000, 10, 10,
                                            Config::object{{"nlist", 100}, {"dim", 64}, {"metric_type", "L2"}},
                                            Config::object{{"dim", 64}, {"k", 10}, {"nprobe", 10}}
                            ),
                            // to_gpu_test Failed
                            std::make_tuple(IndexType::FAISS_IVFFLAT_GPU, "Default",
                                            DIM, NB, 10, 10,
                                            Config::object{{"nlist", 100}, {"dim", DIM}, {"metric_type", "L2"}, {"gpu_id", DEVICE_ID}},
                                            Config::object{{"dim", DIM}, {"k", 10}, {"nprobe", 40}}
                            ),
                            std::make_tuple(IndexType::FAISS_IVFFLAT_MIX, "Default",
                                            64, 100000, 10, 10,
                                            Config::object{{"nlist", 1000}, {"dim", 64}, {"metric_type", "L2"}},
                                            Config::object{{"dim", 64}, {"k", 10}, {"nprobe", 5}}
                            ),
                            std::make_tuple(IndexType::FAISS_IDMAP, "Default",
                                            64, 100000, 10, 10,
                                            Config::object{{"dim", 64}, {"metric_type", "L2"}},
                                            Config::object{{"dim", 64}, {"k", 10}}
                            ),
                            std::make_tuple(IndexType::FAISS_IVFSQ8_CPU, "Default",
                                            DIM, NB, 10, 10,
                                            Config::object{{"dim", DIM}, {"nlist", 1000}, {"nbits", 8}, {"metric_type", "L2"}, {"gpu_id", DEVICE_ID}},
                                            Config::object{{"dim", DIM}, {"k", 10}, {"nprobe", 5}}
                            ),
                            std::make_tuple(IndexType::FAISS_IVFSQ8_GPU, "Default",
                                            DIM, NB, 10, 10,
                                            Config::object{{"dim", DIM}, {"nlist", 1000}, {"nbits", 8}, {"metric_type", "L2"}, {"gpu_id", DEVICE_ID}},
                                            Config::object{{"dim", DIM}, {"k", 10}, {"nprobe", 5}}
                            ),
                            std::make_tuple(IndexType::FAISS_IVFSQ8_MIX, "Default",
                                            DIM, NB, 10, 10,
                                            Config::object{{"dim", DIM}, {"nlist", 1000}, {"nbits", 8}, {"metric_type", "L2"}, {"gpu_id", DEVICE_ID}},
                                            Config::object{{"dim", DIM}, {"k", 10}, {"nprobe", 5}}
                            )
//                            std::make_tuple(IndexType::NSG_MIX, "Default",
//                                            128, 250000, 10, 10,
//                                            Config::object{{"dim", 128}, {"nlist", 8192}, {"nprobe", 16}, {"metric_type", "L2"},
//                                                           {"knng", 200}, {"search_length", 40}, {"out_degree", 60}, {"candidate_pool_size", 200}},
//                                            Config::object{{"k", 10}, {"search_length", 20}}
//                            )
                            //std::make_tuple(IndexType::SPTAG_KDT_RNT_CPU, "Default",
                            //                64, 10000, 10, 10,
                            //                Config::object{{"TPTNumber", 1}, {"dim", 64}},
                            //                Config::object{{"dim", 64}, {"k", 10}}
                            //)
                        )
);

TEST_P(KnowhereWrapperTest, BASE_TEST) {
    EXPECT_EQ(index_->GetType(), index_type);

    auto elems = nq * k;
    std::vector<int64_t> res_ids(elems);
    std::vector<float> res_dis(elems);

    index_->BuildAll(nb, xb.data(), ids.data(), train_cfg);
    index_->Search(nq, xq.data(), res_dis.data(), res_ids.data(), search_cfg);
    AssertResult(res_ids, res_dis);
}

TEST_P(KnowhereWrapperTest, TO_GPU_TEST) {
    EXPECT_EQ(index_->GetType(), index_type);

    auto elems = nq * k;
    std::vector<int64_t> res_ids(elems);
    std::vector<float> res_dis(elems);

    index_->BuildAll(nb, xb.data(), ids.data(), train_cfg);
    index_->Search(nq, xq.data(), res_dis.data(), res_ids.data(), search_cfg);
    AssertResult(res_ids, res_dis);

    {
        auto dev_idx = index_->CopyToGpu(DEVICE_ID);
        for (int i = 0; i < 10; ++i) {
            dev_idx->Search(nq, xq.data(), res_dis.data(), res_ids.data(), search_cfg);
        }
        AssertResult(res_ids, res_dis);
    }

    {
        std::string file_location = "/tmp/knowhere_gpu_file";
        write_index(index_, file_location);
        auto new_index = read_index(file_location);

        auto dev_idx = new_index->CopyToGpu(DEVICE_ID);
        for (int i = 0; i < 10; ++i) {
            dev_idx->Search(nq, xq.data(), res_dis.data(), res_ids.data(), search_cfg);
        }
        AssertResult(res_ids, res_dis);
    }
}

TEST_P(KnowhereWrapperTest, TO_CPU_TEST) {
    // dev
}

TEST_P(KnowhereWrapperTest, SERIALIZE_TEST) {
    EXPECT_EQ(index_->GetType(), index_type);

    auto elems = nq * k;
    std::vector<int64_t> res_ids(elems);
    std::vector<float> res_dis(elems);
    index_->BuildAll(nb, xb.data(), ids.data(), train_cfg);
    index_->Search(nq, xq.data(), res_dis.data(), res_ids.data(), search_cfg);
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
        new_index->Search(nq, xq.data(), res_dis.data(), res_ids.data(), search_cfg);
        AssertResult(res_ids, res_dis);
    }

    {
        std::string file_location = "/tmp/knowhere";
        write_index(index_, file_location);
        auto new_index = read_index(file_location);
        EXPECT_EQ(new_index->GetType(), ConvertToCpuIndexType(index_type));
        EXPECT_EQ(new_index->Dimension(), index_->Dimension());
        EXPECT_EQ(new_index->Count(), index_->Count());

        std::vector<int64_t> res_ids(elems);
        std::vector<float> res_dis(elems);
        new_index->Search(nq, xq.data(), res_dis.data(), res_ids.data(), search_cfg);
        AssertResult(res_ids, res_dis);
    }
}

// TODO(linxj): add exception test
//TEST_P(KnowhereWrapperTest, exception_test) {
//}

