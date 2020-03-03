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

#include <tuple>
#include "knowhere/index/vector_index/helpers/IndexParameter.h"
#include "wrapper/VecIndex.h"
#include "wrapper/utils.h"

#include <gtest/gtest.h>

using ::testing::Combine;
using ::testing::TestWithParam;
using ::testing::Values;

class BinIndexTest : public BinDataGen,
                     public TestWithParam<::std::tuple<milvus::engine::IndexType, int, int, int, int>> {
 protected:
    void
    SetUp() override {
        std::tie(index_type, dim, nb, nq, k) = GetParam();
        Generate(dim, nb, nq, k);

        milvus::engine::TempMetaConf tempconf;
        tempconf.metric_type = knowhere::METRICTYPE::TANIMOTO;
        tempconf.size = nb;
        tempconf.dim = dim;
        tempconf.k = k;
        tempconf.nprobe = 16;

        index_ = GetVecIndexFactory(index_type);
        conf = ParamGenerator::GetInstance().GenBuild(index_type, tempconf);
        searchconf = ParamGenerator::GetInstance().GenSearchConf(index_type, tempconf);
    }

    void
    TearDown() override {
    }

 protected:
    milvus::engine::IndexType index_type;
    milvus::engine::VecIndexPtr index_ = nullptr;
    knowhere::Config conf;
    knowhere::Config searchconf;
};

INSTANTIATE_TEST_CASE_P(WrapperParam, BinIndexTest,
                        Values(
                            //["Index type", "dim", "nb", "nq", "k", "build config", "search config"]
                            std::make_tuple(milvus::engine::IndexType::FAISS_BIN_IDMAP, 64, 1000, 10, 10),
                            std::make_tuple(milvus::engine::IndexType::FAISS_BIN_IVFLAT_CPU, DIM, NB, 10, 10)));

TEST_P(BinIndexTest, BASE_TEST) {
    EXPECT_EQ(index_->GetType(), index_type);
    conf->Dump();
    searchconf->Dump();

    auto elems = nq * k;
    std::vector<int64_t> res_ids(elems);
    std::vector<float> res_dis(elems);

    index_->BuildAll(nb, xb.data(), ids.data(), conf);
    index_->Search(nq, xq.data(), res_dis.data(), res_ids.data(), searchconf);
    AssertResult(res_ids, res_dis);
}

TEST_P(BinIndexTest, SERIALIZE_TEST) {
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
}
