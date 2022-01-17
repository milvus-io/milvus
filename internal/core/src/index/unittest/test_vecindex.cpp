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

#include "knowhere/index/IndexType.h"
#include "knowhere/index/vector_index/VecIndex.h"
#include "knowhere/index/vector_index/VecIndexFactory.h"
#include "knowhere/index/vector_index/helpers/FaissGpuResourceMgr.h"

#ifdef MILVUS_GPU_VERSION
#include "knowhere/index/vector_index/helpers/Cloner.h"
#endif

#include "unittest/Helper.h"
#include "unittest/utils.h"

using ::testing::Combine;
using ::testing::TestWithParam;
using ::testing::Values;

class VecIndexTest : public DataGen, public Tuple<milvus::knowhere::IndexType, milvus::knowhere::IndexMode> > {
 protected:
    void
    SetUp() override {
#ifdef MILVUS_GPU_VERSION
        milvus::knowhere::FaissGpuResourceMgr::GetInstance().InitDevice(DEVICEID, PINMEM, TEMPMEM, RESNUM);
#endif
        std::tie(index_type_, index_mode_, parameter_type_) = GetParam();
        Generate(DIM, NB, NQ);
        index_ = milvus::knowhere::VecIndexFactory::GetInstance().CreateVecIndex(index_type_, index_mode_);
        conf = ParamGenerator::GetInstance().Gen(parameter_type_);
    }

    void
    TearDown() override {
#ifdef MILVUS_GPU_VERSION
        milvus::knowhere::FaissGpuResourceMgr::GetInstance().Free();
#endif
    }

 protected:
    milvus::knowhere::IndexType index_type_;
    milvus::knowhere::IndexMode index_mode_;
    ParameterType parameter_type_;
    milvus::knowhere::Config conf;
    milvus::knowhere::VecIndexPtr index_ = nullptr;
};

INSTANTIATE_TEST_CASE_P(
    IVFParameters,
    IVFTest,
    Values(
#ifdef MILVUS_GPU_VERSION
        std::make_tuple(milvus::knowhere::IndexType::INDEX_FAISS_IVFFLAT, milvus::knowhere::IndexMode::MODE_GPU),
        std::make_tuple(milvus::knowhere::IndexType::INDEX_FAISS_IVFPQ, milvus::knowhere::IndexMode::MODE_GPU),
        std::make_tuple(milvus::knowhere::IndexType::INDEX_FAISS_IVFSQ8, milvus::knowhere::IndexMode::MODE_GPU),
        std::make_tuple(milvus::knowhere::IndexType::INDEX_FAISS_IVFSQ8H, milvus::knowhere::IndexMode::MODE_GPU),
#endif
        std::make_tuple(milvus::knowhere::IndexType::INDEX_FAISS_IVFFLAT, milvus::knowhere::IndexMode::MODE_CPU),
        std::make_tuple(milvus::knowhere::IndexType::INDEX_FAISS_IVFPQ, milvus::knowhere::IndexMode::MODE_CPU),
        std::make_tuple(milvus::knowhere::IndexType::INDEX_FAISS_IVFSQ8, milvus::knowhere::IndexMode::MODE_CPU),
        std::make_tuple(milvus::knowhere::IndexType::INDEX_NSG, milvus::knowhere::IndexMode::MODE_CPU),
        std::make_tuple(milvus::knowhere::IndexType::INDEX_HNSW, milvus::knowhere::IndexMode::MODE_CPU),
        std::make_tuple(milvus::knowhere::IndexType::INDEX_SPTAG_KDT_RNT, milvus::knowhere::IndexMode::MODE_CPU),
        std::make_tuple(milvus::knowhere::IndexType::INDEX_SPTAG_BKT_RNT, milvus::knowhere::IndexMode::MODE_CPU)));

TEST_P(VecIndexTest, basic) {
    assert(!xb.empty());
    KNOWHERE_LOG_DEBUG << "conf: " << conf->dump();

    index_->BuildAll(base_dataset, conf);
    EXPECT_EQ(index_->Dim(), dim);
    EXPECT_EQ(index_->Count(), nb);
    EXPECT_EQ(index_->index_type(), index_type_);
    EXPECT_EQ(index_->index_mode(), index_mode_);

    auto result = index_->Query(query_dataset, conf, nullptr);
    AssertAnns(result, nq, conf[milvus::knowhere::meta::TOPK]);
    PrintResult(result, nq, k);
}

TEST_P(VecIndexTest, serialize) {
    index_->BuildAll(base_dataset, conf);
    EXPECT_EQ(index_->Dim(), dim);
    EXPECT_EQ(index_->Count(), nb);
    EXPECT_EQ(index_->index_type(), index_type_);
    EXPECT_EQ(index_->index_mode(), index_mode_);
    auto result = index_->Query(query_dataset, conf, nullptr);
    AssertAnns(result, nq, conf[milvus::knowhere::meta::TOPK]);

    auto binaryset = index_->Serialize();
    auto new_index = milvus::knowhere::VecIndexFactory::GetInstance().CreateVecIndex(index_type_, index_mode_);
    new_index->Load(binaryset);
    EXPECT_EQ(index_->Dim(), new_index->Dim());
    EXPECT_EQ(index_->Count(), new_index->Count());
    EXPECT_EQ(index_->index_type(), new_index->index_type());
    EXPECT_EQ(index_->index_mode(), new_index->index_mode());
    auto new_result = new_index_->Query(query_dataset, conf, nullptr);
    AssertAnns(new_result, nq, conf[milvus::knowhere::meta::TOPK]);
}

// todo
#ifdef MILVUS_GPU_VERSION
TEST_P(VecIndexTest, copytogpu) {
    // todo
}

TEST_P(VecIndexTest, copytocpu) {
    // todo
}
#endif
