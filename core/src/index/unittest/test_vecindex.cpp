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

#include "knowhere/index/vector_index/IndexType.h"
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

class VecIndexTest : public DataGen,
                     public TestWithParam<::std::tuple<knowhere::IndexType, knowhere::IndexMode, ParameterType>> {
 protected:
    void
    SetUp() override {
#ifdef MILVUS_GPU_VERSION
        knowhere::FaissGpuResourceMgr::GetInstance().InitDevice(DEVICEID, PINMEM, TEMPMEM, RESNUM);
#endif
        std::tie(index_type_, index_mode_, parameter_type_) = GetParam();
        Generate(DIM, NB, NQ);
        index_ = knowhere::VecIndexFactory::GetInstance().CreateVecIndex(index_type_, index_mode_);
        conf = ParamGenerator::GetInstance().Gen(parameter_type_);
    }

    void
    TearDown() override {
#ifdef MILVUS_GPU_VERSION
        knowhere::FaissGpuResourceMgr::GetInstance().Free();
#endif
    }

 protected:
    knowhere::IndexType index_type_;
    knowhere::IndexMode index_mode_;
    ParameterType parameter_type_;
    knowhere::Config conf;
    knowhere::VecIndexPtr index_ = nullptr;
};

INSTANTIATE_TEST_CASE_P(
    IVFParameters, IVFTest,
    Values(
#ifdef MILVUS_GPU_VERSION
        std::make_tuple(knowhere::IndexType::INDEX_FAISS_IVFFLAT, knowhere::IndexMode::MODE_GPU, ParameterType::ivf),
        std::make_tuple(knowhere::IndexType::INDEX_FAISS_IVFPQ, knowhere::IndexMode::MODE_GPU, ParameterType::ivfpq),
        std::make_tuple(knowhere::IndexType::INDEX_FAISS_IVFSQ8, knowhere::IndexMode::MODE_GPU, ParameterType::ivfsq),
#ifdef CUSTOMIZATION
        std::make_tuple(knowhere::IndexType::INDEX_FAISS_IVFSQ8H, knowhere::IndexMode::MODE_GPU, ParameterType::ivfsq),
#endif
#endif
        std::make_tuple(knowhere::IndexType::INDEX_FAISS_IVFFLAT, knowhere::IndexMode::MODE_CPU, ParameterType::ivf),
        std::make_tuple(knowhere::IndexType::INDEX_FAISS_IVFPQ, knowhere::IndexMode::MODE_CPU, ParameterType::ivfpq),
        std::make_tuple(knowhere::IndexType::INDEX_FAISS_IVFSQ8, knowhere::IndexMode::MODE_CPU, ParameterType::ivfsq)
        // std::make_tuple(knowhere::IndexType::INDEX_NSG, knowhere::IndexMode::MODE_CPU, ParameterType::ivfsq),
        // std::make_tuple(knowhere::IndexType::INDEX_HNSW, knowhere::IndexMode::MODE_CPU, ParameterType::ivfsq),
        // std::make_tuple(knowhere::IndexType::INDEX_SPTAG_KDT_RNT, knowhere::IndexMode::MODE_CPU,
        // ParameterType::ivfsq), std::make_tuple(knowhere::IndexType::INDEX_SPTAG_BKT_RNT,
        // knowhere::IndexMode::MODE_CPU, ParameterType::ivfsq),
        ));

TEST_P(VecIndexTest, basic) {
    assert(!xb.empty());
    KNOWHERE_LOG_DEBUG << "conf: " << conf->dump();

    index_->BuildAll(base_dataset, conf);
    EXPECT_EQ(index_->Dim(), dim);
    EXPECT_EQ(index_->Count(), nb);
    EXPECT_EQ(index_->index_type(), index_type_);
    EXPECT_EQ(index_->index_mode(), index_mode_);

    auto result = index_->Query(query_dataset, conf);
    AssertAnns(result, nq, conf[knowhere::meta::TOPK]);
    PrintResult(result, nq, k);
}

TEST_P(VecIndexTest, serialize) {
    index_->BuildAll(base_dataset, conf);
    EXPECT_EQ(index_->Dim(), dim);
    EXPECT_EQ(index_->Count(), nb);
    EXPECT_EQ(index_->index_type(), index_type_);
    EXPECT_EQ(index_->index_mode(), index_mode_);
    auto result = index_->Query(query_dataset, conf);
    AssertAnns(result, nq, conf[knowhere::meta::TOPK]);

    auto binaryset = index_->Serialize();
    auto new_index = knowhere::VecIndexFactory::GetInstance().CreateVecIndex(index_type_, index_mode_);
    new_index->Load(binaryset);
    EXPECT_EQ(index_->Dim(), new_index->Dim());
    EXPECT_EQ(index_->Count(), new_index->Count());
    EXPECT_EQ(index_->index_type(), new_index->index_type());
    EXPECT_EQ(index_->index_mode(), new_index->index_mode());
    auto new_result = new_index_->Query(query_dataset, conf);
    AssertAnns(new_result, nq, conf[knowhere::meta::TOPK]);
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