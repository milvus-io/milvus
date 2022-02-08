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

#include <fiu-control.h>
#include <fiu/fiu-local.h>
#include <iostream>
#include <thread>

#include "knowhere/common/Exception.h"
#include "knowhere/common/Timer.h"
#include "knowhere/index/IndexType.h"
#include "knowhere/index/vector_index/IndexIVF.h"
#include "knowhere/index/vector_index/IndexIVFHNSW.h"
#include "knowhere/index/vector_index/adapter/VectorAdapter.h"

#include "unittest/Helper.h"
#include "unittest/utils.h"

using ::testing::Combine;
using ::testing::TestWithParam;
using ::testing::Values;

class IVFHNSWTest : public DataGen,
                    public TestWithParam<::std::tuple<milvus::knowhere::IndexType, milvus::knowhere::IndexMode>> {
 protected:
    void
    SetUp() override {
        std::tie(index_type_, index_mode_) = GetParam();
        Generate(dim, nb, nq);
        index_ = IndexFactory(index_type_, index_mode_);
        conf_ = ParamGenerator::GetInstance().Gen(index_type_);
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
    milvus::knowhere::Config conf_;
    milvus::knowhere::IVFPtr index_ = nullptr;
};

INSTANTIATE_TEST_CASE_P(IVFParameters,
                        IVFHNSWTest,
                        Values(std::make_tuple(milvus::knowhere::IndexEnum::INDEX_FAISS_IVFHNSW,
                                               milvus::knowhere::IndexMode::MODE_CPU)));

TEST_P(IVFHNSWTest, ivfhnsw_basic_cpu) {
    assert(!xb.empty());

    if (index_mode_ != milvus::knowhere::IndexMode::MODE_CPU) {
        return;
    }

    // null faiss index
    ASSERT_ANY_THROW(index_->AddWithoutIds(base_dataset, conf_));

    index_->Train(base_dataset, conf_);
    index_->AddWithoutIds(base_dataset, conf_);
    EXPECT_EQ(index_->Count(), nb);
    EXPECT_EQ(index_->Dim(), dim);

    auto result = index_->Query(query_dataset, conf_, nullptr);
    AssertAnns(result, nq, k);
}

TEST_P(IVFHNSWTest, ivfhnsw_slice) {
    fiu_init(0);
    {
        // serialize index
        index_->Train(base_dataset, conf_);
        index_->AddWithoutIds(base_dataset, conf_);
        auto binaryset = index_->Serialize(conf_);
        // load index
        index_->Load(binaryset);
        EXPECT_EQ(index_->Count(), nb);
        EXPECT_EQ(index_->Dim(), dim);
        auto result = index_->Query(query_dataset, conf_, nullptr);
        AssertAnns(result, nq, k);
    }
}
