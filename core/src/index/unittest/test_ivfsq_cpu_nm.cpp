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
#include <fiu-local.h>
#include <iostream>
#include <thread>

#ifdef MILVUS_GPU_VERSION
#include <faiss/gpu/GpuIndexIVFFlat.h>
#endif

#include <faiss/IndexScalarQuantizer.h>
#include "knowhere/common/Exception.h"
#include "knowhere/common/Timer.h"
#include "knowhere/index/IndexType.h"
#include "knowhere/index/vector_index/IndexIVFSQ.h"
#include "knowhere/index/vector_index/adapter/VectorAdapter.h"
#include "knowhere/index/vector_offset_index/IndexIVFSQNR_NM.h"

#ifdef MILVUS_GPU_VERSION
#include "knowhere/index/vector_index/helpers/Cloner.h"
#include "knowhere/index/vector_index/helpers/FaissGpuResourceMgr.h"
#endif

#include "unittest/Helper.h"
#include "unittest/utils.h"

using ::testing::Combine;
using ::testing::TestWithParam;
using ::testing::Values;

class IVFSQNMCPUTest : public DataGen,
                       public TestWithParam<::std::tuple<milvus::knowhere::IndexType, milvus::knowhere::IndexMode>> {
 protected:
    void
    SetUp() override {
#ifdef MILVUS_GPU_VERSION
        milvus::knowhere::FaissGpuResourceMgr::GetInstance().InitDevice(DEVICEID, PINMEM, TEMPMEM, RESNUM);
#endif
        std::tie(index_type_, index_mode_) = GetParam();
        Generate(DIM, NB, NQ);
        index_ = std::make_shared<milvus::knowhere::IVFSQNR_NM>();
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
    milvus::knowhere::IVFSQNRNMPtr index_ = nullptr;
};

INSTANTIATE_TEST_CASE_P(IVFParameters, IVFSQNMCPUTest,
                        Values(std::make_tuple(milvus::knowhere::IndexEnum::INDEX_FAISS_IVFSQ8NR,
                                               milvus::knowhere::IndexMode::MODE_CPU)));

TEST_P(IVFSQNMCPUTest, ivf_basic_cpu) {
    assert(!xb.empty());

    if (index_mode_ != milvus::knowhere::IndexMode::MODE_CPU) {
        return;
    }

    // null faiss index
    ASSERT_ANY_THROW(index_->Add(base_dataset, conf_));
    ASSERT_ANY_THROW(index_->AddWithoutIds(base_dataset, conf_));

    index_->Train(base_dataset, conf_);
    index_->AddWithoutIds(base_dataset, conf_);
    EXPECT_EQ(index_->Count(), nb);
    EXPECT_EQ(index_->Dim(), dim);

    index_->SetIndexSize(nq * dim * sizeof(float));

    milvus::knowhere::BinarySet bs = index_->Serialize();
    index_->Load(bs);

    auto result = index_->Query(query_dataset, conf_);
    AssertAnns(result, nq, k);

    auto AssertEqual = [&](milvus::knowhere::DatasetPtr p1, milvus::knowhere::DatasetPtr p2) {
        auto ids_p1 = p1->Get<int64_t*>(milvus::knowhere::meta::IDS);
        auto ids_p2 = p2->Get<int64_t*>(milvus::knowhere::meta::IDS);

        for (int i = 0; i < nq * k; ++i) {
            EXPECT_EQ(*((int64_t*)(ids_p2) + i), *((int64_t*)(ids_p1) + i));
        }
    };

#ifdef MILVUS_GPU_VERSION
    // copy from cpu to gpu
    {
        EXPECT_NO_THROW({
            auto clone_index = milvus::knowhere::cloner::CopyCpuToGpu(index_, DEVICEID, milvus::knowhere::Config());
            auto clone_result = clone_index->Query(query_dataset, conf_);
            AssertEqual(result, clone_result);
            std::cout << "clone C <=> G [" << index_type_ << "] success" << std::endl;
        });
        EXPECT_ANY_THROW(milvus::knowhere::cloner::CopyCpuToGpu(index_, -1, milvus::knowhere::Config()));
    }
#endif

    faiss::ConcurrentBitsetPtr concurrent_bitset_ptr = std::make_shared<faiss::ConcurrentBitset>(nb);
    for (int64_t i = 0; i < nq; ++i) {
        concurrent_bitset_ptr->set(i);
    }
    index_->SetBlacklist(concurrent_bitset_ptr);

    auto result_bs_1 = index_->Query(query_dataset, conf_);
    AssertAnns(result_bs_1, nq, k, CheckMode::CHECK_NOT_EQUAL);

#ifdef MILVUS_GPU_VERSION
    milvus::knowhere::FaissGpuResourceMgr::GetInstance().Dump();
#endif
}
