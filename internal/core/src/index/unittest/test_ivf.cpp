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

#ifdef MILVUS_GPU_VERSION
#include <faiss/gpu/GpuIndexIVFFlat.h>
#endif

#include "knowhere/common/Exception.h"
#include "knowhere/common/Timer.h"
#include "knowhere/index/IndexType.h"
#include "knowhere/index/vector_index/IndexIVF.h"
#include "knowhere/index/vector_index/IndexIVFPQ.h"
#include "knowhere/index/vector_index/IndexIVFSQ.h"
#include "knowhere/index/vector_index/adapter/VectorAdapter.h"

#ifdef MILVUS_GPU_VERSION
#include "knowhere/index/vector_index/gpu/IndexGPUIVF.h"
#include "knowhere/index/vector_index/gpu/IndexGPUIVFPQ.h"
#include "knowhere/index/vector_index/gpu/IndexGPUIVFSQ.h"
#include "knowhere/index/vector_index/gpu/IndexIVFSQHybrid.h"
#include "knowhere/index/vector_index/helpers/Cloner.h"
#include "knowhere/index/vector_index/helpers/FaissGpuResourceMgr.h"
#endif

#include "unittest/Helper.h"
#include "unittest/utils.h"

using ::testing::Combine;
using ::testing::TestWithParam;
using ::testing::Values;

class IVFTest : public DataGen,
                public TestWithParam<::std::tuple<milvus::knowhere::IndexType, milvus::knowhere::IndexMode>> {
 protected:
    void
    SetUp() override {
#ifdef MILVUS_GPU_VERSION
        milvus::knowhere::FaissGpuResourceMgr::GetInstance().InitDevice(DEVICEID, PINMEM, TEMPMEM, RESNUM);
#endif
        std::tie(index_type_, index_mode_) = GetParam();
        // Init_with_default();
        //        nb = 1000000;
        //        nq = 1000;
        //        k = 1000;
        Generate(DIM, NB, NQ);
        index_ = IndexFactory(index_type_, index_mode_);
        conf_ = ParamGenerator::GetInstance().Gen(index_type_);
        // conf_->Dump();
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

INSTANTIATE_TEST_CASE_P(
    IVFParameters,
    IVFTest,
    Values(
#ifdef MILVUS_GPU_VERSION
        std::make_tuple(milvus::knowhere::IndexEnum::INDEX_FAISS_IVFPQ, milvus::knowhere::IndexMode::MODE_GPU),
        std::make_tuple(milvus::knowhere::IndexEnum::INDEX_FAISS_IVFSQ8, milvus::knowhere::IndexMode::MODE_GPU),
        std::make_tuple(milvus::knowhere::IndexEnum::INDEX_FAISS_IVFSQ8H, milvus::knowhere::IndexMode::MODE_GPU),
#endif
        std::make_tuple(milvus::knowhere::IndexEnum::INDEX_FAISS_IVFPQ, milvus::knowhere::IndexMode::MODE_CPU),
        std::make_tuple(milvus::knowhere::IndexEnum::INDEX_FAISS_IVFSQ8, milvus::knowhere::IndexMode::MODE_CPU)));

TEST_P(IVFTest, ivf_basic_cpu) {
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
    // PrintResult(result, nq, k);

    if (index_type_ != milvus::knowhere::IndexEnum::INDEX_FAISS_IVFPQ) {
#if 0
        auto result2 = index_->QueryById(id_dataset, conf_);
        AssertAnns(result2, nq, k);

        if (index_type_ != milvus::knowhere::IndexEnum::INDEX_FAISS_IVFSQ8) {
            auto result3 = index_->GetVectorById(xid_dataset, conf_);
            AssertVec(result3, base_dataset, xid_dataset, 1, dim);
        } else {
            auto result3 = index_->GetVectorById(xid_dataset, conf_);
            /* for SQ8, sometimes the mean diff can bigger than 20% */
            // AssertVec(result3, base_dataset, xid_dataset, 1, dim, CheckMode::CHECK_APPROXIMATE_EQUAL);
        }
#endif

        faiss::ConcurrentBitsetPtr concurrent_bitset_ptr = std::make_shared<faiss::ConcurrentBitset>(nb);
        for (int64_t i = 0; i < nq; ++i) {
            concurrent_bitset_ptr->set(i);
        }

        auto result_bs_1 = index_->Query(query_dataset, conf_, concurrent_bitset_ptr);
        AssertAnns(result_bs_1, nq, k, CheckMode::CHECK_NOT_EQUAL);
        // PrintResult(result, nq, k);

#if 0
        auto result_bs_2 = index_->QueryById(id_dataset, conf_);
        AssertAnns(result_bs_2, nq, k, CheckMode::CHECK_NOT_EQUAL);
        // PrintResult(result, nq, k);

        auto result_bs_3 = index_->GetVectorById(xid_dataset, conf_);
        AssertVec(result_bs_3, base_dataset, xid_dataset, 1, dim, CheckMode::CHECK_NOT_EQUAL);
#endif
    }

#ifdef MILVUS_GPU_VERSION
    milvus::knowhere::FaissGpuResourceMgr::GetInstance().Dump();
#endif
}

TEST_P(IVFTest, ivf_basic_gpu) {
    assert(!xb.empty());

    if (index_mode_ != milvus::knowhere::IndexMode::MODE_GPU) {
        return;
    }

    // null faiss index
    ASSERT_ANY_THROW(index_->AddWithoutIds(base_dataset, conf_));

    index_->BuildAll(base_dataset, conf_);
    EXPECT_EQ(index_->Count(), nb);
    EXPECT_EQ(index_->Dim(), dim);

    auto result = index_->Query(query_dataset, conf_, nullptr);
    AssertAnns(result, nq, k);
    // PrintResult(result, nq, k);

    faiss::ConcurrentBitsetPtr concurrent_bitset_ptr = std::make_shared<faiss::ConcurrentBitset>(nb);
    for (int64_t i = 0; i < nq; ++i) {
        concurrent_bitset_ptr->set(i);
    }

    auto result_bs_1 = index_->Query(query_dataset, conf_, concurrent_bitset_ptr);
    AssertAnns(result_bs_1, nq, k, CheckMode::CHECK_NOT_EQUAL);
    // PrintResult(result, nq, k);

#ifdef MILVUS_GPU_VERSION
    milvus::knowhere::FaissGpuResourceMgr::GetInstance().Dump();
#endif
}

TEST_P(IVFTest, ivf_serialize) {
    fiu_init(0);
    auto serialize = [](const std::string& filename, milvus::knowhere::BinaryPtr& bin, uint8_t* ret) {
        FileIOWriter writer(filename);
        writer(static_cast<void*>(bin->data.get()), bin->size);

        FileIOReader reader(filename);
        reader(ret, bin->size);
    };

    {
        // serialize index
        index_->Train(base_dataset, conf_);
        index_->AddWithoutIds(base_dataset, conf_);
        auto binaryset = index_->Serialize(conf_);
        auto bin = binaryset.GetByName("IVF");

        std::string filename = "/tmp/ivf_test_serialize.bin";
        auto load_data = new uint8_t[bin->size];
        serialize(filename, bin, load_data);

        binaryset.clear();
        std::shared_ptr<uint8_t[]> data(load_data);
        binaryset.Append("IVF", data, bin->size);

        index_->Load(binaryset);
        EXPECT_EQ(index_->Count(), nb);
        EXPECT_EQ(index_->Dim(), dim);
        auto result = index_->Query(query_dataset, conf_, nullptr);
        AssertAnns(result, nq, conf_[milvus::knowhere::meta::TOPK]);
    }
}

TEST_P(IVFTest, ivf_slice) {
    fiu_init(0);
    {
        // serialize index
        index_->Train(base_dataset, conf_);
        index_->AddWithoutIds(base_dataset, conf_);
        auto binaryset = index_->Serialize(conf_);

        index_->Load(binaryset);
        EXPECT_EQ(index_->Count(), nb);
        EXPECT_EQ(index_->Dim(), dim);
        auto result = index_->Query(query_dataset, conf_, nullptr);
        AssertAnns(result, nq, conf_[milvus::knowhere::meta::TOPK]);
    }
}

// TODO(linxj): deprecated
#ifdef MILVUS_GPU_VERSION
TEST_P(IVFTest, clone_test) {
    assert(!xb.empty());

    index_->Train(base_dataset, conf_);
    index_->AddWithoutIds(base_dataset, conf_);
    EXPECT_EQ(index_->Count(), nb);
    EXPECT_EQ(index_->Dim(), dim);

    /* set peseodo index size, avoid throw exception */
    index_->SetIndexSize(nq * dim * sizeof(float));

    auto result = index_->Query(query_dataset, conf_, nullptr);
    AssertAnns(result, nq, conf_[milvus::knowhere::meta::TOPK]);
    // PrintResult(result, nq, k);

    auto AssertEqual = [&](milvus::knowhere::DatasetPtr p1, milvus::knowhere::DatasetPtr p2) {
        auto ids_p1 = p1->Get<int64_t*>(milvus::knowhere::meta::IDS);
        auto ids_p2 = p2->Get<int64_t*>(milvus::knowhere::meta::IDS);

        for (int i = 0; i < nq * k; ++i) {
            EXPECT_EQ(*((int64_t*)(ids_p2) + i), *((int64_t*)(ids_p1) + i));
            //            EXPECT_EQ(*(ids_p2->data()->GetValues<int64_t>(1, i)), *(ids_p1->data()->GetValues<int64_t>(1,
            //            i)));
        }
    };

    {
        // copy from gpu to cpu
        if (index_mode_ == milvus::knowhere::IndexMode::MODE_GPU) {
            EXPECT_NO_THROW({
                auto clone_index = milvus::knowhere::cloner::CopyGpuToCpu(index_, milvus::knowhere::Config());
                auto clone_result = clone_index->Query(query_dataset, conf_, nullptr);
                AssertEqual(result, clone_result);
                std::cout << "clone G <=> C [" << index_type_ << "] success" << std::endl;
            });
        } else {
            EXPECT_THROW(
                {
                    std::cout << "clone G <=> C [" << index_type_ << "] failed" << std::endl;
                    auto clone_index = milvus::knowhere::cloner::CopyGpuToCpu(index_, milvus::knowhere::Config());
                },
                milvus::knowhere::KnowhereException);
        }
    }

    {
        // copy to gpu
        if (index_type_ != milvus::knowhere::IndexEnum::INDEX_FAISS_IVFSQ8H) {
            EXPECT_NO_THROW({
                auto clone_index = milvus::knowhere::cloner::CopyCpuToGpu(index_, DEVICEID, milvus::knowhere::Config());
                auto clone_result = clone_index->Query(query_dataset, conf_, nullptr);
                AssertEqual(result, clone_result);
                std::cout << "clone C <=> G [" << index_type_ << "] success" << std::endl;
            });
            EXPECT_ANY_THROW(milvus::knowhere::cloner::CopyCpuToGpu(index_, -1, milvus::knowhere::Config()));
        }
    }
}
#endif

#ifdef MILVUS_GPU_VERSION
TEST_P(IVFTest, gpu_seal_test) {
    if (index_mode_ != milvus::knowhere::IndexMode::MODE_GPU) {
        return;
    }
    assert(!xb.empty());

    ASSERT_ANY_THROW(index_->Query(query_dataset, conf_, nullptr));
    ASSERT_ANY_THROW(index_->Seal());

    index_->Train(base_dataset, conf_);
    index_->AddWithoutIds(base_dataset, conf_);
    EXPECT_EQ(index_->Count(), nb);
    EXPECT_EQ(index_->Dim(), dim);

    /* set peseodo index size, avoid throw exception */
    index_->SetIndexSize(nq * dim * sizeof(float));

    auto result = index_->Query(query_dataset, conf_, nullptr);
    AssertAnns(result, nq, conf_[milvus::knowhere::meta::TOPK]);

    fiu_init(0);
    fiu_enable("IVF.Search.throw_std_exception", 1, nullptr, 0);
    ASSERT_ANY_THROW(index_->Query(query_dataset, conf_, nullptr));
    fiu_disable("IVF.Search.throw_std_exception");
    fiu_enable("IVF.Search.throw_faiss_exception", 1, nullptr, 0);
    ASSERT_ANY_THROW(index_->Query(query_dataset, conf_, nullptr));
    fiu_disable("IVF.Search.throw_faiss_exception");

    auto cpu_idx = milvus::knowhere::cloner::CopyGpuToCpu(index_, milvus::knowhere::Config());
    milvus::knowhere::IVFPtr ivf_idx = std::dynamic_pointer_cast<milvus::knowhere::IVF>(cpu_idx);

    milvus::knowhere::TimeRecorder tc("CopyToGpu");
    milvus::knowhere::cloner::CopyCpuToGpu(cpu_idx, DEVICEID, milvus::knowhere::Config());
    auto without_seal = tc.RecordSection("Without seal");
    ivf_idx->Seal();
    tc.RecordSection("seal cost");
    milvus::knowhere::cloner::CopyCpuToGpu(cpu_idx, DEVICEID, milvus::knowhere::Config());
    auto with_seal = tc.RecordSection("With seal");
    ASSERT_GE(without_seal, with_seal);

    // copy to GPU with invalid device id
    ASSERT_ANY_THROW(milvus::knowhere::cloner::CopyCpuToGpu(cpu_idx, -1, milvus::knowhere::Config()));
}

TEST_P(IVFTest, invalid_gpu_source) {
    if (index_mode_ != milvus::knowhere::IndexMode::MODE_GPU) {
        return;
    }

    auto invalid_conf = ParamGenerator::GetInstance().Gen(index_type_);
    invalid_conf[milvus::knowhere::meta::DEVICEID] = -1;

    // if (index_type_ == milvus::knowhere::IndexEnum::INDEX_FAISS_IVFFLAT) {
    //     null faiss index
    //     index_->SetIndexSize(0);
    //     milvus::knowhere::cloner::CopyGpuToCpu(index_, milvus::knowhere::Config());
    // }

    index_->Train(base_dataset, conf_);

    fiu_init(0);
    fiu_enable("GPUIVF.SerializeImpl.throw_exception", 1, nullptr, 0);
    ASSERT_ANY_THROW(index_->Serialize(conf_));
    fiu_disable("GPUIVF.SerializeImpl.throw_exception");

    fiu_enable("GPUIVF.search_impl.invald_index", 1, nullptr, 0);
    ASSERT_ANY_THROW(index_->Query(base_dataset, invalid_conf, nullptr));
    fiu_disable("GPUIVF.search_impl.invald_index");

    auto ivf_index = std::dynamic_pointer_cast<milvus::knowhere::GPUIVF>(index_);
    if (ivf_index) {
        auto gpu_index = std::dynamic_pointer_cast<milvus::knowhere::GPUIndex>(ivf_index);
        gpu_index->SetGpuDevice(-1);
        ASSERT_EQ(gpu_index->GetGpuDevice(), -1);
    }

    // ASSERT_ANY_THROW(index_->Load(binaryset));
    ASSERT_ANY_THROW(index_->Train(base_dataset, invalid_conf));
}

TEST_P(IVFTest, IVFSQHybrid_test) {
    if (index_type_ != milvus::knowhere::IndexEnum::INDEX_FAISS_IVFSQ8H) {
        return;
    }
    fiu_init(0);

    index_->SetIndexSize(0);
    milvus::knowhere::cloner::CopyGpuToCpu(index_, conf_);
    ASSERT_ANY_THROW(milvus::knowhere::cloner::CopyCpuToGpu(index_, -1, conf_));

    fiu_enable("FaissGpuResourceMgr.GetRes.ret_null", 1, nullptr, 0);
    ASSERT_ANY_THROW(index_->Train(base_dataset, conf_));
    ASSERT_ANY_THROW(index_->CopyCpuToGpu(DEVICEID, conf_));
    fiu_disable("FaissGpuResourceMgr.GetRes.ret_null");

    index_->Train(base_dataset, conf_);
    auto index = std::dynamic_pointer_cast<milvus::knowhere::IVFSQHybrid>(index_);
    ASSERT_TRUE(index != nullptr);
    ASSERT_ANY_THROW(index->UnsetQuantizer());

    ASSERT_ANY_THROW(index->SetQuantizer(nullptr));
}
#endif
