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
#include <boost/filesystem.hpp>
#include <vector>

#include "db/engine/EngineFactory.h"
#include "db/engine/ExecutionEngineImpl.h"
#include "db/utils.h"
#include <fiu-local.h>
#include <fiu-control.h>

TEST_F(EngineTest, FACTORY_TEST) {
    {
        auto engine_ptr = milvus::engine::EngineFactory::Build(
            512, "/tmp/milvus_index_1", milvus::engine::EngineType::INVALID, milvus::engine::MetricType::IP, 1024);

        ASSERT_TRUE(engine_ptr == nullptr);
    }

    {
        auto engine_ptr = milvus::engine::EngineFactory::Build(
            512, "/tmp/milvus_index_1", milvus::engine::EngineType::FAISS_IDMAP, milvus::engine::MetricType::IP, 1024);

        ASSERT_TRUE(engine_ptr != nullptr);
    }

    {
        auto engine_ptr =
            milvus::engine::EngineFactory::Build(512, "/tmp/milvus_index_1", milvus::engine::EngineType::FAISS_IVFFLAT,
                                                 milvus::engine::MetricType::IP, 1024);

        ASSERT_TRUE(engine_ptr != nullptr);
    }

    {
        auto engine_ptr = milvus::engine::EngineFactory::Build(
            512, "/tmp/milvus_index_1", milvus::engine::EngineType::FAISS_IVFSQ8, milvus::engine::MetricType::IP, 1024);

        ASSERT_TRUE(engine_ptr != nullptr);
    }

    {
        auto engine_ptr = milvus::engine::EngineFactory::Build(
            512, "/tmp/milvus_index_1", milvus::engine::EngineType::NSG_MIX, milvus::engine::MetricType::IP, 1024);

        ASSERT_TRUE(engine_ptr != nullptr);
    }

    {
        auto engine_ptr = milvus::engine::EngineFactory::Build(
            512, "/tmp/milvus_index_1", milvus::engine::EngineType::FAISS_PQ, milvus::engine::MetricType::IP, 1024);

        ASSERT_TRUE(engine_ptr != nullptr);
    }

    {
        auto engine_ptr = milvus::engine::EngineFactory::Build(
            512, "/tmp/milvus_index_1", milvus::engine::EngineType::SPTAG_KDT,
            milvus::engine::MetricType::L2, 1024);

        ASSERT_TRUE(engine_ptr != nullptr);
    }

    {
        auto engine_ptr = milvus::engine::EngineFactory::Build(
            512, "/tmp/milvus_index_1", milvus::engine::EngineType::SPTAG_KDT,
            milvus::engine::MetricType::L2, 1024);

        ASSERT_TRUE(engine_ptr != nullptr);
    }

    {
        fiu_init(0);
        // test ExecutionEngineImpl constructor when create vecindex failed
        FIU_ENABLE_FIU("ExecutionEngineImpl.CreatetVecIndex.invalid_type");
        ASSERT_ANY_THROW(milvus::engine::EngineFactory::Build(
            512, "/tmp/milvus_index_1", milvus::engine::EngineType::SPTAG_KDT,
            milvus::engine::MetricType::L2, 1024));
        fiu_disable("ExecutionEngineImpl.CreatetVecIndex.invalid_type");
    }

    {
        // test ExecutionEngineImpl constructor when build BFindex failed
        FIU_ENABLE_FIU("BFIndex.Build.throw_knowhere_exception");
        ASSERT_ANY_THROW(milvus::engine::EngineFactory::Build(
            512, "/tmp/milvus_index_1", milvus::engine::EngineType::SPTAG_KDT,
            milvus::engine::MetricType::L2, 1024));
        fiu_disable("BFIndex.Build.throw_knowhere_exception");
    }
}

TEST_F(EngineTest, ENGINE_IMPL_TEST) {
    fiu_init(0);
    uint16_t dimension = 64;
    std::string file_path = "/tmp/milvus_index_1";
    auto engine_ptr = milvus::engine::EngineFactory::Build(
        dimension, file_path, milvus::engine::EngineType::FAISS_IVFFLAT, milvus::engine::MetricType::IP, 1024);

    std::vector<float> data;
    std::vector<int64_t> ids;
    const int row_count = 500;
    data.reserve(row_count * dimension);
    ids.reserve(row_count);
    for (int64_t i = 0; i < row_count; i++) {
        ids.push_back(i);
        for (uint16_t k = 0; k < dimension; k++) {
            data.push_back(i * dimension + k);
        }
    }

    auto status = engine_ptr->AddWithIds((int64_t)ids.size(), data.data(), ids.data());
    ASSERT_TRUE(status.ok());

    ASSERT_EQ(engine_ptr->Dimension(), dimension);
    ASSERT_EQ(engine_ptr->Count(), ids.size());

    ASSERT_EQ(engine_ptr->GetLocation(), file_path);
    ASSERT_EQ(engine_ptr->IndexMetricType(), milvus::engine::MetricType::IP);

    ASSERT_ANY_THROW(engine_ptr->BuildIndex(file_path, milvus::engine::EngineType::INVALID));
    FIU_ENABLE_FIU("VecIndexImpl.BuildAll.throw_knowhere_exception");
    ASSERT_ANY_THROW(engine_ptr->BuildIndex(file_path, milvus::engine::EngineType::SPTAG_KDT));
    fiu_disable("VecIndexImpl.BuildAll.throw_knowhere_exception");

    auto engine_build = engine_ptr->BuildIndex("/tmp/milvus_index_2", milvus::engine::EngineType::FAISS_IVFSQ8);
#ifndef MILVUS_GPU_VERSION
    //PQ don't support IP In gpu version
    engine_build = engine_ptr->BuildIndex("/tmp/milvus_index_3", milvus::engine::EngineType::FAISS_PQ);
#endif
    engine_build = engine_ptr->BuildIndex("/tmp/milvus_index_4", milvus::engine::EngineType::SPTAG_KDT);
    engine_build = engine_ptr->BuildIndex("/tmp/milvus_index_5", milvus::engine::EngineType::SPTAG_BKT);
    engine_ptr->BuildIndex("/tmp/milvus_index_SPTAG_BKT", milvus::engine::EngineType::SPTAG_BKT);

#ifdef MILVUS_GPU_VERSION
    FIU_ENABLE_FIU("ExecutionEngineImpl.CreatetVecIndex.gpu_res_disabled");
    engine_ptr->BuildIndex("/tmp/milvus_index_NSG_MIX", milvus::engine::EngineType::NSG_MIX);
    engine_ptr->BuildIndex("/tmp/milvus_index_6", milvus::engine::EngineType::FAISS_IVFFLAT);
    engine_ptr->BuildIndex("/tmp/milvus_index_7", milvus::engine::EngineType::FAISS_IVFSQ8);
    ASSERT_ANY_THROW(engine_ptr->BuildIndex("/tmp/milvus_index_8", milvus::engine::EngineType::FAISS_IVFSQ8H));
    ASSERT_ANY_THROW(engine_ptr->BuildIndex("/tmp/milvus_index_9", milvus::engine::EngineType::FAISS_PQ));
    fiu_disable("ExecutionEngineImpl.CreatetVecIndex.gpu_res_disabled");
#endif

    //merge self
    status = engine_ptr->Merge(file_path);
    ASSERT_FALSE(status.ok());

//    FIU_ENABLE_FIU("VecIndexImpl.Add.throw_knowhere_exception");
//    status = engine_ptr->Merge("/tmp/milvus_index_2");
//    ASSERT_FALSE(status.ok());
//    fiu_disable("VecIndexImpl.Add.throw_knowhere_exception");

    FIU_ENABLE_FIU("vecIndex.throw_read_exception");
    status = engine_ptr->Merge("dummy");
    ASSERT_FALSE(status.ok());
    fiu_disable("vecIndex.throw_read_exception");

    //CPU version invoke CopyToCpu will fail
    status = engine_ptr->CopyToCpu();
    ASSERT_FALSE(status.ok());

#ifdef MILVUS_GPU_VERSION
    status = engine_ptr->CopyToGpu(0, false);
    ASSERT_TRUE(status.ok());
    status = engine_ptr->GpuCache(0);
    ASSERT_TRUE(status.ok());
    status = engine_ptr->CopyToGpu(0, false);
    ASSERT_TRUE(status.ok());

//    auto new_engine = engine_ptr->Clone();
//    ASSERT_EQ(new_engine->Dimension(), dimension);
//    ASSERT_EQ(new_engine->Count(), ids.size());

    status = engine_ptr->CopyToCpu();
    ASSERT_TRUE(status.ok());
    engine_ptr->CopyToCpu();
    ASSERT_TRUE(status.ok());
#endif
}

TEST_F(EngineTest, ENGINE_IMPL_NULL_INDEX_TEST) {
    uint16_t dimension = 64;
    std::string file_path = "/tmp/milvus_index_1";
    auto engine_ptr = milvus::engine::EngineFactory::Build(
        dimension, file_path, milvus::engine::EngineType::FAISS_IVFFLAT, milvus::engine::MetricType::IP, 1024);

    fiu_init(0); // init
    fiu_enable("read_null_index", 1, NULL, 0);

    engine_ptr->Load(true);
    auto count = engine_ptr->Count();
    ASSERT_EQ(count, 0);

    auto dim = engine_ptr->Dimension();
    ASSERT_EQ(dim, dimension);

    auto status = engine_ptr->Merge("/tmp/milvus_index_2");
    ASSERT_FALSE(status.ok());

    auto build_index = engine_ptr->BuildIndex("/tmp/milvus_index_2", milvus::engine::EngineType::FAISS_IDMAP);
    ASSERT_EQ(build_index, nullptr);

    int64_t n = 0;
    const float* data = nullptr;
    int64_t k = 10;
    int64_t nprobe = 0;
    float* distances = nullptr;
    int64_t* labels = nullptr;
    bool hybrid = false;
    status = engine_ptr->Search(n, data, k, nprobe, distances, labels, hybrid);
    ASSERT_FALSE(status.ok());

    fiu_disable("read_null_index");
}

TEST_F(EngineTest, ENGINE_IMPL_THROW_EXCEPTION_TEST) {
    uint16_t dimension = 64;
    std::string file_path = "/tmp/invalid_file";
    fiu_init(0); // init
    fiu_enable("ValidateStringNotBool", 1, NULL, 0);

    auto engine_ptr = milvus::engine::EngineFactory::Build(
        dimension, file_path, milvus::engine::EngineType::FAISS_IVFFLAT, milvus::engine::MetricType::IP, 1024);

    fiu_disable("ValidateStringNotBool");

    fiu_init(0); // init
    fiu_enable("vecIndex.throw_read_exception", 1, NULL, 0);

    engine_ptr->Load(true);
    engine_ptr->CopyToGpu(0, true);
    engine_ptr->CopyToCpu();

    fiu_disable("vecIndex.throw_read_exception");
}
