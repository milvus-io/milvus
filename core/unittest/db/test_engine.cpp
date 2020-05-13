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

namespace {

static constexpr uint16_t DIMENSION = 64;
static constexpr int64_t ROW_COUNT = 1000;
static const char* INIT_PATH = "/tmp/milvus_index_1";

milvus::engine::ExecutionEnginePtr
CreateExecEngine(const milvus::json& json_params, milvus::engine::MetricType metric = milvus::engine::MetricType::IP) {
    auto engine_ptr = milvus::engine::EngineFactory::Build(
        DIMENSION,
        INIT_PATH,
        milvus::engine::EngineType::FAISS_IDMAP,
        metric,
        json_params);

    std::vector<float> data;
    std::vector<int64_t> ids;
    data.reserve(ROW_COUNT * DIMENSION);
    ids.reserve(ROW_COUNT);
    for (int64_t i = 0; i < ROW_COUNT; i++) {
        ids.push_back(i);
        for (uint16_t k = 0; k < DIMENSION; k++) {
            data.push_back(i * DIMENSION + k);
        }
    }

    auto status = engine_ptr->AddWithIds((int64_t)ids.size(), data.data(), ids.data());
    return engine_ptr;
}

} // namespace

TEST_F(EngineTest, FACTORY_TEST) {
    const milvus::json index_params = {{"nlist", 1024}};
    {
        auto engine_ptr = milvus::engine::EngineFactory::Build(
            512,
            "/tmp/milvus_index_1",
            milvus::engine::EngineType::INVALID,
            milvus::engine::MetricType::IP,
            index_params);

        ASSERT_TRUE(engine_ptr == nullptr);
    }

    {
        auto engine_ptr = milvus::engine::EngineFactory::Build(
            512,
            "/tmp/milvus_index_1",
            milvus::engine::EngineType::FAISS_IDMAP,
            milvus::engine::MetricType::IP,
            index_params);

        ASSERT_TRUE(engine_ptr != nullptr);
    }

    {
        auto engine_ptr =
            milvus::engine::EngineFactory::Build(512, "/tmp/milvus_index_1", milvus::engine::EngineType::FAISS_IVFFLAT,
                                                 milvus::engine::MetricType::IP, index_params);

        ASSERT_TRUE(engine_ptr != nullptr);
    }

    {
        auto engine_ptr = milvus::engine::EngineFactory::Build(
            512,
            "/tmp/milvus_index_1",
            milvus::engine::EngineType::FAISS_IVFSQ8,
            milvus::engine::MetricType::IP,
            index_params);

        ASSERT_TRUE(engine_ptr != nullptr);
    }

    {
        auto engine_ptr = milvus::engine::EngineFactory::Build(
            512,
            "/tmp/milvus_index_1",
            milvus::engine::EngineType::NSG_MIX,
            milvus::engine::MetricType::IP,
            index_params);

        ASSERT_TRUE(engine_ptr != nullptr);
    }

    {
        auto engine_ptr = milvus::engine::EngineFactory::Build(
            512,
            "/tmp/milvus_index_1",
            milvus::engine::EngineType::FAISS_PQ,
            milvus::engine::MetricType::IP,
            index_params);

        ASSERT_TRUE(engine_ptr != nullptr);
    }

    {
        auto engine_ptr = milvus::engine::EngineFactory::Build(
            512, "/tmp/milvus_index_1", milvus::engine::EngineType::SPTAG_KDT,
            milvus::engine::MetricType::L2, index_params);

        ASSERT_TRUE(engine_ptr != nullptr);
    }

    {
        auto engine_ptr = milvus::engine::EngineFactory::Build(
            512, "/tmp/milvus_index_1", milvus::engine::EngineType::SPTAG_KDT,
            milvus::engine::MetricType::L2, index_params);

        ASSERT_TRUE(engine_ptr != nullptr);
    }

    {
        fiu_init(0);
        // test ExecutionEngineImpl constructor when create VecIndex failed
        FIU_ENABLE_FIU("ExecutionEngineImpl.CreateVecIndex.invalid_type");
        ASSERT_ANY_THROW(milvus::engine::EngineFactory::Build(
            512, "/tmp/milvus_index_1", milvus::engine::EngineType::SPTAG_KDT,
            milvus::engine::MetricType::L2, index_params));
        fiu_disable("ExecutionEngineImpl.CreateVecIndex.invalid_type");
    }

    {
        // test ExecutionEngineImpl constructor when build failed
        FIU_ENABLE_FIU("ExecutionEngineImpl.throw_exception");
        ASSERT_ANY_THROW(milvus::engine::EngineFactory::Build(
            512, "/tmp/milvus_index_1", milvus::engine::EngineType::SPTAG_KDT,
            milvus::engine::MetricType::L2, index_params));
        fiu_disable("ExecutionEngineImpl.throw_exception");
    }
}

TEST_F(EngineTest, ENGINE_IMPL_TEST) {
    fiu_init(0);

    {
        milvus::json index_params = {{"nlist", 10}};
        auto engine_ptr = CreateExecEngine(index_params);

        ASSERT_EQ(engine_ptr->Dimension(), DIMENSION);
        ASSERT_EQ(engine_ptr->Count(), ROW_COUNT);
        ASSERT_EQ(engine_ptr->GetLocation(), INIT_PATH);
        ASSERT_EQ(engine_ptr->IndexMetricType(), milvus::engine::MetricType::IP);

        ASSERT_ANY_THROW(engine_ptr->BuildIndex(INIT_PATH, milvus::engine::EngineType::INVALID));

        auto engine_build = engine_ptr->BuildIndex("/tmp/milvus_index_2", milvus::engine::EngineType::FAISS_IVFSQ8);
        ASSERT_NE(engine_build, nullptr);
    }

    {
#ifndef MILVUS_GPU_VERSION
        milvus::json index_params = {{"nlist", 10}, {"m", 16}};
        auto engine_ptr = CreateExecEngine(index_params);
        //PQ don't support IP In gpu version
        auto engine_build = engine_ptr->BuildIndex("/tmp/milvus_index_3", milvus::engine::EngineType::FAISS_PQ);
        ASSERT_NE(engine_build, nullptr);
#endif
    }

#ifdef MILVUS_SUPPORT_SPTAG
    {
        milvus::json index_params = {{"nlist", 10}};
        auto engine_ptr = CreateExecEngine(index_params);
        auto engine_build = engine_ptr->BuildIndex("/tmp/milvus_index_4", milvus::engine::EngineType::SPTAG_KDT);
        engine_build = engine_ptr->BuildIndex("/tmp/milvus_index_5", milvus::engine::EngineType::SPTAG_BKT);
        engine_ptr->BuildIndex("/tmp/milvus_index_SPTAG_BKT", milvus::engine::EngineType::SPTAG_BKT);

        //CPU version invoke CopyToCpu will fail
        auto status = engine_ptr->CopyToCpu();
        ASSERT_FALSE(status.ok());
    }
#endif

#ifdef MILVUS_GPU_VERSION
    {
        FIU_ENABLE_FIU("ExecutionEngineImpl.CreatetVecIndex.gpu_res_disabled");
        milvus::json index_params = {{"search_length", 100}, {"out_degree", 40}, {"pool_size", 100}, {"knng", 200},
                                     {"candidate_pool_size", 500}};
        auto engine_ptr = CreateExecEngine(index_params, milvus::engine::MetricType::L2);
        engine_ptr->BuildIndex("/tmp/milvus_index_NSG_MIX", milvus::engine::EngineType::NSG_MIX);
        fiu_disable("ExecutionEngineImpl.CreatetVecIndex.gpu_res_disabled");

        auto status = engine_ptr->CopyToGpu(0, false);
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
    }
#endif
}

TEST_F(EngineTest, ENGINE_IMPL_NULL_INDEX_TEST) {
    uint16_t dimension = 64;
    std::string file_path = "/tmp/milvus_index_1";
    milvus::json index_params = {{"nlist", 1024}};
    auto engine_ptr = milvus::engine::EngineFactory::Build(
        dimension, file_path, milvus::engine::EngineType::FAISS_IVFFLAT, milvus::engine::MetricType::IP, index_params);

    fiu_init(0); // init
    fiu_enable("read_null_index", 1, NULL, 0);

    engine_ptr->Load(true);
    auto count = engine_ptr->Count();
    ASSERT_EQ(count, 0);

    auto dim = engine_ptr->Dimension();
    ASSERT_EQ(dim, dimension);

    auto build_index = engine_ptr->BuildIndex("/tmp/milvus_index_2", milvus::engine::EngineType::FAISS_IDMAP);
    ASSERT_EQ(build_index, nullptr);

    int64_t n = 0;
    const float* data = nullptr;
    int64_t k = 10;
    int64_t nprobe = 0;
    float* distances = nullptr;
    int64_t* labels = nullptr;
    bool hybrid = false;
    auto status = engine_ptr->Search(n, data, k, nprobe, distances, labels, hybrid);
    ASSERT_FALSE(status.ok());

    fiu_disable("read_null_index");
}

TEST_F(EngineTest, ENGINE_IMPL_THROW_EXCEPTION_TEST) {
    uint16_t dimension = 64;
    std::string file_path = "/tmp/invalid_file";
    milvus::json index_params = {{"nlist", 1024}};

    fiu_init(0); // init
    fiu_enable("ValidateStringNotBool", 1, NULL, 0);

    auto engine_ptr = milvus::engine::EngineFactory::Build(
        dimension, file_path, milvus::engine::EngineType::FAISS_IVFFLAT, milvus::engine::MetricType::IP, index_params);

    fiu_disable("ValidateStringNotBool");

    // Temporary removed for UT.
    // engine_ptr->Load(true);
    // engine_ptr->CopyToGpu(0, true);
    // engine_ptr->CopyToCpu();
}
