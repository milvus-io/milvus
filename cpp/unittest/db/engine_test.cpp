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
#include <boost/filesystem.hpp>
#include <vector>

#include "db/engine/EngineFactory.h"
#include "db/engine/ExecutionEngineImpl.h"
#include "server/ServerConfig.h"
#include "utils.h"

using namespace zilliz::milvus;

TEST_F(EngineTest, FACTORY_TEST) {
    {
        auto engine_ptr = engine::EngineFactory::Build(
                512,
                "/tmp/milvus_index_1",
                engine::EngineType::INVALID,
                engine::MetricType::IP,
                1024
        );

        ASSERT_TRUE(engine_ptr == nullptr);
    }

    {
        auto engine_ptr = engine::EngineFactory::Build(
                512,
                "/tmp/milvus_index_1",
                engine::EngineType::FAISS_IDMAP,
                engine::MetricType::IP,
                1024
        );

        ASSERT_TRUE(engine_ptr != nullptr);
    }

    {
        auto engine_ptr = engine::EngineFactory::Build(
                512,
                "/tmp/milvus_index_1",
                engine::EngineType::FAISS_IVFFLAT,
                engine::MetricType::IP,
                1024
        );

        ASSERT_TRUE(engine_ptr != nullptr);
    }

    {
        auto engine_ptr = engine::EngineFactory::Build(
                512,
                "/tmp/milvus_index_1",
                engine::EngineType::FAISS_IVFSQ8,
                engine::MetricType::IP,
                1024
        );

        ASSERT_TRUE(engine_ptr != nullptr);
    }

    {
        auto engine_ptr = engine::EngineFactory::Build(
                512,
                "/tmp/milvus_index_1",
                engine::EngineType::NSG_MIX,
                engine::MetricType::IP,
                1024
        );

        ASSERT_TRUE(engine_ptr != nullptr);
    }
}

TEST_F(EngineTest, ENGINE_IMPL_TEST) {
    uint16_t dimension = 64;
    std::string file_path = "/tmp/milvus_index_1";
    auto engine_ptr = engine::EngineFactory::Build(
            dimension,
            file_path,
            engine::EngineType::FAISS_IVFFLAT,
            engine::MetricType::IP,
            1024
    );

    std::vector<float> data;
    std::vector<long> ids;
    const int row_count = 10000;
    data.reserve(row_count*dimension);
    ids.reserve(row_count);
    for(long i = 0; i < row_count; i++) {
        ids.push_back(i);
        for(uint16_t k = 0; k < dimension; k++) {
            data.push_back(i*dimension + k);
        }
    }

    auto status = engine_ptr->AddWithIds((long)ids.size(), data.data(), ids.data());
    ASSERT_TRUE(status.ok());

    ASSERT_EQ(engine_ptr->Dimension(), dimension);
    ASSERT_EQ(engine_ptr->Count(), ids.size());

//    status = engine_ptr->CopyToGpu(0);
//    //ASSERT_TRUE(status.ok());
//
//    auto new_engine = engine_ptr->Clone();
//    ASSERT_EQ(new_engine->Dimension(), dimension);
//    ASSERT_EQ(new_engine->Count(), ids.size());
//    status = new_engine->CopyToCpu();
//    //ASSERT_TRUE(status.ok());
//
//    auto engine_build = new_engine->BuildIndex("/tmp/milvus_index_2", engine::EngineType::FAISS_IVFSQ8);
//    //ASSERT_TRUE(status.ok());

}
