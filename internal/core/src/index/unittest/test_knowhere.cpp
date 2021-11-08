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

#include "value/config/ServerConfig.h"
#include "wrapper/KnowhereConfig.h"
#include "wrapper/utils.h"

#include <fiu-control.h>
#include <fiu/fiu-local.h>
#include <gtest/gtest.h>

using namespace milvus;
using namespace milvus::engine;

TEST_F(KnowhereTest, KNOWHERE_RESOURCE_TEST) {
    std::string config_path(CONFIG_PATH);
    config_path += CONFIG_FILE;
    milvus::server::Config& config = milvus::server::Config::GetInstance();
    milvus::Status s = config.LoadConfigFile(config_path);
    ASSERT_TRUE(s.ok());

    KnowhereConfig::SetSimdType(KnowhereConfig::SimdType::AUTO);
    KnowhereConfig::SetSimdType(KnowhereConfig::SimdType::SSE4_2);
    KnowhereConfig::SetSimdType(KnowhereConfig::SimdType::AVX2);
    KnowhereConfig::SetSimdType(KnowhereConfig::SimdType::AVX512);

    KnowhereConfig::SetBlasThreshold(100);

    KnowhereConfig::SetEarlyStopThreshold(0.0);

    KnowhereConfig::SetClusteringType(KnowhereConfig::ClusteringType::K_MEANS);
    KnowhereConfig::SetClusteringType(KnowhereConfig::ClusteringType::K_MEANS_PLUS_PLUS);

    KnowhereConfig::SetStatisticsLevel(0);

    KnowhereConfig::SetLogHandler();

#ifdef MILVUS_GPU_VERSION
    std::vector<int64_t> gpu_ids = {0};
    KnowhereConfig::InitGPUResource(gpu_ids);

    KnowhereConfig::FreeGPUResource();
#endif
}
