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

#include "config/Config.h"
#include "wrapper/KnowhereResource.h"
#include "wrapper/utils.h"

#include <fiu-control.h>
#include <fiu-local.h>
#include <gtest/gtest.h>

TEST_F(KnowhereTest, KNOWHERE_RESOURCE_TEST) {
    std::string config_path(CONFIG_PATH);
    config_path += CONFIG_FILE;
    milvus::server::Config& config = milvus::server::Config::GetInstance();
    milvus::Status s = config.LoadConfigFile(config_path);
    ASSERT_TRUE(s.ok());

    milvus::engine::KnowhereResource::Initialize();
    milvus::engine::KnowhereResource::Finalize();

#ifdef MILVUS_GPU_VERSION
    fiu_init(0);
    fiu_enable("check_config_gpu_resource_enable_fail", 1, nullptr, 0);
    s = milvus::engine::KnowhereResource::Initialize();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_gpu_resource_enable_fail");

    fiu_enable("KnowhereResource.Initialize.disable_gpu", 1, nullptr, 0);
    s = milvus::engine::KnowhereResource::Initialize();
    ASSERT_TRUE(s.ok());
    fiu_disable("KnowhereResource.Initialize.disable_gpu");

    fiu_enable("check_gpu_resource_config_build_index_fail", 1, nullptr, 0);
    s = milvus::engine::KnowhereResource::Initialize();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_gpu_resource_config_build_index_fail");

    fiu_enable("check_gpu_resource_config_search_fail", 1, nullptr, 0);
    s = milvus::engine::KnowhereResource::Initialize();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_gpu_resource_config_search_fail");
#endif
}
