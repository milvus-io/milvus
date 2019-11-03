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

#include "wrapper/KnowhereResource.h"
#include "wrapper/utils.h"
#include "server/Config.h"

#include <gtest/gtest.h>

TEST_F(KnowhereTest, KNOWHERE_RESOURCE_TEST) {
    std::string config_path(CONFIG_PATH);
    config_path += CONFIG_FILE;
    milvus::server::Config& config = milvus::server::Config::GetInstance();
    milvus::Status s = config.LoadConfigFile(config_path);
    ASSERT_TRUE(s.ok());

    milvus::engine::KnowhereResource::Initialize();
    milvus::engine::KnowhereResource::Finalize();
}
