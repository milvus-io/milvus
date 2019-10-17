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
#include "server/Config.h"

#include <gtest/gtest.h>

namespace {

static const char* CONFIG_FILE_PATH = "./milvus/conf/server_config.yaml";
static const char* LOG_FILE_PATH = "./milvus/conf/log_config.conf";

} // namespace

TEST(KnowhereTest, KNOWHERE_RESOURCE_TEST) {
    milvus::server::Config &config = milvus::server::Config::GetInstance();
    milvus::Status s = config.LoadConfigFile(CONFIG_FILE_PATH);
    ASSERT_TRUE(s.ok());

    milvus::engine::KnowhereResource::Initialize();
    milvus::engine::KnowhereResource::Finalize();
}
