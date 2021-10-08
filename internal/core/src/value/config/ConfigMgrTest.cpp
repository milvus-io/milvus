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

#include <fiu-control.h>
#include <fiu/fiu-local.h>
#include <gtest/gtest.h>
#include "ConfigMgr.h"

#include "value/config/ServerConfig.h"

namespace milvus {

// TODO: need a safe directory for testing
// TEST(ConfigMgrTest, set_version) {
//    ConfigMgr::GetInstance().Init();
//    ConfigMgr::GetInstance().LoadMemory(R"(
// version: 0.1
//)");
//    ConfigMgr::GetInstance().FilePath() = "/tmp/milvus_unittest_configmgr.yaml";
//
//    ASSERT_EQ(ConfigMgr::GetInstance().Get("version"), "0.1");
//    ConfigMgr::GetInstance().Set("version", "100.0");
//    ASSERT_EQ(ConfigMgr::GetInstance().Get("version"), "100.0");
//}

}  // namespace milvus
