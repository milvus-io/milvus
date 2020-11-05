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

#include "value/config/ServerConfig.h"

TEST(ServerConfigTest, parse_invalid_devices) {
    fiu_init(0);
    fiu_enable("ParseGPUDevices.invalid_format", 1, nullptr, 0);
    auto collections = milvus::ParseGPUDevices("gpu0,gpu1");
    ASSERT_EQ(collections.size(), 0);
}
