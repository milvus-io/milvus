// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <gtest/gtest.h>

#include "segcore/collection_c.h"
#include "segcore/segment_c.h"
#include "segcore/reduce_c.h"

#include "test_utils/c_api_test_utils.h"
#include "test_utils/storage_test_utils.h"
#include "test_utils/GenExprProto.h"

TEST(CApiTest, LoadInfoTest) {
    auto load_info = std::make_shared<LoadFieldDataInfo>();
    auto c_load_info = reinterpret_cast<CLoadFieldDataInfo*>(load_info.get());
    AppendLoadFieldInfo(c_load_info, 100, 100);
    EnableMmap(c_load_info, 100, true);

    EXPECT_TRUE(load_info->field_infos.at(100).enable_mmap);
}
