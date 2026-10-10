// Copyright(C) 2019 - 2020 Zilliz.All rights reserved.
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

#include "folly/ScopeGuard.h"
#include "index/LoadResource.h"
#include "index/Meta.h"
#include "storage/LoadAdmissionController.h"
#include "storage/PluginLoader.h"
#include "test_utils/PlannerCipherPlugin.h"

namespace milvus::storage {
namespace {

TEST(ScalarLoadAdmissionTest, EncryptedBitmapIncludesDestinationAndFullStreamMemory) {
    auto& admission = LoadAdmissionController::GetInstance();
    const auto previous = admission.CapacityBytes();
    auto& plugins = PluginLoader::GetInstance();
    plugins.registerPluginForTest(std::make_shared<milvus::test::PlannerCipherPlugin>());
    auto restore = folly::makeGuard([&] {
        admission.SetCapacityBytes(previous);
        plugins.unregisterPluginForTest("CipherPlugin");
    });
    admission.SetCapacityBytes(0);
    constexpr uint64_t bytes = 32 * 1024 * 1024;
    const auto request = index::ScalarIndexLoadResource(
        DataType::INT64, 0, bytes,
        {{index::INDEX_TYPE, index::BITMAP_INDEX_TYPE},
         {index::SCALAR_INDEX_ENGINE_VERSION, "3"}}, false, 1024);
    EXPECT_EQ(request.final_memory_cost,
              bytes + index::kScalarIndexFixedResidentBytes);
    EXPECT_EQ(request.max_memory_cost,
              4 * bytes + index::kScalarIndexFixedResidentBytes);
    EXPECT_EQ(request.final_disk_cost, 0);
    EXPECT_EQ(request.max_disk_cost, 0);
}

}  // namespace
}  // namespace milvus::storage
