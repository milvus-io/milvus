// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <utility>

#include "index/contracts/Registry.h"
#include "index/contracts/build/ScalarBuildInput.h"
#include "indexbuilder/BuildInputMaterializer.h"
#include "knowhere/build_scheduler.h"

namespace milvus::indexbuilder {
namespace {

class DefaultReservationBuilder final
    : public index::IArtifactBuilder<index::ScalarBuildInput<int32_t>> {
 public:
    storage::ArtifactPtr
        Build(const index::ScalarBuildInput<int32_t>&) &&
        override {
        EXPECT_FALSE(knowhere::BuildScheduler::Instance().TryReserve());
        return nullptr;
    }
};

TEST(BuildInputMaterializerTest, ScalarBuildSharesDefaultReservation) {
    auto& scheduler = knowhere::BuildScheduler::Instance();
    index::BuilderRegistry<index::ScalarBuildInput<int32_t>>::Instance()
        .Register("test_default_reservation", [](const index::BuildParams&) {
            return std::make_unique<DefaultReservationBuilder>();
        });
    auto materializer = MakeScalarBuildInputMaterializer(
        DataType::INT32, DataType::INT32, 0, "test_default_reservation", {});
    auto other = scheduler.TryReserve();
    ASSERT_TRUE(other.has_value());
    EXPECT_FALSE(materializer->TryReserveBuild());
    other.reset();
    ASSERT_TRUE(materializer->TryReserveBuild());
    EXPECT_TRUE(materializer->TryReserveBuild());
    EXPECT_FALSE(scheduler.TryReserve());
    EXPECT_EQ(std::move(*materializer).Build(), nullptr);
    // BuildSession destroys the materializer before serialization/upload.
    materializer.reset();
    EXPECT_TRUE(scheduler.TryReserve());
}

}  // namespace
}  // namespace milvus::indexbuilder
