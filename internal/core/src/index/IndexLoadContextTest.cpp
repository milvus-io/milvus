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

#include "index/IndexLoadContext.h"

namespace milvus::index {
namespace {

TEST(IndexLoadContextTest, PriorityOverridePreservesCallerStateAndAccounting) {
    folly::CancellationSource cancel;
    OpContext parent(cancel.getToken());
    parent.coload_fields = {101, 102};
    parent.pinned_segment_state = std::make_shared<const int>(7);
    parent.pinned_state_owner = &parent;
    for (const auto runtime : {std::optional<int32_t>{},
                               std::optional<int32_t>{0},
                               std::optional<int32_t>{1}}) {
        parent.runtime_load_priority = runtime;
        {
            IndexLoadContext load(&parent, 1);
            EXPECT_EQ(load.ctx.runtime_load_priority, runtime.value_or(1));
            EXPECT_EQ(load.ctx.coload_fields, parent.coload_fields);
            EXPECT_EQ(load.ctx.pinned_segment_state,
                      parent.pinned_segment_state);
            EXPECT_EQ(load.ctx.pinned_state_owner, &parent);
            cancel.requestCancellation();
            EXPECT_TRUE(load.ctx.cancellation_token.isCancellationRequested());
            load.ctx.storage_usage.scanned_cold_bytes = 3;
            load.ctx.storage_usage.scanned_total_bytes = 5;
        }
        EXPECT_EQ(parent.runtime_load_priority, runtime);
    }
    EXPECT_EQ(parent.storage_usage.scanned_cold_bytes, 9);
    EXPECT_EQ(parent.storage_usage.scanned_total_bytes, 15);
    IndexLoadContext warmup(nullptr, 1);
    EXPECT_EQ(warmup.ctx.runtime_load_priority, 1);
}

}  // namespace
}  // namespace milvus::index
