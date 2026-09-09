// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "storage/AsyncLoadExecutor.h"

#include <chrono>
#include <future>

#include "common/EasyAssert.h"
#include "folly/ScopeGuard.h"
#include "gtest/gtest.h"

namespace milvus::storage {
namespace {

TEST(AsyncLoadExecutorTest, ResizesExistingExecutorWithQueuedWork) {
    const int previous = GetAsyncLoadThreadPoolSize();
    auto restore =
        folly::makeGuard([&]() { SetAsyncLoadThreadPoolSize(previous); });
    SetAsyncLoadThreadPoolSize(1);
    auto executor =
        ResolveAsyncLoadExecutor({}, milvus::proto::common::LoadPriority::HIGH);
    std::promise<void> release;
    auto released = release.get_future().share();
    std::promise<void> first_started;
    std::promise<void> second_started;
    std::promise<void> first_done;
    std::promise<void> second_done;
    auto first_completion = first_done.get_future();
    auto second_completion = second_done.get_future();
    bool first_submitted = false;
    bool second_submitted = false;
    auto unblock = folly::makeGuard([&]() {
        release.set_value();
        if (first_submitted && first_completion.valid()) {
            first_completion.wait();
        }
        if (second_submitted && second_completion.valid()) {
            second_completion.wait();
        }
    });
    executor->add([&]() {
        first_started.set_value();
        released.wait();
        first_done.set_value();
    });
    first_submitted = true;
    EXPECT_EQ(first_started.get_future().wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    executor->add([&]() {
        second_started.set_value();
        released.wait();
        second_done.set_value();
    });
    second_submitted = true;
    auto second_start = second_started.get_future();
    EXPECT_EQ(second_start.wait_for(std::chrono::milliseconds(20)),
              std::future_status::timeout);

    SetAsyncLoadThreadPoolSize(2);
    EXPECT_EQ(GetAsyncLoadThreadPoolSize(), 2);
    EXPECT_EQ(second_start.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    release.set_value();
    unblock.dismiss();
    first_completion.get();
    second_completion.get();

    SetAsyncLoadThreadPoolSize(1);
    EXPECT_EQ(GetAsyncLoadThreadPoolSize(), 1);
    // A keep-alive acquired before resizing still submits to the same pool.
    std::promise<void> after_resize;
    auto completion = after_resize.get_future();
    executor->add([&]() { after_resize.set_value(); });
    EXPECT_EQ(completion.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    completion.get();
}

TEST(AsyncLoadExecutorTest, InvalidSizePreservesConfiguration) {
    const int previous = GetAsyncLoadThreadPoolSize();
    for (const int value : {0, -1}) {
        EXPECT_THROW(SetAsyncLoadThreadPoolSize(value), milvus::SegcoreError);
        EXPECT_EQ(GetAsyncLoadThreadPoolSize(), previous);
    }
}

}  // namespace
}  // namespace milvus::storage
