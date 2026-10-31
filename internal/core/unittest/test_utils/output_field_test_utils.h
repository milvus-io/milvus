// Copyright (C) 2026 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <thread>

namespace milvus::test {

// A deadlocked future cannot be safely abandoned. Fail only this child after
// 30 seconds; never leave a blocked worker running in the parent test process.
template <typename TestBody>
void
AssertCompletesInChild(TestBody body) {
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    ASSERT_EXIT(
        {
            std::thread([] {
                std::this_thread::sleep_for(std::chrono::seconds(30));
                std::_Exit(124);
            }).detach();
            body();
            std::_Exit(::testing::Test::HasFailure() ? 1 : 0);
        },
        ::testing::ExitedWithCode(0),
        "");
}

}  // namespace milvus::test
