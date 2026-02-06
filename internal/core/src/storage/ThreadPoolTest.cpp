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

#include "storage/ThreadPool.h"

namespace milvus {

class ThreadPoolTest : public testing::Test {
 protected:
    void
    SetUp() override {
        // Ensure CPU_NUM is initialized
        InitCpuNum(4);
    }
};

TEST_F(ThreadPoolTest, ResizeWithinBounds) {
    ThreadPool pool(1.0, "test_pool");

    // Resize to a value within bounds (1-16)
    pool.Resize(8);
    EXPECT_EQ(pool.GetMaxThreadNum(), 8);

    pool.Resize(1);
    EXPECT_EQ(pool.GetMaxThreadNum(), 1);

    pool.Resize(16);
    EXPECT_EQ(pool.GetMaxThreadNum(), 16);
}

TEST_F(ThreadPoolTest, ResizeBelowMinimum) {
    ThreadPool pool(1.0, "test_pool");

    // Resize to values below minimum (should be clamped to 1)
    pool.Resize(0);
    EXPECT_EQ(pool.GetMaxThreadNum(), 1);

    pool.Resize(-1);
    EXPECT_EQ(pool.GetMaxThreadNum(), 1);

    pool.Resize(-100);
    EXPECT_EQ(pool.GetMaxThreadNum(), 1);
}

TEST_F(ThreadPoolTest, ResizeAboveMaximum) {
    ThreadPool pool(1.0, "test_pool");

    // Resize to large values (no upper limit, should be accepted as-is)
    pool.Resize(17);
    EXPECT_EQ(pool.GetMaxThreadNum(), 17);

    pool.Resize(100);
    EXPECT_EQ(pool.GetMaxThreadNum(), 100);

    pool.Resize(1000);
    EXPECT_EQ(pool.GetMaxThreadNum(), 1000);
}

}  // namespace milvus
