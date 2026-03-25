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
        // Reset to default max threads size
        SetThreadPoolMaxThreadsSize(16);
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

    // Resize to values above maximum (should be clamped to 16)
    pool.Resize(17);
    EXPECT_EQ(pool.GetMaxThreadNum(), 16);

    pool.Resize(100);
    EXPECT_EQ(pool.GetMaxThreadNum(), 16);

    pool.Resize(1000);
    EXPECT_EQ(pool.GetMaxThreadNum(), 16);
}

TEST_F(ThreadPoolTest, ConfigurableMaxThreadsSize) {
    // Set a custom max threads size
    SetThreadPoolMaxThreadsSize(32);

    ThreadPool pool(10.0, "test_pool");
    // CPU_NUM=4, coefficient=10.0, so computed=40, clamped to 32
    EXPECT_EQ(pool.GetMaxThreadNum(), 32);

    // Resize should also respect the new limit
    pool.Resize(40);
    EXPECT_EQ(pool.GetMaxThreadNum(), 32);

    pool.Resize(20);
    EXPECT_EQ(pool.GetMaxThreadNum(), 20);
}

TEST_F(ThreadPoolTest, DisableMaxThreadsLimit) {
    // Set to 0 to disable the limit
    SetThreadPoolMaxThreadsSize(0);

    ThreadPool pool(10.0, "test_pool");
    // CPU_NUM=4, coefficient=10.0, so computed=40, no limit applied
    EXPECT_EQ(pool.GetMaxThreadNum(), 40);

    // Resize should also have no limit
    pool.Resize(100);
    EXPECT_EQ(pool.GetMaxThreadNum(), 100);
}

TEST_F(ThreadPoolTest, SetThreadPoolMaxThreadsSize) {
    // Default is 16
    EXPECT_EQ(THREAD_POOL_MAX_THREADS_SIZE.load(), 16);

    // Set to a positive value
    SetThreadPoolMaxThreadsSize(64);
    EXPECT_EQ(THREAD_POOL_MAX_THREADS_SIZE.load(), 64);

    // Set to 0 (disable limit)
    SetThreadPoolMaxThreadsSize(0);
    EXPECT_EQ(THREAD_POOL_MAX_THREADS_SIZE.load(), 0);

    // Set to negative (also disables limit)
    SetThreadPoolMaxThreadsSize(-1);
    EXPECT_EQ(THREAD_POOL_MAX_THREADS_SIZE.load(), -1);
}

TEST_F(ThreadPoolTest, DynamicMaxThreadsSizeUpdate) {
    // Start with limit=16, create pool with coefficient that exceeds it
    ThreadPool pool(10.0, "test_pool");
    // CPU_NUM=4, coefficient=10.0, computed=40, clamped to 16
    EXPECT_EQ(pool.GetMaxThreadNum(), 16);

    // Increase limit and resize - should now allow larger size
    SetThreadPoolMaxThreadsSize(64);
    pool.Resize(40);
    EXPECT_EQ(pool.GetMaxThreadNum(), 40);

    // Decrease limit and resize - should clamp to new limit
    SetThreadPoolMaxThreadsSize(8);
    pool.Resize(20);
    EXPECT_EQ(pool.GetMaxThreadNum(), 8);
}

}  // namespace milvus
