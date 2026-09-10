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

#include <chrono>
#include <future>
#include <gtest/gtest.h>
#include <memory>

#include "gtest/gtest.h"
#include "storage/ThreadPool.h"
#include "storage/ThreadPools.h"

namespace milvus {

struct ThreadPoolsTestAccess {
    static std::unique_lock<std::shared_mutex>
    LockPoolMap() {
        return std::unique_lock<std::shared_mutex>(ThreadPools::mutex_);
    }
};

}  // namespace milvus

TEST(ThreadPool, ThreadNum) {
    auto& threadPool =
        milvus::ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::HIGH);
    auto& lowThreadPool =
        milvus::ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::LOW);
    auto max_thread_num = threadPool.GetMaxThreadNum();
    ASSERT_EQ(milvus::ThreadPools::GetLoadExecutorWorkers(),
              max_thread_num + lowThreadPool.GetMaxThreadNum());
    milvus::ThreadPools::ResizeThreadPool(milvus::ThreadPoolPriority::HIGH,
                                          0.0);
    ASSERT_EQ(threadPool.GetMaxThreadNum(), max_thread_num);
    milvus::ThreadPools::ResizeThreadPool(
        static_cast<milvus::ThreadPoolPriority>(6), 3.0);
    ASSERT_EQ(threadPool.GetMaxThreadNum(), max_thread_num);
    milvus::ThreadPools::ResizeThreadPool(milvus::ThreadPoolPriority::HIGH,
                                          2.0);
    ASSERT_EQ(threadPool.GetMaxThreadNum(), 2.0 * milvus::CPU_NUM);
    ASSERT_EQ(milvus::ThreadPools::GetLoadExecutorWorkers(),
              threadPool.GetMaxThreadNum() + lowThreadPool.GetMaxThreadNum());

    milvus::ThreadPools::ResizeThreadPool(
        milvus::ThreadPoolPriority::HIGH,
        static_cast<float>(max_thread_num) / milvus::CPU_NUM);
    ASSERT_EQ(threadPool.GetMaxThreadNum(), max_thread_num);
    ASSERT_EQ(milvus::ThreadPools::GetLoadExecutorWorkers(),
              max_thread_num + lowThreadPool.GetMaxThreadNum());
}

TEST(ThreadPool, LoadExecutorWorkerCountUsesCacheAfterInitialization) {
    const auto expected = milvus::ThreadPools::GetLoadExecutorWorkers();
    auto pool_map_lock = milvus::ThreadPoolsTestAccess::LockPoolMap();
    std::promise<void> query_started;
    auto started = query_started.get_future();
    auto query = std::async(std::launch::async, [&query_started]() {
        query_started.set_value();
        return milvus::ThreadPools::GetLoadExecutorWorkers();
    });

    const auto started_status = started.wait_for(std::chrono::seconds(1));
    const auto query_status = query.wait_for(std::chrono::seconds(2));
    pool_map_lock.unlock();

    EXPECT_EQ(started_status, std::future_status::ready);
    EXPECT_EQ(query_status, std::future_status::ready);
    EXPECT_EQ(query.get(), expected);
}
