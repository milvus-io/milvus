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
#include "futures/Future.h"
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <stdlib.h>
#include <mutex>
#include <exception>

using namespace milvus::futures;

TEST(Futures, LeakyResult) {
    {
        LeakyResult<int> leaky_result;
        ASSERT_ANY_THROW(leaky_result.leakyGet());
    }

    {
        auto leaky_result = LeakyResult<int>(1, "error");
        auto [r, s] = leaky_result.leakyGet();
        ASSERT_EQ(r, nullptr);
        ASSERT_EQ(s.error_code, 1);
        ASSERT_STREQ(s.error_msg, "error");
        free((char*)(s.error_msg));
    }
    {
        auto leaky_result = LeakyResult<int>(new int(1));
        auto [r, s] = leaky_result.leakyGet();
        ASSERT_NE(r, nullptr);
        ASSERT_EQ(*(int*)(r), 1);
        ASSERT_EQ(s.error_code, 0);
        ASSERT_EQ(s.error_msg, nullptr);
        delete (int*)(r);
    }
    {
        LeakyResult<int> leaky_result(1, "error");
        LeakyResult<int> leaky_result_moved(std::move(leaky_result));
        auto [r, s] = leaky_result_moved.leakyGet();
        ASSERT_EQ(r, nullptr);
        ASSERT_EQ(s.error_code, 1);
        ASSERT_STREQ(s.error_msg, "error");
        free((char*)(s.error_msg));
    }
    {
        LeakyResult<int> leaky_result(1, "error");
        LeakyResult<int> leaky_result_moved;
        leaky_result_moved = std::move(leaky_result);
        auto [r, s] = leaky_result_moved.leakyGet();
        ASSERT_EQ(r, nullptr);
        ASSERT_EQ(s.error_code, 1);
        ASSERT_STREQ(s.error_msg, "error");
        free((char*)(s.error_msg));
    }
}

TEST(Futures, Ready) {
    Ready<int> ready;
    int a = 0;
    ready.callOrRegisterCallback([&a]() { a++; });
    ASSERT_EQ(a, 0);
    ASSERT_FALSE(ready.isReady());
    ready.setValue(1);
    ASSERT_EQ(a, 1);
    ASSERT_TRUE(ready.isReady());
    ready.callOrRegisterCallback([&a]() { a++; });
    ASSERT_EQ(a, 2);

    ASSERT_EQ(std::move(ready).getValue(), 1);
}

TEST(Futures, Future) {
    folly::CPUThreadPoolExecutor executor(2);

    // success path.
    {
        // try a async function
        auto future = milvus::futures::Future<int>::async(
            &executor, 0, [](milvus::futures::CancellationToken token) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                return new int(1);
            });
        ASSERT_FALSE(future->isReady());

        std::mutex mu;
        mu.lock();
        future->registerReadyCallback(
            [](CLockedGoMutex* mutex) { ((std::mutex*)(mutex))->unlock(); },
            (CLockedGoMutex*)(&mu));
        mu.lock();
        ASSERT_TRUE(future->isReady());
        auto [r, s] = future->leakyGet();

        ASSERT_NE(r, nullptr);
        ASSERT_EQ(*(int*)(r), 1);
        ASSERT_EQ(s.error_code, 0);
        ASSERT_EQ(s.error_msg, nullptr);
        delete (int*)(r);
    }

    // error path.
    {
        // try a async function
        auto future = milvus::futures::Future<int>::async(
            &executor, 0, [](milvus::futures::CancellationToken token) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                throw milvus::SegcoreError(milvus::NotImplemented,
                                           "unimplemented");
                return new int(1);
            });
        ASSERT_FALSE(future->isReady());

        std::mutex mu;
        mu.lock();
        future->registerReadyCallback(
            [](CLockedGoMutex* mutex) { ((std::mutex*)(mutex))->unlock(); },
            (CLockedGoMutex*)(&mu));
        mu.lock();
        ASSERT_TRUE(future->isReady());
        auto [r, s] = future->leakyGet();

        ASSERT_EQ(r, nullptr);
        ASSERT_EQ(s.error_code, milvus::NotImplemented);
        ASSERT_STREQ(s.error_msg, "unimplemented");
        free((char*)(s.error_msg));
    }

    {
        // try a async function
        auto future = milvus::futures::Future<int>::async(
            &executor, 0, [](milvus::futures::CancellationToken token) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                throw std::runtime_error("unimplemented");
                return new int(1);
            });
        ASSERT_FALSE(future->isReady());

        std::mutex mu;
        mu.lock();
        future->registerReadyCallback(
            [](CLockedGoMutex* mutex) { ((std::mutex*)(mutex))->unlock(); },
            (CLockedGoMutex*)(&mu));
        mu.lock();
        ASSERT_TRUE(future->isReady());
        auto [r, s] = future->leakyGet();

        ASSERT_EQ(r, nullptr);
        ASSERT_EQ(s.error_code, milvus::UnexpectedError);
        free((char*)(s.error_msg));
    }

    {
        // try a async function
        auto future = milvus::futures::Future<int>::async(
            &executor, 0, [](milvus::futures::CancellationToken token) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                throw folly::FutureNotReady();
                return new int(1);
            });
        ASSERT_FALSE(future->isReady());

        std::mutex mu;
        mu.lock();
        future->registerReadyCallback(
            [](CLockedGoMutex* mutex) { ((std::mutex*)(mutex))->unlock(); },
            (CLockedGoMutex*)(&mu));
        mu.lock();
        ASSERT_TRUE(future->isReady());
        auto [r, s] = future->leakyGet();

        ASSERT_EQ(r, nullptr);
        ASSERT_EQ(s.error_code, milvus::FollyOtherException);
        free((char*)(s.error_msg));
    }

    // cancellation path.
    {
        // try a async function
        auto future = milvus::futures::Future<int>::async(
            &executor, 0, [](milvus::futures::CancellationToken token) {
                for (int i = 0; i < 10; i++) {
                    token.throwIfCancelled();
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }
                return new int(1);
            });
        ASSERT_FALSE(future->isReady());
        future->cancel();

        std::mutex mu;
        mu.lock();
        future->registerReadyCallback(
            [](CLockedGoMutex* mutex) { ((std::mutex*)(mutex))->unlock(); },
            (CLockedGoMutex*)(&mu));
        mu.lock();
        ASSERT_TRUE(future->isReady());
        auto [r, s] = future->leakyGet();

        ASSERT_EQ(r, nullptr);
        ASSERT_EQ(s.error_code, milvus::FollyCancel);
    }
}