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

#include <folly/CancellationToken.h>
#include <folly/ScopeGuard.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/futures/Future.h>
#include <gtest/gtest.h>
#include <stdlib.h>
#include <chrono>
#include <condition_variable>
#include <atomic>
#include <exception>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "common/common_type_c.h"
#include "futures/Future.h"
#include "futures/LeakyResult.h"
#include "futures/Ready.h"
#include "futures/future_c.h"
#include "futures/future_c_types.h"
#include "gtest/gtest.h"
#include "monitor/Monitor.h"
#include "segcore/OutputFields.h"
#include "storage/ThreadPool.h"
#include "storage/ThreadPools.h"
#include "test_utils/output_field_test_utils.h"

using namespace milvus::futures;

namespace {

class SearchFanoutPoolDeathTest : public testing::Test {
 protected:
    template <typename Fn>
    static auto
    Submit(Fn&& fn) {
        using Result = std::invoke_result_t<Fn>;
        auto task = std::make_shared<std::packaged_task<Result()>>(
            std::forward<Fn>(fn));
        auto future = task->get_future();
        milvus::futures::getSearchFanoutCPUExecutor()->add(
            [task] { (*task)(); });
        return future;
    }

    template <typename TestBody>
    void
    RunInChild(TestBody body) {
        milvus::test::AssertCompletesInChild([&] {
            milvus::InitCpuNum(2);
            milvus::SetThreadPoolMaxThreadsSize(16);
            milvus::SetMiddlePriorityThreadCoreCoefficient(1.0);
            body();
        });
    }
};

}  // namespace

TEST_F(SearchFanoutPoolDeathTest, ConfigurationCreatesPoolAndResizesInPlace) {
    RunInChild([] {
        milvus::ThreadPools::ResizeThreadPool(milvus::MIDDLE, 3.0);
        executor_set_search_fanout_thread_core_coefficient(3.5f);
        // The setter must create and size the pool before any getter call.
        EXPECT_EQ(milvus::monitor::internal_storage_pool_capacity_search_fanout
                      .Value(),
                  7);
        auto* search = milvus::futures::getSearchFanoutCPUExecutor();
        EXPECT_EQ(search->numThreads(), 7);
        executor_set_search_fanout_thread_core_coefficient(12.0f);
        EXPECT_EQ(milvus::futures::getSearchFanoutCPUExecutor(), search);
        auto& middle = milvus::ThreadPools::GetThreadPool(milvus::MIDDLE);
        EXPECT_EQ(middle.GetMaxThreadNum(), 6);
        EXPECT_EQ(search->numThreads(), 24);
        EXPECT_NE(search, milvus::futures::getSearchCPUExecutor());
    });
}

TEST_F(SearchFanoutPoolDeathTest, SmallPositiveRatioAtStartupAndHotUpdate) {
    RunInChild([] {
        executor_set_search_fanout_thread_core_coefficient(0.1f);
        EXPECT_EQ(milvus::monitor::internal_storage_pool_capacity_search_fanout
                      .Value(),
                  1);
        auto* fanout = milvus::futures::getSearchFanoutCPUExecutor();
        EXPECT_EQ(fanout->numThreads(), 1);

        executor_set_search_fanout_thread_core_coefficient(2.0f);
        ASSERT_EQ(fanout->numThreads(), 4);
        executor_set_search_fanout_thread_core_coefficient(0.1f);
        EXPECT_EQ(fanout->numThreads(), 1);
    });
}

TEST_F(SearchFanoutPoolDeathTest, LiveResizeIsIndependentAndUncapped) {
    RunInChild([] {
        const auto load_workers = milvus::ThreadPools::GetLoadExecutorWorkers();
        auto& middle = milvus::ThreadPools::GetThreadPool(milvus::MIDDLE);
        auto* search = milvus::futures::getSearchFanoutCPUExecutor();
        EXPECT_EQ(search->numThreads(), 4);
        for (const auto cap : {3, 0, 16}) {
            milvus::SetThreadPoolMaxThreadsSize(cap);
            milvus::ThreadPools::ResizeThreadPool(milvus::MIDDLE, 3.0);
            const auto expected = cap == 3 ? 3 : 6;
            EXPECT_EQ(middle.GetMaxThreadNum(), expected);
            EXPECT_EQ(search->numThreads(), 4);
            EXPECT_EQ(
                milvus::monitor::internal_storage_pool_capacity_middle.Value(),
                expected);
            EXPECT_EQ(
                milvus::monitor::internal_storage_pool_capacity_search_fanout
                    .Value(),
                4);
            EXPECT_EQ(milvus::ThreadPools::GetLoadExecutorWorkers(),
                      load_workers);
        }
        milvus::SetThreadPoolMaxThreadsSize(1);
        milvus::futures::setSearchFanoutThreadCoreCoefficient(12.0);
        EXPECT_EQ(middle.GetMaxThreadNum(), 6);
        EXPECT_EQ(search->numThreads(), 24);
        // Further configuration updates remain independent of the load limit.
        milvus::futures::setSearchFanoutThreadCoreCoefficient(20.0f);
        EXPECT_EQ(search->numThreads(), 40);
        EXPECT_EQ(milvus::monitor::internal_storage_pool_capacity_search_fanout
                      .Value(),
                  40);
        auto* executor = milvus::futures::getSearchCPUExecutor();
        const auto search_threads = executor->numThreads();
        auto restore = folly::makeGuard([&] {
            executor_set_search_thread_num(static_cast<int>(search_threads));
        });
        executor_set_search_thread_num(3);
        EXPECT_EQ(search->numThreads(), 40);
        EXPECT_EQ(milvus::ThreadPools::GetLoadExecutorWorkers(), load_workers);
    });
}

TEST_F(SearchFanoutPoolDeathTest, InvalidResizeKeepsBothPools) {
    RunInChild([] {
        auto& middle = milvus::ThreadPools::GetThreadPool(milvus::MIDDLE);
        auto* search = milvus::futures::getSearchFanoutCPUExecutor();
        for (const auto ratio : {0.0f, -1.0f}) {
            milvus::ThreadPools::ResizeThreadPool(milvus::MIDDLE, ratio);
            EXPECT_EQ(middle.GetMaxThreadNum(), 2);
            EXPECT_EQ(search->numThreads(), 4);
            EXPECT_EQ(milvus::MIDDLE_PRIORITY_THREAD_CORE_COEFFICIENT.load(),
                      1.0);
            milvus::futures::setSearchFanoutThreadCoreCoefficient(ratio);
            EXPECT_EQ(search->numThreads(), 4);
        }
    });
}

TEST_F(SearchFanoutPoolDeathTest,
       ExecutesWhileMiddleIsBlockedWithSeparateMetrics) {
    RunInChild([] {
        milvus::SetThreadPoolMaxThreadsSize(1);
        auto& middle = milvus::ThreadPools::GetThreadPool(milvus::MIDDLE);
        milvus::futures::getSearchFanoutCPUExecutor();
        std::promise<void> started;
        std::promise<void> unblock;
        auto gate = unblock.get_future().share();
        auto blocker = middle.Submit([&started, gate] {
            started.set_value();
            gate.wait();
        });
        auto release = folly::makeGuard([&] {
            unblock.set_value();
            blocker.wait();
        });
        ASSERT_EQ(started.get_future().wait_for(std::chrono::seconds(2)),
                  std::future_status::ready);
        auto queued = middle.Submit([] { return 1; });
        EXPECT_EQ(queued.wait_for(std::chrono::milliseconds(0)),
                  std::future_status::timeout);
        const auto middle_submitted =
            milvus::monitor::internal_storage_pool_task_submitted_total_middle
                .Value();
        const auto search_submitted =
            milvus::monitor::
                internal_storage_pool_task_submitted_total_search_fanout
                    .Value();

        auto task = Submit([] { return 42; });

        ASSERT_EQ(task.wait_for(std::chrono::seconds(2)),
                  std::future_status::ready);
        EXPECT_EQ(task.get(), 42);
        EXPECT_EQ(
            milvus::monitor::internal_storage_pool_task_submitted_total_middle
                .Value(),
            middle_submitted);
        EXPECT_EQ(milvus::monitor::
                      internal_storage_pool_task_submitted_total_search_fanout
                          .Value(),
                  search_submitted + 1);
    });
}

TEST_F(SearchFanoutPoolDeathTest, TaskErrorsAndCancellationDoNotStopWorkers) {
    RunInChild([] {
        milvus::SetThreadPoolMaxThreadsSize(1);
        auto failed =
            Submit([] { throw std::runtime_error("fanout failure"); });
        EXPECT_THROW(failed.get(), std::runtime_error);
        auto canceled = Submit([] { throw folly::FutureCancellation(); });
        EXPECT_THROW(canceled.get(), folly::FutureCancellation);
        auto next = Submit([] { return 42; });
        ASSERT_EQ(next.wait_for(std::chrono::seconds(2)),
                  std::future_status::ready);
        EXPECT_EQ(next.get(), 42);
    });
}

TEST_F(SearchFanoutPoolDeathTest, ConcurrentFirstAccessAndConfiguration) {
    RunInChild([] {
        std::promise<void> start;
        auto gate = start.get_future().share();
        auto creating = std::async(std::launch::async, [gate] {
            gate.wait();
            return milvus::futures::getSearchFanoutCPUExecutor();
        });
        start.set_value();
        milvus::futures::setSearchFanoutThreadCoreCoefficient(3.0);
        auto* pool = creating.get();
        EXPECT_EQ(pool, milvus::futures::getSearchFanoutCPUExecutor());
        EXPECT_EQ(pool->numThreads(), 6);
    });
}

TEST_F(SearchFanoutPoolDeathTest, ShrinkingAllowsWorkersToAcquirePool) {
    RunInChild([] {
        auto* pool = milvus::futures::getSearchFanoutCPUExecutor();
        pool->setNumThreads(2);
        std::atomic<int> started{0};
        std::promise<void> both_started, release;
        auto gate = release.get_future().share();
        std::vector<std::future<folly::CPUThreadPoolExecutor*>> tasks;
        for (int i = 0; i < 2; ++i) {
            tasks.emplace_back(Submit([&] {
                if (++started == 2) {
                    both_started.set_value();
                }
                gate.wait();
                return milvus::futures::getSearchFanoutCPUExecutor();
            }));
        }
        both_started.get_future().wait();
        auto resizing = std::async(std::launch::async, [] {
            milvus::futures::setSearchFanoutThreadCoreCoefficient(0.1f);
        });
        // Resize publishes capacity before it waits for retiring workers.
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (pool->numThreads() != 1 &&
               std::chrono::steady_clock::now() < deadline) {
            std::this_thread::yield();
        }
        EXPECT_EQ(pool->numThreads(), 1);
        release.set_value();
        for (auto& task : tasks) {
            EXPECT_EQ(task.get(), pool);
        }
        resizing.get();
    });
}

TEST_F(SearchFanoutPoolDeathTest, PreservesTaskMetricsAndPoolSampling) {
    RunInChild([] {
        using namespace milvus::monitor;
        milvus::futures::setSearchFanoutThreadCoreCoefficient(0.1f);
        auto* executor = milvus::futures::getSearchFanoutCPUExecutor();
        const auto submitted =
            internal_storage_pool_task_submitted_total_search_fanout.Value();
        const auto completed =
            internal_storage_pool_task_completed_total_search_fanout.Value();
        const auto queued =
            internal_storage_pool_queue_duration_seconds_search_fanout.Collect()
                .histogram;
        const auto executed =
            internal_storage_pool_execute_duration_seconds_search_fanout
                .Collect()
                .histogram;
        std::promise<void> started, release;
        auto gate = release.get_future().share();
        auto running = Submit([&] {
            started.set_value();
            gate.wait();
        });
        auto unblock = folly::makeGuard([&] {
            release.set_value();
            running.wait();
        });
        ASSERT_EQ(started.get_future().wait_for(std::chrono::seconds(2)),
                  std::future_status::ready);
        auto pending = Submit([] { return 42; });
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (
            (internal_storage_pool_active_threads_search_fanout.Value() != 1 ||
             internal_storage_pool_queue_depth_search_fanout.Value() != 1) &&
            std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        EXPECT_EQ(internal_storage_pool_capacity_search_fanout.Value(), 1);
        EXPECT_EQ(internal_storage_pool_active_threads_search_fanout.Value(),
                  1);
        EXPECT_EQ(internal_storage_pool_idle_threads_search_fanout.Value(), 0);
        EXPECT_EQ(internal_storage_pool_queue_depth_search_fanout.Value(), 1);
        unblock.dismiss();
        release.set_value();
        running.get();
        EXPECT_EQ(pending.get(), 42);
        // Joining also waits for Folly's task observers to finish reporting.
        executor->join();
        EXPECT_EQ(
            internal_storage_pool_task_submitted_total_search_fanout.Value(),
            submitted + 2);
        EXPECT_EQ(
            internal_storage_pool_task_completed_total_search_fanout.Value(),
            completed + 2);
        const auto queue_stats =
            internal_storage_pool_queue_duration_seconds_search_fanout.Collect()
                .histogram;
        const auto execute_stats =
            internal_storage_pool_execute_duration_seconds_search_fanout
                .Collect()
                .histogram;
        EXPECT_EQ(queue_stats.sample_count, queued.sample_count + 2);
        EXPECT_EQ(execute_stats.sample_count, executed.sample_count + 2);
        EXPECT_GT(queue_stats.sample_sum, queued.sample_sum);
        EXPECT_GT(execute_stats.sample_sum, executed.sample_sum);
    });
}

TEST_F(SearchFanoutPoolDeathTest, DispatchesOnlyParallelOutputWork) {
    RunInChild([] {
        using namespace milvus;
        using namespace milvus::segcore;
        struct Output {
            int64_t segment_id;
            std::vector<FetchedOutputField> fields;
        };
        const auto caller_thread = std::this_thread::get_id();
        for (int segment_count : {1, 2}) {
            for (int field_count : {1, 2}) {
                SCOPED_TRACE("segments=" + std::to_string(segment_count) +
                             ", fields=" + std::to_string(field_count));
                std::map<int32_t, Output> outputs;
                for (int i = 0; i < segment_count; ++i) {
                    outputs.emplace(i, Output{i, {}});
                }
                std::vector<FieldId> field_ids;
                for (int i = 0; i < field_count; ++i) {
                    field_ids.emplace_back(100 + i);
                }
                OpContext parent_ctx;
                parent_ctx.storage_usage.scanned_cold_bytes.store(1000);
                const auto submitted =
                    milvus::monitor::
                        internal_storage_pool_task_submitted_total_search_fanout
                            .Value();
                MaterializeOutputSegments(outputs, {}, [&](Output& output) {
                    const auto segment_thread = std::this_thread::get_id();
                    EXPECT_EQ(segment_thread == caller_thread,
                              segment_count == 1);
                    output.fields = FetchOutputFields(
                        field_ids,
                        output.segment_id,
                        &parent_ctx,
                        [&](FieldId field_id, OpContext* ctx) {
                            EXPECT_EQ(
                                std::this_thread::get_id() == segment_thread,
                                field_count == 1);
                            EXPECT_NE(ctx, &parent_ctx);
                            EXPECT_EQ(
                                ctx->storage_usage.scanned_cold_bytes.load(),
                                0);
                            ctx->storage_usage.scanned_cold_bytes.store(3);
                            ctx->storage_usage.scanned_total_bytes.store(5);
                            auto data = std::make_unique<DataArray>();
                            data->set_type(proto::schema::DataType::Int64);
                            data->mutable_scalars()
                                ->mutable_long_data()
                                ->add_data(output.segment_id * 1000 +
                                           field_id.get());
                            return data;
                        });
                });
                const auto expected_tasks =
                    field_count == 1 ? 0 : segment_count * field_count;
                EXPECT_EQ(
                    milvus::monitor::
                        internal_storage_pool_task_submitted_total_search_fanout
                            .Value(),
                    submitted + expected_tasks);
                EXPECT_EQ(parent_ctx.storage_usage.scanned_cold_bytes.load(),
                          1000);
                for (const auto& [_, output] : outputs) {
                    ASSERT_EQ(output.fields.size(), field_count);
                    for (int i = 0; i < field_count; ++i) {
                        const auto& field = output.fields[i];
                        EXPECT_EQ(field.field_id.get(), field_ids[i].get());
                        EXPECT_EQ(
                            field.field_data->scalars().long_data().data(0),
                            output.segment_id * 1000 + field_ids[i].get());
                        EXPECT_EQ(field.scanned_remote_bytes, 3);
                        EXPECT_EQ(field.scanned_total_bytes, 5);
                    }
                }
            }
        }
    });
}

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
            &executor, 0, [](folly::CancellationToken token) {
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
            &executor, 0, [](folly::CancellationToken token) {
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
            &executor, 0, [](folly::CancellationToken token) {
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
            &executor, 0, [](folly::CancellationToken token) {
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
            &executor, 0, [](folly::CancellationToken token) {
                for (int i = 0; i < 10; i++) {
                    milvus::futures::throwIfCancelled(token);
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
        free((char*)(s.error_msg));
    }
}

namespace {
class FutureTestEvent {
 public:
    void
    signal() {
        std::lock_guard<std::mutex> lock(mutex_);
        signaled_ = true;
        cv_.notify_all();
    }

    bool
    wait() {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(
            lock, std::chrono::seconds(5), [this] { return signaled_; });
    }

 private:
    std::mutex mutex_;
    std::condition_variable cv_;
    bool signaled_{false};
};

}  // namespace

TEST(Futures, ReadyBeforeQueuedProducer) {
    folly::CPUThreadPoolExecutor executor(1);
    FutureTestEvent started, produce, release_next;
    struct CallbackState {
        FutureTestEvent notified;
        std::atomic<int> first{0};
        std::thread::id producer_thread;
    } state;
    auto future =
        Future<int>::async(&executor, 0, [&](folly::CancellationToken) {
            state.producer_thread = std::this_thread::get_id();
            started.signal();
            EXPECT_TRUE(produce.wait());
            return new int(1);
        });
    EXPECT_TRUE(started.wait());
    future->registerReadyCallback(
        [](CLockedGoMutex* opaque) {
            auto& state = *reinterpret_cast<CallbackState*>(opaque);
            EXPECT_EQ(std::this_thread::get_id(), state.producer_thread);
            int expected = 0;
            state.first.compare_exchange_strong(expected, 1);
            state.notified.signal();
        },
        reinterpret_cast<CLockedGoMutex*>(&state));
    executor.add([&] {
        int expected = 0;
        state.first.compare_exchange_strong(expected, 2);
        EXPECT_TRUE(release_next.wait());
    });
    produce.signal();
    const bool notified = state.notified.wait();
    // Always release the next task, including when testing the old behavior.
    release_next.signal();
    EXPECT_TRUE(notified);
    EXPECT_EQ(state.first.load(), 1);
    executor.join();
    auto [result, status] = future->leakyGet();
    EXPECT_EQ(status.error_code, milvus::Success);
    delete static_cast<int*>(result);
}

TEST(Futures, ConsumeAndDestroyDuringNotification) {
    folly::CPUThreadPoolExecutor executor(1);
    FutureTestEvent started, produce;
    struct CallbackState {
        IFuture* future;
        FutureTestEvent consumed;
        FutureTestEvent finish;
    } state;
    auto future =
        Future<int>::async(&executor, 0, [&](folly::CancellationToken) {
            started.signal();
            EXPECT_TRUE(produce.wait());
            return new int(42);
        });
    EXPECT_TRUE(started.wait());
    state.future = future.get();
    future->registerReadyCallback(
        [](CLockedGoMutex* opaque) {
            auto& state = *reinterpret_cast<CallbackState*>(opaque);
            EXPECT_TRUE(state.future->isReady());
            auto [result, status] = state.future->leakyGet();
            EXPECT_EQ(status.error_code, milvus::Success);
            EXPECT_EQ(*static_cast<int*>(result), 42);
            delete static_cast<int*>(result);
            state.consumed.signal();
            EXPECT_TRUE(state.finish.wait());
        },
        reinterpret_cast<CLockedGoMutex*>(&state));
    produce.signal();
    EXPECT_TRUE(state.consumed.wait());
    future.reset();
    state.finish.signal();
    executor.join();
}
