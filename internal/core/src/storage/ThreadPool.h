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

#pragma once

#include <functional>
#include <future>
#include <mutex>
#include <memory>
#include <queue>
#include <thread>
#include <vector>
#include <utility>

#include "SafeQueue.h"
#include "common/Common.h"
#include "log/Log.h"

namespace milvus {

class ThreadPool {
 public:
    explicit ThreadPool(const int thread_core_coefficient) : shutdown_(false) {
        auto thread_num = CPU_NUM * thread_core_coefficient;
        LOG_SEGCORE_INFO_ << "Thread pool's worker num:" << thread_num;
        threads_ = std::vector<std::thread>(thread_num);
        Init();
    }

    ~ThreadPool() {
        ShutDown();
    }

    static ThreadPool&
    GetInstance() {
        static ThreadPool pool(THREAD_CORE_COEFFICIENT);
        return pool;
    }

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool(ThreadPool&&) = delete;
    ThreadPool&
    operator=(const ThreadPool&) = delete;
    ThreadPool&
    operator=(ThreadPool&&) = delete;

    void
    Init();

    void
    ShutDown();

    template <typename F, typename... Args>
    auto
    // Submit(F&& f, Args&&... args) -> std::future<decltype(f(args...))>;
    Submit(F&& f, Args&&... args) -> std::future<decltype(f(args...))> {
        std::function<decltype(f(args...))()> func =
            std::bind(std::forward<F>(f), std::forward<Args>(args)...);
        auto task_ptr =
            std::make_shared<std::packaged_task<decltype(f(args...))()>>(func);

        std::function<void()> wrap_func = [task_ptr]() { (*task_ptr)(); };

        work_queue_.enqueue(wrap_func);

        condition_lock_.notify_one();

        return task_ptr->get_future();
    }

 public:
    bool shutdown_;
    SafeQueue<std::function<void()>> work_queue_;
    std::vector<std::thread> threads_;
    std::mutex mutex_;
    std::condition_variable condition_lock_;
};

class Worker {
 private:
    int id_;
    ThreadPool* pool_;

 public:
    Worker(ThreadPool* pool, const int id) : pool_(pool), id_(id) {
    }

    void
    operator()() {
        std::function<void()> func;
        bool dequeue;
        while (!pool_->shutdown_) {
            std::unique_lock<std::mutex> lock(pool_->mutex_);
            if (pool_->work_queue_.empty()) {
                pool_->condition_lock_.wait(lock);
            }
            dequeue = pool_->work_queue_.dequeue(func);
            lock.unlock();
            if (dequeue) {
                func();
            }
        }
    }
};

}  // namespace milvus
