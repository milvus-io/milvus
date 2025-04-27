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
    explicit ThreadPool(const int thread_core_coefficient, std::string name)
        : shutdown_(false), name_(std::move(name)) {
        idle_threads_size_ = 0;
        current_threads_size_ = 0;
        min_threads_size_ = CPU_NUM;
        max_threads_size_ = CPU_NUM * thread_core_coefficient;

        // only IO pool will set large limit, but the CPU helps nothing to IO operations,
        // we need to limit the max thread num, each thread will download 16~64 MiB data,
        // according to our benchmark, 16 threads is enough to saturate the network bandwidth.
        if (min_threads_size_ > 16) {
            min_threads_size_ = 16;
        }
        if (max_threads_size_ > 16) {
            max_threads_size_ = 16;
        }
        LOG_INFO("Init thread pool:{}", name_)
            << " with min worker num:" << min_threads_size_
            << " and max worker num:" << max_threads_size_;
        Init();
    }

    ~ThreadPool() {
        ShutDown();
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

    size_t
    GetThreadNum() {
        std::lock_guard<std::mutex> lock(mutex_);
        return current_threads_size_;
    }

    size_t
    GetMaxThreadNum() {
        return max_threads_size_;
    }

    template <typename F, typename... Args>
    auto
    Submit(F&& f, Args&&... args) -> std::future<decltype(f(args...))> {
        std::function<decltype(f(args...))()> func =
            std::bind(std::forward<F>(f), std::forward<Args>(args)...);
        auto task_ptr =
            std::make_shared<std::packaged_task<decltype(f(args...))()>>(func);

        std::function<void()> wrap_func = [task_ptr]() { (*task_ptr)(); };

        work_queue_.enqueue(wrap_func);

        std::lock_guard<std::mutex> lock(mutex_);

        if (idle_threads_size_ > 0) {
            condition_lock_.notify_one();
        } else if (current_threads_size_ < max_threads_size_) {
            // Dynamic increase thread number
            std::thread t(&ThreadPool::Worker, this);
            assert(threads_.find(t.get_id()) == threads_.end());
            threads_[t.get_id()] = std::move(t);
            current_threads_size_++;
        }

        return task_ptr->get_future();
    }

    void
    Worker();

    void
    FinishThreads();

 public:
    int min_threads_size_;
    int idle_threads_size_;
    int current_threads_size_;
    int max_threads_size_;
    bool shutdown_;
    static constexpr size_t WAIT_SECONDS = 2;
    SafeQueue<std::function<void()>> work_queue_;
    std::unordered_map<std::thread::id, std::thread> threads_;
    SafeQueue<std::thread::id> need_finish_threads_;
    std::mutex mutex_;
    std::condition_variable condition_lock_;
    std::string name_;
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
