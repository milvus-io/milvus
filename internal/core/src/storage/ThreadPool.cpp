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

#include "ThreadPool.h"

namespace milvus {

void
ThreadPool::Init() {
    std::lock_guard<std::mutex> lock(mutex_);
    for (int i = 0; i < min_threads_size_; i++) {
        std::thread t(&ThreadPool::Worker, this);
        assert(threads_.find(t.get_id()) == threads_.end());
        threads_[t.get_id()] = std::move(t);
        current_threads_size_++;
    }
}

void
ThreadPool::ShutDown() {
    LOG_SEGCORE_INFO_ << "Start shutting down " << name_;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        shutdown_ = true;
    }
    condition_lock_.notify_all();
    for (auto& thread : threads_) {
        if (thread.second.joinable()) {
            thread.second.join();
        }
    }
    LOG_SEGCORE_INFO_ << "Finish shutting down " << name_;
}

void
ThreadPool::FinishThreads() {
    while (!need_finish_threads_.empty()) {
        std::thread::id id;
        auto dequeue = need_finish_threads_.dequeue(id);
        if (dequeue) {
            auto iter = threads_.find(id);
            assert(iter != threads_.end());
            if (iter->second.joinable()) {
                iter->second.join();
            }
            threads_.erase(iter);
        }
    }
}

void
ThreadPool::Worker() {
    std::function<void()> func;
    bool dequeue;
    while (!shutdown_) {
        std::unique_lock<std::mutex> lock(mutex_);
        idle_threads_size_++;
        auto is_timeout = !condition_lock_.wait_for(
            lock, std::chrono::seconds(WAIT_SECONDS), [this]() {
                return shutdown_ || !work_queue_.empty();
            });
        idle_threads_size_--;
        if (work_queue_.empty()) {
            // Dynamic reduce thread number
            if (shutdown_) {
                current_threads_size_--;
                return;
            }
            if (is_timeout) {
                FinishThreads();
                if (current_threads_size_ > min_threads_size_) {
                    need_finish_threads_.enqueue(std::this_thread::get_id());
                    current_threads_size_--;
                    return;
                }
                continue;
            }
        }
        dequeue = work_queue_.dequeue(func);
        lock.unlock();
        if (dequeue) {
            func();
        }
    }
}
};  // namespace milvus
