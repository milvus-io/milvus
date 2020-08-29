// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once

#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <utility>

#include "db/SimpleWaitNotify.h"
#include "db/snapshot/MetaEvent.h"
#include "utils/BlockingQueue.h"

namespace milvus::engine::snapshot {

using EventPtr = std::shared_ptr<MetaEvent>;
using ThreadPtr = std::shared_ptr<std::thread>;
using EventQueue = std::queue<EventPtr>;

constexpr size_t EVENT_QUEUE_SIZE = 4096;
constexpr size_t EVENT_TIMING_INTERVAL = 5;

class EventExecutor {
 public:
    ~EventExecutor() {
        Stop();
    }

    static void
    Init(StorePtr store) {
        auto& instance = GetInstanceImpl();
        if (instance.initialized_) {
            return;
        }
        instance.store_ = store;
        instance.initialized_ = true;
    }

    static EventExecutor&
    GetInstance() {
        auto& instance = GetInstanceImpl();
        if (!instance.initialized_) {
            throw std::runtime_error("OperationExecutor should be init");
        }
        return instance;
    }

    Status
    Submit(const EventPtr& evt, bool flush = false) {
        if (evt == nullptr) {
            return Status(SS_INVALID_ARGUMENT_ERROR, "Invalid Resource");
        }
        Enqueue(evt, flush);
        return Status::OK();
    }

    void
    Start() {
        if (running_.exchange(true)) {
            return;
        }

        if (gc_thread_ptr_ == nullptr) {
            gc_thread_ptr_ = std::make_shared<std::thread>(&EventExecutor::GCThread, this);
        }

        if (timing_thread_ptr_ == nullptr) {
            timing_thread_ptr_ = std::make_shared<std::thread>(&EventExecutor::TimingThread, this);
        }
    }

    void
    Stop() {
        if (!running_.exchange(false)) {
            //             executor has been stopped, just return
            return;
        }

        timing_.Notify();

        if (timing_thread_ptr_ != nullptr) {
            timing_thread_ptr_->join();
            timing_thread_ptr_ = nullptr;
        }

        if (gc_thread_ptr_ != nullptr) {
            gc_thread_ptr_->join();
            gc_thread_ptr_ = nullptr;
        }
    }

 private:
    EventExecutor() = default;

    static EventExecutor&
    GetInstanceImpl() {
        static EventExecutor executor;
        return executor;
    }

    void
    GCThread() {
        Status status;
        while (true) {
            auto front = cache_queues_.Take();

            while (front && !front->empty()) {
                EventPtr event_front(front->front());
                front->pop();
                if (event_front == nullptr) {
                    continue;
                }

                /* std::cout << std::this_thread::get_id() << " Dequeue Event " << std::endl; */
                status = event_front->Process(store_);
                if (!status.ok()) {
                    LOG_ENGINE_ERROR_ << "EventExecutor Handle Event Error: " << status.ToString();
                }
            }

            if ((!initialized_.load() || !running_.load()) && cache_queues_.Empty()) {
                break;
            }
        }
    }

    void
    TimingThread() {
        while (true) {
            timing_.Wait_For(std::chrono::seconds(EVENT_TIMING_INTERVAL));

            std::shared_ptr<EventQueue> queue;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                if (queue_ != nullptr && !queue_->empty()) {
                    queue = std::move(queue_);
                    queue_ = nullptr;
                }
            }

            if (queue) {
                cache_queues_.Put(queue);
            }

            if (!running_.load() || !initialized_.load()) {
                cache_queues_.Put(nullptr);
                break;
            }
        }
    }

    void
    Enqueue(const EventPtr& evt, bool flush = false) {
        if (!initialized_.load() || !running_.load()) {
            LOG_ENGINE_WARNING_ << "GcEvent exiting ...";
            return;
        }

        bool need_notify = flush;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            if (queue_ == nullptr) {
                queue_ = std::make_shared<EventQueue>();
            }

            queue_->push(evt);
            if (queue_->size() >= EVENT_QUEUE_SIZE) {
                need_notify = true;
            }
        }

        if (!initialized_.load() || !running_.load() || need_notify) {
            timing_.Notify();
        }
        if (evt != nullptr) {
            /* std::cout << std::this_thread::get_id() << " Enqueue Event " << std::endl; */
        }
    }

    EventExecutor(const EventExecutor&) = delete;

    ThreadPtr timing_thread_ptr_ = nullptr;
    ThreadPtr gc_thread_ptr_ = nullptr;
    SimpleWaitNotify timing_;
    std::mutex mutex_;
    std::shared_ptr<EventQueue> queue_;
    BlockingQueue<std::shared_ptr<EventQueue>> cache_queues_;
    std::atomic_bool initialized_ = false;
    std::atomic_bool running_ = false;
    StorePtr store_;
};

}  // namespace milvus::engine::snapshot
