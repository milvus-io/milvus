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
#include <thread>

#include "db/snapshot/Event.h"
#include "utils/BlockingQueue.h"

namespace milvus {
namespace engine {
namespace snapshot {

using EventPtr = std::shared_ptr<Event>;
using ThreadPtr = std::shared_ptr<std::thread>;
using EventQueue = BlockingQueue<EventPtr>;

class EventExecutor {
 public:
    EventExecutor() = default;
    EventExecutor(const EventExecutor&) = delete;

    ~EventExecutor() {
        Stop();
    }

    static EventExecutor&
    GetInstance() {
        static EventExecutor inst;
        return inst;
    }

    Status
    Submit(const EventPtr& evt) {
        if (evt == nullptr) {
            return Status(SS_INVALID_ARGUMENT_ERROR, "Invalid Resource");
        }
        Enqueue(evt);
        return Status::OK();
    }

    void
    Start() {
        if (thread_ptr_ == nullptr) {
            thread_ptr_ = std::make_shared<std::thread>(&EventExecutor::ThreadMain, this);
        }
    }

    void
    Stop() {
        if (thread_ptr_ != nullptr) {
            Enqueue(nullptr);
            thread_ptr_->join();
            thread_ptr_ = nullptr;
            std::cout << "EventExecutor Stopped" << std::endl;
        }
    }

 private:
    void
    ThreadMain() {
        Status status;
        while (true) {
            EventPtr evt = queue_.Take();
            if (evt == nullptr) {
                break;
            }
            /* std::cout << std::this_thread::get_id() << " Dequeue Event " << std::endl; */
            status = evt->Process();
            if (!status.ok()) {
                std::cout << "EventExecutor Handle Event Error: " << status.ToString() << std::endl;
            }
        }
    }

    void
    Enqueue(const EventPtr& evt) {
        queue_.Put(evt);
        if (evt != nullptr) {
            /* std::cout << std::this_thread::get_id() << " Enqueue Event " << std::endl; */
        }
    }

 private:
    ThreadPtr thread_ptr_ = nullptr;
    EventQueue queue_;
};

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
