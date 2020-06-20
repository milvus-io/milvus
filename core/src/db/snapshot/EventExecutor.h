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
#include "ResourceTypes.h"
#include "utils/BlockingQueue.h"

namespace milvus {
namespace engine {
namespace snapshot {

enum class EventType {
    EVENT_INVALID = 0,
    EVENT_GC = 1,
};

struct EventContext {
    ID_TYPE id;
    std::string res_type;
};

struct Event {
    EventType type;
    EventContext context;

    std::string ToString() {
        return context.res_type + "_" + std::to_string(context.id);
    };
};

using EventPtr = std::shared_ptr<Event>;
using ThreadPtr = std::shared_ptr<std::thread>;
using EventQueue = BlockingQueue<EventPtr>;
using EventQueuePtr = std::shared_ptr<EventQueue>;

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
    Submit(EventPtr& evt) {
        if (evt == nullptr) {
            return Status(SS_INVALID_ARGUMENT_ERROR, "Invalid Resource");
        }
        Enqueue(evt);
        return Status::OK();
    }

    void
    Start() {
        thread_ = std::thread(&EventExecutor::ThreadMain, this);
    }

    void
    Stop() {
        Enqueue(nullptr);
        thread_.join();
        std::cout << "EventExecutor Stopped" << std::endl;
    }

 private:
    void
    ThreadMain() {
        while (true) {
            EventPtr evt = queue_.Take();
            if (evt == nullptr) {
                std::cout << "Stopping EventExecutor thread " << std::this_thread::get_id() << std::endl;
                break;
            }
            std::cout << std::this_thread::get_id() << " Dequeue Event " << evt->ToString() << std::endl;
            switch (evt->type) {
                case EventType::EVENT_GC:
                    break;
                default:
                    break;
            }
        }
    }

    void
    Enqueue(EventPtr evt) {
        queue_.Put(evt);
        if (evt != nullptr) {
            std::cout << std::this_thread::get_id() << " Enqueue Event " << evt->ToString() << std::endl;
        }
    }

 private:
    std::thread thread_;
    EventQueue queue_;
};

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
