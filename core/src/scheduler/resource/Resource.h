// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <condition_variable>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "../TaskTable.h"
#include "../event/Event.h"
#include "../event/FinishTaskEvent.h"
#include "../event/LoadCompletedEvent.h"
#include "../event/StartUpEvent.h"
#include "../event/TaskTableUpdatedEvent.h"
#include "../task/Task.h"
#include "Connection.h"
#include "Node.h"

namespace milvus {
namespace scheduler {

// TODO(wxyu): Storage, Route, Executor
enum class ResourceType {
    DISK = 0,
    CPU = 1,
    GPU = 2,
    TEST = 3,
};

class Resource : public Node, public std::enable_shared_from_this<Resource> {
 public:
    /*
     * Start loader and executor if enable;
     */
    void
    Start();

    /*
     * Stop loader and executor, join it, blocking util thread exited;
     */
    void
    Stop();

    /*
     * wake up loader;
     */
    void
    WakeupLoader();

    /*
     * wake up executor;
     */
    void
    WakeupExecutor();

    inline void
    RegisterSubscriber(std::function<void(EventPtr)> subscriber) {
        subscriber_ = std::move(subscriber);
    }

    json
    Dump() const override;

 public:
    inline std::string
    name() const {
        return name_;
    }

    inline ResourceType
    type() const {
        return type_;
    }

    inline uint64_t
    device_id() const {
        return device_id_;
    }

    TaskTable&
    task_table() {
        return task_table_;
    }

 public:
    inline bool
    HasExecutor() const {
        return enable_executor_;
    }

    // TODO(wxyu): const
    uint64_t
    NumOfTaskToExec();

    // TODO(wxyu): need double ?
    inline uint64_t
    TaskAvgCost() const {
        if (total_task_ == 0) {
            return 0;
        }
        return total_cost_ / total_task_;
    }

    inline uint64_t
    TotalTasks() const {
        return total_task_;
    }

    friend std::ostream&
    operator<<(std::ostream& out, const Resource& resource);

 protected:
    Resource(std::string name, ResourceType type, uint64_t device_id, bool enable_executor);

    /*
     * Implementation by inherit class;
     * Blocking function;
     */
    virtual void
    LoadFile(TaskPtr task) = 0;

    /*
     * Implementation by inherit class;
     * Blocking function;
     */
    virtual void
    Process(TaskPtr task) = 0;

 private:
    /*
     * Pick one task to load;
     * Order by start time;
     */
    TaskTableItemPtr
    pick_task_load();

    /*
     * Pick one task to execute;
     * Pick by start time and priority;
     */
    TaskTableItemPtr
    pick_task_execute();

 private:
    /*
     * Only called by load thread;
     */
    void
    loader_function();

    /*
     * Only called by worker thread;
     */
    void
    executor_function();

 protected:
    uint64_t device_id_;
    std::string name_;

 private:
    ResourceType type_;

    TaskTable task_table_;

    uint64_t total_cost_ = 0;
    uint64_t total_task_ = 0;

    std::function<void(EventPtr)> subscriber_ = nullptr;

    bool running_ = false;
    bool enable_executor_ = true;
    std::thread loader_thread_;
    std::thread executor_thread_;

    bool load_flag_ = false;
    bool exec_flag_ = false;
    std::mutex load_mutex_;
    std::mutex exec_mutex_;
    std::condition_variable load_cv_;
    std::condition_variable exec_cv_;
};

using ResourcePtr = std::shared_ptr<Resource>;
using ResourceWPtr = std::weak_ptr<Resource>;

}  // namespace scheduler
}  // namespace milvus
