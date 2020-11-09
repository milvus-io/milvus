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

#include <deque>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "CircleQueue.h"
#include "event/Event.h"
#include "interface/interfaces.h"
#include "scheduler/Definition.h"
#include "scheduler/task/Task.h"

namespace milvus {
namespace scheduler {

enum class TaskTableItemState {
    INVALID,
    START,      // idle
    LOADING,    // loading data from other resource
    LOADED,     // ready to exec or move
    EXECUTING,  // executing, locking util executed or failed
    EXECUTED,   // executed, termination state
    MOVING,     // moving to another resource, locking util executed or failed
    MOVED,      // moved, termination state
};

struct TaskTimestamp : public interface::dumpable {
    uint64_t start = 0;
    uint64_t move = 0;
    uint64_t moved = 0;
    uint64_t load = 0;
    uint64_t loaded = 0;
    uint64_t execute = 0;
    uint64_t executed = 0;
    uint64_t finish = 0;

    json
    Dump() const override;
};

struct TaskTableItem;
using TaskTableItemPtr = std::shared_ptr<TaskTableItem>;

struct TaskTableItem : public interface::dumpable {
    explicit TaskTableItem(TaskTableItemPtr f = nullptr)
        : id(0), task(nullptr), state(TaskTableItemState::INVALID), mutex(), from(std::move(f)) {
    }

    TaskTableItem(const TaskTableItem& src) = delete;
    TaskTableItem(TaskTableItem&&) = delete;

    uint64_t id;               // auto increment from 0;
    TaskPtr task;              // the task;
    TaskTableItemState state;  // the state;
    std::mutex mutex;
    TaskTimestamp timestamp;
    TaskTableItemPtr from;

    bool
    IsFinish();

    bool
    Load();

    bool
    Loaded();

    bool
    Execute();

    bool
    Executed();

    bool
    Move();

    bool
    Moved();

    json
    Dump() const override;

 public:
    void
    SetFinished(const TaskPtr& t);
};

class TaskTable : public interface::dumpable {
 public:
    TaskTable() = default;

    TaskTable(const TaskTable&) = delete;
    TaskTable(TaskTable&&) = delete;

 public:
    json
    Dump() const override;

 public:
    inline void
    RegisterSubscriber(std::function<void(void)> subscriber) {
        subscriber_ = std::move(subscriber);
    }

    TaskTableItemPtr
    PickToLoad();

    TaskTableItemPtr
    PickToExecute();

    void
    PutToLoad(TaskPtr task, TaskTableItemPtr from = nullptr);

    void
    PutToExecute(TaskTableItemPtr task);

 private:
    std::uint64_t id_ = 0;
    std::list<TaskTableItemPtr> load_queue_;
    std::list<TaskTableItemPtr> execute_queue_;
    std::mutex load_mutex_;
    std::mutex execute_mutex_;
    std::function<void(void)> subscriber_ = nullptr;

    // cache last finish avoid Pick task from begin always
    // pick from (last_finish_ + 1)
    // init with -1, pick from (last_finish_ + 1) = 0
    uint64_t last_finish_ = -1;
};

}  // namespace scheduler
}  // namespace milvus
