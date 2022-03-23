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
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "CircleQueue.h"
#include "event/Event.h"
#include "interface/interfaces.h"
#include "task/SearchTask.h"

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
    TaskTableItemState state;  // the state;
    std::mutex mutex;
    TaskTimestamp timestamp;
    TaskTableItemPtr from;

    TaskPtr
    get_task();

    void
    set_task(TaskPtr task);

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

 private:
    TaskPtr task;  // the task;
};

class TaskTable : public interface::dumpable {
 public:
    TaskTable() : table_(TASK_TABLE_MAX_COUNT) {
    }

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

    void
    Put(TaskPtr task, TaskTableItemPtr from = nullptr);

    size_t
    TaskToExecute();

    std::vector<uint64_t>
    PickToLoad(uint64_t limit);

    std::vector<uint64_t>
    PickToExecute(uint64_t limit);

 public:
    inline const TaskTableItemPtr& operator[](uint64_t index) {
        return table_[index];
    }

    inline const TaskTableItemPtr&
    at(uint64_t index) {
        return table_[index];
    }

    inline size_t
    capacity() {
        return table_.capacity();
    }

    inline size_t
    size() {
        return table_.size();
    }

 public:
    /******** Action ********/

    // TODO(wxyu): bool to Status
    /*
     * Load a task;
     * Set state loading;
     * Called by loader;
     */
    inline bool
    Load(uint64_t index) {
        return table_[index]->Load();
    }

    /*
     * Load task finished;
     * Set state loaded;
     * Called by loader;
     */
    inline bool
    Loaded(uint64_t index) {
        return table_[index]->Loaded();
    }

    /*
     * Execute a task;
     * Set state executing;
     * Called by executor;
     */
    inline bool
    Execute(uint64_t index) {
        return table_[index]->Execute();
    }

    /*
     * Execute task finished;
     * Set state executed;
     * Called by executor;
     */
    inline bool
    Executed(uint64_t index) {
        return table_[index]->Executed();
    }

    /*
     * Move a task;
     * Set state moving;
     * Called by scheduler;
     */

    inline bool
    Move(uint64_t index) {
        return table_[index]->Move();
    }

    /*
     * Move task finished;
     * Set state moved;
     * Called by scheduler;
     */
    inline bool
    Moved(uint64_t index) {
        return table_[index]->Moved();
    }

 private:
    std::uint64_t id_ = 0;
    CircleQueue<TaskTableItemPtr> table_;
    std::mutex table_mutex_; // protect table_
    std::function<void(void)> subscriber_ = nullptr;

    // cache last finish avoid Pick task from begin always
    // pick from (last_finish_ + 1)
    // init with -1, pick from (last_finish_ + 1) = 0
    uint64_t last_finish_ = -1;
};

}  // namespace scheduler
}  // namespace milvus
