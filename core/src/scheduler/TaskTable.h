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

#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

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
    Dump() override;
};

struct TaskTableItem : public interface::dumpable {
    TaskTableItem() : id(0), task(nullptr), state(TaskTableItemState::INVALID), mutex() {
    }

    TaskTableItem(const TaskTableItem& src) = delete;
    TaskTableItem(TaskTableItem&&) = delete;

    uint64_t id;               // auto increment from 0;
    TaskPtr task;              // the task;
    TaskTableItemState state;  // the state;
    std::mutex mutex;
    TaskTimestamp timestamp;

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
    Dump() override;
};

using TaskTableItemPtr = std::shared_ptr<TaskTableItem>;

class TaskTable : public interface::dumpable {
 public:
    TaskTable() = default;

    TaskTable(const TaskTable&) = delete;
    TaskTable(TaskTable&&) = delete;

    inline void
    RegisterSubscriber(std::function<void(void)> subscriber) {
        subscriber_ = std::move(subscriber);
    }

    /*
     * Put one task;
     */
    void
    Put(TaskPtr task);

    /*
     * Put tasks back of task table;
     * Called by DBImpl;
     */
    void
    Put(std::vector<TaskPtr>& tasks);

    /*
     * Return task table item reference;
     */
    TaskTableItemPtr
    Get(uint64_t index);

    /*
     * TODO(wxyu): BIG GC
     * Remove sequence task which is DONE or MOVED from front;
     * Called by ?
     */
    //    void
    //    Clear();

    /*
     * Return true if task table empty, otherwise false;
     */
    inline bool
    Empty() {
        return table_.empty();
    }

    /*
     * Return size of task table;
     */
    inline size_t
    Size() {
        return table_.size();
    }

 public:
    TaskTableItemPtr& operator[](uint64_t index) {
        std::lock_guard<std::mutex> lock(mutex_);
        return table_[index];
    }

    std::deque<TaskTableItemPtr>::iterator
    begin() {
        return table_.begin();
    }

    std::deque<TaskTableItemPtr>::iterator
    end() {
        return table_.end();
    }

 public:
    std::vector<uint64_t>
    PickToLoad(uint64_t limit);

    std::vector<uint64_t>
    PickToExecute(uint64_t limit);

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

 public:
    /*
     * Dump;
     */
    json
    Dump() override;

 private:
    std::uint64_t id_ = 0;
    mutable std::mutex mutex_;
    std::deque<TaskTableItemPtr> table_;
    std::function<void(void)> subscriber_ = nullptr;

    // cache last finish avoid Pick task from begin always
    // pick from (last_finish_ + 1)
    // init with -1, pick from (last_finish_ + 1) = 0
    uint64_t last_finish_ = -1;
};

}  // namespace scheduler
}  // namespace milvus
