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

#include "scheduler/TaskTable.h"
#include "Utils.h"
#include "event/TaskTableUpdatedEvent.h"
#include "scheduler/SchedInst.h"
#include "scheduler/task/FinishedTask.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

#include <src/scheduler/task/SearchTask.h>
#include <ctime>
#include <sstream>
#include <vector>

namespace milvus {
namespace scheduler {

std::string
ToString(TaskTableItemState state) {
    switch (state) {
        case TaskTableItemState::INVALID:
            return "INVALID";
        case TaskTableItemState::START:
            return "START";
        case TaskTableItemState::LOADING:
            return "LOADING";
        case TaskTableItemState::LOADED:
            return "LOADED";
        case TaskTableItemState::EXECUTING:
            return "EXECUTING";
        case TaskTableItemState::EXECUTED:
            return "EXECUTED";
        case TaskTableItemState::MOVING:
            return "MOVING";
        case TaskTableItemState::MOVED:
            return "MOVED";
        default:
            return "";
    }
}

json
TaskTimestamp::Dump() const {
    json ret{
        {"start", start},       {"load", load}, {"loaded", loaded}, {"execute", execute},
        {"executed", executed}, {"move", move}, {"moved", moved},   {"finish", finish},
    };
    return ret;
}

bool
TaskTableItem::IsFinish() {
    return state == TaskTableItemState::MOVED || state == TaskTableItemState::EXECUTED;
}

bool
TaskTableItem::Load() {
    std::unique_lock<std::mutex> lock(mutex);
    if (state == TaskTableItemState::START) {
        state = TaskTableItemState::LOADING;
        lock.unlock();
        timestamp.load = get_current_timestamp();
        return true;
    }
    return false;
}

bool
TaskTableItem::Loaded() {
    std::unique_lock<std::mutex> lock(mutex);
    if (state == TaskTableItemState::LOADING) {
        state = TaskTableItemState::LOADED;
        lock.unlock();
        timestamp.loaded = get_current_timestamp();
        return true;
    }
    return false;
}

bool
TaskTableItem::Execute() {
    std::unique_lock<std::mutex> lock(mutex);
    if (state == TaskTableItemState::LOADED) {
        state = TaskTableItemState::EXECUTING;
        lock.unlock();
        timestamp.execute = get_current_timestamp();
        return true;
    }
    return false;
}

bool
TaskTableItem::Executed() {
    std::unique_lock<std::mutex> lock(mutex);
    if (state == TaskTableItemState::EXECUTING) {
        state = TaskTableItemState::EXECUTED;
        lock.unlock();
        timestamp.executed = get_current_timestamp();
        timestamp.finish = get_current_timestamp();
        return true;
    }
    return false;
}

bool
TaskTableItem::Move() {
    std::unique_lock<std::mutex> lock(mutex);
    if (state == TaskTableItemState::LOADED) {
        state = TaskTableItemState::MOVING;
        lock.unlock();
        timestamp.move = get_current_timestamp();
        return true;
    }
    return false;
}

bool
TaskTableItem::Moved() {
    std::unique_lock<std::mutex> lock(mutex);
    if (state == TaskTableItemState::MOVING) {
        state = TaskTableItemState::MOVED;
        lock.unlock();
        timestamp.moved = get_current_timestamp();
        timestamp.finish = get_current_timestamp();
        return true;
    }
    return false;
}

json
TaskTableItem::Dump() const {
    json ret{
        {"id", id},
        {"task", reinterpret_cast<int64_t>(task.get())},
        {"state", ToString(state)},
        {"timestamp", timestamp.Dump()},
    };
    return ret;
}

void
TaskTableItem::SetFinished(const TaskPtr& t) {
    task = t;
}

TaskTableItemPtr
TaskTable::PickToLoad() {
    std::lock_guard<std::mutex> lock(load_mutex_);
    if (load_queue_.size() > 0) {
        auto task = load_queue_.front();
        load_queue_.pop_front();
        return task;
    } else {
        return nullptr;
    }
}

TaskTableItemPtr
TaskTable::PickToExecute() {
    std::lock_guard<std::mutex> lock(execute_mutex_);
    if (execute_queue_.size() > 0) {
        auto task = execute_queue_.front();
        execute_queue_.pop_front();
        return task;
    } else {
        return nullptr;
    }
}

void
TaskTable::PutToLoad(TaskPtr task, TaskTableItemPtr from) {
    auto item = std::make_shared<TaskTableItem>(std::move(from));
    item->id = id_++;
    item->task = std::move(task);
    item->state = TaskTableItemState::START;
    item->timestamp.start = get_current_timestamp();
    load_queue_.emplace_back(item);
    if (subscriber_) {
        subscriber_();
    }
}

void
TaskTable::PutToExecute(TaskTableItemPtr task) {
    execute_queue_.emplace_back(task);
    if (subscriber_) {
        subscriber_();
    }
}

json
TaskTable::Dump() const {
    std::vector<json> list;
    std::cout << "for start" << std::endl;
    std::cout << "for end" << std::endl;
    return json{list};
}

}  // namespace scheduler
}  // namespace milvus
