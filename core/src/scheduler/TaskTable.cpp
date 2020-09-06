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

std::vector<uint64_t>
TaskTable::PickToLoad(uint64_t limit) {
#if 1
    // TimeRecorder rc("");
    std::vector<uint64_t> indexes;
    bool cross = false;

    uint64_t available_begin = table_.front() + 1;
    for (uint64_t i = 0, loaded_count = 0, pick_count = 0; i < table_.size() && pick_count < limit; ++i) {
        auto index = available_begin + i;
        if (table_[index] == nullptr) {
            break;
        }
        if (index % table_.capacity() == table_.rear()) {
            break;
        }
        if (not cross && table_[index]->IsFinish()) {
            table_.set_front(index);
        } else if (table_[index]->state == TaskTableItemState::LOADED) {
            cross = true;
            ++loaded_count;
            if (loaded_count > 2) {
                return std::vector<uint64_t>();
            }
        } else if (table_[index]->state == TaskTableItemState::START) {
            cross = true;
            auto task = table_[index]->task;

            // if task is a build index task, limit it
            if (task->Type() == TaskType::BuildIndexTask && task->path().Current() == "cpu") {
                if (BuildMgrInst::GetInstance()->NumOfAvailable() < 1) {
                    LOG_SERVER_WARNING_ << "BuildMgr doesnot have available place for building index";
                    continue;
                }
            }
            indexes.push_back(index);
            ++pick_count;
        } else {
            cross = true;
        }
    }
    // rc.ElapseFromBegin("PickToLoad ");
    return indexes;
#else
    size_t count = 0;
    for (uint64_t j = last_finish_ + 1; j < table_.size(); ++j) {
        if (not table_[j]) {
            LOG_SERVER_WARNING_ << "collection[" << j << "] is nullptr";
        }

        if (table_[j]->task->path().Current() == "cpu") {
            if (table_[j]->task->Type() == TaskType::BuildIndexTask && BuildMgrInst::GetInstance()->numoftasks() < 1) {
                return std::vector<uint64_t>();
            }
        }

        if (table_[j]->state == TaskTableItemState::LOADED) {
            ++count;
            if (count > 2)
                return std::vector<uint64_t>();
        }
    }

    std::vector<uint64_t> indexes;
    bool cross = false;
    for (uint64_t i = last_finish_ + 1, count = 0; i < table_.size() && count < limit; ++i) {
        if (not cross && table_[i]->IsFinish()) {
            last_finish_ = i;
        } else if (table_[i]->state == TaskTableItemState::START) {
            auto task = table_[i]->task;
            if (task->Type() == TaskType::BuildIndexTask && task->path().Current() == "cpu") {
                if (BuildMgrInst::GetInstance()->numoftasks() == 0) {
                    break;
                } else {
                    cross = true;
                    indexes.push_back(i);
                    ++count;
                    BuildMgrInst::GetInstance()->take();
                }
            } else {
                cross = true;
                indexes.push_back(i);
                ++count;
            }
        }
    }
    return indexes;
#endif
}

std::vector<uint64_t>
TaskTable::PickToExecute(uint64_t limit) {
    // TimeRecorder rc("");
    std::vector<uint64_t> indexes;
    bool cross = false;
    uint64_t available_begin = table_.front() + 1;
    for (uint64_t i = 0, pick_count = 0; i < table_.size() && pick_count < limit; ++i) {
        uint64_t index = available_begin + i;
        if (not table_[index]) {
            break;
        }
        if (index % table_.capacity() == table_.rear()) {
            break;
        }

        if (not cross && table_[index]->IsFinish()) {
            table_.set_front(index);
        } else if (table_[index]->state == TaskTableItemState::LOADED) {
            cross = true;
            indexes.push_back(index);
            ++pick_count;
        } else {
            cross = true;
        }
    }
    // rc.ElapseFromBegin("PickToExecute ");
    return indexes;
}

void
TaskTable::Put(TaskPtr task, TaskTableItemPtr from) {
    auto item = std::make_shared<TaskTableItem>(std::move(from));
    item->id = id_++;
    item->task = std::move(task);
    item->state = TaskTableItemState::START;
    item->timestamp.start = get_current_timestamp();
    table_.put(std::move(item));
    if (subscriber_) {
        subscriber_();
    }
}

size_t
TaskTable::TaskToExecute() {
    size_t count = 0;
    auto begin = table_.front() + 1;
    for (size_t i = 0; i < table_.size(); ++i) {
        auto index = begin + i;
        if (table_[index] && table_[index]->state == TaskTableItemState::LOADED) {
            ++count;
        }
    }
    return count;
}

json
TaskTable::Dump() const {
    json ret{{"error.message", "not support yet."}};
    return ret;
}

}  // namespace scheduler
}  // namespace milvus
