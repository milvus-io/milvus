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

#include "scheduler/TaskTable.h"
#include "Utils.h"
#include "event/TaskTableUpdatedEvent.h"
#include "utils/Log.h"

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

std::string
ToString(const TaskTimestamp& timestamp) {
    std::stringstream ss;
    ss << "<start=" << timestamp.start;
    ss << ", load=" << timestamp.load;
    ss << ", loaded=" << timestamp.loaded;
    ss << ", execute=" << timestamp.execute;
    ss << ", executed=" << timestamp.executed;
    ss << ", move=" << timestamp.move;
    ss << ", moved=" << timestamp.moved;
    ss << ", finish=" << timestamp.finish;
    ss << ">";
    return ss.str();
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

std::string
TaskTableItem::Dump() {
    std::stringstream ss;
    ss << "<id=" << id;
    ss << ", task=" << task;
    ss << ", state=" << ToString(state);
    ss << ", timestamp=" << ToString(timestamp);
    ss << ">";
    return ss.str();
}

std::vector<uint64_t>
TaskTable::PickToLoad(uint64_t limit) {
    size_t count = 0;
    for (int j = last_finish_ + 1; j < table_.size(); ++j) {
        if (not table_[j]) {
            SERVER_LOG_WARNING << "table[" << j << "] is nullptr";
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
            cross = true;
            indexes.push_back(i);
            ++count;
        }
    }
    return indexes;
}

std::vector<uint64_t>
TaskTable::PickToExecute(uint64_t limit) {
    std::vector<uint64_t> indexes;
    bool cross = false;
    for (uint64_t i = last_finish_ + 1, count = 0; i < table_.size() && count < limit; ++i) {
        if (not cross && table_[i]->IsFinish()) {
            last_finish_ = i;
        } else if (table_[i]->state == TaskTableItemState::LOADED) {
            cross = true;
            indexes.push_back(i);
            ++count;
        }
    }
    return indexes;
}

void
TaskTable::Put(TaskPtr task) {
    std::lock_guard<std::mutex> lock(id_mutex_);
    auto item = std::make_shared<TaskTableItem>();
    item->id = id_++;
    item->task = std::move(task);
    item->state = TaskTableItemState::START;
    item->timestamp.start = get_current_timestamp();
    table_.push_back(item);
    if (subscriber_) {
        subscriber_();
    }
}

void
TaskTable::Put(std::vector<TaskPtr>& tasks) {
    std::lock_guard<std::mutex> lock(id_mutex_);
    for (auto& task : tasks) {
        auto item = std::make_shared<TaskTableItem>();
        item->id = id_++;
        item->task = std::move(task);
        item->state = TaskTableItemState::START;
        item->timestamp.start = get_current_timestamp();
        table_.push_back(item);
    }
    if (subscriber_) {
        subscriber_();
    }
}

TaskTableItemPtr
TaskTable::Get(uint64_t index) {
    return table_[index];
}

// void
// TaskTable::Clear() {
//// find first task is NOT (done or moved), erase from begin to it;
////        auto iterator = table_.begin();
////        while (iterator->state == TaskTableItemState::EXECUTED or
////            iterator->state == TaskTableItemState::MOVED)
////            iterator++;
////        table_.erase(table_.begin(), iterator);
//}

std::string
TaskTable::Dump() {
    std::stringstream ss;
    for (auto& item : table_) {
        ss << item->Dump() << std::endl;
    }
    return ss.str();
}

}  // namespace scheduler
}  // namespace milvus
